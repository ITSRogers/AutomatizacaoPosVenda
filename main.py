from fastapi import FastAPI, HTTPException, Query, Depends, Body
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel, EmailStr, Field
from functools import lru_cache
from typing import Optional, List
import httpx, logging, asyncio, re
from scheduler import start_scheduler
from hubsoft_auth import get_hubsoft_token, HUBSOFT_BASE_URL
from dotenv import load_dotenv
from datetime import date, datetime
from os_repository import upsert_ordens, list_ordens, get_ordem, list_concluidas_ontem
from auth_backend import create_user, authenticate_user, create_access_token, get_current_user

load_dotenv()
app = FastAPI()

# ===== Validação e de login/register =====
class RegisterIn(BaseModel):
    name: str = Field(..., min_length=2, max_length=120)
    email: EmailStr
    password: str = Field(..., min_length=6)

class TokenOut(BaseModel):
    access_token: str
    token_type: str = "bearer"

@app.get("/")
def home():
    return {"status": "API Online"}

@app.post("/auth/register", response_model=TokenOut)
def register(payload: RegisterIn):
    create_user(payload.name, payload.email, payload.password)
    token = create_access_token({"sub": payload.email})
    return {"access_token": token, "token_type": "bearer"}

@app.post("/auth/login", response_model=TokenOut)
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    user = authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(401, "Credenciais inválidas.")
    token = create_access_token({"sub": user["email"]})
    return {"access_token": token, "token_type": "bearer"}


# ===== Valida a data do filtro =====
def _valida_data(s: str) -> None:
    try:
        datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(422, f"Data inválida: {s}. Use YYYY-MM-DD")

# ===== Validação do codigo cliente =====
# realiza a extração do código do cliente na ordem e faz a comparação para puxar os dados do cliente
async def _get_cliente_por_codigo(codigo: str, token: str) -> dict | None:
    url_cli = f"{HUBSOFT_BASE_URL}/api/v1/integracao/cliente"
    headers = {"Authorization": f"Bearer {token}"}
    attempts = [
        {"busca": "", "termo_busca": codigo, "limit": 5},
        {"busca": "codigo_cliente", "termo_busca": codigo, "limit": 5},
        {"busca": "codigo", "termo_busca": codigo, "limit": 5},
    ]
    async with httpx.AsyncClient(timeout=60) as c:
        for params in attempts:
            try:
                r = await c.get(url_cli, headers=headers, params=params)
                r.raise_for_status()
                j = r.json() or {}
                clientes = j.get("clientes") or []
                print(f"[CLIENTE] tentativa params={params} retornou {len(clientes)} cliente(s)")
                for cli in clientes:
                    if str(cli.get("codigo_cliente")) == str(codigo):
                        print(f"[CLIENTE] match exato codigo_cliente={codigo} -> id={cli.get('id_cliente')}")
                        return cli
            except Exception as e:
                print(f"[CLIENTE] erro HTTP params={params} err={e}")
    return None
def _extrai_codigo_cliente(rotulo: str | None) -> str | None:
    if not rotulo: 
        return None
    m = re.match(r"\((\d+)\)", rotulo.strip())
    return m.group(1) if m else None

def _enriquecer_os_com_cliente(item: dict, cliente: dict, rotulo_cliente: str | None = None) -> None:
    base_dados_cliente = {
        "id_cliente": cliente.get("id_cliente"),
        "codigo_cliente": cliente.get("codigo_cliente"),
        "nome_razaosocial": cliente.get("nome_razaosocial"),
        "telefones": {
            "telefone_primario": cliente.get("telefone_primario"),
            "telefone_secundario": cliente.get("telefone_secundario"),
        },
    }
    item["dados_cliente"] = _merge_missing(item.get("dados_cliente", {}), base_dados_cliente)
    servicos = cliente.get("servicos") or []
    svc = next((s for s in servicos if (s.get("status_prefixo") or "").lower().startswith("servico_habilitado")), None)
    if not svc and servicos:
        svc = servicos[0]
    base_dados_servico = {}
    base_end_inst = {}
    ds_tmp = item.setdefault("dados_servico", {})
    if svc:
        base_dados_servico = {
            "id_cliente_servico": svc.get("id_cliente_servico"),
            "descricao": (svc.get("nome") or svc.get("referencia") or "").strip() or None,
        }
        end_inst = svc.get("endereco_instalacao") or {}
        coords = (end_inst.get("coordenadas") or {})
        ds_tmp["_tmp_endereco_completo_inst"] = end_inst.get("completo")
        rua, numero = _normaliza_endereco_from_completo(end_inst.get("completo"))
        base_end_inst = {
            "endereco": end_inst.get("endereco") or rua,
            "numero": end_inst.get("numero") or numero,
            "bairro": end_inst.get("bairro"),
            "cidade": end_inst.get("cidade"),
            "estado": end_inst.get("estado") or end_inst.get("uf"),
            "cep": end_inst.get("cep"),
            "coordenadas": {
                "latitude": coords.get("latitude"),
                "longitude": coords.get("longitude"),
            },
        }
    for key_cli, tmpkey in (
        ("endereco_cadastral", "_tmp_endereco_completo_cad"),
        ("endereco_fiscal", "_tmp_endereco_completo_fiscal"),
        ("endereco_cobranca", "_tmp_endereco_completo_cobr"),
    ):
        blk = cliente.get(key_cli) or {}
        if isinstance(blk, dict) and blk.get("completo"):
            ds_tmp[tmpkey] = blk.get("completo")
    item["dados_servico"] = _merge_missing(item.get("dados_servico", {}), base_dados_servico)
    item["dados_endereco_instalacao"] = _merge_missing(item.get("dados_endereco_instalacao", {}), base_end_inst)


def _merge_missing(dst: dict, src: dict) -> dict:
    if not isinstance(dst, dict):
        dst = {}
    for k, v in (src or {}).items():
        if isinstance(v, dict):
            dst[k] = _merge_missing(dst.get(k, {}), v)
        else:
            if dst.get(k) in (None, "", {}, []):
                dst[k] = v
    return dst
def _normaliza_endereco_from_completo(completo: str | None) -> tuple[str | None, str | None]:
    if not completo:
        return (None, None)
    parte_rua = completo.split(" - ", 1)[0]
    pedacos = [p.strip() for p in parte_rua.split(",")]
    if len(pedacos) >= 2:
        rua = pedacos[0]
        numero = pedacos[1]
    else:
        rua = parte_rua.strip()
        numero = None
    for pref in ("RUA ", "AV ", "AV.", "AVENIDA ", "TRAVESSA ", "ALAMEDA ", "RODOVIA "):
        if rua.upper().startswith(pref):
            rua = rua[len(pref):].strip()
            break
    return (rua or None, numero or None)

# ------------------------------
# Helpers de normalização Hubsoft
# ------------------------------
def _to_list(x):
    if not x:
        return []
    return x if isinstance(x, list) else [x]

def _extract_os_from_consultar(jd: dict) -> List[dict]:
    st = (jd.get("status") or "").strip().lower()
    if st and st not in ("ok", "success", "sucesso"):
        return []
    dets = _to_list(jd.get("ordens_servico")) \
        or _to_list(jd.get("ordem_servico")) \
        or _to_list(jd.get("itens"))
    if not dets:
        dados = jd.get("dados") or {}
        if isinstance(dados, dict):
            dets = _to_list(dados.get("ordens_servico")) \
                or _to_list(dados.get("ordem_servico")) \
                or _to_list(dados.get("itens"))
        else:
            dets = _to_list(dados)
    return dets

ALLOWED_RELACOES = {"tecnicos", "motivos_fechamento", "cobrancas_disponiveis", "assinatura"}

def _sanitize_relacoes(relacoes_in):
    relacoes_in = relacoes_in or []
    ok = [r for r in relacoes_in if r in ALLOWED_RELACOES]
    dropped = [r for r in relacoes_in if r not in ALLOWED_RELACOES]
    if dropped:
        print(f"[RELACOES] Removidas por não suportadas: {dropped} | Mantidas: {ok}")
    return ok

def _is_blank(v):
    return v in (None, "", [], {})

def _any_missing_address(end: dict) -> bool:
    if not isinstance(end, dict):
        return True
    need = [
        "endereco", "numero", "bairro", "cidade", "estado", "cep",
    ]
    if any(_is_blank(end.get(k)) for k in need):
        return True
    coords = end.get("coordenadas") or {}
    if _is_blank(coords.get("latitude")) or _is_blank(coords.get("longitude")):
        return True
    return False

def _apply_address_fallbacks(item: dict) -> None:
    end = item.get("dados_endereco_instalacao") or {}
    if not isinstance(end, dict):
        end = {}
    candidatos = []
    ds = item.get("dados_servico") or {}
    for key in ("_tmp_endereco_completo_inst",
                "_tmp_endereco_completo_cad",
                "_tmp_endereco_completo_fiscal",
                "_tmp_endereco_completo_cobr"):
        v = ds.get(key)
        if v:
            candidatos.append(v)
    for key in ("endereco_instalacao", "endereco_instalacao_text"):
        v = item.get(key)
        if v:
            candidatos.append(v)
    for comp in candidatos:
        parsed = _parse_from_completo(comp)
        if not parsed:
            continue
        if _is_blank(end.get("endereco")) and parsed.get("endereco"):
            end["endereco"] = parsed["endereco"]
        if _is_blank(end.get("numero")) and parsed.get("numero"):
            end["numero"] = parsed["numero"]
        for k in ("bairro", "cidade", "estado", "cep"):
            if _is_blank(end.get(k)) and parsed.get(k):
                end[k] = parsed[k]
        if not _any_missing_address(end):
            break
    item["dados_endereco_instalacao"] = end
CEP_RE = re.compile(r"\b(\d{5}-?\d{3})\b")

def _parse_from_completo(completo: str | None) -> dict:
    if not completo or not isinstance(completo, str):
        return {}
    s = completo.strip()
    cep = None
    mcep = CEP_RE.search(s)
    if mcep:
        cep = mcep.group(1)
        s = CEP_RE.sub("", s)
        s = s.replace("CEP:", "").replace("CEP", "").strip(" |,-")
    partes = [p.strip() for p in s.split(" - ")]
    rua_num = partes[0] if partes else ""
    resto = " - ".join(partes[1:]) if len(partes) > 1 else ""
    pedacos_rua = [p.strip() for p in rua_num.split(",")]
    if len(pedacos_rua) >= 2:
        rua = pedacos_rua[0]
        numero = pedacos_rua[1]
    else:
        rua = rua_num.strip()
        numero = None
    for pref in ("RUA ", "AV ", "AV.", "AVENIDA ", "TRAVESSA ", "ALAMEDA ", "RODOVIA "):
        if rua.upper().startswith(pref):
            rua = rua[len(pref):].strip()
            break
    bairro = cidade = estado = None
    if resto:
        pedacos_rest = [p.strip() for p in resto.split(",")]
        if pedacos_rest:
            poss_bairro = pedacos_rest[0]
            poss_ciduf = None
            if len(pedacos_rest) >= 2:
                poss_ciduf = pedacos_rest[1]
            else:
                poss_ciduf = poss_bairro if "/" in poss_bairro else None
            if poss_bairro and "/" not in poss_bairro:
                bairro = poss_bairro
            if poss_ciduf and "/" in poss_ciduf:
                poss_ciduf = poss_ciduf.split(" - ", 1)[0].strip()
                sub = poss_ciduf.split(",")[-1].strip()
                if "/" in sub:
                    cid, uf = sub.split("/", 1)
                    cidade = cid.strip(" ,|-")
                    estado = uf.strip(" ,|-")
            else:
                m = re.search(r",\s*([^,/-][^,]+?)\s*/\s*([A-Za-z]{2})\b", resto)
                if m:
                    cidade = m.group(1).strip()
                    estado = m.group(2).strip()
    if cidade:
        cidade = " ".join(cidade.split())
    if estado:
        estado = estado.upper()[:2]
    return {
        "endereco": rua or None,
        "numero": numero or None,
        "bairro": bairro or None,
        "cidade": cidade or None,
        "estado": estado or None,
        "cep": cep or None,
    }


@app.post("/import_os")
async def import_os(
    data_inicio: str = Query(..., description="YYYY-MM-DD"),
    data_fim: str = Query(..., description="YYYY-MM-DD"),
    itens_por_pagina: int = 100,
    user: dict = Depends(get_current_user)
):
    _valida_data(data_inicio)
    _valida_data(data_fim)

    headers = {"Authorization": f"Bearer {await get_hubsoft_token()}"}
    url = f"{HUBSOFT_BASE_URL}/api/v1/integracao/ordem_servico/todos"
    total_baixadas = 0
    total_salvas = 0
    ultima_paginacao = None
    pagina = 0

    async with httpx.AsyncClient(timeout=60) as c:
        while True:
            params = {
                "pagina": pagina,
                "itens_por_pagina": itens_por_pagina,
                "data_inicio": data_inicio,
                "data_fim": data_fim,
            }
            print(f"[IMPORT_OS] GET {url} params={params}")
            r = await c.get(url, headers=headers, params=params)
            if r.status_code != 200:
                raise HTTPException(status_code=r.status_code, detail=r.text)
            data = r.json()
            lista = data.get("ordens_servico") or data.get("dados") or data.get("itens") or []
            pag = data.get("paginacao") or {}
            ultima_paginacao = pag
            print(f"[IMPORT_OS] status={data.get('status')} msg={data.get('msg')} pag={pag} itens={len(lista)}")
            if not lista:
                break
            total_baixadas += len(lista)
            total_salvas += upsert_ordens(lista)
            ult = pag.get("ultima_pagina")
            atual = pag.get("pagina_atual")
            if ult is not None and atual is not None:
                if atual >= ult:
                    break
            else:
                if len(lista) < itens_por_pagina:
                    break
            pagina += 1
    return {
        "status": "success",
        "intervalo": [data_inicio, data_fim],
        "total_baixadas": total_baixadas,
        "total_salvas": total_salvas,
        "paginacao_ultima_resposta": ultima_paginacao,
    }

# ------------------------------
# Import detalhado (lista em /todos e enriquece com /consultar)
# ------------------------------
DEFAULT_RELACOES = ["tecnicos", "motivos_fechamento", "cobrancas_disponiveis", "assinatura"]

@app.post("/import_os_detalhado")
async def import_os_detalhado(
    data_inicio: str = Query(..., description="YYYY-MM-DD"),
    data_fim: str = Query(..., description="YYYY-MM-DD"),
    itens_por_pagina: int = 200,
    relacoes: List[str] = Body(DEFAULT_RELACOES, embed=True, description="Relacionamentos extras para /consultar"),
    user: dict = Depends(get_current_user),
):
    _valida_data(data_inicio)
    _valida_data(data_fim)

    token = await get_hubsoft_token()
    headers = {"Authorization": f"Bearer {token}"}
    url_todos = f"{HUBSOFT_BASE_URL}/api/v1/integracao/ordem_servico/todos"
    url_consultar = f"{HUBSOFT_BASE_URL}/api/v1/integracao/ordem_servico/consultar"
    numeros: List[str] = []
    relacoes = _sanitize_relacoes(relacoes)
    pagina = 0
    cliente_cache: dict[str, dict] = {}
    limits = httpx.Limits(max_keepalive_connections=20, max_connections=50)
    async with httpx.AsyncClient(timeout=60, limits=limits) as c:
        while True:
            params = {
                "pagina": pagina,
                "itens_por_pagina": itens_por_pagina,
                "data_inicio": data_inicio,
                "data_fim": data_fim,
            }
            print(f"[TODOS] GET {url_todos} params={params}")
            r = await c.get(url_todos, headers=headers, params=params)
            r.raise_for_status()
            j = r.json()
            itens = j.get("ordens_servico") or j.get("dados") or j.get("itens") or []
            print(f"[TODOS] pagina={pagina} itens={len(itens)} paginacao={j.get('paginacao')}")
            if not itens:
                break
            for it in itens:
                num = it.get("numero")
                if num is not None:
                    numeros.append(str(num))
            pag = j.get("paginacao") or {}
            ult = pag.get("ultima_pagina")
            atual = pag.get("pagina_atual")
            if ult is not None and atual is not None:
                if atual >= ult:
                    break
            else:
                if len(itens) < itens_por_pagina:
                    break
            pagina += 1
        if not numeros:
            return {
                "status": "success",
                "intervalo": [data_inicio, data_fim],
                "mensagem": "Nenhuma O.S. listada no período via /todos.",
                "total_baixadas": 0,
                "total_salvas": 0,
                "relacoes_usadas": relacoes,
            }
        total_baixadas = 0
        sem = asyncio.Semaphore(6)
        async def fetch_and_upsert(num: str) -> int:
            nonlocal total_baixadas
            rels_candidates = [
                relacoes,
                [r for r in relacoes if r != "assinatura"],
                [r for r in relacoes if r != "atendimento"],
                [r for r in relacoes if r in ("tecnicos", "motivos_fechamento", "cobrancas_disponiveis")],
                [],
            ]
            async with sem:
                jd_ok = None
                used_rels = None
                for rels in rels_candidates:
                    try:
                        payload = {"consulta": num}
                        if rels:
                            payload["relacoes"] = rels
                        print(f"[CONSULTAR] POST {url_consultar} consulta={num} relacoes={rels}")
                        r2 = await c.post(url_consultar, headers=headers, json=payload)
                        r2.raise_for_status()
                        jd = r2.json()
                        st = (jd.get("status") or "").strip().lower()
                        msg = (jd.get("msg") or jd.get("mensagem") or "").lower()
                        if st and st not in ("ok", "success", "sucesso") and ("relac" in msg or "relação" in msg):
                            print(f"[CONSULTAR] rejeitou relacoes={rels} num={num} msg={msg}")
                            continue
                        if st and st not in ("ok", "success", "sucesso"):
                            print(f"[CONSULTAR] status={jd.get('status')} msg={jd.get('msg') or jd.get('mensagem')} num={num}")
                            return 0
                        jd_ok = jd
                        used_rels = rels
                        break
                    except Exception as e:
                        body = None
                        try:
                            body = r2.text
                        except Exception:
                            pass
                        print(f"[CONSULTAR] ERRO HTTP num={num} relacoes={rels} err={e} body={body}")
                if jd_ok is None:
                    return 0
                dets = _extract_os_from_consultar(jd_ok)
                if not dets:
                    print(f"[CONSULTAR] sem OS reconhecível num={num} relacoes={used_rels} keys={list(jd_ok.keys())}")
                    return 0
                for item in dets:
                    ass = (item.get("assinatura") or {})
                    v = ass.get("assinado")
                    item["assinatura_assinado"] = 1 if v is True else 0 if v is False else None
                    end = item.get("dados_endereco_instalacao") or {}
                    precisa_cliente = (_is_blank(item.get("dados_cliente")) or _is_blank(item.get("dados_servico")) or _any_missing_address(end))
                    if precisa_cliente:
                        rotulo = item.get("cliente")
                        codigo = _extrai_codigo_cliente(rotulo)
                        if codigo:
                            if codigo not in cliente_cache:
                                cliente_cache[codigo] = await _get_cliente_por_codigo(codigo, token)
                            cli = cliente_cache[codigo]
                            if cli:
                                print(f"[ENRIQUECER] OK codigo={codigo} nome='{cli.get('nome_razaosocial')}'")
                                _enriquecer_os_com_cliente(item, cli, rotulo_cliente=rotulo)
                            else:
                                print(f"[ENRIQUECER] NAO ENCONTRADO codigo={codigo} rotulo='{rotulo}'")
                        else:
                            print(f"[ENRIQUECER] sem codigo_cliente no rotulo='{rotulo}'")
                    _apply_address_fallbacks(item)
                n = len(dets)
                total_baixadas += n
                return upsert_ordens(dets) if n else 0
        saved_counts = await asyncio.gather(*(fetch_and_upsert(n) for n in numeros))
        total_salvas = sum(saved_counts)
    return {
        "status": "success",
        "intervalo": [data_inicio, data_fim],
        "total_numeros_encontrados": len(numeros),
        "total_baixadas": total_baixadas,
        "total_salvas": total_salvas,
        "relacoes_usadas": relacoes,
    }

# ------------------------------
# APIs de consulta ao banco
# ------------------------------
@app.get("/api/ordens")
def api_listar_ordens(
    status: Optional[str] = Query(None, description="Filtro por status (parcial, ex.: 'Finaliz')"),
    q: Optional[str] = Query(None, description="Busca por número/tipo/cliente/cidade"),
    data_inicio: Optional[str] = Query(None, description="YYYY-MM-DD"),
    data_fim: Optional[str] = Query(None, description="YYYY-MM-DD"),
    page: int = 1,
    page_size: int = 50
):
    if page < 1: page = 1
    if page_size < 1 or page_size > 500: page_size = 50
    offset = (page - 1) * page_size
    res = list_ordens(status, q, data_inicio, data_fim, page_size, offset)
    return {
        "items": res["items"],
        "page": page,
        "page_size": page_size,
        "total": res["total"],
        "total_pages": (res["total"] + page_size - 1) // page_size
    }

@app.get("/api/ordens/{id_os}")
def api_ordem_detalhe(id_os: int):
    row = get_ordem(id_os)
    if not row:
        raise HTTPException(404, "O.S. não encontrada")
    return row

@app.get("/api/relatorios/concluidas-ontem")
def api_concluidas_ontem():
    itens = list_concluidas_ontem()
    return {
        "data_referencia": str(date.today()),
        "total": len(itens),
        "items": itens
    }

logging.basicConfig(level=logging.INFO)

@app.on_event("startup")
async def on_startup():
    start_scheduler()
