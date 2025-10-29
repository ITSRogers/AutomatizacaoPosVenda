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

def _valida_data(s: str) -> None:
    try:
        datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(422, f"Data inválida: {s}. Use YYYY-MM-DD")

async def _get_cliente_por_codigo(codigo: str, token: str) -> dict | None:
    """
    Consulta /cliente e retorna o cliente cujo codigo_cliente == codigo.
    Tenta várias estratégias de query porque o Hubsoft varia por ambiente.
    """
    url_cli = f"{HUBSOFT_BASE_URL}/api/v1/integracao/cliente"
    headers = {"Authorization": f"Bearer {token}"}

    # Estratégias de query (ordem importa)
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

                # 1) preferência: match exato por codigo_cliente
                for cli in clientes:
                    if str(cli.get("codigo_cliente")) == str(codigo):
                        print(f"[CLIENTE] match exato codigo_cliente={codigo} -> id={cli.get('id_cliente')}")
                        return cli

                # 2) fallback: se só vier 1 cliente, e o rótulo do cliente na OS bate parcialmente com o nome
                # (esse fallback vamos decidir no chamador quando tivermos o 'rotulo')
            except Exception as e:
                print(f"[CLIENTE] erro HTTP params={params} err={e}")

    return None

def _extrai_codigo_cliente(rotulo: str | None) -> str | None:
    """Extrai o número entre o primeiro parênteses do campo cliente, ex.: '(2131) Fulano' -> '2131'."""
    if not rotulo: 
        return None
    m = re.match(r"\((\d+)\)", rotulo.strip())
    return m.group(1) if m else None

def _enriquecer_os_com_cliente(item: dict, cliente: dict) -> None:
    """Monta os blocos esperados pelo map_item: dados_cliente, dados_servico, dados_endereco_instalacao."""
    # dados do cliente
    dados_cliente = {
        "id_cliente": cliente.get("id_cliente"),
        "codigo_cliente": cliente.get("codigo_cliente"),
        "nome_razaosocial": cliente.get("nome_razaosocial"),
        "telefones": {
            "telefone_primario": cliente.get("telefone_primario"),
            "telefone_secundario": cliente.get("telefone_secundario"),
        },
    }

    # tenta descobrir o serviço “mais provável” (habilitado; senão o primeiro)
    servicos = cliente.get("servicos") or []
    svc = None
    for s in servicos:
        if (s.get("status_prefixo") or "").lower().startswith("servico_habilitado"):
            svc = s
            break
    if not svc and servicos:
        svc = servicos[0]

    dados_servico = {}
    dados_endereco_instalacao = {}
    if svc:
        dados_servico = {
            "id_cliente_servico": svc.get("id_cliente_servico"),
            "descricao": (svc.get("nome") or svc.get("referencia") or "").strip() or None,
        }
        end_inst = svc.get("endereco_instalacao") or {}
        coords = end_inst.get("coordenadas") or {}
        dados_endereco_instalacao = {
            "endereco": end_inst.get("endereco"),
            "numero": end_inst.get("numero"),
            "bairro": end_inst.get("bairro"),
            "cidade": end_inst.get("cidade"),
            "estado": end_inst.get("estado"),
            "cep": end_inst.get("cep"),
            "coordenadas": {
                "latitude": coords.get("latitude"),
                "longitude": coords.get("longitude"),
            },
        }

    # injeta nos campos esperados pelo map_item
    item["dados_cliente"] = dados_cliente
    item["dados_servico"] = dados_servico
    item["dados_endereco_instalacao"] = dados_endereco_instalacao

# ------------------------------
# Helpers de normalização Hubsoft
# ------------------------------
def _to_list(x):
    if not x:
        return []
    return x if isinstance(x, list) else [x]

def _extract_os_from_consultar(jd: dict) -> List[dict]:
    """Extrai lista de OS das possíveis estruturas retornadas pelo /consultar."""
    # se houver status lógico e ele indicar erro, devolve vazio
    st = (jd.get("status") or "").strip().lower()
    if st and st not in ("ok", "success", "sucesso"):
        # deixe o caller logar msg se quiser
        return []
    # raiz
    dets = _to_list(jd.get("ordens_servico")) \
        or _to_list(jd.get("ordem_servico")) \
        or _to_list(jd.get("itens"))
    # aninhado
    if not dets:
        dados = jd.get("dados") or {}
        if isinstance(dados, dict):
            dets = _to_list(dados.get("ordens_servico")) \
                or _to_list(dados.get("ordem_servico")) \
                or _to_list(dados.get("itens"))
        else:
            dets = _to_list(dados)
    return dets

ALLOWED_RELACOES = {"tecnicos", "motivos_fechamento", "cobrancas_disponiveis", "assinatura", "atendimento"}

def _sanitize_relacoes(relacoes_in):
    relacoes_in = relacoes_in or []
    ok = [r for r in relacoes_in if r in ALLOWED_RELACOES]
    dropped = [r for r in relacoes_in if r not in ALLOWED_RELACOES]
    if dropped:
        print(f"[RELACOES] Removidas por não suportadas: {dropped} | Mantidas: {ok}")
    return ok

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
DEFAULT_RELACOES = ["tecnicos", "motivos_fechamento", "cobrancas_disponiveis", "assinatura", "atendimento"]

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
    cliente_cache: dict[str, dict] = {}
    numeros: List[str] = []
    relacoes = _sanitize_relacoes(relacoes)
    pagina = 0

    async with httpx.AsyncClient(timeout=60) as c:
        # 1) Listagem
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

        # 2) Detalhamento com degradação progressiva de relações + ENRIQUECIMENTO VIA /cliente
        total_baixadas = 0
        sem = asyncio.Semaphore(6)

        async def fetch_and_upsert(num: str) -> int:
            nonlocal total_baixadas

            rels_candidates = [
                relacoes,  # mais completo possível
                [r for r in relacoes if r != "assinatura"],
                [r for r in relacoes if r != "atendimento"],
                [r for r in relacoes if r in ("tecnicos", "motivos_fechamento", "cobrancas_disponiveis")],
                [],  # sem relações
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

                        # erro lógico relacionado a relações → tenta próximo degrau
                        if st and st not in ("ok", "success", "sucesso") and ("relac" in msg or "relação" in msg):
                            print(f"[CONSULTAR] rejeitou relacoes={rels} num={num} msg={msg}")
                            continue
                        # erro lógico genérico → aborta essa OS
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

                # normaliza assinatura_assinado (0/1/NULL) + ENRIQUECE COM /cliente quando faltarem blocos
                for item in dets:
                    # assinatura 0/1/NULL
                    ass = (item.get("assinatura") or {})
                    v = ass.get("assinado")
                    item["assinatura_assinado"] = 1 if v is True else 0 if v is False else None

                    # enriquecer se faltar dados_cliente/dados_servico/dados_endereco_instalacao
                    precisa_cliente = not (item.get("dados_cliente") and item.get("dados_servico") and item.get("dados_endereco_instalacao"))
                    if precisa_cliente:
                        rotulo = item.get("cliente")  # ex.: "(2131) Fulano"
                        codigo = _extrai_codigo_cliente(rotulo)
                        if codigo:
                            # CACHE
                            cli = cliente_cache.get(codigo)
                            if cli is None:
                                cli = await _get_cliente_por_codigo(codigo, token)
                                cliente_cache[codigo] = cli  # guarda até None
                            if cli:
                                print(f"[ENRIQUECER] OK codigo={codigo} nome='{cli.get('nome_razaosocial')}'")
                                _enriquecer_os_com_cliente(item, cli, rotulo_cliente=rotulo)
                            else:
                                print(f"[ENRIQUECER] NAO ENCONTRADO codigo={codigo} rotulo='{rotulo}'")
                        else:
                            print(f"[ENRIQUECER] sem codigo_cliente no rotulo='{rotulo}'")

                n = len(dets)
                total_baixadas += n
                return upsert_ordens(dets) if n else 0

        # dispara paralelismo e soma salvas
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
