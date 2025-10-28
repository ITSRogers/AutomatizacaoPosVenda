from fastapi import FastAPI, HTTPException, Query, Depends, Body
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel, EmailStr, Field
from typing import Optional, List
import httpx
import logging
import asyncio
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
    url = f"{HUBSOFT_BASE_URL}/api/v1/integracao/ordem_servico/consultar"
    total_baixadas = 0
    total_salvas = 0
    ultima_paginacao = None
    for start_page in (0, 1):
        pagina = start_page
        teve_dados = False
        async with httpx.AsyncClient(timeout=60) as c:
            while True:
                payload = {
                    "pagina": pagina,
                    "itens_por_pagina": itens_por_pagina,
                    "data_inicio": data_inicio,
                    "data_fim": data_fim,
                }
                print(f"[IMPORT_OS] POST {url} pagina={pagina} payload={payload}")
                r = await c.post(url, headers=headers, json=payload)
                if r.status_code != 200:
                    raise HTTPException(status_code=r.status_code, detail=r.text)
                data = r.json()
                lista = data.get("ordens_servico") or data.get("dados") or data.get("itens") or []
                pag = data.get("paginacao") or {}
                ultima_paginacao = pag
                print(f"[IMPORT_OS] resp.status={data.get('status')} msg={data.get('msg')} pag={pag} itens={len(lista)}")
                if not lista:
                    break
                teve_dados = True
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
        if teve_dados:
            break
    return {
        "status": "success",
        "intervalo": [data_inicio, data_fim],
        "total_baixadas": total_baixadas,
        "total_salvas": total_salvas,
        "paginacao_ultima_resposta": ultima_paginacao,
    }

DEFAULT_RELACOES = ["tecnicos", "motivos_fechamento", "cobrancas_disponiveis", "equipamentos_insumos", "atendimento", "assinatura"]
@app.post("/import_os_detalhado")
async def import_os_detalhado(
    data_inicio: str = Query(..., description="YYYY-MM-DD"),
    data_fim: str = Query(..., description="YYYY-MM-DD"),
    itens_por_pagina: int = 200,
    relacoes: List[str] = Body(DEFAULT_RELACOES, embed=True, description="Relacionamentos extras para /consultar"),
    user: dict = Depends(get_current_user),
):
    _valapp_fileidosso_data(data_inicio)
    _valida_data(data_fim)
    headers = {"Authorization": f"Bearer {await get_hubsoft_token()}"}
    url_todos = f"{HUBSOFT_BASE_URL}/api/v1/integracao/ordem_servico/todos"
    url_consultar = f"{HUBSOFT_BASE_URL}/api/v1/integracao/ordem_servico/consultar"
    numeros: List[str] = []
    pagina = 0
    async with httpx.AsyncClient(timeout=60) as c:
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
    total_salvas = 0
    sem = asyncio.Semaphore(6)
    async def fetch_and_upsert(num: str) -> int:
        nonlocal total_baixadas
        payload = {"consulta": num, "relacoes": relacoes}
        async with sem:
            try:
                async with httpx.AsyncClient(timeout=60) as c2:
                    print(f"[CONSULTAR] POST {url_consultar} consulta={num}")
                    r2 = await c2.post(url_consultar, headers=headers, json=payload)
                    r2.raise_for_status()
                    jd = r2.json()
                    dets = jd.get("ordens_s­ervico") or jd.get("ordens_servico") or jd.get("dados") or []
                    if not isinstance(dets, list):
                        dets = [dets]
                    n = len(dets)
                    total_baixadas += n
                    if n:
                        return upsert_ordens(dets)
                    return 0
            except Exception as e:
                print(f"[CONSULTAR] ERRO num={num}: {e}")
                return 0
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