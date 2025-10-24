from fastapi import FastAPI, HTTPException, Query
from typing import Optional
import httpx
from hubsoft_auth import get_hubsoft_token, HUBSOFT_BASE_URL
from dotenv import load_dotenv
from datetime import date, timedelta
from os_repository import upsert_ordens, list_ordens, get_ordem, list_concluidas_ontem

load_dotenv()
app = FastAPI()

@app.get("/")
def home():
    return {"status": "API Online"}

# EndPoint principal
@app.post("/import_os")
async def import_os(data_inicio: str = Query(..., description="YYYY-MM-DD"),
                    data_fim: str = Query(..., description="YYYY-MM-DD"),
                    itens_por_pagina:int = 10):
    token = await get_hubsoft_token()
    headers = {"Authorization": f"Bearer {token}"}
    pagina = 0
    total_baixadas = 0
    total_salvas = 0
    async with httpx.AsyncClient(timeout=60) as c:
        while True:
            params = {"pagina": pagina,
                    "itens_por_pagina": itens_por_pagina,
                    "data_inicio": data_inicio,
                    "data_fim": data_fim}
            r = await c.get(f"{HUBSOFT_BASE_URL}/api/v1/integracao/ordem_servico/todos", headers=headers, params=params)
            if r.status_code != 200:
                raise HTTPException(status_code=502, detail=f"Erro: {r.text}")
            data = r.json()
            lista = data.get("ordens_servico") or data.get("dados") or data.get("itens") or []
            print("Página:", pagina, "→", len(lista), "ordens recebidas")
            if not lista:
                break
            total_baixadas += len(lista)
            total_salvas += upsert_ordens(lista)
            pag = data.get("paginacao") or {}
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
        "total_salvas": total_salvas
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