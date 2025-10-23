from fastapi import FastAPI, HTTPException
import httpx, os
from hubsoft_auth import get_hubsoft_token, HUBSOFT_BASE_URL
from dotenv import load_dotenv
from datetime import date, timedelta

load_dotenv()
app = FastAPI()

@app.get("/")
def home():
    return {"status": "API Online"}

# EndPoint principal
@app.get("/import_os")
async def import_os():
    token = await get_hubsoft_token()
    headers = {"Authorization": f"Bearer {token}"}
    data_fim = date.today()
    data_inicio = data_fim - timedelta(days=7)
    params = {"pagina": 0, "itens_por_pagina": 1,"data_inicio": data_inicio.isoformat(), "data_fim": data_fim.isoformat()}
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.get(f"{HUBSOFT_BASE_URL}/api/v1/integracao/ordem_servico/todos", headers=headers, params=params)
        if r.status_code != 200:
            raise HTTPException(status_code=502, detail=f"Erro ao buscar OS: {r.text}")
        return r.json()