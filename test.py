import asyncio
from datetime import date, timedelta
from hubsoft_auth import get_hubsoft_token, HUBSOFT_BASE_URL
import httpx
from dotenv import load_dotenv

load_dotenv()
async def main():
    token = await get_hubsoft_token()
    print("TOKEN:", token[:40], "...")  # só pra ver o início
    headers = {"Authorization": f"Bearer {token}"}
    data_fim = date.today()
    data_inicio = data_fim - timedelta(days=7)
    params = {"pagina": 0, "itens_por_pagina": 5, "data_inicio": data_inicio.isoformat(), "data_fim": data_fim.isoformat()}
    async with httpx.AsyncClient() as c:
        r = await c.get(f"{HUBSOFT_BASE_URL}/api/v1/integracao/ordem_servico/todos",
                        headers=headers, params=params)
        print(r.status_code)
        print(r.json())

asyncio.run(main())
