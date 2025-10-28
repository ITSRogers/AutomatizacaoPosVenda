import os, asyncio, logging
from datetime import datetime, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz
import httpx
from hubsoft_auth import get_hubsoft_token, HUBSOFT_BASE_URL
from os_repository import upsert_ordens

TZ = os.getenv("TIMEZONE", "America/Sao_Paulo")
tz = pytz.timezone(TZ)
log = logging.getLogger("scheduler")

async def importar_intervalo(data_inicio: str, data_fim: str, itens_por_pagina: int = 100):
    token = await get_hubsoft_token()
    headers = {"Authorization": f"Bearer {token}"}
    pagina = 0
    total_baixadas = 0
    total_salvas = 0
    async with httpx.AsyncClient(timeout=60) as c:
        while True:
            params = {
                "pagina": pagina,
                "itens_por_pagina": itens_por_pagina,
                "data_inicio": data_inicio,
                "data_fim": data_fim
            }
            r = await c.get(f"{HUBSOFT_BASE_URL}/api/v1/integracao/ordem_servico/todos",
                            headers=headers, params=params)
            r.raise_for_status()
            data = r.json()
            lista = data.get("ordens_servico") or []
            if not lista:
                break
            total_baixadas += len(lista)
            total_salvas += upsert_ordens(lista)
            pag = data.get("paginacao") or {}
            if pag and (pag.get("pagina_atual") >= pag.get("ultima_pagina", 0)):
                break
            if len(lista) < params["itens_por_pagina"]:
                break
            pagina += 1
    log.info(f"[IMPORTADOR] {data_inicio}..{data_fim} -> baixadas={total_baixadas} salvas={total_salvas}")

async def job_diario_ontem():
    agora = datetime.now(tz)
    ontem = (agora - timedelta(days=1)).date()
    di = ontem.strftime("%Y-%m-%d")
    df = ontem.strftime("%Y-%m-%d")
    await importar_intervalo(di, df, itens_por_pagina=200)

def start_scheduler():
    scheduler = AsyncIOScheduler(timezone=tz)
    scheduler.add_job(job_diario_ontem, CronTrigger(hour=0, minute=5))
    scheduler.start()
    log.info("⏰ Scheduler iniciado (job diário às 00:05).")
