import os, json, time, asyncio
import httpx
from fastapi import HTTPException
from dotenv import load_dotenv

print("🟢 [PASSO 1] Carregando variáveis do .env ...")
load_dotenv()
print("✅ Variáveis carregadas.")


HUBSOFT_BASE_URL = os.getenv("HUBSOFT_BASE_URL","").rstrip("/")
CLIENT_ID = os.getenv("HUBSOFT_CLIENT_ID","")
CLIENT_SECRET = os.getenv("HUBSOFT_CLIENT_SECRET","")
USERNAME = os.getenv("HUBSOFT_USERNAME","")
PASSWORD = os.getenv("HUBSOFT_PASSWORD","")
print("🟢 [PASSO 2] Variáveis carregadas da .env:")
print(f"   HUBSOFT_BASE_URL: {HUBSOFT_BASE_URL}")
print(f"   CLIENT_ID: {CLIENT_ID[:5]}...")
print(f"   USERNAME: {USERNAME}")

TOKEN_FILE = os.getenv("HUBSOFT_TOKEN_FILE","hubsoft_token.json")
SKEW_SECONDS = 300

_lock = asyncio.Lock()

def _load_cache():
    print("🟢 [PASSO 3] Verificando se já existe token salvo...")
    if not os.path.exists(TOKEN_FILE):
        print("   ❌ Token ainda não existe localmente.")
        return None
    try:
        with open(TOKEN_FILE, "r", encoding="utf-8") as f:
            cache = json.load(f)
            print(f"   ✅ Token encontrado. Expira em {time.ctime(cache.get('expires_at', 0))}")
            return cache
    except Exception:
        print(f"   ⚠️ Erro ao ler o cache: {e}")
        return None

def _save_cache(data: dict):
    with open(TOKEN_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f)
    print("💾 [PASSO 4] Token salvo em cache local (.hubsoft_token.json).")

def _is_valid(cache: dict) -> bool:
    if not cache:
        return False
    expires_at = cache.get("expires_at", 0)
    valido = time.time() + SKEW_SECONDS < expires_at and bool(cache.get("access_token"))
    print(f"🟢 [PASSO 5] Token ainda é válido? {'✅ SIM' if valido else '❌ NÃO'}")
    return valido

async def _password_grant() -> dict:
    url = f"{HUBSOFT_BASE_URL}/oauth/token"
    payload = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "username": USERNAME,
        "password": PASSWORD,
        "grant_type": "password"
    }
    print("🚀 [PASSO 6] Solicitando novo token via senha...")
    print(f"   URL: {url}")
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(url, json=payload)
        if r.status_code != 200:
            raise HTTPException(status_code=500, detail=f"Erro token: {r.text}")
        data = r.json()
        data["expires_at"] = time.time() + int(data.get("expires_in", 0))
        print("✅ Novo token gerado com sucesso.")
        print(f"   Access Token: {data['access_token'][:50]}...")
        print(f"   Expira em: {time.ctime(data['expires_at'])}")
        return data
    
async def _refresh_grant(refresh_token: str) -> dict:
    url = f"{HUBSOFT_BASE_URL}/oauth/token"
    payload = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token"
    }
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(url, json=payload)
        if r.status_code != 200:
            raise HTTPException(status_code=401, detail=f"Erro refresh token: {r.text}")
        data = r.json()
        data["expires_at"] = time.time() + int(data.get("expires_in", 0))
        return data

async def get_hubsoft_token() -> str:
    async with _lock:
        cache = _load_cache()
        if _is_valid(cache):
            print("✅ [PASSO 7] Reutilizando token existente.")
            return cache["access_token"]
        if cache and cache.get("refresh_token"):
            try:
                refreshed = await _refresh_grant(cache["refresh_token"])
                _save_cache(refreshed)
                return refreshed["access_token"]
            except HTTPException:
                pass
        print("🔁 [PASSO 8] Gerando novo token (cache expirado ou inexistente)...")
        fresh = await _password_grant()
        _save_cache(fresh)
        print("🎉 [PASSO 9] Token pronto para uso!")
        return fresh["access_token"]