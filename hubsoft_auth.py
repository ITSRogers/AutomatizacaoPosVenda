import os
import json
import time
import asyncio
from typing import Any, Dict, Optional
import httpx
from fastapi import HTTPException
from dotenv import load_dotenv

load_dotenv()

HUBSOFT_BASE_URL = os.getenv("HUBSOFT_BASE_URL", "").rstrip("/")
CLIENT_ID = os.getenv("HUBSOFT_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("HUBSOFT_CLIENT_SECRET", "")
USERNAME = os.getenv("HUBSOFT_USERNAME", "")
PASSWORD = os.getenv("HUBSOFT_PASSWORD", "")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TOKEN_FILE = os.getenv(
    "HUBSOFT_TOKEN_FILE",
    os.path.join(BASE_DIR, ".cache_hubsoft_token.json")
)

SKEW_SECONDS = 300
_lock = asyncio.Lock()

def _load_cache() -> Optional[Dict[str, Any]]:
    if not os.path.exists(TOKEN_FILE):
        print("🔐 [AUTH] Cache de token não encontrado.")
        return None
    try:
        with open(TOKEN_FILE, "r", encoding="utf-8") as f:
            cache = json.load(f)
        print(f"🔐 [AUTH] Cache carregado. Expira em {time.ctime(cache.get('expires_at', 0))}")
        return cache
    except Exception as e:
        print(f"⚠️  [AUTH] Falha ao ler cache de token: {e}")
        return None

def _save_cache(data: Dict[str, Any]) -> None:
    try:
        with open(TOKEN_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f)
        print("💾 [AUTH] Token salvo em cache.")
    except Exception as e:
        print(f"⚠️  [AUTH] Falha ao salvar cache: {e}")

def _invalidate_cache() -> None:
    try:
        if os.path.exists(TOKEN_FILE):
            os.remove(TOKEN_FILE)
            print("🧹 [AUTH] Cache de token invalidado/removido.")
    except Exception as e:
        print(f"⚠️  [AUTH] Falha ao invalidar cache: {e}")

def _is_valid(cache: Optional[Dict[str, Any]]) -> bool:
    if not cache:
        return False
    expires_at = cache.get("expires_at", 0)
    return (time.time() + SKEW_SECONDS) < expires_at and bool(cache.get("access_token"))

# Grants

async def _password_grant() -> Dict[str, Any]:
    url = f"{HUBSOFT_BASE_URL}/oauth/token"
    payload = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "username": USERNAME,
        "password": PASSWORD,
        "grant_type": "password",
    }
    print("🚀 [AUTH] Solicitando novo token (password grant)...")
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(url, json=payload)
        if r.status_code != 200:
            raise HTTPException(status_code=500, detail=f"Erro ao obter token: {r.text}")
        data = r.json()
        data["expires_at"] = time.time() + int(data.get("expires_in", 0))
        print(f"✅ [AUTH] Novo token obtido. Expira em {time.ctime(data['expires_at'])}")
        return data

async def _refresh_grant(refresh_token: str) -> Dict[str, Any]:
    url = f"{HUBSOFT_BASE_URL}/oauth/token"
    payload = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
    }
    print("🔁 [AUTH] Tentando refresh do token...")
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(url, json=payload)
        if r.status_code != 200:
            raise HTTPException(status_code=401, detail=f"Erro em refresh token: {r.text}")
        data = r.json()
        data["expires_at"] = time.time() + int(data.get("expires_in", 0))
        print(f"✅ [AUTH] Token renovado. Expira em {time.ctime(data['expires_at'])}")
        return data

# API externa: obter token / headers

async def get_hubsoft_token() -> str:
    async with _lock:
        cache = _load_cache()
        if _is_valid(cache):
            return cache["access_token"]
        if cache and cache.get("refresh_token"):
            try:
                refreshed = await _refresh_grant(cache["refresh_token"])
                _save_cache(refreshed)
                return refreshed["access_token"]
            except HTTPException:
                pass

        fresh = await _password_grant()
        _save_cache(fresh)
        return fresh["access_token"]

async def get_auth_headers() -> Dict[str, str]:
    token = await get_hubsoft_token()
    return {"Authorization": f"Bearer {token}"}

async def _request_with_retry(
    method: str,
    path: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    timeout: int = 60,
) -> Dict[str, Any]:

    # Chamada HTTP à API Hubsoft com retry em caso de 401 (expiração do token).
    
    if not HUBSOFT_BASE_URL:
        raise HTTPException(500, "HUBSOFT_BASE_URL não configurada.")
    url = f"{HUBSOFT_BASE_URL.rstrip('/')}/{path.lstrip('/')}"
    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.request(method, url, headers=await get_auth_headers(), params=params, json=json_body)
        if r.status_code == 401:
            _invalidate_cache()
            r = await client.request(method, url, headers=await get_auth_headers(), params=params, json=json_body)
        r.raise_for_status()
        try:
            return r.json()
        except ValueError:
            return {"status_code": r.status_code, "text": r.text}

async def hubsoft_get(path: str, params: Optional[Dict[str, Any]] = None, timeout: int = 60) -> Dict[str, Any]:
    return await _request_with_retry("GET", path, params=params, timeout=timeout)

async def hubsoft_post(path: str, json_body: Optional[Dict[str, Any]] = None, timeout: int = 60) -> Dict[str, Any]:
    return await _request_with_retry("POST", path, json_body=json_body, timeout=timeout)