import os, datetime
from typing import Optional, Dict, Any
from jose import jwt, JWTError
from passlib.context import CryptContext
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from db_mysql import get_conn

SECRET_KEY = os.getenv("SECRET_KEY", "devsecret-change")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "60"))

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")

def hash_password(password: str) -> str:
    return pwd_context.hash(password)

def verify_password(password: str, password_hash: str) -> bool:
    return pwd_context.verify(password, password_hash)

def create_access_token(data: dict, expires_minutes: int = ACCESS_TOKEN_EXPIRE_MINUTES) -> str:
    to_encode = data.copy()
    expire = datetime.datetime.utcnow() + datetime.timedelta(minutes=expires_minutes)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def get_user_by_email(email: str) -> Optional[Dict[str, Any]]:
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT * FROM users WHERE email=%s LIMIT 1", (email,))
        return cur.fetchone()
    finally:
        conn.close()

def create_user(name: str, email: str, password: str) -> int:
    if get_user_by_email(email):
        raise HTTPException(409, "E-mail já cadastrado.")
    password_hash = hash_password(password)
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO users (name, email, password_hash) VALUES (%s,%s,%s)",
            (name, email, password_hash)
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()

def authenticate_user(email: str, password: str) -> Optional[Dict[str, Any]]:
    user = get_user_by_email(email)
    if not user:
        return None
    if not verify_password(password, user["password_hash"]):
        return None
    if not user["is_active"]:
        raise HTTPException(403, "Usuário desativado.")
    return user

async def get_current_user(token: str = Depends(oauth2_scheme)) -> Dict[str, Any]:
    cred_exc = HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token inválido ou ausente.", headers={"WWW-Authenticate": "Bearer"})
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        email: str = payload.get("sub")
        if email is None:
            raise cred_exc
    except JWTError:
        raise cred_exc
    user = get_user_by_email(email)
    if not user:
        raise cred_exc
    return user
