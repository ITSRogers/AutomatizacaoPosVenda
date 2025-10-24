import os
from dotenv import load_dotenv
import mysql.connector
from fastapi import HTTPException

load_dotenv()

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_DB = os.getenv("MYSQL_DB", "posvenda")
MYSQL_USER = os.getenv("MYSQL_USER", "rodrigo")
MYSQL_PWD = os.getenv("MYSQL_PASSWORD", "Opsim354")

def get_conn():
    try:
        return mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            database=MYSQL_DB,
            user=MYSQL_USER,
            password=MYSQL_PWD,
            autocommit=False
        )
    except mysql.connector.Error as e:
        # mensagem amigável:
        msg = (f"Falha ao conectar no MySQL {MYSQL_HOST}:{MYSQL_PORT} "
               f"db={MYSQL_DB} user={MYSQL_USER} -> {e}")
        print("❌", msg)
        raise HTTPException(status_code=500, detail=msg)