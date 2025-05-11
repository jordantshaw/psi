import os
import sqlite3

from sqlalchemy import create_engine, Integer, String

DATABASE_PATH = os.environ.get("DB_PATH", 'psi.db')

def get_db_engine():
    return create_engine(f"sqlite:///{DATABASE_PATH}", echo=False)


