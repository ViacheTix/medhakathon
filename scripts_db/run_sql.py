# scripts_db/run_sql.py
import duckdb
import sys
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "db" / "medinsight.duckdb"
SQL_FILE = Path(sys.argv[1])

con = duckdb.connect(str(DB_PATH))
with open(SQL_FILE, "r", encoding="utf-8") as f:
    con.execute(f.read())
con.close()
print("✅ Выполнено.")