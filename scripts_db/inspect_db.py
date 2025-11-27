# scripts_db/inspect_db.py
import duckdb
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "db" / "medinsight.duckdb"
con = duckdb.connect(str(DB_PATH))

tables = con.execute("""
    SELECT table_name, table_type 
    FROM information_schema.tables 
    WHERE table_schema = 'main'
    ORDER BY table_name
""").fetchdf()

for _, r in tables.iterrows():
    name, typ = r["table_name"], r["table_type"]
    count = con.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]
    print(f"\n🔹 {name} ({typ}) — {count:,} строк")
    cols = con.execute(f'DESCRIBE "{name}"').fetchdf()
    for _, c in cols.iterrows():
        print(f"  • {c['column_name']} : {c['column_type']}")

con.close()
print("\n✅ Готово.")