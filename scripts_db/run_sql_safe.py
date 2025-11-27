# scripts_db/run_sql_safe.py
import duckdb
import sys
import re
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "db" / "medinsight.duckdb"
SQL_FILE = Path(sys.argv[1])
OUTPUT_CSV = Path("answer.csv")

DANGEROUS = {'CREATE','DROP','INSERT','UPDATE','DELETE','ALTER','TRUNCATE','REPLACE','COPY'}

def is_safe(q):
    clean = re.sub(r'--.*', '', q)
    tokens = re.findall(r'\b\w+\b', clean.upper())
    return not any(t in DANGEROUS for t in tokens[:3])

con = duckdb.connect(str(DB_PATH), read_only=True)
with open(SQL_FILE, "r", encoding="utf-8") as f:
    sql = f.read().strip()

queries = [q.strip() for q in sql.split(";") if q.strip()]

for i, query in enumerate(queries):
    if not is_safe(query):
        print(f"❌ Запрещённый запрос {i+1}")
        sys.exit(1)
    
    # Добавляем лимит 50, если SELECT и нет LIMIT
    if query.upper().startswith("SELECT") and "LIMIT" not in query.upper():
        query += " LIMIT 50"

    df = con.execute(query).fetchdf()
    print(f"\n🟢 Результат {i+1} (макс. 50 строк):")
    print(df.to_string(index=False))
    
    # Сохраняем в CSV (перезаписываем каждый раз)
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")

con.close()
print(f"\n💾 Результат сохранён в: {OUTPUT_CSV}")