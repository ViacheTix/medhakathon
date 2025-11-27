# scripts_db/list_tables.py
import duckdb

con = duckdb.connect("../db/medinsight.duckdb")
tables = con.execute("SHOW TABLES").fetchdf()
print("Таблицы в базе:")
print(tables)
con.close()