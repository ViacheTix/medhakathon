# scripts_db/01_setup_db.py
import duckdb
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT / "db" / "medinsight.duckdb"
DATA_DIR = PROJECT_ROOT / "data"

# Удаляем старую БД
if DB_PATH.exists():
    DB_PATH.unlink()

DB_PATH.parent.mkdir(exist_ok=True)
print("📁 Создаём чистую базу данных...")

con = duckdb.connect(str(DB_PATH))

# === 1. patients (без дубликатов) ===
print("✅ Загружаем patients (уникальные)...")
con.execute(f"""
    CREATE TABLE patients AS
    SELECT DISTINCT ON (id_пациента)
        id_пациента,
        дата_рождения,
        пол,
        район_проживания,
        регион
    FROM read_csv_auto(
        '{DATA_DIR / "данные_пациентов.csv"}',
        header=true,
        nullstr='',
        types={{'id_пациента': 'VARCHAR'}},
        strict_mode=false,
        ignore_errors=true,
        null_padding=true
    )
    WHERE id_пациента IS NOT NULL
    ORDER BY id_пациента;
""")

# === 2. diagnoses и drugs ===
for name, file in [("diagnoses", "данные_диагнозы.csv"), ("drugs", "данные_препараты.csv")]:
    print(f"✅ Загружаем {name}...")
    con.execute(f"""
        CREATE TABLE {name} AS
        SELECT * FROM read_csv_auto(
            '{DATA_DIR / file}',
            header=true,
            nullstr='',
            types={{'код_мкб': 'VARCHAR', 'код_препарата': 'VARCHAR'}},
            strict_mode=false,
            ignore_errors=true,
            null_padding=true
        );
    """)

# === 3. prescriptions — через ПОСЛЕДНЮЮ колонку id_пациента_1 ===
print("✅ Загружаем prescriptions (связь через id_пациента_1)...")
con.execute(f"""
    CREATE TABLE prescriptions AS
    SELECT
        CAST("id_пациента_1" AS VARCHAR) AS id_пациента,
        дата_рецепта,
        код_диагноза,
        код_препарата
    FROM read_csv_auto(
        '{DATA_DIR / "данные_рецептов.csv"}',
        header=true,
        nullstr='',
        types={{
            'id_пациента': 'VARCHAR',
            'id_пациента_1': 'VARCHAR',
            'код_диагноза': 'VARCHAR',
            'код_препарата': 'VARCHAR'
        }},
        strict_mode=false,
        ignore_errors=true,
        null_padding=true
    )
    WHERE "id_пациента_1" IS NOT NULL;
""")

# === 4. Проверка связности ===
total_patients = con.execute("SELECT COUNT(*) FROM patients").fetchone()[0]
total_presc = con.execute("SELECT COUNT(*) FROM prescriptions").fetchone()[0]
linked = con.execute("""
    SELECT COUNT(*)
    FROM prescriptions p
    JOIN patients pa ON p.id_пациента = pa.id_пациента
""").fetchone()[0]

print(f"\n📊 Проверка связности:")
print(f"   Пациентов: {total_patients:,}")
print(f"   Рецептов:  {total_presc:,}")
print(f"   Связано:   {linked:,} ({linked/total_presc:.1%})")

con.close()
print("\n✨ База готова! Все данные доступны напрямую.")