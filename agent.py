import os
import sys
import subprocess
import pandas as pd
import re
import duckdb
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from dotenv import load_dotenv

load_dotenv()

# --- КОНФИГУРАЦИЯ ---
SCRIPTS_DIR = "scripts_db"
REQUEST_FILE = os.path.join(SCRIPTS_DIR, "request.sql")
ANSWER_FILE = os.path.join(SCRIPTS_DIR, "answer.csv")
RUNNER_SCRIPT = "run_sql_safe.py"
DB_PATH = "db/medinsight.duckdb"

def get_smart_schema(db_path, explicit_relationships=None):
    if not os.path.exists(db_path):
        return f"Error: Database file not found at {db_path}"

    con = duckdb.connect(db_path, read_only=True)
    schema_prompt = "### TABLES & COLUMNS:\n"
    
    # 1. СПИСОК ТАБЛИЦ И ОПИСАНИЯ
    table_descriptions = {
        "insight_cost_by_disease": "ВИТРИНА (20 строк). Агрегаты: стоимость лечения по группам болезней.",
        "insight_gender_disease": "ВИТРИНА (72 строки). Агрегаты: демография (пол, возраст) и болезни.",
        "insight_region_drug_choice": "ВИТРИНА (150k строк). Агрегаты: популярность лекарств по регионам.",
        "prescriptions": "СЫРЫЕ ДАННЫЕ (1 млн строк). Факты выдачи рецептов. Главная таблица.",
        "patients": "Справочник (379k строк). Данные о пациентах (пол, дата рождения, район).",
        "drugs": "Справочник (3k строк). Лекарства (торговое название, стоимость, дозировка).",
        "diagnoses": "Справочник (14k строк). МКБ-10 (расшифровка диагнозов и классы)."
    }

    try:
        tables = con.execute("SHOW TABLES").fetchall()
        table_names = [t[0] for t in tables]
        
        for table in table_names:
            columns_info = con.execute(f"DESCRIBE {table}").fetchall()
            columns_str = ", ".join([f"{col[0]} ({col[1]})" for col in columns_info])
            desc = table_descriptions.get(table, "Таблица данных")
            
            schema_prompt += f"- Table '{table}':\n"
            schema_prompt += f"  Description: {desc}\n"
            schema_prompt += f"  Columns: {columns_str}\n\n"
            
    except Exception as e:
        schema_prompt += f"Error reading schema: {e}"
    finally:
        con.close()

    if explicit_relationships:
        schema_prompt += "### RELATIONSHIPS (JOINS):\n"
        for rel in explicit_relationships:
            schema_prompt += f"- {rel}\n"
            
    return schema_prompt

# Правила JOIN
MY_RELATIONSHIPS = [
    "JOIN patients ON prescriptions.id_пациента = patients.id_пациента",
    "JOIN drugs ON prescriptions.код_препарата = drugs.код_препарата",
    "JOIN diagnoses ON prescriptions.код_диагноза = diagnoses.код_мкб",
    "ЕСЛИ НУЖЕН ПОЛ/ВОЗРАСТ/РЕГИОН ПАЦИЕНТА -> делай JOIN patients.",
    "ЕСЛИ НУЖНО НАЗВАНИЕ ДИАГНОЗА (текст) -> делай JOIN diagnoses и ищи по полю 'название_диагноза'.",
    "ЕСЛИ НУЖНО НАЗВАНИЕ ЛЕКАРСТВА (текст) -> делай JOIN drugs и ищи по полю 'Торговое название'.",
    "ВИТРИНА 'insight_region_drug_choice' уже содержит названия лекарств и регион. НЕ джойни её с patients или drugs без необходимости.",
    "ВИТРИНА 'insight_cost_by_disease' содержит уже посчитанные средние чеки."
]

FEW_SHOT_EXAMPLES = """
### Примеры рабочих SQL запросов:

Q: "Динамика заболеваемости гриппом по месяцам"
SQL: SELECT strftime(дата_рецепта, '%Y-%m') as month, COUNT(*) as cnt FROM prescriptions JOIN diagnoses ON prescriptions.код_диагноза = diagnoses.код_мкб WHERE diagnoses.название_диагноза ILIKE '%грипп%' GROUP BY month ORDER BY month;

Q: "В каком районе больше всего пациентов с диабетом?"
SQL: SELECT region, SUM(prescriptions_count) as cnt FROM insight_region_drug_choice WHERE disease_group ILIKE '%диабет%' OR disease_group ILIKE '%эндокрин%' GROUP BY region ORDER BY cnt DESC LIMIT 1;

Q: "Топ 5 дорогих лекарств"
SQL: SELECT "Торговое название", стоимость FROM drugs ORDER BY стоимость DESC LIMIT 5;
"""

# --- АГЕНТ ---
class OpenRouterSQLAgent:
    def __init__(self, api_key: str):
        self.llm = ChatOpenAI(
            model="meta-llama/llama-3.3-70b-instruct", 
            openai_api_key=api_key,
            openai_api_base="https://openrouter.ai/api/v1",
            temperature=0.1,
            request_timeout=60, # <--- ВАЖНО: Тайм-аут API запроса (чтобы не висело 4 минуты)
            max_retries=2,
            default_headers={"HTTP-Referer": "https://medinsight.com", "X-Title": "Medical Agent"}
        )
        self.db_schema = get_smart_schema(DB_PATH, MY_RELATIONSHIPS)
    
    def _clean_sql(self, text: str) -> str:
        match = re.search(r'```sql(.*?)```', text, re.DOTALL)
        if match:
            return match.group(1).strip()
        text = re.sub(r'^```sql', '', text, flags=re.IGNORECASE)
        text = re.sub(r'^```', '', text)
        return text.strip()

    def _execute_sql(self, sql_query: str):
        if not os.path.exists(SCRIPTS_DIR): os.makedirs(SCRIPTS_DIR, exist_ok=True)
        with open(REQUEST_FILE, "w", encoding="utf-8") as f: f.write(sql_query)
        
        try:
            # <--- ВАЖНО: Добавил timeout=30, чтобы SQL не зависал навечно
            result = subprocess.run(
                [sys.executable, RUNNER_SCRIPT, "request.sql"],
                cwd=SCRIPTS_DIR, capture_output=True, text=True, timeout=30
            )
            if result.returncode != 0:
                return None, result.stderr.strip()
            
            if not os.path.exists(ANSWER_FILE) or os.path.getsize(ANSWER_FILE) == 0:
                return pd.DataFrame(), None
                
            return pd.read_csv(ANSWER_FILE), None
        except subprocess.TimeoutExpired:
            return None, "SQL Query Timed Out (более 30 сек)."
        except Exception as e:
            return None, str(e)

    def _generate_initial_sql(self, question: str) -> str:
        system_message = f"""
        Ты — эксперт SQL-аналитик на DuckDB.
        Твоя задача — генерировать SQL-запросы для медицинской базы данных.

        === DATABASE SCHEMA ===
        {self.db_schema}

        === FEW-SHOT EXAMPLES ===
        {FEW_SHOT_EXAMPLES}
        
        === RULES (CRITICAL) ===
        1. Верни ТОЛЬКО SQL код внутри тегов ```sql ... ```.
        2. Приоритет №1: ВСЕГДА проверяй, можно ли ответить через таблицы 'ВИТРИНА' (insight_...). Они быстрее.
        3. Приоритет №2: Только если нет в витринах, используй prescriptions.
        4. Используй ILIKE '%...%' для поиска текста.
        """
        user_message = f"Напиши SQL запрос для вопроса: {question}"

        prompt_template = ChatPromptTemplate.from_messages([
            ("system", system_message),
            ("human", user_message)
        ])
        
        chain = prompt_template | self.llm
        response = chain.invoke({})
        return self._clean_sql(response.content)

    def _fix_sql_error(self, question: str, bad_sql: str, error_msg: str) -> str:
        system_message = f"""
        Ты — SQL-дебаггер. Твоя задача — исправить ошибку в запросе.
        === SCHEMA ===
        {self.db_schema}
        """
        user_message = f"""
        У меня проблема с ответом на вопрос: "{question}"
        Я написал этот SQL:
        ```sql
        {bad_sql}
        ```
        База данных вернула ошибку:
        {error_msg}
        ЗАДАЧА: Исправь SQL запрос. Верни ТОЛЬКО исправленный SQL код.
        """
        prompt = ChatPromptTemplate.from_messages([("system", system_message), ("human", user_message)])
        chain = prompt | self.llm
        response = chain.invoke({})
        return self._clean_sql(response.content)
    
    def _fix_empty_result(self, question: str, bad_sql: str) -> str:
        system_message = f"""
        Ты — опытный SQL-аналитик / Data Detective.
        Твоя задача — найти данные, которые "потерялись" из-за слишком строгих фильтров.
        === DATABASE SCHEMA ===
        {self.db_schema}
        """
        user_message = f"""
        У меня проблема с ответом на вопрос: "{question}"
        Я выполнил этот SQL запрос:
        ```sql
        {bad_sql}
        ```
        Результат: 0 строк (EMPTY RESULT). Но данные в базе точно должны быть.
        ЗАДАЧА: Перепиши SQL запрос так, чтобы найти данные (используй ILIKE, синонимы).
        Верни ТОЛЬКО исправленный SQL код.
        """
        prompt = ChatPromptTemplate.from_messages([("system", system_message), ("human", user_message)])
        chain = prompt | self.llm
        response = chain.invoke({})
        return self._clean_sql(response.content)

    def _analyze_data(self, question: str, df: pd.DataFrame) -> str:
        if df is None: return "⚠️ Ошибка выполнения запроса."
        if df.empty: return "Данных не найдено даже после нескольких попыток."

        df_head = df.head(50).to_markdown(index=False)
        system_message = """
        Ты — профессиональный медицинский аналитик.
        Твоя задача — ответить на вопрос пользователя, опираясь ИСКЛЮЧИТЕЛЬНО на предоставленные данные.
        ПРАВИЛА ОТВЕТА:
        1. Отвечай кратко и по делу.
        2. Обязательно приводи конкретные цифры из таблицы.
        3. Не описывай структуру таблицы.
        """
        user_message = f"""
        Вопрос пользователя: "{question}"
        Полученные данные из БД:
        {df_head}
        Сделай вывод на основе этих данных.
        """
        prompt_template = ChatPromptTemplate.from_messages([("system", system_message), ("human", user_message)])
        chain = prompt_template | self.llm
        response = chain.invoke({})
        return response.content

    def answer(self, user_question: str):
        try:
            current_sql = self._generate_initial_sql(user_question)
            print(f"🔹 GENERATED SQL: {current_sql}")

            MAX_RETRIES = 3 
            
            for attempt in range(MAX_RETRIES + 1):
                df, error = self._execute_sql(current_sql)
                
                if error:
                    print(f"🔸 ATTEMPT {attempt+1} SQL ERROR: {error}")
                    if attempt < MAX_RETRIES:
                        current_sql = self._fix_sql_error(user_question, current_sql, error)
                        continue
                    else:
                        return f"🚫 Не удалось выполнить запрос. Ошибка: {error}"

                if df.empty:
                    print(f"🔸 ATTEMPT {attempt+1} EMPTY RESULT (0 rows).")
                    if attempt < MAX_RETRIES:
                        current_sql = self._fix_empty_result(user_question, current_sql)
                        continue
                    else:
                        return "📭 По вашему запросу данных не найдено."

                print(f"✅ SUCCESS ({len(df)} rows)")
                return self._analyze_data(user_question, df)
        except Exception as e:
            return f"🔥 Критическая ошибка агента: {str(e)}"