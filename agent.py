import os
import subprocess
import pandas as pd
import re
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

# --- КОНФИГУРАЦИЯ ---
# Пути к скриптам базы данных
SCRIPTS_DIR = "scripts_db"
REQUEST_FILE = os.path.join(SCRIPTS_DIR, "request.sql")
ANSWER_FILE = os.path.join(SCRIPTS_DIR, "answer.csv")
RUNNER_SCRIPT = "run_sql_safe.py" # Имя скрипта внутри папки scripts_db

# --- ОБНОВЛЕННАЯ СХЕМА БД ---
DB_SCHEMA = """
Ты работаешь с базой данных DuckDB 'medinsight.duckdb'.
В базе есть "Сырые данные" (Base Tables) и "Аналитические витрины" (Insight Tables).

--- ПРИОРИТЕТ ВЫБОРА ТАБЛИЦ ---
1. СНАЧАЛА проверь, можно ли ответить на вопрос через таблицы `insight_...`. Это готовые агрегаты, они точнее.
2. ТОЛЬКО ЕСЛИ в витринах нет нужных данных (нужен сложный фильтр по датам или редкая выборка), используй сырые таблицы (`prescriptions`...).

=== 1. АНАЛИТИЧЕСКИЕ ВИТРИНЫ (INSIGHT TABLES) ===

A. insight_cost_by_disease (Финансы):
   - disease_group (VARCHAR): Группа болезней.
   - avg_cost_per_prescription (DOUBLE): Средняя цена рецепта.
   - avg_cost_per_patient (DOUBLE): Средняя цена лечения пациента.
   - top_expensive_drugs (VARCHAR): Список дорогих лекарств.
   *Используй для вопросов про стоимость лечения.*

B. insight_gender_disease (Демография):
   - disease_group (VARCHAR).
   - age_group (VARCHAR): Возраст ('18-25', '60+').
   - male_patients (BIGINT), female_patients (BIGINT).
   - female_minus_male (BIGINT): Разница полов.
   - conclusion (VARCHAR): Готовый текстовый вывод.
   *Используй для вопросов: "Кто чаще болеет?", "Распределение по полу".*

C. insight_region_drug_choice (География и Лекарства):
   - region (VARCHAR): Район.
   - disease_group (VARCHAR).
   - drug_name (VARCHAR): Название лекарства.
   - prescriptions_count (BIGINT): Популярность.
   *Используй для вопросов: "Что выписывают в Центральном районе?", "Популярные лекарства".*

=== 2. СЫРЫЕ ДАННЫЕ (BASE TABLES) ===

1. patients (пациенты) [379k строк]:
   - id_пациента (VARCHAR) [PK]
   - дата_рождения (DATE)
   - пол (VARCHAR) -> 'М', 'Ж'
   - район_проживания (VARCHAR)
   - регион (VARCHAR)

2. prescriptions (рецепты) [1 млн строк]:
   - id_пациента (VARCHAR) [FK]
   - дата_рецепта (TIMESTAMP)
   - код_диагноза (VARCHAR) [FK]
   - код_препарата (VARCHAR) [FK]

3. diagnoses (справочник):
   - код_мкб (VARCHAR) [PK]
   - название_диагноза (VARCHAR) -> Поиск ILIKE.
   - класс_заболевания (VARCHAR)

4. drugs (справочник):
   - код_препарата (VARCHAR) [PK]
   - Торговое название (VARCHAR)
   - стоимость (DOUBLE)

   Если по имеющейся структуре данных БД невозможно написать SQL запрос,
   то в качестве ответа дай пустую строку.
=== ПРАВИЛА ГЕНЕРАЦИИ SQL (СОБЛЮДАЙ СТРОГО) ===
   Никогда не пиши `WHERE name = 'Ветрянка'`.
   ВСЕГДА пиши `WHERE name ILIKE '%Ветрянка%'`.
   СИНОНИМЫ И МЕДИЦИНСКИЙ КОНТЕКСТ (КРИТИЧЕСКИ ВАЖНО):
   Пользователь пишет простым языком. Ты должен искать медицинское.
   Если вопрос подразумевает рейтинг (Топ, Чаще всего) -> ВСЕГДА `ORDER BY ... DESC LIMIT 5`.
   Генерируй ТОЛЬКО чистый SQL код. Без Markdown.
"""

class OpenRouterSQLAgent:
    def __init__(self, api_key: str):
        # Настройка OpenRouter
        self.llm = ChatOpenAI(
            # MODEL SELECTION:
            # "meta-llama/llama-3.3-70b-instruct" - Очень умная, дешевая.
            # "anthropic/claude-3.5-sonnet" - Лучшая в мире, но дороже.
            # "qwen/qwen-2.5-72b-instruct" - Топ для кода и SQL.
            model="meta-llama/llama-3.3-70b-instruct", 
            
            openai_api_key=api_key,
            openai_api_base="https://openrouter.ai/api/v1",
            temperature=0,
            # заголовки для OpenRouter
            default_headers={
                "HTTP-Referer": "https://medinsight-hack.com", # Это нам точно нужно?
                "X-Title": "Medical Insight Agent"
            }
        )
    # Очистка output'a запроса SQL от формата markdown
    def _clean_sql(self, text: str) -> str:
        text = text.strip()
        text = re.sub(r'^```sql', '', text, flags=re.IGNORECASE)
        text = re.sub(r'^```', '', text)
        text = re.sub(r'```$', '', text)
        return text.strip()
    # Просим LLM создать нам SQL запрос
    def _generate_sql(self, question: str) -> str:
        prompt = ChatPromptTemplate.from_messages([
            ("system", f"{DB_SCHEMA}"),
            ("human", question)
        ])
        chain = prompt | self.llm
        response = chain.invoke({})
        return self._clean_sql(response.content)
    # Исполнение написанного SQL промпта и сохранение ответа в answer.csv
    def _execute_sql(self, sql_query: str) -> pd.DataFrame:
        # Логика 1-в-1 как в agent.py
        if not os.path.exists(SCRIPTS_DIR):
            os.makedirs(SCRIPTS_DIR, exist_ok=True)
        with open(REQUEST_FILE, "w", encoding="utf-8") as f:
            f.write(sql_query)
        
        try:
            result = subprocess.run(
                ["python3", RUNNER_SCRIPT, "request.sql"],
                cwd=SCRIPTS_DIR,
                capture_output=True,
                text=True
            )
            if result.returncode != 0:
                print(f"SQL Error: {result.stderr}")
                return None
            
            if os.path.exists(ANSWER_FILE) and os.path.getsize(ANSWER_FILE) > 0:
                return pd.read_csv(ANSWER_FILE)
            else:
                return pd.DataFrame()
        except Exception as e:
            print(f"Exec Error: {e}")
            return None
    # Итоговый вердикт по данным от LLM
    def _analyze_data(self, question: str, df: pd.DataFrame, sql: str) -> str:
        if df is None or df.empty:
            return "Данные не найдены или ошибка SQL."
            
        data_md = df.to_markdown(index=False)
        
        prompt = ChatPromptTemplate.from_messages([
            ("system", "Ты — экспертный медицинский аналитик. Сделай вывод на основе данных."),
            ("human", f"Вопрос: {question}\nSQL: {sql}\nДанные:\n{data_md}")
        ])
        chain = prompt | self.llm
        response = chain.invoke({})
        return response.content

    def answer(self, user_question: str):
        print(f"Ожидаем ответ от OpenRouter…")
        sql = self._generate_sql(user_question)
        print(f"SQL: {sql}")
        df = self._execute_sql(sql)
        return self._analyze_data(user_question, df, sql)