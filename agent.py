import os
import sys
import subprocess
import pandas as pd
import re
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from dotenv import load_dotenv

load_dotenv()

# --- КОНФИГУРАЦИЯ ---
SCRIPTS_DIR = "scripts_db"
REQUEST_FILE = os.path.join(SCRIPTS_DIR, "request.sql")
ANSWER_FILE = os.path.join(SCRIPTS_DIR, "answer.csv")
RUNNER_SCRIPT = "run_sql_safe.py"

# --- СХЕМА БД (ОСТАВЛЯЕМ КАК ЕСТЬ) ---
DB_SCHEMA = """
Ты работаешь с базой данных DuckDB 'medinsight.duckdb'.
В базе есть "Сырые данные" (Base Tables) и "Аналитические витрины" (Insight Tables).

--- ПРИОРИТЕТ ВЫБОРА ТАБЛИЦ (CRITICAL) ---
1. СНАЧАЛА смотри на таблицы `insight_...`. Они маленькие и быстрые.
2. ТОЛЬКО ЕСЛИ там нет ответа, иди в тяжелые таблицы `prescriptions`.

=== 1. АНАЛИТИЧЕСКИЕ ВИТРИНЫ (INSIGHT TABLES) ===

A. insight_cost_by_disease (Всего 20 строк!):
   * Используй для вопросов про деньги, стоимость лечения, самые дорогие болезни.
   - disease_group (VARCHAR): Группа болезней.
   - avg_cost_per_prescription (DOUBLE): Ср. чек рецепта.
   - avg_cost_per_patient (DOUBLE): Ср. стоимость на пациента.
   - top_expensive_drugs (VARCHAR): Список дорогих лекарств (текст).

B. insight_gender_disease (Всего 72 строки!):
   * Используй для демографии (Пол, Возраст, "Кто чаще болеет").
   - disease_group (VARCHAR).
   - age_group (VARCHAR): Группы '0-17', '18-25' и т.д.
   - male_patients (BIGINT), female_patients (BIGINT).
   - total_patients (BIGINT).
   - female_minus_male (BIGINT): Разница (>0 значит женщин больше).
   - conclusion (VARCHAR): Готовый текстовый вывод.
   - top_diagnoses (VARCHAR): Популярные диагнозы в группе.

C. insight_region_drug_choice (~150k строк):
   * Используй для анализа по Регионам/Районам.
   - region (VARCHAR): Район проживания.
   - disease_group (VARCHAR).
   - drug_code (VARCHAR), drug_name (VARCHAR).
   - prescriptions_count (BIGINT): Сколько раз выписали.
   - prescriptions_share (DOUBLE): Доля популярности (0.0 - 1.0).

=== 2. СЫРЫЕ ДАННЫЕ (BASE TABLES) ===

1. patients (~379k строк):
   - id_пациента (VARCHAR) [PK]
   - дата_рождения (DATE) -> считай возраст через `date_diff('year', дата_рождения, CURRENT_DATE)`.
   - пол (VARCHAR) -> 'М', 'Ж'.
   - район_проживания (VARCHAR).
   - регион (VARCHAR).

2. prescriptions (ТЯЖЕЛАЯ ТАБЛИЦА):
   - id_пациента (VARCHAR) [FK]
   - дата_рецепта (TIMESTAMP) -> Используй для временных рядов (по месяцам/годам).
   - код_диагноза (VARCHAR) [FK]
   - код_препарата (VARCHAR) [FK] -> Это ID! Для названия нужен JOIN с drugs.

3. diagnoses (14k строк):
   - код_мкб (VARCHAR) [PK]
   - название_диагноза (VARCHAR) -> Поиск через ILIKE.
   - класс_заболевания (VARCHAR).

4. drugs (3k строк):
   - код_препарата (VARCHAR) [PK]
   - Торговое название (VARCHAR) -> Используй для поиска лекарств.
   - стоимость (DOUBLE).
   - Полное_название (VARCHAR).
   *ИГНОРИРУЙ колонки: column5, column6, column7 (это мусор).*

=== ПРАВИЛА ГЕНЕРАЦИИ SQL ===
1. СИНОНИМЫ: Если ищешь болезнь в `diagnoses`, ищи и по `название_диагноза` (ILIKE '%народное%'), и по `класс_заболевания`.
2. ПОИСК: Всегда используй `ILIKE` вместо `=`.
3. ЛИМИТЫ: Для топов всегда `LIMIT 5`. Для динамики (графиков) - без лимита.
4. СВЯЗИ: В таблице `prescriptions` лежат только коды. Всегда делай JOIN с `drugs` или `diagnoses`, если нужно название.
5. ВЫВОД: Только SQL код. Без Markdown.
"""

class OpenRouterSQLAgent:
    def __init__(self, api_key: str):
        self.llm = ChatOpenAI(
            model="meta-llama/llama-3.3-70b-instruct", 
            openai_api_key=api_key,
            openai_api_base="https://openrouter.ai/api/v1",
            temperature=0.1, # Чуть выше 0, чтобы он мог выбирать варианты
            default_headers={
                "HTTP-Referer": "https://medinsight.com",
                "X-Title": "Medical Agent"
            }
        )

    def _clean_sql(self, text: str) -> str:
        text = text.strip()
        # Иногда модель пишет пояснения до или после кода, вырезаем код
        match = re.search(r'```sql(.*?)```', text, re.DOTALL)
        if match:
            return match.group(1).strip()
        
        # Если кавычек нет, просто чистим
        text = re.sub(r'^```sql', '', text, flags=re.IGNORECASE)
        text = re.sub(r'^```', '', text)
        text = re.sub(r'```$', '', text)
        return text.strip()

    def _generate_sql(self, question: str) -> str:
        """
        Логика 'Лучший запрос': 
        Мы просим модель подумать о 2-3 вариантах внутри (Chain of Thought), 
        но выдать ТОЛЬКО финальный лучший код.
        """
        system_prompt = f"""{DB_SCHEMA}
        
        ТВОЯ ЗАДАЧА: Написать ЛУЧШИЙ SQL запрос для ответа на вопрос.
        
        АЛГОРИТМ МЫШЛЕНИЯ (Chain of Thought):
        1. Сначала подумай, можно ли взять данные из таблиц `insight_...`? Это самый лучший вариант.
        2. Если нет, подумай, как собрать данные из `prescriptions` через JOIN.
        3. Сравни варианты. Выбери тот, который точнее и быстрее.
        4. Если вопрос про динамику — не ставь LIMIT. Если про топ — ставь LIMIT 5.
        
        ВЫВОД:
        Верни ТОЛЬКО финальный SQL код. Никаких рассуждений, никаких "Here is the code". Только SQL.
        """
        
        prompt = ChatPromptTemplate.from_messages([
            ("system", system_prompt),
            ("human", question)
        ])
        chain = prompt | self.llm
        response = chain.invoke({})
        return self._clean_sql(response.content)

    def _execute_sql(self, sql_query: str) -> pd.DataFrame:
        if not os.path.exists(SCRIPTS_DIR):
            os.makedirs(SCRIPTS_DIR, exist_ok=True)
            
        with open(REQUEST_FILE, "w", encoding="utf-8") as f:
            f.write(sql_query)
        
        try:
            # ИСПОЛЬЗУЕМ sys.executable для фикса ошибки "SQL Error: Python"
            result = subprocess.run(
                [sys.executable, RUNNER_SCRIPT, "request.sql"],
                cwd=SCRIPTS_DIR,
                capture_output=True,
                text=True
            )
            
            if result.returncode != 0:
                print(f"SQL Stderr: {result.stderr}")
                return None
            
            if os.path.exists(ANSWER_FILE) and os.path.getsize(ANSWER_FILE) > 0:
                return pd.read_csv(ANSWER_FILE)
            else:
                return pd.DataFrame()
        except Exception as e:
            print(f"Subprocess Error: {e}")
            return None

    def _analyze_data(self, question: str, df: pd.DataFrame, sql: str) -> str:
        """
        Анализ данных с защитой от галлюцинаций.
        """
        if df is None:
            return "⚠️ Произошла ошибка при выполнении запроса к базе данных. Попробуйте переформулировать."
        if df.empty:
            return "По вашему запросу данных не найдено. Возможно, стоит упростить вопрос."
            
        # ЗАЩИТА ОТ ЗАВИСАНИЯ:
        # Передаем только первые 40 строк. Если строк больше, модель должна сделать вывод по топу.
        # Это предотвращает бесконечную генерацию текста.
        df_preview = df.head(40)
        data_md = df_preview.to_markdown(index=False)
        
        row_count = len(df)
        limit_warning = f"(Показаны первые 40 строк из {row_count})" if row_count > 40 else ""
        
        system_prompt = """Ты — профессиональный медицинский аналитик.
        Твоя задача — дать краткий и четкий ответ на вопрос пользователя, основываясь ТОЛЬКО на предоставленных данных.
        
        СТРОГИЕ ЗАПРЕТЫ (ЧТОБЫ НЕ БЫЛО ОШИБОК):
        1. ЗАПРЕЩЕНО описывать структуру таблицы ("Таблица содержит столбцы...").
        2. ЗАПРЕЩЕНО перечислять типы данных ("VARCHAR", "FLOAT").
        3. ЗАПРЕЩЕНО выводить саму таблицу в ответе, если пользователь не просил список.
        4. ЗАПРЕЩЕНО писать "Пример данных из таблицы drugs...".
        
        ЧТО НУЖНО ДЕЛАТЬ:
        1. Сразу отвечай на вопрос. (Пример: "Больше всего болеют в Невском районе...").
        2. Приводи конкретные цифры из таблицы.
        3. Если видишь явный тренд — скажи о нем.
        """
        
        prompt = ChatPromptTemplate.from_messages([
            ("system", system_prompt),
            ("human", f"Вопрос пользователя: {question}\n\nДанные SQL ({limit_warning}):\n{data_md}")
        ])
        
        chain = prompt | self.llm
        response = chain.invoke({})
        return response.content

    def answer(self, user_question: str):
        # 1. Генерация (с внутренней проверкой лучшего варианта)
        sql = self._generate_sql(user_question)
        print(f"DEBUG SQL: {sql}")
        
        # 2. Выполнение
        df = self._execute_sql(sql)
        
        # 3. Анализ (на обрезанных данных)
        return self._analyze_data(user_question, df, sql)