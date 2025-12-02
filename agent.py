import os
import subprocess
import pandas as pd
import re
from langchain_google_genai import ChatGoogleGenerativeAI
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

# --- СХЕМА БАЗЫ ДАННЫХ ---
DB_SCHEMA = """
Ты работаешь с базой данных DuckDB 'medinsight.duckdb'.
В базе 4 таблицы. Схема типа "Звезда".

1. patients (пациенты):
   - id_пациента (VARCHAR) [PK]
   - дата_рождения (DATE)
   - пол (VARCHAR)
   - район_проживания (VARCHAR)
   - регион (VARCHAR)

2. prescriptions (рецепты/обращения):
   - id_пациента (VARCHAR) [FK -> patients.id_пациента]
   - дата_рецепта (TIMESTAMP)
   - код_диагноза (VARCHAR) [FK -> diagnoses.код_мкб]
   - код_препарата (VARCHAR) [FK -> drugs.код_препарата]

3. diagnoses (справочник):
   - код_мкб (VARCHAR) [PK]
   - название_диагноза (VARCHAR)
   - класс_заболевания (VARCHAR)

4. drugs (справочник):
   - код_препарата (VARCHAR) [PK]
   - Торговое название (VARCHAR)
   - стоимость (DOUBLE)

ВАЖНО:
- Используй JOIN для связей.
- Для поиска текста используй ILIKE '%текст%'.
- Генерируй ТОЛЬКО SQL код. Без Markdown. Без ```sql.
"""

class MedicalSQLAgent:
    def __init__(self, api_key: str):
        # Используем 1.5-flash для скорости и стабильности SQL
        self.llm = ChatGoogleGenerativeAI(
            model="gemini-2.5-flash", 
            google_api_key=api_key, 
            temperature=0
        )

    def _clean_sql(self, text: str) -> str:
        """Очищает ответ LLM от маркдауна"""
        text = text.strip()
        text = re.sub(r'^```sql', '', text, flags=re.IGNORECASE)
        text = re.sub(r'^```', '', text)
        text = re.sub(r'```$', '', text)
        return text.strip()

    def _generate_sql(self, question: str) -> str:
        """Шаг 1: Превращаем вопрос в SQL"""
        prompt = ChatPromptTemplate.from_messages([
            ("system", f"{DB_SCHEMA}\nНапиши SQL запрос DuckDB для ответа на вопрос. LIMIT 50."),
            ("human", question)
        ])
        chain = prompt | self.llm
        response = chain.invoke({})
        return self._clean_sql(response.content)

    def _execute_sql(self, sql_query: str) -> pd.DataFrame:
        """Шаг 2: Запускаем скрипт run_sql_safe.py"""
        
        # 1. Сохраняем запрос в файл
        if not os.path.exists(SCRIPTS_DIR):
            os.makedirs(SCRIPTS_DIR, exist_ok=True)
            
        with open(REQUEST_FILE, "w", encoding="utf-8") as f:
            f.write(sql_query)
            
        # 2. Запускаем внешний python скрипт
        # Мы запускаем его из текущей директории, но указываем путь к скрипту
        runner_path = os.path.join(SCRIPTS_DIR, RUNNER_SCRIPT)
        
        try:
            # Запускаем скрипт и ждем выполнения
            # cwd=SCRIPTS_DIR важно, чтобы скрипт видел базу данных рядом с собой
            result = subprocess.run(
                ["python3", RUNNER_SCRIPT, "request.sql"],
                cwd=SCRIPTS_DIR,
                capture_output=True,
                text=True
            )
            
            if result.returncode != 0:
                print(f"SQL Error Log: {result.stderr}")
                return None
                
            # 3. Читаем результат
            if os.path.exists(ANSWER_FILE) and os.path.getsize(ANSWER_FILE) > 0:
                return pd.read_csv(ANSWER_FILE)
            else:
                return pd.DataFrame() # Пустой результат
                
        except Exception as e:
            print(f"Execution Error: {e}")
            return None

    def _analyze_data(self, question: str, df: pd.DataFrame, sql: str) -> str:
        """Шаг 3: Интерпретируем CSV для пользователя"""
        if df is None:
            return "❌ Ошибка при выполнении SQL запроса. Проверьте консоль."
        if df.empty:
            return "По вашему запросу данных не найдено."
            
        data_md = df.to_markdown(index=False)
        
        prompt = ChatPromptTemplate.from_messages([
            ("system", "Ты — медицинский аналитик. Ответь на вопрос пользователя, используя данные из таблицы."),
            ("human", f"Вопрос: {question}\n\nИспользованный SQL: {sql}\n\nПолученные данные:\n{data_md}")
        ])
        
        chain = prompt | self.llm
        response = chain.invoke({})
        return response.content

    def answer(self, user_question: str):
        """Главный метод"""
        # 1. Генерация
        sql = self._generate_sql(user_question)
        print(f"Generated SQL: {sql}") # Лог в консоль
        
        # 2. Выполнение
        df = self._execute_sql(sql)
        
        # 3. Ответ
        final_answer = self._analyze_data(user_question, df, sql)
        
        return final_answer