# agent.py
import os
import duckdb
import re
from pathlib import Path
from langchain_community.llms import Ollama  # для CodeLlama
from langchain_google_genai import ChatGoogleGenerativeAI

def generate_sql_with_codellama(question: str) -> str:
    """Генерирует SQL-запрос с помощью CodeLlama."""
    # Инициализация CodeLlama (локально через Ollama)
    codellama = Ollama(model="codellama:7b", temperature=0)
    
    prompt = f"""
Ты — эксперт по SQL. Напиши запрос к базе данных, чтобы ответить на вопрос: "{question}"

Схема базы:
- patients(id_пациента VARCHAR, дата_рождения DATE, пол VARCHAR, район_проживания VARCHAR, регион VARCHAR)
- prescriptions(id_пациента VARCHAR, дата_рецепта TIMESTAMP, код_диагноза VARCHAR, код_препарата VARCHAR)
- diagnoses(код_мкб VARCHAR, название_диагноза VARCHAR, класс_заболевания VARCHAR)

Правила:
- Используй только SELECT
- Связывай таблицы через prescriptions.id_пациента = patients.id_пациента
- Для ОРВИ: код_диагноза ILIKE 'J0[0-6]%'
- Всегда фильтруй по региону, если вопрос про СПб: patients.регион = 'Санкт-Петербург'
- Не используй CREATE, DROP, INSERT
- Верни ТОЛЬКО SQL-запрос, без пояснений

Пример вопроса: Сколько случаев ОРВИ в октябре 2024?
Пример ответа: SELECT COUNT(*) FROM prescriptions p JOIN patients pa ON p.id_пациента = pa.id_пациента WHERE pa.регион = 'Санкт-Петербург' AND p.код_диагноза ILIKE 'J0[0-6]%' AND strftime('%Y-%m', p.дата_рецепта) = '2024-10';
"""
    sql = codellama.invoke(prompt)
    # Очистка: оставляем только SQL
    sql = sql.strip().split("\n")[0]  # берём первую строку
    return sql

def execute_sql_safe(sql: str) -> str:
    """Безопасное выполнение SQL и возврат данных в виде текста."""
    # Проверка на SELECT
    if not sql.strip().lower().startswith("select"):
        return "Разрешены только SELECT-запросы."
    
    # Запрет опасных слов
    if re.search(r'\b(create|drop|insert|update|delete|alter)\b', sql, re.IGNORECASE):
        return "Запрещённые команды в запросе."
    
    try:
        con = duckdb.connect("db/medinsight.duckdb", read_only=True)
        if "limit" not in sql.lower():
            sql += " LIMIT 50"
        df = con.execute(sql).fetchdf()
        con.close()
        
        if df.empty:
            return "Нет данных."
        
        # Преобразуем в CSV-подобный текст (для Gemini)
        return df.to_csv(index=False)
    
    except Exception as e:
        return f"Ошибка: {str(e)}"

def generate_answer_with_gemini(question: str, sql: str,  str) -> str:
    """Gemini формулирует ответ на основе вопроса, SQL и данных."""
    gemini = ChatGoogleGenerativeAI(
        model="gemini-2.5-flash",
        google_api_key=os.getenv("GOOGLE_API_KEY"),
        temperature=0
    )
    
    prompt = f"""
Вопрос пользователя: {question}

Сгенерированный SQL-запрос: {sql}

Результат выполнения (CSV):
{data}

Сформулируй краткий, понятный и точный ответ на русском языке.
Если данных нет — скажи: "Нет данных по вашему запросу".
Не упоминай SQL и технические детали.
"""
    return gemini.invoke(prompt).content




# streamlit 

with tab_agent:
    st.header("Чат с медицинским ассистентом")
    
    # Проверка API-ключей
    google_api_key = os.getenv("GOOGLE_API_KEY")
    if not google_api_key:
        st.error("GOOGLE_API_KEY не найден в .env")
        st.stop()
    
    # История чата
    if "messages" not in st.session_state:
        st.session_state.messages = [
            {"role": "assistant", "content": "Привет! Спросите, например: «Сколько ОРВИ в октябре?» или «Где больше болеют?»"}
        ]

    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    if prompt := st.chat_input("Ваш вопрос..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            try:
                with st.spinner("Генерирую SQL..."):
                    sql = generate_sql_with_codellama(prompt)
                
                with st.spinner("Выполняю запрос..."):
                    data = execute_sql_safe(sql)
                
                with st.spinner("Формулирую ответ..."):
                    answer = generate_answer_with_gemini(prompt, sql, data)
                
                st.markdown(answer)
                st.session_state.messages.append({"role": "assistant", "content": answer})
                
                # Опционально: показать SQL для прозрачности
                with st.expander("Сгенерированный SQL"):
                    st.code(sql, language="sql")
                    
            except Exception as e:
                st.error(f"Ошибка: {e}")

https://chat.qwen.ai/s/01c3d016-232d-4aaf-917b-70ca1e4fb3ae?fev=0.1.6