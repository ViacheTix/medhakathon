import streamlit as st
import pandas as pd
import json
import plotly.express as px
import os
from dotenv import load_dotenv

# --- импорт класса LLM ---
from agent import MedicalSQLAgent 

load_dotenv()

# --- Настройки страницы ---
st.set_page_config(layout="wide", page_title="Medical Insight", page_icon="🏥")

# --- Загрузка данных для ДАШБОРДА (Оставляем как есть, это для графиков) ---
@st.cache_data
def load_stats():
    # Проверяем пути - у тебя в коде было output/, я оставил как у тебя
    if not os.path.exists('output/stats_by_district.json'):
        return None, None

    with open('output/stats_by_district.json', 'r', encoding='utf-8') as f:
        district_data = json.load(f)
    
    with open('output/stats_by_season.json', 'r', encoding='utf-8') as f:
        season_data = json.load(f)
        
    return district_data, season_data

def prepare_dfs(district_data, season_data):
    # (Код без изменений - копируй свою функцию prepare_dfs сюда)
    dist_rows = []
    for dist, diseases in district_data.items():
        for disease, count in diseases.items():
            dist_rows.append({'Район': dist, 'Заболевание': disease, 'Случаев': count})
    df_dist = pd.DataFrame(dist_rows)
    
    seas_rows = []
    for season, diseases in season_data.items():
        for disease, count in diseases.items():
            seas_rows.append({'Сезон': season, 'Заболевание': disease, 'Случаев': count})
    df_seas = pd.DataFrame(seas_rows)
    
    season_order = {'Зима': 1, 'Весна': 2, 'Лето': 3, 'Осень': 4}
    if not df_seas.empty:
        df_seas['order'] = df_seas['Сезон'].map(season_order)
        df_seas = df_seas.sort_values('order')
    return df_dist, df_seas

# --- Интерфейс ---
st.title("Medical Insight: Аналитика Санкт-Петербурга")

district_json, season_json = load_stats()

# Логика для дашборда
if district_json:
    df_dist, df_seas = prepare_dfs(district_json, season_json)
else:
    # Заглушка, чтобы не падало, если json пока нет
    df_dist, df_seas = pd.DataFrame(), pd.DataFrame()

tab_dashboard, tab_agent = st.tabs(["📊 Аналитический Дашборд", "🤖 AI Агент"])

# === ВКЛАДКА 1: ДАШБОРД (Твой код без изменений) ===
with tab_dashboard:
    if df_dist.empty:
        st.warning("⚠️ Файлы JSON для дашборда не найдены в папке output/. Запустите предобработку.")
    else:
        st.markdown("### Географический анализ заболеваемости")
        col1, col2, col3 = st.columns(3)
        total_cases = df_dist['Случаев'].sum()
        top_district = df_dist.groupby('Район')['Случаев'].sum().idxmax()
        top_disease = df_dist.groupby('Заболевание')['Случаев'].sum().idxmax()
        
        col1.metric("Всего обращений", f"{total_cases:,}")
        col2.metric("Самый 'больной' район", top_district)
        col3.metric("Самая частая болезнь", top_disease)
        
        st.divider()

        col_chart1, col_chart2 = st.columns([2, 1])
        with col_chart1:
            st.subheader("Распределение по районам")
            fig_dist = px.bar(df_dist, x="Район", y="Случаев", color="Заболевание")
            st.plotly_chart(fig_dist, use_container_width=True)
            
        with col_chart2:
            st.subheader("Детализация района")
            selected_dist = st.selectbox("Выберите район:", df_dist['Район'].unique())
            filtered_df = df_dist[df_dist['Район'] == selected_dist]
            fig_pie = px.pie(filtered_df, values='Случаев', names='Заболевание')
            st.plotly_chart(fig_pie, use_container_width=True)

# === ВКЛАДКА 2: АГЕНТ (ОБНОВЛЕННАЯ ЛОГИКА) ===
with tab_agent:
    st.header("Чат с SQL-агентом")
    
    # 1. Получаем ключ
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        api_key = st.text_input("Введите API Key", type="password")
        if not api_key:
            st.stop()

    # 2. История сообщений
    if "messages" not in st.session_state:
        st.session_state.messages = [
            {"role": "assistant", "content": "Я подключен к базе данных DuckDB. Задавайте сложные вопросы, например: 'Сколько женщин заболело ОРВИ в Центральном районе?'"}
        ]

    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    # 3. Обработка вопроса
    if prompt := st.chat_input("Ваш вопрос к базе данных..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            # --- ВОТ ТУТ ГЛАВНОЕ ИЗМЕНЕНИЕ ---
            try:
                # Инициализируем наш новый класс
                sql_agent = MedicalSQLAgent(api_key)
                
                with st.spinner("🤖 Пишу SQL запрос и опрашиваю базу данных..."):
                    # Вызываем метод answer(), который делает всю магию
                    final_response = sql_agent.answer(prompt)
                
                st.markdown(final_response)
                st.session_state.messages.append({"role": "assistant", "content": final_response})
                
            except Exception as e:
                st.error(f"Произошла ошибка: {e}")