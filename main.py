import streamlit as st
import pandas as pd
import json
import plotly.express as px
import os
from dotenv import load_dotenv
from agent import get_agent_executor

load_dotenv()

# --- Настройки страницы (обязательно первой строчкой) ---
st.set_page_config(layout="wide", page_title="Medical Insight", page_icon="🏥")

# --- Загрузка данных (с кэшированием, чтобы летало) ---
@st.cache_data
def load_stats():
    # Проверяем, существуют ли файлы
    if not os.path.exists('data/stats_by_district.json'):
        return None, None

    with open('data/stats_by_district.json', 'r', encoding='utf-8') as f:
        district_data = json.load(f)
    
    with open('data/stats_by_season.json', 'r', encoding='utf-8') as f:
        season_data = json.load(f)
        
    return district_data, season_data

# --- Преобразование JSON в DataFrame для графиков ---
def prepare_dfs(district_data, season_data):
    # 1. Районы
    dist_rows = []
    for dist, diseases in district_data.items():
        for disease, count in diseases.items():
            dist_rows.append({'Район': dist, 'Заболевание': disease, 'Случаев': count})
    df_dist = pd.DataFrame(dist_rows)
    
    # 2. Сезоны
    seas_rows = []
    for season, diseases in season_data.items():
        for disease, count in diseases.items():
            seas_rows.append({'Сезон': season, 'Заболевание': disease, 'Случаев': count})
    df_seas = pd.DataFrame(seas_rows)
    
    # Сортировка сезонов (чтобы шли логично, а не по алфавиту)
    season_order = {'Зима': 1, 'Весна': 2, 'Лето': 3, 'Осень': 4}
    if not df_seas.empty:
        df_seas['order'] = df_seas['Сезон'].map(season_order)
        df_seas = df_seas.sort_values('order')
    
    return df_dist, df_seas

# --- Интерфейс ---
st.title("Medical Insight: Аналитика Санкт-Петербурга")

# Загружаем данные
district_json, season_json = load_stats()

if district_json is None:
    st.error("⚠️ Данные не найдены! Запустите сначала `python src/data_processor.py`")
    st.stop()

df_dist, df_seas = prepare_dfs(district_json, season_json)

# Вкладки
tab_dashboard, tab_agent = st.tabs(["📊 Аналитический Дашборд", "🤖 AI Агент"])

# === ВКЛАДКА 1: ДАШБОРД ===
with tab_dashboard:
    st.markdown("### Географический анализ заболеваемости")
    
    # Метрики (KPI)
    col1, col2, col3 = st.columns(3)
    total_cases = df_dist['Случаев'].sum()
    top_district = df_dist.groupby('Район')['Случаев'].sum().idxmax()
    top_disease = df_dist.groupby('Заболевание')['Случаев'].sum().idxmax()
    
    col1.metric("Всего обращений (в выборке)", f"{total_cases:,}")
    col2.metric("Самый 'больной' район", top_district)
    col3.metric("Самая частая болезнь", top_disease)
    
    st.divider()

    # График 1: Районы (Заменяет карту)
    col_chart1, col_chart2 = st.columns([2, 1])
    
    with col_chart1:
        st.subheader("Распределение по районам")
        fig_dist = px.bar(
            df_dist, 
            x="Район", 
            y="Случаев", 
            color="Заболевание", 
            title="Структура заболеваний по районам",
            hover_data=["Случаев"],
            template="plotly_white"
        )
        st.plotly_chart(fig_dist, use_container_width=True)
        
    with col_chart2:
        st.subheader("Детализация района")
        selected_dist = st.selectbox("Выберите район:", df_dist['Район'].unique())
        
        filtered_df = df_dist[df_dist['Район'] == selected_dist]
        fig_pie = px.pie(
            filtered_df, 
            values='Случаев', 
            names='Заболевание', 
            hole=0.4,
            title=f"Болезни: {selected_dist}"
        )
        st.plotly_chart(fig_pie, use_container_width=True)

    st.divider()
    
    # График 2: Сезонность
    st.markdown("### 🍂 Сезонные тренды")
    if not df_seas.empty:
        fig_season = px.line(
            df_seas, 
            x="Сезон", 
            y="Случаев", 
            color="Заболевание", 
            markers=True,
            title="Динамика заболеваний по сезонам года"
        )
        st.plotly_chart(fig_season, use_container_width=True)

# === ВКЛАДКА 2: АГЕНТ (Заглушка для следующего шага) ===
with tab_agent:
    st.header("Чат с медицинским ассистентом")
    api_key = os.getenv("GOOGLE_API_KEY")
    
    if not api_key:
        print("Нет ключа API в .env для модели")
        st.warning("Для работы агента нужен API Key.")
        st.stop()

    # Инициализация истории чата
    if "messages" not in st.session_state:
        st.session_state.messages = [
            {"role": "assistant", "content": "Привет! Я проанализировал медицинские данные Петербурга. Спросите меня, например: 'В каком районе больше всего болеют?' или 'Какие болезни популярны зимой?'"}
        ]

    # Отображение истории
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    # Поле ввода
    if prompt := st.chat_input("Ваш вопрос..."):
        # 1. Показываем вопрос пользователя
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        # 2. Получаем ответ от агента
        with st.chat_message("assistant"):
            try:
                # Создаем агента на лету (это быстро)
                agent_executor = get_agent_executor(api_key)
                
                # Запускаем мыслительный процесс
                with st.spinner("Анализирую данные..."):
                    # invoke запускает всю магию: LLM решит, какую функцию вызвать
                    response = agent_executor.invoke({"input": prompt})
                    answer = response["output"]
                
                st.markdown(answer)
                st.session_state.messages.append({"role": "assistant", "content": answer})
                
            except Exception as e:
                st.error(f"Произошла ошибка: {e}")