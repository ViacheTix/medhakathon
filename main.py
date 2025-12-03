
import os
from dotenv import load_dotenv
import pandas as pd
import streamlit as st
import plotly.express as px
import duckdb

from agent import OpenRouterSQLAgent # Новый (OpenRouter)

load_dotenv()

# --- Настройки страницы ---
DB_PATH = "db/medinsight.duckdb" # Проверь путь! Если запускаешь из корня, должно быть так.
st.set_page_config(layout="wide", page_title="Medical Insight", page_icon="🏥")

# --- ФУНКЦИИ ЗАГРУЗКИ ДАННЫХ (ПРЯМО ИЗ DUCKDB) ---
@st.cache_data

# --- ФУНКЦИИ ЗАГРУЗКИ ДАННЫХ (ПРЯМО ИЗ DUCKDB) ---
@st.cache_data
def load_dashboard_data():
    """
    Подключается к DuckDB и забирает данные для графиков.
    Использует кэширование Streamlit, чтобы не нагружать базу при каждом клике.
    """
    if not os.path.exists(DB_PATH):
        return None, None, None, None

    # Подключаемся в режиме read_only, чтобы не блокировать файл
    con = duckdb.connect(DB_PATH, read_only=True)
    
    # 1. Демография (Пол + Группы болезней)
    # Берем топ-10 групп болезней по количеству пациентов
    df_demography = con.execute("""
        SELECT disease_group, age_group, male_patients, female_patients, total_patients
        FROM insight_gender_disease
        ORDER BY total_patients DESC
        LIMIT 15
    """).df()

    # 2. Финансы (Стоимость лечения)
    df_finance = con.execute("""
        SELECT disease_group, avg_cost_per_prescription, avg_cost_per_patient
        FROM insight_cost_by_disease
        ORDER BY avg_cost_per_patient DESC
        LIMIT 10
    """).df()

    # 3. География (Регионы и популярность)
    df_geo = con.execute("""
        SELECT region, SUM(prescriptions_count) as total_prescriptions
        FROM insight_region_drug_choice
        GROUP BY region
        ORDER BY total_prescriptions DESC
    """).df()
    
    # 4. Сезонность (Это тяжелый запрос к сырой таблице, но DuckDB справится быстро)
    # Агрегируем по месяцам за все время
    df_season = con.execute("""
        SELECT 
            strftime(дата_рецепта, '%Y-%m') as month_year,
            COUNT(*) as cases
        FROM prescriptions
        GROUP BY month_year
        ORDER BY month_year
    """).df()

    con.close()
    return df_demography, df_finance, df_geo, df_season

# --- ИНТЕРФЕЙС ---

st.title("🏥 Medical Insight: Центр Аналитики")
st.markdown("Интерактивная панель управления медицинскими данными Санкт-Петербурга.")

# Загрузка данных
df_demo, df_finance, df_geo, df_season = load_dashboard_data()

if df_demo is None:
    st.error(f"❌ База данных не найдена по пути: `{DB_PATH}`. Запустите `python scripts_db/01_setup_db.py`")
    st.stop()

# ВКЛАДКИ
tab_dashboard, tab_agent = st.tabs(["📊 Аналитический Дашборд", "🤖 AI Агент"])

# === ВКЛАДКА 1: ВИЗУАЛИЗАЦИЯ ===
with tab_dashboard:
    
    # KPI (Метрики сверху)
    col1, col2, col3, col4 = st.columns(4)
    total_patients_kpi = df_demo['total_patients'].sum()
    avg_check_kpi = df_finance['avg_cost_per_prescription'].mean()
    top_region_kpi = df_geo.iloc[0]['region']
    
    col1.metric("Пациентов в выборке", f"{total_patients_kpi:,.0f}")
    col2.metric("Ср. чек рецепта", f"{avg_check_kpi:.1f} ₽")
    col3.metric("Самый активный район", top_region_kpi)
    col4.metric("Всего категорий болезней", len(df_demo))
    
    st.divider()

    # РЯД 1: ДЕМОГРАФИЯ И ГЕОГРАФИЯ
    c1, c2 = st.columns([1, 1])
    
    with c1:
        st.subheader("👥 Структура пациентов (М vs Ж)")
        # Преобразуем данные для красивого графика
        # Нам нужно "расплавить" (melt) таблицу, чтобы Seaborn/Plotly поняли формат
        df_melted = df_demo.melt(
            id_vars=["disease_group"], 
            value_vars=["male_patients", "female_patients"], 
            var_name="Пол", 
            value_name="Пациенты"
        )
        
        fig_demo = px.bar(
            df_melted, 
            x="Пациенты", 
            y="disease_group", 
            color="Пол", 
            orientation='h',
            title="Кого больше по группам болезней?",
            color_discrete_map={"male_patients": "#636EFA", "female_patients": "#EF553B"},
            barmode='group' # Или 'relative' для стека
        )
        st.plotly_chart(fig_demo, use_container_width=True)

    with c2:
        st.subheader("🌍 Загруженность районов")
        fig_geo = px.bar(
            df_geo,
            x="region",
            y="total_prescriptions",
            color="total_prescriptions",
            title="Количество рецептов по районам",
            color_continuous_scale="Viridis"
        )
        st.plotly_chart(fig_geo, use_container_width=True)

    # РЯД 2: ФИНАНСЫ И СЕЗОННОСТЬ
    c3, c4 = st.columns([1, 1])
    
    with c3:
        st.subheader("💰 Самые 'дорогие' болезни")
        fig_fin = px.scatter(
            df_finance,
            x="avg_cost_per_prescription",
            y="avg_cost_per_patient",
            size="avg_cost_per_patient",
            color="disease_group",
            title="Стоимость рецепта vs Стоимость лечения пациента",
            hover_name="disease_group"
        )
        st.plotly_chart(fig_fin, use_container_width=True)
        
    with c4:
        st.subheader("📅 Динамика обращений")
        fig_season = px.area(
            df_season,
            x="month_year",
            y="cases",
            title="Тренд выдачи рецептов по месяцам",
            markers=True
        )
        st.plotly_chart(fig_season, use_container_width=True)

# === ВКЛАДКА 2: АГЕНТ (ОБНОВЛЕННАЯ ЛОГИКА) ===
with tab_agent:
    st.header("Чат с SQL-агентом (Powered by Llama 3.3)")
    
    # 1. Проверка ключа OpenRouter
    api_key = os.getenv("OPENROUTER_API_KEY")
    
    if not api_key:
        st.warning("⚠️ Ключ OPENROUTER_API_KEY не найден в .env файле.")
        api_key = st.text_input("Введите ключ OpenRouter вручную:", type="password")
        
    if not api_key:
        st.stop()

    # 2. История сообщений
    if "messages" not in st.session_state:
        st.session_state.messages = [
            {"role": "assistant", "content": "Я подключен к базе данных через OpenRouter. Могу анализировать сложные запросы. О чем вам рассказать?"}
        ]

    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    # 3. Обработка вопроса
    if prompt := st.chat_input("Ваш вопрос к базе данных..."):
        # Сохраняем вопрос пользователя
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        # Генерируем ответ
        with st.chat_message("assistant"):
            try:
                # Инициализируем агента OpenRouter
                agent = OpenRouterSQLAgent(api_key)
                
                with st.spinner("🤖 Llama 3.3 думает и пишет SQL..."):
                    final_response = agent.answer(prompt)
                
                st.markdown(final_response)
                st.session_state.messages.append({"role": "assistant", "content": final_response})
                
            except Exception as e:
                st.error(f"Произошла ошибка: {e}")