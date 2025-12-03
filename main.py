import streamlit as st
import pandas as pd
import plotly.express as px
import duckdb
import os
from dotenv import load_dotenv

# Импортируем твоего агента
from agent import MedicalSQLAgent # Новый (OpenRouter)

load_dotenv()

# --- КОНФИГУРАЦИЯ ---
DB_PATH = "db/medinsight.duckdb"
st.set_page_config(layout="wide", page_title="Medical Insight", page_icon="🏥")

# --- ФУНКЦИИ ЗАГРУЗКИ ДАННЫХ ---
@st.cache_data
def load_dashboard_data():
    if not os.path.exists(DB_PATH):
        return None, None, None, None, None, None

    con = duckdb.connect(DB_PATH, read_only=True)
    
    # 1. Демография (Пол) - Прямо из таблицы пациентов
    df_gender = con.execute("""
        SELECT пол, COUNT(*) as count 
        FROM patients 
        GROUP BY пол
    """).df()

    # 2. Демография (Возраст) - Вычисляем возраст на лету
    # date_diff('year', start, end) работает очень быстро в DuckDB
    df_age = con.execute("""
        SELECT 
            date_diff('year', дата_рождения, CURRENT_DATE) as age
        FROM patients
        WHERE дата_рождения IS NOT NULL
    """).df()

    # 3. География ПАЦИЕНТОВ (Где они живут)
    df_district_patients = con.execute("""
        SELECT район_проживания, COUNT(*) as count
        FROM patients
        WHERE район_проживания IS NOT NULL
        GROUP BY район_проживания
        ORDER BY count DESC
    """).df()

    # 4. Финансы (Стоимость лечения) - из витрин
    df_finance = con.execute("""
        SELECT disease_group, avg_cost_per_prescription, avg_cost_per_patient
        FROM insight_cost_by_disease
        ORDER BY avg_cost_per_patient DESC
        LIMIT 10
    """).df()

    # 5. География ЛЕКАРСТВ (Где больше выписывают) - из витрин
    df_geo_drugs = con.execute("""
        SELECT region, SUM(prescriptions_count) as total_prescriptions
        FROM insight_region_drug_choice
        GROUP BY region
        ORDER BY total_prescriptions DESC
    """).df()
    
    # 6. Сезонность
    df_season = con.execute("""
        SELECT 
            strftime(дата_рецепта, '%Y-%m') as month_year,
            COUNT(*) as cases
        FROM prescriptions
        GROUP BY month_year
        ORDER BY month_year
    """).df()

    con.close()
    return df_gender, df_age, df_district_patients, df_finance, df_geo_drugs, df_season

# --- ИНТЕРФЕЙС ---

st.title("🏥 Medical Insight: Центр Аналитики")
st.markdown("Интерактивная панель управления медицинскими данными Санкт-Петербурга.")

# Загрузка
data = load_dashboard_data()
df_gender, df_age, df_district_patients, df_finance, df_geo_drugs, df_season = data

if df_gender is None:
    st.error(f"❌ База данных не найдена по пути: {DB_PATH}.")
    st.stop()

# ВКЛАДКИ
tab_dashboard, tab_agent = st.tabs(["📊 Аналитический Дашборд", "🤖 AI Агент"])
# === ВКЛАДКА 1: ВИЗУАЛИЗАЦИЯ ===
with tab_dashboard:
    
    # KPI
    col1, col2, col3, col4 = st.columns(4)
    total_patients_kpi = df_gender['count'].sum()
    avg_age_kpi = df_age['age'].mean()
    top_district_kpi = df_district_patients.iloc[0]['район_проживания']
    
    col1.metric("Всего пациентов", f"{total_patients_kpi:,.0f}")
    col2.metric("Средний возраст", f"{avg_age_kpi:.1f} лет")
    col3.metric("Самый населенный район", top_district_kpi)
    col4.metric("Всего рецептов", f"{df_season['cases'].sum():,.0f}")
    
    st.divider()

    # БЛОК 1: ПОРТРЕТ ПАЦИЕНТА (Пол + Возраст)
    st.subheader("👤 Портрет пациента")
    c1, c2 = st.columns([1, 2]) # Левая колонка уже, правая шире
    
    with c1:
        # Круговая диаграмма пола
        fig_gender = px.pie(
            df_gender, 
            values='count', 
            names='пол',
            title='Распределение по полу',
            color_discrete_map={"М": "#636EFA", "Ж": "#EF553B"},
            hole=0.4
        )
        st.plotly_chart(fig_gender, use_container_width=True)
        
    with c2:
        # Гистограмма возраста
        fig_age = px.histogram(
            df_age, 
            x="age", 
            nbins=30, # Количество столбиков
            title="Возрастная структура (Гистограмма)",
            labels={'age': 'Возраст', 'count': 'Кол-во пациентов'},
            color_discrete_sequence=['#00CC96']
        )
        fig_age.update_layout(bargap=0.1) # Зазор между столбиками
        st.plotly_chart(fig_age, use_container_width=True)

    st.divider()

    # БЛОК 2: ГЕОГРАФИЯ ПАЦИЕНТОВ
    st.subheader("🏠 Где живут наши пациенты?")
    # Treemap - отлично подходит для показа долей районов
    fig_tree = px.treemap(
        df_district_patients,
        path=['район_проживания'],
        values='count',
        title='Распределение пациентов по районам (Площадь = Кол-во)',
        color='count',
        color_continuous_scale='Blues'
    )
    st.plotly_chart(fig_tree, use_container_width=True)

    st.divider()

    # БЛОК 3: ФИНАНСЫ И СЕЗОННОСТЬ (Оставляем как было, это важные инсайты)
    c3, c4 = st.columns([1, 1])
    
    with c3:
        st.subheader("💰 Экономика лечения")
        fig_fin = px.scatter(
            df_finance,
            x="avg_cost_per_prescription",
            y="avg_cost_per_patient",
            size="avg_cost_per_patient",
            color="disease_group",
            title="Стоимость рецепта vs Пациента",
            hover_name="disease_group"
        )
        st.plotly_chart(fig_fin, use_container_width=True)
        
    with c4:
        st.subheader("📅 Динамика обращений")
        fig_season = px.area(
            df_season,
            x="month_year",
            y="cases",
            title="Выдача рецептов по месяцам",
            markers=True
        )
        st.plotly_chart(fig_season, use_container_width=True)


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