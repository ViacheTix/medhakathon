import os
import uuid  # <--- НУЖНО ДЛЯ ID ЧАТОВ
from dotenv import load_dotenv
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu
import plotly.express as px
import duckdb

from agent import OpenRouterSQLAgent

load_dotenv()

# ==========================================
# 🎨 CSS СТИЛИЗАЦИЯ
# ==========================================
def local_css():
    st.markdown(
        """
        <style>
        /* 1. Глобальные настройки шрифта */
        html, body, [class*="css"]  {
            font-size: 14px; 
            font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif;
        }

        /* 2. Заголовки */
        h1 { font-size: 24px !important; }
        h2 { font-size: 20px !important; }
        h3 { font-size: 18px !important; }

        /* 3. Отступы главного контейнера */
        .block-container {
            padding-top: 2rem !important;
            padding-bottom: 2rem !important;
            padding-left: 3rem !important;
            padding-right: 3rem !important;
            max-width: 95% !important;
        }

        /* 4. Карточки метрик (KPI) */
        [data-testid="stMetric"] {
            background-color: #f9f9f9;
            border: 1px solid #e0e0e0;
            padding: 10px;
            border-radius: 5px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        }
        [data-testid="stMetricValue"] {
            font-size: 20px !important;
        }
        
        /* 5. Кнопки в чате */
        .stButton button {
            text_align: left !important;
        }
        </style>
        """,
        unsafe_allow_html=True
    )

# --- КОНФИГУРАЦИЯ ---
DB_PATH = "db/medinsight.duckdb"
st.set_page_config(layout="wide", page_title="Medical Insight", page_icon="🏥")
local_css()

# ==========================================
# 📊 ФУНКЦИЯ АВТО-ВИЗУАЛИЗАЦИИ
# ==========================================
def auto_visualize_data(df: pd.DataFrame):
    """
    Автоматически подбирает график для DataFrame из ответа SQL.
    """
    if df.empty or len(df.columns) < 2:
        return None

    # Типы колонок
    num_cols = df.select_dtypes(include=['number']).columns.tolist()
    cat_cols = df.select_dtypes(include=['object', 'string']).columns.tolist()
    # Ищем даты
    date_cols = [col for col in df.columns if any(x in col.lower() for x in ['date', 'time', 'year', 'month', 'day', 'дата', 'год', 'месяц', 'quarter'])]

    fig = None
    
    # 1. Временной ряд (Линия)
    if len(date_cols) > 0 and len(num_cols) > 0:
        x_col = date_cols[0]
        y_col = num_cols[0]
        df = df.sort_values(by=x_col)
        fig = px.line(df, x=x_col, y=y_col, markers=True, title=f"Динамика: {y_col}", template="plotly_white")

    # 2. Категории (Бар чарт / Pie)
    elif len(cat_cols) > 0 and len(num_cols) > 0:
        x_col = cat_cols[0]
        y_col = num_cols[0]
        # Если мало строк и это похоже на доли -> Pie
        if len(df) <= 6 and any(x in y_col.lower() for x in ['share', 'доля', 'процент']): 
            fig = px.pie(df, names=x_col, values=y_col, title=f"Распределение: {x_col}")
        else:
            fig = px.bar(df, x=x_col, y=y_col, title=f"{y_col} по {x_col}", color=y_col, template="plotly_white", color_continuous_scale="Blues")

    # 3. Числа (Scatter)
    elif len(num_cols) >= 2:
        fig = px.scatter(df, x=num_cols[0], y=num_cols[1], size=num_cols[1], title=f"Корреляция", template="plotly_white")

    return fig

# ==========================================
# 📂 УПРАВЛЕНИЕ ЧАТАМИ
# ==========================================
def init_chat_state():
    if "chats" not in st.session_state:
        new_id = str(uuid.uuid4())
        st.session_state.chats = {new_id: {"title": "Новый чат", "messages": []}}
        st.session_state.current_chat_id = new_id

def create_new_chat():
    new_id = str(uuid.uuid4())
    st.session_state.chats[new_id] = {"title": "Новый чат", "messages": []}
    st.session_state.current_chat_id = new_id

def delete_chat(chat_id):
    if len(st.session_state.chats) > 1:
        del st.session_state.chats[chat_id]
        st.session_state.current_chat_id = list(st.session_state.chats.keys())[0]

# --- ЗАГРУЗКА ДАННЫХ ДЛЯ ДАШБОРДА ---
@st.cache_data
def load_dashboard_data():
    if not os.path.exists(DB_PATH):
        return None, None, None, None, None, None
    con = duckdb.connect(DB_PATH, read_only=True)
    
    try:
        df_gender = con.execute("SELECT пол, COUNT(*) as count FROM patients GROUP BY пол").df()
        df_age = con.execute("SELECT date_diff('year', дата_рождения, CURRENT_DATE) as age FROM patients WHERE дата_рождения IS NOT NULL").df()
        df_district_patients = con.execute("SELECT район_проживания, COUNT(*) as count FROM patients WHERE район_проживания IS NOT NULL GROUP BY район_проживания ORDER BY count DESC").df()
        df_finance = con.execute("SELECT disease_group, avg_cost_per_prescription, avg_cost_per_patient FROM insight_cost_by_disease ORDER BY avg_cost_per_patient DESC LIMIT 10").df()
        df_geo_drugs = con.execute("SELECT region, SUM(prescriptions_count) as total_prescriptions FROM insight_region_drug_choice GROUP BY region ORDER BY total_prescriptions DESC").df()
        df_season = con.execute("SELECT strftime(дата_рецепта, '%Y-%m') as month_year, COUNT(*) as cases FROM prescriptions GROUP BY month_year ORDER BY month_year").df()
        return df_gender, df_age, df_district_patients, df_finance, df_geo_drugs, df_season
    except Exception as e:
        return None, None, None, None, None, None
    finally:
        con.close()

# --- ИНИЦИАЛИЗАЦИЯ ---
init_chat_state()

# МЕНЮ
with st.sidebar:
    selected = option_menu(
        menu_title="Меню", 
        options=["Дашборд", "AI Агент"], 
        icons=["bar-chart-fill", "chat-left-text-fill"], 
        menu_icon="cast", 
        default_index=0,
        styles={
            "container": {"padding": "5!important", "background-color": "#fafafa"},
            "icon": {"color": "orange", "font-size": "15px"}, 
            "nav-link": {"font-size": "16px", "text-align": "left", "margin":"0px", "--hover-color": "#eee"},
            "nav-link-selected": {"background-color": "#1f77b4"},
        }
    )

# ==========================================
# 📊 ВКЛАДКА 1: ДАШБОРД (ТВОЙ КОД)
# ==========================================
if selected == "Дашборд":
    st.title("📊 Аналитический Дашборд")
    
    data = load_dashboard_data()
    df_gender, df_age, df_district_patients, df_finance, df_geo_drugs, df_season = data

    if df_gender is None:
        st.error("❌ Не удалось загрузить данные. Проверьте путь к БД или запустите setup_db.py")
        st.stop()

    # KPI
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Всего пациентов", f"{df_gender['count'].sum():,.0f}")
    col2.metric("Средний возраст", f"{df_age['age'].mean():.1f} лет")
    col3.metric("Топ район", df_district_patients.iloc[0]['район_проживания'])
    col4.metric("Всего рецептов", f"{df_season['cases'].sum():,.0f}")
    
    st.divider()

    # ГРАФИКИ
    c1, c2 = st.columns([1, 2])
    with c1:
        fig_gender = px.pie(df_gender, values="count", names="пол", title="Пол", color_discrete_map={"М": "#1f77b4", "Ж": "#ff7f0e"}, hole=0.4)
        st.plotly_chart(fig_gender, use_container_width=True)
    with c2:
        fig_age = px.histogram(df_age, x="age", nbins=30, title="Возраст", color_discrete_sequence=['#00CC96'])
        st.plotly_chart(fig_age, use_container_width=True)

    st.plotly_chart(px.treemap(df_district_patients, path=['район_проживания'], values='count', title='Районы'), use_container_width=True)
    
    st.plotly_chart(px.area(df_season, x="month_year", y="cases", title="Динамика рецептов"), use_container_width=True)

    # Статистика заболеваний (Твой блок)
    st.subheader("📈 Статистика заболеваний")
    con = duckdb.connect(DB_PATH, read_only=True)
    df_top_classes = con.execute("SELECT класс_заболевания, COUNT(*) AS cases FROM prescriptions p JOIN diagnoses d ON p.код_диагноза = d.код_мкб GROUP BY класс_заболевания ORDER BY cases DESC LIMIT 20").df()
    fig_top_classes = px.bar(df_top_classes, x="cases", y="класс_заболевания", orientation='h', title="Топ-20 классов", color="cases")
    st.plotly_chart(fig_top_classes, use_container_width=True)
    con.close()


# ==========================================
# 🤖 ВКЛАДКА 2: AI АГЕНТ (НОВАЯ ЛОГИКА)
# ==========================================
elif selected == "AI Агент":
    st.title("🤖 Чат с SQL-агентом")

    # 1. API KEY
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        api_key = st.text_input("Введите ключ OpenRouter:", type="password")
        if not api_key:
            st.warning("Требуется ключ API.")
            st.stop()

    # 2. САЙДБАР ЧАТОВ (Внутри страницы агента)
    with st.sidebar:
        st.markdown("---")
        st.subheader("🗂 История диалогов")
        if st.button("➕ Новый диалог", use_container_width=True):
            create_new_chat()
            st.rerun()
        
        # Список чатов
        chat_ids = list(st.session_state.chats.keys())
        for c_id in chat_ids:
            chat = st.session_state.chats[c_id]
            # Кнопка чата
            col_btn, col_del = st.columns([5, 1])
            is_active = (c_id == st.session_state.current_chat_id)
            label = f"{'📂' if is_active else '📁'} {chat['title']}"
            
            if col_btn.button(label, key=f"chat_{c_id}", use_container_width=True):
                st.session_state.current_chat_id = c_id
                st.rerun()
            
            if col_del.button("✕", key=f"del_{c_id}"):
                delete_chat(c_id)
                st.rerun()

    # 3. ТЕКУЩИЙ ЧАТ
    current_id = st.session_state.current_chat_id
    current_chat = st.session_state.chats[current_id]

    # Приветствие
    if not current_chat["messages"]:
        current_chat["messages"].append({"role": "assistant", "content": "Привет! Я готов анализировать данные. Задай вопрос (например: 'Динамика гриппа')."})

    # Вывод истории
    for msg in current_chat["messages"]:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            # Если в сообщении есть сохраненный график (сложно реализовать сохранение объекта figure, поэтому просто рендерим текущий ниже)

    # 4. ОБРАБОТКА ВОПРОСА
    if prompt := st.chat_input("Ваш вопрос к базе данных..."):
        # Обновляем заголовок чата
        if len(current_chat["messages"]) <= 2:
            current_chat["title"] = " ".join(prompt.split()[:3]) + "..."

        st.session_state.chats[current_id]["messages"].append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            try:
                agent = OpenRouterSQLAgent(api_key)
                
                with st.spinner("🔍 Анализирую базу данных..."):
                    # Вызов агента (он сам делает Loop и Self-Correction)
                    response = agent.answer(prompt)
                
                st.markdown(response)
                st.session_state.chats[current_id]["messages"].append({"role": "assistant", "content": response})

                # 5. АВТО-ВИЗУАЛИЗАЦИЯ
                # Читаем файл, который создал агент
                csv_path = "scripts_db/answer.csv"
                if os.path.exists(csv_path) and os.path.getsize(csv_path) > 0:
                    try:
                        df_result = pd.read_csv(csv_path)
                        if not df_result.empty and len(df_result) < 300:
                            fig = auto_visualize_data(df_result)
                            if fig:
                                st.markdown("---")
                                st.caption("📊 Автоматическая визуализация:")
                                st.plotly_chart(fig, use_container_width=True)
                            else:
                                with st.expander("Посмотреть таблицу данных"):
                                    st.dataframe(df_result)
                    except Exception as e:
                        print(f"Ошибка визуализации: {e}")

            except Exception as e:
                st.error(f"Ошибка: {e}")