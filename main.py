import os
import uuid
from dotenv import load_dotenv
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu
import plotly.express as px
import duckdb
import uuid

from agent import OpenRouterSQLAgent

load_dotenv()
def create_new_chat():
    new_id = str(uuid.uuid4())[:8]
    st.session_state.chat_histories[new_id] = {
        "name": f"Чат {len(st.session_state.chat_histories) + 1}",
        "messages": [
            {"role": "assistant", "content": "Новый чат открыт. Чем могу помочь?"}
        ]
    }
    st.session_state.current_chat_id = new_id

def delete_chat(chat_id):
    if chat_id in st.session_state.chat_histories:
        del st.session_state.chat_histories[chat_id]

# ==========================================
# 🛠 ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ (Кэш и Визуал)
# ==========================================

# 1. Кэширование агента (ВАЖНО для скорости)
@st.cache_resource
def get_agent(api_key_val):
    return OpenRouterSQLAgent(api_key_val)

# 2. Функция Авто-визуализации
def auto_visualize_data(df: pd.DataFrame):
    """Автоматически строит график по DataFrame"""
    if df is None or df.empty or len(df.columns) < 2: return None
    
    # Определяем типы колонок
    num_cols = df.select_dtypes(include=['number']).columns.tolist()
    cat_cols = df.select_dtypes(include=['object', 'string']).columns.tolist()
    date_cols = [col for col in df.columns if any(x in col.lower() for x in ['date', 'time', 'year', 'month', 'day', 'дата', 'год', 'месяц'])]
    
    fig = None
    try:
        # Линейный график (Временной ряд)
        if len(date_cols) > 0 and len(num_cols) > 0:
            x_col = date_cols[0]; y_col = num_cols[0]
            df = df.sort_values(by=x_col)
            fig = px.line(df, x=x_col, y=y_col, markers=True, title=f"Динамика: {y_col}", template="plotly_white")
        
        # Бар-чарт или Пай-чарт (Категории)
        elif len(cat_cols) > 0 and len(num_cols) > 0:
            x_col = cat_cols[0]; y_col = num_cols[0]
            # Если мало категорий и это похоже на доли -> Pie Chart
            if len(df) <= 6 and any(x in y_col.lower() for x in ['share', 'доля', 'процент']): 
                fig = px.pie(df, names=x_col, values=y_col, title=f"Распределение: {x_col}")
            else:
                fig = px.bar(df, x=x_col, y=y_col, title=f"{y_col} по {x_col}", color=y_col, template="plotly_white", color_continuous_scale="Blues")
    except Exception:
        return None
        
    return fig

# 3. Управление чатами
def create_new_chat():
    new_id = str(uuid.uuid4())[:8]
    st.session_state.chat_histories[new_id] = {
        "name": f"Чат {len(st.session_state.chat_histories) + 1}",
        "messages": [
            {"role": "assistant", "content": "Новый чат открыт. Чем могу помочь?"}
        ]
    }
    st.session_state.current_chat_id = new_id

def delete_chat(chat_id):
    if chat_id in st.session_state.chat_histories:
        del st.session_state.chat_histories[chat_id]

    if st.session_state.current_chat_id == chat_id:
        if st.session_state.chat_histories:
            st.session_state.current_chat_id = next(iter(st.session_state.chat_histories))
        else:
            create_new_chat()

def switch_chat(chat_id):
    st.session_state.current_chat_id = chat_id

# 4. CSS Стили
def local_css():
    st.markdown(
        """
        <style>
        html, body, [class*="css"]  { font-size: 14px; font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif; }
        h1 { font-size: 24px !important; }
        h2 { font-size: 20px !important; }
        .block-container { padding-top: 2rem !important; padding-bottom: 2rem !important; padding-left: 3rem !important; padding-right: 3rem !important; max-width: 95% !important; }
        [data-testid="stMetric"] { background-color: #f9f9f9; border: 1px solid #e0e0e0; padding: 10px; border-radius: 5px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }
        [data-testid="stMetricValue"] { font-size: 20px !important; }
        .element-container { margin-bottom: 0.5rem !important; }
        .stButton button { text_align: left !important; }
        </style>
        """,
        unsafe_allow_html=True
    )

# --- КОНФИГУРАЦИЯ ---
DB_PATH = "db/medinsight.duckdb"
st.set_page_config(layout="wide", page_title="Medical Insight", page_icon="🏥")
local_css()

# --- ЗАГРУЗКА ДАННЫХ ---
@st.cache_data
def load_dashboard_data():
    if not os.path.exists(DB_PATH):
        return None, None, None, None, None, None

    con = duckdb.connect(DB_PATH, read_only=True)
    
    # Запросы к БД (как в твоем коде)
    df_gender = con.execute("SELECT пол, COUNT(*) as count FROM patients GROUP BY пол").df()
    df_age = con.execute("SELECT date_diff('year', дата_рождения, CURRENT_DATE) as age FROM patients WHERE дата_рождения IS NOT NULL").df()
    df_district_patients = con.execute("SELECT район_проживания, COUNT(*) as count FROM patients WHERE район_проживания IS NOT NULL GROUP BY район_проживания ORDER BY count DESC").df()
    df_finance = con.execute("SELECT disease_group, avg_cost_per_prescription, avg_cost_per_patient FROM insight_cost_by_disease ORDER BY avg_cost_per_patient DESC LIMIT 10").df()
    df_geo_drugs = con.execute("SELECT region, SUM(prescriptions_count) as total_prescriptions FROM insight_region_drug_choice GROUP BY region ORDER BY total_prescriptions DESC").df()
    df_season = con.execute("SELECT strftime(дата_рецепта, '%Y-%m') as month_year, COUNT(*) as cases FROM prescriptions GROUP BY month_year ORDER BY month_year").df()

    con.close()
    return df_gender, df_age, df_district_patients, df_finance, df_geo_drugs, df_season

# --- ИНИЦИАЛИЗАЦИЯ СОСТОЯНИЯ ---
if "chat_histories" not in st.session_state:
    st.session_state.chat_histories = {} 
if "current_chat_id" not in st.session_state:
    st.session_state.current_chat_id = None

# --- ГЛАВНЫЙ ИНТЕРФЕЙС ---

# Загрузка
data = load_dashboard_data()
df_gender, df_age, df_district_patients, df_finance, df_geo_drugs, df_season = data

if df_gender is None:
    st.error(f"❌ База данных не найдена по пути: {DB_PATH}.")
    st.stop()

# САЙДБАР
with st.sidebar:
    selected = option_menu(
        menu_title="Меню",
        options=["Дашборд", "AI Агент"],
        icons=["bar-chart-fill", "chat-left-text-fill"],
        menu_icon="cast",
        default_index=0,
        styles={
            "container": {"padding": "5!important", "background-color": "#fafafa"},
            "nav-link": {"font-size": "16px", "text-align": "left", "margin":"0px", "--hover-color": "#eee"},
            "nav-link-selected": {"background-color": "#1f77b4"},
        }
    )

    # Управление чатами (только если выбран агент)
    if selected == "AI Агент":
        st.divider()
        st.subheader("🗂 Чаты")
        
        # Если нет активного чата — создаем
        if st.session_state.current_chat_id is None:
            create_new_chat()
            
        # Список чатов
        for cid in list(st.session_state.chat_histories.keys()):
            chat_data = st.session_state.chat_histories[cid]
            is_active = (cid == st.session_state.current_chat_id)
            
            col1, col2 = st.columns([4, 1])
            with col1:
                label = f"**{chat_data['name']}**" if is_active else chat_data['name']
                if st.button(label, key=f"open_{cid}", use_container_width=True):
                    switch_chat(cid)
                    st.rerun()
            with col2:
                if st.button("✕", key=f"del_{cid}"):
                    delete_chat(cid)
                    st.rerun()

        if st.button("➕ Новый чат", use_container_width=True):
            create_new_chat()
            st.rerun()


# === ВКЛАДКА 1: ДАШБОРД ===
if selected == "Дашборд":
    st.title("📊 Аналитический Дашборд")
    
    # KPI
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Всего пациентов", f"{df_gender['count'].sum():,.0f}")
    col2.metric("Средний возраст", f"{df_age['age'].mean():.1f} лет")
    col3.metric("Самый населенный район", df_district_patients.iloc[0]['район_проживания'])
    col4.metric("Всего рецептов", f"{df_season['cases'].sum():,.0f}")
    st.divider()

    # Графики демографии
    c1, c2 = st.columns([1, 2])
    
    with c1:
        fig_gender = px.pie(
            df_gender,
            values="count",
            names="пол",
            title="Распределение по полу",
            color_discrete_map={"М": "#636EFA", "Ж": "#EF553B"},
            hole=0.4
        )
        fig_gender.update_traces(textinfo='percent', textfont_size=18)
        st.plotly_chart(fig_gender, use_container_width=True)
        
    with c2:
        fig_age = px.histogram(
            df_age, 
            x="age", 
            nbins=30,
            title="Возрастная структура пациентов",
            labels={'age': 'Возраст', 'count': 'Количество пациентов'},
            color_discrete_sequence=['#00CC96']
        )
        fig_age.update_layout(bargap=0.1)
        st.plotly_chart(fig_age, use_container_width=True)

    st.divider()

    # ----------------------------------------
    # 🏠 ГЕОГРАФИЯ ПАЦИЕНТОВ
    # ----------------------------------------
    st.subheader("🏠 Где живут наши пациенты?")
    fig_tree = px.treemap(
        df_district_patients,
        path=['район_проживания'],
        values='count',
        title='Распределение пациентов по районам',
        color='count',
        color_continuous_scale='Blues' 
    )

    fig_tree.update_traces(
        texttemplate='%{label}<br>%{value}',
        textfont_size=18
    )

    fig_tree.update_layout(
        margin=dict(t=50, l=25, r=25, b=25),
        height=650,
        title_font_size=18
    )

    st.plotly_chart(fig_tree, use_container_width=True)

    st.divider()

    # Статистика заболеваний (Твой блок)
    st.subheader("📈 Статистика заболеваний")
    con = duckdb.connect(DB_PATH, read_only=True)

    # --- 1. Топ-20 классов заболеваний ---
    short_names = {
        "Болезни системы кровообращения": "Сердечно-сосудистые",
        "Болезни дыхательной системы": "Дыхательная система",
        "Болезни эндокринной системы": "Эндокринная система",
        "Болезни нервной системы": "Нервная система",
        "Болезни мочеполовой системы": "Мочеполовая система",
        "Болезни органов пищеварения": "Пищеварение"
    }
    df_top_classes = con.execute("""
        SELECT 
            класс_заболевания,
            COUNT(*) AS cases
        FROM prescriptions p
        JOIN diagnoses d ON p.код_диагноза = d.код_мкб
        GROUP BY класс_заболевания
        ORDER BY cases DESC
        LIMIT 20
    """).df()
    df_top_classes["класс_заболевания"] = df_top_classes["класс_заболевания"].replace(short_names)

    fig_top_classes = px.bar(
        df_top_classes,
        x="cases",
        y="класс_заболевания",
        orientation='h',
        title="Топ-20 классов заболеваний",
        labels={"cases": "Число обращений", "класс_заболевания": "Класс заболеваний"},
        color="cases",
        color_continuous_scale="Tealgrn"
    )
    st.plotly_chart(fig_top_classes, use_container_width=True)

    st.markdown("---")

    # --- 2. Частота заболеваний внутри выбранного класса ---
    st.markdown("### 🧬 Частота заболеваний внутри класса")

    classes_list = df_top_classes["класс_заболевания"].unique().tolist()
    selected_class = st.selectbox("Выберите класс заболевания:", classes_list)

    # Получаем детальную статистику по всем заболеваниям в классе
    df_group_detail = con.execute(f"""
        SELECT 
            d.название_диагноза,
            COUNT(*) AS cnt
        FROM prescriptions p
        JOIN diagnoses d ON p.код_диагноза = d.код_мкб
        WHERE d.класс_заболевания = '{selected_class}'
        GROUP BY d.название_диагноза
        ORDER BY cnt DESC
    """).df()

    # Рассчитываем долю каждого заболевания
    total_cases = df_group_detail['cnt'].sum()
    df_group_detail['доля'] = (df_group_detail['cnt'] / total_cases * 100).round(2)
    df_group_detail['процент'] = df_group_detail['доля'].astype(str) + '%'

    # Ограничиваем количество отображаемых заболеваний (топ-6 + остальные для компактности)
    top_n = 6  # Уменьшили для компактной легенды
    if len(df_group_detail) > top_n:
        top_diseases = df_group_detail.head(top_n).copy()
        other_cases = df_group_detail.iloc[top_n:]['cnt'].sum()
        other_share = (other_cases / total_cases * 100).round(2)
        
        # Создаем строку для "Остальных"
        other_row = pd.DataFrame({
            'название_диагноза': [f'Остальные ({len(df_group_detail) - top_n} диагнозов)'],
            'cnt': [other_cases],
            'доля': [other_share],
            'процент': [f'{other_share}%']
        })
        
        df_plot = pd.concat([top_diseases, other_row], ignore_index=True)
    else:
        df_plot = df_group_detail.copy()

    # Сортируем по убыванию для лучшей читаемости
    df_plot = df_plot.sort_values('доля', ascending=True)

    # Создаем stacked bar chart
    fig_group_details = px.bar(
        df_plot,
        x='доля',
        y=pd.Series([selected_class] * len(df_plot)),  # Все столбцы будут в одной строке
        orientation='h',
        color='название_диагноза',
        title=f"Структура диагнозов в классе: {selected_class}",
        labels={
            'доля': 'Доля от всех случаев в классе (%)',
            'y': '',
            'название_диагноза': 'Конкретный диагноз'
        },
        text='процент',
        color_discrete_sequence=px.colors.qualitative.Prism
    )

    # НАСТРОЙКА ЛЕГЕНДЫ - КАЖДЫЙ ЭЛЕМЕНТ В НОВОЙ СТРОКЕ
    fig_group_details.update_layout(
        showlegend=True,
        legend_title=dict(
            text="<b>Диагнозы:</b>",
            font=dict(size=12)
        ),
        # ВЕРТИКАЛЬНАЯ ЛЕГЕНДА С ОДНИМ ЭЛЕМЕНТОМ В СТРОКЕ
        legend=dict(
            orientation="v",  # Вертикальная ориентация
            yanchor="top",
            y=-0.45,  # Размещаем ниже графика
            xanchor="center",
            x=0.5,    # Центрируем по горизонтали
            font=dict(size=11),
            itemwidth=30,
            itemsizing="constant",
            # НАСТРОЙКИ ДЛЯ ОДНОГО ЭЛЕМЕНТА В СТРОКУ
            traceorder="normal",
            itemclick="toggleothers",
            itemdoubleclick="toggle",
            # Группируем элементы в столбцы (если нужно)
            groupclick="toggleitem",
            # Отступы между элементами
            borderwidth=1,
            bordercolor="LightGray",
            bgcolor="rgba(255, 255, 255, 0.9)",
            # Фиксируем размеры для читаемости
            entrywidth=200,  # Ширина каждой записи
            entrywidthmode="pixels"
        ),
        # Увеличиваем отступ снизу для легенды
        margin=dict(l=10, r=10, t=50, b=180),  # Увеличили bottom
        height=500,
        bargap=0.5,
        yaxis=dict(
            showticklabels=False,
            title_text=""
        ),
        xaxis=dict(
            range=[0, 100],
            title_text="Доля случаев (%)",
            ticksuffix="%"
        ),
        title=dict(
            y=0.95,
            x=0.5,
            xanchor='center',
            font=dict(size=16)
        )
    )

    # Настраиваем подписи на столбцах
    fig_group_details.update_traces(
        textposition='inside',
        insidetextanchor='middle',
        textfont=dict(size=10, color='black', family="Arial"),
        hovertemplate=(
            "<b>%{customdata[0]}</b><br>" +
            "Доля: %{customdata[1]:.1f}%<br>" +
            "Количество: %{customdata[2]:,} случаев<br>" +
            "<extra></extra>"
        )
    )

    # Добавляем абсолютные числа в кастомные данные для тултипа
    fig_group_details.data[0].customdata = list(zip(
        df_plot['название_диагноза'],
        df_plot['доля'],
        df_plot['cnt']
    ))

    # Дополнительная информация под графиком
    st.plotly_chart(fig_group_details, use_container_width=True)
    
    # Информационная панель под графиком
    with st.expander("📊 Детальная информация о классе заболеваний", expanded=False):
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Всего диагнозов в классе", len(df_group_detail))
        with col2:
            st.metric("Всего случаев", f"{total_cases:,}")
        with col3:
            most_common = df_group_detail.iloc[0]
            st.metric("Самый частый диагноз", 
                     f"{most_common['название_диагноза'][:30]}...", 
                     f"{most_common['доля']}%")

    st.markdown("---")

    # --- 3. Половые различия ---
    st.subheader("🚻 Половые различия по группам заболеваний")

    df_gender_diff = con.execute("""
        SELECT 
            disease_group AS группа_заболеваний,
            male_patients AS мужчины,
            female_patients AS женщины,
            female_minus_male AS разница
        FROM insight_gender_disease
        ORDER BY разница DESC
    """).df()
    df_gender_diff["короткое_название"] = df_gender_diff["группа_заболеваний"].replace(short_names)
    df_gender_diff = df_gender_diff.sort_values("разница", ascending=False)

    fig_gender_diff = px.bar(
        df_gender_diff,
        x="разница",
        y="группа_заболеваний",
        orientation="h",
        title="Разница количества пациентов (Ж − М)",
        labels={"разница": "Разница (Ж − М)", "короткое_название": "Группа заболеваний"},
        color="разница",
        color_continuous_scale="Tealgrn"
    )

    st.plotly_chart(fig_gender_diff, use_container_width=True)

    st.markdown("---")

    # --- 4. Топ-10 заболеваний по стоимости лечения ---
    st.subheader("💰 Топ-10 заболеваний по стоимости лечения пациента")

    df_cost_top10 = con.execute("""
        SELECT 
            disease_group AS группа,
            avg_cost_per_patient AS стоимость
        FROM insight_cost_by_disease
        ORDER BY avg_cost_per_patient DESC
        LIMIT 10
    """).df()
    df_cost_top10["короткое"] = df_cost_top10["группа"].replace(short_names)
    df_cost_top10 = df_cost_top10.sort_values("стоимость", ascending=False)

    fig_cost_top10 = px.bar(
        df_cost_top10,
        x="стоимость",
        y="короткое",
        orientation="h",
        title="Топ-10 заболеваний по стоимости лечения пациента",
        labels={"стоимость": "Стоимость на пациента", "короткое": "Группа заболеваний"},
        color="стоимость",
        color_continuous_scale="Tealgrn"
    )

    st.plotly_chart(fig_cost_top10, use_container_width=True)

    con.close()


# === ВКЛАДКА 2: AI АГЕНТ (ИСПРАВЛЕННАЯ) ===
elif selected == "AI Агент":
    st.title("🤖 Чат с SQL-агентом")

    # 1. API KEY
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        api_key = st.text_input("Введите ключ OpenRouter:", type="password")
        if not api_key: st.stop()

    # Получаем текущий чат
    if st.session_state.current_chat_id is None:
        create_new_chat()
    chat_id = st.session_state.current_chat_id
    messages = st.session_state.chat_histories[chat_id]["messages"]

    # 2. ИСТОРИЯ СООБЩЕНИЙ (С ГРАФИКАМИ)
    for msg in messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            # ВАЖНО: Если есть сохраненный DataFrame, рисуем его
            if "dataframe" in msg and msg["dataframe"] is not None:
                fig = auto_visualize_data(msg["dataframe"])
                if fig:
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    with st.expander("Показать данные"):
                        st.dataframe(msg["dataframe"])

    # 3. ОБРАБОТКА НОВОГО ВОПРОСА
    if prompt := st.chat_input("Ваш вопрос к базе данных..."):
        # Обновляем имя чата
        if len(messages) <= 2:
            st.session_state.chat_histories[chat_id]["name"] = " ".join(prompt.split()[:4]) + "..."

        # Добавляем вопрос пользователя
        st.session_state.chat_histories[chat_id]["messages"].append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            try:
                # Используем кэшированного агента (быстро!)
                agent = get_agent(api_key)
                
                with st.spinner("🤖 Анализирую данные..."):
                    answer = agent.answer(prompt)
                
                # Выводим текст
                st.markdown(answer)
                
                # Подготовка сообщения для сохранения
                msg_data = {"role": "assistant", "content": answer}

                # ПРОВЕРЯЕМ ФАЙЛ CSV ДЛЯ ГРАФИКА
                csv_path = "scripts_db/answer.csv"
                if os.path.exists(csv_path) and os.path.getsize(csv_path) > 0:
                    try:
                        df_result = pd.read_csv(csv_path)
                        # Если данные ок — визуализируем и сохраняем
                        if not df_result.empty and len(df_result) < 300:
                            fig = auto_visualize_data(df_result)
                            if fig:
                                st.plotly_chart(fig, use_container_width=True)
                            # Сохраняем DF в историю, чтобы график остался навсегда
                            msg_data["dataframe"] = df_result
                    except Exception: pass

                # Сохраняем ответ в историю
                st.session_state.chat_histories[chat_id]["messages"].append(msg_data)

            except Exception as e:
                st.error(f"Ошибка: {e}")
