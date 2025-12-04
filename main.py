
import os
from dotenv import load_dotenv
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu
import plotly.express as px
import duckdb

from agent import OpenRouterSQLAgent # Новый сервис

load_dotenv()

def local_css():
    st.markdown(
        """
        <style>
        /* 1. Уменьшение основного текста (кегль) */
        html, body, [class*="css"]  {
            font-size: 14px; 
            font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif;
        }

        /* 2. Уменьшение заголовков */
        h1 { font-size: 24px !important; }
        h2 { font-size: 20px !important; }
        h3 { font-size: 18px !important; }

        /* 3. Изменение отступов (Margins/Padding) у главного контейнера */
        .block-container {
            padding-top: 2rem !important; /* Отступ сверху */
            padding-bottom: 2rem !important;
            padding-left: 3rem !important;
            padding-right: 3rem !important;
            max-width: 95% !important; /* Ширина контента */
        }

        /* 4. Стилизация метрик (KPI) - добавляем границы и тень */
        [data-testid="stMetric"] {
            background-color: #f9f9f9;
            border: 1px solid #e0e0e0;
            padding: 10px;
            border-radius: 5px; /* Закругление углов */
            box-shadow: 0 2px 4px rgba(0,0,0,0.05); /* Легкая тень */
        }
        
        /* Уменьшаем цифры в метриках */
        [data-testid="stMetricValue"] {
            font-size: 20px !important;
        }

        /* 5. Убираем лишние отступы между элементами */
        .element-container {
            margin-bottom: 0.5rem !important;
        }
        
        /* 6. Границы для вкладок (Tabs) */
        .stTabs [data-baseweb="tab-list"] {
            gap: 10px;
        }
        .stTabs [data-baseweb="tab"] {
            height: 40px;
            white-space: pre-wrap;
            background-color: #f0f2f6;
            border-radius: 4px 4px 0px 0px;
            gap: 1px;
            padding-top: 10px;
            padding-bottom: 10px;
        }
        .stTabs [aria-selected="true"] {
            background-color: #FFFFFF;
            border-bottom: 2px solid #1f77b4;
        }
        </style>
        """,
        unsafe_allow_html=True
    )

# --- КОНФИГУРАЦИЯ ---
DB_PATH = "db/medinsight.duckdb"
st.set_page_config(layout="wide", page_title="Medical Insight", page_icon="🏥")
local_css()

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
with st.sidebar:
    selected = option_menu(
        menu_title="Меню",  # Название меню
        options=["Дашборд", "AI Агент"],  # Пункты
        icons=["bar-chart-fill", "chat-left-text-fill"],  # Иконки (Bootstrap icons)
        menu_icon="cast",  # Иконка меню
        default_index=0,  # выбрано по умолчанию
        styles={
            "container": {"padding": "5!important", "background-color": "#fafafa"},
            "icon": {"color": "orange", "font-size": "15px"}, 
            "nav-link": {"font-size": "16px", "text-align": "left", "margin":"0px", "--hover-color": "#eee"},
            "nav-link-selected": {"background-color": "#1f77b4"},
        }
    )
# === ВКЛАДКА 1: ВИЗУАЛИЗАЦИЯ ===
if selected == "Дашборд":
    st.title("📊 Аналитический Дашборд")
    # ----------------------------------------
    # KPI
    # ----------------------------------------
    col1, col2, col3, col4 = st.columns(4)
    total_patients_kpi = df_gender['count'].sum()
    avg_age_kpi = df_age['age'].mean()
    top_district_kpi = df_district_patients.iloc[0]['район_проживания']
    
    col1.metric("Всего пациентов", f"{total_patients_kpi:,.0f}")
    col2.metric("Средний возраст", f"{avg_age_kpi:.1f} лет")
    col3.metric("Самый населенный район", top_district_kpi)
    col4.metric("Всего рецептов", f"{df_season['cases'].sum():,.0f}")
    
    st.divider()

    # ----------------------------------------
    # 👤 ПОРТРЕТ ПАЦИЕНТА
    # ----------------------------------------
    st.subheader("👤 Портрет пациента")
    c1, c2 = st.columns([1, 2])
    
    with c1:
        fig_gender = px.pie(
            df_gender,
            values="count",
            names="пол",
            title="Распределение по полу",
            color_discrete_map={"М": "#1f77b4", "Ж": "#f30f9b"},
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
        color_continuous_scale='cividis'
    )

    fig_tree.update_traces(
        texttemplate='%{label}<br>%{value}',
        textfont_size=18
    )

    fig_tree.update_layout(
        margin=dict(t=50, l=25, r=25, b=25),
        height=650,
        title_font_size=22
    )

    st.plotly_chart(fig_tree, use_container_width=True)

    st.divider()

    # ----------------------------------------
    # 📅 ДИНАМИКА ОБРАЩЕНИЙ (оставляем)
    # ----------------------------------------
    st.subheader("📅 Динамика обращений")
    fig_season = px.area(
        df_season,
        x="month_year",
        y="cases",
        title="Выдача рецептов по месяцам",
        labels={"cases": "Количество рецептов", "month_year": "Месяц"},
        markers=True,
        color_discrete_sequence=["#1f77b4"]
    )
    fig_season.update_xaxes(
        dtick="M1",
        tickformat="%b %Y",
        showgrid=True,
        ticks="outside"
    )
    st.plotly_chart(fig_season, use_container_width=True)

    st.divider()

    # ======================================================
    # 📈 НОВЫЙ БЛОК: СТАТИСТИКА ЗАБОЛЕВАНИЙ
    # ======================================================
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
        color_continuous_scale="cividis"
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
        color_discrete_sequence=px.colors.qualitative.Set3
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
        color_continuous_scale="cividis"
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
        color_continuous_scale="cividis"
    )

    st.plotly_chart(fig_cost_top10, use_container_width=True)

    con.close()



# === ВКЛАДКА 2: АГЕНТ (ОБНОВЛЕННАЯ ЛОГИКА) ===
elif selected == "AI Агент":
    st.title("Чат с SQL-агентом")

    # st.header("Чат с SQL-агентом (Powered by Llama 3.3)")
    
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