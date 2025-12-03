import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# --- Настройка путей и папки для сохранения (Оставляем как есть) ---
DB_PATH = Path(__file__).parent.parent / "db" / "medinsight.duckdb"
charts_dir = Path(__file__).parent / 'charts'

# --- 1. Гарантированное создание папки charts (Оставляем как есть) ---
try:
    charts_dir.mkdir(exist_ok=True)
    print(f"Папка для графиков создана/проверена: {charts_dir}")
except Exception as e:
    print(f"❌ ОШИБКА: Не удалось создать папку для графиков: {e}")
    exit(1)

# --- Настройка графиков ---
sns.set_theme(style="whitegrid")
# Убедитесь, что ваш шрифт поддерживает кириллицу (может потребоваться установка системного шрифта)
plt.rcParams['font.sans-serif'] = ['DejaVu Sans', 'Arial Unicode MS', 'Helvetica'] 
plt.rcParams['axes.unicode_minus'] = False 

print("Попытка установки соединения с DuckDB...")

# --- НОВАЯ ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ ДЛЯ СОКРАЩЕНИЯ ТЕКСТА ---
def shorten_label(label, max_length=40):
    """Сокращает строку до max_length, добавляя '...'."""
    if isinstance(label, str) and len(label) > max_length:
        return label[:max_length-3] + '...'
    return label

# --- Функция для выгрузки данных (Оставляем как есть) ---
def load_insight_data(con, table_name):
    """Выгружает данные из инсайт-таблицы в DataFrame."""
    query = f"SELECT * FROM {table_name}"
    df = con.execute(query).fetchdf()
    print(f"Загружены данные из таблицы: {table_name}. Строк: {len(df)}")
    return df

# --- Основная логика ---
con = None
try:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    print("Соединение с DuckDB установлено.")
    
    # 2. Загрузка всех инсайт-таблиц (Оставляем как есть)
    df_cost = load_insight_data(con, 'insight_cost_by_disease')
    df_region_drug = load_insight_data(con, 'insight_region_drug_choice')
    df_gender = load_insight_data(con, 'insight_gender_disease')
    
    # -----------------------------------------------------------
    # ГРАФИК 1: Средняя стоимость лечения на пациента (ТОП-10)
    # -----------------------------------------------------------
    print("\nСтроим график 1: Топ-10 дорогих заболеваний (на пациента).")
    top_n = 10
    df_plot = df_cost.sort_values('avg_cost_per_patient', ascending=False).head(top_n)

    # *** Изменение 1: Сокращаем названия заболеваний для оси Y ***
    df_plot['disease_group_short'] = df_plot['disease_group'].apply(shorten_label)
    
    plt.figure(figsize=(12, 6))
    # Используем сокращенное название для оси Y
    sns.barplot(x='avg_cost_per_patient', y='disease_group_short', data=df_plot, palette='rocket')
    
    plt.title(f'Топ {top_n} классов заболеваний по средней стоимости на пациента')
    plt.xlabel('Средняя стоимость лечения на пациента (руб.)')
    plt.ylabel('Класс заболевания') # Метка остается прежней
    
    # *** Изменение 2: Увеличение левого поля для размещения длинных меток ***
    plt.subplots_adjust(left=0.35) 
    # ВАЖНО: plt.tight_layout() после plt.subplots_adjust(left=0.35) может отменить left=0.35
    # Поэтому иногда лучше использовать только plt.subplots_adjust
    # Но для сохранения других настроек (заголовка, легенды) мы попробуем их сочетать
    #plt.tight_layout() 
    
    plt.savefig(charts_dir / '01_avg_cost_per_patient.png')
    plt.close()

    # -----------------------------------------------------------
    # ГРАФИК 2: Доля ведущих препаратов по районам (Эффект врача)
    # -----------------------------------------------------------
    print("Строим график 2: Эффект врача (топ-диагноз).")
    
    # ... (Подготовка данных остается как есть) ...
    target_disease = df_region_drug['disease_group'].value_counts().index[0] 
    df_filtered = df_region_drug[df_region_drug['disease_group'] == target_disease].copy()
    top_regions = df_filtered.groupby('region')['prescriptions_count'].sum().nlargest(5).index
    top_drugs = df_filtered.groupby('drug_name')['prescriptions_count'].sum().nlargest(5).index 
    df_plot = df_filtered[
        df_filtered['region'].isin(top_regions) & 
        df_filtered['drug_name'].isin(top_drugs)
    ].copy() # Добавляем .copy() для безопасности

    # *** Изменение 3: Сокращаем названия регионов для оси Y ***
    df_plot['region_short'] = df_plot['region'].apply(shorten_label, max_length=20)
    
    plt.figure(figsize=(12, 8))
    sns.barplot(
        data=df_plot, 
        x='prescriptions_share', 
        y='region_short', # Используем сокращенное название
        hue='drug_name', 
        palette='Spectral'
    )
    plt.title(f'Доля (share) топ-5 препаратов для "{shorten_label(target_disease, 30)}" по районам')
    plt.xlabel('Доля назначений в регионе/диагнозе')
    plt.ylabel('Район')
    
    # *** Изменение 4: Настраиваем положение легенды, чтобы не мешала ***
    plt.legend(title='Препарат', bbox_to_anchor=(1.05, 1), loc=2)
    plt.subplots_adjust(left=0.20, right=0.75) # Увеличение левого и уменьшение правого поля
    
    plt.savefig(charts_dir / '02_region_drug_choice.png')
    plt.close()
    
    # -----------------------------------------------------------
    # ГРАФИК 3: Топ-10 классов по общей стоимости рецепта
    # -----------------------------------------------------------
    print("Строим график 3: Распределение рецептов по болезням.")
    top_n = 10
    df_plot = df_cost.sort_values('avg_cost_per_prescription', ascending=False).head(top_n)

    # *** Изменение 5: Сокращаем названия заболеваний для оси Y ***
    df_plot['disease_group_short'] = df_plot['disease_group'].apply(shorten_label)

    plt.figure(figsize=(12, 6))
    sns.barplot(x='avg_cost_per_prescription', y='disease_group_short', data=df_plot, palette='viridis')
    plt.title(f'Топ {top_n} классов заболеваний по средней стоимости рецепта')
    plt.xlabel('Средняя стоимость рецепта (руб.)')
    plt.ylabel('Класс заболевания')
    
    # *** Изменение 6: Увеличение левого поля ***
    plt.subplots_adjust(left=0.35)
    #plt.tight_layout()
    
    plt.savefig(charts_dir / '03_avg_cost_per_prescription.png')
    plt.close()

    # -----------------------------------------------------------
    # ГРАФИК 4: Разница в заболеваемости по полу (топ-10)
    # -----------------------------------------------------------
    print("Строим график 4: Гендерный дисбаланс (женщины - мужчины).")
    target_age_group = df_gender['age_group'].value_counts().index[0] 
    df_plot = df_gender[df_gender['age_group'] == target_age_group].copy()
    df_plot = df_plot.sort_values('female_minus_male', ascending=False).head(10)
    
    # *** Изменение 7: Сокращаем названия заболеваний для оси Y ***
    df_plot['disease_group_short'] = df_plot['disease_group'].apply(shorten_label)
    
    plt.figure(figsize=(12, 6))
    sns.barplot(x='female_minus_male', y='disease_group_short', data=df_plot, palette='coolwarm')
    plt.title(f'Разница в количестве пациентов (Ж - М) для группы "{target_age_group}"')
    plt.xlabel('Разница (Женщины - Мужчины)')
    plt.ylabel('Класс заболевания')
    
    # *** Изменение 8: Увеличение левого поля ***
    plt.subplots_adjust(left=0.35)
    #plt.tight_layout()
    
    plt.savefig(charts_dir / '04_gender_difference.png')
    plt.close()
    
    # ... (Графики 5, 6, 7, 8, 9, 10 не требуют существенных изменений 
    #     или уже используют другие методы: круговая диаграмма, таблица, 
    #     вертикальные столбцы или точечная диаграмма.) ...
    
    # -----------------------------------------------------------
    # ГРАФИК 10: Топ-10 классов заболеваний по количеству пациентов
    # -----------------------------------------------------------
    print("Строим график 10: Топ-10 заболеваний по количеству пациентов.")
    top_n = 10
    df_plot = df_cost.sort_values('total_patients', ascending=False).head(top_n)

    # *** Изменение 9: Сокращаем названия заболеваний для оси Y ***
    df_plot['disease_group_short'] = df_plot['disease_group'].apply(shorten_label)
    
    plt.figure(figsize=(12, 6))
    sns.barplot(x='total_patients', y='disease_group_short', data=df_plot, palette='cubehelix')
    plt.title(f'Топ {top_n} классов заболеваний по общему количеству пациентов')
    plt.xlabel('Количество пациентов')
    plt.ylabel('Класс заболевания')

    # *** Изменение 10: Увеличение левого поля ***
    plt.subplots_adjust(left=0.35)
    #plt.tight_layout()
    
    plt.savefig(charts_dir / '10_total_patients_by_disease.png')
    plt.close()


    print("\n✅ Генерация 10 графиков завершена с улучшенной читаемостью. Файлы сохранены в папке 'charts'.")

except Exception as e:
    print(f"\n❌ Произошла ошибка во время работы: {e}")
    if "lock" in str(e) or "DBeaver" in str(e):
        print("\nПОЖАЛУЙСТА, ЗАКРОЙТЕ DBeaver! Он держит блокировку на файле базы данных.")
        print("Повторите запуск скрипта после закрытия приложения.")
    else:
        print("Проверьте ваши инсайт-таблицы на наличие ошибок в SQL-запросах (например, пустые или некорректные данные).")

finally:
    if con:
        con.close()
        print("Соединение с DuckDB закрыто.")