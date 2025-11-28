import json
import os
from langchain.tools import Tool
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.agents import initialize_agent, AgentType

# --- 1. ФУНКЦИИ (ЛОГИКА) ---

def _get_district_stats_func(district_name: str) -> str:
    """Внутренняя логика поиска по районам"""
    path = 'data/stats_by_district.json'
    if not os.path.exists(path):
        return "Ошибка: Файл данных не найден."
        
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        
    if district_name:
        clean_name = str(district_name).replace('"', '').replace("'", "").strip().upper()
        # Ищем точное или частичное совпадение
        for key in data.keys():
            if clean_name in key:
                return json.dumps({key: data[key]}, ensure_ascii=False)
        return f"Данных по району {clean_name} нет. Доступные: {list(data.keys())[:5]}"
            
    return json.dumps(data, ensure_ascii=False)

def _get_seasonal_stats_func(season: str) -> str:
    """Внутренняя логика поиска по сезонам"""
    path = 'data/stats_by_season.json'
    if not os.path.exists(path):
        return "Ошибка: Файл данных не найден."
        
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        
    if season:
        clean_season = str(season).replace('"', '').replace("'", "").strip().capitalize()
        if clean_season in data:
            return json.dumps({clean_season: data[clean_season]}, ensure_ascii=False)
        
    return json.dumps(data, ensure_ascii=False)

# def _get_sql_based_answer() -> str:
#     """Внутренняя логика по поиску информации в БД по запросу в SQL"""
#     path = 'data'
#     if not os.path.exists(path):
#         return "Ошибка: Файл данных не найден."
        
#     with open(path, 'r', encoding='utf-8') as f:
#         data = json.load(f)
        
#     if season:
#         clean_season = str(season).replace('"', '').replace("'", "").strip().capitalize()
#         if clean_season in data:
#             return json.dumps({clean_season: data[clean_season]}, ensure_ascii=False)
        
#     return json.dumps(data, ensure_ascii=False)

# --- 2. СБОРКА АГЕНТА (LEGACY METHOD) ---

def get_agent_executor(api_key: str):
    # 1. Модель
    llm = ChatGoogleGenerativeAI(
        model="gemini-2.5-flash",
        google_api_key=api_key,
        temperature=0
    )
    
    # 2. Определение инструментов старым способом (через класс Tool)
    # Это самый надежный способ, который не ломается при смене версий
    tools = [
        Tool(
            name="GetDistrictStats",
            func=_get_district_stats_func,
            description="Используй, когда спрашивают про районы, географию или где больше/меньше болеют. Принимает название района или пустую строку."
        ),
        Tool(
            name="GetSeasonalStats",
            func=_get_seasonal_stats_func,
            description="Используй, когда спрашивают про сезоны (Зима, Весна, Лето, Осень) или время года."
        )
        # Tool(
        #     name="GetSQLBasedAnswer",
        #     func=_get_sql_based_answer,
        #     description="Используй, когда треубется найти специфичную информацию по запросу"
        # )
    ]
    
    # 3. Инициализация через initialize_agent
    # ZERO_SHOT_REACT_DESCRIPTION — это классический ReAct агент.
    agent_executor = initialize_agent(
        tools=tools,
        llm=llm,
        agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
        verbose=True,
        handle_parsing_errors=True # Обязательно, чтобы агент не падал при ошибке формата
    )
    
    return agent_executor