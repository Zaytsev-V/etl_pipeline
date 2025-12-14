# load_countries_fixed.py
import requests
import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

def create_countries_table(engine, table_name='worldbank_countries'):
    """Создает таблицу с нужными типами данных используя транзакцию"""

    drop_table_sql = f"DROP TABLE IF EXISTS {table_name};"
    create_table_sql = f"""    
    CREATE TABLE {table_name} (
        country_id VARCHAR(10) PRIMARY KEY,
        iso2_code VARCHAR(5),
        country_name VARCHAR(200) NOT NULL,
        region_id VARCHAR(10),
        region_name VARCHAR(100),
        income_level_id VARCHAR(10),
        income_level_name VARCHAR(100),
        capital_city VARCHAR(100),
        longitude NUMERIC(10,6),
        latitude NUMERIC(10,6)
        );
    """
    try:
        # используем явное начало транзакции с автоматическим коммитом
        with engine.begin() as conn:
            conn.execute(text(drop_table_sql))
            conn.execute(text(create_table_sql))
        print(f"Таблица {table_name} создана с правильными типами данных")
        return True
    except Exception as e:
        print(f"Ошибка создания таблицы: {e}")
        return False

def main():
    """Основная функция скрипта"""

    print("Запуск загрузки стран из World Bank API...")

    load_dotenv('wb.env')
    # ппрвепяю наличие нужных переменных
    required_vars = ['DB_USER', 'DB_PASS', 'DB_HOST', 'DB_PORT', 'DB_NAME']
    missing_vars = [var for var in required_vars if not os.getenv(var)]

    if missing_vars:
        print(f"Ошибка: отсутствуют переменные окружения: {missing_vars}")
        return

    # создаю подключение
    try:
        connection_string = (
            f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}"
            f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        )
        engine = create_engine(connection_string, connect_args={'sslmode': 'require'})

        with engine.connect() as conn:
            conn.execute(text("SELECT 1")) # проверка на успешность подключения к базе данных

        print("Успешное подключение к Supabase PostgreSQL")

    except Exception as e:
        print(f"Ошибка подключения к Supabase: {e}")
        return

    print("Загружаем страны из World Bank API...")
    # подключаюсь к Всемирному Банку для загрузки данных по странам
    
    all_countries = []
    page = 1

    try:
        while True:
            url = "https://api.worldbank.org/v2/country"
            params = {'format': 'json', 'page': page, 'per_page': 500}

            print(f"Страница {page}...")

            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            if not isinstance(data, list) or len(data) < 2 or not data[1]:
                break

            for country in data[1]:
                all_countries.append({
                    'country_id': country.get('id'),
                    'iso2_code': country.get('iso2Code'),
                    'country_name': country.get('name'),
                    'region_id': country.get('region', {}).get('id'),
                    'region_name': country.get('region', {}).get('value'),
                    'income_level_id': country.get('incomeLevel', {}).get('id'),
                    'income_level_name': country.get('incomeLevel', {}).get('value'),
                    'capital_city': country.get('capitalCity'),
                    'longitude': country.get('longitude'),
                    'latitude': country.get('latitude')
                })

            print(f"Обработано {len(data[1])} стран")

            if page >= data[0].get('pages', 1): # проверка пагинации
                break
            page += 1

    except Exception as e:
        print(f"Ошибка при загрузке данных: {e}")
        return

    if not all_countries:
        print("Не удалось загрузить данные")
        return

    # сохранаю данные в DataFrame
    df = pd.DataFrame(all_countries)
    total_loaded = len(df)
    print(f"Всего загружено: {total_loaded} записей")
    # фильтрую данные, убираю строки с агрегацией
    df_final = df[df['region_name'] != 'Aggregates'].copy()
    final_count = len(df_final)
    deleted_count = total_loaded - final_count
    deleted_percent = (deleted_count / total_loaded) * 100

    print(f"После фильтрации осталось стран: {final_count}")
    print(f"Удалено строк: {deleted_count}, {deleted_percent:.1f}%")

    # привожу longitude и latitude к числовому типу с обработкой ошибок (конвертируются ошибки в NaN)
    df_final['longitude'] = pd.to_numeric(df_final['longitude'], errors='coerce')
    df_final['latitude'] = pd.to_numeric(df_final['latitude'], errors='coerce')

    print("Сохраняем данные в Supabase ...")

    try:
        if not create_countries_table(engine):
            return

        df_final.to_sql(
            name='worldbank_countries',
            con=engine,
            if_exists='append',  # Добавляем записи в пустую таблицу
            index=False,
            method='multi'
        )

        print(f"Готово! Сохранено {final_count} стран")

        # делаю запрос к созданной базе
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'worldbank_countries'
                ORDER BY ordinal_position
            """))
            # вывожу структуру таблицы
            print("Структура таблицы:")
            for column_name, data_type in result:
                print(f"   {column_name}: {data_type}")

            count_result = conn.execute(text("SELECT COUNT(*) FROM worldbank_countries"))
            count = count_result.scalar()
            print(f"В таблице {count} записей")

    except Exception as e:
        print(f"Ошибка при сохранении: {e}")
    finally:
        engine.dispose()

# Этот блок выполняется только при прямом запуске скрипта
if __name__ == "__main__":
    main()
