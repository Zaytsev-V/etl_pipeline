import pandas as pd
import requests
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import time

def create_values_table(engine, table_name='worldbank_values'):
    """Создает таблицу с нужными типами данныз используя транзакцию"""

    drop_table_sql = f"DROP TABLE IF EXISTS {table_name}"
    create_table_sql = f"""
    CREATE TABLE {table_name} (
        country_id VARCHAR(10),
        country VARCHAR(100),
        indicator_id VARCHAR(50),
        indicator VARCHAR(100),
        year INT,
        value REAL 
    );
    """
    try:
        # используем явное начало транзакции с автоматическим коммитом
        with engine.begin() as conn:
            conn.execute(text(drop_table_sql))
            conn.execute(text(create_table_sql))
        print(f"Таблица {table_name} создана")
        return True
    except Exception as e:
        print(f"Ошибка создания таблицы: {e}")
        return False

def main():
    """Основная функция скрипта
        Сначала проверяем подключение к базе данных
        Потом извлекаем данные по api
        Сохраняем данные в dataframe
        Преобразуем данные
        Сохраняем данные в базу данных
        Проверяем сохраненные данные
    """
    # загружаю данные для подулючения к базе данных
    load_dotenv('wb.env')
    # проверяем, все ли переменные есть в env файле
    required_vars = ['DB_USER', 'DB_PASS', 'DB_HOST', 'DB_PORT', 'DB_NAME']
    missing_vars = [var for var in required_vars if not os.getenv(var)]

    if missing_vars:
        print(f'Ошибка: нет переменных окружения: {missing_vars}')
        return

    # создаю подключение к bd
    try:
        connection_string = (
            f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}"
            f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        )
        engine = create_engine(connection_string, connect_args={'sslmode': 'require'}) # аргументы для Supabase

        with engine.connect() as conn:
            conn.execute(text('SELECT 1')) # проверка на успешность подключения к базе данных

        print("Успешное подключение к Supabase PostgreSQL")

    except Exception as e:
        print(f"Ошибка подключения к Supabase: {e}")
        return

    print("Загружаем страны из World Bank API...")
    # подключаюсь к Всемирному Банку для загрузки данных по показателям

    base_url = "https://api.worldbank.org/v2/countries/all"
    start_year = 1960
    end_year = 2024
    indicators = ['NY.GDP.MKTP.CD', 'NY.GDP.PCAP.CD', 'SP.POP.TOTL', 'SP.URB.TOTL.IN.ZS', 'SE.XPD.TOTL.GD.ZS',
                  'IP.JRN.ARTC.SC',
                  'SH.XPD.CHEX.GD.ZS', 'SH.DYN.MORT', 'IT.NET.USER.ZS', 'EG.ELC.ACCS.ZS', 'EG.USE.PCAP.KG.OE']

    all_data = []

    try:
        for indicator in indicators:
            page = 1
            print(f"Загружаем индикатор: {indicator}")

            while True:
                url = f"{base_url}/indicator/{indicator}"
                params = {
                    'format': 'json',
                    'page': page,
                    'date': f"{start_year}:{end_year}",
                    'per_page': 10000
                }
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()

                # Проверяем структуру ответа
                if not data or len(data) < 2:
                    print(f"Нет данных для индикатора {indicator}")
                    break

                # Первый элемент содержит мета-информацию
                metadata = data[0]
                total_pages = metadata.get('pages', 1)
                print(f'Индикатор {indicator}: страница {page} из {total_pages}')

                # Второй элемент содержит фактические данные
                page_data = data[1]
                if not page_data:
                    print(f"Пустая страница для индикатора {indicator}")
                    break

                for item in page_data:
                    if item.get('value') is not None:
                        all_data.append({
                            'country_id': item['countryiso3code'],
                            'country': item['country']['value'],
                            'indicator_id': item['indicator']['id'],
                            'indicator': item['indicator']['value'],
                            'year': item['date'],
                            'value': item['value']
                        })

                print(f"Обработано {len(page_data)} значений на странице {page}")

                if page >= total_pages: # проверка пагинации
                    print(f"Завершена загрузка индикатора {indicator}. Всего страниц: {total_pages}")
                    break
                page += 1
                time.sleep(0.5)

            print(f"Завершен индикатор {indicator}. Всего записей: {len(all_data)}")

    except Exception as e:
        print(f"Ошибка при загрузке данных: {e}")
        return

    if not all_data:
        print("Не удалось получить данные")
        return

    # сохраняю данные в DataFrame
    df = pd.DataFrame(all_data)
    values_loaded = len(df)
    print(f'Получено {values_loaded} значений')

    # преобразую формат года в число
    df['year'] = df['year'].astype('int64')

    print("Сохраняем данные в Supabase ...")

    try:
        if not create_values_table(engine):
            return

        df.to_sql(
            name='worldbank_values',
            con=engine,
            if_exists='append', # добавляем данные в пустую таблицу
            index=False,
            method='multi'
        )
        print(f"Готово! Сохранено {len(df)} значений")

        # делаю запрос к созданной базе
        with engine.connect() as conn:
            result = conn.execute(text("""
                            SELECT column_name, data_type 
                            FROM information_schema.columns 
                            WHERE table_name = 'worldbank_values'
                            ORDER BY ordinal_position
                        """))
            # вывожу структуру таблицы
            print("Структура таблицы:")
            for column_name, data_type in result:
                print(f"   {column_name}: {data_type}")

            count_result = conn.execute(text("SELECT COUNT(*) FROM worldbank_values"))
            count = count_result.scalar()
            print(f"В таблице {count} записей")

    except Exception as e:
        print(f"Ошибка при сохранении: {e}")
    finally:
        engine.dispose()

# Этот блок выполняется только при прямом запуске скрипта
if __name__ == "__main__":
    main()

        