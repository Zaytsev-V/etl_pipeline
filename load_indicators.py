# load_indicators.py
import requests
import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

def create_indicators_table(engine, table_name='worldbank_indicators'):
    """Создает таблицу с нужными типами данных используя транзакцию"""

    drop_table_sql = f"DROP TABLE IF EXISTS {table_name};"
    create_table_sql = f"""
    CREATE TABLE {table_name} (
        indicator_id VARCHAR(50) PRIMARY KEY,
        indicator_name VARCHAR(300) NOT NULL,
        source_id VARCHAR(10),
        source_name VARCHAR(100),
        source_note TEXT,
        source_organization TEXT,
        topic_id VARCHAR(10),
        topic VARCHAR(100)
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

def get_topic_field(indicator, field):
    """проверяет, нет ли пустых значений в ключе topics"""
    topics = indicator.get('topics')
    if isinstance(topics, list) and len(topics) > 0 and isinstance(topics[0], dict):
        return topics[0].get(field)
    return None
    
def main():
    """Основная функция скрипта"""

    load_dotenv('wb.env')
    # проверяю наличие нужных переменных
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
    # подключаюсь к Всемирному Банку для загрузки данных по показателям

    all_indicators = []
    page = 1

    try:
        while True:
            url = "https://api.worldbank.org/v2/indicators"
            params = {'format': 'json', 'page': page, 'per_page': 5000}

            print(f"Страница {page}...")

            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            if not isinstance(data, list) or len(data) < 2 or not data[1]:
                break

            for indicator in data[1]:
                all_indicators.append({
                    'indicator_id': indicator.get('id'),
                    'indicator_name': indicator.get('name'),
                    'source_id': indicator.get('source', {}).get('id'),
                    'source_name': indicator.get('source', {}).get('value'),
                    'source_note': indicator.get('sourceNote'),
                    'source_organization': indicator.get('sourceOrganization'),
                    'topic_id': get_topic_field(indicator, 'id'),
                    'topic': get_topic_field(indicator, 'value')
                     })

            print(f"Обработано {len(data[1])} показателей")

            if page >= data[0].get('pages', 1): # проверка пагинации
                break
            page += 1

    except Exception as e:
        print(f"Ошибка при загрузке данных: {e}")
        return

    if not all_indicators:
        print("Не удалось загрузить данные")
        return

    # сохранаю данные в DataFrame
    df = pd.DataFrame(all_indicators)
    indicator_loaded = len(df)
    print(f"Всего получено: {indicator_loaded} показателей")

    # удаляю дубликаты по id
    df = df.drop_duplicates(subset='indicator_id')
    print(f'Осталось показателей после удаления дубликатов по идентификатору: {len(df)}')

    print("Сохраняем данные в Supabase ...")

    try:
        if not create_indicators_table(engine):
            return

        df.to_sql(
            name='worldbank_indicators',
            con=engine,
            if_exists='append',  # Добавляем записи в пустую таблицу
            index=False,
            method='multi'
        )

        print(f"Готово! Сохранено {len(df)} показателей")

        # делаю запрос к созданной базе
        with engine.connect() as conn:
            result = conn.execute(text("""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = 'worldbank_indicators'
                    ORDER BY ordinal_position
                """))
            # вывожу структуру таблицы
            print("Структура таблицы:")
            for column_name, data_type in result:
                print(f"   {column_name}: {data_type}")

            count_result = conn.execute(text("SELECT COUNT(*) FROM worldbank_indicators"))
            count = count_result.scalar()
            print(f"В таблице {count} записей")

    except Exception as e:
        print(f"Ошибка при сохранении: {e}")
    finally:
        engine.dispose()


# Этот блок выполняется только при прямом запуске скрипта
if __name__ == "__main__":
    main()