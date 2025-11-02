from sqlalchemy import create_engine, text as sql_text, Table, MetaData
import pandas as pd
import os
import sys
from dotenv import load_dotenv

# Ensure shared api_scripts package is importable when executed by Airflow
API_SCRIPTS_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '../api_scripts'))
if API_SCRIPTS_PATH not in sys.path:
    sys.path.insert(0, API_SCRIPTS_PATH)

from fetch_bettingpros import get_bettingpros_df

load_dotenv()
db_username = os.getenv("DB_USERNAME")
db_password = os.getenv("DB_PASSWORD")

connection_string = (
    f'postgresql+psycopg2://{db_username}:{db_password}@localhost:5432/nba_deeplearning'
)

db_eng = create_engine(connection_string)
print("Successfully created db engine.")

def append_to_postgres(df, table_name):
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=db_eng)

    with db_eng.begin() as conn:  # transaction automatically commits
        for row in df.to_dict(orient='records'):
            conn.execute(table.insert(), row)
            
def check_if_table_exists(table_name):
    query = sql_text(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name=:table)"
    )
    with db_eng.connect() as conn:
        result = conn.execute(query, {"table": table_name})
        return result.scalar()

def check_df_columns(df):
    table_columns = ['player_name', 'team', 'sportsbook', 'line_score', 'game_start', 'time_scraped', 'opponent_team']
    df_columns = df.columns.tolist()
    return all(col in df_columns for col in table_columns) and len(df_columns) == len(table_columns)

def __main__(table_name):
    if not check_if_table_exists(table_name):
        raise Exception(f"Table {table_name} does not exist.")

    dfs = [get_bettingpros_df()]
    for df in dfs:
        if df.empty:
            print(f"Skipping empty DataFrame...")
            continue
        if not check_df_columns(df):
            raise Exception(f"df columns do not match WITH {table_name} attributes.")
        append_to_postgres(df, table_name)

        print(f"Successfully appended dataframe of length {len(df)} to {table_name} in postgres.")


if __name__ == "__main__":
    __main__('player_lines')
