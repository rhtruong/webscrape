import psycopg2
from sqlalchemy import create_engine, text as sql_text
import pandas as pd
import os
from dotenv import load_dotenv

from fetch_bettingpros import get_bettingpros_df
from fetch_prizepicks import get_prizepicks_df
# from fetch_draftedge import get_draftedge_df


load_dotenv()
db_username = os.getenv("DB_USERNAME")
db_password = os.getenv("DB_PASSWORD")

db_eng = create_engine(
    f'postgresql+psycopg2://{db_username}:{db_password}@localhost:5432/nba_deeplearning',
    isolation_level='SERIALIZABLE'
)
print("Successfully created db engine.")


def append_to_postgres(df, table_name):
    df.to_sql(
        table_name,
        db_eng,
        if_exists='append',
        index=False,
        chunksize=1000         # for large DataFrames
    )    

def check_if_table_exists(table_name):
    query = sql_text(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name=:table)"
    )
    with db_eng.connect() as conn:
        return conn.execute(query, {"table": table_name}).scalar()

def check_df_columns(df):
    table_columns = ['player_name', 'team', 'sportsbook', 'line_score', 'game_start', 'time_scraped', 'opponent_team']
    df_columns = df.columns.tolist()
    return all(col in df_columns for col in table_columns) and len(df_columns) == len(table_columns)

def __main__(table_name):
    if not check_if_table_exists(table_name):
        raise Exception(f"Table {table_name} does not exist.")

    # Fetch latest DataFrames inside migration
    dfs = [get_bettingpros_df(), get_prizepicks_df()]  # add draftedge here if needed
    for df in dfs:
        if df.empty:
            print(f"Skipping empty DataFrame...")
            continue
        if not check_df_columns(df):
            raise Exception(f"df columns do not match WITH {table_name} attributes.")
        append_to_postgres(df, table_name)

    print(f"Successfully appended dataframes to {table_name} in postgres.")


if __name__ == "__main__":
    __main__('player_lines')