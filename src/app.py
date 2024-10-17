import sqlite3
import numpy as np
import pandas as pd
import datetime as dt
from sqlite3 import Connection
from contextlib import contextmanager, closing

# Write a function that connects to a sqlite database file
@contextmanager
def connect_to_database(db_file: str = None):
    """Yield a connection to the database."""
    # Default database file is `data/us_flights.db`
    db_file = db_file or 'data/us_flights.db'
    
    conn = None
    try:
        # Connect to the database file
        conn = sqlite3.connect(db_file)

        yield conn

    except sqlite3.Error as e:
        print(e)

    finally:
        if conn:
            conn.close()

def load_to_database(
        df: pd.DataFrame, table_name: str, schema: str = 'public', if_exists: str = 'replace', con: Connection = None,
    ):
    """Load data to a database table."""
    if not con:
        with connect_to_database() as conn:
            df.to_sql(
                table_name, schema=schema,
                if_exists=if_exists, index=False,
                con=conn,
            )
    else:
        df.to_sql(
            table_name, schema=schema,
            if_exists=if_exists, index=False,
            con=conn,
        )

def get_null_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Return a pandas.DataFrame of null values and their percentages."""
    null_ct = df.isna().sum()
    null_pct = (100*(null_ct/len(df))).round(1)

    return pd.concat([null_ct, null_pct], axis=1, keys=['null_count','null_pct'])

def drop_null_columns(df: pd.DataFrame, null_pct: float = .8) -> pd.DataFrame:
    """Drop columns with null percentage greater than `null_pct`."""
    # List columns above null threshold
    useless_cols = [col for col, val in (df.isna().sum()/len(df) > null_pct).items() if val == True]

    # Drop columns
    return df.drop(columns=useless_cols)

def convert_to_date(row: pd.Series) -> dt.date:
    return dt.date(row['YEAR'], row['MONTH'], row['DAY'])

def convert_to_datetime(row: pd.Series, column: str) -> dt.datetime:
    val = f'{int(row[column]):04d}' if row[column] not in [2400, 2400.0] else '0000'

    year, month, day = row['YEAR'], row['MONTH'], row['DAY']

    hour, minute = int(val[:2]), int(val[2:])

    return dt.datetime(year, month, day, hour, minute)



def main():
    with connect_to_database() as conn:
        flights_df = pd.read_csv('data/raw/flights.csv')
        airlines_df = pd.read_csv('data/raw/airlines.csv')
        airports_df = pd.read_csv('data/raw/airports.csv')

    # Drop unnamed columns
    flights_df.drop(columns=['Unnamed: 0.2', 'Unnamed: 0.1', 'Unnamed: 0'], inplace=True)
    
    # For columns with null percentages greater than 80%, replace with 0
    useless_cols = [col for col, val in (flights_df.isna().sum()/len(flights_df) > .8).items() if val == True]
    flights_df[useless_cols] = flights_df[useless_cols].replace(np.nan, 0,)
    
    # Drop rows w/ null values
    flights_df.dropna(inplace=True)

    # Create date column
    flights_df.insert(0, 'DATE', flights_df.apply(convert_to_date, axis=1))

    # # Flight timeline columns
    # timedelta_cols = ['TAXI_OUT', 'SCHEDULED_TIME',
    #                   'ELAPSED_TIME', 'AIR_TIME',
    #                   'DISTANCE', 'TAXI_IN',
    # ]
    # print(flights_df[timedelta_cols])
    # print()

    # timeline_cols = ['DEPARTURE_TIME', 'TAXI_OUT', 'WHEELS_OFF',
    #                  'WHEELS_ON', 'TAXI_IN', 'ARRIVAL_TIME',
    # ]
    
    # print(flights_df[timeline_cols])
    # print()
    # print([*flights_df.columns])
    # # print()
    # # print([*enumerate(flights_df.columns)])

    # print(airlines_df)
    # print()
    # print(airports_df)
    # print()
    # print(flights_df)

    flights_df.to_csv('data/csv/flights.csv', index=False)


if __name__ == '__main__':
    print()
    main()
    print()