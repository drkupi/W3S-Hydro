from typing import Tuple
import psycopg2
from io import StringIO
import psycopg2.extras as extras


def run_w3s_query(query: str, params: dict=None) -> Tuple[list, list]:
    """Run query in the World Climate Database

    Args:
        query (str): Query string.
        params (dict, optional): Query parameters. Defaults to None.

    Returns:
        Tuple[list, list]: Return items of query
    """  
    # Connect to postgres database
    db_setting = {
        "host": "localhost",
        "name": "postgres",
        "user": "postgres",
        "password": "Drkupi2019!"
    }
    conn = psycopg2.connect(host=db_setting['host'],
                            database=db_setting['name'],
                            user=db_setting['user'],
                            password=db_setting['password'],
                            port=6543)
    if 'SELECT' in query:
        names, records = run_query(conn, query, params=params)
        conn.close()
        return names, records
    else:
        affected = run_query(conn, query, params=params)
        conn.close()
        return affected, affected


def run_query(conn, query, params=None):
    """Run an SQL query

    :param query: SQL Query
    :type query: str

    :return: Records after running query
    :rtype: list

    """
    try:
        with conn.cursor() as cur:
            if 'SELECT' in query:
                if params is not None:
                    cur.execute(query, params)
                else:
                    cur.execute(query)
                records = cur.fetchall()
                names = [x[0] for x in cur.description]
                cur.close()
                conn.close()
                return names, records
            else:
                if params is not None:
                    _ = cur.execute(query, params)
                else:
                    _ = cur.execute(query)
                conn.commit()
                affected = f"{cur.rowcount} rows affected."
                cur.close()
                conn.close()
                return affected
    except psycopg2.DatabaseError as e:
        print(e)


def to_sql(engine, df, table_name, schema_name,
           sep='\t', encoding='utf8'):
    """Fast method for exporting large dataframe to DB

    :param engine: SQLAlchemy engine object
    :type engine: object
    :param df: Dataframe to export
    :type df: pandas.df
    :param table_name: Name of output database table
    :type table_name: str
    :param schema_name: Name of output db schema
    :type schema_name: str
    :param sep: Separator for data
    :type sep: str
    :param encoding: Encoding type
    :type encoding: str

    """
    
    # Prepare data
    output = StringIO()
    df.to_csv(output, sep=sep, header=False,
              encoding=encoding, index=False)
    output.seek(0)

    # Insert data
    connection = engine.raw_connection()
    cursor = connection.cursor()
    table = schema_name + "." + table_name
    cursor.copy_from(output, table, sep=sep, null='')
    connection.commit()
    cursor.close()
    connection.close()
    engine.dispose()