import psycopg2
import pandas as pd

def create_database():
    conn = psycopg2.connect("host = 127.0.0.1 dbname=postgres user=postgres password=root")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    #cur.execute("DROP DATABASE NBA")
    cur.execute("CREATE DATABASE IF NOT EXIST NBA")
    conn.close()

    conn = psycopg2.connect("host = 127.0.0.1 dbname=NBA user=postgres password=mikosa")
    cur = conn.cursor()
    
    return conn, cur

def create_tables(cur, conn, create_table_queries):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def drop_tables(cur, conn, drop_table_queries):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def insert_data(cur, conn, data, data_table_insert):
    for i, row in data.iterrows():
        cur.execute(data_table_insert, list(row))


def run(create_table_queries, data_table_insert, path): 
    conn, cur = create_database()
    tables_creation = create_tables(conn, cur, create_table_queries)
    data = pd.read_csv(path)
    table_insert = insert_data(cur, conn, data, data_table_insert)