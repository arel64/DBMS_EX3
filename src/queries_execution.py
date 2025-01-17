import mysql.connector
from queries_db_script import query_1, query_2, query_3, query_4, query_5
from create_db_script import get_connection


def main():
    conn = get_connection()
    print(query_1(conn, 'alien'))
    print(query_2(conn, 'love'))
    print(query_3(conn))
    print(query_4(conn))
    print(query_5(conn))
    conn.close()


if __name__ == "__main__":
    main()
