from queries_db_script import query_1, query_2, query_3, query_4, query_5
from create_db_script import get_connection


def main():
    conn = get_connection()
    query_1_ans = query_1(conn, 'alien')
    query_2_ans = query_2(conn, 'love')
    query_3_ans = query_3(conn)
    query_4_ans = query_4(conn)
    query_5_ans = query_5(conn)
    for idx,ans in enumerate([query_1_ans,query_2_ans,query_3_ans,query_4_ans,query_5_ans]):
        print(f"Query {idx+1} ans: {ans} \n")
    conn.close()


if __name__ == "__main__":
    main()
