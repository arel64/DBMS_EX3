from dataclasses import dataclass
import os
from typing import Union
import mysql.connector
from mysql.connector.pooling import PooledMySQLConnection
from mysql.connector.abstracts import MySQLConnectionAbstract
Connection = Union[PooledMySQLConnection, MySQLConnectionAbstract]


@dataclass
class Credentials:
    host: str
    user: str
    password: str
    database: str

    @staticmethod
    def from_text_file(file_path: str):
        try:
            with open(file_path, 'r') as f:
                lines = [line.strip() for line in f]
                if len(lines) < 4:
                    raise ValueError("Configuration file is incomplete.")
                return Credentials(
                    host=lines[0].split(':', 1)[1].strip(),
                    user=lines[1].split(':', 1)[1].strip(),
                    password=lines[2].split(':', 1)[1].strip(),
                    database=lines[3].split(':', 1)[1].strip()
                )
        except FileNotFoundError:
            raise ValueError(f"Configuration file {file_path} not found.")


def get_connection() -> Connection:
    credfile = f"{os.path.dirname(os.path.abspath(
        __file__))}/../documentation/mysql_and_user_password.txt"
    cred = Credentials.from_text_file(
        credfile)
    cnx: Connection = mysql.connector.connect(host=cred.host,
                                              user=cred.user, password=cred.password, database=cred.database, port=3305)
    return cnx


def create_database(cnx: Connection):
    cursor = cnx.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {cnx.database}")
    cursor.execute(f"USE {cnx.database}")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS movies(
            movie_id INT PRIMARY KEY,
            budget BIGINT,
            homepage VARCHAR(500),
            original_language VARCHAR(50),
            original_title VARCHAR(500),
            overview TEXT,
            popularity FLOAT,
            release_date DATE,
            revenue BIGINT,
            runtime INT,
            status VARCHAR(100),
            tagline VARCHAR(500),
            title VARCHAR(500),
            vote_average FLOAT,
            vote_count INT,
            FULLTEXT(overview, tagline)
        )
    """)
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS genres(genre_id INT PRIMARY KEY,name VARCHAR(200))")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS movie_genres(
            movie_id INT,
            genre_id INT,
            PRIMARY KEY(movie_id, genre_id),
            FOREIGN KEY(movie_id) REFERENCES movies(movie_id),
            FOREIGN KEY(genre_id) REFERENCES genres(genre_id)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS production_companies(
            company_id INT PRIMARY KEY,
            name VARCHAR(500)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS movie_companies(
            movie_id INT,
            company_id INT,
            PRIMARY KEY(movie_id, company_id),
            FOREIGN KEY(movie_id) REFERENCES movies(movie_id),
            FOREIGN KEY(company_id) REFERENCES production_companies(company_id)
        )
    """)
    cursor.execute("CREATE INDEX idx_revenue ON movies(revenue)")
    cursor.execute("CREATE INDEX idx_popularity ON movies(popularity)")
    cnx.commit()  # Commit the transactions
    cursor.close()
    cnx.close()


if __name__ == "__main__":
    cnx: Connection = get_connection()
    create_database(cnx)
