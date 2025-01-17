from create_db_script import get_connection
import mysql.connector

import csv
import json


def insert_data(csv_file):
    cnx = get_connection()
    cursor = cnx.cursor()
    with open(csv_file, encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            movie_id = int(row[3])
            budget = int(row[0]) if row[0].isdigit() else 0
            homepage = row[2]
            original_language = row[5]
            original_title = row[6]
            overview = row[7]
            popularity = float(row[8]) if row[8] else 0
            release_date = row[11]
            revenue = int(row[12]) if row[12].isdigit() else 0
            runtime = int(row[13]) if row[13].isdigit() else 0
            status = row[15]
            tagline = row[16]
            title = row[17]
            vote_average = float(row[18]) if row[18] else 0
            vote_count = int(row[19]) if row[19].isdigit() else 0
            cursor.execute("INSERT IGNORE INTO movies(movie_id,budget,homepage,original_language,original_title,overview,popularity,release_date,revenue,runtime,status,tagline,title,vote_average,vote_count) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                           (movie_id, budget, homepage, original_language, original_title, overview, popularity, release_date, revenue, runtime, status, tagline, title, vote_average, vote_count))
            genres_data = json.loads(row[1]) if row[1] else []
            for g in genres_data:
                cursor.execute(
                    "INSERT IGNORE INTO genres(genre_id,name) VALUES(%s,%s)", (g['id'], g['name']))
                cursor.execute(
                    "INSERT IGNORE INTO movie_genres(movie_id,genre_id) VALUES(%s,%s)", (movie_id, g['id']))
            companies_data = json.loads(row[9]) if row[9] else []
            for c in companies_data:
                cursor.execute(
                    "INSERT IGNORE INTO production_companies(company_id,name) VALUES(%s,%s)", (c['id'], c['name']))
                cursor.execute(
                    "INSERT IGNORE INTO movie_companies(movie_id,company_id) VALUES(%s,%s)", (movie_id, c['id']))
    cnx.commit()
    cursor.close()
    cnx.close()


if __name__ == "__main__":
    insert_data("movies.csv")
