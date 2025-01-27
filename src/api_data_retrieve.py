import pandas as pd
import json
from tqdm import tqdm
from create_db_script import get_connection


def insert_data(csv_file):
    cnx = get_connection()
    cursor = cnx.cursor()
    df = pd.read_csv(csv_file, encoding='utf-8')
    debug_mode = True
    for i, row in tqdm(df.iterrows(), total=len(df)):
        if debug_mode and i % 100 != 0:
            continue
        movie_id = int(row['id']) if pd.notna(
            row['id']) and str(row['id']).isdigit() else 0
        budget = int(row['budget']) if pd.notna(
            row['budget']) and str(row['budget']).isdigit() else 0
        homepage = row['homepage'] if pd.notna(row['homepage']) else ''
        original_language = row['original_language'] if pd.notna(
            row['original_language']) else ''
        original_title = row['original_title'] if pd.notna(
            row['original_title']) else ''
        overview = row['overview'] if pd.notna(row['overview']) else ''
        popularity = float(row['popularity']) if pd.notna(
            row['popularity']) else 0
        release_date = row['release_date'] if pd.notna(
            row['release_date']) else None
        revenue = int(row['revenue']) if pd.notna(
            row['revenue']) and str(row['revenue']).isdigit() else 0
        runtime = int(float(row['runtime'])) if pd.notna(row['runtime']) else 0
        status = row['status'] if pd.notna(row['status']) else ''
        tagline = row['tagline'] if pd.notna(row['tagline']) else ''
        title = row['title'] if pd.notna(row['title']) else ''
        vote_average = float(row['vote_average']) if pd.notna(
            row['vote_average']) else 0
        vote_count = int(row['vote_count']) if pd.notna(
            row['vote_count']) and str(row['vote_count']).isdigit() else 0
        cursor.execute("INSERT IGNORE INTO movies(movie_id,budget,homepage,original_language,original_title,overview,popularity,release_date,revenue,runtime,status,tagline,title,vote_average,vote_count) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                       (movie_id, budget, homepage, original_language, original_title, overview, popularity, release_date, revenue, runtime, status, tagline, title, vote_average, vote_count))
        genres_data = []
        if pd.notna(row['genres']):
            try:
                genres_data = json.loads(row['genres'])
            except:
                pass
        for g in genres_data:
            cursor.execute(
                "INSERT IGNORE INTO genres(genre_id,name) VALUES(%s,%s)", (g['id'], g['name']))
            cursor.execute(
                "INSERT IGNORE INTO movie_genres(movie_id,genre_id) VALUES(%s,%s)", (movie_id, g['id']))
        companies_data = []
        if pd.notna(row['production_companies']):
            try:
                companies_data = json.loads(row['production_companies'])
            except:
                pass
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
