def query_1(conn, text):
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM movies WHERE overview LIKE CONCAT('%', %s, '%')", (text,))
    return cursor.fetchall()

def query_2(conn, text):
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM movies WHERE tagline LIKE CONCAT('%', %s, '%')", (text,))
    return cursor.fetchall()

def query_3(conn):
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT genres.name,COUNT(movie_genres.movie_id) AS count_movies FROM genres JOIN movie_genres ON genres.genre_id=movie_genres.genre_id GROUP BY genres.genre_id ORDER BY count_movies DESC")
    return cursor.fetchall()

def query_4(conn):
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT title,release_date,revenue FROM movies WHERE revenue = (SELECT MAX(revenue) FROM movies)")
    return cursor.fetchall()

def query_5(conn):
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT title FROM movies WHERE EXISTS (SELECT * FROM movie_genres WHERE movie_genres.movie_id = movies.movie_id AND movie_genres.genre_id = 28)")
    return cursor.fetchall()
