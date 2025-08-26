import psycopg2

conn = psycopg2.connect(
    database="social_media_db",
    user="tushar",
    password="tushar",
    host="localhost",
    port="5432"
)
print("Connected successfully")
conn.close()
