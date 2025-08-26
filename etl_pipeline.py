import pandas as pd
import psycopg2
import random
import re
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk
nltk.download('vader_lexicon')

def extract_hashtags(text):
    return re.findall(r"#(\w+)", str(text))

def connect_db():
    return psycopg2.connect(
        database="social_media_db",
        user="tushar",
        password="tushar",
        host="localhost",
        port="5432"
    )

df = pd.read_csv("data/social_media_data.csv")

# Sentiment analysis using VADER
sid = SentimentIntensityAnalyzer()
df['sentiment_score'] = df['content'].apply(lambda x: sid.polarity_scores(str(x))['compound'])
df['sentiment_label'] = df['sentiment_score'].apply(
    lambda x: 'positive' if x > 0.2 else 'negative' if x < -0.2 else 'neutral'
)
df['hashtags'] = df['content'].apply(extract_hashtags)

conn = connect_db()
cursor = conn.cursor()

# Insert platforms
for name in df['platform'].unique():
    cursor.execute("INSERT INTO platforms (name, source) VALUES (%s, %s) ON CONFLICT DO NOTHING", (name, 'Mock'))

# Insert users
for i, username in enumerate(df['username'].unique()):
    user_id = f"u{i+1:03}"
    cursor.execute("""
        INSERT INTO users (user_id, username, location, followers, verified, account_created)
        VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
    """, (user_id, username, 'India', random.randint(1000, 10000), random.choice([True, False]), '2022-01-01'))

# Insert posts, engagement, sentiment, hashtags
for _, row in df.iterrows():
    cursor.execute("SELECT platform_id FROM platforms WHERE name = %s", (row['platform'],))
    platform_id = cursor.fetchone()[0]

    cursor.execute("SELECT user_id FROM users WHERE username = %s", (row['username'],))
    user_id = cursor.fetchone()[0]

    cursor.execute("""
        INSERT INTO posts (user_id, platform_id, content, timestamp, post_type, language, location)
        VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING post_id
    """, (user_id, platform_id, row['content'], row['timestamp'], 'original', 'en', 'India'))
    post_id = cursor.fetchone()[0]

    cursor.execute("INSERT INTO engagement (post_id, likes, comments, shares) VALUES (%s, %s, %s, %s)",
                   (post_id, row['likes'], row['comments'], row['shares']))

    cursor.execute("INSERT INTO sentiment (post_id, polarity, label) VALUES (%s, %s, %s)",
                   (post_id, row['sentiment_score'], row['sentiment_label']))

    for tag in row['hashtags']:
        cursor.execute("INSERT INTO hashtags (post_id, hashtag) VALUES (%s, %s)", (post_id, tag))

conn.commit()
cursor.close()
conn.close()
print("ETL pipeline executed successfully!")
