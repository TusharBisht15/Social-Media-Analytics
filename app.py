import streamlit as st
import pandas as pd
import psycopg2

st.set_page_config(page_title="Tushar's Social Media Dashboard", layout="wide")
st.title("📊 Tushar's Social Media Analytics")

conn = psycopg2.connect(
    database="social_media_db",
    user="tushar",
    password="tushar",
    host="localhost",
    port="5432"
)

platforms_df = pd.read_sql("SELECT DISTINCT name FROM platforms", conn)
platform_list = platforms_df['name'].tolist()
platform_list.insert(0, "All Platforms")
selected_platform = st.selectbox("Choose a platform", platform_list)

if selected_platform == "All Platforms":
    query = """SELECT posts.post_id, platforms.name AS platform, users.username, posts.content, posts.timestamp,
                      engagement.likes, engagement.comments, engagement.shares,
                      sentiment.label, sentiment.polarity
               FROM posts
               JOIN platforms ON posts.platform_id = platforms.platform_id
               JOIN users ON posts.user_id = users.user_id
               JOIN engagement ON posts.post_id = engagement.post_id
               JOIN sentiment ON posts.post_id = sentiment.post_id"""
    df = pd.read_sql(query, conn)
else:
    query = """SELECT posts.post_id, platforms.name AS platform, users.username, posts.content, posts.timestamp,
                      engagement.likes, engagement.comments, engagement.shares,
                      sentiment.label, sentiment.polarity
               FROM posts
               JOIN platforms ON posts.platform_id = platforms.platform_id
               JOIN users ON posts.user_id = users.user_id
               JOIN engagement ON posts.post_id = engagement.post_id
               JOIN sentiment ON posts.post_id = sentiment.post_id
               WHERE platforms.name = %s"""
    df = pd.read_sql(query, conn, params=(selected_platform,))

col1, col2 = st.columns(2)

with col1:
    st.subheader(f"Sentiment Distribution for {selected_platform}")
    st.bar_chart(df['label'].value_counts())

with col2:
    st.subheader(f"Engagement Overview on {selected_platform}")
    st.metric("Total Likes", df['likes'].sum())
    st.metric("Total Comments", df['comments'].sum())
    st.metric("Total Shares", df['shares'].sum())


st.subheader("👤 User-Level Analytics")

user_query = """
    SELECT users.username,
           COUNT(posts.post_id) AS total_posts,
           SUM(engagement.likes) AS total_likes,
           SUM(engagement.comments) AS total_comments,
           SUM(engagement.shares) AS total_shares,
           SUM(CASE WHEN sentiment.label = 'positive' THEN 1 ELSE 0 END) AS positive_posts,
           SUM(CASE WHEN sentiment.label = 'negative' THEN 1 ELSE 0 END) AS negative_posts,
           SUM(CASE WHEN sentiment.label = 'neutral' THEN 1 ELSE 0 END) AS neutral_posts
    FROM posts
    JOIN users ON posts.user_id = users.user_id
    JOIN engagement ON posts.post_id = engagement.post_id
    JOIN sentiment ON posts.post_id = sentiment.post_id
    GROUP BY users.username
    ORDER BY total_posts DESC, total_likes DESC;
"""

user_df = pd.read_sql(user_query, conn)
st.dataframe(user_df)

# Top Hashtags
st.subheader("📌 Top Hashtags")
hashtags_df = pd.read_sql("""
                          SELECT hashtag, COUNT(*) as freq
                          FROM hashtags
                          GROUP BY hashtag
                          ORDER BY freq DESC LIMIT 10
                          """, conn)
st.dataframe(hashtags_df)

# Recent Posts
st.subheader("📝 Recent Posts")
st.dataframe(
    df[['platform', 'username', 'content', 'timestamp', 'label']].sort_values(by='timestamp', ascending=False).head(10))

# Close connection
conn.close()