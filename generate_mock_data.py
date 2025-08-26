import pandas as pd
import random
from datetime import datetime, timedelta
import os

os.makedirs("data", exist_ok=True)

platforms = ['Twitter', 'YouTube', 'Instagram']
users = [
    '@rahul_dev', '@tech_guru', '@vlog_queen', '@chai_coder', '@fitness_bhai',
    '@bollywood_buff', '@startup_singh', '@cricket_fan', '@travel_diaries', '@foodie_rani'
]
hashtags_pool = ['#AI', '#Tech', '#TravelIndia', '#Bollywood', '#Startup', '#Cricket', '#Foodie', '#FitnessGoals']

def generate_post():
    topic = random.choice(['AI', 'travel', 'movies', 'food', 'fitness', 'startups', 'cricket'])
    tone = random.choice([
        "Absolutely loved", "Totally disappointed with", "Excited to explore",
        "Frustrated by", "Inspired by", "Can’t stand", "Obsessed with"
    ])
    hashtags = ' '.join(random.sample(hashtags_pool, k=random.randint(1, 3)))
    return f"{tone} {topic} content today! {hashtags}"

data = []
for _ in range(200):
    platform = random.choice(platforms)
    username = random.choice(users)
    content = generate_post()
    timestamp = datetime.now() - timedelta(days=random.randint(0, 30), hours=random.randint(0, 23))
    likes = random.randint(10, 1000)
    comments = random.randint(0, 300)
    shares = random.randint(0, 500)
    data.append([platform, username, content, timestamp, likes, comments, shares])

df = pd.DataFrame(data, columns=['platform', 'username', 'content', 'timestamp', 'likes', 'comments', 'shares'])
df.to_csv('data/social_media_data.csv', index=False)
print("✅ social_data.csv generated successfully!")