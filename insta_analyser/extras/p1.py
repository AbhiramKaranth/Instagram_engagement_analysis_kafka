import instaloader
import sqlite3
import os
from datetime import datetime

def init_db(db_path):
    # Ensure directory exists for the database
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    # Drop the existing table if it exists and then create a new one
    c.execute('DROP TABLE IF EXISTS posts')
    c.execute('''CREATE TABLE posts
                 (timestamp TEXT, post_id TEXT, likes INTEGER, comments INTEGER, followers INTEGER, team TEXT)''')
    conn.commit()
    conn.close()

def insert_post(db_path, post_data):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute('INSERT INTO posts (timestamp, post_id, likes, comments, followers, team) VALUES (?, ?, ?, ?, ?, ?)',
              (post_data['timestamp'], post_data['post_id'], post_data['likes'], post_data['comments'], post_data['followers'], post_data['team']))
    conn.commit()
    conn.close()

def fetch_and_store_data(username, db_path):
    L = instaloader.Instaloader()
    profile = instaloader.Profile.from_username(L.context, username)
    followers = profile.followers
    posts = profile.get_posts()
    count = 0
    for post in posts:
        if count >= 700:
            break
        post_data = {
            "timestamp": datetime.now().isoformat(),
            "post_id": post.shortcode,
            "likes": post.likes,
            "comments": post.comments,
            "followers": followers,
            "team": "VAL"
        }
        insert_post(db_path, post_data)
        print(f"Post data stored in {db_path}: {post_data}")
        count += 1

if __name__ == "__main__":
    db_name = "VAL_original_data.db"
    # Set the database path to be within the 'original_databases' folder one level up from the 'extras' folder
    db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'original_databases', db_name))
    init_db(db_path)
    username = "valenciacf"
    fetch_and_store_data(username, db_path)
