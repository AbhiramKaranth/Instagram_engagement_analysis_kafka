import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import sqlite3
import os

# Set the path for the database directory relative to this script's location
db_directory = os.path.join(os.path.dirname(__file__), '..', 'database')
if not os.path.exists(db_directory):
    os.makedirs(db_directory)
db_path = os.path.join(db_directory, 'FCB.db')

def init_db(db_name):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute('DROP TABLE IF EXISTS posts')
    c.execute('''CREATE TABLE posts
                 (timestamp TEXT, post_id TEXT, likes INTEGER, comments INTEGER, followers INTEGER, team TEXT)''')
    conn.commit()
    conn.close()

def insert_post(db_name, post_data):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute('INSERT INTO posts (timestamp, post_id, likes, comments, followers, team) VALUES (?, ?, ?, ?, ?, ?)',
              (post_data['timestamp'], post_data['post_id'], post_data['likes'], post_data['comments'], post_data['followers'], post_data['team']))
    conn.commit()
    conn.close()

def create_topic(topic_name, bootstrap_servers='localhost:9092'):
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    topic_list = [NewTopic(name=topic_name, num_partitions=1, replication_factor=1)]
    try:
        admin_client.create_topics(new_topics=topic_list)
        print(f"Topic '{topic_name}' created.")
    except Exception as e:
        print(f"Topic '{topic_name}' creation failed: {e}")

def delete_topic(topic_name, bootstrap_servers='localhost:9092'):
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    try:
        admin_client.delete_topics(topics=[topic_name])
        print(f"Topic '{topic_name}' deleted successfully.")
    except Exception as e:
        print(f"Failed to delete topic '{topic_name}': {e}")

def simulate_data(username, topic_name, db_name, post_count=1500, bootstrap_servers='localhost:9092'):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    followers = 127000000  # Approximation of followers
    for i in range(post_count):
        post_data = {
            "timestamp": datetime.now().isoformat(),
            "post_id": f"post_{i}",
            "likes": random.randint(80000, 120000),
            "comments": random.randint(500, 2000),
            "followers": followers,
            "team": "FCB"
        }
        producer.send(topic_name, value=post_data)
        insert_post(db_name, post_data)
        print(f"Simulated post data sent to {topic_name} and stored in {db_name}: {post_data}")
        time.sleep(0.3)  # Simulate data flow
    producer.close()

if __name__ == "__main__":
    db_name = "FCB.db"
    topic_name = "fcbarcelona"
    bootstrap_servers = 'localhost:9092'
    init_db(db_path)
    delete_topic(topic_name, bootstrap_servers)
    create_topic(topic_name, bootstrap_servers)
    simulate_data("fcbarcelona", topic_name, db_path)
