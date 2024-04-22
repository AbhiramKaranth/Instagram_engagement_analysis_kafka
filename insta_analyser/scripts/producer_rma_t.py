import json
import time
from datetime import datetime
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import sqlite3
import os

# Define the paths for the databases
original_db_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'original_databases'))
original_db_path = os.path.join(original_db_directory, 'RMA_original_data.db')  # Path for reading existing data

simulated_db_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'database'))
simulated_db_path = os.path.join(simulated_db_directory, 'RMA.db')  # Path for storing streamed data

# Ensure database directories exist
if not os.path.exists(simulated_db_directory):
    os.makedirs(simulated_db_directory)

def init_db(db_path):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
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

def stream_data_from_db(topic_name, read_db_path, write_db_path, bootstrap_servers='localhost:9092'):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    conn_read = sqlite3.connect(read_db_path)
    cursor = conn_read.cursor()
    cursor.execute("SELECT timestamp, post_id, likes, comments, followers, team FROM posts")
    rows = cursor.fetchall()

    for row in rows:
        post_data = {
            "timestamp": row[0],
            "post_id": row[1],
            "likes": row[2],
            "comments": row[3],
            "followers": row[4],
            "team": row[5]
        }
        producer.send(topic_name, value=post_data)
        insert_post(write_db_path, post_data)
        print(f"Post data sent to {topic_name} and stored in {write_db_path}: {post_data}")
        time.sleep(0.1)  # Simulate data flow
    producer.close()
    conn_read.close()

if __name__ == "__main__":
    topic_name = "realmadrid"
    bootstrap_servers = 'localhost:9092'
    delete_topic(topic_name, bootstrap_servers)
    create_topic(topic_name, bootstrap_servers)
    init_db(simulated_db_path)  # Initialize the database for storing streamed data
    stream_data_from_db(topic_name, original_db_path, simulated_db_path, bootstrap_servers)
