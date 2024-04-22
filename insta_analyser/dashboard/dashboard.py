import streamlit as st
import pandas as pd
import sqlite3
import os
import time

# Define the path to the database directory
db_directory = os.path.join(os.path.dirname(__file__), '..', 'database')

# Database options
db_options = {
    'FC Barcelona': ('FCB.db', 'posts'),
    'Real Madrid': ('RMA.db', 'posts'),
    'Valencia CF': ('VAL.db', 'posts'),
    'Engagement Data': ('engagement_data.db', 'engagement_data')  # Correct table name for engagement data
}

# Function to load data from the selected database and table
def load_data(db_filename, table_name):
    conn = sqlite3.connect(os.path.join(db_directory, db_filename))
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Streamlit sidebar for database selection
selected_db_key = st.sidebar.selectbox('Select a Database', list(db_options.keys()))
selected_db_name, selected_table_name = db_options[selected_db_key]

# Display the selected database contents
st.title(f"Database Content: {selected_db_key}")
data_load_state = st.text('Loading data...')
data = load_data(selected_db_name, selected_table_name)
data_load_state.text("Data loaded!")
st.write(data)

# Auto-refresh setup
refresh_rate = st.sidebar.slider('Refresh rate in seconds', min_value=1, max_value=10, value=2)
st.sidebar.text("Note: Lower refresh rates may affect performance.")

# Loop to update the page with new data
while True:
    time.sleep(refresh_rate)
    data = load_data(selected_db_name, selected_table_name)
    st.experimental_rerun()
