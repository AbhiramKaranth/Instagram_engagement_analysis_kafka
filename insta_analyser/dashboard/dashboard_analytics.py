import os
import sqlite3
import pandas as pd
import datetime
import numpy as np
import streamlit as st

# Path to the database directory
DATABASE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'database')

def fetch_streaming_results(team):
    db_path = os.path.join(DATABASE_DIR, 'engagement_data.db')
    conn = sqlite3.connect(db_path)
    query = f"""
    SELECT window_start, window_end, engagement_rate
    FROM engagement_data
    WHERE team = '{team}'
    ORDER BY window_start
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def calculate_batch_engagement(team_db, window_start, window_end):
    db_path = os.path.join(DATABASE_DIR, team_db)
    conn = sqlite3.connect(db_path)
    window_start_dt = datetime.datetime.strptime(window_start, '%Y-%m-%d %H:%M:%S')
    window_end_dt = datetime.datetime.strptime(window_end, '%Y-%m-%d %H:%M:%S')
    query = f"""
    SELECT likes, comments, followers
    FROM posts
    WHERE datetime(substr(timestamp, 1, 19)) BETWEEN datetime('{window_start_dt}') AND datetime('{window_end_dt}')
    """
    df = pd.read_sql(query, conn)
    conn.close()
    if df.empty:
        return 0  # No data found
    interactions = df['likes'].sum() + df['comments'].sum()
    avg_followers = df['followers'].mean() if not df['followers'].empty else 0
    engagement_rate = 100 * interactions / avg_followers if avg_followers > 0 else 0
    return engagement_rate

def get_comparison_data(team):
    team_db = team_db_mapping[team]
    streaming_data = fetch_streaming_results(team)
    batch_engagement_rates = []
    errors = []

    for _, row in streaming_data.iterrows():
        batch_rate = calculate_batch_engagement(team_db, row['window_start'], row['window_end'])
        batch_engagement_rates.append(batch_rate)
        if row['engagement_rate'] > 0:
            abs_error = abs(row['engagement_rate'] - batch_rate)
            rel_error = abs_error / row['engagement_rate']   # Relative error as percentage
        else:
            abs_error = rel_error = np.nan  # Avoid division by zero
        errors.append({'Absolute Error': abs_error, 'Relative Error': rel_error})

    streaming_data['Batch Engagement Rate'] = batch_engagement_rates
    error_df = pd.DataFrame(errors)
    comparison_df = pd.concat([streaming_data, error_df], axis=1)

    # Calculate overall metrics
    valid_data = comparison_df.dropna()
    mae = np.mean(valid_data['Absolute Error'])
    rmse = np.sqrt(np.mean(valid_data['Absolute Error']**2))
    mape = np.mean(valid_data['Relative Error'])  # MAPE already as percentage
    total_followers = valid_data['Batch Engagement Rate'].sum()
    overall_batch_engagement = total_followers  / len(valid_data) if len(valid_data) > 0 else 0  # Multiply by 100 to convert to percentage

    return comparison_df, mae, rmse, mape, overall_batch_engagement

team_db_mapping = {
    'RMA': 'RMA.db',
    'FCB': 'FCB.db',
    'VAL': 'VAL.db'
}

st.title("Streaming vs Batch Engagement Rate Comparison")
st.sidebar.header("Settings")

selected_team = st.sidebar.selectbox("Select Team", options=list(team_db_mapping.keys()))
if st.sidebar.button("Refresh Data"):
    st.experimental_rerun()

comparison_df, mae, rmse, mape, overall_batch_engagement = get_comparison_data(selected_team)
st.dataframe(comparison_df[['window_start', 'window_end', 'engagement_rate', 'Batch Engagement Rate', 'Absolute Error', 'Relative Error']])

st.subheader("Overall Evaluation Metrics")
st.write(f"Mean Absolute Error (MAE): {mae:.2f}")
st.write(f"Root Mean Squared Error (RMSE): {rmse:.2f}")
st.write(f"Mean Absolute Percentage Error (MAPE): {mape:.2f}%")
st.write(f"Overall Batch Engagement Rate: {overall_batch_engagement:.2f}%")
