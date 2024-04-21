import threading
import subprocess

def run_streamlit(script, port):
    subprocess.call(['streamlit', 'run', script, '--server.port', str(port)])

# Define the scripts and their respective ports
apps = [
    ("dashboard.py", 8501),
    ("dashboard_analytics.py", 8502)
]

threads = []

# Create and start a new thread for each application
for app, port in apps:
    thread = threading.Thread(target=run_streamlit, args=(app, port))
    thread.start()
    threads.append(thread)

# Optional: wait for all threads to complete
for thread in threads:
    thread.join()

print("All Streamlit apps are running.")
