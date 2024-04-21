import subprocess

scripts = ["producer_rma.py", "producer_fcb.py", "producer_val.py", "consumer.py"]

processes = []

for script in scripts:
    processes.append(subprocess.Popen(['python3', script]))

# Wait for all processes to complete
for process in processes:
    process.wait()

print("All scripts have completed.")
