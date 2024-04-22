import subprocess

scripts = ["producer_rma_t.py", "producer_fcb_t.py", "producer_val_t.py", "consumer.py"]

processes = []

for script in scripts:
    processes.append(subprocess.Popen(['python3', script]))

# Wait for all processes to complete
for process in processes:
    process.wait()

print("All scripts have completed.")
