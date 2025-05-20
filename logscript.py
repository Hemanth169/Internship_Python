import os
from datetime import datetime

def write_log(message):
    # Define the log directory
    log_dir = "/Users/home/Desktop/oracle/Log"
    
    # Ensure the directory exists
    os.makedirs(log_dir, exist_ok=True)

    # Format date and time
    date = datetime.now().strftime('%Y%m%d')
    time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Define the log file path with .txt extension
    log_file = os.path.join(log_dir, f"log_{date}.txt")

    # Create the log entry
    log_entry = f"{time} - {message}\n"

    # Write the entry to the .txt file
    with open(log_file, "a") as f:
        f.write(log_entry)

