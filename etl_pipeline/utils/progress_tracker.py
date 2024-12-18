import json

progress_file = "progress_tracker.json"

def get_last_batch(table_name):
    try:
        with open(progress_file, "r") as file:
            progress = json.load(file)
            return progress.get(table_name, 0)
    except FileNotFoundError:
        return 0

def update_last_batch(table_name, batch):
    try:
        with open(progress_file, "r") as file:
            progress = json.load(file)
    except FileNotFoundError:
        progress = {}

    progress[table_name] = batch

    with open(progress_file, "w") as file:
        json.dump(progress, file)
