import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), 'monitoring'))
import json
import time
import argparse
from log_helper import write_counter_file
from concurrent.futures import ProcessPoolExecutor, as_completed

def get_project_root():
    current = os.path.abspath(os.path.dirname(__file__))
    while True:
        if os.path.isdir(os.path.join(current, 'scripts')):
            return current
        parent = os.path.dirname(current)
        if parent == current:
            break
        current = parent
    return os.path.abspath(os.path.dirname(__file__))

BATCH_SIZE = 1000

def scandir_recursive(path, runtime_root):
    found = []
    try:
        with os.scandir(path) as it:
            for entry in it:
                if entry.is_file() and entry.name == 'index.html':
                    rel_path = os.path.relpath(entry.path, runtime_root)
                    found.append(rel_path)
                elif entry.is_dir():
                    found.extend(scandir_recursive(entry.path, runtime_root))
    except Exception as e:
        print(f"[DEBUG] Error scanning {path}: {e}")
    return found

def main():
    parser = argparse.ArgumentParser(description='Scan for index.html files in Runtime API docs.')
    parser.add_argument('--profile', action='store_true', help='Enable profiling output')
    args = parser.parse_args()

    project_root = get_project_root()
    RUNTIME_ROOT = os.path.join(project_root, 'en-US', 'API', 'Runtime')
    NDJSON_OUTPUT = os.path.join(project_root, 'json_output', 'runtime_index_files.ndjson')

    index_files = []
    start_time = time.time()
    if args.profile:
        print('[DEBUG] Starting scan of Runtime index files.')
    write_counter_file("Runtime", 0, "Scanning")
    try:
        subdirs = [os.path.join(RUNTIME_ROOT, d) for d in os.listdir(RUNTIME_ROOT) if os.path.isdir(os.path.join(RUNTIME_ROOT, d))]
        total_found = 0
        with ProcessPoolExecutor() as executor:
            futures = [executor.submit(scandir_recursive, RUNTIME_ROOT, RUNTIME_ROOT)]
            for subdir in subdirs:
                futures.append(executor.submit(scandir_recursive, subdir, RUNTIME_ROOT))
            for i, future in enumerate(as_completed(futures)):
                batch = future.result()
                index_files.extend(batch)
                total_found += len(batch)
                if args.profile and total_found % BATCH_SIZE == 0:
                    print(f"[PROFILE] {total_found} index.html files found so far.")
                write_counter_file("Runtime", total_found, "Scanning")
        write_counter_file("Runtime", total_found, "Scanning")
        os.makedirs(os.path.dirname(NDJSON_OUTPUT), exist_ok=True)
        batch_size = BATCH_SIZE
        buffer = []
        with open(NDJSON_OUTPUT, 'w', encoding='utf-8') as ndjson_file:
            for i, path in enumerate(index_files, 1):
                buffer.append(json.dumps(path) + '\n')
                if i % batch_size == 0:
                    ndjson_file.writelines(buffer)
                    buffer.clear()
            if buffer:
                ndjson_file.writelines(buffer)
        write_counter_file("Runtime", total_found, "Done")
        end_time = time.time()
        if args.profile:
            print(f"[PROFILE] Found {total_found} index.html files under {RUNTIME_ROOT}. Saved to {NDJSON_OUTPUT}. Time: {end_time - start_time:.2f}s")
    except KeyboardInterrupt:
        print("[DEBUG] Scan interrupted by user. Saving progress...")
        os.makedirs(os.path.dirname(NDJSON_OUTPUT), exist_ok=True)
        with open(NDJSON_OUTPUT, 'w', encoding='utf-8') as f:
            json.dump(index_files, f, indent=2)
        write_counter_file("Runtime", len(index_files), "Interrupted")
        print(f"[PROFILE] Interrupted. {len(index_files)} index.html files saved.")
        raise

if __name__ == "__main__":
    main()
