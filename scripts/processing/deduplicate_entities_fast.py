import os
import sys
import orjson
import argparse
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
import importlib.util
# Robust import for log_helper
log_helper_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'monitoring', 'log_helper.py')
spec = importlib.util.spec_from_file_location('log_helper', log_helper_path)
log_helper = importlib.util.module_from_spec(spec)
sys.modules['log_helper'] = log_helper
spec.loader.exec_module(log_helper)
write_counter_file = log_helper.write_counter_file

ENTITY_FOLDERS = [
    'json_editor_entities',
    'json_developer_entities',
    'json_plugins_entities',
    'json_runtime_entities',
]
UNIQUE_KEY = 'source_path'  # Most robust unique key for your data
DEFAULT_OUTPUT_FILE = 'all_entities_deduped.ndjson'
DEFAULT_MAX_WORKERS = 16
BATCH_SIZE = 10000
CATEGORY = "Deduplication"


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


def list_ndjson_files(folders, project_root):
    for folder in folders:
        abs_folder = os.path.join(project_root, folder)
        if not os.path.exists(abs_folder):
            continue
        for filename in os.listdir(abs_folder):
            if filename.endswith('.ndjson'):
                yield os.path.join(abs_folder, filename)


def process_ndjson_file(file_path):
    results = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    entity = orjson.loads(line)
                    key = entity.get(UNIQUE_KEY)
                    results.append((key, entity))
                except Exception as e:
                    print(f"[ERROR] Error parsing line in {file_path}: {e}")
    except Exception as e:
        print(f"[ERROR] Error reading {file_path}: {e}")
    return results


def main():
    parser = argparse.ArgumentParser(description='Deduplicate all entity NDJSON files into a single NDJSON file.')
    parser.add_argument('--profile', action='store_true', help='Enable profiling output')
    parser.add_argument('--max-workers', type=int, default=DEFAULT_MAX_WORKERS, help='Number of parallel workers')
    parser.add_argument('--output', type=str, default=None, help='Output NDJSON file path')
    args = parser.parse_args()

    project_root = get_project_root()
    output_dir = os.path.join(project_root, 'json_output')
    os.makedirs(output_dir, exist_ok=True)
    output_path = args.output or os.path.join(output_dir, DEFAULT_OUTPUT_FILE)
    seen = set()
    buffer = []
    total_written = 0
    t0 = time.time() if args.profile else None
    ndjson_files = list(list_ndjson_files(ENTITY_FOLDERS, project_root))
    if not ndjson_files:
        print("[ERROR] No NDJSON files found in entity folders.")
        sys.exit(1)
    if args.profile:
        print(f"[DEBUG] Found {len(ndjson_files)} NDJSON files to process.")
    write_counter_file(CATEGORY, 0, "Deduplicating")
    with open(output_path, 'w', encoding='utf-8') as out:
        with ProcessPoolExecutor(max_workers=args.max_workers) as executor:
            futures = [executor.submit(process_ndjson_file, file_path) for file_path in ndjson_files]
            for i, future in enumerate(as_completed(futures), 1):
                batch = future.result()
                for key, entity in batch:
                    if key and key not in seen:
                        seen.add(key)
                        buffer.append(orjson.dumps(entity).decode('utf-8') + '\n')
                        total_written += 1
                        if len(buffer) >= BATCH_SIZE:
                            out.writelines(buffer)
                            buffer.clear()
                            if args.profile:
                                print(f"[PROFILE] Written {total_written} deduplicated entities so far...")
                            write_counter_file(CATEGORY, total_written, "Deduplicating")
            if buffer:
                out.writelines(buffer)
                buffer.clear()
    write_counter_file(CATEGORY, total_written, "Done")
    if args.profile:
        t1 = time.time()
        print(f"[PROFILE] Deduplication complete. Total unique entities: {total_written}. Time: {t1-t0:.2f}s")
    print(f"Deduplicated entities written to {output_path}")


if __name__ == '__main__':
    main()
