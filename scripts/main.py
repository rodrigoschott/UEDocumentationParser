import argparse
import concurrent.futures
import subprocess
import os
import time
import threading
import sys
import importlib.util
# Robust import for log_helper
log_helper_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'scripts', 'monitoring', 'log_helper.py')
spec = importlib.util.spec_from_file_location('log_helper', log_helper_path)
log_helper = importlib.util.module_from_spec(spec)
sys.modules['log_helper'] = log_helper
spec.loader.exec_module(log_helper)
cleanup_counter_files = log_helper.cleanup_counter_files
start_counter_display = log_helper.start_counter_display
stop_counter_display = log_helper.stop_counter_display

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

# List of scan and extraction scripts, grouped by folder (project-root-relative)
scan_scripts = [
    (cat, os.path.join('scripts', 'scanning', script)) for cat, script in [
        ('editor', 'scan_editor_index_files.py'),
        ('developer', 'scan_developer_index_files.py'),
        ('plugins', 'scan_plugins_index_files.py'),
        ('runtime', 'scan_runtime_index_files.py'),
    ]
]
extract_scripts = [
    (cat, os.path.join('scripts', 'extraction', script)) for cat, script in [
        ('editor', 'extract_editor_entities.py'),
        ('developer', 'extract_developer_entities.py'),
        ('plugins', 'extract_plugins_entities.py'),
        ('runtime', 'extract_runtime_entities.py'),
    ]
]
parse_scripts = [
    os.path.join('scripts', 'processing', script) for script in [
        'parse_classes.py',
        'parse_constants.py',
        'parse_enums.py',
        'parse_functions.py',
        'parse_class_hierarchy.py',
    ]
]
LOG_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'main_extraction_error.log')

# Helper to run a script and capture output/errors
def run_script(script):
    if not os.path.exists(script):
        print(f"[ERROR] Script not found: {script}")
        with open(LOG_FILE, 'a', encoding='utf-8') as log:
            log.write(f"ERROR: Script not found: {script}\n")
        return (script, -1, f"Script not found: {script}")
    try:
        print(f"Running: {script}")
        result = subprocess.run([sys.executable, script], capture_output=True, text=True, check=False)
        if result.returncode != 0:
            with open(LOG_FILE, 'a', encoding='utf-8') as log:
                log.write(f"ERROR in {script}:\n{result.stderr}\n")
        return (script, result.returncode, result.stdout)
    except Exception as e:
        with open(LOG_FILE, 'a', encoding='utf-8') as log:
            log.write(f"EXCEPTION in {script}: {e}\n")
        return (script, -1, str(e))

def count_ndjson_files_and_lines(folders):
    ndjson_summary = {}
    for folder in folders:
        if os.path.exists(folder):
            for f in os.listdir(folder):
                if f.endswith('.ndjson'):
                    path = os.path.join(folder, f)
                    try:
                        with open(path, 'r', encoding='utf-8') as ndjsonf:
                            line_count = sum(1 for _ in ndjsonf)
                        ndjson_summary[path] = line_count
                    except Exception:
                        ndjson_summary[path] = 'ERROR'
    return ndjson_summary

def count_json_files(folders):
    # Cache directory listings for performance
    file_counts = {}
    for folder in folders:
        if os.path.exists(folder):
            file_counts[folder] = sum(1 for f in os.listdir(folder) if f.endswith('.json'))
        else:
            file_counts[folder] = 0
    return file_counts

def run_script_with_retries(script, idx, total, live_output=False, retries=2, extra_args=None):
    # script can be a string (path) or a tuple (path, [args])
    if isinstance(script, (list, tuple)):
        script_path = script[0]
        script_args = script[1] if len(script) > 1 else []
    else:
        script_path = script
        script_args = []
    script_path = os.path.abspath(script_path)
    if not os.path.exists(script_path):
        print(f"[ERROR] Script not found: {script_path}")
        with open(LOG_FILE, 'a', encoding='utf-8') as log:
            log.write(f"ERROR: Script not found: {script_path}\n")
        return (script_path, -1, f"Script not found: {script_path}")
    attempt = 0
    while attempt <= retries:
        try:
            print(f"[{idx+1}/{total}] Running: {script_path} (Attempt {attempt+1})")
            cmd = [sys.executable, script_path] + (script_args or [])
            if extra_args:
                cmd += extra_args
            if live_output:
                result = subprocess.run(cmd)
                code = result.returncode
                out = None
            else:
                result = subprocess.run(cmd, capture_output=True, text=True, check=False)
                code = result.returncode
                out = result.stdout
                if code != 0:
                    with open(LOG_FILE, 'a', encoding='utf-8') as log:
                        log.write(f"ERROR in {script_path} (Attempt {attempt+1}):\n{result.stderr}\n")
            if code == 0:
                return (script_path, code, out)
        except Exception as e:
            with open(LOG_FILE, 'a', encoding='utf-8') as log:
                log.write(f"EXCEPTION in {script_path} (Attempt {attempt+1}): {e}\n")
        attempt += 1
    return (script_path, -1, f"Failed after {retries+1} attempts")

def run_script_with_progress(script, idx, total, live_output=False):
    if not os.path.exists(script):
        print(f"[ERROR] Script not found: {script}")
        with open(LOG_FILE, 'a', encoding='utf-8') as log:
            log.write(f"ERROR: Script not found: {script}\n")
        return (script, -1, f"Script not found: {script}")
    try:
        print(f"[{idx+1}/{total}] Running: {script}")
        if live_output:
            result = subprocess.run([sys.executable, script])
            code = result.returncode
            out = None
        else:
            result = subprocess.run([sys.executable, script], capture_output=True, text=True, check=False)
            code = result.returncode
            out = result.stdout
            if code != 0:
                with open(LOG_FILE, 'a', encoding='utf-8') as log:
                    log.write(f"ERROR in {script}:\n{result.stderr}\n")
        return (script, code, out)
    except Exception as e:
        with open(LOG_FILE, 'a', encoding='utf-8') as log:
            log.write(f"EXCEPTION in {script}: {e}\n")
        return (script, -1, str(e))

def main():
    parser = argparse.ArgumentParser(description="Extract and process Unreal Engine C++ API documentation.")
    parser.add_argument('--exclude', type=str, default='', help='Comma-separated list of categories to exclude (e.g., plugins,editor)')
    parser.add_argument('--max-workers', type=int, default=os.cpu_count(), help='Number of parallel workers (default: CPU count)')
    parser.add_argument('--scanning', action='store_true', help='Run only the scanning/indexing step')
    parser.add_argument('--extraction', action='store_true', help='Run only the extraction step (requires scanning)')
    parser.add_argument('--processing', action='store_true', help='Run only the processing/parsing step (requires extraction)')
    parser.add_argument('--profile', action='store_true', help='Enable timing/profiling output')
    args = parser.parse_args()
    project_root = get_project_root()
    exclude_categories = set([c.strip().lower() for c in args.exclude.split(',') if c.strip()])
    if exclude_categories:
        print(f"[Info] Excluding categories: {', '.join(exclude_categories)}")
    max_workers = args.max_workers
    folders = [os.path.join(project_root, f) for f in ['json_editor_entities', 'json_developer_entities', 'json_plugins_entities', 'json_runtime_entities', 'json_output', 'json_constants', 'json_enums', 'json_functions']]
    LOG_FILE = os.path.join(project_root, 'main_extraction_error.log')
    # Filter scan and extract scripts based on exclusion
    filtered_scan_scripts = [(cat, script) for cat, script in scan_scripts if cat not in exclude_categories]
    filtered_extract_scripts = [(cat, script) for cat, script in extract_scripts if cat not in exclude_categories]
    timings = {}
    def profile_step(name, func):
        if args.profile:
            t0 = time.time()
            func()
            t1 = time.time()
            timings[name] = t1-t0
        else:
            func()
    # Clean error log
    if os.path.exists(LOG_FILE):
        os.remove(LOG_FILE)
    # Step selection logic
    run_scanning = args.scanning
    run_extraction = args.extraction
    run_processing = args.processing
    # If no flags given, run all
    if not (run_scanning or run_extraction or run_processing):
        run_scanning = run_extraction = run_processing = True
    # Pre-step checks
    # 1. Extraction requires index files
    index_files_required = [
        os.path.join(project_root, 'json_output', f'{cat}_index_files.ndjson')
        for cat in ['editor', 'developer', 'plugins', 'runtime'] if cat not in exclude_categories
    ]
    missing_index = [f for f in index_files_required if not os.path.exists(f)]
    # 2. Processing requires NDJSON entity files
    ndjson_files_required = [
        os.path.join(project_root, f'json_{cat}_entities', f'all_{cat}_entities.ndjson')
        for cat in ['editor', 'developer', 'plugins', 'runtime'] if cat not in exclude_categories
    ]
    missing_ndjson = [f for f in ndjson_files_required if not os.path.exists(f)]
    # Print summary and warnings
    print("\n[Step Selection Summary]")
    if run_scanning:
        print("- Scanning: ENABLED")
    else:
        print("- Scanning: DISABLED")
    if run_extraction:
        if missing_index:
            print(f"- Extraction: ENABLED, but [WARNING] missing index files: {missing_index}")
        else:
            print("- Extraction: ENABLED")
    else:
        print("- Extraction: DISABLED")
    if run_processing:
        if missing_ndjson:
            print(f"- Processing: ENABLED, but [WARNING] missing NDJSON entity files: {missing_ndjson}")
        else:
            print("- Processing: ENABLED")
    else:
        print("- Processing: DISABLED")
    print()
    # Step execution
    # --- Scanning ---
    if run_scanning:
        print("--- Scanning index files ---")
        def scan_phase():
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                total = len(filtered_scan_scripts)
                futures = [executor.submit(run_script_with_retries, script, idx, total, False) for idx, (_, script) in enumerate(filtered_scan_scripts)]
                for future in concurrent.futures.as_completed(futures):
                    script, code, out = future.result()
                    print(f"Finished {script} (code {code})")
        profile_step('scan', scan_phase)
        # Async file counting
        def async_count():
            counts = count_json_files(folders)
            print(f"[Progress] JSON files created so far: {sum(counts.values())}")
        threading.Thread(target=async_count, daemon=True).start()
        # After scanning, refresh index file existence
        missing_index = [f for f in index_files_required if not os.path.exists(f)]

    # --- Extraction ---
    if run_extraction:
        if missing_index:
            print(f"[WARNING] Extraction step skipped due to missing index files: {missing_index}")
        else:
            print("--- Extracting entities ---")
            cleanup_counter_files()
            start_counter_display()
            def extract_phase():
                with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                    total = len(filtered_extract_scripts)
                    futures = [executor.submit(run_script_with_retries, script, idx, total, True) for idx, (_, script) in enumerate(filtered_extract_scripts)]
                    for future in concurrent.futures.as_completed(futures):
                        script, code, out = future.result()
            profile_step('extract', extract_phase)
            stop_counter_display()
            cleanup_counter_files()
            threading.Thread(target=lambda: print(f"[Progress] JSON files created so far: {sum(count_json_files(folders).values())}"), daemon=True).start()
            # After extraction, refresh NDJSON file existence
            missing_ndjson = [f for f in ndjson_files_required if not os.path.exists(f)]

    # --- Processing ---
    if run_processing:
        if missing_ndjson:
            print(f"[WARNING] Processing step skipped due to missing NDJSON entity files: {missing_ndjson}")
        else:
            print("--- Parsing base entities ---")
            def parse_phase():
                with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                    total = len(parse_scripts)
                    futures = [executor.submit(run_script_with_retries, [os.path.abspath(script), ['--profile']], idx, total, False) for idx, script in enumerate(parse_scripts)]
                    for future in concurrent.futures.as_completed(futures):
                        script, code, out = future.result()
                        print(f"Finished {script} (code {code})")
            profile_step('parse', parse_phase)
            print(f"[Progress] JSON files created so far: {sum(count_json_files(folders).values())}")
    # 4. Count output files (NDJSON summary)
    ndjson_summary = count_ndjson_files_and_lines(folders)
    if ndjson_summary:
        print("[NDJSON Entity File Summary]:")
        for path, count in ndjson_summary.items():
            print(f"{path}: {count} entities")
        print(f"Total NDJSON entity files: {len(ndjson_summary)}")
        print(f"Total entities: {sum(c for c in ndjson_summary.values() if isinstance(c, int))}")
    else:
        print("No NDJSON entity files found.")
    if os.path.exists(LOG_FILE):
        print(f"Errors were logged to {LOG_FILE}")
    else:
        print("No errors encountered.")
    if args.profile:
        print("--- Profiling Results (seconds) ---")
        for k, v in timings.items():
            print(f"{k}: {v:.2f}s")
    # Print all expected script paths for debugging
    print("[DEBUG] Expected scan scripts:")
    for _, script in scan_scripts:
        print(f"  {script}")
    print("[DEBUG] Expected extract scripts:")
    for _, script in extract_scripts:
        print(f"  {script}")
    print("[DEBUG] Expected parse scripts:")
    for script in parse_scripts:
        print(f"  {script}")

if __name__ == '__main__':
    main()
