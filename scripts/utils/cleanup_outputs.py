import os
import shutil
import argparse
from concurrent.futures import ThreadPoolExecutor

# List of output folders to clean (relative to project root)
TARGET_FOLDERS = [
    'json_editor_entities',
    'json_developer_entities',
    'json_plugins_entities',
    'json_runtime_entities',
    'json_output',
    'json_constants',
    'json_enums',
    'json_functions',
]
# List of output files to clean (relative to json_output)
OUTPUT_FILES = [
    'all_editor_entities.ndjson',
    'all_developer_entities.ndjson',
    'all_plugins_entities.ndjson',
    'all_runtime_entities.ndjson',
    'all_entities_sanitized.ndjson',
    'editor_index_files.json',
    'developer_index_files.json',
    'plugins_index_files.json',
    'runtime_index_files.json',
    'main_extraction_error.log',
]

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

def fast_remove_folder(folder_path):
    if os.path.exists(folder_path):
        try:
            shutil.rmtree(folder_path)
        except Exception as e:
            print(f"Failed to remove {folder_path}: {e}")

def fast_remove_file(file_path):
    if os.path.exists(file_path):
        try:
            os.remove(file_path)
        except Exception as e:
            print(f"Failed to delete {file_path}: {e}")

def main():
    parser = argparse.ArgumentParser(description="Fast cleanup of all generated output files and folders.")
    parser.add_argument('--recreate', action='store_true', help='Recreate empty output folders after deletion')
    parser.add_argument('--max-workers', type=int, default=os.cpu_count(), help='Parallel workers (default: CPU count)')
    args = parser.parse_args()
    project_root = get_project_root()
    # Remove all output folders in parallel
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        abs_folders = [os.path.join(project_root, folder) for folder in TARGET_FOLDERS]
        executor.map(fast_remove_folder, abs_folders)
    # Remove all output files in parallel (from json_output)
    json_output_dir = os.path.join(project_root, 'json_output')
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        abs_files = [os.path.join(json_output_dir, file) for file in OUTPUT_FILES]
        executor.map(fast_remove_file, abs_files)
    # Optionally, recreate empty folders
    if args.recreate:
        for folder in TARGET_FOLDERS:
            abs_folder = os.path.join(project_root, folder)
            os.makedirs(abs_folder, exist_ok=True)
    print("Fast cleanup complete.")

if __name__ == "__main__":
    main()
