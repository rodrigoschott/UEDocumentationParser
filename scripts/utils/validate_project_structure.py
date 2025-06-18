import os
import sys

EXPECTED_STRUCTURE = {
    'scripts': {
        'extraction': [
            'extract_editor_entities.py',
            'extract_developer_entities.py',
            'extract_plugins_entities.py',
            'extract_runtime_entities.py',
        ],
        'scanning': [
            'scan_editor_index_files.py',
            'scan_developer_index_files.py',
            'scan_plugins_index_files.py',
            'scan_runtime_index_files.py',
        ],
        'processing': [
            'sanitize_entities.py',
            'parse_classes.py',
            'parse_class_hierarchy.py',
            'parse_enums.py',
            'parse_functions.py',
            'parse_constants.py',
            'deduplicate_entities.py',
            'deduplicate_entities_fast.py',
            'organize_by_hierarchy.py',
        ],
        'monitoring': [
            'log_helper.py',
        ],
        'utils': [
            'validate_project_structure.py',
            'sample_ndjson_lines.py',
            'cleanup_outputs.py',
            'README.md',
        ]
    },
    'json_output': [],
    'json_editor_entities': [],
    'json_developer_entities': [],
    'json_plugins_entities': [],
    'json_runtime_entities': [],
    'json_constants': [],
    'json_enums': [],
    'json_functions': [],
    'en-US': [],
    'Images': [],
}


def get_project_root():
    # Find the project root by going up until we find a marker (e.g., .git or scripts folder)
    current = os.path.abspath(os.path.dirname(__file__))
    while True:
        if os.path.isdir(os.path.join(current, 'scripts')):
            return current
        parent = os.path.dirname(current)
        if parent == current:
            break
        current = parent
    return os.path.abspath(os.path.dirname(__file__))

def check_structure(base_path, structure, parent=''):
    errors = []
    for key, value in structure.items():
        path = os.path.join(base_path, parent, key) if parent else os.path.join(base_path, key)
        if not os.path.exists(path):
            errors.append(f"Missing folder: {path}")
            continue
        if isinstance(value, dict):
            errors.extend(check_structure(base_path, value, os.path.join(parent, key) if parent else key))
        elif isinstance(value, list) and value:
            for fname in value:
                fpath = os.path.join(path, fname)
                if not os.path.exists(fpath):
                    errors.append(f"Missing file: {fpath}")
    return errors

def find_misplaced_files(base_path, structure, parent=''):
    misplaced = []
    for key, value in structure.items():
        path = os.path.join(base_path, parent, key) if parent else os.path.join(base_path, key)
        if not os.path.exists(path):
            continue
        if isinstance(value, dict):
            misplaced.extend(find_misplaced_files(base_path, value, os.path.join(parent, key) if parent else key))
        elif isinstance(value, list):
            expected = set(value)
            actual = set(os.listdir(path)) if os.path.isdir(path) else set()
            for fname in actual:
                if value and fname not in expected:
                    misplaced.append(f"Misplaced file in {path}: {fname}")
    return misplaced

def main():
    base_path = get_project_root()
    errors = check_structure(base_path, EXPECTED_STRUCTURE)
    misplaced = find_misplaced_files(base_path, EXPECTED_STRUCTURE)
    if not errors and not misplaced:
        print("[VALIDATION] Project structure is correct.")
    else:
        print("[VALIDATION] Issues found:")
        for e in errors:
            print("  -", e)
        for m in misplaced:
            print("  -", m)
    sys.exit(1 if errors or misplaced else 0)

if __name__ == "__main__":
    main()
