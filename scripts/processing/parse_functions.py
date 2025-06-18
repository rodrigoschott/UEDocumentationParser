import os
import sys
import re
import orjson
from selectolax.parser import HTMLParser
from concurrent.futures import ProcessPoolExecutor
import argparse
import time
import importlib.util

# Robust import for log_helper
log_helper_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'monitoring', 'log_helper.py')
spec = importlib.util.spec_from_file_location('log_helper', log_helper_path)
log_helper = importlib.util.module_from_spec(spec)
sys.modules['log_helper'] = log_helper
spec.loader.exec_module(log_helper)
write_counter_file = log_helper.write_counter_file

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

def extract_text_or_none_sel(node):
    return node.text(strip=True) if node else None

def extract_function_details(function_html_path, folder):
    details = {
        'function_name': None,
        'short_description': None,
        'navigation': [],
        'overloads': [],
        'folder': folder
    }
    try:
        with open(function_html_path, 'r', encoding='utf-8') as f:
            html = f.read()
        tree = HTMLParser(html)
        h1 = tree.css_first('h1')
        if h1:
            details['function_name'] = extract_text_or_none_sel(h1)
        h2 = tree.css_first('h2')
        if h2:
            details['short_description'] = extract_text_or_none_sel(h2)
        nav_div = tree.css_first('div.crumbs')
        if nav_div:
            details['navigation'] = [extract_text_or_none_sel(a) for a in nav_div.css('a')]
        members_div = tree.css_first('div.members')
        if members_div:
            table = members_div.css_first('table')
            if table:
                for row in table.css('tr.normal-row'):
                    cells = row.css('td')
                    if len(cells) >= 4:
                        return_type = extract_text_or_none_sel(cells[1])
                        name_cell = cells[2]
                        name_link = name_cell.css_first('a[href]')
                        name = extract_text_or_none_sel(name_link) if name_link else extract_text_or_none_sel(name_cell)
                        link = name_link.attributes.get('href') if name_link else None
                        args_div = name_cell.css_first('div.name-cell-arguments')
                        arguments = extract_text_or_none_sel(args_div) if args_div else None
                        desc = extract_text_or_none_sel(cells[3])
                        details['overloads'].append({
                            'return_type': return_type,
                            'name': name,
                            'link': link,
                            'arguments': arguments,
                            'description': desc
                        })
    except FileNotFoundError:
        print(f"[ERROR] File not found: {function_html_path}")
    except Exception as e:
        print(f"[ERROR] Failed to parse {function_html_path}: {e}")
    return details

def process_function(args):
    folder, functions_dir = args
    function_html_path = os.path.join(functions_dir, folder, 'index.html')
    if not os.path.exists(function_html_path):
        print(f"[ERROR] index.html not found for folder: {folder}")
        return None
    return extract_function_details(function_html_path, folder)

def extract_all_functions_parallel(max_workers=None, profile=False, batch_size=1000):
    project_root = get_project_root()
    functions_dir = os.path.join(project_root, 'en-US', 'API', 'Functions')
    json_functions_dir = os.path.join(project_root, 'json_functions')
    os.makedirs(json_functions_dir, exist_ok=True)
    ndjson_path = os.path.join(json_functions_dir, 'all_functions.ndjson')
    if not os.path.exists(functions_dir):
        print(f"[ERROR] Functions directory not found: {functions_dir}")
        return
    function_folders = [f for f in os.listdir(functions_dir) if os.path.isdir(os.path.join(functions_dir, f))]
    if not function_folders:
        print(f"[ERROR] No function folders found in: {functions_dir}")
        return
    t0 = time.time()
    total = len(function_folders)
    category = "Functions"
    write_counter_file(category, 0, "Parsing")
    processed_count = 0
    buffer = []
    batch_start = time.time() if profile else None
    with open(ndjson_path, 'w', encoding='utf-8') as ndjson_file:
        with ProcessPoolExecutor(max_workers=max_workers or os.cpu_count()) as executor:
            for i, result in enumerate(executor.map(process_function, ((folder, functions_dir) for folder in function_folders)), 1):
                if result:
                    buffer.append(orjson.dumps(result).decode('utf-8') + '\n')
                    processed_count += 1
                if i % batch_size == 0:
                    ndjson_file.writelines(buffer)
                    if profile:
                        batch_end = time.time()
                        print(f"[PROFILE] Batch of {batch_size} NDJSON writes took {batch_end - batch_start:.2f} seconds.")
                        batch_start = time.time()
                    buffer.clear()
                if i % 100 == 0 or i == total:
                    write_counter_file(category, i, "Parsing")
            if buffer:
                ndjson_file.writelines(buffer)
                if profile:
                    batch_end = time.time()
                    print(f"[PROFILE] Final batch of {len(buffer)} NDJSON writes took {batch_end - batch_start:.2f} seconds.")
    t1 = time.time()
    write_counter_file(category, total, "Done")
    if profile:
        print(f"Processed {processed_count} functions in {t1-t0:.2f}s")

def main():
    parser = argparse.ArgumentParser(description="Extract Unreal API functions documentation to NDJSON.")
    parser.add_argument('--max-workers', type=int, default=os.cpu_count(), help='Parallel workers (default: CPU count)')
    parser.add_argument('--profile', action='store_true', help='Enable timing/profiling output')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for NDJSON writes (default: 1000)')
    args = parser.parse_args()
    extract_all_functions_parallel(max_workers=args.max_workers, profile=args.profile, batch_size=args.batch_size)

if __name__ == '__main__':
    main()
