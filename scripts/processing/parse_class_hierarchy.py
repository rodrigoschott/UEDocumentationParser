import os
import sys
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

def parse_hierarchy_table_sel(table, depth=0, parent=None, max_depth=None):
    result = []
    if max_depth is not None and depth > max_depth:
        return result
    rows = table.css('tr')
    for row in rows:
        label_cell = row.css_first('td.hierarchy-label-cell')
        if not label_cell:
            continue
        link_tag = label_cell.css_first('a[href]')
        if not link_tag:
            continue
        class_name = link_tag.text(strip=True)
        class_link = link_tag.attributes.get('href')
        children = []
        nested_table = label_cell.css_first('table.hierarchy-table-collapsed')
        if nested_table:
            children = parse_hierarchy_table_sel(nested_table, depth+1, parent=class_name, max_depth=max_depth)
        result.append({
            'name': class_name,
            'link': class_link,
            'children': children
        })
    return result

def extract_class_hierarchy_sel(index_html_path, max_depth=None):
    try:
        with open(index_html_path, 'r', encoding='utf-8') as f:
            html = f.read()
        tree = HTMLParser(html)
        table = tree.css_first('table.hierarchy-table#hrch')
        if not table:
            print(f'[ERROR] Could not find class hierarchy table in {index_html_path}')
            return None
        hierarchy = parse_hierarchy_table_sel(table, max_depth=max_depth)
        return hierarchy
    except Exception as e:
        print(f'[ERROR] Failed to parse {index_html_path}: {e}')
        return None

def flatten_hierarchy_tree(tree, parent=None):
    flat = []
    for node in tree:
        entry = {
            'name': node['name'],
            'link': node['link'],
            'parent': parent,
            'children': [child['name'] for child in node.get('children', [])]
        }
        flat.append(entry)
        if node.get('children'):
            flat.extend(flatten_hierarchy_tree(node['children'], parent=node['name']))
    return flat

def process_one_hierarchy(index_html_path, max_depth=None):
    hierarchy = extract_class_hierarchy_sel(index_html_path, max_depth=max_depth)
    if hierarchy is not None:
        flat = flatten_hierarchy_tree(hierarchy)
        return flat
    return []

def extract_all_hierarchies_parallel(root_dir, output_ndjson_path, max_depth=None, max_workers=None, batch_size=100, profile=False):
    index_files = []
    for dirpath, dirnames, filenames in os.walk(root_dir):
        for fname in filenames:
            if fname == 'index.html':
                index_files.append(os.path.join(dirpath, fname))
    total = len(index_files)
    category = "ClassHierarchy"
    write_counter_file(category, 0, "Parsing")
    t0 = time.time()
    processed_count = 0
    buffer = []
    batch_start = time.time() if profile else None
    with open(output_ndjson_path, 'w', encoding='utf-8') as ndjson_file:
        with ProcessPoolExecutor(max_workers=max_workers or os.cpu_count()) as executor:
            for i, flat_list in enumerate(executor.map(lambda path: process_one_hierarchy(path, max_depth), index_files), 1):
                for entry in flat_list:
                    buffer.append(orjson.dumps(entry).decode('utf-8') + '\n')
                    processed_count += 1
                if processed_count % batch_size == 0:
                    ndjson_file.writelines(buffer)
                    if profile:
                        batch_end = time.time()
                        print(f"[PROFILE] Batch of {batch_size} NDJSON writes took {batch_end - batch_start:.2f} seconds.")
                        batch_start = time.time()
                    buffer.clear()
                if i % 10 == 0 or i == total:
                    write_counter_file(category, i, "Parsing")
            if buffer:
                ndjson_file.writelines(buffer)
                if profile:
                    batch_end = time.time()
                    print(f"[PROFILE] Final batch of {len(buffer)} NDJSON writes took {batch_end - batch_start:.2f} seconds.")
    t1 = time.time()
    write_counter_file(category, total, "Done")
    if profile:
        print(f"Processed {processed_count} class hierarchies in {t1-t0:.2f}s")

def main():
    parser = argparse.ArgumentParser(description="Extract Unreal Engine class hierarchy to NDJSON.")
    parser.add_argument('--max-depth', type=int, default=10, help='Maximum depth to parse (for debugging)')
    parser.add_argument('--all', action='store_true', help='Parse all index.html files under ClassHierarchy in parallel')
    parser.add_argument('--max-workers', type=int, default=os.cpu_count(), help='Parallel workers (default: CPU count)')
    parser.add_argument('--batch-size', type=int, default=100, help='Batch size for NDJSON writes (default: 100)')
    parser.add_argument('--profile', action='store_true', help='Enable timing/profiling output')
    parser.add_argument('--output-path', type=str, default=None, help='Override output NDJSON path')
    args = parser.parse_args()
    project_root = get_project_root()
    if args.all:
        root_dir = os.path.join(project_root, 'en-US', 'API', 'ClassHierarchy')
        output_ndjson_path = args.output_path or os.path.join(project_root, 'json_output', 'all_class_hierarchies.ndjson')
        os.makedirs(os.path.dirname(output_ndjson_path), exist_ok=True)
        extract_all_hierarchies_parallel(root_dir, output_ndjson_path, max_depth=args.max_depth, max_workers=args.max_workers, batch_size=args.batch_size, profile=args.profile)
    else:
        index_html_path = os.path.join(project_root, 'en-US', 'API', 'ClassHierarchy', 'index.html')
        output_ndjson_path = args.output_path or os.path.join(project_root, 'json_output', 'class_hierarchy.ndjson')
        os.makedirs(os.path.dirname(output_ndjson_path), exist_ok=True)
        hierarchy = extract_class_hierarchy_sel(index_html_path, max_depth=args.max_depth)
        if hierarchy is not None:
            flat = flatten_hierarchy_tree(hierarchy)
            with open(output_ndjson_path, 'w', encoding='utf-8') as out:
                for entry in flat:
                    out.write(orjson.dumps(entry).decode('utf-8') + '\n')
            print(f'Class hierarchy saved to {output_ndjson_path}')
        else:
            print(f'[ERROR] Could not extract class hierarchy from {index_html_path}')

if __name__ == '__main__':
    main()
