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

def extract_constants_from_index(constants_index_path):
    constants = []
    try:
        with open(constants_index_path, 'r', encoding='utf-8') as f:
            html = f.read()
        tree = HTMLParser(html)
        main_col = tree.css_first('div#maincol')
        if not main_col:
            return constants
        for member_div in main_col.css('div.memberindexitem'):
            link_tag = member_div.css_first('a#content_link[href]')
            if link_tag:
                constant_name_tag = link_tag.css_first('span') if link_tag.css_first('span') else link_tag
                constant_name = extract_text_or_none_sel(constant_name_tag) if constant_name_tag else None
                relative_url = link_tag.attributes.get('href')
                if constant_name and relative_url and not relative_url.startswith('#'):
                    constants.append((constant_name, relative_url.strip()))
    except Exception as e:
        print(f"[ERROR] Exception in extract_constants_from_index: {e}")
    return constants

def extract_constant_details_full(constant_page_path):
    details = {
        'constant_name': None,
        'short_description': None,
        'navigation': [],
        'module': None,
        'header': None,
        'include': None,
        'syntax': None,
        'values': [],
        'remarks': None
    }
    try:
        with open(constant_page_path, 'r', encoding='utf-8') as f:
            html = f.read()
        tree = HTMLParser(html)
        h1 = tree.css_first('h1')
        if h1:
            details['constant_name'] = extract_text_or_none_sel(h1)
        h2 = tree.css_first('h2')
        if h2:
            details['short_description'] = extract_text_or_none_sel(h2)
        nav_div = tree.css_first('div.crumbs')
        if nav_div:
            details['navigation'] = [extract_text_or_none_sel(a) for a in nav_div.css('a')]
        references_div = tree.css_first('div#references')
        if references_div:
            table = references_div.css_first('table')
            if table:
                for row in table.css('tr.normal-row'):
                    cells = row.css('td')
                    if len(cells) == 2:
                        key = extract_text_or_none_sel(cells[0].css_first('p')).lower() if cells[0].css_first('p') else ''
                        value = extract_text_or_none_sel(cells[1].css_first('p')) if cells[1].css_first('p') else ''
                        if 'module' in key:
                            details['module'] = value
                        elif 'header' in key:
                            details['header'] = value
                        elif 'include' in key:
                            details['include'] = value
        syntax_div = tree.css_first('div#syntax')
        if syntax_div:
            simplecode_api_div = syntax_div.css_first('div.simplecode_api')
            if simplecode_api_div:
                p_tag = simplecode_api_div.css_first('p')
                if p_tag:
                    details['syntax'] = p_tag.text(strip=True)
        values_div = tree.css_first('div#values')
        if values_div:
            table = values_div.css_first('table')
            if table:
                for row in table.css('tr.normal-row'):
                    cells = row.css('td')
                    if len(cells) >= 2:
                        val_name = extract_text_or_none_sel(cells[0])
                        val_desc = extract_text_or_none_sel(cells[1])
                        details['values'].append({'name': val_name, 'description': val_desc})
        remarks_div = tree.css_first('div#description')
        if remarks_div:
            details['remarks'] = extract_text_or_none_sel(remarks_div)
    except Exception as e:
        print(f"[ERROR] Exception in extract_constant_details_full: {e}")
    return details

def process_constant(args):
    constant_tuple, api_docs_base_path = args
    constant_name, relative_url = constant_tuple
    path_segment = relative_url.replace('../', '', 1)
    constant_doc_dir = os.path.join(api_docs_base_path, path_segment)
    constant_html_page_path = os.path.join(constant_doc_dir, 'index.html')
    constant_html_page_path = os.path.normpath(constant_html_page_path)
    details = extract_constant_details_full(constant_html_page_path)
    details['constant_name'] = constant_name
    details['relative_url'] = relative_url
    return details

def extract_all_constants_parallel(max_workers=None, profile=False, constants_index_path=None, output_path=None, batch_size=1000):
    project_root = get_project_root()
    constants_index_path = constants_index_path or os.path.join(project_root, 'en-US', 'API', 'Constants', 'index.html')
    json_constants_dir = os.path.join(project_root, 'json_constants')
    os.makedirs(json_constants_dir, exist_ok=True)
    ndjson_path = output_path or os.path.join(json_constants_dir, 'all_constants.ndjson')
    if not os.path.exists(constants_index_path):
        print(f"[ERROR] Constants index file not found: {constants_index_path}")
        return
    constants_data = extract_constants_from_index(constants_index_path)
    if not constants_data:
        print(f"[ERROR] No constants found in index file: {constants_index_path}")
        return
    api_docs_base_path = os.path.abspath(os.path.join(project_root, 'en-US', 'API'))
    t0 = time.time()
    total = len(constants_data)
    category = "Constants"
    write_counter_file(category, 0, "Parsing")
    buffer = []
    processed_count = 0
    batch_start = time.time() if profile else None
    with open(ndjson_path, 'w', encoding='utf-8') as ndjson_file:
        with ProcessPoolExecutor(max_workers=max_workers or os.cpu_count()) as executor:
            for i, result in enumerate(executor.map(process_constant, ((tup, api_docs_base_path) for tup in constants_data)), 1):
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
        print(f"Processed {processed_count} constants in {t1-t0:.2f}s")

def main():
    parser = argparse.ArgumentParser(description="Extract Unreal API constants documentation to NDJSON.")
    parser.add_argument('--max-workers', type=int, default=os.cpu_count(), help='Parallel workers (default: CPU count)')
    parser.add_argument('--profile', action='store_true', help='Enable timing/profiling output')
    parser.add_argument('--constants-index-path', type=str, default=None, help='Override constants index.html path')
    parser.add_argument('--output-path', type=str, default=None, help='Override output NDJSON path')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for NDJSON writes (default: 1000)')
    args = parser.parse_args()
    extract_all_constants_parallel(max_workers=args.max_workers, profile=args.profile, constants_index_path=args.constants_index_path, output_path=args.output_path, batch_size=args.batch_size)

if __name__ == '__main__':
    main()
