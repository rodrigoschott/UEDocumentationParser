import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), 'monitoring'))
import json
from bs4 import BeautifulSoup
import re
from log_helper import write_counter_file
from concurrent.futures import ThreadPoolExecutor
import orjson
import time

if sys.version_info >= (3, 12):
    print("[ERROR] This script requires Python 3.10 or 3.11 due to BeautifulSoup/soupsieve compatibility. Please use Python 3.10 or 3.11.")
    sys.exit(1)

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

def extract_text_or_none(tag):
    return tag.get_text(strip=True) if tag else None

def detect_entity_type(soup):
    h1 = soup.find('h1')
    if h1:
        h1_text = extract_text_or_none(h1).lower()
        if 'class' in h1_text:
            return 'class'
        if 'function' in h1_text or 'overload' in h1_text:
            return 'function'
        if 'enum' in h1_text:
            return 'enum'
        if 'struct' in h1_text:
            return 'struct'
        if 'module' in h1_text:
            return 'module'
    if soup.find('div', id='variables'):
        return 'class'
    if soup.find('div', class_='members'):
        return 'function'
    if soup.find('div', id='values'):
        return 'enum'
    return 'unknown'

def extract_common_details(soup):
    details = {
        'name': None,
        'short_description': None,
        'navigation': [],
        'module': None,
        'header': None,
        'include': None,
        'syntax': None,
        'remarks': None
    }
    h1 = soup.find('h1')
    if h1:
        details['name'] = extract_text_or_none(h1)
    h2 = soup.find('h2')
    if h2:
        details['short_description'] = extract_text_or_none(h2)
    nav_div = soup.find('div', class_='crumbs')
    if nav_div:
        details['navigation'] = [extract_text_or_none(x) for x in nav_div.find_all('a')]
    references_div = soup.find('div', id='references')
    if references_div:
        table = references_div.find('table')
        if table:
            for row in table.find_all('tr', class_='normal-row'):
                cells = row.find_all('td')
                if len(cells) == 2:
                    key = extract_text_or_none(cells[0].find('p')).lower()
                    value = extract_text_or_none(cells[1].find('p'))
                    if 'module' in key:
                        details['module'] = value
                    elif 'header' in key:
                        details['header'] = value
                    elif 'include' in key:
                        details['include'] = value
    syntax_div = soup.find('div', id='syntax')
    if syntax_div:
        simplecode_api_div = syntax_div.find('div', class_='simplecode_api')
        if simplecode_api_div:
            p_tag = simplecode_api_div.find('p')
            if p_tag:
                details['syntax'] = p_tag.get_text("\n", strip=True)
    remarks_div = soup.find('div', id='description')
    if remarks_div:
        details['remarks'] = extract_text_or_none(remarks_div)
    return details

def extract_entity_details(entity_type, soup):
    details = extract_common_details(soup)
    if entity_type == 'class':
        details['variables'] = []
        variables_div = soup.find('div', id='variables')
        if variables_div:
            table = variables_div.find('table')
            if table:
                for row in table.find_all('tr', class_='normal-row'):
                    cells = row.find_all('td')
                    if len(cells) >= 3:
                        details['variables'].append({
                            'type': extract_text_or_none(cells[0]),
                            'name': extract_text_or_none(cells[1]),
                            'description': extract_text_or_none(cells[2])
                        })
    elif entity_type == 'function':
        details['overloads'] = []
        members_div = soup.find('div', class_='members')
        if members_div:
            table = members_div.find('table')
            if table:
                for row in table.find_all('tr', class_='normal-row'):
                    cells = row.find_all('td')
                    if len(cells) >= 4:
                        return_type = extract_text_or_none(cells[1])
                        name_cell = cells[2]
                        name_link = name_cell.find('a', href=True)
                        name = extract_text_or_none(name_link) if name_link else extract_text_or_none(name_cell)
                        link = name_link['href'] if name_link else None
                        args_div = name_cell.find('div', class_='name-cell-arguments')
                        arguments = extract_text_or_none(args_div) if args_div else None
                        desc = extract_text_or_none(cells[3])
                        details['overloads'].append({
                            'return_type': return_type,
                            'name': name,
                            'link': link,
                            'arguments': arguments,
                            'description': desc
                        })
    elif entity_type == 'enum':
        details['values'] = []
        values_div = soup.find('div', id='values')
        if values_div:
            table = values_div.find('table')
            if table:
                for row in table.find_all('tr', class_='normal-row'):
                    cells = row.find_all('td')
                    if len(cells) >= 2:
                        details['values'].append({
                            'name': extract_text_or_none(cells[0]),
                            'description': extract_text_or_none(cells[1])
                        })
    return details

def process_entity(args):
    abs_path, rel_path, output_dir, count = args
    try:
        with open(abs_path, 'r', encoding='utf-8') as f:
            soup = BeautifulSoup(f, 'html.parser')
        entity_type = detect_entity_type(soup)
        details = extract_entity_details(entity_type, soup)
        details['entity_type'] = entity_type
        details['source_path'] = rel_path
        safe_name = re.sub(r'[\\/:*?"<>|]', '_', rel_path.replace('\\', '__').replace('/', '__'))
        json_file_path = os.path.join(output_dir, f'{safe_name}.json')
        if os.path.exists(json_file_path):
            return None
        return (json_file_path, details)
    except Exception as e:
        return None

def main():
    do_profile = '--profile' in sys.argv
    do_debug = '--debug' in sys.argv
    flush_times = [] if do_profile else None
    project_root = get_project_root()
    developer_root = os.path.join(project_root, 'en-US', 'API', 'Developer')
    # Use NDJSON index file
    index_json = os.path.join(project_root, 'json_output', 'developer_index_files.ndjson')
    output_dir = os.path.join(project_root, 'json_developer_entities')
    os.makedirs(output_dir, exist_ok=True)
    ndjson_path = os.path.join(output_dir, 'all_developer_entities.ndjson')
    index_path = os.path.join(output_dir, 'developer_entities_index.json')
    # Read NDJSON index file (one path per line)
    with open(index_json, 'r', encoding='utf-8') as f:
        index_files = [json.loads(line) for line in f if line.strip()]
    batch_size = 1000
    processed = 0
    skipped = 0
    total = len(index_files)
    category = "Developer"
    write_counter_file(category, 0, "Extracting")
    if do_debug:
        print(f"[DEBUG] Total index files: {total}")
    index_map = {}
    ndjson_line = 0
    batch_profile_size = 1000
    batch_start_time = time.time() if do_profile else None
    batch_written = 0
    if do_profile:
        t0 = time.time()
    with open(ndjson_path, 'wb') as ndjson_file:
        with ThreadPoolExecutor(max_workers=8) as executor:
            for result in executor.map(process_entity, [(os.path.join(developer_root, rel_path), rel_path, output_dir, processed) for rel_path in index_files], chunksize=batch_size):
                if result:
                    json_file_path, details = result
                    ndjson_file.write(orjson.dumps(details) + b'\n')
                    index_map[details['source_path']] = ndjson_line
                    ndjson_line += 1
                    processed += 1
                    batch_written += 1
                    if do_profile and batch_written == batch_profile_size:
                        batch_end_time = time.time()
                        print(f"[PROFILE] Batch of {batch_profile_size} NDJSON writes took {batch_end_time - batch_start_time:.2f} seconds.")
                        batch_start_time = time.time()
                        batch_written = 0
                else:
                    skipped += 1
                if (processed + skipped) % 100 == 0 or (processed + skipped) == total:
                    if do_debug:
                        print(f"[DEBUG] Processed: {processed} (new), Skipped: {skipped}, Total seen: {processed + skipped} / {total}")
                    write_counter_file(category, processed, "Extracting")
        # Print time for any partial batch at the end
        if do_profile and batch_written > 0:
            batch_end_time = time.time()
            print(f"[PROFILE] Final batch of {batch_written} NDJSON writes took {batch_end_time - batch_start_time:.2f} seconds.")
    write_counter_file(category, processed, "Done")
    if do_debug:
        print(f"[DEBUG] Extraction complete. New files: {processed}, Skipped: {skipped}")
    with open(index_path, 'w', encoding='utf-8') as idxf:
        json.dump(index_map, idxf, indent=2, ensure_ascii=False)
    if do_profile:
        t1 = time.time()
        print(f"[PROFILE] Extraction took {t1-t0:.2f} seconds.")
        print(f"[PROFILE] NDJSON lines written: {ndjson_line}")

if __name__ == '__main__':
    main()
