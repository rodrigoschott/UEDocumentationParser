import os
import sys
import re
import orjson
from selectolax.parser import HTMLParser
from concurrent.futures import ThreadPoolExecutor
import argparse
import time
import warnings
# Robust import for log_helper
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), 'monitoring'))
from log_helper import write_counter_file

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

def extract_text_or_none(node):
    return node.text(strip=True) if node else None

def extract_classes_from_index(index_file_path):
    classes = []
    try:
        with open(index_file_path, 'r', encoding='utf-8') as f:
            html = f.read()
        tree = HTMLParser(html)
        main_col = tree.css_first('div#maincol')
        if not main_col:
            return classes
        for member_div in main_col.css('div.memberindexitem'):
            link_tag = member_div.css_first('a#content_link')
            if link_tag:
                class_name_tag = link_tag.css_first('span') or link_tag
                class_name = extract_text_or_none(class_name_tag)
                relative_url = link_tag.attributes.get('href')
                if class_name and relative_url and not relative_url.startswith('#'):
                    classes.append((class_name, relative_url.strip()))
    except Exception as e:
        print(f"[ERROR] Exception in extract_classes_from_index: {e}")
    return classes

def extract_class_details_full(class_page_path, profile=False, profile_detailed=False):
    details = {
        'class_name': None,
        'short_description': None,
        'navigation': [],
        'inheritance': [],
        'remarks': None,
        'syntax': [],
        'module': None,
        'header': None,
        'include': None,
        'variables': [],
        'constructors': [],
        'functions': [],
        'constants': [],
        'see_also': []
    }
    timings = {}
    try:
        t0 = time.time() if profile else None
        with open(class_page_path, 'r', encoding='utf-8') as f:
            html = f.read()
        t1 = time.time() if profile else None
        if profile: timings['read_file'] = t1-t0
        tree = HTMLParser(html)
        t2 = time.time() if profile else None
        if profile: timings['parse_html'] = t2-t1
        # Class name
        h1 = tree.css_first('h1')
        if h1:
            details['class_name'] = h1.text(strip=True)
        t3 = time.time() if profile else None
        if profile: timings['class_name'] = t3-t2
        # Short Description
        desc = None
        desc_div = tree.css_first('div.summary')
        if desc_div:
            desc = desc_div.text(strip=True)
        else:
            p = tree.css_first('h1 + p')
            if p:
                desc = p.text(strip=True)
        details['short_description'] = desc
        t4 = time.time() if profile else None
        if profile: timings['short_description'] = t4-t3
        # Navigation (breadcrumb)
        nav_div = tree.css_first('div.breadcrumb')
        if nav_div:
            details['navigation'] = [a.text(strip=True) for a in nav_div.css('a')]
        t5 = time.time() if profile else None
        if profile: timings['navigation'] = t5-t4
        # Inheritance Hierarchy
        inheritance_div = tree.css_first('div#hierarchy')
        if inheritance_div:
            details['inheritance'] = [x.text(strip=True) for x in inheritance_div.css('span.hierarchyitem')]
        t6 = time.time() if profile else None
        if profile: timings['inheritance'] = t6-t5
        # Remarks
        remarks_div = tree.css_first('div#remarks')
        if remarks_div:
            details['remarks'] = remarks_div.text(strip=True)
        t7 = time.time() if profile else None
        if profile: timings['remarks'] = t7-t6
        # Syntax (UCLASS, class definition)
        syntax_div = tree.css_first('div#syntax')
        if syntax_div:
            details['syntax'] = [pre.text(strip=True) for pre in syntax_div.css('pre')]
        else:
            type_div = tree.css_first('div#type')
            if type_div:
                simplecode_api_div = type_div.css_first('div.simplecode_api')
                if simplecode_api_div:
                    p_tag = simplecode_api_div.css_first('p')
                    if p_tag:
                        details['syntax'] = [p_tag.text(strip=True)]
        t8 = time.time() if profile else None
        if profile: timings['syntax'] = t8-t7
        # References (Module, Header, Include)
        references_div = tree.css_first('div#references')
        if references_div:
            for row in references_div.css('tr.normal-row'):
                cells = row.css('td')
                if len(cells) == 2:
                    key = cells[0].css_first('p').text(strip=True).lower() if cells[0].css_first('p') else ''
                    value = cells[1].css_first('p').text(strip=True) if cells[1].css_first('p') else ''
                    if 'module' in key:
                        details['module'] = value
                    elif 'header' in key:
                        details['header'] = value
                    elif 'include' in key:
                        details['include'] = value
        t9 = time.time() if profile else None
        if profile: timings['references'] = t9-t8
        # Variables
        variables_div = tree.css_first('div#variables')
        if variables_div:
            for row in variables_div.css('tr.normal-row'):
                cells = row.css('td')
                if len(cells) >= 3:
                    var_type = cells[0].text(strip=True)
                    var_name = cells[1].text(strip=True)
                    var_desc = cells[2].text(strip=True)
                    var_spec = var_type if var_type and 'UProperty' in var_type else ''
                    details['variables'].append({
                        'type': var_type,
                        'name': var_name,
                        'description': var_desc,
                        'specifiers': var_spec
                    })
        t10 = time.time() if profile else None
        if profile: timings['variables'] = t10-t9
        # Constructors
        constructors_div = tree.css_first('div#constructors')
        if constructors_div:
            for row in constructors_div.css('tr.normal-row'):
                cells = row.css('td')
                if len(cells) >= 3:
                    ctor_type = cells[0].text(strip=True)
                    ctor_name = cells[1].text(strip=True)
                    ctor_desc = cells[2].text(strip=True)
                    details['constructors'].append({
                        'type': ctor_type,
                        'name': ctor_name,
                        'description': ctor_desc
                    })
        t11 = time.time() if profile else None
        if profile: timings['constructors'] = t11-t10
        # Functions
        functions_div = tree.css_first('div#functions')
        if functions_div:
            for row in functions_div.css('tr.normal-row'):
                cells = row.css('td')
                if len(cells) >= 3:
                    func_type = cells[0].text(strip=True)
                    func_name = cells[1].text(strip=True)
                    func_desc = cells[2].text(strip=True)
                    details['functions'].append({
                        'type': func_type,
                        'name': func_name,
                        'description': func_desc
                    })
        t12 = time.time() if profile else None
        if profile: timings['functions'] = t12-t11
        # Constants
        constants_div = tree.css_first('div#constants')
        if constants_div:
            for row in constants_div.css('tr.normal-row'):
                cells = row.css('td')
                if len(cells) >= 2:
                    const_name = cells[0].text(strip=True)
                    const_desc = cells[1].text(strip=True)
                    details['constants'].append({
                        'name': const_name,
                        'description': const_desc
                    })
        t13 = time.time() if profile else None
        if profile: timings['constants'] = t13-t12
        # See Also
        see_also_div = tree.css_first('div#seealso')
        if see_also_div:
            for a in see_also_div.css('a[href]'):
                details['see_also'].append(a.attributes.get('href'))
        t14 = time.time() if profile else None
        if profile: timings['see_also'] = t14-t13
    except Exception as e:
        print(f"[ERROR] Failed to parse {class_page_path}: {e}")
    if profile_detailed:
        print(f"[PROFILE][CLASS] {os.path.basename(class_page_path)} timings: {timings}")
    return details

def process_class(class_tuple, api_docs_base_path, profile=False, profile_detailed=False):
    class_name, relative_url = class_tuple
    path_segment = relative_url.replace('../', '', 1)
    class_doc_dir = os.path.join(api_docs_base_path, path_segment)
    class_html_page_path = os.path.join(class_doc_dir, 'index.html')
    class_html_page_path = os.path.normpath(class_html_page_path)
    details = extract_class_details_full(class_html_page_path, profile=profile, profile_detailed=profile_detailed)
    details['class_name'] = class_name
    details['relative_url'] = relative_url
    return details

def process_class_with_base(args):
    # args: (class_tuple, api_docs_base_path, profile, profile_detailed)
    return process_class(*args)

def extract_all_class_details_parallel(max_workers=None, profile=False, profile_detailed=False, classes_index_path=None, output_path=None, batch_size=1000):
    project_root = get_project_root()
    classes_index_path = classes_index_path or os.path.join(project_root, 'en-US', 'API', 'Classes', 'index.html')
    json_output_dir = os.path.join(project_root, 'json_output')
    os.makedirs(json_output_dir, exist_ok=True)
    ndjson_path = output_path or os.path.join(json_output_dir, 'all_classes.ndjson')
    if not os.path.exists(classes_index_path):
        print(f"[ERROR] Classes index file not found: {classes_index_path}")
        return
    classes_data = extract_classes_from_index(classes_index_path)
    if not classes_data:
        print(f"[ERROR] No classes found in index file: {classes_index_path}")
        return
    api_docs_base_path = os.path.abspath(os.path.join(project_root, 'en-US', 'API'))
    t0 = time.time()
    total = len(classes_data)
    category = "Classes"
    write_counter_file(category, 0, "Parsing")
    buffer = []
    processed_count = 0
    batch_start = time.time() if profile else None
    with open(ndjson_path, 'w', encoding='utf-8') as ndjson_file:
        with ThreadPoolExecutor(max_workers=max_workers or os.cpu_count()) as executor:
            for i, result in enumerate(executor.map(process_class_with_base, ((tup, api_docs_base_path, profile, profile_detailed) for tup in classes_data)), 1):
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
        print(f"Processed {processed_count} classes in {t1-t0:.2f}s")

def main():
    parser = argparse.ArgumentParser(description="Extract Unreal API class documentation to NDJSON.")
    parser.add_argument('--max-workers', type=int, default=os.cpu_count(), help='Parallel workers (default: CPU count)')
    parser.add_argument('--profile', action='store_true', help='Enable timing/profiling output')
    parser.add_argument('--profile-detailed', action='store_true', help='Enable detailed per-class profiling output')
    parser.add_argument('--classes-index-path', type=str, default=None, help='Override classes index.html path')
    parser.add_argument('--output-path', type=str, default=None, help='Override output NDJSON path')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for NDJSON writes (default: 1000)')
    args = parser.parse_args()
    extract_all_class_details_parallel(max_workers=args.max_workers, profile=args.profile, profile_detailed=args.profile_detailed, classes_index_path=args.classes_index_path, output_path=args.output_path, batch_size=args.batch_size)

if __name__ == '__main__':
    main()
