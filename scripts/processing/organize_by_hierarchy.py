import os
import sys
import argparse
import orjson
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
import time
# Robust import for log_helper
import importlib.util
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

def main():
    parser = argparse.ArgumentParser(description='Organize entities by class hierarchy and output NDJSON.')
    parser.add_argument('--profile', action='store_true', help='Enable profiling output')
    parser.add_argument('--hierarchy', default=None, help='Path to class_hierarchy.json')
    parser.add_argument('--input', default=None, help='Path to all_entities_sanitized.ndjson')
    parser.add_argument('--output', default=None, help='Path to all_entities_hierarchical.ndjson')
    args = parser.parse_args()

    project_root = get_project_root()
    hierarchy_file = args.hierarchy or os.path.join(project_root, 'json_output', 'class_hierarchy.ndjson')
    input_ndjson = args.input or os.path.join(project_root, 'json_output', 'all_entities_sanitized.ndjson')
    output_ndjson = args.output or os.path.join(project_root, 'json_output', 'all_entities_hierarchical.ndjson')
    BATCH_SIZE = 10000
    MAX_WORKERS = 8
    category = "Hierarchy"

    t0 = time.time() if args.profile else None
    # Robust error handling for hierarchy file
    if not os.path.exists(hierarchy_file):
        print(f"[ERROR] Hierarchy file not found: {hierarchy_file}")
        sys.exit(1)
    if args.profile:
        print(f"[DEBUG] Loading class hierarchy from {hierarchy_file}...")
    # Load flat NDJSON class hierarchy
    class_nodes = {}
    class_children = defaultdict(list)
    with open(hierarchy_file, 'r', encoding='utf-8') as f:
        for line in f:
            entry = orjson.loads(line)
            class_nodes[entry['name']] = entry
            for child in entry.get('children', []):
                class_children[entry['name']].append(child)
    if args.profile:
        print(f"[DEBUG] Indexed {len(class_nodes)} classes from hierarchy NDJSON.")

    # Robust error handling for input NDJSON
    if not os.path.exists(input_ndjson):
        print(f"[ERROR] Input NDJSON file not found: {input_ndjson}")
        sys.exit(1)
    # Load all entities in parallel and optimize memory
    if args.profile:
        t1 = time.time()
        print(f"[DEBUG] Loading sanitized entities from {input_ndjson} in parallel...")
    entities = {}
    non_class_entities = []
    class_names_set = set(class_nodes.keys())
    total_entities = 0
    def process_line(line):
        entity = orjson.loads(line)
        name = entity.get('name')
        if name:
            if entity.get('entity_type') == 'class':
                return ('class', name, entity)
            else:
                return ('nonclass', name, entity)
        return (None, None, None)
    with open(input_ndjson, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    # Use ProcessPoolExecutor for parallel entity processing
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(process_line, lines, chunksize=BATCH_SIZE))
        for typ, name, entity in results:
            if typ == 'class':
                entities[name] = entity
            elif typ == 'nonclass':
                non_class_entities.append(entity)
            total_entities += 1
    if args.profile:
        t2 = time.time()
        print(f"[DEBUG] Loaded {total_entities} entities. Entity load time: {t2-t1:.2f}s")

    # Pre-index related entities for each class
    if args.profile:
        t3 = time.time()
        print("[DEBUG] Indexing related entities by class name...")
    class_related_entities = defaultdict(list)
    for e in non_class_entities:
        nav = e.get('navigation', [])
        for nav_name in nav:
            if nav_name in class_names_set:
                class_related_entities[nav_name].append(e)
    if args.profile:
        t4 = time.time()
        print(f"[DEBUG] Indexed related entities for {len(class_related_entities)} classes. Related index time: {t4-t3:.2f}s")

    # Write hierarchical NDJSON in batches with buffer
    if args.profile:
        t5 = time.time()
        print(f"[DEBUG] Writing hierarchical NDJSON to {output_ndjson} in batches...")
    write_counter_file(category, 0, "Parsing")
    with open(output_ndjson, 'w', encoding='utf-8', buffering=1024*1024) as out:
        written = set()
        buffer = []
        def flush_buffer():
            if buffer:
                out.write(''.join(buffer))
                buffer.clear()
        def write_class_and_children(node_name, depth=0):
            node = class_nodes[node_name]
            # Write the class entity if it exists
            if node_name in entities:
                buffer.append(orjson.dumps(entities[node_name]).decode('utf-8') + '\n')
                written.add(node_name)
                if len(written) % 1000 == 0:
                    print(f"[PROFILE] Written {len(written)} classes...") if args.profile else None
                    write_counter_file(category, len(written), "Parsing")
                if len(buffer) >= BATCH_SIZE:
                    flush_buffer()
            # Write related entities (functions, etc.)
            related = class_related_entities.get(node_name, [])
            for e in related:
                buffer.append(orjson.dumps(e).decode('utf-8') + '\n')
                if len(buffer) >= BATCH_SIZE:
                    flush_buffer()
            # Recurse for children
            for child_name in node.get('children', []):
                if child_name in class_nodes:
                    write_class_and_children(child_name, depth+1)
        # Find root nodes (classes with no parent)
        all_children = set()
        for entry in class_nodes.values():
            all_children.update(entry.get('children', []))
        root_nodes = [name for name in class_nodes if name not in all_children]
        for root_name in root_nodes:
            write_class_and_children(root_name)
        # Write any remaining entities not attached to the hierarchy
        for e in entities.values():
            if e['name'] not in written:
                buffer.append(orjson.dumps(e).decode('utf-8') + '\n')
                if len(buffer) >= BATCH_SIZE:
                    flush_buffer()
        # Write non-class entities not attached to any class
        attached = set()
        for rels in class_related_entities.values():
            for e in rels:
                attached.add(id(e))
        for e in non_class_entities:
            if id(e) not in attached:
                buffer.append(orjson.dumps(e).decode('utf-8') + '\n')
                if len(buffer) >= BATCH_SIZE:
                    flush_buffer()
        flush_buffer()
    write_counter_file(category, len(written), "Done")
    if args.profile:
        t6 = time.time()
        print(f"[PROFILE] Hierarchical NDJSON written to {output_ndjson}. Time: {t6-t5:.2f}s (Total: {t6-t0:.2f}s)")

if __name__ == '__main__':
    main()
