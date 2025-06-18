import orjson
import re
import sys
import argparse
import time
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from log_helper import write_counter_file

NDJSON_FILE = 'all_entities_deduped.ndjson'
OUTPUT_FILE = 'all_entities_sanitized.ndjson'

# Define required fields and default values
REQUIRED_FIELDS = [
    'name', 'short_description', 'navigation', 'module', 'header', 'include', 'syntax', 'remarks', 'entity_type', 'source_path'
]
DEFAULTS = {
    'name': '',
    'short_description': '',
    'navigation': [],
    'module': '',
    'header': '',
    'include': '',
    'syntax': '',
    'remarks': '',
    'entity_type': 'unknown',
    'source_path': ''
}

# Optionally, define a function to clean up text fields
RE_MULTISPACE = re.compile(r'\s+')
BATCH_SIZE = 10000
def clean_text(text):
    if not isinstance(text, str):
        return ''
    text = text.strip()
    text = RE_MULTISPACE.sub(' ', text)
    return text

def sanitize_entity(entity):
    sanitized = {}
    for field in REQUIRED_FIELDS:
        value = entity.get(field, DEFAULTS[field])
        if isinstance(value, str):
            value = clean_text(value)
        elif field == 'navigation' and not isinstance(value, list):
            value = []
        sanitized[field] = value
    return sanitized

def process_batch(lines):
    sanitized_lines = []
    errors = 0
    for i, line in enumerate(lines):
        try:
            entity = orjson.loads(line)
            sanitized = sanitize_entity(entity)
            sanitized_lines.append(orjson.dumps(sanitized).decode('utf-8'))
        except Exception as e:
            errors += 1
    return sanitized_lines, errors

def main():
    parser = argparse.ArgumentParser(description='Sanitize entities NDJSON file.')
    parser.add_argument('--input', default='all_entities_deduped.ndjson', help='Input NDJSON file')
    parser.add_argument('--output', default=os.path.join('json_output', 'all_entities_sanitized.ndjson'), help='Output NDJSON file')
    parser.add_argument('--profile', action='store_true', help='Enable profiling output')
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    start_time = time.time()
    total = 0
    errors = 0
    batch_times = []
    if args.profile:
        print(f"[DEBUG] Sanitizing entities from {args.input} to {args.output}...")
    write_counter_file("Sanitize", 0, "Sanitizing")
    with open(args.input, 'r', encoding='utf-8') as inp, open(args.output, 'w', encoding='utf-8') as out:
        lines_batch = []
        batch_idx = 0
        with ProcessPoolExecutor() as executor:
            futures = []
            for i, line in enumerate(inp, 1):
                lines_batch.append(line)
                if len(lines_batch) == BATCH_SIZE:
                    futures.append(executor.submit(process_batch, lines_batch))
                    lines_batch = []
                total = i
            if lines_batch:
                futures.append(executor.submit(process_batch, lines_batch))
            for batch_num, future in enumerate(as_completed(futures), 1):
                batch_start = time.time()
                sanitized_lines, batch_errors = future.result()
                out.write('\n'.join(sanitized_lines) + '\n')
                errors += batch_errors
                batch_end = time.time()
                batch_times.append(batch_end - batch_start)
                if args.profile:
                    print(f"[PROFILE] Batch {batch_num}: {len(sanitized_lines)} lines sanitized in {batch_end - batch_start:.2f}s. Total errors: {errors}")
                write_counter_file("Sanitize", batch_num * BATCH_SIZE, "Sanitizing")
    end_time = time.time()
    if args.profile:
        print(f"[PROFILE] Sanitization complete. Total lines: {total}. Errors: {errors}. Time: {end_time - start_time:.2f}s. Avg batch: {sum(batch_times)/len(batch_times):.2f}s")
    write_counter_file("Sanitize", total, "Done")

if __name__ == '__main__':
    main()
