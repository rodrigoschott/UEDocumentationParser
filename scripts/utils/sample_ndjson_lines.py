import random

NDJSON_FILE = 'all_entities_hierarchical.ndjson'
SAMPLE_SIZE = 10  # Number of random lines to print

# First, count the number of lines
with open(NDJSON_FILE, 'r', encoding='utf-8') as f:
    total_lines = sum(1 for _ in f)

# Pick random line numbers
sample_lines = sorted(random.sample(range(total_lines), SAMPLE_SIZE))

print(f"Sampling {SAMPLE_SIZE} random lines from {NDJSON_FILE} (total lines: {total_lines})\n")

with open(NDJSON_FILE, 'r', encoding='utf-8') as f:
    current = 0
    target_idx = 0
    for i, line in enumerate(f):
        if target_idx >= len(sample_lines):
            break
        if i == sample_lines[target_idx]:
            print(f"Line {i+1}: {line.strip()[:500]}")  # Print up to 500 chars per line
            target_idx += 1
