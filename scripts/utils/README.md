# Project Structure Validation Utility

This script checks that your project folders and key files are present and correctly organized, and warns about misplaced files.

## Usage

```bash
python scripts/utils/validate_project_structure.py
```

- Reports missing folders/files and misplaced files.
- Exits with code 0 if all is correct, 1 if any issues are found.

## What it checks
- All main folders (scripts, json_output, etc.)
- All expected scripts in their subfolders (extraction, scanning, processing, monitoring, utils)
- All main output files in `json_output/`
- Warns if files are misplaced in those folders

## Customization
- Edit the `EXPECTED_STRUCTURE` dictionary in the script to match your evolving project layout.

---

**Tip:** Run this after reorganizing or before pushing changes to keep your project clean and consistent.
