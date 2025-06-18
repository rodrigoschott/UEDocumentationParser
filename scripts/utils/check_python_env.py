import sys
import os
import subprocess

REQUIRED_MAJOR = 3
REQUIRED_MIN = 10
REQUIRED_MAX = 11

REQUIREMENTS = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'requirements.txt')


def main():
    print(f"[INFO] Detected Python: {sys.executable}")
    print(f"[INFO] Python version: {sys.version}")
    major, minor = sys.version_info[:2]
    if not (major == REQUIRED_MAJOR and REQUIRED_MIN <= minor <= REQUIRED_MAX):
        print(f"[ERROR] Python {REQUIRED_MIN} or {REQUIRED_MAX} required. You are using {major}.{minor}.")
        print(f"[ACTION] Please install Python {REQUIRED_MIN} or {REQUIRED_MAX} and create a new virtual environment:")
        print(f"    py -{REQUIRED_MIN} -m venv .venv    # or py -{REQUIRED_MAX} -m venv .venv")
        print(f"    .\\.venv\\Scripts\\activate")
        print(f"    pip install -r scripts/requirements.txt")
        sys.exit(1)
    # Check if venv is active
    if not hasattr(sys, 'real_prefix') and sys.prefix == sys.base_prefix:
        print("[WARNING] You are not in a virtual environment!")
        print("[ACTION] Run:")
        print("    py -3.10 -m venv .venv    # or py -3.11 -m venv .venv")
        print("    .\\.venv\\Scripts\\activate")
        print("    pip install -r scripts/requirements.txt")
    else:
        print("[INFO] Virtual environment detected.")
    # Check requirements
    print("[INFO] Checking requirements...")
    try:
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-r', REQUIREMENTS])
        print("[INFO] Requirements are installed and up to date.")
    except Exception as e:
        print(f"[ERROR] Failed to install requirements: {e}")
        sys.exit(1)
    print("[SUCCESS] Python environment is ready.")

if __name__ == '__main__':
    main()
