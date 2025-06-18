import os
import json
import threading
import time
from typing import Dict, Optional
from rich.live import Live
from rich.table import Table
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn
import humanize

# Global counter storage
_counters = {
    'Editor': 0,
    'Developer': 0,
    'Plugins': 0,
    'Runtime': 0
}
_statuses = {
    'Editor': 'Idle',
    'Developer': 'Idle',
    'Plugins': 'Idle',
    'Runtime': 'Idle'
}
_heartbeats = {
    'Editor': 0,
    'Developer': 0,
    'Plugins': 0,
    'Runtime': 0
}
_counter_lock = threading.Lock()
_display_running = False
_display_thread = None

COUNTER_DIR = 'counter_files'  # Directory to store counter files
os.makedirs(COUNTER_DIR, exist_ok=True)
console = Console()

HEARTBEAT_TIMEOUT = 30  # seconds

# --- Counter/Status API ---
def update_counter(category: str, count: int, status: Optional[str] = None):
    """Update counter and status for a category (in-memory and file)"""
    global _counters, _statuses, _heartbeats
    now = int(time.time())
    with _counter_lock:
        _counters[category] = count
        if status:
            _statuses[category] = status
        _heartbeats[category] = now
    # Write to file for cross-process visibility
    with open(os.path.join(COUNTER_DIR, f'counter_{category}.json'), 'w', encoding='utf-8') as f:
        json.dump({'count': count, 'status': _statuses[category], 'heartbeat': now}, f)

def get_counters() -> Dict[str, int]:
    """Get current counters (from files if available)"""
    counters = {}
    for category in _counters.keys():
        file_path = os.path.join(COUNTER_DIR, f'counter_{category}.json')
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    counters[category] = data.get('count', 0)
            except Exception:
                counters[category] = 0
        else:
            counters[category] = 0
    return counters

def get_statuses() -> Dict[str, str]:
    statuses = {}
    for category in _statuses.keys():
        file_path = os.path.join(COUNTER_DIR, f'counter_{category}.json')
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    statuses[category] = data.get('status', 'Idle')
            except Exception:
                statuses[category] = 'Idle'
        else:
            statuses[category] = 'Idle'
    return statuses

def get_heartbeats() -> Dict[str, int]:
    heartbeats = {}
    for category in _heartbeats.keys():
        file_path = os.path.join(COUNTER_DIR, f'counter_{category}.json')
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    heartbeats[category] = data.get('heartbeat', 0)
            except Exception:
                heartbeats[category] = 0
        else:
            heartbeats[category] = 0
    return heartbeats

# --- Display ---
def _make_counter_table(counters, statuses, heartbeats):
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Category")
    table.add_column("Created", justify="right")
    table.add_column("Status", justify="left")
    table.add_column("Last Update", justify="right")
    now = int(time.time())
    for name in counters.keys():
        count = counters[name]
        status = statuses[name]
        hb = heartbeats[name]
        if hb == 0:
            last = "-"
        else:
            ago = now - hb
            last = f"{humanize.naturaltime(ago)}" if ago < 3600 else time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(hb))
            if ago > HEARTBEAT_TIMEOUT:
                status = f"[red]Stale ({status})[/red]"
        table.add_row(name, str(count), status, last)
    return table

def start_counter_display():
    """Start displaying counters using rich Live"""
    global _display_running, _display_thread
    _display_running = True
    _display_thread = threading.Thread(target=_display_loop, daemon=True)
    _display_thread.start()

def stop_counter_display():
    """Stop displaying counters"""
    global _display_running, _display_thread
    _display_running = False
    if _display_thread:
        _display_thread.join()
    print()  # Final newline

def _display_loop():
    """Internal display loop using rich Live"""
    with Live(console=console, refresh_per_second=2, transient=True) as live:
        while _display_running:
            counters = get_counters()
            statuses = get_statuses()
            heartbeats = get_heartbeats()
            table = _make_counter_table(counters, statuses, heartbeats)
            live.update(Panel(table, title="[bold green]Global Extraction Progress", border_style="green"))
            time.sleep(0.5)

# File-based functions for compatibility
def write_counter_file(category: str, count: int, status: Optional[str] = None):
    """Write counter and status (updates in-memory and file)"""
    update_counter(category, count, status)

def read_all_counters() -> Dict[str, int]:
    """Read all counters (from files)"""
    return get_counters()

def cleanup_counter_files():
    """Remove all counter files"""
    for category in _counters.keys():
        file_path = os.path.join(COUNTER_DIR, f'counter_{category}.json')
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
            except Exception:
                pass
