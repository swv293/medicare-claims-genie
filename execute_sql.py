"""
Execute SQL statements against Databricks SQL warehouse.
Reads SQL files and executes each statement via the Databricks CLI.
"""
import subprocess
import json
import sys
import time
import re

PROFILE = "fe-vm-fevm-serverless-stable-swv01"
WAREHOUSE = "084543d48aafaeb2"

def execute_sql(statement, label="", timeout=50):
    """Execute a single SQL statement via Databricks API."""
    # Clean up the statement
    statement = statement.strip().rstrip(';').strip()
    if not statement or statement.startswith('--'):
        return True

    payload = {
        "warehouse_id": WAREHOUSE,
        "statement": statement,
        "wait_timeout": "50s"
    }

    cmd = [
        "databricks", "api", "post", "/api/2.0/sql/statements",
        "--profile", PROFILE,
        "--json", json.dumps(payload)
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout+30)
        if result.returncode != 0:
            print(f"  ERROR ({label}): CLI error: {result.stderr[:200]}")
            return False

        resp = json.loads(result.stdout)
        state = resp.get('status', {}).get('state', 'UNKNOWN')

        if state == 'SUCCEEDED':
            return True
        elif state == 'PENDING' or state == 'RUNNING':
            # Poll for completion
            stmt_id = resp.get('statement_id')
            for _ in range(60):
                time.sleep(2)
                poll_cmd = ["databricks", "api", "get", f"/api/2.0/sql/statements/{stmt_id}", "--profile", PROFILE]
                poll_result = subprocess.run(poll_cmd, capture_output=True, text=True, timeout=30)
                if poll_result.returncode == 0:
                    poll_resp = json.loads(poll_result.stdout)
                    poll_state = poll_resp.get('status', {}).get('state', '')
                    if poll_state == 'SUCCEEDED':
                        return True
                    elif poll_state in ('FAILED', 'CANCELED', 'CLOSED'):
                        err = poll_resp.get('status', {}).get('error', {}).get('message', 'Unknown')
                        print(f"  ERROR ({label}): {err[:200]}")
                        return False
            print(f"  TIMEOUT ({label})")
            return False
        else:
            err = resp.get('status', {}).get('error', {}).get('message', f'State: {state}')
            print(f"  ERROR ({label}): {err[:200]}")
            return False
    except Exception as e:
        print(f"  EXCEPTION ({label}): {str(e)[:200]}")
        return False


def split_sql_statements(content):
    """Split SQL content into individual statements, handling complex SQL."""
    statements = []
    current = []
    in_string = False
    string_char = None
    i = 0

    while i < len(content):
        ch = content[i]

        # Handle string literals
        if ch in ("'", '"') and not in_string:
            in_string = True
            string_char = ch
            current.append(ch)
        elif ch == string_char and in_string:
            # Check for escaped quote
            if i + 1 < len(content) and content[i+1] == string_char:
                current.append(ch)
                current.append(content[i+1])
                i += 2
                continue
            in_string = False
            current.append(ch)
        elif ch == ';' and not in_string:
            stmt = ''.join(current).strip()
            if stmt and not stmt.startswith('--'):
                statements.append(stmt)
            current = []
        elif ch == '-' and i + 1 < len(content) and content[i+1] == '-' and not in_string:
            # Skip line comment
            while i < len(content) and content[i] != '\n':
                i += 1
            current.append('\n')
        else:
            current.append(ch)
        i += 1

    # Handle last statement
    stmt = ''.join(current).strip()
    if stmt and not stmt.startswith('--'):
        statements.append(stmt)

    return statements


def main():
    sql_file = sys.argv[1] if len(sys.argv) > 1 else "create_tables.sql"

    print(f"Reading {sql_file}...")
    with open(sql_file, 'r') as f:
        content = f.read()

    statements = split_sql_statements(content)
    print(f"Found {len(statements)} SQL statements to execute")

    success = 0
    failed = 0

    for i, stmt in enumerate(statements):
        # Create a label from first ~60 chars
        label = stmt[:60].replace('\n', ' ').strip()
        progress = f"[{i+1}/{len(statements)}]"
        print(f"{progress} Executing: {label}...")

        if execute_sql(stmt, label):
            success += 1
        else:
            failed += 1
            # Print the failed statement for debugging
            if '--debug' in sys.argv:
                print(f"  Failed SQL: {stmt[:300]}...")

    print(f"\nDone: {success} succeeded, {failed} failed out of {len(statements)} total")
    return 0 if failed == 0 else 1

if __name__ == '__main__':
    main()
