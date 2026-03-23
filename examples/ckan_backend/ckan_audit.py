import subprocess
from datetime import datetime


REPORT_FILE = "CKAN_Backend_Report.md"


TEST_DESCRIPTIONS = """
# CKAN Backend Audit Report

## Overview

This report validates the CKAN backend integration within DSI.

It confirms:
- Backend loads correctly
- CKAN API connection works
- Data ingestion functions
- Query system works
- Find functions operate correctly
- Backend remains read-only

---

## Test Descriptions

### 1. test_ckan_backend_load
Ensures CKAN backend loads successfully inside DSI.

### 2. test_ckan_ingest
Verifies:
- CKAN API connectivity
- Metadata retrieval
- Dataset/resource caching

### 3. test_ckan_data_retrieved
Confirms that real data is returned from the CKAN catalog.

### 4. test_ckan_query
Tests backend query functionality on cached metadata.

### 5. test_ckan_find
Validates search capability (table, column, cell matching).

---

"""


def run_tests():
    result = subprocess.run(
        ["pytest", "-v", "examples/ckan_backend/"],
        capture_output=True,
        text=True
    )
    return result.stdout + "\n" + result.stderr


def generate_report():
    test_output = run_tests()

    with open(REPORT_FILE, "w") as f:
        f.write(TEST_DESCRIPTIONS)
        f.write("\n\n")
        f.write("## Execution Results\n")
        f.write(f"\nGenerated: {datetime.now()}\n\n")
        f.write("```\n")
        f.write(test_output)
        f.write("\n```\n")

    print(f"Markdown report generated: {REPORT_FILE}")


if __name__ == "__main__":
    generate_report()