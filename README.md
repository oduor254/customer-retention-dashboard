# Customer Retenetion

Short description
-----------------
This repository contains Python scripts and simple templates used to analyze customer retention and verify data quality from an API and cached CSVs. 
It includes utilities for inspecting columns, validating API responses, and running quick retention logic checks.

Quick start
-----------
1. Create a virtual environment (recommended):

   ```bash
   python -m venv .venv
   .\.venv\Scripts\activate   # Windows
   pip install -r requirements.txt
   ```

2. Explore or run scripts (examples):

   - `verify_api.py` — verify an API response against sample JSON
   - `analyze_cache.py` — analyze cached data
   - `verify_rejects_count.py` — check reject counts and discrepancies
   - `verify_retention_logic.py` — run retention logic checks

Tests
-----
Run tests with `pytest`:

```bash
pytest -q
```

Project layout
--------------
- `data.py` — core data helpers
- `analyze_cache.py` — cache analysis workflow
- `verify_api.py`, `verify_json.py` — API/JSON verification
- `debug_*.py` — helper scripts for debugging columns and sheets
- `templates/` — simple HTML templates used for rendering results
- `test_*.py` — small pytest files used for unit/integration checks

Notes & conventions
-------------------
- Files use UTF-8; `requirements_utf8.txt` may help for encoding-specific installs.
- The workspace folder name contains a typographical error ("Retenetion"). Consider renaming to "Retention" if you want a clean repository name on GitHub.

Contributing
------------
Open issues or send PRs. If contributing, run the tests locally and include a short description of your change.

Contact
-------
If you need help or have questions, open an issue on the repository or reach out to the maintainer listed on your GitHub profile.
