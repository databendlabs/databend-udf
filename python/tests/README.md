# Running the Test Suite

Use the project virtualenv and Pytest:

```bash
python/venv/bin/python -m pytest
```

For targeted runs, point to the desired file or test case, e.g.:

```bash
python/venv/bin/python -m pytest python/tests/test_stage_integration.py::test_stage_integration_single_stage
```
