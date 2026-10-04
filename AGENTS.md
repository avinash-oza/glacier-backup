# Project guidance

## Python tests

- Tests use pytest. Add test modules named `test_*.py` under `tests/` so default pytest discovery finds them.
- Install the development dependencies with `uv sync --group dev`.
- Run the suite with `uv run pytest` from the repository root.
- Measure `FileData` line and branch coverage with `uv run pytest --cov=glacier_backup.file_data --cov-branch --cov-report=term-missing tests/test_filedata.py`.
- Prefer plain test functions, built-in `assert` statements, pytest fixtures such as `tmp_path`, and `pytest.mark.parametrize` for related cases.
- Mark intentionally deferred tests with `@pytest.mark.skip(reason="...")` and keep the reason current.
- Keep test data in `tests/` and locate it relative to the test module rather than relying on the process working directory.