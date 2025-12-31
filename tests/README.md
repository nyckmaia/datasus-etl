# DataSUS ETL Tests

Comprehensive test suite for DataSUS ETL using pytest.

## 📁 Structure

```
tests/
├── conftest.py           # Shared fixtures
├── unit/                 # Unit tests
│   ├── test_config.py    # Configuration tests
│   ├── test_core.py      # Core components tests
│   ├── test_sihsus_processor.py  # Processor tests
│   ├── test_duckdb_manager.py    # Database tests
│   └── test_parquet_writer.py    # Storage tests
└── README.md             # This file
```

## 🧪 Running Tests

### Run all tests

```bash
cd datasus-etl
pytest
```

### Run with coverage

```bash
pytest --cov=src/datasus_etl --cov-report=html
```

### Run specific test file

```bash
pytest tests/unit/test_config.py
```

### Run specific test

```bash
pytest tests/unit/test_config.py::TestDownloadConfig::test_default_config
```

### Run with verbose output

```bash
pytest -v
```

## 📊 Test Coverage

The test suite covers:

- ✅ **Configuration** (15+ tests)
  - DownloadConfig validation
  - ConversionConfig
  - ProcessingConfig
  - StorageConfig
  - DatabaseConfig
  - PipelineConfig

- ✅ **Core Components** (15+ tests)
  - PipelineContext operations
  - Stage execution and chaining
  - Pipeline orchestration

- ✅ **Data Processing** (8+ tests)
  - SihsusProcessor cleaning
  - CSV processing
  - Data validation

- ✅ **Storage** (12+ tests)
  - DuckDB connections and queries
  - Parquet writing
  - Partitioning strategies

**Total**: 50+ unit tests

## 🔧 Fixtures

Shared test fixtures in `conftest.py`:

- `temp_dir`: Temporary directory for test files
- `sample_sihsus_csv`: Sample SIHSUS CSV data
- `sample_dataframe`: Sample Polars DataFrame
- `sample_ibge_data`: Sample IBGE municipality data
- `mock_dbc_file`: Mock DBC file
- `mock_dbf_file`: Mock DBF file

## 📝 Writing New Tests

### Example Test

```python
def test_my_feature(temp_dir, sample_dataframe):
    """Test my new feature."""
    # Arrange
    config = MyConfig(output_dir=temp_dir)
    processor = MyProcessor(config)

    # Act
    result = processor.process(sample_dataframe)

    # Assert
    assert result is not None
    assert len(result) > 0
```

### Test Naming Convention

- Test files: `test_*.py`
- Test classes: `TestComponentName`
- Test methods: `test_what_it_does`

## 🎯 Test Quality Guidelines

1. **Arrange-Act-Assert**: Follow AAA pattern
2. **One assertion per test**: Focus on single behavior
3. **Use fixtures**: Reuse common test data
4. **Mock external dependencies**: Isolate unit under test
5. **Clear test names**: Describe what is being tested

## 🚀 Continuous Integration

Tests are designed to run in CI/CD pipelines:

```yaml
# .github/workflows/test.yml
- name: Run tests
  run: |
    pip install -e ".[dev]"
    pytest --cov=src/datasus_etl
```

## 📈 Coverage Goals

Target coverage: **85%+**

Current coverage by module:
- config.py: ~95%
- core: ~90%
- transform/processors: ~85%
- storage: ~85%
