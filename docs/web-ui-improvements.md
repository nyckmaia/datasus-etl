# Web UI Improvements - Melhoria 10

## Overview

Suggestions for improving the PyDataSUS web interface experience.

## Current State

The project currently provides:
- CLI interface (`datasus run`, `datasus status`, `datasus update`)
- Python API (`SihsusPipeline`, `SimPipeline`)

No web interface exists in the current codebase.

## Potential Web UI Options

### Option 1: Streamlit Dashboard

**Technology**: Streamlit (Python)

**Features**:
- Interactive parameter selection
- Real-time progress monitoring
- Data preview/visualization
- Export configuration

**Example Structure**:
```python
# app.py
import streamlit as st
from pydatasus import PipelineConfig, SihsusPipeline

st.title("PyDataSUS - Web Interface")

# Sidebar for configuration
with st.sidebar:
    subsystem = st.selectbox("Subsystem", ["sihsus", "sim"])
    start_date = st.date_input("Start Date")
    end_date = st.date_input("End Date")
    ufs = st.multiselect("UFs", ["SP", "RJ", "MG", ...])

# Run button
if st.button("Run Pipeline"):
    config = PipelineConfig.create(...)
    pipeline = SihsusPipeline(config)

    with st.spinner("Processing..."):
        result = pipeline.run()

    st.success(f"Exported {result.total_rows:,} rows!")
```

**Pros**:
- Pure Python (no JS needed)
- Quick to implement
- Good for data science workflows

**Cons**:
- Requires Streamlit server
- Limited customization
- Not ideal for production deployment

### Option 2: FastAPI + HTMX

**Technology**: FastAPI backend + HTMX frontend

**Features**:
- RESTful API for pipeline control
- Server-side rendering with HTMX
- Real-time updates via SSE
- Minimal JavaScript

**Example Structure**:
```
web/
├── api/
│   ├── __init__.py
│   ├── main.py          # FastAPI app
│   ├── routes/
│   │   ├── pipeline.py  # Pipeline endpoints
│   │   └── status.py    # Status endpoints
│   └── templates/
│       ├── base.html
│       ├── index.html
│       └── partials/
│           ├── progress.html
│           └── results.html
```

**Pros**:
- Modern, lightweight
- Good performance
- Easy to add API endpoints

**Cons**:
- Requires template design
- More files to maintain

### Option 3: Gradio Interface

**Technology**: Gradio

**Features**:
- Form-based input
- Output display
- Shareable links
- Minimal code

**Example**:
```python
import gradio as gr
from pydatasus import PipelineConfig, SihsusPipeline

def run_pipeline(subsystem, start_date, end_date, ufs):
    config = PipelineConfig.create(
        base_dir=Path("./data"),
        subsystem=subsystem,
        start_date=start_date,
        end_date=end_date,
        uf_list=ufs.split(","),
    )
    pipeline = SihsusPipeline(config)
    result = pipeline.run()
    return f"Exported {result.total_rows:,} rows"

iface = gr.Interface(
    fn=run_pipeline,
    inputs=[
        gr.Dropdown(["sihsus", "sim"], label="Subsystem"),
        gr.Textbox(label="Start Date (YYYY-MM-DD)"),
        gr.Textbox(label="End Date (YYYY-MM-DD)"),
        gr.Textbox(label="UFs (comma-separated)"),
    ],
    outputs="text",
    title="PyDataSUS Pipeline",
)
```

**Pros**:
- Extremely quick to implement
- Auto-generated UI
- Shareable demos

**Cons**:
- Limited customization
- Not suitable for complex workflows

### Option 4: Jupyter Widgets

**Technology**: ipywidgets + Jupyter

**Features**:
- Interactive notebooks
- Widget-based controls
- Inline visualization

**Example**:
```python
import ipywidgets as widgets
from IPython.display import display

subsystem = widgets.Dropdown(
    options=['sihsus', 'sim'],
    description='Subsystem:'
)
run_button = widgets.Button(description="Run Pipeline")
output = widgets.Output()

def on_run(b):
    with output:
        # Run pipeline
        pass

run_button.on_click(on_run)
display(subsystem, run_button, output)
```

**Pros**:
- Native Jupyter integration
- Good for exploration
- No server needed

**Cons**:
- Limited to Jupyter environment
- Not standalone

## Recommended Features

### 1. Configuration Form

- Subsystem selection (dropdown)
- Date range picker
- UF multi-select with "Select All"
- Output format (parquet/csv)
- Compression options
- Raw mode toggle

### 2. Progress Monitoring

```
Download: [████████░░] 80% (21/27 files)
Convert:  [██████████] 100%
Load:     [█████░░░░░] 50%
Transform: Pending
Export:   Pending
```

### 3. Results Summary

| Metric | Value |
|--------|-------|
| Total rows | 1,234,567 |
| Files created | 27 |
| Output size | 450 MB |
| Duration | 5m 32s |

### 4. Data Preview

- Sample rows from output
- Column statistics
- Schema display

### 5. Download/Export

- Direct download of Parquet/CSV
- Copy DuckDB query
- Export configuration as JSON

## Implementation Priority

| Feature | Priority | Effort |
|---------|----------|--------|
| Basic Streamlit app | High | Low |
| Configuration form | High | Low |
| Progress monitoring | Medium | Medium |
| Data preview | Medium | Medium |
| Results visualization | Low | High |
| Multi-user support | Low | High |

## Technical Considerations

### 1. Long-Running Tasks

Pipeline execution can take minutes. Solutions:
- Background tasks with Celery/RQ
- Server-Sent Events (SSE) for progress
- WebSocket for real-time updates

### 2. File Storage

Output files need storage. Options:
- Local filesystem (simple)
- Cloud storage (S3, GCS)
- Temporary with auto-cleanup

### 3. Authentication

For production deployment:
- Basic auth for simple protection
- OAuth for organization use
- API keys for programmatic access

### 4. Containerization

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY . .
RUN pip install -e .[web]

EXPOSE 8501
CMD ["streamlit", "run", "app.py"]
```

## Quick Start Implementation

### Minimal Streamlit App

```python
# web/app.py
import streamlit as st
from pathlib import Path
from pydatasus import PipelineConfig, SihsusPipeline, SimPipeline

st.set_page_config(page_title="PyDataSUS", layout="wide")
st.title("🏥 PyDataSUS - DataSUS ETL Pipeline")

col1, col2 = st.columns(2)

with col1:
    subsystem = st.selectbox(
        "Subsystem",
        ["sihsus", "sim"],
        format_func=lambda x: {"sihsus": "SIHSUS (Hospitalizações)", "sim": "SIM (Mortalidade)"}[x]
    )

with col2:
    output_format = st.selectbox("Output Format", ["parquet", "csv"])

col3, col4 = st.columns(2)

with col3:
    start_date = st.date_input("Start Date")

with col4:
    end_date = st.date_input("End Date")

ufs = st.multiselect(
    "UFs (estados)",
    ["AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA",
     "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN",
     "RS", "RO", "RR", "SC", "SP", "SE", "TO"],
    default=["SP"]
)

raw_mode = st.checkbox("Raw mode (no type conversions)")

if st.button("▶️ Run Pipeline", type="primary"):
    if not ufs:
        st.error("Please select at least one UF")
    else:
        config = PipelineConfig.create(
            base_dir=Path("./data/datasus"),
            subsystem=subsystem,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            uf_list=ufs,
            output_format=output_format,
            raw_mode=raw_mode,
        )

        PipelineClass = SihsusPipeline if subsystem == "sihsus" else SimPipeline
        pipeline = PipelineClass(config)

        with st.spinner(f"Running {subsystem.upper()} pipeline..."):
            result = pipeline.run()

        st.success(f"✅ Pipeline completed!")
        st.metric("Total Rows", f"{result.get_metadata('total_rows_exported', 0):,}")
        st.info(f"Output: {config.storage.parquet_dir}")
```

### Running

```bash
# Install streamlit
pip install streamlit

# Run app
streamlit run web/app.py
```

## Future Enhancements

1. **Job Queue**: Support multiple concurrent pipelines
2. **Scheduling**: Cron-like scheduled runs
3. **Notifications**: Email/Slack on completion
4. **Data Catalog**: Browse available datasets
5. **Query Builder**: Custom SQL on exported data
6. **Visualization**: Charts and dashboards
