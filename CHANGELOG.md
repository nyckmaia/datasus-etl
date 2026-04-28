# Changelog

## [0.1.13] - 2026-04-28

### Added
- enhance Query page with sidebar functionality and SQL editor improvements

## [0.1.12] - 2026-04-27

### Added
- update version to 0.1.12 and enhance SQL query endpoint documentation

## [0.1.11] - 2026-04-25

### Added
- integrate i18next for internationalization across multiple pages

### Other
- **release:** rebuild site for v0.1.10

## [0.1.10] - 2026-04-25

### Added
- add version check endpoint and update UI for notifications

### Other
- **release:** rebuild site for v0.1.9

## [0.1.9] - 2026-04-25

### Fixed
- **build:** include datasus_etl.web package to prevent ModuleNotFoundError
- **windows:** bundle rich._unicode_data; default desktop icon; i18n README

### Other
- **release:** rebuild site for v0.1.8

## [0.1.8] - 2026-04-24

### Other
- **release:** bump version to 0.1.8 and update build settings
- **release:** rebuild site for v0.1.7

## [0.1.7] - 2026-04-24

### Documentation
- **release:** document PyPI + Pages one-time setup for maintainers

### Other
- **release:** bump version to 0.1.7
- **pkg:** fill in PyPI metadata (author, URLs) before first publish
- **release:** rebuild site for v0.1.6

## [0.1.6] - 2026-04-24

### Other
- add TODO.md to .gitignore
- **site:** clinical-instrument aesthetic, dark-first with theme toggle
- **release:** rebuild site for v0.1.5 [skip ci]

## [0.1.5] - 2026-04-23

### Fixed
- untrack mkdocs-era /site ignore, add Astro source, sync docs to v0.1.4

### Other
- bump version to 0.1.5

## [0.1.4] - 2026-04-23

### Added
- add version synchronization and build scripts
- **web-ui:** implement subsystem selection in download steps and enhance navigation
- enhance SIM filename parsing and update FTP date range checks
- enhance settings and directory management
- new React+FastAPI web UI, fix double-nested datasus_db storage path
- Update storage paths to use 'datasus_db' for Parquet files and enhance compatibility with legacy structures
- Add SIM descriptive mappings and enhance VIEW creation for categorical fields
- Add new fields to SIM schema and update CID array transformation for maternal cause
- Implement custom BOOLEAN mappings and enhance CID array transformation for SIM subsystem
- Enhance SIM data processing with CID array transformation and subsystem-specific mappings
- Enhance SIM data processing with IDADE field transformation
- Add upload command for MotherDuck integration
- Implement TODO.md improvements (tasks 1-3)
- Update TODO with new features and enhancements
- Implement Unicode support and enhance logging in CLI and Web Interface
- Integrate progress callback with Web Interface
- Implement TODO improvements (CID cleanup, logs, cancellation, progress)
- **I8:** Implement memory-aware processing for large datasets
- **I7:** Improve Query page data dictionary with types and null stats
- **I6:** Improve Status page with tables instead of charts
- **I4:** Add download-estimate command to preview file sizes
- **I3:** Improve DBC conversion with CSV output and single file support
- **I5:** Rename CLI commands run→pipeline, download→download-only
- **I2:** Configure Streamlit to skip email prompt on first run
- **I1:** Rename project from pydatasus to datasus-etl
- Refactor project structure and enhance Web Interface functionality
- Update dataframe display settings to use 'stretch' width for improved layout
- Add CID validation transform for SQL data processing
- Enhance SQL transformation and validation, update web date inputs
- **web:** Enhance SQL templates and editor functionality in web UI
- **web:** Improve web UI for health researchers
- Enhance Bash permissions with additional commands for modular transformations
- Use original DBC filename for exported files (Melhoria 04)
- Add --output-format option for CSV export (Melhoria 03)
- Add --raw option for export without type conversions (Melhoria 05)
- Add modular SQL transform system (Melhoria 07)
- Add TODO.md for pipeline performance improvements and enhancements
- Update permissions and ignore virtual environment files
- Add SIM pipeline and base architecture (Melhoria 04)
- Add pre-download report with confirmation (Melhoria 03)
- Improve CLI UX with required params and examples (Melhoria 02)
- Add automatic cleanup of temporary DBC/DBF files (Melhoria 01)
- Add git reset command to permissions in settings.local.json
- Add IBGE municipality enrichment (Melhoria 02)
- Add automatic cleanup of temporary DBC/DBF files (Melhoria 01)
- Update .gitignore to include additional file types and add new IBGE report
- Add Streamlit web interface command to CLI
- Add Streamlit web interface (Melhoria 07)
- Add unit and integration tests (Melhoria 06)
- Add incremental update support (Melhoria 05)
- Modularize dataset configs with base class (Melhoria 04)
- Create datasus CLI with typer (Melhoria 03)
- Add source_file column to track origin DBC file (Melhoria 02)
- Add subsystem folder structure (Melhoria 01)
- Add TODO.md to .gitignore to prevent tracking of TODO file
- Add Hive-partitioned Parquet export with canonical schema
- Enhance configuration and processing pipeline with new constants and improved SQL transformations
- Add SQL auto-generation helpers to SIHSUS schema
- Add SIHSUS Parquet schema definition with DuckDB types
- Replace TABWIN with datasus-dbc for cross-platform support
- Add Phase 2 optimizations and comprehensive examples
- Optimize pipeline with DuckDB streaming and SQL transformations

### Fixed
- **nuitka:** compile package dir (not __main__.py); Windows 4-part version
- **web-ui:** drop postcss.config.js from tsconfig.node includes, add bun.lock
- Correct MotherDuck upload to use direct connection
- Use subprocess + log file for Web Interface terminal output
- Use module-level dict for thread-safe progress updates
- Use st.session_state + st.rerun() for Web Interface progress
- Use threading for real-time progress updates in Web Interface
- Update PyPI metadata with expanded keywords and classifiers (Melhoria 06)
- Update repository URL in README for cloning instructions
- Delete empty dbc/ and dbf/ directories after cleanup
- Remove typer[all] extra to fix pip installation warning
- Use rglob for recursive cleanup of DBC/DBF files
- Correct IBGE code extraction and remove duplicate columns
- Use rglob for recursive cleanup of DBC/DBF files

### Performance
- Optimize data transfer using Parquet instead of row-by-row inserts
- Add adaptive DBF insertion strategy for optimal performance

### Changed
- Replace progress bar with terminal output in Web Interface
- Remove redundant output of generated files in basic usage example
- Remove PHASE3_SUMMARY.md as it is no longer needed
- Remove check_transform.py script as it is no longer needed
- Clean up export logging and remove unused date extraction - Remove file size control at 512MB per file (unsupported in Duckdb)

### Documentation
- **I8:** Add RAM analysis report for pipeline memory optimization
- Add research documents for improvements 01, 02, 09, 10
- Add example scripts for new features (Melhoria 08)
- Update README.md with complete usage examples (Melhoria 03)
- Add schema usage examples
- Remove all CSV references and legacy migration guide
- Add Phase 3 summary and update Phase 2 references
- Update README for optimized architecture
- Add comprehensive Phase 2 summary
- Add deprecation warnings to legacy modules
- Add usage example and optimization summary

### Other
- update version to 0.1.2
- (feat): Try the first cloud build
- Remove outdated documentation and improve user experience with new tutorial and web UI enhancements
- fixÇ commit web-ui lib modules
- Better UI
- Fix datasus CLI
- Switch storage from duckdb to parquet
- Refactor DataSUS ETL: Replace Parquet with DuckDB integration
- Update README.md to correct descriptions, enhance CLI usage examples, and add memory-aware processing details
- Rename project from "pydatasus" to "datasus-etl" and update related documentation; add DuckDB CLI integration in the command line interface.
- Fix in Paulo's house
- Enhance ParquetQueryEngine and SQLTransformer for improved schema handling and error logging
- Update examples to remove CSV references and reset version to 0.1.0
- Remove deprecated code and unused dependencies (v2.0)
- Init repo
- Initial commit

