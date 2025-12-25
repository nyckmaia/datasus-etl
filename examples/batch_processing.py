"""Batch processing example: Process data by state to save RAM.

When processing large volumes of data with limited RAM,
process one state (UF) at a time and append results to the same Parquet directory.

This approach:
- Reduces peak RAM usage significantly
- Allows processing 500GB+ data on systems with < 16GB RAM
- Enables parallel processing of different states on different machines
- Provides checkpoint/resume capability (skip already processed states)
"""

from pathlib import Path
from pydatasus.config import (
    PipelineConfig,
    DownloadConfig,
    ConversionConfig,
    ProcessingConfig,
    StorageConfig,
    DatabaseConfig,
)
from pydatasus.pipeline import SihsusPipeline
from pydatasus.constants import ALL_UFS


def main():
    """Process SIHSUS data one state at a time."""
    base_dir = Path("./data/datasus")
    parquet_dir = base_dir / "parquet_all_states"  # Single output directory

    # List of states to process (or use ALL_UFS for all 27 states)
    states_to_process = ["SP", "RJ", "MG", "BA", "RS", "PR"]  # Example subset
    # states_to_process = ALL_UFS  # Uncomment to process all states

    # Optional: Check which states are already processed
    already_processed = []
    if parquet_dir.exists():
        # Look for UF_ZI partitions
        uf_dirs = [d.name for d in parquet_dir.glob("*") if d.is_dir() and d.name.startswith("UF_ZI=")]
        already_processed = [d.split("=")[1] for d in uf_dirs]
        if already_processed:
            print(f"Already processed states: {already_processed}")

    print("="*60)
    print("PyDataSUS - Batch Processing by State")
    print("="*60)
    print(f"Total states to process: {len(states_to_process)}")
    print(f"Output directory: {parquet_dir}")
    print(f"Time period: 2020-01-01 to 2023-12-31")
    print("="*60)

    total_rows_all_states = 0

    for i, uf in enumerate(states_to_process, 1):
        # Skip if already processed
        if uf in already_processed:
            print(f"\n[{i}/{len(states_to_process)}] Skipping {uf} (already processed)")
            continue

        print(f"\n{'='*60}")
        print(f"[{i}/{len(states_to_process)}] Processing State: {uf}")
        print(f"{'='*60}")

        # Configure pipeline for this specific state
        # Use separate directories for intermediate files to avoid conflicts
        uf_base = base_dir / f"temp_{uf}"

        config = PipelineConfig(
            download=DownloadConfig(
                output_dir=uf_base / "dbc",
                start_date="2020-01-01",
                end_date="2023-12-31",
                uf_list=[uf],  # Process only this state
                override=False,
            ),
            conversion=ConversionConfig(
                dbc_dir=uf_base / "dbc",
                dbf_dir=uf_base / "dbf",
                csv_dir=uf_base / "csv",
                tabwin_dir=Path("C:/Program Files/TAB415"),
                override=False,
            ),
            processing=ProcessingConfig(
                input_dir=uf_base / "csv",
                output_dir=uf_base / "processed",
                override=False,
            ),
            storage=StorageConfig(
                parquet_dir=parquet_dir,  # SAME directory for all states!
                partition_cols=["ANO_INTER", "UF_ZI"],  # Partitioned by year and state
                compression="zstd",
                row_group_size=100_000,
            ),
            database=DatabaseConfig(
                chunk_size=10000  # Smaller chunks for limited RAM
            ),
        )

        try:
            # Run pipeline for this state
            pipeline = SihsusPipeline(config)
            result = pipeline.run()

            rows = result.get_metadata("total_rows_exported")
            total_rows_all_states += rows

            print(f"✓ {uf} completed: {rows:,} rows exported")

            # Optional: Cleanup intermediate files to save disk space
            cleanup_intermediate_files(uf_base)

        except Exception as e:
            print(f"✗ Failed to process {uf}: {e}")
            # Continue with next state instead of crashing entire batch
            continue

    # Summary
    print("\n" + "="*60)
    print("Batch Processing Completed!")
    print("="*60)
    print(f"Processed states: {len(states_to_process)}")
    print(f"Total rows exported: {total_rows_all_states:,}")
    print(f"Output directory: {parquet_dir}")
    print("="*60)

    # Now all states are in a single partitioned Parquet dataset
    # You can query all states together efficiently
    print("\nYou can now query all states together:")
    print(f"  engine = ParquetQueryEngine('{parquet_dir}')")
    print(f"  df = engine.sql('SELECT uf_zi, COUNT(*) FROM sihsus GROUP BY uf_zi')")


def cleanup_intermediate_files(uf_base: Path):
    """Remove intermediate DBC and DBF files to save disk space.

    Args:
        uf_base: Base directory for this state's intermediate files
    """
    import shutil

    # Only remove DBC and DBF, keep logs if they exist
    for subdir in ["dbc", "dbf"]:
        dir_path = uf_base / subdir
        if dir_path.exists():
            shutil.rmtree(dir_path)
            print(f"  Cleaned up {dir_path}")


def resume_from_failure():
    """Example: Resume batch processing after a failure.

    The batch processor automatically skips already-processed states
    by checking existing UF_ZI partitions in the Parquet directory.
    """
    base_dir = Path("./data/datasus")
    parquet_dir = base_dir / "parquet_all_states"

    if not parquet_dir.exists():
        print("No existing Parquet directory found. Nothing to resume.")
        return

    # Find which states are already done
    completed = []
    if parquet_dir.exists():
        uf_dirs = [d.name for d in parquet_dir.glob("*") if d.is_dir() and d.name.startswith("UF_ZI=")]
        completed = [d.split("=")[1] for d in uf_dirs]

    print(f"Already completed states: {completed}")
    print(f"Remaining states: {[uf for uf in ALL_UFS if uf not in completed]}")

    # Just run main() again - it will skip completed states
    print("\nResuming batch processing...")
    main()


def parallel_batch_processing():
    """Example: Process multiple states in parallel on different machines.

    Machine 1: Process SP, RJ, MG
    Machine 2: Process BA, RS, PR
    Machine 3: Process CE, PE, SC
    ...

    Each machine writes to the SAME shared Parquet directory (network drive).
    Hive partitioning ensures no conflicts (each state has its own partition).
    """
    import socket

    hostname = socket.gethostname()
    print(f"Running on: {hostname}")

    # Distribute states across machines
    state_assignments = {
        "machine1": ["SP", "RJ", "MG"],
        "machine2": ["BA", "RS", "PR"],
        "machine3": ["CE", "PE", "SC"],
        # ... assign remaining states
    }

    # Determine which states this machine should process
    assigned_states = state_assignments.get(hostname, [])

    if not assigned_states:
        print(f"No states assigned to {hostname}")
        return

    print(f"This machine will process: {assigned_states}")

    # Process assigned states
    # (Use same logic as main(), but with assigned_states instead of states_to_process)


if __name__ == "__main__":
    # Run the main batch processing example
    main()

    # Optional: Demonstrate resume functionality
    # resume_from_failure()

    # Optional: Parallel processing example
    # parallel_batch_processing()
