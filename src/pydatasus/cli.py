"""Command-line interface for PyDataSUS."""

import logging
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.logging import RichHandler

from pydatasus import __version__
from pydatasus.config import (
    ConversionConfig,
    DatabaseConfig,
    DownloadConfig,
    PipelineConfig,
    ProcessingConfig,
    StorageConfig,
)
from pydatasus.download.ftp_downloader import FTPDownloader
from pydatasus.pipeline.sihsus_pipeline import SihsusPipeline
from pydatasus.transform.converters.dbc_to_dbf import DbcToDbfConverter

app = typer.Typer(
    name="pydatasus",
    help="Pipeline profissional para dados do DATASUS/SIHSUS",
    add_completion=False,
)
console = Console()


def setup_logging(level: str = "INFO") -> None:
    """Configure logging."""
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console, rich_tracebacks=True)],
    )


@app.command()
def version() -> None:
    """Show version information."""
    console.print(f"PyDataSUS version {__version__}")


@app.command()
def download(
    output_dir: Path = typer.Option(
        "./data/datasus/dbc",
        "--output-dir",
        "-o",
        help="Diretório de saída",
    ),
    start_date: str = typer.Option(
        "2000-01-01",
        "--start-date",
        help="Data inicial (YYYY-MM-DD)",
    ),
    end_date: Optional[str] = typer.Option(
        None,
        "--end-date",
        help="Data final (YYYY-MM-DD)",
    ),
    ufs: Optional[str] = typer.Option(
        None,
        "--ufs",
        help="UFs separadas por vírgula (ex: SP,RJ,MG)",
    ),
    override: bool = typer.Option(
        False,
        "--override",
        help="Sobrescrever arquivos existentes",
    ),
) -> None:
    """Download SIHSUS data from DATASUS FTP."""
    setup_logging()

    console.print("\n[bold blue]PyDataSUS - Download FTP[/bold blue]\n")

    uf_list = [uf.strip() for uf in ufs.split(",")] if ufs else None

    config = DownloadConfig(
        output_dir=output_dir,
        start_date=start_date,
        end_date=end_date,
        uf_list=uf_list,
        override=override,
    )

    downloader = FTPDownloader(config)
    files = downloader.download()

    console.print(f"\n[green]✓ Download concluído: {len(files)} arquivos[/green]")


@app.command()
def convert_dbc(
    input_dir: Path = typer.Argument(..., help="Diretório com arquivos DBC"),
    output_dir: Path = typer.Argument(..., help="Diretório de saída para DBF"),
    tabwin_dir: Path = typer.Option(
        Path("C:/Program Files/TAB415"),
        "--tabwin-dir",
        help="Diretório do TABWIN",
    ),
) -> None:
    """Convert DBC files to DBF using TABWIN."""
    setup_logging()

    config = ConversionConfig(
        dbc_dir=input_dir,
        dbf_dir=output_dir,
        csv_dir=output_dir,  # Not used
        tabwin_dir=tabwin_dir,
    )

    converter = DbcToDbfConverter(config)
    stats = converter.convert_directory()

    console.print(
        f"\n[green]✓ Conversão concluída: {stats['converted']} arquivos convertidos[/green]"
    )


@app.command()
def pipeline(
    base_dir: Path = typer.Option(
        "./data/datasus",
        "--base-dir",
        "-b",
        help="Diretório base para todos os dados",
    ),
    start_date: str = typer.Option(
        "2020-01-01",
        "--start-date",
        help="Data inicial (YYYY-MM-DD)",
    ),
    end_date: Optional[str] = typer.Option(
        None,
        "--end-date",
        help="Data final (YYYY-MM-DD)",
    ),
    ufs: Optional[str] = typer.Option(
        None,
        "--ufs",
        help="UFs separadas por vírgula (ex: SP,RJ,MG)",
    ),
    tabwin_dir: Path = typer.Option(
        Path("C:/Program Files/TAB415"),
        "--tabwin-dir",
        help="Diretório do TABWIN",
    ),
    db_path: Optional[Path] = typer.Option(
        None,
        "--db-path",
        help="Caminho do arquivo DuckDB (None = em memória)",
    ),
) -> None:
    """Run the complete SIHSUS data pipeline."""
    setup_logging()

    console.print("\n[bold blue]PyDataSUS - Pipeline Completo[/bold blue]\n")

    # Parse UF list
    uf_list = [uf.strip() for uf in ufs.split(",")] if ufs else None

    # Setup directories
    dbc_dir = base_dir / "dbc"
    dbf_dir = base_dir / "dbf"
    csv_dir = base_dir / "csv"
    processed_dir = base_dir / "processed"
    parquet_dir = base_dir / "parquet"

    # Create pipeline configuration
    config = PipelineConfig(
        download=DownloadConfig(
            output_dir=dbc_dir,
            start_date=start_date,
            end_date=end_date,
            uf_list=uf_list,
        ),
        conversion=ConversionConfig(
            dbc_dir=dbc_dir,
            dbf_dir=dbf_dir,
            csv_dir=csv_dir,
            tabwin_dir=tabwin_dir,
        ),
        processing=ProcessingConfig(
            input_dir=csv_dir,
            output_dir=processed_dir,
        ),
        storage=StorageConfig(
            parquet_dir=parquet_dir,
        ),
        database=DatabaseConfig(
            db_path=db_path if db_path else base_dir / "sihsus.duckdb",
        ),
    )

    # Run pipeline
    console.print("[bold]Iniciando pipeline...[/bold]\n")

    pipeline_obj = SihsusPipeline(config)
    result = pipeline_obj.run()

    # Print summary
    console.print("\n[bold green]✓ Pipeline concluído com sucesso![/bold green]\n")
    console.print("[bold]Resumo:[/bold]")
    console.print(f"  • Arquivos baixados: {result.get_metadata('download_count', 0)}")
    console.print(f"  • DBC→DBF convertidos: {result.get_metadata('dbc_converted_count', 0)}")
    console.print(f"  • CSV convertidos: {result.get_metadata('csv_converted_count', 0)}")
    console.print(f"  • Arquivos processados: {result.get_metadata('processed_count', 0)}")
    console.print(f"  • Arquivos enriquecidos: {result.get_metadata('enriched_count', 0)}")
    console.print(f"  • Parquet criados: {result.get_metadata('parquet_count', 0)}")
    console.print(f"  • Tabelas no DuckDB: {result.get_metadata('tables_loaded', 0)}")


if __name__ == "__main__":
    app()
