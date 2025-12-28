"""Command-line interface for PyDataSUS.

Usage:
    datasus --help
    datasus run --source sihsus --start-date 2023-01-01 --end-date 2023-12-31 --uf SP,RJ
    datasus download --source sihsus --start-date 2023-01-01 --uf SP
    datasus version
"""

import logging
from pathlib import Path
from typing import Literal, Optional

import typer
from rich.console import Console
from rich.logging import RichHandler
from rich.panel import Panel
from rich.table import Table

from pydatasus import __version__
from pydatasus.config import PipelineConfig
from pydatasus.download.ftp_downloader import FTPDownloader
from pydatasus.pipeline.sihsus_pipeline import SihsusPipeline
from pydatasus.transform.converters.dbc_to_dbf import DbcToDbfConverter

app = typer.Typer(
    name="datasus",
    help="CLI para download e processamento de dados do DataSUS (SIHSUS, SIM, etc)",
    add_completion=True,
    rich_markup_mode="rich",
)
console = Console()


def setup_logging(level: str = "INFO") -> None:
    """Configure logging with Rich handler."""
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console, rich_tracebacks=True)],
    )


def parse_uf_list(ufs: Optional[str]) -> Optional[list[str]]:
    """Parse comma-separated UF list into a list of strings."""
    if not ufs:
        return None
    return [uf.strip().upper() for uf in ufs.split(",")]


@app.command()
def version() -> None:
    """Show version information."""
    console.print(Panel(
        f"[bold cyan]DataSUS CLI[/bold cyan] v{__version__}\n"
        "Pipeline para dados do Sistema Único de Saúde (SUS)",
        title="PyDataSUS",
        border_style="cyan",
    ))


@app.command()
def run(
    source: str = typer.Option(
        "sihsus",
        "--source",
        "-s",
        help="Subsistema DataSUS: sihsus (hospitalar), sim (mortalidade), siasus (ambulatorial)",
    ),
    start_date: str = typer.Option(
        "2023-01-01",
        "--start-date",
        help="Data inicial (YYYY-MM-DD)",
    ),
    end_date: Optional[str] = typer.Option(
        None,
        "--end-date",
        help="Data final (YYYY-MM-DD). Padrão: hoje",
    ),
    uf: Optional[str] = typer.Option(
        None,
        "--uf",
        help="Estados (UF) separados por vírgula. Ex: SP,RJ,MG. Padrão: todos",
    ),
    data_dir: Path = typer.Option(
        Path("./data/datasus"),
        "--data-dir",
        "-d",
        help="Diretório base para os dados",
    ),
    compression: str = typer.Option(
        "zstd",
        "--compression",
        "-c",
        help="Compressão Parquet: snappy, gzip, brotli, zstd",
    ),
    chunk_size: int = typer.Option(
        10000,
        "--chunk-size",
        help="Linhas por chunk no streaming DBF->DuckDB",
    ),
    override: bool = typer.Option(
        False,
        "--override",
        help="Sobrescrever arquivos existentes",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Modo verboso (mais logs)",
    ),
) -> None:
    """Run the complete DataSUS pipeline: download -> convert -> transform -> export.

    [bold]Example:[/bold]
        datasus run --source sihsus --start-date 2023-01-01 --end-date 2023-06-30 --uf SP,RJ
    """
    setup_logging("DEBUG" if verbose else "INFO")

    # Validate source
    valid_sources = ["sihsus", "sim", "siasus"]
    if source.lower() not in valid_sources:
        console.print(f"[red]Erro: --source deve ser um de: {', '.join(valid_sources)}[/red]")
        raise typer.Exit(1)

    # Validate compression
    valid_compressions = ["snappy", "gzip", "brotli", "zstd"]
    if compression.lower() not in valid_compressions:
        console.print(f"[red]Erro: --compression deve ser um de: {', '.join(valid_compressions)}[/red]")
        raise typer.Exit(1)

    # Parse UF list
    uf_list = parse_uf_list(uf)

    # Show configuration summary
    console.print()
    table = Table(title="[bold cyan]Configuração do Pipeline[/bold cyan]", border_style="cyan")
    table.add_column("Parâmetro", style="cyan")
    table.add_column("Valor", style="white")

    table.add_row("Subsistema", source.upper())
    table.add_row("Data inicial", start_date)
    table.add_row("Data final", end_date or "hoje")
    table.add_row("Estados (UF)", uf or "todos")
    table.add_row("Diretório", str(data_dir))
    table.add_row("Compressão", compression)
    table.add_row("Chunk size", f"{chunk_size:,}")

    console.print(table)
    console.print()

    # Create configuration using factory method
    config = PipelineConfig.create(
        base_dir=data_dir,
        subsystem=source.lower(),
        start_date=start_date,
        end_date=end_date,
        uf_list=uf_list,
        compression=compression,  # type: ignore
        override=override,
        chunk_size=chunk_size,
    )

    # Run pipeline
    console.print("[bold]Iniciando pipeline...[/bold]\n")

    if source.lower() == "sihsus":
        pipeline_obj = SihsusPipeline(config)
    else:
        console.print(f"[yellow]Aviso: Subsistema '{source}' ainda não implementado. Usando SIHSUS.[/yellow]")
        pipeline_obj = SihsusPipeline(config)

    result = pipeline_obj.run()

    # Print summary
    console.print("\n[bold green]✓ Pipeline concluído com sucesso![/bold green]\n")

    total_rows = result.get_metadata("total_rows_exported", 0)
    exported_files = result.get("exported_parquet_files", [])

    summary = Table(title="[bold green]Resumo[/bold green]", border_style="green")
    summary.add_column("Métrica", style="green")
    summary.add_column("Valor", style="white")

    summary.add_row("Total de linhas", f"{total_rows:,}")
    summary.add_row("Arquivos Parquet", str(len(exported_files)))
    summary.add_row("Diretório de saída", str(config.storage.parquet_dir))

    console.print(summary)


@app.command()
def download(
    source: str = typer.Option(
        "sihsus",
        "--source",
        "-s",
        help="Subsistema DataSUS: sihsus, sim, siasus",
    ),
    output_dir: Path = typer.Option(
        Path("./data/datasus/sihsus/dbc"),
        "--output-dir",
        "-o",
        help="Diretório de saída para arquivos DBC",
    ),
    start_date: str = typer.Option(
        "2023-01-01",
        "--start-date",
        help="Data inicial (YYYY-MM-DD)",
    ),
    end_date: Optional[str] = typer.Option(
        None,
        "--end-date",
        help="Data final (YYYY-MM-DD). Padrão: hoje",
    ),
    uf: Optional[str] = typer.Option(
        None,
        "--uf",
        help="Estados (UF) separados por vírgula. Ex: SP,RJ,MG",
    ),
    override: bool = typer.Option(
        False,
        "--override",
        help="Sobrescrever arquivos existentes",
    ),
) -> None:
    """Download DBC files from DataSUS FTP server.

    [bold]Example:[/bold]
        datasus download --source sihsus --start-date 2023-01-01 --uf SP
    """
    setup_logging()

    console.print("\n[bold cyan]DataSUS - Download FTP[/bold cyan]\n")

    from pydatasus.config import DownloadConfig

    uf_list = parse_uf_list(uf)

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
def convert(
    input_dir: Path = typer.Argument(..., help="Diretório com arquivos DBC"),
    output_dir: Path = typer.Argument(..., help="Diretório de saída para DBF"),
    override: bool = typer.Option(
        False,
        "--override",
        help="Sobrescrever arquivos existentes",
    ),
) -> None:
    """Convert DBC files to DBF format.

    [bold]Example:[/bold]
        datasus convert ./data/dbc ./data/dbf
    """
    setup_logging()

    console.print("\n[bold cyan]DataSUS - Conversao DBC->DBF[/bold cyan]\n")

    from pydatasus.config import ConversionConfig

    config = ConversionConfig(
        dbc_dir=input_dir,
        dbf_dir=output_dir,
        override=override,
    )

    converter = DbcToDbfConverter(config)
    stats = converter.convert_directory()

    console.print(f"\n[green]✓ Conversão concluída: {stats['converted']} arquivos convertidos[/green]")


# Legacy alias for backward compatibility
@app.command(hidden=True)
def pipeline(
    base_dir: Path = typer.Option(Path("./data/datasus"), "--base-dir", "-b"),
    start_date: str = typer.Option("2023-01-01", "--start-date"),
    end_date: Optional[str] = typer.Option(None, "--end-date"),
    ufs: Optional[str] = typer.Option(None, "--ufs"),
    tabwin_dir: Path = typer.Option(Path("C:/Program Files/TAB415"), "--tabwin-dir"),
    db_path: Optional[Path] = typer.Option(None, "--db-path"),
) -> None:
    """[DEPRECATED] Use 'datasus run' instead."""
    console.print("[yellow]Aviso: 'pipeline' está deprecado. Use 'datasus run' em seu lugar.[/yellow]\n")

    # Redirect to run command
    run(
        source="sihsus",
        start_date=start_date,
        end_date=end_date,
        uf=ufs,
        data_dir=base_dir,
        compression="zstd",
        chunk_size=10000,
        override=False,
        verbose=False,
    )


if __name__ == "__main__":
    app()
