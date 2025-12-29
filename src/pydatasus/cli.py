"""Command-line interface for PyDataSUS.

Usage:
    datasus --help
    datasus run --source sihsus --start-date 2023-01-01 --end-date 2023-12-31 --uf SP,RJ
    datasus download --source sihsus --start-date 2023-01-01 --uf SP
    datasus version
"""

import logging
import shutil
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.logging import RichHandler
from rich.panel import Panel
from rich.table import Table

from pydatasus import __version__
from pydatasus.config import PipelineConfig
from pydatasus.download.ftp_downloader import FTPDownloader
from pydatasus.pipeline.sihsus_pipeline import SihsusPipeline
from pydatasus.pipeline.sim_pipeline import SIMPipeline
from pydatasus.transform.converters.dbc_to_dbf import DbcToDbfConverter


# Average DBC file sizes by subsystem (in MB)
AVG_DBC_SIZES_MB = {
    "sihsus": 20.0,
    "sim": 5.0,
    "siasus": 30.0,
}

# Parquet compression ratio (typically ~60% of DBC size)
PARQUET_COMPRESSION_RATIO = 0.6


def estimate_download_size(file_count: int, subsystem: str) -> tuple[float, float]:
    """Estimate download and parquet sizes in MB.

    Args:
        file_count: Number of files to download
        subsystem: Subsystem name (sihsus, sim, siasus)

    Returns:
        Tuple of (download_size_mb, parquet_size_mb)
    """
    avg_mb = AVG_DBC_SIZES_MB.get(subsystem.lower(), 20.0)
    download_size = file_count * avg_mb
    parquet_size = download_size * PARQUET_COMPRESSION_RATIO
    return download_size, parquet_size


def check_disk_space(path: Path, required_mb: float) -> tuple[bool, float]:
    """Check if there's enough disk space.

    Args:
        path: Path to check (uses the drive/mount point)
        required_mb: Required space in MB

    Returns:
        Tuple of (has_enough_space, available_mb)
    """
    # Ensure path exists (use parent if path doesn't exist)
    check_path = path
    while not check_path.exists():
        check_path = check_path.parent
        if check_path == check_path.parent:  # Root
            break

    try:
        usage = shutil.disk_usage(check_path)
        available_mb = usage.free / (1024 * 1024)
        return available_mb >= required_mb, available_mb
    except Exception:
        return True, 0.0  # Assume OK if we can't check


def format_size_mb(size_mb: float) -> str:
    """Format size in MB to human readable string."""
    if size_mb >= 1024:
        return f"{size_mb / 1024:.1f} GB"
    return f"{size_mb:.1f} MB"

app = typer.Typer(
    name="datasus",
    help="CLI para download e processamento de dados do DataSUS (SIHSUS, SIM, etc)",
    add_completion=False,  # Hide --show-completion from help
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
        None,
        "--source",
        "-s",
        help="Subsistema DataSUS: sihsus (hospitalar), sim (mortalidade), siasus (ambulatorial)",
    ),
    start_date: str = typer.Option(
        None,
        "--start-date",
        help="Data inicial (YYYY-MM-DD)",
    ),
    end_date: Optional[str] = typer.Option(
        None,
        "--end-date",
        help="Data final (YYYY-MM-DD). Padrao: hoje",
    ),
    uf: Optional[str] = typer.Option(
        None,
        "--uf",
        help="Estados (UF) separados por virgula. Ex: SP,RJ,MG. Padrao: todos",
    ),
    data_dir: Path = typer.Option(
        None,
        "--data-dir",
        "-d",
        help="Diretorio base para os dados",
    ),
    compression: str = typer.Option(
        "zstd",
        "--compression",
        "-c",
        help="Compressao Parquet: snappy, gzip, brotli, zstd",
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
    keep_temp_files: bool = typer.Option(
        False,
        "--keep-temp-files",
        help="Manter arquivos DBC e DBF apos processamento (padrao: deletar)",
    ),
    raw: bool = typer.Option(
        False,
        "--raw",
        help="Exportar dados sem conversoes de tipo (apenas limpeza basica). "
             "Todas colunas como VARCHAR. Util para debug ou processamento customizado.",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Modo verboso (mais logs)",
    ),
    yes: bool = typer.Option(
        False,
        "--yes",
        "-y",
        help="Pular confirmacao e executar diretamente",
    ),
) -> None:
    """Executa o pipeline completo: download -> convert -> transform -> export.

    [bold]Exemplos de uso:[/bold]

        # Baixar SIHSUS de SP e RJ em 2023
        datasus run -s sihsus --start-date 2023-01-01 -d ./data/datasus --uf SP,RJ

        # Baixar todos os estados de janeiro a marco 2024
        datasus run -s sihsus --start-date 2024-01-01 --end-date 2024-03-31 -d ./data

        # Baixar SIM (mortalidade) de 2022
        datasus run -s sim --start-date 2022-01-01 --end-date 2022-12-31 -d ./data

        # Executar sem confirmacao
        datasus run -s sihsus --start-date 2023-01-01 -d ./data --yes
    """
    # Validate required parameters
    missing_params = []
    if source is None:
        missing_params.append("--source (-s)")
    if start_date is None:
        missing_params.append("--start-date")
    if data_dir is None:
        missing_params.append("--data-dir (-d)")

    if missing_params:
        console.print("[red bold]Erro: Parametros obrigatorios faltando:[/red bold]")
        for param in missing_params:
            console.print(f"  [red]-[/red] {param}")
        console.print()
        console.print("[bold]Exemplo de uso:[/bold]")
        console.print("  datasus run --source sihsus --start-date 2023-01-01 --data-dir ./data/datasus")
        console.print()
        console.print("[dim]Use 'datasus run --help' para ver todas as opcoes.[/dim]")
        raise typer.Exit(1)

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
    table.add_row("Manter temporários", "Sim" if keep_temp_files else "Não")
    table.add_row("Modo raw", "Sim (sem conversões)" if raw else "Não (com tipos)")

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
        keep_temp_files=keep_temp_files,
        raw_mode=raw,
    )

    # Pre-download report: get file count from FTP
    console.print("[dim]Consultando servidor FTP...[/dim]")

    from pydatasus.storage.incremental_updater import IncrementalUpdater

    updater = IncrementalUpdater(config)
    available_files = updater.get_available_files_from_ftp()
    file_count = len(available_files)

    if file_count == 0:
        console.print("[yellow]Nenhum arquivo encontrado no FTP para os criterios especificados.[/yellow]")
        console.print("[dim]Verifique o subsistema, datas e UFs.[/dim]")
        raise typer.Exit(0)

    # Estimate sizes
    download_mb, parquet_mb = estimate_download_size(file_count, source.lower())
    total_needed_mb = download_mb + parquet_mb

    # Check disk space
    has_space, available_mb = check_disk_space(data_dir, total_needed_mb)

    # Show pre-download report
    console.print()
    report_table = Table(title="[bold cyan]Resumo do Download[/bold cyan]", border_style="cyan")
    report_table.add_column("Item", style="cyan")
    report_table.add_column("Valor", style="white")

    report_table.add_row("Arquivos a baixar", f"{file_count}")
    report_table.add_row("Tamanho estimado (DBC)", format_size_mb(download_mb))
    report_table.add_row("Tamanho final (Parquet)", format_size_mb(parquet_mb))
    report_table.add_row("Espaco necessario total", format_size_mb(total_needed_mb))
    report_table.add_row("Espaco livre no disco", format_size_mb(available_mb))

    console.print(report_table)
    console.print()

    # Warn if not enough space
    if not has_space:
        console.print("[red bold]AVISO: Espaco em disco insuficiente![/red bold]")
        console.print(
            f"[red]Necessario: {format_size_mb(total_needed_mb)} | "
            f"Disponivel: {format_size_mb(available_mb)}[/red]"
        )
        console.print()
        console.print("[dim]Libere espaco no disco ou escolha outro diretorio.[/dim]")
        raise typer.Exit(1)

    # Ask for confirmation (unless --yes was passed)
    if not yes:
        if not typer.confirm("Deseja continuar com o download?"):
            console.print("[yellow]Download cancelado pelo usuario.[/yellow]")
            raise typer.Exit(0)

    # Run pipeline
    console.print()
    console.print("[bold]Iniciando pipeline...[/bold]\n")

    if source.lower() == "sihsus":
        pipeline_obj = SihsusPipeline(config)
    elif source.lower() == "sim":
        pipeline_obj = SIMPipeline(config)
    else:
        console.print(f"[red]Erro: Subsistema '{source}' ainda nao implementado.[/red]")
        console.print("[dim]Subsistemas disponiveis: sihsus, sim[/dim]")
        raise typer.Exit(1)

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
def update(
    source: str = typer.Option(
        None,
        "--source",
        "-s",
        help="Subsistema DataSUS: sihsus, sim, siasus",
    ),
    data_dir: Path = typer.Option(
        None,
        "--data-dir",
        "-d",
        help="Diretorio base para os dados",
    ),
    start_date: str = typer.Option(
        None,
        "--start-date",
        help="Data inicial (YYYY-MM-DD)",
    ),
    end_date: Optional[str] = typer.Option(
        None,
        "--end-date",
        help="Data final (YYYY-MM-DD). Padrao: hoje",
    ),
    uf: Optional[str] = typer.Option(
        None,
        "--uf",
        help="Estados (UF) separados por virgula. Ex: SP,RJ,MG",
    ),
    compression: str = typer.Option(
        "zstd",
        "--compression",
        "-c",
        help="Compressao Parquet: snappy, gzip, brotli, zstd",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Apenas mostrar arquivos novos, sem processar",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Modo verboso (mais logs)",
    ),
    yes: bool = typer.Option(
        False,
        "--yes",
        "-y",
        help="Pular confirmacao e executar diretamente",
    ),
) -> None:
    """Atualiza banco de dados com novos arquivos do FTP (update incremental).

    Compara dados Parquet existentes com arquivos disponiveis no FTP
    e processa apenas arquivos novos que ainda nao foram importados.

    [bold]Exemplos de uso:[/bold]

        # Verificar e baixar novos arquivos SIHSUS
        datasus update -s sihsus --start-date 2023-01-01 -d ./data/datasus

        # Ver arquivos novos sem baixar (dry-run)
        datasus update -s sihsus --start-date 2023-01-01 -d ./data/datasus --dry-run

        # Atualizar sem confirmacao
        datasus update -s sihsus --start-date 2023-01-01 -d ./data/datasus --yes
    """
    # Validate required parameters
    missing_params = []
    if source is None:
        missing_params.append("--source (-s)")
    if start_date is None:
        missing_params.append("--start-date")
    if data_dir is None:
        missing_params.append("--data-dir (-d)")

    if missing_params:
        console.print("[red bold]Erro: Parametros obrigatorios faltando:[/red bold]")
        for param in missing_params:
            console.print(f"  [red]-[/red] {param}")
        console.print()
        console.print("[bold]Exemplo de uso:[/bold]")
        console.print("  datasus update --source sihsus --start-date 2023-01-01 --data-dir ./data/datasus")
        console.print()
        console.print("[dim]Use 'datasus update --help' para ver todas as opcoes.[/dim]")
        raise typer.Exit(1)

    setup_logging("DEBUG" if verbose else "INFO")

    from pydatasus.storage.incremental_updater import IncrementalUpdater

    # Parse UF list
    uf_list = parse_uf_list(uf)

    # Create configuration
    config = PipelineConfig.create(
        base_dir=data_dir,
        subsystem=source.lower(),
        start_date=start_date,
        end_date=end_date,
        uf_list=uf_list,
        compression=compression,  # type: ignore
    )

    console.print()
    console.print("[bold cyan]DataSUS - Incremental Update[/bold cyan]")
    console.print()

    # Create updater and check for new files
    updater = IncrementalUpdater(config)
    summary_data = updater.get_update_summary()

    # Show summary
    summary = Table(title="[bold cyan]Status do Banco[/bold cyan]", border_style="cyan")
    summary.add_column("Metrica", style="cyan")
    summary.add_column("Valor", style="white")

    summary.add_row("Arquivos processados", str(summary_data["processed_count"]))
    summary.add_row("Arquivos disponiveis", str(summary_data["available_count"]))
    summary.add_row("Arquivos novos", str(summary_data["new_count"]))

    console.print(summary)
    console.print()

    if summary_data["is_up_to_date"]:
        console.print("[green]Banco de dados esta atualizado![/green]")
        return

    if summary_data["new_count"] > 0:
        console.print(f"[yellow]Encontrados {summary_data['new_count']} arquivos novos:[/yellow]")
        for f in summary_data["new_files"][:10]:
            console.print(f"  - {f}")
        if summary_data["new_count"] > 10:
            console.print(f"  ... e mais {summary_data['new_count'] - 10}")

        # Show size estimates
        download_mb, parquet_mb = estimate_download_size(summary_data["new_count"], source.lower())
        total_needed_mb = download_mb + parquet_mb
        has_space, available_mb = check_disk_space(data_dir, total_needed_mb)

        console.print()
        console.print(f"[dim]Tamanho estimado: {format_size_mb(download_mb)} (DBC) + {format_size_mb(parquet_mb)} (Parquet)[/dim]")
        console.print(f"[dim]Espaco livre: {format_size_mb(available_mb)}[/dim]")

        if not has_space:
            console.print()
            console.print("[red bold]AVISO: Espaco em disco insuficiente![/red bold]")
            raise typer.Exit(1)

    if dry_run:
        console.print("\n[yellow]Modo dry-run: nenhum arquivo sera processado.[/yellow]")
        return

    # Create incremental config and run pipeline
    incremental_config = updater.create_incremental_config()
    if incremental_config is None:
        console.print("[green]Nada para atualizar.[/green]")
        return

    # Ask for confirmation (unless --yes was passed)
    if not yes:
        console.print()
        if not typer.confirm("Deseja continuar com a atualizacao?"):
            console.print("[yellow]Atualizacao cancelada pelo usuario.[/yellow]")
            raise typer.Exit(0)

    console.print("\n[bold]Iniciando atualizacao incremental...[/bold]")

    if source.lower() == "sihsus":
        pipeline_obj = SihsusPipeline(incremental_config)
    elif source.lower() == "sim":
        pipeline_obj = SIMPipeline(incremental_config)
    else:
        console.print(f"[red]Erro: Subsistema '{source}' ainda nao implementado.[/red]")
        raise typer.Exit(1)

    result = pipeline_obj.run()

    # Print summary
    console.print("\n[bold green]Atualizacao concluida![/bold green]")

    total_rows = result.get_metadata("total_rows_exported", 0)
    console.print(f"Novas linhas adicionadas: {total_rows:,}")


@app.command()
def status(
    source: str = typer.Option(
        None,
        "--source",
        "-s",
        help="Subsistema DataSUS: sihsus, sim, siasus",
    ),
    data_dir: Path = typer.Option(
        None,
        "--data-dir",
        "-d",
        help="Diretorio base para os dados",
    ),
) -> None:
    """Mostra status do banco de dados e atualizacoes disponiveis.

    [bold]Exemplos de uso:[/bold]

        # Ver status do banco SIHSUS
        datasus status -s sihsus -d ./data/datasus
    """
    # Validate required parameters
    missing_params = []
    if source is None:
        missing_params.append("--source (-s)")
    if data_dir is None:
        missing_params.append("--data-dir (-d)")

    if missing_params:
        console.print("[red bold]Erro: Parametros obrigatorios faltando:[/red bold]")
        for param in missing_params:
            console.print(f"  [red]-[/red] {param}")
        console.print()
        console.print("[bold]Exemplo de uso:[/bold]")
        console.print("  datasus status --source sihsus --data-dir ./data/datasus")
        console.print()
        console.print("[dim]Use 'datasus status --help' para ver todas as opcoes.[/dim]")
        raise typer.Exit(1)

    setup_logging("WARNING")

    from pydatasus.storage.parquet_query_engine import ParquetQueryEngine

    parquet_dir = data_dir / source / "parquet"

    console.print()
    console.print("[bold cyan]DataSUS - Status do Banco[/bold cyan]")
    console.print()

    if not parquet_dir.exists():
        console.print(f"[yellow]Diretorio nao encontrado: {parquet_dir}[/yellow]")
        console.print("Execute 'datasus run' para criar o banco de dados.")
        return

    parquet_files = list(parquet_dir.rglob("*.parquet"))
    if not parquet_files:
        console.print("[yellow]Nenhum arquivo Parquet encontrado.[/yellow]")
        return

    try:
        engine = ParquetQueryEngine(parquet_dir, view_name=source)

        # Get statistics
        total_rows = engine.count()
        file_counts = engine.get_file_row_counts()
        processed_files = engine.get_processed_source_files()

        # Calculate total size
        total_size = sum(f.stat().st_size for f in parquet_files)
        total_size_mb = total_size / (1024 * 1024)

        # Show summary
        summary = Table(title="[bold green]Estatisticas[/bold green]", border_style="green")
        summary.add_column("Metrica", style="green")
        summary.add_column("Valor", style="white")

        summary.add_row("Total de linhas", f"{total_rows:,}")
        summary.add_row("Arquivos fonte", str(len(processed_files)))
        summary.add_row("Arquivos Parquet", str(len(parquet_files)))
        summary.add_row("Tamanho total", f"{total_size_mb:.1f} MB")
        summary.add_row("Diretorio", str(parquet_dir))

        console.print(summary)

        # Show per-file details if not too many
        if len(file_counts) <= 20:
            console.print()
            files_table = Table(title="[bold]Arquivos por Fonte[/bold]", border_style="blue")
            files_table.add_column("Arquivo", style="blue")
            files_table.add_column("Linhas", style="white", justify="right")

            for filename, count in sorted(file_counts.items()):
                files_table.add_row(filename, f"{count:,}")

            console.print(files_table)

        engine.close()

    except Exception as e:
        console.print(f"[red]Erro ao ler banco: {e}[/red]")


@app.command()
def download(
    source: str = typer.Option(
        None,
        "--source",
        "-s",
        help="Subsistema DataSUS: sihsus, sim, siasus",
    ),
    output_dir: Path = typer.Option(
        None,
        "--output-dir",
        "-o",
        help="Diretorio de saida para arquivos DBC",
    ),
    start_date: str = typer.Option(
        None,
        "--start-date",
        help="Data inicial (YYYY-MM-DD)",
    ),
    end_date: Optional[str] = typer.Option(
        None,
        "--end-date",
        help="Data final (YYYY-MM-DD). Padrao: hoje",
    ),
    uf: Optional[str] = typer.Option(
        None,
        "--uf",
        help="Estados (UF) separados por virgula. Ex: SP,RJ,MG",
    ),
    override: bool = typer.Option(
        False,
        "--override",
        help="Sobrescrever arquivos existentes",
    ),
) -> None:
    """Baixa arquivos DBC do servidor FTP do DataSUS.

    [bold]Exemplos de uso:[/bold]

        # Baixar arquivos DBC do SIHSUS de SP
        datasus download -s sihsus --start-date 2023-01-01 -o ./data/dbc --uf SP

        # Baixar todos os estados de 2023
        datasus download -s sihsus --start-date 2023-01-01 --end-date 2023-12-31 -o ./data/dbc
    """
    # Validate required parameters
    missing_params = []
    if source is None:
        missing_params.append("--source (-s)")
    if start_date is None:
        missing_params.append("--start-date")
    if output_dir is None:
        missing_params.append("--output-dir (-o)")

    if missing_params:
        console.print("[red bold]Erro: Parametros obrigatorios faltando:[/red bold]")
        for param in missing_params:
            console.print(f"  [red]-[/red] {param}")
        console.print()
        console.print("[bold]Exemplo de uso:[/bold]")
        console.print("  datasus download --source sihsus --start-date 2023-01-01 --output-dir ./data/dbc")
        console.print()
        console.print("[dim]Use 'datasus download --help' para ver todas as opcoes.[/dim]")
        raise typer.Exit(1)

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


@app.command()
def ui(
    port: int = typer.Option(
        8501,
        "--port",
        "-p",
        help="Porta do servidor web",
    ),
) -> None:
    """Open the web interface (Streamlit).

    [bold]Example:[/bold]
        datasus ui
        datasus ui --port 8080
    """
    import subprocess
    import sys

    # Get path to app.py
    app_path = Path(__file__).parent / "web" / "app.py"

    if not app_path.exists():
        console.print(f"[red]Erro: arquivo nao encontrado: {app_path}[/red]")
        raise typer.Exit(1)

    console.print("\n[bold cyan]DataSUS - Interface Web[/bold cyan]")
    console.print(f"Abrindo em http://localhost:{port}")
    console.print("[dim]Pressione Ctrl+C para encerrar[/dim]\n")

    # Run streamlit
    subprocess.run([
        sys.executable, "-m", "streamlit", "run",
        str(app_path),
        "--server.port", str(port),
        "--server.headless", "false",
    ])


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
