"""Command-line interface for DataSUS ETL.

Usage:
    datasus --help
    datasus pipeline --source sihsus --start-date 2023-01-01 --end-date 2023-12-31 --uf SP,RJ
    datasus download-only --source sihsus --start-date 2023-01-01 --uf SP
    datasus version
"""

import logging
import os
import shutil
import signal
import sys
from pathlib import Path
from typing import Optional

import duckdb
import typer
from rich.console import Console
from rich.logging import RichHandler
from rich.panel import Panel
from rich.table import Table


def _can_use_unicode() -> bool:
    """Check if the current environment supports Unicode output."""
    # Check for NO_COLOR or TERM=dumb (set by Web Interface subprocess)
    if os.environ.get("NO_COLOR") or os.environ.get("TERM") == "dumb":
        return False

    # Check if stdout is a terminal
    if not sys.stdout.isatty():
        return False

    # On Windows, check encoding
    if sys.platform == "win32":
        try:
            # Try to encode a Unicode character
            "✓".encode(sys.stdout.encoding or "utf-8")
            return True
        except (UnicodeEncodeError, LookupError):
            return False

    return True


# Unicode symbols with ASCII fallbacks
USE_UNICODE = _can_use_unicode()
SYM_CHECK = "✓" if USE_UNICODE else "[OK]"
SYM_ARROW = "→" if USE_UNICODE else "->"
SYM_FILE = "📄" if USE_UNICODE else "[FILE]"

from datasus_etl import __version__
from datasus_etl.config import PipelineConfig
from datasus_etl.download.ftp_downloader import FTPDownloader
from datasus_etl.exceptions import PipelineCancelled
from datasus_etl.pipeline.sihsus_pipeline import SihsusPipeline
from datasus_etl.pipeline.sim_pipeline import SIMPipeline
from datasus_etl.transform.converters.dbc_to_dbf import DbcToDbfConverter


# Global reference to current pipeline context for signal handling
_current_context = None
_original_sigint_handler = None


def _handle_sigint(signum, frame):
    """Handle Ctrl+C for graceful cancellation."""
    global _current_context
    if _current_context is not None:
        console.print(
            "\n[yellow]Cancelando... aguarde o arquivo atual terminar.[/yellow]"
        )
        _current_context.request_cancel()
    else:
        # No pipeline running, raise KeyboardInterrupt as usual
        raise KeyboardInterrupt


# Average DBC file sizes by subsystem (in MB)
AVG_DBC_SIZES_MB = {
    "sihsus": 20.0,
    "sim": 5.0,
    "siasus": 30.0,
}

# DuckDB compression ratio (typically ~50% of DBC size)
DUCKDB_COMPRESSION_RATIO = 0.5


def estimate_download_size(file_count: int, subsystem: str) -> tuple[float, float]:
    """Estimate download and DuckDB sizes in MB.

    Args:
        file_count: Number of files to download
        subsystem: Subsystem name (sihsus, sim, siasus)

    Returns:
        Tuple of (download_size_mb, duckdb_size_mb)
    """
    avg_mb = AVG_DBC_SIZES_MB.get(subsystem.lower(), 20.0)
    download_size = file_count * avg_mb
    duckdb_size = download_size * DUCKDB_COMPRESSION_RATIO
    return download_size, duckdb_size


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
    help="""CLI para download e processamento de dados do DataSUS (SIHSUS, SIM, etc)

[bold]Exemplo de uso (pipeline completo):[/bold]

    datasus pipeline -s sihsus --start-date 2023-01-01 -d ./data --uf SP,RJ

[bold]Apenas baixar arquivos DBC:[/bold]

    datasus download-only -s sihsus --start-date 2023-01-01 -o ./data/dbc --uf SP
""",
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
        title="DataSUS ETL",
        border_style="cyan",
    ))


@app.command(name="pipeline")
def pipeline_cmd(
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
    write_mode: str = typer.Option(
        "append",
        "--write-mode",
        "-w",
        help="Modo de escrita: append (padrao, adiciona novos registros) ou replace (substitui todos)",
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
    num_workers: int = typer.Option(
        4,
        "--num-workers",
        "-n",
        help="Numero de workers paralelos (1-8, padrao: 4). "
             "Usado para processar DBF files em paralelo.",
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
    skip_disk_check: bool = typer.Option(
        False,
        "--skip-disk-check",
        help="Pular verificacao de espaco em disco (use se tiver certeza que ha espaco suficiente)",
    ),
) -> None:
    """Executa o pipeline completo: download -> convert -> transform -> export.

    [bold]Exemplos de uso:[/bold]

        # Baixar SIHSUS de SP e RJ em 2023
        datasus pipeline -s sihsus --start-date 2023-01-01 -d ./data/datasus --uf SP,RJ

        # Baixar todos os estados de janeiro a marco 2024
        datasus pipeline -s sihsus --start-date 2024-01-01 --end-date 2024-03-31 -d ./data

        # Baixar SIM (mortalidade) de 2022
        datasus pipeline -s sim --start-date 2022-01-01 --end-date 2022-12-31 -d ./data

        # Executar sem confirmacao
        datasus pipeline -s sihsus --start-date 2023-01-01 -d ./data --yes

        # Substituir todos os dados existentes (ao inves de append)
        datasus pipeline -s sihsus --start-date 2023-01-01 -d ./data --write-mode replace
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
        console.print("  datasus pipeline --source sihsus --start-date 2023-01-01 --data-dir ./data/datasus")
        console.print()
        console.print("[dim]Use 'datasus pipeline --help' para ver todas as opcoes.[/dim]")
        raise typer.Exit(1)

    setup_logging("DEBUG" if verbose else "INFO")

    # Validate source
    valid_sources = ["sihsus", "sim", "siasus"]
    if source.lower() not in valid_sources:
        console.print(f"[red]Erro: --source deve ser um de: {', '.join(valid_sources)}[/red]")
        raise typer.Exit(1)

    # Validate write_mode
    valid_write_modes = ["append", "replace"]
    if write_mode.lower() not in valid_write_modes:
        console.print(f"[red]Erro: --write-mode deve ser um de: {', '.join(valid_write_modes)}[/red]")
        raise typer.Exit(1)

    # Parse UF list
    uf_list = parse_uf_list(uf)

    # Show configuration summary
    console.print()
    table = Table(title="[bold cyan]Configuracao do Pipeline[/bold cyan]", border_style="cyan")
    table.add_column("Parametro", style="cyan")
    table.add_column("Valor", style="white")

    table.add_row("Subsistema", source.upper())
    table.add_row("Data inicial", start_date)
    table.add_row("Data final", end_date or "hoje")
    table.add_row("Estados (UF)", uf or "todos")
    table.add_row("Diretorio", str(data_dir))
    table.add_row("Formato saida", "Parquet (Hive-partitioned)")
    table.add_row("Saida", f"datasus_db/{source.lower()}/uf=*/")
    table.add_row("Modo escrita", write_mode)
    table.add_row("Chunk size", f"{chunk_size:,}")
    table.add_row("Workers", f"{num_workers}")
    table.add_row("Manter temporarios", "Sim" if keep_temp_files else "Nao")
    table.add_row("Modo raw", "Sim (sem conversoes)" if raw else "Nao (com tipos)")

    console.print(table)
    console.print()

    # Validate num_workers
    if num_workers < 1 or num_workers > 8:
        console.print("[red]Erro: --num-workers deve estar entre 1 e 8[/red]")
        raise typer.Exit(1)

    # Create configuration using factory method
    config = PipelineConfig.create(
        base_dir=data_dir,
        subsystem=source.lower(),
        start_date=start_date,
        end_date=end_date,
        uf_list=uf_list,
        override=override,
        chunk_size=chunk_size,
        keep_temp_files=keep_temp_files,
        raw_mode=raw,
        num_workers=num_workers,
        write_mode=write_mode.lower(),  # type: ignore
    )

    # Pre-download report: get file count from FTP
    console.print("[dim]Consultando servidor FTP...[/dim]")

    from datasus_etl.storage.incremental_updater import IncrementalUpdater

    updater = IncrementalUpdater(config)
    available_files = updater.get_available_files_from_ftp()
    file_count = len(available_files)

    if file_count == 0:
        console.print("[yellow]Nenhum arquivo encontrado no FTP para os criterios especificados.[/yellow]")
        console.print("[dim]Verifique o subsistema, datas e UFs.[/dim]")
        raise typer.Exit(0)

    # Estimate sizes
    download_mb, duckdb_mb = estimate_download_size(file_count, source.lower())
    total_needed_mb = download_mb + duckdb_mb

    # Check disk space
    has_space, available_mb = check_disk_space(data_dir, total_needed_mb)

    # Show pre-download report
    console.print()
    report_table = Table(title="[bold cyan]Resumo do Download[/bold cyan]", border_style="cyan")
    report_table.add_column("Item", style="cyan")
    report_table.add_column("Valor", style="white")

    report_table.add_row("Arquivos a baixar", f"{file_count}")
    report_table.add_row("Tamanho estimado (DBC)", format_size_mb(download_mb))
    report_table.add_row("Tamanho final (DuckDB)", format_size_mb(duckdb_mb))
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
        if not skip_disk_check:
            console.print("[dim]Libere espaco no disco ou escolha outro diretorio.[/dim]")
            console.print("[dim]Use --skip-disk-check para ignorar esta verificacao.[/dim]")
            raise typer.Exit(1)
        else:
            console.print("[yellow]Continuando mesmo assim (--skip-disk-check ativo)...[/yellow]")

    # Ask for confirmation (unless --yes was passed)
    if not yes:
        if not typer.confirm("Deseja continuar com o download?"):
            console.print("[yellow]Download cancelado pelo usuario.[/yellow]")
            raise typer.Exit(0)

    # Run pipeline
    console.print()
    console.print("[bold]Iniciando pipeline...[/bold]")
    console.print("[dim]Pressione Ctrl+C para cancelar graciosamente[/dim]\n")

    if source.lower() == "sihsus":
        pipeline_obj = SihsusPipeline(config)
    elif source.lower() == "sim":
        pipeline_obj = SIMPipeline(config)
    else:
        console.print(f"[red]Erro: Subsistema '{source}' ainda nao implementado.[/red]")
        console.print("[dim]Subsistemas disponiveis: sihsus, sim[/dim]")
        raise typer.Exit(1)

    # Set up signal handler for graceful cancellation
    global _current_context, _original_sigint_handler
    _current_context = pipeline_obj.context
    _original_sigint_handler = signal.signal(signal.SIGINT, _handle_sigint)

    try:
        result = pipeline_obj.run()

        # Print summary
        console.print(f"\n[bold green]{SYM_CHECK} Pipeline concluido com sucesso![/bold green]\n")

        total_rows = result.get_metadata("total_rows_exported", 0)
        database_path = result.get_metadata("database_path", str(config.get_database_path()))

        summary = Table(title="[bold green]Resumo[/bold green]", border_style="green")
        summary.add_column("Métrica", style="green")
        summary.add_column("Valor", style="white")

        summary.add_row("Total de linhas", f"{total_rows:,}")
        summary.add_row("Database DuckDB", database_path)

        console.print(summary)

    except PipelineCancelled:
        console.print("\n[yellow]Pipeline cancelado pelo usuário.[/yellow]")
        console.print("[dim]Dados processados até o momento foram salvos.[/dim]")
        raise typer.Exit(0)

    finally:
        # Restore original signal handler
        _current_context = None
        if _original_sigint_handler is not None:
            signal.signal(signal.SIGINT, _original_sigint_handler)


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

    Compara dados DuckDB existentes com arquivos disponiveis no FTP
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

    from datasus_etl.storage.incremental_updater import IncrementalUpdater

    # Parse UF list
    uf_list = parse_uf_list(uf)

    # Create configuration
    config = PipelineConfig.create(
        base_dir=data_dir,
        subsystem=source.lower(),
        start_date=start_date,
        end_date=end_date,
        uf_list=uf_list,
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
        download_mb, duckdb_mb = estimate_download_size(summary_data["new_count"], source.lower())
        total_needed_mb = download_mb + duckdb_mb
        has_space, available_mb = check_disk_space(data_dir, total_needed_mb)

        console.print()
        console.print(f"[dim]Tamanho estimado: {format_size_mb(download_mb)} (DBC) + {format_size_mb(duckdb_mb)} (DuckDB)[/dim]")
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
    console.print("[dim]Pressione Ctrl+C para cancelar graciosamente[/dim]\n")

    if source.lower() == "sihsus":
        pipeline_obj = SihsusPipeline(incremental_config)
    elif source.lower() == "sim":
        pipeline_obj = SIMPipeline(incremental_config)
    else:
        console.print(f"[red]Erro: Subsistema '{source}' ainda nao implementado.[/red]")
        raise typer.Exit(1)

    # Set up signal handler for graceful cancellation
    global _current_context, _original_sigint_handler
    _current_context = pipeline_obj.context
    _original_sigint_handler = signal.signal(signal.SIGINT, _handle_sigint)

    try:
        result = pipeline_obj.run()

        # Print summary
        console.print("\n[bold green]Atualizacao concluida![/bold green]")

        total_rows = result.get_metadata("total_rows_exported", 0)
        console.print(f"Novas linhas adicionadas: {total_rows:,}")

    except PipelineCancelled:
        console.print("\n[yellow]Atualizacao cancelada pelo usuário.[/yellow]")
        console.print("[dim]Dados processados até o momento foram salvos.[/dim]")
        raise typer.Exit(0)

    finally:
        # Restore original signal handler
        _current_context = None
        if _original_sigint_handler is not None:
            signal.signal(signal.SIGINT, _original_sigint_handler)


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
    """Mostra status do banco de dados DuckDB.

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

    console.print()
    console.print("[bold cyan]DataSUS - Status do Banco[/bold cyan]")
    console.print()

    source_lower = source.lower()

    # Check for Parquet storage first (supports both datasus_db/ and legacy parquet/)
    from datasus_etl.storage.parquet_manager import ParquetManager
    parquet_manager = ParquetManager(data_dir, source_lower)
    parquet_dir = parquet_manager.parquet_dir
    db_path = data_dir / f"{source_lower}.duckdb"

    if parquet_dir.exists() and any(parquet_dir.rglob("*.parquet")):
        # Parquet mode: show Parquet storage stats
        stats = parquet_manager.get_storage_stats()
        processed_files = parquet_manager.get_processed_files()

        # Show summary
        summary = Table(title="[bold green]Estatisticas (Parquet)[/bold green]", border_style="green")
        summary.add_column("Metrica", style="green")
        summary.add_column("Valor", style="white")

        summary.add_row("Arquivos Parquet", f"{stats.total_files}")
        summary.add_row("Arquivos fonte processados", str(len(processed_files)))
        summary.add_row("Tamanho total", f"{stats.total_size_bytes / (1024*1024):.1f} MB")
        summary.add_row("Particoes (UF)", f"{len(stats.partitions)}")
        summary.add_row("Compressao", "ZSTD")
        summary.add_row("Caminho", str(parquet_dir))

        console.print(summary)

        # Show partition details
        if stats.file_count_by_partition:
            console.print()
            partition_table = Table(title="[bold]Arquivos por Particao (UF)[/bold]", border_style="blue")
            partition_table.add_column("UF", style="blue")
            partition_table.add_column("Arquivos", style="white", justify="right")

            for uf in sorted(stats.file_count_by_partition.keys()):
                partition_table.add_row(uf, str(stats.file_count_by_partition[uf]))

            console.print(partition_table)

        # Show row count (requires reading Parquet files)
        try:
            conn = duckdb.connect(":memory:")
            parquet_manager.create_duckdb_view(conn, f"{source_lower}_raw")
            row_count = conn.execute(f"SELECT COUNT(*) FROM {source_lower}_raw").fetchone()[0]
            conn.close()
            console.print()
            console.print(f"[bold]Total de linhas:[/bold] {row_count:,}")
        except Exception as e:
            console.print(f"[dim]Nao foi possivel contar linhas: {e}[/dim]")

    elif db_path.exists():
        # DuckDB mode: use existing DuckDBQueryEngine
        from datasus_etl.storage.duckdb_query_engine import DuckDBQueryEngine

        try:
            engine = DuckDBQueryEngine(db_path)

            # Get database info
            db_info = engine.get_database_info()

            # Show summary
            summary = Table(title="[bold green]Estatisticas (DuckDB)[/bold green]", border_style="green")
            summary.add_column("Metrica", style="green")
            summary.add_column("Valor", style="white")

            summary.add_row("Total de linhas", f"{db_info['row_count']:,}")
            summary.add_row("Arquivos fonte processados", str(len(engine.get_processed_source_files())))
            summary.add_row("Tamanho do database", f"{db_info['size_mb']:.1f} MB")
            summary.add_row("Tabelas", ", ".join(db_info["tables"]))
            summary.add_row("Caminho", str(db_path))

            console.print(summary)

            # Show dimension status
            dim_status = db_info["dimensions"]
            has_dimensions = any(count > 0 for count in dim_status.values())
            if has_dimensions:
                console.print()
                dim_table = Table(title="[bold]Tabelas de Dimensao[/bold]", border_style="blue")
                dim_table.add_column("Tabela", style="blue")
                dim_table.add_column("Registros", style="white", justify="right")

                for table_name, count in dim_status.items():
                    if count >= 0:
                        dim_table.add_row(table_name, f"{count:,}" if count > 0 else "[dim]vazia[/dim]")

                console.print(dim_table)

            # Show file counts if not too many
            file_counts = engine.get_file_row_counts()
            if file_counts and len(file_counts) <= 20:
                console.print()
                files_table = Table(title="[bold]Registros por Arquivo Fonte[/bold]", border_style="blue")
                files_table.add_column("Arquivo", style="blue")
                files_table.add_column("Linhas", style="white", justify="right")

                for filename, count in sorted(file_counts.items()):
                    files_table.add_row(filename, f"{count:,}")

                console.print(files_table)

            engine.close()

        except Exception as e:
            console.print(f"[red]Erro ao ler banco: {e}[/red]")
    else:
        console.print(f"[yellow]Nenhum dado encontrado para {source_lower}[/yellow]")
        console.print(f"[dim]Procurado em: {db_path}[/dim]")
        console.print(f"[dim]Procurado em: {parquet_dir}[/dim]")
        console.print("Execute 'datasus pipeline' para criar o banco de dados.")


def _format_size(size_bytes: int) -> str:
    """Format bytes to human readable string."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"


@app.command(name="download-estimate")
def download_estimate(
    source: str = typer.Option(
        None,
        "--source",
        "-s",
        help="Subsistema DataSUS: sihsus, sim, siasus",
    ),
    start_date: str = typer.Option(
        None,
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
) -> None:
    """Estima quantidade e tamanho dos arquivos a serem baixados.

    Consulta o servidor FTP do DataSUS para obter informações sobre os
    arquivos disponíveis sem efetuar download.

    [bold]Exemplos de uso:[/bold]

        # Estimar downloads do SIHSUS de SP em 2023
        datasus download-estimate -s sihsus --start-date 2023-01-01 --end-date 2023-12-31 --uf SP

        # Estimar todos os estados
        datasus download-estimate -s sihsus --start-date 2023-01-01
    """
    # Validate required parameters
    missing_params = []
    if source is None:
        missing_params.append("--source (-s)")
    if start_date is None:
        missing_params.append("--start-date")

    if missing_params:
        console.print("[red bold]Erro: Parâmetros obrigatórios faltando:[/red bold]")
        for param in missing_params:
            console.print(f"  [red]-[/red] {param}")
        console.print()
        console.print("[bold]Exemplo de uso:[/bold]")
        console.print("  datasus download-estimate --source sihsus --start-date 2023-01-01")
        console.print()
        console.print("[dim]Use 'datasus download-estimate --help' para ver todas as opções.[/dim]")
        raise typer.Exit(1)

    # Validate source
    source_lower = source.lower() if source else ""
    if source_lower not in ("sihsus", "sim", "siasus"):
        console.print(f"[red]Erro: Subsistema inválido '{source}'. Use: sihsus, sim, siasus[/red]")
        raise typer.Exit(1)

    setup_logging()

    console.print("\n[bold cyan]DataSUS - Estimativa de Download[/bold cyan]\n")
    console.print(f"[dim]Consultando servidor FTP...[/dim]\n")

    from datasus_etl.config import DownloadConfig

    uf_list = parse_uf_list(uf)

    # Create config (output_dir not needed for estimate)
    config = DownloadConfig(
        output_dir=Path("."),  # Not used
        start_date=start_date,
        end_date=end_date,
        uf_list=uf_list,
    )

    downloader = FTPDownloader(config)
    info = downloader.get_file_info()

    file_count = info["file_count"]
    total_size = info["total_size_bytes"]
    duckdb_size = info.get("estimated_duckdb_bytes", int(total_size * 0.6))
    csv_size = info["estimated_csv_bytes"]

    # Display results
    console.print(f"[bold]Arquivos encontrados:[/bold] {file_count}")
    console.print()

    from rich.table import Table

    table = Table(title="Estimativa de Armazenamento", show_header=True, header_style="bold cyan")
    table.add_column("Formato", style="cyan")
    table.add_column("Tamanho Estimado", justify="right")
    table.add_column("Observação", style="dim")

    table.add_row("DBC (comprimido)", _format_size(total_size), "Tamanho no FTP")  # type: ignore
    table.add_row("DuckDB", _format_size(duckdb_size), "~60% do DBC")  # type: ignore
    table.add_row("CSV", _format_size(csv_size), "~300% do DBC")  # type: ignore

    console.print(table)
    console.print()

    # Show files by UF
    files_by_uf: dict[str, int] = {}
    for _, file_uf, size in info["files"]:  # type: ignore
        files_by_uf[file_uf] = files_by_uf.get(file_uf, 0) + 1

    if files_by_uf:
        uf_table = Table(title="Arquivos por UF", show_header=True, header_style="bold cyan")
        uf_table.add_column("UF", style="cyan")
        uf_table.add_column("Arquivos", justify="right")

        for uf_code in sorted(files_by_uf.keys()):
            uf_table.add_row(uf_code, str(files_by_uf[uf_code]))

        console.print(uf_table)
        console.print()

    # Warning about DBC deletion
    console.print("[yellow]⚠ Nota:[/yellow] Por padrão, o comando 'pipeline' deleta os arquivos DBC")
    console.print("  após processamento. Use 'download-only' para manter os arquivos DBC.")


@app.command(name="download-only")
def download_only(
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
    """Baixa arquivos DBC do servidor FTP do DataSUS (sem processar).

    [bold]Exemplos de uso:[/bold]

        # Baixar arquivos DBC do SIHSUS de SP
        datasus download-only -s sihsus --start-date 2023-01-01 -o ./data/dbc --uf SP

        # Baixar todos os estados de 2023
        datasus download-only -s sihsus --start-date 2023-01-01 --end-date 2023-12-31 -o ./data/dbc
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
        console.print("  datasus download-only --source sihsus --start-date 2023-01-01 --output-dir ./data/dbc")
        console.print()
        console.print("[dim]Use 'datasus download-only --help' para ver todas as opcoes.[/dim]")
        raise typer.Exit(1)

    setup_logging()

    console.print("\n[bold cyan]DataSUS - Download FTP[/bold cyan]\n")

    from datasus_etl.config import DownloadConfig

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

    console.print(f"\n[green]{SYM_CHECK} Download concluido: {len(files)} arquivos[/green]")


@app.command()
def convert(
    input_path: Path = typer.Argument(
        ...,
        help="Arquivo DBC ou diretório com arquivos DBC",
    ),
    output_dir: Path = typer.Argument(
        ...,
        help="Diretório de saída para arquivos convertidos",
    ),
    format: str = typer.Option(
        "dbf",
        "--format",
        "-f",
        help="Formato de saída: 'dbf' (padrão) ou 'csv'",
    ),
    override: bool = typer.Option(
        False,
        "--override",
        help="Sobrescrever arquivos existentes",
    ),
) -> None:
    """Convert DBC files to DBF or CSV format.

    Accepts a single DBC file or a directory containing DBC files.

    [bold]Examples:[/bold]
        datasus convert ./data/dbc ./data/dbf
        datasus convert ./data/arquivo.dbc ./data/dbf
        datasus convert ./data/dbc ./data/csv --format csv
    """
    setup_logging()

    # Validate format
    if format.lower() not in ("dbf", "csv"):
        console.print(f"[red]Erro: formato inválido '{format}'. Use 'dbf' ou 'csv'.[/red]")
        raise typer.Exit(1)

    format_upper = format.upper()
    console.print(f"\n[bold cyan]DataSUS - Conversao DBC{SYM_ARROW}{format_upper}[/bold cyan]\n")

    from datasus_etl.config import ConversionConfig

    config = ConversionConfig(
        dbc_dir=input_path,
        dbf_dir=output_dir,
        output_format=format.lower(),  # type: ignore
        override=override,
    )

    converter = DbcToDbfConverter(config)
    stats = converter.convert(input_path, output_dir)

    console.print(f"\n[green]{SYM_CHECK} Conversao concluida: {stats['converted']} arquivo(s) convertido(s)[/green]")
    if stats.get('skipped', 0) > 0:
        console.print(f"[yellow]  {stats['skipped']} arquivo(s) já existente(s) (use --override para reconverter)[/yellow]")
    if stats.get('errors', 0) > 0:
        console.print(f"[red]  {stats['errors']} erro(s) durante conversão[/red]")


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


def _check_duckdb_cli() -> Optional[str]:
    """Check if DuckDB CLI is installed and return its path.

    Returns:
        Path to duckdb CLI executable, or None if not found
    """
    duckdb_path = shutil.which("duckdb")
    return duckdb_path


def _run_duckdb_cli(
    db_path: Path,
    console_obj: Console,
    read_only: bool = True,
) -> None:
    """Run native DuckDB CLI on a database file.

    Args:
        db_path: Path to the DuckDB database file
        console_obj: Rich console for output
        read_only: If True, open database in read-only mode (default: True)
    """
    import subprocess

    mode_str = "[yellow]somente-leitura[/yellow]" if read_only else "[red]escrita[/red]"

    # Show info before launching CLI
    console_obj.print()
    console_obj.print("[bold cyan]DuckDB CLI[/bold cyan]")
    console_obj.print(f"Database: [green]{db_path.name}[/green] ({mode_str})")
    console_obj.print("[dim]Digite .help para ver comandos do DuckDB CLI[/dim]")
    console_obj.print("[dim]Digite .exit ou Ctrl+D para sair[/dim]")
    console_obj.print()

    # Launch DuckDB CLI
    cmd = ["duckdb"]
    if read_only:
        cmd.append("-readonly")
    cmd.append(str(db_path))
    subprocess.run(cmd)


def _run_duckdb_cli_parquet(
    init_sql: str,
    views_info: list[str],
    console_obj: Console,
) -> None:
    """Run native DuckDB CLI with Parquet VIEWs initialized via SQL script.

    Args:
        init_sql: SQL commands to create VIEWs (will be written to temp file)
        views_info: List of VIEW names for display
        console_obj: Rich console for output
    """
    import subprocess
    import tempfile

    # Create temporary init file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
        f.write(init_sql)
        init_file = f.name

    try:
        # Show info before launching CLI
        console_obj.print()
        console_obj.print("[bold cyan]DuckDB CLI (Parquet Mode)[/bold cyan]")
        console_obj.print(f"VIEWs disponiveis: [green]{', '.join(views_info)}[/green]")
        console_obj.print("[dim]Digite .help para ver comandos do DuckDB CLI[/dim]")
        console_obj.print("[dim]Digite .exit ou Ctrl+D para sair[/dim]")
        console_obj.print()

        # Launch DuckDB CLI with init script
        cmd = ["duckdb", "-init", init_file]
        subprocess.run(cmd)
    finally:
        # Clean up temp file
        import os
        os.unlink(init_file)


def _discover_subsystems(data_dir: Path) -> list[tuple[str, str]]:
    """Discover available subsystems by scanning for .duckdb files or Parquet directories.

    Args:
        data_dir: Base data directory

    Returns:
        List of tuples (subsystem_name, storage_type) where storage_type is 'duckdb' or 'parquet'
    """
    subsystems = []
    if not data_dir.exists():
        return subsystems

    # Check for DuckDB files
    for db_file in data_dir.glob("*.duckdb"):
        subsystems.append((db_file.stem, "duckdb"))

    # Check for Parquet directories
    # Support datasus_db/, legacy parquet/, and data_dir itself if named accordingly
    parquet_dirs_to_check = []
    if data_dir.name in ("datasus_db", "parquet"):
        # User passed the parquet directory directly
        parquet_dirs_to_check.append(data_dir)
    else:
        # Check both new and legacy folder names
        if (data_dir / "datasus_db").exists():
            parquet_dirs_to_check.append(data_dir / "datasus_db")
        if (data_dir / "parquet").exists():
            parquet_dirs_to_check.append(data_dir / "parquet")

    for parquet_dir in parquet_dirs_to_check:
        for subsystem_dir in parquet_dir.iterdir():
            if subsystem_dir.is_dir() and any(subsystem_dir.rglob("*.parquet")):
                # Only add if not already found as duckdb or parquet
                subsystem_name = subsystem_dir.name
                if not any(s[0] == subsystem_name for s in subsystems):
                    subsystems.append((subsystem_name, "parquet"))

    return sorted(subsystems, key=lambda x: x[0])


def _show_db_help(console_obj: Console, max_rows: int) -> None:
    """Show help for interactive shell commands."""
    help_table = Table(title="Comandos Disponiveis", border_style="cyan")
    help_table.add_column("Comando", style="cyan")
    help_table.add_column("Descricao", style="white")

    help_table.add_row(".tables", "Lista VIEWs disponiveis")
    help_table.add_row(".schema <view>", "Mostra schema da VIEW")
    help_table.add_row(".count <view>", "Conta registros na VIEW")
    help_table.add_row(".sample <view> [n]", "Mostra N registros aleatorios (padrao: 10)")
    help_table.add_row(".csv <arquivo>", "Exporta ultimo resultado para CSV")
    help_table.add_row(".maxrows [n]", f"Define max linhas exibidas (atual: {max_rows})")
    help_table.add_row(".exit / .quit", "Sai do shell")
    help_table.add_row("Ctrl+C", "Cancela query atual")
    help_table.add_row("Ctrl+D", "Sai do shell")

    console_obj.print(help_table)


def _run_interactive_shell(
    conn: duckdb.DuckDBPyConnection,
    tables: list[str],
    console_obj: Console,
) -> None:
    """Run interactive DuckDB shell.

    Args:
        conn: DuckDB connection
        tables: List of available table/view names
        console_obj: Rich console for output
    """
    import polars as pl

    last_result: Optional[pl.DataFrame] = None
    max_rows: int = 50  # Default max rows to display

    # Configure polars display settings
    pl.Config.set_tbl_rows(max_rows)

    console_obj.print()
    console_obj.print("[bold cyan]DataSUS DuckDB Shell[/bold cyan] [dim](REPL Python)[/dim]")
    console_obj.print(f"Tabelas/VIEWs disponiveis: [green]{', '.join(tables)}[/green]")
    console_obj.print("[dim]Digite .help para ver comandos disponiveis[/dim]")
    console_obj.print()
    console_obj.print(
        "[yellow]Dica:[/yellow] Para melhor performance e visualizacao, instale o DuckDB CLI:\n"
        "      [link=https://duckdb.org/docs/installation/]https://duckdb.org/docs/installation/[/link]"
    )
    console_obj.print()

    while True:
        try:
            query = input("datasus> ").strip()
            if not query:
                continue

            # Special commands
            query_lower = query.lower()
            if query_lower in (".exit", ".quit", "exit", "quit"):
                console_obj.print("[dim]Ate logo![/dim]")
                break

            if query_lower == ".help":
                _show_db_help(console_obj, max_rows)
                continue

            if query_lower == ".tables":
                console_obj.print(f"Tabelas/VIEWs: [green]{', '.join(tables)}[/green]")
                continue

            if query_lower.startswith(".schema"):
                parts = query.split()
                table = parts[1] if len(parts) > 1 else (tables[0] if tables else None)
                if table is None:
                    console_obj.print("[yellow]Nenhuma tabela disponivel[/yellow]")
                    continue
                if table not in tables:
                    console_obj.print(f"[red]Tabela '{table}' nao encontrada. Disponiveis: {', '.join(tables)}[/red]")
                    continue
                result = conn.execute(f"DESCRIBE {table}").pl()
                console_obj.print(result)
                continue

            if query_lower.startswith(".count"):
                parts = query.split()
                table = parts[1] if len(parts) > 1 else (tables[0] if tables else None)
                if table is None:
                    console_obj.print("[yellow]Nenhuma tabela disponivel[/yellow]")
                    continue
                if table not in tables:
                    console_obj.print(f"[red]Tabela '{table}' nao encontrada. Disponiveis: {', '.join(tables)}[/red]")
                    continue
                result = conn.execute(f"SELECT COUNT(*) as total FROM {table}").pl()
                total = result["total"][0]
                console_obj.print(f"Total de registros em [cyan]{table}[/cyan]: [bold]{total:,}[/bold]")
                continue

            if query_lower.startswith(".sample"):
                parts = query.split()
                table = parts[1] if len(parts) > 1 else (tables[0] if tables else None)
                n = int(parts[2]) if len(parts) > 2 else 10
                if table is None:
                    console_obj.print("[yellow]Nenhuma tabela disponivel[/yellow]")
                    continue
                if table not in tables:
                    console_obj.print(f"[red]Tabela '{table}' nao encontrada. Disponiveis: {', '.join(tables)}[/red]")
                    continue
                result = conn.execute(f"SELECT * FROM {table} USING SAMPLE {n} ROWS").pl()
                last_result = result
                console_obj.print(result)
                continue

            if query_lower.startswith(".csv"):
                if last_result is None:
                    console_obj.print("[yellow]Nenhuma query executada ainda[/yellow]")
                    continue
                parts = query.split()
                filename = parts[1] if len(parts) > 1 else "output.csv"
                last_result.write_csv(filename)
                console_obj.print(f"[green]Exportado para {filename} ({len(last_result)} linhas)[/green]")
                continue

            if query_lower.startswith(".maxrows"):
                parts = query.split()
                if len(parts) > 1:
                    try:
                        new_max = int(parts[1])
                        if new_max < 1:
                            console_obj.print("[red]Erro: maxrows deve ser >= 1[/red]")
                            continue
                        max_rows = new_max
                        pl.Config.set_tbl_rows(max_rows)
                        console_obj.print(f"[green]Max linhas alterado para {max_rows}[/green]")
                    except ValueError:
                        console_obj.print(f"[red]Erro: valor invalido '{parts[1]}'. Use um numero inteiro.[/red]")
                else:
                    console_obj.print(f"Max linhas atual: [cyan]{max_rows}[/cyan]")
                continue

            # Regular SQL query
            result = conn.execute(query).pl()
            last_result = result
            console_obj.print(result)

        except KeyboardInterrupt:
            console_obj.print("\n[dim]Use .exit para sair[/dim]")
        except EOFError:
            console_obj.print("\n[dim]Ate logo![/dim]")
            break
        except Exception as e:
            console_obj.print(f"[red]Erro: {e}[/red]")


def _get_date_column(source: str) -> str:
    """Get the date column name for a given subsystem.

    Args:
        source: Subsystem name (sihsus, sim)

    Returns:
        Date column name for filtering
    """
    date_columns = {
        "sihsus": "dt_inter",
        "sim": "dtobito",
    }
    return date_columns.get(source.lower(), "dt_inter")


def _export_to_sql(conn, query: str, output_file: Path, table_name: str) -> int:
    """Export query results as INSERT statements SQL.

    Args:
        conn: DuckDB connection
        query: SQL query to execute
        output_file: Output file path
        table_name: Table name for INSERT statements

    Returns:
        Number of rows exported
    """
    import pandas as pd

    df = conn.execute(query).fetchdf()
    row_count = len(df)

    if row_count == 0:
        return 0

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(f"-- Exported from {table_name}\n")
        f.write(f"-- Rows: {row_count}\n\n")

        # Get column names
        columns = ", ".join(df.columns)

        for _, row in df.iterrows():
            values = ", ".join([
                f"'{str(v).replace(chr(39), chr(39)+chr(39))}'" if isinstance(v, str) else
                "NULL" if pd.isna(v) else str(v)
                for v in row
            ])
            f.write(f"INSERT INTO {table_name} ({columns}) VALUES ({values});\n")

    return row_count


@app.command(name="export")
def export_cmd(
    source: str = typer.Option(
        ...,
        "--source",
        "-s",
        help="Subsistema: sihsus, sim",
    ),
    start_date: str = typer.Option(
        ...,
        "--start-date",
        help="Data inicio (YYYY-MM-DD)",
    ),
    end_date: str = typer.Option(
        ...,
        "--end-date",
        help="Data fim (YYYY-MM-DD)",
    ),
    output_dir: Path = typer.Option(
        ...,
        "--output-dir",
        "-o",
        help="Diretorio de saida para os arquivos exportados",
    ),
    format: str = typer.Option(
        "csv",
        "--format",
        "-f",
        help="Formato de saida: csv, parquet, sql",
    ),
    partition_column: str = typer.Option(
        "source_file",
        "--partition-column",
        "-p",
        help="Coluna para particionar os arquivos de saida",
    ),
    raw: bool = typer.Option(
        False,
        "--raw",
        help="Exportar tabela _raw ao inves da VIEW enriquecida",
    ),
    data_dir: Path = typer.Option(
        Path("./data"),
        "--data-dir",
        "-d",
        help="Diretorio do banco DuckDB",
    ),
) -> None:
    """Exporta dados do DuckDB para CSV, Parquet ou SQL.

    Exporta dados do banco DuckDB particionados por uma coluna especifica.
    Por padrao, exporta a VIEW enriquecida. Use --raw para exportar a tabela bruta.

    [bold]Exemplos de uso:[/bold]

        # Exportar SIHSUS para CSV (padrao)
        datasus export --source sihsus --start-date 2023-01-01 --end-date 2023-12-31 --output-dir ./csv-data

        # Exportar para Parquet com particao customizada
        datasus export -s sihsus --start-date 2023-01-01 --end-date 2023-06-30 -f parquet -o ./parquet-data

        # Exportar tabela _raw para SQL
        datasus export -s sihsus --start-date 2023-01-01 --end-date 2023-03-31 -f sql -o ./sql-data --raw
    """
    from tqdm import tqdm

    # Validate format
    valid_formats = ["csv", "parquet", "sql"]
    format_lower = format.lower()
    if format_lower not in valid_formats:
        console.print(f"[red]Erro: Formato invalido '{format}'.[/red]")
        console.print(f"[dim]Formatos validos: {', '.join(valid_formats)}[/dim]")
        raise typer.Exit(1)

    # Validate source
    valid_sources = ["sihsus", "sim"]
    source_lower = source.lower()
    if source_lower not in valid_sources:
        console.print(f"[red]Erro: Subsistema invalido '{source}'.[/red]")
        console.print(f"[dim]Subsistemas validos: {', '.join(valid_sources)}[/dim]")
        raise typer.Exit(1)

    # Locate database
    db_path = data_dir / f"{source_lower}.duckdb"
    if not db_path.exists():
        console.print(f"[red]Erro: Banco nao encontrado: {db_path}[/red]")
        console.print("[dim]Execute 'datasus pipeline' primeiro para criar os dados.[/dim]")
        raise typer.Exit(1)

    # Determine table/view name
    table_name = f"{source_lower}_raw" if raw else source_lower

    # Get date column for this subsystem
    date_column = _get_date_column(source_lower)

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    console.print()
    console.print("[bold cyan]DataSUS Export[/bold cyan]")
    console.print(f"Banco: [green]{db_path.name}[/green]")
    console.print(f"Tabela: [green]{table_name}[/green]")
    console.print(f"Periodo: [green]{start_date}[/green] a [green]{end_date}[/green]")
    console.print(f"Formato: [green]{format_lower}[/green]")
    console.print(f"Particao: [green]{partition_column}[/green]")
    console.print(f"Saida: [green]{output_dir}[/green]")
    console.print()

    try:
        conn = duckdb.connect(str(db_path), read_only=True)

        # Check if table exists
        tables = [row[0] for row in conn.execute("SHOW TABLES").fetchall()]
        if table_name not in tables:
            console.print(f"[red]Erro: Tabela '{table_name}' nao encontrada.[/red]")
            console.print(f"[dim]Tabelas disponiveis: {', '.join(tables)}[/dim]")
            conn.close()
            raise typer.Exit(1)

        # Check if columns exist
        columns_result = conn.execute(f"DESCRIBE {table_name}").fetchall()
        column_names = [row[0].lower() for row in columns_result]

        if partition_column.lower() not in column_names:
            console.print(f"[red]Erro: Coluna '{partition_column}' nao encontrada.[/red]")
            conn.close()
            raise typer.Exit(1)

        if date_column.lower() not in column_names:
            console.print(f"[red]Erro: Coluna de data '{date_column}' nao encontrada.[/red]")
            conn.close()
            raise typer.Exit(1)

        # Get distinct partition values within date range
        partition_query = f"""
            SELECT DISTINCT {partition_column}
            FROM {table_name}
            WHERE {date_column} >= '{start_date}'
              AND {date_column} <= '{end_date}'
            ORDER BY {partition_column}
        """
        partition_values = conn.execute(partition_query).fetchall()

        if not partition_values:
            console.print("[yellow]Nenhum registro encontrado no periodo especificado.[/yellow]")
            conn.close()
            return

        console.print(f"Encontrados [cyan]{len(partition_values)}[/cyan] arquivos para exportar...")
        console.print()

        total_rows = 0
        exported_files = 0

        for (partition_value,) in tqdm(partition_values, desc="Exportando", unit="arquivo"):
            # Generate output filename
            base_name = Path(str(partition_value)).stem if partition_value else "unknown"
            output_file = output_dir / f"{base_name}.{format_lower}"

            # Build query for this partition
            query = f"""
                SELECT * FROM {table_name}
                WHERE {partition_column} = '{partition_value}'
                  AND {date_column} >= '{start_date}'
                  AND {date_column} <= '{end_date}'
            """

            if format_lower == "csv":
                # Use DuckDB COPY for CSV
                conn.execute(f"COPY ({query}) TO '{output_file}' (HEADER, DELIMITER ';')")
                # Count rows
                row_count = conn.execute(f"SELECT COUNT(*) FROM ({query})").fetchone()[0]
            elif format_lower == "parquet":
                # Use DuckDB COPY for Parquet
                conn.execute(f"COPY ({query}) TO '{output_file}' (FORMAT PARQUET)")
                row_count = conn.execute(f"SELECT COUNT(*) FROM ({query})").fetchone()[0]
            else:  # sql
                row_count = _export_to_sql(conn, query, output_file, table_name)

            total_rows += row_count
            if row_count > 0:
                exported_files += 1

        conn.close()

        console.print()
        console.print(f"[bold green]{SYM_CHECK} Export concluido![/bold green]")
        console.print(f"  Arquivos: [cyan]{exported_files}[/cyan]")
        console.print(f"  Registros: [cyan]{total_rows:,}[/cyan]")
        console.print(f"  Destino: [cyan]{output_dir}[/cyan]")

    except Exception as e:
        console.print(f"[red]Erro durante exportacao: {e}[/red]")
        raise typer.Exit(1)


def _upload_full_database(
    db_path: Path,
    md_connection: str,
    target_db: str,
    overwrite: bool,
    console_obj: Console,
) -> None:
    """Upload completo do banco DuckDB local para MotherDuck.

    Args:
        db_path: Path to local DuckDB database
        md_connection: MotherDuck connection string
        target_db: Target database name in MotherDuck
        overwrite: Whether to overwrite existing database
        console_obj: Rich console for output
    """
    # Verificar tamanho do banco
    size_mb = db_path.stat().st_size / (1024 * 1024)
    console_obj.print(f"Tamanho do banco: [cyan]{size_mb:.1f} MB[/cyan]")

    if size_mb > 10 * 1024:  # 10GB
        console_obj.print("[yellow]Aviso: Banco excede 10GB do plano gratuito do MotherDuck.[/yellow]")

    # Conectar ao MotherDuck primeiro
    md_conn = duckdb.connect(md_connection)

    # Verificar se banco ja existe no MotherDuck
    existing_dbs = [row[0] for row in md_conn.execute("SHOW DATABASES").fetchall()]
    if target_db in existing_dbs:
        if not overwrite:
            md_conn.close()
            raise Exception(f"Database '{target_db}' already exists in MotherDuck")
        else:
            console_obj.print(f"[yellow]Removendo banco existente '{target_db}'...[/yellow]")
            md_conn.execute(f"DROP DATABASE IF EXISTS {target_db}")

    # Criar banco no MotherDuck a partir do arquivo local
    console_obj.print("Enviando dados para MotherDuck...")
    md_conn.execute(f"CREATE DATABASE {target_db} FROM '{db_path}'")

    md_conn.close()


def _upload_filtered_data(
    db_path: Path,
    md_connection: str,
    target_db: str,
    source: str,
    date_column: str,
    start_date: str,
    end_date: str,
    overwrite: bool,
    console_obj: Console,
) -> None:
    """Upload filtrado por periodo para MotherDuck.

    Args:
        db_path: Path to local DuckDB database
        md_connection: MotherDuck connection string
        target_db: Target database name in MotherDuck
        source: Subsystem name (sihsus, sim)
        date_column: Date column name for filtering
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        overwrite: Whether to overwrite existing table
        console_obj: Rich console for output
    """
    # Conectar ao MotherDuck
    md_conn = duckdb.connect(md_connection)

    # Criar banco se nao existir
    or_replace = "OR REPLACE " if overwrite else ""
    md_conn.sql(f"CREATE DATABASE IF NOT EXISTS {target_db}")
    md_conn.sql(f"USE {target_db}")

    # Attach banco local
    md_conn.sql(f"ATTACH '{db_path}' AS local_db (READ_ONLY)")

    # Contar registros a enviar
    count_query = f"""
        SELECT COUNT(*) FROM local_db.{source}_raw
        WHERE {date_column} >= '{start_date}'
          AND {date_column} <= '{end_date}'
    """
    row_count = md_conn.execute(count_query).fetchone()[0]
    console_obj.print(f"Registros a enviar: [cyan]{row_count:,}[/cyan]")

    if row_count == 0:
        console_obj.print("[yellow]Nenhum registro encontrado no periodo especificado.[/yellow]")
        md_conn.close()
        return

    # Criar tabela no MotherDuck com dados filtrados
    console_obj.print("Enviando dados para MotherDuck...")

    md_conn.sql(f"""
        CREATE {or_replace}TABLE {source}_raw AS
        SELECT * FROM local_db.{source}_raw
        WHERE {date_column} >= '{start_date}'
          AND {date_column} <= '{end_date}'
    """)

    md_conn.close()


@app.command(name="upload")
def upload_cmd(
    source: str = typer.Option(
        ...,
        "--source",
        "-s",
        help="Subsistema: sihsus, sim",
    ),
    start_date: Optional[str] = typer.Option(
        None,
        "--start-date",
        help="Data inicio (YYYY-MM-DD). Obrigatorio se --full nao for usado.",
    ),
    end_date: Optional[str] = typer.Option(
        None,
        "--end-date",
        help="Data fim (YYYY-MM-DD). Obrigatorio se --full nao for usado.",
    ),
    full: bool = typer.Option(
        False,
        "--full",
        help="Upload do banco completo (ignora filtros de data).",
    ),
    motherduck_token: Optional[str] = typer.Option(
        None,
        "--motherduck-token",
        "-t",
        help="Token de acesso MotherDuck. Se nao informado, abre browser para autenticacao.",
    ),
    db_name: Optional[str] = typer.Option(
        None,
        "--db-name",
        help="Nome do banco no MotherDuck. Padrao: nome do subsistema.",
    ),
    overwrite: bool = typer.Option(
        False,
        "--overwrite",
        help="Sobrescrever banco existente no MotherDuck.",
    ),
    data_dir: Path = typer.Option(
        Path("./data"),
        "--data-dir",
        "-d",
        help="Diretorio do banco DuckDB local.",
    ),
) -> None:
    """Upload de banco DuckDB local para MotherDuck.

    Envia dados do banco local para sua conta MotherDuck na nuvem.
    O plano gratuito do MotherDuck oferece 10GB de armazenamento.

    [bold]Autenticacao:[/bold]

        # Via browser (padrao) - abre pagina de login
        datasus upload -s sihsus --full

        # Via token (sem browser) - ideal para automacao
        datasus upload -s sihsus --full --motherduck-token "seu_token"

    [bold]Exemplos de uso:[/bold]

        # Upload filtrado por periodo
        datasus upload -s sihsus --start-date 2023-01-01 --end-date 2023-12-31

        # Upload do banco completo
        datasus upload -s sihsus --full

        # Com nome customizado
        datasus upload -s sihsus --full --db-name meu_banco_sihsus

        # Sobrescrever banco existente
        datasus upload -s sihsus --full --overwrite
    """
    # 1. Validar parametros
    if not full and (start_date is None or end_date is None):
        console.print("[red bold]Erro: Parametros obrigatorios faltando:[/red bold]")
        console.print("  [red]-[/red] --start-date")
        console.print("  [red]-[/red] --end-date")
        console.print()
        console.print("[dim]Use --full para upload do banco completo sem filtro de datas.[/dim]")
        raise typer.Exit(1)

    # 2. Validar source
    valid_sources = ["sihsus", "sim"]
    source_lower = source.lower()
    if source_lower not in valid_sources:
        console.print(f"[red]Erro: Subsistema invalido '{source}'.[/red]")
        console.print(f"[dim]Subsistemas validos: {', '.join(valid_sources)}[/dim]")
        raise typer.Exit(1)

    # 3. Verificar banco local existe
    db_path = data_dir / f"{source_lower}.duckdb"
    if not db_path.exists():
        console.print(f"[red]Erro: Banco nao encontrado: {db_path}[/red]")
        console.print("[dim]Execute 'datasus pipeline' primeiro para criar os dados.[/dim]")
        raise typer.Exit(1)

    # 4. Definir nome do banco no MotherDuck
    target_db = db_name if db_name else source_lower

    # 5. Montar connection string do MotherDuck
    if motherduck_token:
        md_connection = f"md:?motherduck_token={motherduck_token}"
    else:
        md_connection = "md:"
        console.print("[yellow]Abrindo browser para autenticacao no MotherDuck...[/yellow]")

    # 6. Mostrar configuracao
    console.print()
    console.print("[bold cyan]DataSUS Upload para MotherDuck[/bold cyan]")
    console.print(f"Banco local: [green]{db_path.name}[/green]")
    console.print(f"Banco destino: [green]md:{target_db}[/green]")
    if full:
        console.print("Modo: [green]Completo[/green]")
    else:
        console.print(f"Periodo: [green]{start_date}[/green] a [green]{end_date}[/green]")
    console.print()

    try:
        if full:
            # Upload completo
            _upload_full_database(db_path, md_connection, target_db, overwrite, console)
        else:
            # Upload filtrado
            date_column = _get_date_column(source_lower)
            _upload_filtered_data(
                db_path, md_connection, target_db, source_lower,
                date_column, start_date, end_date, overwrite, console
            )

        console.print()
        console.print(f"[bold green]{SYM_CHECK} Upload concluido![/bold green]")
        console.print(f"  Banco: [cyan]md:{target_db}[/cyan]")
        console.print()
        console.print("[dim]Acesse https://app.motherduck.com para visualizar seus dados.[/dim]")

    except Exception as e:
        error_msg = str(e)
        if "already exists" in error_msg.lower():
            console.print(f"[red]Erro: Banco '{target_db}' ja existe no MotherDuck.[/red]")
            console.print("[dim]Use --overwrite para sobrescrever.[/dim]")
        elif "authentication" in error_msg.lower() or "token" in error_msg.lower():
            console.print("[red]Erro: Falha na autenticacao com MotherDuck.[/red]")
            console.print("[dim]Verifique seu token ou tente novamente via browser.[/dim]")
        else:
            console.print(f"[red]Erro durante upload: {e}[/red]")
        raise typer.Exit(1)


@app.command()
def db(
    data_dir: Path = typer.Option(
        None,
        "--data-dir",
        "-d",
        help="Diretorio base dos dados (contendo arquivos .duckdb)",
    ),
    source: Optional[str] = typer.Option(
        None,
        "--source",
        "-s",
        help="Filtrar por subsistema especifico (sihsus, sim, siasus)",
    ),
    read_only: bool = typer.Option(
        True,
        "--read-only/--write",
        help="Modo somente leitura (padrao) ou escrita. Use --write para permitir modificacoes.",
    ),
) -> None:
    """Abre shell interativo DuckDB para consultar dados.

    Abre o arquivo DuckDB do subsistema especificado ou lista
    databases disponiveis para selecao.

    [bold]Exemplos de uso:[/bold]

        # Abre o database SIHSUS
        datasus db --data-dir ./data/datasus --source sihsus

        # Lista databases disponiveis
        datasus db --data-dir ./data/datasus

    [bold]Comandos do shell:[/bold]

        .tables          Lista tabelas/VIEWs disponiveis
        .schema <table>  Mostra colunas da tabela
        .count <table>   Conta registros
        .sample <table>  Mostra amostra de 10 registros
        .csv <arquivo>   Exporta ultimo resultado para CSV
        .maxrows [n]     Define max linhas exibidas (padrao: 50)
        .exit            Sai do shell
    """
    # Validate required parameters
    if data_dir is None:
        console.print("[red bold]Erro: Parametro obrigatorio faltando:[/red bold]")
        console.print("  [red]-[/red] --data-dir (-d)")
        console.print()
        console.print("[bold]Exemplo de uso:[/bold]")
        console.print("  datasus db --data-dir ./data/datasus --source sihsus")
        console.print()
        console.print("[dim]Use 'datasus db --help' para ver todas as opcoes.[/dim]")
        raise typer.Exit(1)

    if not data_dir.exists():
        console.print(f"[red]Erro: Diretorio nao encontrado: {data_dir}[/red]")
        raise typer.Exit(1)

    # Discover available databases (both DuckDB and Parquet)
    available_dbs = _discover_subsystems(data_dir)

    if not available_dbs:
        console.print(f"[yellow]Nenhum database encontrado em: {data_dir}[/yellow]")
        console.print("[dim]Execute 'datasus pipeline' primeiro para criar os dados.[/dim]")
        raise typer.Exit(1)

    # Determine which database to open
    storage_type = None
    if source:
        source_lower = source.lower()
        # Check for matching subsystem in available databases
        matching = [(name, stype) for name, stype in available_dbs if name == source_lower]
        if not matching:
            console.print(f"[red]Erro: Subsistema nao encontrado: {source_lower}[/red]")
            console.print(f"[dim]Disponiveis: {', '.join([f'{n} ({t})' for n, t in available_dbs])}[/dim]")
            raise typer.Exit(1)
        source_lower, storage_type = matching[0]
    else:
        # If only one database, use it; otherwise ask user to specify
        if len(available_dbs) == 1:
            source_lower, storage_type = available_dbs[0]
        else:
            console.print("[bold cyan]Databases disponiveis:[/bold cyan]")
            for db_name, db_type in available_dbs:
                if db_type == "duckdb":
                    db_file = data_dir / f"{db_name}.duckdb"
                    size_mb = db_file.stat().st_size / (1024 * 1024)
                    console.print(f"  - [green]{db_name}[/green] (DuckDB, {size_mb:.1f} MB)")
                else:
                    # Use ParquetManager to get the correct path (supports both datasus_db/ and legacy parquet/)
                    from datasus_etl.storage.parquet_manager import ParquetManager
                    pm = ParquetManager(data_dir, db_name)
                    parquet_subdir = pm.parquet_dir
                    total_size = sum(f.stat().st_size for f in parquet_subdir.rglob("*.parquet"))
                    size_mb = total_size / (1024 * 1024)
                    num_files = len(list(parquet_subdir.rglob("*.parquet")))
                    console.print(f"  - [green]{db_name}[/green] (Parquet, {num_files} files, {size_mb:.1f} MB)")
            console.print()
            console.print("[dim]Use --source para especificar o database.[/dim]")
            console.print("[dim]Exemplo: datasus db -d ./data/datasus -s sihsus[/dim]")
            return

    # Handle based on storage type
    if storage_type == "parquet":
        # Parquet mode: create in-memory DuckDB with VIEW
        from datasus_etl.storage.parquet_manager import ParquetManager

        parquet_manager = ParquetManager(data_dir, source_lower)
        if not parquet_manager.exists():
            console.print(f"[red]Erro: Nenhum arquivo Parquet encontrado para {source_lower}[/red]")
            raise typer.Exit(1)

        stats = parquet_manager.get_storage_stats()
        console.print()
        console.print(f"[bold cyan]Parquet Storage: {source_lower}[/bold cyan]")
        console.print(f"  Arquivos: {stats.total_files}")
        console.print(f"  Tamanho: {stats.total_size_bytes / (1024*1024):.1f} MB")
        console.print(f"  Particoes: {', '.join(stats.partitions[:10])}{'...' if len(stats.partitions) > 10 else ''}")
        console.print()

        # Determine paths for VIEWs
        # Use ParquetManager for ibge to get the correct path
        ibge_manager = ParquetManager(data_dir, "ibge")
        ibge_parquet = ibge_manager.parquet_dir / "ibge_locais.parquet"
        has_ibge = ibge_parquet.exists()

        # Get parquet glob pattern from manager
        parquet_glob = parquet_manager.get_glob_pattern()

        # Check if DuckDB CLI is available
        duckdb_cli_path = _check_duckdb_cli()

        if duckdb_cli_path:
            # Use native DuckDB CLI for better performance
            # Build SQL init script with VIEW definitions
            views_list = [f"{source_lower}_all"]
            init_sql_parts = [
                f"-- DataSUS Parquet VIEWs",
                f"CREATE OR REPLACE VIEW {source_lower}_all AS SELECT * FROM read_parquet('{parquet_glob}', hive_partitioning=true);",
            ]

            if has_ibge:
                views_list.append("ibge_locais")
                views_list.append(source_lower)
                init_sql_parts.append(f"CREATE OR REPLACE VIEW ibge_locais AS SELECT * FROM read_parquet('{ibge_parquet}');")
                init_sql_parts.append(f"""CREATE OR REPLACE VIEW {source_lower} AS
SELECT s.*, i.sigla_uf AS uf_res, i.nome_municipio AS municipio_res,
       i.nome_regiao_geografica_imediata AS rg_imediata_res,
       i.nome_regiao_geografica_intermediaria AS rg_intermediaria_res
FROM {source_lower}_all s
LEFT JOIN ibge_locais i ON s.munic_res = i.codigo_municipio_6_digitos;""")
            else:
                views_list.append(source_lower)
                init_sql_parts.append(f"CREATE OR REPLACE VIEW {source_lower} AS SELECT * FROM {source_lower}_all;")

            init_sql = "\n".join(init_sql_parts)
            _run_duckdb_cli_parquet(init_sql, views_list, console)
        else:
            # Fallback to Python REPL
            console.print("[dim]Dica: Para melhor performance e visualizacao, instale o DuckDB CLI:[/dim]")
            console.print("[dim]      https://duckdb.org/docs/installation/[/dim]")
            console.print()

            # Create in-memory connection with VIEW
            conn = duckdb.connect(":memory:")
            parquet_manager.create_duckdb_view(conn, f"{source_lower}_all")

            if has_ibge:
                conn.execute(f"""
                    CREATE OR REPLACE VIEW ibge_locais AS
                    SELECT * FROM read_parquet('{ibge_parquet}')
                """)
                console.print("[dim]VIEW ibge_locais criada a partir de parquet/ibge/ibge_locais.parquet[/dim]")

                # Create enriched VIEW with JOIN to ibge_locais
                # Uses codigo_municipio_6_digitos for direct JOIN with munic_res
                conn.execute(f"""
                    CREATE OR REPLACE VIEW {source_lower} AS
                    SELECT
                        s.*,
                        i.sigla_uf AS uf_res,
                        i.nome_municipio AS municipio_res,
                        i.nome_regiao_geografica_imediata AS rg_imediata_res,
                        i.nome_regiao_geografica_intermediaria AS rg_intermediaria_res
                    FROM {source_lower}_all s
                    LEFT JOIN ibge_locais i
                        ON s.munic_res = i.codigo_municipio_6_digitos
                """)
                console.print(f"[dim]VIEW {source_lower} criada com JOIN de ibge_locais (colunas uf_res, municipio_res, rg_imediata_res, rg_intermediaria_res)[/dim]")
            else:
                # No IBGE data, create simple VIEW alias
                conn.execute(f"CREATE VIEW {source_lower} AS SELECT * FROM {source_lower}_all")
                console.print("[dim]Dica: Execute 'datasus generate-ibge-data' para habilitar o JOIN com dados do IBGE[/dim]")

            # Get available tables/views
            result = conn.execute("SHOW TABLES").fetchall()
            tables = [row[0] for row in result]

            # Run interactive shell
            _run_interactive_shell(conn, tables, console)
            conn.close()
    else:
        # DuckDB mode: open file directly
        db_path = data_dir / f"{source_lower}.duckdb"

        # Check if DuckDB CLI is available
        duckdb_cli_path = _check_duckdb_cli()

        if duckdb_cli_path:
            # Use native DuckDB CLI for better performance
            _run_duckdb_cli(db_path, console, read_only=read_only)
        else:
            # Fallback to Python REPL
            mode_str = "somente-leitura" if read_only else "escrita"
            console.print(f"[dim]Modo: {mode_str}[/dim]")
            conn = duckdb.connect(str(db_path), read_only=read_only)

            # Get available tables/views
            try:
                result = conn.execute("SHOW TABLES").fetchall()
                tables = [row[0] for row in result]

                if not tables:
                    console.print("[yellow]Nenhuma tabela encontrada no database.[/yellow]")
                    conn.close()
                    raise typer.Exit(1)

                # Run interactive shell
                _run_interactive_shell(conn, tables, console)
            finally:
                conn.close()


@app.command(name="generate-ibge-data")
def generate_ibge_data_cmd(
    data_dir: Path = typer.Option(
        None,
        "--data-dir",
        "-d",
        help="Diretorio base para os dados",
    ),
) -> None:
    """Gera arquivo Parquet com dados de municipios do IBGE.

    Converte o arquivo Excel do IBGE (DTB 2024) para formato Parquet,
    normalizando nomes de colunas (minusculas, sem acentos, espacos -> underline).

    O arquivo sera salvo em: {data_dir}/datasus_db/ibge/ibge_locais.parquet
    (ou {data_dir}/parquet/ibge/ se a pasta parquet/ ja existir)

    [bold]Exemplo:[/bold]
        datasus generate-ibge-data -d ./data/datasus
    """
    # Validate required parameter
    if data_dir is None:
        console.print("[red bold]Erro: --data-dir (-d) e obrigatorio[/red bold]")
        console.print()
        console.print("[bold]Exemplo de uso:[/bold]")
        console.print("  datasus generate-ibge-data -d ./data/datasus")
        raise typer.Exit(1)

    setup_logging("INFO")

    # Define output path using ParquetManager for path resolution
    from datasus_etl.storage.parquet_manager import ParquetManager
    ibge_manager = ParquetManager(data_dir, "ibge")
    output_path = ibge_manager.parquet_dir / "ibge_locais.parquet"

    console.print()
    console.print("[bold cyan]Gerando Parquet do IBGE...[/bold cyan]")
    console.print(f"  Entrada: arquivo Excel DTB 2024 (embutido no pacote)")
    console.print(f"  Saida: {output_path}")
    console.print()

    try:
        # Generate the parquet file
        from datasus_etl.utils.ibge_loader import generate_ibge_parquet

        result_path = generate_ibge_parquet(output_path)

        # Get file stats
        file_size = result_path.stat().st_size
        size_str = f"{file_size / 1024:.1f} KB" if file_size < 1024 * 1024 else f"{file_size / (1024 * 1024):.1f} MB"

        console.print(f"[bold green]{SYM_CHECK} Arquivo gerado com sucesso![/bold green]")
        console.print(f"  Caminho: {result_path}")
        console.print(f"  Tamanho: {size_str}")
        console.print()
        console.print("[dim]Dica: Use 'datasus db -d {data_dir}' para consultar os dados.[/dim]")

    except FileNotFoundError as e:
        console.print(f"[red bold]Erro: Arquivo Excel do IBGE nao encontrado[/red bold]")
        console.print(f"  {e}")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red bold]Erro ao gerar Parquet:[/red bold]")
        console.print(f"  {e}")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
