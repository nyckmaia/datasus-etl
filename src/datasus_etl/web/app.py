"""Streamlit web interface for DataSUS ETL.

A user-friendly interface for health researchers to:
1. Download and process DataSUS data
2. Query existing datasets with SQL templates
3. Export data to CSV/Excel
4. Visualize data statistics

Run with: streamlit run src/datasus_etl/web/app.py
"""

import streamlit as st
from pathlib import Path
from datetime import datetime, date
from typing import Optional
import time

# Configure page
st.set_page_config(
    page_title="DataSUS ETL",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded",
)


def get_data_dir() -> Path:
    """Get the data directory from session state or default."""
    if "data_dir" not in st.session_state:
        st.session_state.data_dir = Path("./data/datasus")
    return Path(st.session_state.data_dir)


def validate_sql(query: str) -> tuple[bool, str]:
    """Validate SQL query before execution.

    Returns:
        Tuple of (is_valid, message)
    """
    query_upper = query.upper().strip()

    # Check for dangerous commands
    dangerous = ["DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "TRUNCATE", "CREATE"]
    for word in dangerous:
        if word in query_upper.split():
            return False, f"Comando '{word}' nao permitido. Apenas SELECT e permitido."

    # Check if starts with SELECT
    if not query_upper.startswith("SELECT"):
        return False, "A consulta deve comecar com SELECT."

    # Warning if no LIMIT
    if "LIMIT" not in query_upper:
        return True, "Adicione LIMIT para evitar consultas muito grandes."

    return True, ""


def select_folder_dialog() -> Optional[str]:
    """Open a folder selection dialog using tkinter.

    Returns:
        Selected folder path or None if cancelled
    """
    try:
        import tkinter as tk
        from tkinter import filedialog

        # Create a hidden root window
        root = tk.Tk()
        root.withdraw()
        root.wm_attributes("-topmost", 1)

        # Open folder dialog
        folder_path = filedialog.askdirectory(
            title="Selecione o Diretorio de Dados",
            initialdir=str(get_data_dir())
        )

        root.destroy()

        return folder_path if folder_path else None
    except Exception:
        return None


def sidebar_config():
    """Render sidebar configuration options."""
    st.sidebar.title("DataSUS ETL")
    st.sidebar.markdown("---")

    # Data directory with folder picker
    st.sidebar.markdown("**Diretorio de Dados**")

    col1, col2 = st.sidebar.columns([4, 1])

    with col1:
        data_dir = st.text_input(
            "Diretorio de Dados",
            value=str(get_data_dir()),
            help="Diretorio base onde os dados serao salvos",
            label_visibility="collapsed"
        )

    with col2:
        if st.button("📁", help="Selecionar pasta", key="folder_picker"):
            selected_folder = select_folder_dialog()
            if selected_folder:
                st.session_state.data_dir = selected_folder
                st.rerun()

    st.session_state.data_dir = data_dir

    # Subsystem selection
    subsystem = st.sidebar.selectbox(
        "Subsistema",
        options=["sihsus", "sim", "siasus"],
        format_func=lambda x: {
            "sihsus": "SIHSUS - Hospitalar",
            "sim": "SIM - Mortalidade",
            "siasus": "SIASUS - Ambulatorial"
        }.get(x, x),
        help="Selecione o subsistema DataSUS"
    )
    st.session_state.subsystem = subsystem

    st.sidebar.markdown("---")
    st.sidebar.markdown("v0.2.0 | [GitHub](https://github.com/nyck33/datasus-etl)")


def page_home():
    """Render home page."""
    st.title("🏥 DataSUS ETL")
    st.markdown("""
    **Pipeline para dados do Sistema Unico de Saude (SUS)**

    Esta ferramenta permite:
    - 📥 **Download**: Baixar dados do DATASUS via FTP
    - 🔄 **Processamento**: Converter DBC → DBF → Parquet
    - 🔍 **Consultas**: Consultar dados via SQL com templates prontos
    - 📊 **Exportacao**: Exportar para CSV/Excel
    - 📈 **Visualizacao**: Graficos e estatisticas dos dados

    ### Como usar

    1. Configure o diretorio de dados na barra lateral
    2. Selecione o subsistema (SIHSUS, SIM, SIASUS)
    3. Use os botoes de navegacao para:
       - **Status**: Ver o estado atual do banco com graficos
       - **Download**: Baixar novos dados com progresso
       - **Consultar**: Executar consultas SQL com templates
       - **Exportar**: Exportar dados com estimativa de tamanho

    ### Subsistemas disponiveis

    | Subsistema | Descricao |
    |------------|-----------|
    | SIHSUS | Sistema de Informacoes Hospitalares |
    | SIM | Sistema de Informacoes sobre Mortalidade |
    | SIASUS | Sistema de Informacoes Ambulatoriais |
    """)


def page_status():
    """Render database status page with statistics tables."""
    st.title("📊 Status do Banco de Dados")

    data_dir = get_data_dir()
    subsystem = st.session_state.get("subsystem", "sihsus")
    parquet_dir = data_dir / subsystem / "parquet"

    if not parquet_dir.exists():
        st.warning(f"Diretorio nao encontrado: {parquet_dir}")
        st.info("Execute 'Download' para criar o banco de dados.")
        return

    parquet_files = list(parquet_dir.rglob("*.parquet"))
    if not parquet_files:
        st.warning("Nenhum arquivo Parquet encontrado.")
        return

    try:
        from datasus_etl.storage.parquet_query_engine import ParquetQueryEngine
        from datasus_etl.web.dictionary import get_column_descriptions
        import pandas as pd

        engine = ParquetQueryEngine(parquet_dir, view_name=subsystem)

        # Get statistics
        total_rows = engine.count()
        processed_files = engine.get_processed_source_files()

        # Calculate total size
        total_size = sum(f.stat().st_size for f in parquet_files)
        total_size_mb = total_size / (1024 * 1024)

        # Display metrics in columns
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Total de Linhas", f"{total_rows:,}")

        with col2:
            st.metric("Arquivos Fonte", len(processed_files))

        with col3:
            st.metric("Arquivos Parquet", len(parquet_files))

        with col4:
            st.metric("Tamanho", f"{total_size_mb:.1f} MB")

        # UF Table (replacing Plotly chart)
        st.markdown("---")
        st.subheader("📍 Registros por Estado (UF)")

        try:
            # Determine date column based on subsystem
            date_col = "dtobito" if subsystem == "sim" else "dt_inter"

            # Query UF distribution with date range
            uf_query = f"""
                SELECT
                    uf,
                    COUNT(*) as registros,
                    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as uso_pct,
                    MIN({date_col}) as data_inicial,
                    MAX({date_col}) as data_final
                FROM {subsystem}
                GROUP BY uf
                ORDER BY registros DESC
            """

            uf_data = engine.sql(uf_query)

            if uf_data is not None and len(uf_data) > 0:
                df_uf = uf_data.to_pandas()
                df_uf.columns = ["UF", "Registros", "Uso (%)", "Data Inicial", "Data Final"]
                st.dataframe(df_uf, width='stretch', height=400)

        except Exception as e:
            st.warning(f"Erro ao carregar dados por UF: {e}")

        # Column Statistics Table (expanded, not in expander)
        st.markdown("---")
        st.subheader("📊 Estatísticas das Colunas")

        try:
            # Get schema from engine
            schema_df = engine.schema()
            if schema_df is not None:
                schema_data = schema_df.to_pandas()
                column_names = schema_data["column_name"].tolist()

                # Get column descriptions
                descriptions = get_column_descriptions(subsystem)

                # Get type mapping from schema files
                if subsystem == "sim":
                    from datasus_etl.datasets.sim.schema import SIM_PARQUET_SCHEMA as TYPE_SCHEMA
                else:
                    from datasus_etl.constants.sihsus_schema import SIHSUS_PARQUET_SCHEMA as TYPE_SCHEMA

                # Build statistics query for all columns
                stats_parts = []
                for col in column_names:
                    col_lower = col.lower()
                    stats_parts.append(f"""
                        SELECT
                            '{col}' as coluna,
                            CAST(MIN({col}) AS VARCHAR) as minimo,
                            CAST(MAX({col}) AS VARCHAR) as maximo,
                            ROUND(100.0 * SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as pct_nulos
                        FROM {subsystem}
                    """)

                # Execute combined query (limit to avoid performance issues)
                if len(stats_parts) > 0:
                    # Query first 30 columns to avoid timeout
                    limited_parts = stats_parts[:30]
                    stats_query = " UNION ALL ".join(limited_parts)

                    stats_result = engine.sql(stats_query)

                    if stats_result is not None:
                        stats_df = stats_result.to_pandas()

                        # Add descriptions and types
                        stats_list = []
                        for _, row in stats_df.iterrows():
                            col_name = row["coluna"].lower()
                            stats_list.append({
                                "Coluna": row["coluna"],
                                "Descrição": descriptions.get(col_name, ""),
                                "Tipo": TYPE_SCHEMA.get(col_name, "VARCHAR"),
                                "Mínimo": row["minimo"] if row["minimo"] else "-",
                                "Máximo": row["maximo"] if row["maximo"] else "-",
                                "% Nulos": f"{row['pct_nulos']:.1f}%"
                            })

                        final_df = pd.DataFrame(stats_list)
                        st.dataframe(final_df, width='stretch', height=600)

                        if len(column_names) > 30:
                            st.info(f"Mostrando 30 de {len(column_names)} colunas.")

        except Exception as e:
            st.warning(f"Erro ao calcular estatisticas: {e}")

        engine.close()

    except Exception as e:
        st.error(f"Erro ao ler banco: {e}")


# Note: Module-level dicts don't persist across Streamlit reruns
# We'll use st.session_state for persistence and check for completion via log file


def page_download():
    """Render download page with pipeline execution.

    Shows a timer and reads log file for terminal output.
    Uses subprocess to run pipeline and capture all output including tqdm.
    """
    st.title("📥 Download de Dados")

    data_dir = get_data_dir()
    subsystem = st.session_state.get("subsystem", "sihsus")

    # Initialize pipeline state in session_state
    if "pipeline_running" not in st.session_state:
        st.session_state.pipeline_running = False
        st.session_state.pipeline_start_time = 0
        st.session_state.pipeline_config = None
        st.session_state.pipeline_log_file = None
        st.session_state.pipeline_process_pid = None

    st.markdown("""
    Configure os parametros de download abaixo.
    Os dados serao baixados do FTP do DATASUS e processados automaticamente.
    """)

    # UF list
    from datasus_etl.constants import ALL_UFS

    col1, col2 = st.columns(2)

    with col1:
        start_date = st.date_input(
            "Data Inicial",
            value=date(2023, 1, 1),
            min_value=date(1970, 1, 1),
            help="Data de inicio do periodo de download",
            disabled=st.session_state.pipeline_running
        )

    with col2:
        end_date = st.date_input(
            "Data Final",
            value=date.today(),
            min_value=date(1970, 1, 1),
            help="Data final do periodo de download",
            disabled=st.session_state.pipeline_running
        )

    selected_ufs = st.multiselect(
        "Estados (UF)",
        options=ALL_UFS,
        default=["SP"],
        help="Selecione os estados para download. Deixe vazio para todos.",
        disabled=st.session_state.pipeline_running
    )

    col3, col4 = st.columns(2)

    with col3:
        compression = st.selectbox(
            "Compressao Parquet",
            options=["zstd", "snappy", "gzip", "brotli"],
            help="Algoritmo de compressao para arquivos Parquet",
            disabled=st.session_state.pipeline_running
        )

    with col4:
        output_format = st.selectbox(
            "Formato de Saida",
            options=["parquet", "csv"],
            help="Formato dos arquivos de saida",
            disabled=st.session_state.pipeline_running
        )

    st.markdown("---")

    # Check for updates button
    if st.button("🔍 Verificar Atualizacoes", type="secondary", disabled=st.session_state.pipeline_running):
        with st.spinner("Verificando arquivos no FTP..."):
            try:
                from datasus_etl.config import PipelineConfig
                from datasus_etl.storage.incremental_updater import IncrementalUpdater

                config = PipelineConfig.create(
                    base_dir=data_dir,
                    subsystem=subsystem,
                    start_date=start_date.strftime("%Y-%m-%d"),
                    end_date=end_date.strftime("%Y-%m-%d"),
                    uf_list=selected_ufs if selected_ufs else None,
                    compression=compression,
                )

                updater = IncrementalUpdater(config)
                summary = updater.get_update_summary()

                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Processados", summary["processed_count"])
                with col2:
                    st.metric("Disponiveis", summary["available_count"])
                with col3:
                    st.metric("Novos", summary["new_count"])

                if summary["is_up_to_date"]:
                    st.success("Banco de dados esta atualizado!")
                elif summary["new_count"] > 0:
                    st.info(f"Encontrados {summary['new_count']} arquivos novos para processar.")
                    with st.expander("Ver arquivos novos"):
                        for f in summary["new_files"]:
                            st.text(f"- {f}")

                # Store summary in session state for download button
                st.session_state.update_summary = summary

            except Exception as e:
                st.error(f"Erro: {e}")

    # Download button - starts pipeline as subprocess
    if st.button("📥 Iniciar Download", type="primary", disabled=st.session_state.pipeline_running):
        try:
            import subprocess
            import tempfile
            import os
            import sys
            from datasus_etl.config import PipelineConfig

            config = PipelineConfig.create(
                base_dir=data_dir,
                subsystem=subsystem,
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d"),
                uf_list=selected_ufs if selected_ufs else None,
                compression=compression,
                output_format=output_format,
            )

            # Store config in session for later use
            st.session_state.pipeline_config = config

            # Create log file in a known location (temp dir)
            import tempfile
            temp_dir = tempfile.gettempdir()
            log_file_path = os.path.join(temp_dir, "datasus_pipeline.log")

            # Build CLI command using sys.executable to ensure correct Python
            uf_arg = ",".join(selected_ufs) if selected_ufs else ""
            cmd = [
                sys.executable, "-m", "datasus_etl.cli", "pipeline",
                "--source", subsystem,
                "--start-date", start_date.strftime("%Y-%m-%d"),
                "--end-date", end_date.strftime("%Y-%m-%d"),
                "--data-dir", str(data_dir),
                "--compression", compression,
                "--yes",  # Skip confirmation
            ]
            if uf_arg:
                cmd.extend(["--uf", uf_arg])

            # Write initial content to log file
            with open(log_file_path, 'w', encoding='utf-8') as log:
                log.write(f"[COMANDO] {' '.join(cmd)}\n")
                log.write("[PIPELINE] Iniciando subprocess...\n")

            # Start subprocess with output redirected to log file
            # Set environment for proper Unicode handling on Windows
            env = {
                **os.environ,
                "PYTHONUNBUFFERED": "1",
                "PYTHONIOENCODING": "utf-8",
                # Disable Rich features that cause encoding issues on Windows
                "NO_COLOR": "1",  # Disable colored output
                "TERM": "dumb",  # Disable terminal features
            }

            with open(log_file_path, 'a', encoding='utf-8') as log:
                process = subprocess.Popen(
                    cmd,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,  # Line buffered
                    env=env,
                    creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0,
                )

            # Store state in session_state (persists across reruns)
            st.session_state.pipeline_running = True
            st.session_state.pipeline_start_time = time.time()
            st.session_state.pipeline_log_file = log_file_path
            st.session_state.pipeline_process_pid = process.pid

            # Force rerun to enter polling mode
            st.rerun()

        except Exception as e:
            st.error(f"Erro na configuracao: {e}")
            import traceback
            st.code(traceback.format_exc())

    # Helper function to read log file
    def read_log_file(max_lines=100):
        """Read the last N lines from log file."""
        log_path = st.session_state.get("pipeline_log_file")
        if not log_path:
            return "[Aguardando inicio...]"
        try:
            with open(log_path, 'r', encoding='utf-8', errors='replace') as f:
                lines = f.readlines()
                # Get last N lines
                recent_lines = lines[-max_lines:] if len(lines) > max_lines else lines
                # Clean ANSI codes
                import re
                clean_lines = []
                for line in recent_lines:
                    clean = re.sub(r'\x1b\[[0-9;]*m', '', line)
                    clean = re.sub(r'\r', '', clean)
                    clean_lines.append(clean.rstrip())
                return "\n".join(clean_lines)
        except FileNotFoundError:
            return "[Arquivo de log nao encontrado]"
        except Exception as e:
            return f"[Erro ao ler log: {e}]"

    # Helper function to check if process is still running
    def is_process_running(pid):
        """Check if a process with given PID is running."""
        if pid is None:
            return False
        try:
            import psutil
            return psutil.pid_exists(pid) and psutil.Process(pid).is_running()
        except ImportError:
            # Fallback without psutil - check via os
            import os
            try:
                os.kill(pid, 0)
                return True
            except OSError:
                return False
        except Exception:
            return False

    # Helper to check if pipeline finished (look for markers in log)
    def check_pipeline_finished():
        """Check log file for completion markers."""
        log_path = st.session_state.get("pipeline_log_file")
        if not log_path:
            return False, None

        try:
            with open(log_path, 'r', encoding='utf-8', errors='replace') as f:
                content = f.read()

            # Check for success marker
            if "Pipeline concluído com sucesso" in content or "✓ Pipeline" in content:
                return True, None

            # Check for error markers
            if "[ERRO]" in content:
                # Extract last error
                lines = content.split('\n')
                for line in reversed(lines):
                    if "[ERRO]" in line:
                        return True, line

            # Check for cancellation
            if "cancelado pelo" in content.lower():
                return True, "Pipeline cancelado"

            return False, None
        except Exception:
            return False, None

    # Show status while pipeline is running (polling mode)
    if st.session_state.pipeline_running:
        # Check if process is still running or finished
        pid = st.session_state.get("pipeline_process_pid")
        process_running = is_process_running(pid)
        finished, error = check_pipeline_finished()

        if process_running and not finished:
            # Pipeline still running - show progress
            st.markdown("### Pipeline em Execucao")

            # Elapsed time (this updates correctly because it's calculated locally)
            elapsed = time.time() - st.session_state.pipeline_start_time
            minutes = int(elapsed // 60)
            seconds = int(elapsed % 60)
            st.metric("Tempo decorrido", f"{minutes:02d}:{seconds:02d}")

            # Terminal output (read from log file) with auto-scroll
            st.markdown("**Saida do Terminal:**")
            output_text = read_log_file(max_lines=50)

            # Use a container with custom key for auto-scroll
            log_container = st.container()
            with log_container:
                st.code(output_text, language="text")

            # Inject JavaScript to auto-scroll to bottom of code block
            st.markdown(
                """
                <script>
                    const codeBlocks = window.parent.document.querySelectorAll('pre');
                    if (codeBlocks.length > 0) {
                        const lastBlock = codeBlocks[codeBlocks.length - 1];
                        lastBlock.scrollTop = lastBlock.scrollHeight;
                    }
                </script>
                """,
                unsafe_allow_html=True
            )

            st.info("Aguarde... O processo esta em andamento.")

            # Poll every 2 seconds by sleeping then rerunning
            time.sleep(2)
            st.rerun()

        else:
            # Pipeline finished (process ended or completion marker found)
            elapsed = time.time() - st.session_state.pipeline_start_time

            if error:
                st.markdown("### Pipeline Finalizado com Erro")
                st.error(f"Erro no pipeline: {error}")

                # Show terminal output
                st.markdown("**Saida do Terminal:**")
                output_text = read_log_file(max_lines=200)
                st.text_area(
                    "Terminal",
                    value=output_text,
                    height=300,
                    disabled=True,
                    label_visibility="collapsed"
                )
            else:
                st.markdown("### Pipeline Concluido com Sucesso!")

                # Show results
                minutes = int(elapsed // 60)
                seconds = int(elapsed % 60)

                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Tempo Total", f"{minutes:02d}:{seconds:02d}")
                with col2:
                    config = st.session_state.get("pipeline_config")
                    if config:
                        st.metric("Diretorio", str(config.storage.parquet_dir))

                config = st.session_state.get("pipeline_config")
                if config:
                    st.success(f"Arquivos salvos em: {config.storage.parquet_dir}")

                # Show terminal output in expander
                with st.expander("Ver saida do terminal"):
                    output_text = read_log_file(max_lines=500)
                    st.text_area(
                        "Terminal",
                        value=output_text,
                        height=400,
                        disabled=True,
                        label_visibility="collapsed"
                    )

            # Reset running state (but keep results visible)
            st.session_state.pipeline_running = False


def page_query():
    """Render query page with templates and validation."""
    st.title("🔍 Consultar Dados")

    data_dir = get_data_dir()
    subsystem = st.session_state.get("subsystem", "sihsus")
    parquet_dir = data_dir / subsystem / "parquet"

    if not parquet_dir.exists():
        st.warning("Banco de dados nao encontrado. Execute 'Download' primeiro.")
        return

    try:
        from datasus_etl.storage.parquet_query_engine import ParquetQueryEngine
        from datasus_etl.web.templates import get_templates
        from datasus_etl.web.dictionary import get_column_descriptions
        from streamlit_ace import st_ace
        import pandas as pd

        engine = ParquetQueryEngine(parquet_dir, view_name=subsystem)

        # Get templates for this subsystem
        templates = get_templates(subsystem)
        template_names = ["(Escrever SQL personalizado)"] + list(templates.keys())

        # Initialize query in session state if not exists
        default_query = f"""SELECT uf, COUNT(*) as total
FROM {subsystem}
GROUP BY uf
ORDER BY total DESC
LIMIT 10"""

        # Initialize session state for the SQL editor
        if "sql_editor_content" not in st.session_state:
            st.session_state.sql_editor_content = default_query

        # Counter to force st_ace recreation when template changes
        if "ace_editor_version" not in st.session_state:
            st.session_state.ace_editor_version = 0

        # Track previous template selection to detect changes
        if "prev_template" not in st.session_state:
            st.session_state.prev_template = "(Escrever SQL personalizado)"

        st.subheader("📋 Consultas Prontas")

        selected_template = st.selectbox(
            "Escolha um template",
            options=template_names,
            key="template_selector",
            help="Selecione um template - a consulta sera preenchida automaticamente"
        )

        # Update editor content when template selection changes
        if selected_template != st.session_state.prev_template:
            st.session_state.prev_template = selected_template
            if selected_template != "(Escrever SQL personalizado)":
                st.session_state.sql_editor_content = templates[selected_template]
                # Increment version to force st_ace to recreate with new value
                st.session_state.ace_editor_version += 1
                st.rerun()

        # SQL Editor with syntax highlighting and autocomplete
        st.markdown("---")
        st.markdown(f"Execute consultas SQL nos dados. A tabela principal se chama `{subsystem}`.")

        # Use st_ace for SQL code editor with syntax highlighting
        # Key includes version number to force recreation when template changes
        query = st_ace(
            value=st.session_state.sql_editor_content,
            language="sql",
            theme="tomorrow",
            keybinding="vscode",
            font_size=14,
            tab_size=4,
            min_lines=8,
            max_lines=20,
            auto_update=True,
            wrap=True,
            show_gutter=True,
            show_print_margin=False,
            key=f"sql_ace_editor_v{st.session_state.ace_editor_version}",
            placeholder=f"Digite sua consulta SQL aqui...\nTabela disponivel: {subsystem}"
        )

        # Update session state with editor content
        if query:
            st.session_state.sql_editor_content = query

        # Use query from editor or fallback to session state
        current_query = query if query else st.session_state.sql_editor_content

        # Validation feedback
        is_valid, message = validate_sql(current_query)
        if not is_valid:
            st.error(message)
        elif message:
            st.warning(message)

        # Execute button
        if st.button("▶️ Executar", type="primary", disabled=not is_valid or not current_query):
            with st.spinner("Executando consulta..."):
                start_time = time.time()
                try:
                    result = engine.sql(current_query)
                    elapsed = time.time() - start_time

                    if result is not None:
                        st.success(f"Consulta retornou {len(result):,} linhas em {elapsed:.2f}s")
                        st.dataframe(result.to_pandas(), width='stretch')

                        # Download buttons
                        col1, col2 = st.columns(2)
                        with col1:
                            csv = result.to_pandas().to_csv(index=False)
                            st.download_button(
                                "📥 Download CSV",
                                csv,
                                "resultado.csv",
                                "text/csv"
                            )

                except Exception as e:
                    st.error(f"Erro na consulta: {e}")

        # Sample data
        with st.expander("Ver amostra dos dados"):
            sample = engine.sample(10)
            if sample is not None:
                st.dataframe(sample.to_pandas(), width='stretch')

        # Data dictionary as fixed table at the bottom
        st.markdown("---")
        st.subheader("📖 Dicionário de Dados")
        st.markdown(f"Descrição das colunas do subsistema **{subsystem.upper()}**:")

        try:
            descriptions = get_column_descriptions(subsystem)

            # Get type schema
            if subsystem == "sim":
                from datasus_etl.datasets.sim.schema import SIM_PARQUET_SCHEMA as TYPE_SCHEMA
            else:
                from datasus_etl.constants.sihsus_schema import SIHSUS_PARQUET_SCHEMA as TYPE_SCHEMA

            # Get actual columns from the engine schema
            schema_result = engine.schema()
            if schema_result is not None:
                actual_cols = schema_result.to_pandas()["column_name"].tolist()

                # Calculate null percentages for each column
                null_stats = {}
                total_rows = engine.count()

                if total_rows > 0:
                    # Build query for null counts (batch to avoid timeout)
                    cols_to_check = actual_cols[:30]  # Limit to first 30 columns
                    null_parts = []
                    for col in cols_to_check:
                        null_parts.append(
                            f"SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) as {col}_nulls"
                        )

                    if null_parts:
                        null_query = f"SELECT {', '.join(null_parts)} FROM {subsystem}"
                        null_result = engine.sql(null_query)

                        if null_result is not None:
                            null_row = null_result.to_pandas().iloc[0]
                            for col in cols_to_check:
                                null_count = null_row.get(f"{col}_nulls", 0) or 0
                                null_stats[col.lower()] = round(100.0 * null_count / total_rows, 1)

                # Build dictionary data with types and null percentages
                dict_data = []
                for col in actual_cols:
                    col_lower = col.lower()
                    dict_data.append({
                        "Coluna": col,
                        "Descrição": descriptions.get(col_lower, ""),
                        "Tipo": TYPE_SCHEMA.get(col_lower, "VARCHAR"),
                        "% Nulos": f"{null_stats.get(col_lower, 0):.1f}%" if col_lower in null_stats else "-"
                    })

                df_dict = pd.DataFrame(dict_data)
                st.dataframe(df_dict, width='stretch', height=500)

                if len(actual_cols) > 30:
                    st.info(f"% Nulos calculado para as primeiras 30 colunas de {len(actual_cols)}.")

            else:
                # Fallback to simple dictionary
                dict_data = [
                    {"Coluna": col, "Descrição": desc, "Tipo": TYPE_SCHEMA.get(col, "VARCHAR"), "% Nulos": "-"}
                    for col, desc in sorted(descriptions.items())
                ]
                df_dict = pd.DataFrame(dict_data)
                st.dataframe(df_dict, width='stretch', height=500)

        except Exception as e:
            st.warning(f"Erro ao carregar dicionário: {e}")
            # Fallback to simple dictionary
            descriptions = get_column_descriptions(subsystem)
            dict_data = [
                {"Coluna": col, "Descrição": desc}
                for col, desc in sorted(descriptions.items())
            ]
            df_dict = pd.DataFrame(dict_data)
            st.dataframe(df_dict, width='stretch', height=400)

        engine.close()

    except Exception as e:
        st.error(f"Erro ao conectar ao banco: {e}")


def page_export():
    """Render export page with size estimation."""
    st.title("📊 Exportar Dados")

    data_dir = get_data_dir()
    subsystem = st.session_state.get("subsystem", "sihsus")
    parquet_dir = data_dir / subsystem / "parquet"

    if not parquet_dir.exists():
        st.warning("Banco de dados nao encontrado. Execute 'Download' primeiro.")
        return

    st.markdown("""
    Exporte dados para CSV ou Excel.
    Use filtros SQL para selecionar apenas os dados necessarios.
    """)

    try:
        from datasus_etl.storage.parquet_query_engine import ParquetQueryEngine
        from datasus_etl.web.dictionary import get_column_descriptions

        engine = ParquetQueryEngine(parquet_dir, view_name=subsystem)

        # Get schema for column selection
        schema = engine.schema()
        columns = schema["column_name"].to_list()

        # Column descriptions for help
        descriptions = get_column_descriptions(subsystem)

        # Column selection with descriptions
        st.subheader("Selecao de Colunas")

        # Quick select buttons
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("Selecionar Todas"):
                st.session_state.selected_export_cols = columns
        with col2:
            if st.button("Limpar Selecao"):
                st.session_state.selected_export_cols = []
        with col3:
            if st.button("Colunas Principais"):
                # Select main columns based on subsystem
                if subsystem == "sihsus":
                    main_cols = ["uf", "dt_inter", "dt_saida", "diag_princ", "proc_rea",
                                 "idade", "sexo", "val_tot", "dias_perm", "munic_res"]
                else:  # SIM
                    main_cols = ["uf", "dtobito", "dtnasc", "causabas", "sexo",
                                 "idade", "racacor", "codmunocor"]
                st.session_state.selected_export_cols = [c for c in main_cols if c in columns]

        # Initialize if not exists
        if "selected_export_cols" not in st.session_state:
            st.session_state.selected_export_cols = columns[:10]

        selected_columns = st.multiselect(
            "Colunas",
            options=columns,
            default=st.session_state.selected_export_cols,
            help="Selecione as colunas para exportar"
        )

        # Show column descriptions for selected
        if selected_columns:
            with st.expander("Ver descricao das colunas selecionadas"):
                for col in selected_columns:
                    desc = descriptions.get(col, "Sem descricao disponivel")
                    st.markdown(f"**{col}**: {desc}")

        st.markdown("---")

        # Limit and filter
        col1, col2 = st.columns(2)

        with col1:
            limit = st.number_input(
                "Limite de linhas",
                min_value=100,
                max_value=1000000,
                value=10000,
                step=1000,
                help="Numero maximo de linhas para exportar"
            )

        with col2:
            export_format = st.radio(
                "Formato",
                options=["CSV", "Excel"],
                horizontal=True
            )

        # Filter
        where_clause = st.text_input(
            "Filtro WHERE (opcional)",
            placeholder="Ex: uf = 'SP' AND ano_cmpt = 2023",
            help="Condicao SQL para filtrar dados"
        )

        # Estimate button
        if st.button("📊 Estimar Tamanho", type="secondary"):
            with st.spinner("Calculando..."):
                try:
                    # Build count query
                    count_query = f"SELECT COUNT(*) as total FROM {subsystem}"
                    if where_clause:
                        count_query += f" WHERE {where_clause}"

                    count_result = engine.sql(count_query)
                    if count_result is not None:
                        total_available = count_result.to_pandas()["total"].iloc[0]
                        rows_to_export = min(limit, total_available)

                        # Estimate size (rough: ~100 bytes per row per column)
                        avg_bytes_per_cell = 50
                        estimated_bytes = rows_to_export * len(selected_columns) * avg_bytes_per_cell
                        estimated_mb = estimated_bytes / (1024 * 1024)

                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Linhas Disponiveis", f"{total_available:,}")
                        with col2:
                            st.metric("Linhas a Exportar", f"{rows_to_export:,}")
                        with col3:
                            st.metric("Tamanho Estimado", f"~{estimated_mb:.1f} MB")

                        if rows_to_export > 100000:
                            st.warning("Export grande detectado. O download pode demorar alguns segundos.")

                except Exception as e:
                    st.error(f"Erro ao estimar: {e}")

        # Export button
        if st.button("📥 Exportar", type="primary"):
            if not selected_columns:
                st.error("Selecione pelo menos uma coluna.")
            else:
                with st.spinner("Exportando dados..."):
                    try:
                        # Build query
                        cols = ", ".join(selected_columns)
                        query = f"SELECT {cols} FROM {subsystem}"
                        if where_clause:
                            query += f" WHERE {where_clause}"
                        query += f" LIMIT {limit}"

                        result = engine.sql(query)

                        if result is not None:
                            df = result.to_pandas()
                            st.success(f"Exportando {len(df):,} linhas")

                            if export_format == "CSV":
                                csv = df.to_csv(index=False, sep=";")
                                st.download_button(
                                    "📥 Download CSV",
                                    csv,
                                    f"{subsystem}_export.csv",
                                    "text/csv"
                                )
                            else:
                                # Excel export
                                import io
                                buffer = io.BytesIO()
                                df.to_excel(buffer, index=False, engine="openpyxl")
                                st.download_button(
                                    "📥 Download Excel",
                                    buffer.getvalue(),
                                    f"{subsystem}_export.xlsx",
                                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                                )

                    except Exception as e:
                        st.error(f"Erro: {e}")

        engine.close()

    except Exception as e:
        st.error(f"Erro ao conectar ao banco: {e}")


def main():
    """Main application entry point."""
    # Sidebar config
    sidebar_config()

    # Navigation
    page = st.sidebar.radio(
        "Navegacao",
        options=["Inicio", "Status", "Download", "Consultar", "Exportar"],
        label_visibility="collapsed"
    )

    # Render selected page
    if page == "Inicio":
        page_home()
    elif page == "Status":
        page_status()
    elif page == "Download":
        page_download()
    elif page == "Consultar":
        page_query()
    elif page == "Exportar":
        page_export()


if __name__ == "__main__":
    main()
