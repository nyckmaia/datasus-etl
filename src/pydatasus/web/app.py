"""Streamlit web interface for PyDataSUS.

A user-friendly interface for health researchers to:
1. Download and process DataSUS data
2. Query existing datasets with SQL templates
3. Export data to CSV/Excel
4. Visualize data statistics

Run with: streamlit run src/pydatasus/web/app.py
"""

import streamlit as st
from pathlib import Path
from datetime import datetime, date
from typing import Optional
import time

# Configure page
st.set_page_config(
    page_title="PyDataSUS",
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
    st.sidebar.title("PyDataSUS")
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
    st.sidebar.markdown("v0.2.0 | [GitHub](https://github.com/nyck33/pydatasus)")


def page_home():
    """Render home page."""
    st.title("🏥 PyDataSUS")
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
    """Render database status page with visualizations."""
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
        from pydatasus.storage.parquet_query_engine import ParquetQueryEngine
        import pandas as pd

        engine = ParquetQueryEngine(parquet_dir, view_name=subsystem)

        # Get statistics
        total_rows = engine.count()
        processed_files = engine.get_processed_source_files()
        file_counts = engine.get_file_row_counts()

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

        # Plotly chart - Records by UF
        st.markdown("---")
        st.subheader("Registros por Estado (UF)")

        try:
            import plotly.express as px

            # Query UF distribution
            uf_data = engine.sql(f"""
                SELECT uf, COUNT(*) as total
                FROM {subsystem}
                GROUP BY uf
                ORDER BY total DESC
            """)

            if uf_data is not None and len(uf_data) > 0:
                df_uf = uf_data.to_pandas()
                fig = px.bar(
                    df_uf,
                    x="uf",
                    y="total",
                    title="Distribuicao de Registros por UF",
                    labels={"uf": "Estado", "total": "Registros"},
                    color="total",
                    color_continuous_scale="Blues"
                )
                fig.update_layout(showlegend=False)
                st.plotly_chart(fig, width='stretch')

        except Exception as e:
            st.warning(f"Nao foi possivel gerar grafico: {e}")

        # Statistics expander
        with st.expander("📊 Estatisticas das Colunas Numericas"):
            try:
                # Get numeric column stats
                if subsystem == "sihsus":
                    stats_query = f"""
                        SELECT
                            'val_tot' as coluna,
                            MIN(val_tot) as minimo,
                            MAX(val_tot) as maximo,
                            ROUND(AVG(val_tot), 2) as media,
                            COUNT(val_tot) as nao_nulos
                        FROM {subsystem}
                        WHERE val_tot IS NOT NULL
                        UNION ALL
                        SELECT
                            'dias_perm' as coluna,
                            MIN(dias_perm) as minimo,
                            MAX(dias_perm) as maximo,
                            ROUND(AVG(dias_perm), 2) as media,
                            COUNT(dias_perm) as nao_nulos
                        FROM {subsystem}
                        WHERE dias_perm IS NOT NULL
                        UNION ALL
                        SELECT
                            'idade' as coluna,
                            MIN(idade) as minimo,
                            MAX(idade) as maximo,
                            ROUND(AVG(idade), 2) as media,
                            COUNT(idade) as nao_nulos
                        FROM {subsystem}
                        WHERE idade IS NOT NULL
                    """
                else:  # SIM
                    stats_query = f"""
                        SELECT
                            'idade' as coluna,
                            MIN(idade) as minimo,
                            MAX(idade) as maximo,
                            ROUND(AVG(idade), 2) as media,
                            COUNT(idade) as nao_nulos
                        FROM {subsystem}
                        WHERE idade IS NOT NULL
                    """

                stats = engine.sql(stats_query)
                if stats is not None:
                    st.dataframe(stats.to_pandas(), width='stretch')

            except Exception as e:
                st.warning(f"Erro ao calcular estatisticas: {e}")

        # Show file details
        if file_counts:
            st.subheader("Arquivos por Fonte")

            df = pd.DataFrame([
                {"Arquivo": k, "Linhas": v}
                for k, v in sorted(file_counts.items())
            ])
            st.dataframe(df, width='stretch')

        engine.close()

    except Exception as e:
        st.error(f"Erro ao ler banco: {e}")


def page_download():
    """Render download page with pipeline execution."""
    st.title("📥 Download de Dados")

    data_dir = get_data_dir()
    subsystem = st.session_state.get("subsystem", "sihsus")

    st.markdown("""
    Configure os parametros de download abaixo.
    Os dados serao baixados do FTP do DATASUS e processados automaticamente.
    """)

    # UF list
    from pydatasus.constants import ALL_UFS

    col1, col2 = st.columns(2)

    with col1:
        start_date = st.date_input(
            "Data Inicial",
            value=date(2023, 1, 1),
            min_value=date(1970, 1, 1),
            help="Data de inicio do periodo de download"
        )

    with col2:
        end_date = st.date_input(
            "Data Final",
            value=date.today(),
            min_value=date(1970, 1, 1),
            help="Data final do periodo de download"
        )

    selected_ufs = st.multiselect(
        "Estados (UF)",
        options=ALL_UFS,
        default=["SP"],
        help="Selecione os estados para download. Deixe vazio para todos."
    )

    col3, col4 = st.columns(2)

    with col3:
        compression = st.selectbox(
            "Compressao Parquet",
            options=["zstd", "snappy", "gzip", "brotli"],
            help="Algoritmo de compressao para arquivos Parquet"
        )

    with col4:
        output_format = st.selectbox(
            "Formato de Saida",
            options=["parquet", "csv"],
            help="Formato dos arquivos de saida"
        )

    st.markdown("---")

    # Check for updates button
    if st.button("🔍 Verificar Atualizacoes", type="secondary"):
        with st.spinner("Verificando arquivos no FTP..."):
            try:
                from pydatasus.config import PipelineConfig
                from pydatasus.storage.incremental_updater import IncrementalUpdater

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

    # Download button with pipeline execution
    if st.button("📥 Iniciar Download", type="primary"):
        try:
            from pydatasus.config import PipelineConfig
            from pydatasus.pipeline import SihsusPipeline, SIMPipeline

            config = PipelineConfig.create(
                base_dir=data_dir,
                subsystem=subsystem,
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d"),
                uf_list=selected_ufs if selected_ufs else None,
                compression=compression,
                output_format=output_format,
            )

            # Select pipeline based on subsystem
            if subsystem == "sim":
                pipeline = SIMPipeline(config)
            else:
                pipeline = SihsusPipeline(config)

            # Create progress indicators
            st.markdown("### Progresso do Pipeline")

            progress_container = st.container()

            with progress_container:
                progress_bar = st.progress(0, text="Iniciando...")
                status_text = st.empty()
                stage_info = st.empty()

            # Stage tracking
            stages = ["Download", "Conversao", "Carga", "Transformacao", "Exportacao"]
            stage_progress = {s: "pendente" for s in stages}

            def update_progress(stage_name: str, progress_pct: float, message: str = ""):
                """Update progress display."""
                stage_progress[stage_name] = "em_progresso"
                progress_bar.progress(progress_pct, text=f"{stage_name}: {int(progress_pct*100)}%")
                if message:
                    status_text.text(message)

                # Show stage status
                stage_display = []
                for s in stages:
                    if stage_progress[s] == "completo":
                        stage_display.append(f"✅ {s}")
                    elif stage_progress[s] == "em_progresso":
                        stage_display.append(f"🔄 {s}")
                    else:
                        stage_display.append(f"⏳ {s}")

                stage_info.markdown(" → ".join(stage_display))

            # Run pipeline
            start_time = time.time()

            try:
                update_progress("Download", 0.1, "Conectando ao FTP...")
                result = pipeline.run()
                elapsed = time.time() - start_time

                # Mark all stages complete
                for s in stages:
                    stage_progress[s] = "completo"
                progress_bar.progress(1.0, text="Completo!")

                stage_display = [f"✅ {s}" for s in stages]
                stage_info.markdown(" → ".join(stage_display))

                # Show results
                st.success(f"Pipeline concluido em {elapsed:.1f} segundos!")

                col1, col2 = st.columns(2)
                with col1:
                    total_rows = result.get_metadata("total_rows_exported", 0)
                    st.metric("Linhas Exportadas", f"{total_rows:,}")
                with col2:
                    st.metric("Tempo Total", f"{elapsed:.1f}s")

                st.info(f"Arquivos salvos em: {config.storage.parquet_dir}")

            except Exception as e:
                st.error(f"Erro no pipeline: {e}")
                progress_bar.progress(0, text="Erro!")

        except ImportError as e:
            st.error(f"Erro de importacao: {e}")
        except Exception as e:
            st.error(f"Erro na configuracao: {e}")


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
        from pydatasus.storage.parquet_query_engine import ParquetQueryEngine
        from pydatasus.web.templates import get_templates
        from pydatasus.web.dictionary import get_column_descriptions
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
        st.subheader("📖 Dicionario de Dados")
        st.markdown(f"Descricao das colunas do subsistema **{subsystem.upper()}**:")

        descriptions = get_column_descriptions(subsystem)
        dict_data = [
            {"Coluna": col, "Descricao": desc}
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
        from pydatasus.storage.parquet_query_engine import ParquetQueryEngine
        from pydatasus.web.dictionary import get_column_descriptions

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
