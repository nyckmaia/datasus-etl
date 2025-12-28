"""Streamlit web interface for PyDataSUS.

A user-friendly interface for non-technical users to:
1. Download and process DataSUS data
2. Query existing datasets
3. Export data to CSV/Excel

Run with: streamlit run src/pydatasus/web/app.py
"""

import streamlit as st
from pathlib import Path
from datetime import datetime, date
from typing import Optional

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


def sidebar_config():
    """Render sidebar configuration options."""
    st.sidebar.title("PyDataSUS")
    st.sidebar.markdown("---")

    # Data directory
    data_dir = st.sidebar.text_input(
        "Diretorio de Dados",
        value=str(get_data_dir()),
        help="Diretorio base onde os dados serao salvos"
    )
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
    st.sidebar.markdown("v0.1.0 | [GitHub](https://github.com/user/pydatasus)")


def page_home():
    """Render home page."""
    st.title("🏥 PyDataSUS")
    st.markdown("""
    **Pipeline para dados do Sistema Unico de Saude (SUS)**

    Esta ferramenta permite:
    - 📥 **Download**: Baixar dados do DATASUS via FTP
    - 🔄 **Processamento**: Converter DBC → DBF → Parquet
    - 🔍 **Consultas**: Consultar dados via SQL
    - 📊 **Exportacao**: Exportar para CSV/Excel

    ### Como usar

    1. Configure o diretorio de dados na barra lateral
    2. Selecione o subsistema (SIHSUS, SIM, SIASUS)
    3. Use os botoes de navegacao para:
       - **Status**: Ver o estado atual do banco
       - **Download**: Baixar novos dados
       - **Consultar**: Executar consultas SQL
       - **Exportar**: Exportar dados

    ### Subsistemas disponiveis

    | Subsistema | Descricao |
    |------------|-----------|
    | SIHSUS | Sistema de Informacoes Hospitalares |
    | SIM | Sistema de Informacoes sobre Mortalidade |
    | SIASUS | Sistema de Informacoes Ambulatoriais |
    """)


def page_status():
    """Render database status page."""
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

        # Show file details
        if file_counts:
            st.subheader("Arquivos por Fonte")

            import pandas as pd
            df = pd.DataFrame([
                {"Arquivo": k, "Linhas": v}
                for k, v in sorted(file_counts.items())
            ])
            st.dataframe(df, use_container_width=True)

        engine.close()

    except Exception as e:
        st.error(f"Erro ao ler banco: {e}")


def page_download():
    """Render download page."""
    st.title("📥 Download de Dados")

    data_dir = get_data_dir()
    subsystem = st.session_state.get("subsystem", "sihsus")

    st.markdown("""
    Configure os parametros de download abaixo.
    Os dados serao baixados do FTP do DATASUS.
    """)

    # UF list
    from pydatasus.constants import ALL_UFS

    col1, col2 = st.columns(2)

    with col1:
        start_date = st.date_input(
            "Data Inicial",
            value=date(2023, 1, 1),
            help="Data de inicio do periodo de download"
        )

    with col2:
        end_date = st.date_input(
            "Data Final",
            value=date.today(),
            help="Data final do periodo de download"
        )

    selected_ufs = st.multiselect(
        "Estados (UF)",
        options=ALL_UFS,
        default=["SP"],
        help="Selecione os estados para download. Deixe vazio para todos."
    )

    compression = st.selectbox(
        "Compressao Parquet",
        options=["zstd", "snappy", "gzip", "brotli"],
        help="Algoritmo de compressao para arquivos Parquet"
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

            except Exception as e:
                st.error(f"Erro: {e}")

    # Download button
    if st.button("📥 Iniciar Download", type="primary"):
        st.warning("Funcionalidade de download ainda nao implementada na interface web. Use a CLI: `datasus run`")


def page_query():
    """Render query page."""
    st.title("🔍 Consultar Dados")

    data_dir = get_data_dir()
    subsystem = st.session_state.get("subsystem", "sihsus")
    parquet_dir = data_dir / subsystem / "parquet"

    if not parquet_dir.exists():
        st.warning("Banco de dados nao encontrado. Execute 'Download' primeiro.")
        return

    try:
        from pydatasus.storage.parquet_query_engine import ParquetQueryEngine

        engine = ParquetQueryEngine(parquet_dir, view_name=subsystem)

        # SQL query input
        st.markdown(f"""
        Execute consultas SQL nos dados.
        A tabela principal se chama `{subsystem}`.

        **Exemplo:**
        ```sql
        SELECT uf, COUNT(*) as total
        FROM {subsystem}
        GROUP BY uf
        ORDER BY total DESC
        LIMIT 10
        ```
        """)

        default_query = f"""SELECT uf, COUNT(*) as total
FROM {subsystem}
GROUP BY uf
ORDER BY total DESC
LIMIT 10"""

        query = st.text_area(
            "Consulta SQL",
            value=default_query,
            height=150,
        )

        if st.button("▶️ Executar", type="primary"):
            with st.spinner("Executando consulta..."):
                try:
                    result = engine.sql(query)
                    if result is not None:
                        st.success(f"Consulta retornou {len(result)} linhas")
                        st.dataframe(result.to_pandas(), use_container_width=True)

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
                st.dataframe(sample.to_pandas(), use_container_width=True)

        engine.close()

    except Exception as e:
        st.error(f"Erro ao conectar ao banco: {e}")


def page_export():
    """Render export page."""
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

        engine = ParquetQueryEngine(parquet_dir, view_name=subsystem)

        # Get schema for column selection
        schema = engine.schema()
        columns = schema["column_name"].to_list()

        # Column selection
        selected_columns = st.multiselect(
            "Colunas",
            options=columns,
            default=columns[:10],
            help="Selecione as colunas para exportar"
        )

        # Limit
        limit = st.number_input(
            "Limite de linhas",
            min_value=100,
            max_value=1000000,
            value=10000,
            step=1000,
            help="Numero maximo de linhas para exportar"
        )

        # Filter
        where_clause = st.text_input(
            "Filtro WHERE (opcional)",
            placeholder="Ex: uf = 'SP' AND ano_cmpt = 2023",
            help="Condicao SQL para filtrar dados"
        )

        # Format
        export_format = st.radio(
            "Formato",
            options=["CSV", "Excel"],
            horizontal=True
        )

        if st.button("📥 Exportar", type="primary"):
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
                        st.success(f"Exportando {len(df)} linhas")

                        if export_format == "CSV":
                            csv = df.to_csv(index=False)
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
