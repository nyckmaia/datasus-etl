# Web UI Improvements - Melhoria 10

## Overview

Melhorias implementadas na interface web DataSUS-ETL para pesquisadores da saude.

## Estado Atual (Implementado)

A interface web existe em `src/datasus_etl/web/app.py` (Streamlit).

**Paginas:**
- **Inicio** - Documentacao e instrucoes
- **Status** - Metricas do banco com graficos Plotly
- **Download** - Execucao do pipeline com progress bar
- **Consultar** - SQL queries com templates prontos
- **Exportar** - CSV/Excel com estimativa de tamanho

## Melhorias Implementadas

### 1. Execucao de Download Funcional

O botao "Iniciar Download" agora executa o pipeline completo:

```python
if st.button("Iniciar Download"):
    pipeline = SihsusPipeline(config)
    result = pipeline.run()
```

**Progress Bar por Etapa:**
```
Download → Conversao → Carga → Transformacao → Exportacao
```

### 2. Templates SQL Prontos (Auto-Preenchimento)

Arquivo: `src/datasus_etl/web/templates.py`

Ao selecionar um template no dropdown, a caixa de texto SQL e atualizada automaticamente (sem necessidade de clicar em botao separado).

Templates disponiveis para SIHSUS:
- Internacoes por UF
- Serie Temporal Mensal
- Top 10 Diagnosticos (CID-10)
- Top 10 Procedimentos
- Internacoes por Sexo
- Internacoes por Faixa Etaria
- Media de Permanencia por UF
- Valor Total por Mes

Templates disponiveis para SIM:
- Obitos por UF
- Serie Temporal Mensal
- Top 10 Causas Basicas (CID-10)
- Obitos por Sexo
- Obitos por Faixa Etaria
- Obitos por Local de Ocorrencia
- Obitos por Escolaridade
- Obitos por Raca/Cor

### 3. Dicionario de Dados (Tabela Fixa)

Arquivo: `src/datasus_etl/web/dictionary.py`

O dicionario de dados e exibido como uma tabela fixa no final da pagina "Consultar", facilitando a visualizacao durante a escrita de queries.

Descricoes de todas as colunas em portugues:

**SIHSUS:**
- `n_aih`: Numero da AIH (Autorizacao de Internacao Hospitalar)
- `dt_inter`: Data de internacao
- `diag_princ`: Diagnostico principal (CID-10)
- `val_tot`: Valor total da AIH
- ... (116 colunas totais)

**SIM:**
- `numerodo`: Numero da Declaracao de Obito
- `dtobito`: Data do obito
- `causabas`: Causa basica (CID-10)
- ... (40+ colunas)

### 4. Editor SQL com Syntax Highlighting

O campo de consulta SQL agora usa o editor Ace (via `streamlit-ace`) com:

- **Syntax Highlighting SQL**: Palavras-chave coloridas (SELECT, FROM, WHERE, etc.)
- **Autocomplete**: Pressione Ctrl+Space para sugestoes de comandos SQL
- **Tema Visual**: Tema "tomorrow" com boa legibilidade
- **Keybinding VSCode**: Atalhos de teclado familiares
- **Numeracao de Linhas**: Gutter com numeros de linha
- **Auto-update**: Atualizacao em tempo real conforme digita

```python
from streamlit_ace import st_ace

query = st_ace(
    value=default_query,
    language="sql",
    theme="tomorrow",
    keybinding="vscode",
    font_size=14,
    auto_update=True
)
```

### 5. Validacao SQL com Mensagens Claras

```python
def validate_sql(query: str) -> tuple[bool, str]:
    # Bloqueia: DROP, DELETE, UPDATE, INSERT, ALTER
    # Avisa se nao tiver LIMIT
```

### 6. Estimativa de Tamanho Antes do Export

```python
# Mostra antes de exportar:
- Linhas Disponiveis: 1,234,567
- Linhas a Exportar: 50,000
- Tamanho Estimado: ~12.5 MB
```

### 7. Graficos Plotly na Pagina Status

- Grafico de barras: Distribuicao por UF
- Cores proporcionais ao volume de dados
- Interativo (zoom, hover)

### 8. Estatisticas das Colunas Numericas

Expander com estatisticas:
- Minimo
- Maximo
- Media
- Contagem de nao-nulos

### 9. Selecao Rapida de Colunas no Export

Botoes de atalho:
- "Selecionar Todas"
- "Limpar Selecao"
- "Colunas Principais" (subset util por subsystem)

### 10. Seletor de Pasta para Diretorio de Dados

Icone de pasta (📁) ao lado do campo "Diretorio de Dados" que abre o dialogo nativo do sistema operacional para selecao de pasta.

## Estrutura de Arquivos

```
src/datasus_etl/web/
├── __init__.py      # Exports
├── app.py           # Interface Streamlit principal
├── templates.py     # SQL templates por subsistema
└── dictionary.py    # Dicionario de colunas
```

## Dependencias

```toml
# pyproject.toml
dependencies = [
    "streamlit>=1.30.0",
    "streamlit-ace>=0.1.1",  # Editor SQL com syntax highlighting
    "openpyxl>=3.1.0",
    "plotly>=5.18.0",  # Graficos interativos (incluido por padrao)
]
```

## Como Executar

```bash
# Metodo 1: Via CLI
datasus ui

# Metodo 2: Direto com Streamlit
streamlit run src/datasus_etl/web/app.py
```

Todas as dependencias, incluindo Plotly para graficos, sao instaladas automaticamente com `pip install datasus_etl`.

## Proximas Melhorias Possiveis

1. **Paginacao de Resultados** - Para queries muito grandes
2. **Cache de Resultados** - Evitar re-execucao
3. **Export Chunked** - Para exports > 500k linhas
4. **Autenticacao** - Para ambientes multi-usuario
5. **Scheduling** - Agendar downloads periodicos
6. **Notificacoes** - Email/Slack ao finalizar

## Screenshots

### Pagina Status com Grafico
```
┌─────────────────────────────────────────────────────────────┐
│ 📊 Status do Banco de Dados                                 │
├─────────────────────────────────────────────────────────────┤
│  Total Linhas  │  Arquivos  │  Parquet  │  Tamanho         │
│  1,234,567     │  27        │  27       │  450 MB          │
├─────────────────────────────────────────────────────────────┤
│  [Grafico de Barras - Registros por UF]                    │
│  SP ████████████████████ 450k                              │
│  RJ ██████████ 220k                                        │
│  MG ████████ 180k                                          │
└─────────────────────────────────────────────────────────────┘
```

### Pagina Consultar com Templates e Dicionario
```
┌─────────────────────────────────────────────────────────────┐
│ 🔍 Consultar Dados                                          │
├─────────────────────────────────────────────────────────────┤
│ 📋 Consultas Prontas                                        │
│ [Internacoes por UF        ▼] <- Auto-preenche SQL abaixo   │
├─────────────────────────────────────────────────────────────┤
│ SELECT uf, COUNT(*) as total                                │
│ FROM sihsus                                                 │
│ GROUP BY uf                                                 │
│ ORDER BY total DESC                                         │
│                                                             │
│ ⚠️ Adicione LIMIT para evitar consultas muito grandes       │
│                                                             │
│ [▶️ Executar]                                                │
├─────────────────────────────────────────────────────────────┤
│ 📖 Dicionario de Dados                                      │
│ ┌────────────┬──────────────────────────────────────────┐   │
│ │ Coluna     │ Descricao                                │   │
│ ├────────────┼──────────────────────────────────────────┤   │
│ │ diag_princ │ Diagnostico principal (CID-10)           │   │
│ │ dt_inter   │ Data de internacao                       │   │
│ │ val_tot    │ Valor total da AIH                       │   │
│ └────────────┴──────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```
