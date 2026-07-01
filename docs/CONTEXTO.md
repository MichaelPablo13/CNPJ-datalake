# Contexto e Histórico de Decisões — CNPJ DataLake

Documento de referência para retomar o projeto sem perda de contexto.
Atualizar ao tomar novas decisões arquiteturais.

---

## Objetivo do Projeto

Pipeline de dados públicos da Receita Federal (CNPJ) utilizando:
- **PySpark** para processamento distribuído
- **MinIO** como object storage (S3-compatible)
- **PostgreSQL** para metadados de execução
- **Apache Airflow** para orquestração
- Padrão **Bronze / Silver / Gold** (Medallion Architecture)

---

## Estrutura Atual

```
CNPJ-DataLake/
├── services/
│   ├── airflow/
│   │   ├── dags/cnpj_dataset_dags.py
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   ├── pyspark/cli/run_pipeline.py
│   ├── minio/init-minio.sh
│   └── postgres/
│       ├── init-db.sh
│       └── schemas.sql
├── data/
│   ├── input/                  ← subpastas por dataset (empresas, socios, etc.)
│   └── consumed/               ← arquivos movidos após ingestão com sucesso
├── docs/
│   ├── architecture.md
│   ├── runbook.md
│   └── CONTEXTO.md             ← este arquivo
├── infra/
│   └── docker-compose.yml   ← stack completa (todos os serviços)
├── src/
│   └── cnpj_datalake/          ← único pacote Python do projeto
│       ├── config/settings.py
│       ├── domain/layouts.py   (schemas por tipo de arquivo CNPJ)
│       ├── domain/models.py
│       ├── services/
│       │   ├── pyspark/        (bronze, silver, gold, orchestration, spark)
│       │   ├── minio/          (client MinIO)
│       │   └── postgres/       (client PostgreSQL)
│       ├── utils/logger.py
│       └── cli.py
├── tests/
├── .env / .env.example
├── pyproject.toml
└── requirements.txt
```

---

## Decisões Arquiteturais

### PySpark obrigatório
Escolha intencional: permite migrar de `local[*]` para cluster (EMR, Databricks) sem alterar código.
DuckDB foi descartado por não escalar horizontalmente.

### data_version derivado do Airflow
Nos DAGs, `data_version = logical_date.strftime("%Y-%m")` é injetado via `get_current_context()`.
O `.env` mantém `DATA_VERSION` apenas como fallback para execução local via CLI.
**Nunca atualizar DATA_VERSION manualmente para execuções via Airflow.**

### Bronze aceita arquivo único ou glob
- `ingest_csv(source_file, ...)` — arquivo único
- `ingest_glob(source_pattern, ...)` — glob com `*` (ex: `Empresas*.txt`)

`orchestration.py` detecta automaticamente via `_is_glob()`.

### Airflow em modo manual + validação de origem
- `AIRFLOW_PIPELINE_SCHEDULE=manual` desativa execução diária.
- As DAGs validam se o arquivo (ou glob) existe antes do Bronze.
- As variáveis `AIRFLOW_SOURCE_FILE_*` aceitam múltiplos padrões separados por `|`, permitindo nome padrão e nome cru extraído do zip.

### Pós-consumo no host
Após sucesso no pipeline de cada dataset, o DAG move os arquivos processados para:
`data/consumed/<dataset>/<data_version>/`.
Isso evita reprocessamento acidental na próxima execução.

### Silver aplica casts por file_type
Definidos em `_SILVER_CASTS` dentro de `silver.py`:
- `empresas`: `capital_social` → `decimal(18,2)`
- `estabelecimentos`: 3 colunas de data → `date` (formato `yyyyMMdd`)
- `socios`: `data_entrada_sociedade` → `date`

### Silver normaliza strings e remove aspas residuais
Na normalizacao textual, a Silver aplica:
- `trim()` em todas as colunas string;
- remocao de aspas simples e duplas para evitar sujeira em joins/filtros;
- parse robusto de `capital_social` no formato brasileiro (`0,00`, `1.234.567,89`, etc.).

### SparkSession compartilhada em run_pipeline()
`run_pipeline()` cria **uma única SparkSession** e a injeta nas três camadas via `spark=` param.
As funções standalone (`run_bronze_stage`, etc.) criam/destroem sua própria sessão — uso para Airflow (cada task é um processo separado).

### Gold por file_type
`GoldLayer.aggregate(silver_path, file_type)` despacha para:
- `estabelecimentos` → passthrough limpo
- `empresas` → passthrough limpo
- `socios` → passthrough limpo
- referencias (`cnaes`, `motivos`, `municipios`, `naturezas`, `paises`, `qualificacoes`) → passthrough limpo

### Gold grava no Postgres com histórico mensal
Estratégia: `DELETE WHERE dataset_month = X OR data_version = X` + `JDBC append`.
Cada rerrun do mesmo mês é idempotente. Meses anteriores são preservados.
Tabelas em `cnpj_gold.*` possuem `dataset_month` e `data_version`.
Antes do append, a escrita filtra colunas para gravar apenas as existentes na tabela alvo.

### Integracao oficial com o projeto do agente
O projeto externo `agente-contabilizei-tcc` consome este DataLake.

Decisao atual:
- fonte principal do agente: Postgres (`cnpj_gold.*`)
- fallback: DuckDB + MinIO
- descoberta automatica: prioriza tabelas Gold com dados no Postgres

Motivacao:
- reduzir SQL gerada em tabelas vazias
- melhorar respostas de negocio sem depender de uma tabela especifica (ex.: cnaes)

Implicacao operacional:
- apos reset ou ambiente novo, e necessario repovoar a Gold antes de validar o agente.

### Views Gold oficiais para consumo

As views oficiais de consumo para aplicacoes e agente sao:
- `cnpj_gold.vw_empresas_com_uf`
- `cnpj_gold.vw_agente_empresas_contexto`
- `cnpj_gold.vw_agente_estabelecimentos_contexto`
- `cnpj_gold.vw_agente_socios_contexto`
- `cnpj_gold.vw_agente_cnpj_consolidado`

Essas cinco views sao parte do baseline de schema no arquivo `services/postgres/schemas.sql`.

### Observabilidade baseline

O baseline de observabilidade do projeto contem:
- metricas de stage (runs, duracao, registros e ultimo run);
- metricas de fallback de encoding (eventos e linhas corrigidas);
- dashboard Grafana `cnpj-pipeline-overview` com paineis de stage e encoding fallback.

Metricas e paineis experimentais de comportamento de IA/uso de objetos nao fazem parte do baseline atual.

---

## Limpezas Realizadas

### Sessão de 2026-04 → 2026-06

**Melhorias de performance:**
- `withColumn` em loop substituído por `select()` único (Bronze e Silver)
- SparkSession única compartilhada nas 3 camadas dentro de `run_pipeline()`
- Cache + `count()` + `unpersist()` para obter contagem sem duplo scan

**Melhorias funcionais:**
- `ingest_glob()` adicionado ao Bronze para suporte a múltiplos arquivos
- Casts de tipos implementados no Silver (`_cast_types`)
- Limpeza de aspas simples/duplas em colunas textuais na Silver
- Parse monetário robusto para `capital_social` (formato brasileiro)
- `data_version` derivado de `logical_date` do Airflow
- `records_processed` passa o valor real (antes era hardcoded `1`)
- Gold em modo clean passthrough por file_type (sem agregações pré-prontas)
- `_validate_quality()` no Silver usa `quality_threshold` do `.env`
- Gold grava também no Postgres via JDBC (delete por mês + append)
- 9 DAGs independentes — uma por tipo de arquivo (antes eram 4 + 1 genérica)
- DAG genérica `cnpj_datalake_dag.py` removida

**Limpeza estrutural (2026-06-24):**
- Deletados todos os wrappers legados em `src/` raiz:
  `config.py`, `models.py`, `pipeline.py`, `layers/`, `schemas/`, `storage/`, `utils/`
- Deletada pasta `dags/` na raiz (duplicata obsoleta — DAGs corretos em `services/airflow/dags/`)
- Deletados arquivos de análise local: `analisar_pastas.ps1`, `folder_analysis.*`
- Deletados docs redundantes: `CHECKLIST_IMPLEMENTACAO.md`, `COMECE_AQUI.txt`, `RESUMO_PROJETO.md`, `exemplos_praticos.py`
- `infra/` separada por serviço: `airflow/`, `postgres/`, `docker-compose.yml` na raiz do `infra/`
- `session/spark_session.py` fundido em `services/pyspark/spark.py` (eliminada a indireção SparkSessionManager)
- Criado `.gitignore` cobrindo: venv, logs, egg-info, dist, .env, data/input/

---

## Melhorias Pendentes (backlog)

| Melhoria | Impacto | Complexidade |
|---|---|---|
| Particionamento Parquet por `uf`/`data_version` | Alto | Baixo |
| Testes unitários para `_nullify`, `_cast_types`, `_validate_quality` | Médio | Médio |
| Schema StructType explícito no Bronze (sem split manual) | Alto | Médio |
| Integração com agente IA via DuckDB → MinIO/Postgres | Médio | Médio |
| Connection pooling no PostgresClient | Baixo | Baixo |

---

## Como executar localmente

```bash
# 1. subir a stack
docker compose -f infra/docker-compose.yml up -d

# 2. colocar arquivo em data/input/
cp ~/Downloads/Estabelecimentos0.txt data/input/

# 3. rodar via CLI
python services/pyspark/cli/run_pipeline.py --source-file "data/input/Estabelecimentos*.txt" --file-type estabelecimentos

# 4. ou acionar pelo Airflow
# http://localhost:8080 → cnpj_estabelecimentos_pipeline → Trigger DAG
```

---

## Variáveis de ambiente relevantes

| Variável | Descrição | Usado em |
|---|---|---|
| `DATA_VERSION` | Fallback para CLI local | `DataLakeConfig.from_env()` |
| `MINIO_*` | Credenciais e buckets | Bronze/Silver/Gold |
| `PG_*` | Postgres de metadados e data marts | PostgresClient |
| `SPARK_MASTER` | `local[*]` ou `spark://host` | build_spark_session |
| `QUALITY_THRESHOLD` | % mín. de não-nulos no PK | SilverLayer._validate_quality |
| `AIRFLOW_SOURCE_FILE_EMPRESAS` | Glob ou path de empresas | DAG empresas |
| `AIRFLOW_SOURCE_FILE_ESTABELECIMENTOS` | Glob ou path de estabelecimentos | DAG estabelecimentos |
| `AIRFLOW_SOURCE_FILE_SOCIOS` | Glob ou path de sócios | DAG socios |
| `AIRFLOW_SOURCE_FILE_CNAES` | Path de CNAEs | DAG cnaes |
| `AIRFLOW_SOURCE_FILE_MOTIVOS` | Path de motivos | DAG motivos |
| `AIRFLOW_SOURCE_FILE_MUNICIPIOS` | Path de municípios | DAG municipios |
| `AIRFLOW_SOURCE_FILE_NATUREZAS` | Path de naturezas | DAG naturezas |
| `AIRFLOW_SOURCE_FILE_PAISES` | Path de países | DAG paises |
| `AIRFLOW_SOURCE_FILE_QUALIFICACOES` | Path de qualificações | DAG qualificacoes |

