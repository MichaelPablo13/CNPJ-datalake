# Runbook Operacional — CNPJ DataLake

## 1. Preparar os arquivos de entrada

Coloque os arquivos da Receita Federal nas subpastas de `data/input/` no host.
O Airflow os acessa no container como `/opt/airflow/data/input/`.

Observacoes importantes:
- Arquivos `.zip` nao sao lidos diretamente pelo pipeline. Descompacte antes.
- Nomes crus extraidos do zip tambem sao aceitos.

**Nomes esperados:**

| Tipo | Arquivos |
|---|---|
| Empresas | `data/input/empresas/Empresas*.txt` ou nome cru (ex.: `K3241...EMPRECSV`) |
| Estabelecimentos | `data/input/estabelecimentos/Estabelecimentos*.txt` ou nome cru |
| Socios | `data/input/socios/Socios*.txt` ou nome cru |
| CNAEs | `data/input/cnaes/Cnaes.txt` ou nome cru |
| Motivos | `data/input/motivos/Motivos.txt` ou nome cru |
| Municipios | `data/input/municipios/Municipios.txt` ou nome cru |
| Naturezas | `data/input/naturezas/Naturezas.txt` ou nome cru |
| Paises | `data/input/paises/Paises.txt` ou nome cru |
| Qualificacoes | `data/input/qualificacoes/Qualificacoes.txt` ou nome cru |

---

## 2. Subir a infraestrutura

```powershell
docker compose -f infra/docker-compose.yml up -d --build
```

| Servico | URL | Credenciais padrao |
|---|---|---|
| Airflow | http://localhost:8080 | admin / valor de AIRFLOW_ADMIN_PASSWORD |
| MinIO Console | http://localhost:9001 | minio_root / minio_root_123 |
| pgAdmin | http://localhost:5050 | admin@local.com / admin |
| Pushgateway | http://localhost:9091 | sem autenticacao |
| Prometheus | http://localhost:9090 | sem autenticacao |
| Grafana | http://localhost:3000 | admin / valor de GRAFANA_ADMIN_PASSWORD |

---

## 3. Disparar pelo Airflow

1. Abra http://localhost:8080
2. Escolha a DAG do tipo desejado
3. Clique em **Trigger DAG** (modo manual)

### 3.1 Definir data_version no Trigger DAG

No campo **Run Configuration / Conf** do Trigger DAG, informe:

```json
{"dataset_month":"2026-03"}
```

Se sua tela nao mostrar o campo JSON, configure antes de ingerir no `.env`:

```env
INGESTION_DATA_MONTH=2026-03
```

e recarregue:

```powershell
docker compose -f infra/docker-compose.yml up -d airflow-webserver airflow-scheduler
```

Prioridade aplicada no runtime:
1. dag_run.conf.data_version
2. dag_run.conf.dataset_month
3. INGESTION_DATA_MONTH
4. AIRFLOW_DATA_VERSION_OVERRIDE
5. DATA_VERSION
6. logical_date do Airflow

Para fixar o mes para varias execucoes sem preencher o JSON toda vez, use no `.env`:

INGESTION_DATA_MONTH=2026-03

Alternativa legada (tambem suportada):

AIRFLOW_DATA_VERSION_OVERRIDE=2026-03

**DAGs disponíveis (9):**

| DAG | Arquivo de entrada |
|---|---|
| `cnpj_empresas_pipeline` | `Empresas*.txt` |
| `cnpj_estabelecimentos_pipeline` | `Estabelecimentos*.txt` |
| `cnpj_socios_pipeline` | `Socios*.txt` |
| `cnpj_cnaes_pipeline` | `Cnaes.txt` |
| `cnpj_motivos_pipeline` | `Motivos.txt` |
| `cnpj_municipios_pipeline` | `Municipios.txt` |
| `cnpj_naturezas_pipeline` | `Naturezas.txt` |
| `cnpj_paises_pipeline` | `Paises.txt` |
| `cnpj_qualificacoes_pipeline` | `Qualificacoes.txt` |

Ao final de uma execucao bem sucedida, os arquivos de entrada consumidos sao movidos para:
- `data/consumed/<dataset>/<data_version>/`

O mes efetivo da execucao tambem e salvo em:
- `cnpj_metadata.pipeline_execution.dataset_month`
- `cnpj_metadata.pipeline_execution.data_version`

---

## 4. Disparar pela CLI (local)

```powershell
# arquivo unico
python services/pyspark/cli/run_pipeline.py --source-file "data/input/Cnaes.txt" --file-type cnaes

# multiplos arquivos via glob
python services/pyspark/cli/run_pipeline.py --source-file "data/input/Empresas*.txt" --file-type empresas

# com primary keys para deduplicacao na Silver
python services/pyspark/cli/run_pipeline.py `
  --source-file "data/input/Estabelecimentos*.txt" `
  --file-type estabelecimentos `
  --primary-keys cnpj_basico,cnpj_ordem,cnpj_dv
```

---

## 5. Verificar resultado

**No MinIO** (http://localhost:9001):
```
cnpj-bronze / 2026-06 / empresas / *.parquet
cnpj-silver / 2026-06 / empresas / *.parquet
cnpj-gold   / 2026-06 / empresas / *.parquet
```

**No Postgres** (via pgAdmin):
```sql
-- Historico de execucoes
SELECT * FROM cnpj_metadata.pipeline_execution ORDER BY start_time DESC;

-- Checagem rapida para consumo no agente (tabelas Gold com dados)
SELECT 'estabelecimentos' AS tabela, COUNT(*) AS total FROM cnpj_gold.estabelecimentos
UNION ALL SELECT 'empresas', COUNT(*) FROM cnpj_gold.empresas
UNION ALL SELECT 'socios', COUNT(*) FROM cnpj_gold.socios
UNION ALL SELECT 'cnaes', COUNT(*) FROM cnpj_gold.cnaes
UNION ALL SELECT 'motivos', COUNT(*) FROM cnpj_gold.motivos
UNION ALL SELECT 'municipios', COUNT(*) FROM cnpj_gold.municipios
UNION ALL SELECT 'naturezas', COUNT(*) FROM cnpj_gold.naturezas
UNION ALL SELECT 'paises', COUNT(*) FROM cnpj_gold.paises
UNION ALL SELECT 'qualificacoes', COUNT(*) FROM cnpj_gold.qualificacoes
ORDER BY total DESC, tabela;

-- Data mart mensal
SELECT * FROM cnpj_gold.empresas
WHERE data_version = '2026-06'
ORDER BY cnpj_basico
LIMIT 100;

-- Comparacao entre meses
SELECT data_version, COUNT(DISTINCT cnpj_basico) AS total_cnpjs
FROM cnpj_gold.estabelecimentos
GROUP BY data_version
ORDER BY data_version;

-- Historico anual por mes (2026)
SELECT data_version, COUNT(DISTINCT cnpj_basico) AS total
FROM cnpj_gold.estabelecimentos
WHERE LEFT(data_version, 4) = '2026'
GROUP BY data_version
ORDER BY data_version;

-- Consolidado anual (2026)
SELECT LEFT(data_version, 4) AS ano, COUNT(DISTINCT cnpj_basico) AS total_empresas_ano
FROM cnpj_gold.empresas
WHERE LEFT(data_version, 4) = '2026'
GROUP BY LEFT(data_version, 4);
```

### 5.1 Validacao para o projeto do agente

Antes de testar perguntas no `agente-contabilizei-tcc`:

1. confirme que pelo menos uma tabela Gold possui linhas no Postgres;
2. valide se ha `dataset_month` preenchido nas tabelas com dados;
3. se tudo estiver vazio, rode novamente as DAGs no Airflow para repovoar a Gold.

Observacao: o agente usa Postgres como fonte principal e DuckDB+MinIO apenas como fallback.

---

## 6. Diagnosticar arquivos de entrada

```powershell
python src/scripts/profile_source_files.py --folder data/input/empresas
```

Mostra contagem de colunas e primeira linha de cada arquivo.

Se houver acentuacao quebrada (ex.: "?" ou sequencias estranhas), rode o diagnostico de encoding:

```powershell
python src/scripts/detect_input_encoding.py data/input/empresas
```

Depois ajuste no `.env`:

```env
INPUT_FILE_ENCODING=latin1
```

Se necessario, teste `cp1252` e reexecute a ingestao.

---

## 7. Empacotar

```powershell
python -m pip install build
python -m build
```

---

## 8. Limpar e recriar schemas Postgres

Use quando o banco estiver com volume antigo e faltar tabela/schema esperado.

Forma recomendada (limpa Postgres e MinIO juntos):

```powershell
python src/scripts/reset_datastores.py
```

Alternativa manual (apenas Postgres):

```powershell
docker compose -f infra/docker-compose.yml exec -T postgres psql -U postgres -d cnpj_datalake -c "DROP SCHEMA IF EXISTS cnpj_bronze CASCADE; DROP SCHEMA IF EXISTS cnpj_silver CASCADE; DROP SCHEMA IF EXISTS cnpj_gold CASCADE; DROP SCHEMA IF EXISTS cnpj_metadata CASCADE;"

docker compose -f infra/docker-compose.yml exec -T postgres psql -U postgres -d cnpj_datalake -f /docker-entrypoint-initdb.d/02-schemas.sql
```

Observacao:
- Scripts em `/docker-entrypoint-initdb.d` executam automaticamente apenas na primeira inicializacao do volume.

---

## 9. Limpar uma tabela especifica e reingerir

Quando quiser testar o app do agente com reprocessamento de um dataset especifico (sem reset total), use:

```powershell
python src/scripts/reingest_table.py `
  --file-type empresas `
  --source-file "data/input/empresas/Empresas*.txt" `
  --data-version 2026-03 `
  --primary-keys cnpj_basico
```

Comportamento do script:
- remove no Postgres apenas as linhas da tabela alvo no mes informado (`dataset_month`/`data_version`);
- limpa objetos do mesmo dataset/mes nos buckets Bronze, Silver e Gold no MinIO;
- executa nova ingestao Bronze -> Silver -> Gold com o `run_pipeline()`.

---

## 10. Observabilidade (Prometheus + Grafana)

Com a stack em pe, o pipeline publica metricas no Pushgateway e o Prometheus coleta automaticamente.

Subir apenas observabilidade:

```powershell
docker compose -f infra/docker-compose.yml up -d pushgateway prometheus grafana
```

Validar endpoints:

```powershell
curl http://localhost:9091/metrics
curl http://localhost:9090/-/healthy
curl http://localhost:3000/api/health
```

Se as DAGs ja estiverem rodando antes da mudanca de `.env`, recarregue os servicos Airflow para aplicar `PROMETHEUS_PUSHGATEWAY_URL`:

```powershell
docker compose -f infra/docker-compose.yml up -d airflow-webserver airflow-scheduler
```

No Grafana, o datasource Prometheus e o dashboard `CNPJ Pipeline Overview` sao provisionados automaticamente.

Escopo baseline atual do dashboard `cnpj-pipeline-overview`:
- paineis de execucao/sucesso/falha por stage;
- duracao e registros por stage/file_type;
- fallback de encoding: linhas corrigidas e trocas de encoding.

Consultas PromQL de referencia:

```promql
sum(cnpj_pipeline_stage_runs_total)
sum by (stage, file_type) (cnpj_pipeline_records_total)
sum by (file_type, from_encoding, to_encoding) (cnpj_pipeline_encoding_fallback_events_total)
sum(cnpj_pipeline_encoding_fallback_corrected_rows_total)
```

Observacao:
- o baseline nao inclui metricas/paineis experimentais de comportamento de IA e utilizacao de objetos de dados.

Opcoes uteis:
- `--table cnpj_gold.empresas` para forcar tabela especifica;
- `--skip-minio-clean` para manter os objetos no MinIO;
- `--skip-reingest` para apenas limpar;
- `--dry-run` para simular sem alterar dados.
