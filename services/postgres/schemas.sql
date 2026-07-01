-- Inicialização do banco de dados CNPJ Data Lake
-- Segurança básica aplicada com papéis separados de owner e app.

DO
$$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'datalake_owner') THEN
        CREATE ROLE datalake_owner NOLOGIN;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'datalake_app') THEN
        CREATE ROLE datalake_app LOGIN PASSWORD 'datalake_app_change_me';
    END IF;
END
$$;

REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON DATABASE cnpj_datalake FROM PUBLIC;

CREATE SCHEMA IF NOT EXISTS cnpj_metadata AUTHORIZATION datalake_owner;
CREATE SCHEMA IF NOT EXISTS cnpj_bronze AUTHORIZATION datalake_owner;
CREATE SCHEMA IF NOT EXISTS cnpj_silver AUTHORIZATION datalake_owner;
CREATE SCHEMA IF NOT EXISTS cnpj_gold AUTHORIZATION datalake_owner;

GRANT USAGE ON SCHEMA cnpj_metadata TO datalake_app;
GRANT USAGE, CREATE ON SCHEMA cnpj_bronze TO datalake_app;
GRANT USAGE, CREATE ON SCHEMA cnpj_silver TO datalake_app;
GRANT USAGE, CREATE ON SCHEMA cnpj_gold TO datalake_app;

CREATE TABLE IF NOT EXISTS cnpj_metadata.processing_log (
    id SERIAL PRIMARY KEY,
    file_name VARCHAR(255) NOT NULL,
    file_type VARCHAR(50) NOT NULL,
    source_path TEXT NOT NULL,
    total_records BIGINT,
    processed_records BIGINT DEFAULT 0,
    error_records BIGINT DEFAULT 0,
    quality_score DECIMAL(5,2),
    status VARCHAR(50) DEFAULT 'pending',
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    execution_time_seconds INTEGER,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE cnpj_metadata.processing_log OWNER TO datalake_owner;

CREATE INDEX IF NOT EXISTS idx_processing_status ON cnpj_metadata.processing_log(status);
CREATE INDEX IF NOT EXISTS idx_processing_file_type ON cnpj_metadata.processing_log(file_type);
CREATE INDEX IF NOT EXISTS idx_processing_created ON cnpj_metadata.processing_log(created_at DESC);

CREATE TABLE IF NOT EXISTS cnpj_metadata.files_metadata (
    id SERIAL PRIMARY KEY,
    file_name VARCHAR(255) UNIQUE NOT NULL,
    file_type VARCHAR(50) NOT NULL,
    size_bytes BIGINT,
    record_count BIGINT,
    parquet_path TEXT,
    hash_value VARCHAR(64),
    version VARCHAR(20),
    processed_date DATE,
    bucket_location VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS cnpj_metadata.quality_alerts (
    id SERIAL PRIMARY KEY,
    processing_log_id INTEGER REFERENCES cnpj_metadata.processing_log(id) ON DELETE CASCADE,
    alert_type VARCHAR(50) NOT NULL,
    alert_message TEXT NOT NULL,
    record_line BIGINT,
    severity VARCHAR(20) DEFAULT 'WARNING',
    field_name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE cnpj_metadata.files_metadata OWNER TO datalake_owner;
ALTER TABLE cnpj_metadata.quality_alerts OWNER TO datalake_owner;

CREATE INDEX IF NOT EXISTS idx_quality_alerts_severity ON cnpj_metadata.quality_alerts(severity);
CREATE INDEX IF NOT EXISTS idx_quality_alerts_type ON cnpj_metadata.quality_alerts(alert_type);

CREATE TABLE IF NOT EXISTS cnpj_metadata.config (
    key VARCHAR(100) PRIMARY KEY,
    value TEXT NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE cnpj_metadata.config OWNER TO datalake_owner;

INSERT INTO cnpj_metadata.config (key, value, description) VALUES
    ('data_version', '2026-03', 'Versão dos dados CNPJ'),
    ('minio_endpoint', 'minio:9000', 'Endpoint do MinIO'),
    ('minio_bucket_bronze', 'cnpj-bronze', 'Bucket para dados brutos'),
    ('minio_bucket_silver', 'cnpj-silver', 'Bucket para dados processados'),
    ('minio_bucket_gold', 'cnpj-gold', 'Bucket para dados finais'),
    ('max_records_batch', '100000', 'Máximo de registros por batch'),
    ('quality_threshold', '90', 'Limiar mínimo de qualidade (%)'),
    ('retention_days_bronze', '90', 'Dias de retenção dados bronze'),
    ('retention_days_silver', '180', 'Dias de retenção dados silver'),
    ('enable_notifications', 'true', 'Habilitar notificações')
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;

CREATE TABLE IF NOT EXISTS cnpj_metadata.pipeline_execution (
    id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL,
    source_file VARCHAR(255),
    data_version VARCHAR(7),
    dataset_month VARCHAR(7),
    status VARCHAR(50) DEFAULT 'running',
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds INTEGER,
    records_processed BIGINT DEFAULT 0,
    records_failed BIGINT DEFAULT 0,
    output_path TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE cnpj_metadata.pipeline_execution OWNER TO datalake_owner;

CREATE INDEX IF NOT EXISTS idx_pipeline_status ON cnpj_metadata.pipeline_execution(status);
CREATE INDEX IF NOT EXISTS idx_pipeline_name ON cnpj_metadata.pipeline_execution(pipeline_name);

CREATE TABLE IF NOT EXISTS cnpj_bronze.empresas_raw (
    cnpj_basico TEXT,
    razao_social TEXT,
    natureza_juridica TEXT,
    qualificacao_responsavel TEXT,
    capital_social TEXT,
    porte_empresa TEXT,
    ente_federativo_responsavel TEXT,
    source_file TEXT,
    dataset_month TEXT,
    data_version TEXT,
    ingestion_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS cnpj_bronze.estabelecimentos_raw (
    cnpj_basico TEXT,
    cnpj_ordem TEXT,
    cnpj_dv TEXT,
    identificador_matriz_filial TEXT,
    nome_fantasia TEXT,
    situacao_cadastral TEXT,
    data_situacao_cadastral TEXT,
    motivo_situacao_cadastral TEXT,
    nome_cidade_exterior TEXT,
    pais TEXT,
    data_inicio_atividade TEXT,
    cnae_fiscal_principal TEXT,
    cnae_fiscal_secundaria TEXT,
    tipo_logradouro TEXT,
    logradouro TEXT,
    numero TEXT,
    complemento TEXT,
    bairro TEXT,
    cep TEXT,
    uf TEXT,
    municipio TEXT,
    ddd1 TEXT,
    telefone1 TEXT,
    ddd2 TEXT,
    telefone2 TEXT,
    ddd_fax TEXT,
    fax TEXT,
    correio_eletronico TEXT,
    situacao_especial TEXT,
    data_situacao_especial TEXT,
    extra_col_1 TEXT,
    extra_col_2 TEXT,
    source_file TEXT,
    dataset_month TEXT,
    data_version TEXT,
    ingestion_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    layout_status TEXT,
    column_count INTEGER
);

CREATE TABLE IF NOT EXISTS cnpj_bronze.socios_raw (
    cnpj_basico TEXT,
    identificador_socio TEXT,
    nome_socio TEXT,
    cnpj_cpf_socio TEXT,
    qualificacao_socio TEXT,
    data_entrada_sociedade TEXT,
    pais TEXT,
    representante_legal TEXT,
    nome_representante TEXT,
    qualificacao_representante_legal TEXT,
    faixa_etaria TEXT,
    source_file TEXT,
    dataset_month TEXT,
    data_version TEXT,
    ingestion_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS cnpj_bronze.cnaes_raw (
    codigo TEXT,
    descricao TEXT,
    source_file TEXT,
    dataset_month TEXT,
    data_version TEXT,
    ingestion_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS cnpj_bronze.motivos_raw (
    codigo TEXT,
    descricao TEXT,
    source_file TEXT,
    dataset_month TEXT,
    data_version TEXT,
    ingestion_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS cnpj_bronze.municipios_raw (
    codigo TEXT,
    descricao TEXT,
    source_file TEXT,
    dataset_month TEXT,
    data_version TEXT,
    ingestion_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS cnpj_bronze.naturezas_raw (
    codigo TEXT,
    descricao TEXT,
    source_file TEXT,
    dataset_month TEXT,
    data_version TEXT,
    ingestion_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS cnpj_bronze.paises_raw (
    codigo TEXT,
    descricao TEXT,
    source_file TEXT,
    dataset_month TEXT,
    data_version TEXT,
    ingestion_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS cnpj_bronze.qualificacoes_raw (
    codigo TEXT,
    descricao TEXT,
    source_file TEXT,
    dataset_month TEXT,
    data_version TEXT,
    ingestion_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE cnpj_bronze.empresas_raw OWNER TO datalake_owner;
ALTER TABLE cnpj_bronze.estabelecimentos_raw OWNER TO datalake_owner;
ALTER TABLE cnpj_bronze.socios_raw OWNER TO datalake_owner;
ALTER TABLE cnpj_bronze.cnaes_raw OWNER TO datalake_owner;
ALTER TABLE cnpj_bronze.motivos_raw OWNER TO datalake_owner;
ALTER TABLE cnpj_bronze.municipios_raw OWNER TO datalake_owner;
ALTER TABLE cnpj_bronze.naturezas_raw OWNER TO datalake_owner;
ALTER TABLE cnpj_bronze.paises_raw OWNER TO datalake_owner;
ALTER TABLE cnpj_bronze.qualificacoes_raw OWNER TO datalake_owner;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA cnpj_metadata TO datalake_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA cnpj_metadata TO datalake_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA cnpj_bronze TO datalake_app;

ALTER DEFAULT PRIVILEGES FOR ROLE datalake_owner IN SCHEMA cnpj_metadata
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO datalake_app;

ALTER DEFAULT PRIVILEGES FOR ROLE datalake_owner IN SCHEMA cnpj_metadata
GRANT USAGE, SELECT ON SEQUENCES TO datalake_app;

ALTER DEFAULT PRIVILEGES FOR ROLE datalake_owner IN SCHEMA cnpj_bronze
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO datalake_app;

-- =============================================================================
-- DATA MARTS: cnpj_gold
-- Camada Gold limpa (detalhada), sem agregações prontas.
-- As agregações ficam para consulta ad-hoc (agente/BI) via SQL.
-- Todas as colunas são TEXT para máxima compatibilidade com o JDBC write do Spark.
-- Consulte data_version para filtrar por mês (ex: WHERE data_version = '2026-06').
-- =============================================================================

CREATE TABLE IF NOT EXISTS cnpj_gold.empresas (
    cnpj_basico                  TEXT,
    razao_social                 TEXT,
    natureza_juridica            TEXT,
    qualificacao_responsavel     TEXT,
    capital_social               TEXT,
    porte_empresa                TEXT,
    ente_federativo_responsavel  TEXT,
    dataset_month         TEXT,
    data_version          TEXT
);

CREATE TABLE IF NOT EXISTS cnpj_gold.estabelecimentos (
    cnpj_basico                  TEXT,
    cnpj_ordem                   TEXT,
    cnpj_dv                      TEXT,
    identificador_matriz_filial  TEXT,
    nome_fantasia                TEXT,
    situacao_cadastral           TEXT,
    data_situacao_cadastral      TEXT,
    motivo_situacao_cadastral    TEXT,
    nome_cidade_exterior         TEXT,
    pais                         TEXT,
    data_inicio_atividade        TEXT,
    cnae_fiscal_principal        TEXT,
    cnae_fiscal_secundaria       TEXT,
    tipo_logradouro              TEXT,
    logradouro                   TEXT,
    numero                       TEXT,
    complemento                  TEXT,
    bairro                       TEXT,
    cep                          TEXT,
    uf                           TEXT,
    municipio                    TEXT,
    ddd1                         TEXT,
    telefone1                    TEXT,
    ddd2                         TEXT,
    telefone2                    TEXT,
    ddd_fax                      TEXT,
    fax                          TEXT,
    correio_eletronico           TEXT,
    situacao_especial            TEXT,
    data_situacao_especial       TEXT,
    extra_col_1                  TEXT,
    extra_col_2                  TEXT,
    dataset_month     TEXT,
    data_version      TEXT
);

CREATE TABLE IF NOT EXISTS cnpj_gold.socios (
    cnpj_basico                       TEXT,
    identificador_socio               TEXT,
    nome_socio                        TEXT,
    cnpj_cpf_socio                    TEXT,
    qualificacao_socio                TEXT,
    data_entrada_sociedade            TEXT,
    pais                              TEXT,
    representante_legal               TEXT,
    nome_representante                TEXT,
    qualificacao_representante_legal  TEXT,
    faixa_etaria                      TEXT,
    dataset_month      TEXT,
    data_version       TEXT
);

-- Tabelas de referência (passthrough) — evoluem mensalmente junto com a Receita
CREATE TABLE IF NOT EXISTS cnpj_gold.cnaes (
    codigo       TEXT,
    descricao    TEXT,
    dataset_month TEXT,
    data_version TEXT
);

CREATE TABLE IF NOT EXISTS cnpj_gold.motivos (
    codigo       TEXT,
    descricao    TEXT,
    dataset_month TEXT,
    data_version TEXT
);

CREATE TABLE IF NOT EXISTS cnpj_gold.municipios (
    codigo       TEXT,
    descricao    TEXT,
    dataset_month TEXT,
    data_version TEXT
);

CREATE TABLE IF NOT EXISTS cnpj_gold.naturezas (
    codigo       TEXT,
    descricao    TEXT,
    dataset_month TEXT,
    data_version TEXT
);

CREATE TABLE IF NOT EXISTS cnpj_gold.paises (
    codigo       TEXT,
    descricao    TEXT,
    dataset_month TEXT,
    data_version TEXT
);

CREATE TABLE IF NOT EXISTS cnpj_gold.qualificacoes (
    codigo       TEXT,
    descricao    TEXT,
    dataset_month TEXT,
    data_version TEXT
);

-- View de consumo para consultas de empresas com UF/municipio sem depender de join ad-hoc.
-- Prioriza estabelecimento matriz; na ausencia, escolhe o primeiro estabelecimento disponivel.
CREATE OR REPLACE VIEW cnpj_gold.vw_empresas_com_uf AS
WITH estabelecimentos_preferenciais AS (
    SELECT
        est.cnpj_basico,
        est.data_version,
        est.dataset_month,
        est.uf,
        est.municipio,
        ROW_NUMBER() OVER (
            PARTITION BY est.cnpj_basico, est.data_version
            ORDER BY
                CASE WHEN est.identificador_matriz_filial = '1' THEN 0 ELSE 1 END,
                est.cnpj_ordem,
                est.cnpj_dv
        ) AS rn
    FROM cnpj_gold.estabelecimentos est
)
SELECT
    emp.cnpj_basico,
    emp.razao_social,
    emp.natureza_juridica,
    emp.qualificacao_responsavel,
    emp.capital_social,
    emp.porte_empresa,
    emp.ente_federativo_responsavel,
    est.uf AS uf,
    est.municipio AS municipio,
    emp.dataset_month,
    emp.data_version
FROM cnpj_gold.empresas emp
LEFT JOIN estabelecimentos_preferenciais est
    ON est.cnpj_basico = emp.cnpj_basico
   AND est.data_version = emp.data_version
   AND est.rn = 1;

-- View orientada ao agente: um registro por CNPJ basico com contexto de localizacao e atividade principal.
CREATE OR REPLACE VIEW cnpj_gold.vw_agente_empresas_contexto AS
WITH empresas_preferenciais AS (
    SELECT
        emp.*,
        ROW_NUMBER() OVER (
            PARTITION BY emp.cnpj_basico, emp.data_version
            ORDER BY emp.razao_social
        ) AS rn
    FROM cnpj_gold.empresas emp
),
estabelecimentos_preferenciais AS (
    SELECT
        est.*,
        ROW_NUMBER() OVER (
            PARTITION BY est.cnpj_basico, est.data_version
            ORDER BY
                CASE WHEN est.identificador_matriz_filial = '1' THEN 0 ELSE 1 END,
                est.cnpj_ordem,
                est.cnpj_dv
        ) AS rn
    FROM cnpj_gold.estabelecimentos est
)
SELECT
    emp.cnpj_basico,
    emp.razao_social,
    emp.natureza_juridica,
    emp.qualificacao_responsavel,
    emp.capital_social,
    emp.porte_empresa,
    emp.ente_federativo_responsavel,
    est.identificador_matriz_filial,
    est.nome_fantasia,
    est.situacao_cadastral,
    est.data_inicio_atividade,
    est.uf,
    est.municipio AS municipio_codigo,
    mun.descricao AS municipio_nome,
    est.cnae_fiscal_principal,
    cnae.descricao AS cnae_principal_descricao,
    est.ddd1,
    est.telefone1,
    est.correio_eletronico,
    emp.dataset_month,
    emp.data_version
FROM empresas_preferenciais emp
LEFT JOIN estabelecimentos_preferenciais est
    ON est.cnpj_basico = emp.cnpj_basico
   AND est.data_version = emp.data_version
   AND est.rn = 1
LEFT JOIN cnpj_gold.municipios mun
    ON mun.codigo = est.municipio
   AND mun.data_version = est.data_version
LEFT JOIN cnpj_gold.cnaes cnae
    ON cnae.codigo = est.cnae_fiscal_principal
   AND cnae.data_version = est.data_version
WHERE emp.rn = 1;

-- View orientada ao agente: detalhe de estabelecimentos com dimensoes descritivas.
CREATE OR REPLACE VIEW cnpj_gold.vw_agente_estabelecimentos_contexto AS
SELECT
    est.cnpj_basico,
    est.cnpj_ordem,
    est.cnpj_dv,
    (est.cnpj_basico || est.cnpj_ordem || est.cnpj_dv) AS cnpj_completo,
    est.identificador_matriz_filial,
    est.nome_fantasia,
    est.situacao_cadastral,
    est.data_situacao_cadastral,
    est.motivo_situacao_cadastral,
    mot.descricao AS motivo_situacao_cadastral_descricao,
    est.data_inicio_atividade,
    est.cnae_fiscal_principal,
    cnae.descricao AS cnae_principal_descricao,
    est.uf,
    est.municipio AS municipio_codigo,
    mun.descricao AS municipio_nome,
    est.tipo_logradouro,
    est.logradouro,
    est.numero,
    est.complemento,
    est.bairro,
    est.cep,
    est.ddd1,
    est.telefone1,
    est.ddd2,
    est.telefone2,
    est.correio_eletronico,
    est.dataset_month,
    est.data_version
FROM cnpj_gold.estabelecimentos est
LEFT JOIN cnpj_gold.motivos mot
    ON mot.codigo = est.motivo_situacao_cadastral
   AND mot.data_version = est.data_version
LEFT JOIN cnpj_gold.cnaes cnae
    ON cnae.codigo = est.cnae_fiscal_principal
   AND cnae.data_version = est.data_version
LEFT JOIN cnpj_gold.municipios mun
    ON mun.codigo = est.municipio
   AND mun.data_version = est.data_version;

-- View orientada ao agente: contexto de socios com qualificacao e pais.
CREATE OR REPLACE VIEW cnpj_gold.vw_agente_socios_contexto AS
SELECT
    soc.cnpj_basico,
    soc.identificador_socio,
    soc.nome_socio,
    soc.cnpj_cpf_socio,
    soc.qualificacao_socio,
    qual.descricao AS qualificacao_socio_descricao,
    soc.data_entrada_sociedade,
    soc.pais AS pais_codigo,
    pai.descricao AS pais_nome,
    soc.representante_legal,
    soc.nome_representante,
    soc.qualificacao_representante_legal,
    soc.faixa_etaria,
    soc.dataset_month,
    soc.data_version
FROM cnpj_gold.socios soc
LEFT JOIN cnpj_gold.qualificacoes qual
    ON qual.codigo = soc.qualificacao_socio
   AND qual.data_version = soc.data_version
LEFT JOIN cnpj_gold.paises pai
    ON pai.codigo = soc.pais
   AND pai.data_version = soc.data_version;

-- View orientada ao agente: consolidacao por CNPJ com principal estabelecimento e contagem de socios.
CREATE OR REPLACE VIEW cnpj_gold.vw_agente_cnpj_consolidado AS
WITH empresas_preferenciais AS (
    SELECT
        emp.*,
        ROW_NUMBER() OVER (
            PARTITION BY emp.cnpj_basico, emp.data_version
            ORDER BY emp.razao_social
        ) AS rn
    FROM cnpj_gold.empresas emp
),
estabelecimentos_preferenciais AS (
    SELECT
        est.*,
        ROW_NUMBER() OVER (
            PARTITION BY est.cnpj_basico, est.data_version
            ORDER BY
                CASE WHEN est.identificador_matriz_filial = '1' THEN 0 ELSE 1 END,
                est.cnpj_ordem,
                est.cnpj_dv
        ) AS rn
    FROM cnpj_gold.estabelecimentos est
),
socios_por_empresa AS (
    SELECT
        cnpj_basico,
        data_version,
        COUNT(*) AS quantidade_socios
    FROM cnpj_gold.socios
    GROUP BY cnpj_basico, data_version
)
SELECT
    emp.cnpj_basico,
    emp.razao_social,
    emp.natureza_juridica,
    emp.porte_empresa,
    est.cnpj_ordem,
    est.cnpj_dv,
    est.identificador_matriz_filial,
    est.nome_fantasia,
    est.situacao_cadastral,
    est.uf,
    est.municipio AS municipio_codigo,
    mun.descricao AS municipio_nome,
    est.cnae_fiscal_principal,
    cnae.descricao AS cnae_principal_descricao,
    COALESCE(sp.quantidade_socios, 0) AS quantidade_socios,
    emp.dataset_month,
    emp.data_version
FROM empresas_preferenciais emp
LEFT JOIN estabelecimentos_preferenciais est
    ON est.cnpj_basico = emp.cnpj_basico
   AND est.data_version = emp.data_version
   AND est.rn = 1
LEFT JOIN socios_por_empresa sp
    ON sp.cnpj_basico = emp.cnpj_basico
   AND sp.data_version = emp.data_version
LEFT JOIN cnpj_gold.municipios mun
    ON mun.codigo = est.municipio
   AND mun.data_version = est.data_version
LEFT JOIN cnpj_gold.cnaes cnae
    ON cnae.codigo = est.cnae_fiscal_principal
   AND cnae.data_version = est.data_version
WHERE emp.rn = 1;

ALTER TABLE cnpj_gold.empresas                      OWNER TO datalake_owner;
ALTER TABLE cnpj_gold.estabelecimentos              OWNER TO datalake_owner;
ALTER TABLE cnpj_gold.socios                        OWNER TO datalake_owner;
ALTER TABLE cnpj_gold.cnaes                         OWNER TO datalake_owner;
ALTER TABLE cnpj_gold.motivos                       OWNER TO datalake_owner;
ALTER TABLE cnpj_gold.municipios                    OWNER TO datalake_owner;
ALTER TABLE cnpj_gold.naturezas                     OWNER TO datalake_owner;
ALTER TABLE cnpj_gold.paises                        OWNER TO datalake_owner;
ALTER TABLE cnpj_gold.qualificacoes                 OWNER TO datalake_owner;
ALTER VIEW cnpj_gold.vw_empresas_com_uf             OWNER TO datalake_owner;
ALTER VIEW cnpj_gold.vw_agente_empresas_contexto    OWNER TO datalake_owner;
ALTER VIEW cnpj_gold.vw_agente_estabelecimentos_contexto OWNER TO datalake_owner;
ALTER VIEW cnpj_gold.vw_agente_socios_contexto      OWNER TO datalake_owner;
ALTER VIEW cnpj_gold.vw_agente_cnpj_consolidado     OWNER TO datalake_owner;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA cnpj_gold TO datalake_app;
GRANT SELECT ON cnpj_gold.vw_empresas_com_uf TO datalake_app;
GRANT SELECT ON cnpj_gold.vw_agente_empresas_contexto TO datalake_app;
GRANT SELECT ON cnpj_gold.vw_agente_estabelecimentos_contexto TO datalake_app;
GRANT SELECT ON cnpj_gold.vw_agente_socios_contexto TO datalake_app;
GRANT SELECT ON cnpj_gold.vw_agente_cnpj_consolidado TO datalake_app;

ALTER DEFAULT PRIVILEGES FOR ROLE datalake_owner IN SCHEMA cnpj_gold
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO datalake_app;