"""Layouts esperados dos arquivos CNPJ por tipo lógico."""

from __future__ import annotations

from typing import Dict, List


CNPJ_LAYOUTS: Dict[str, List[str]] = {
    "cnaes": ["codigo", "descricao"],
    "motivos": ["codigo", "descricao"],
    "municipios": ["codigo", "descricao"],
    "naturezas": ["codigo", "descricao"],
    "paises": ["codigo", "descricao"],
    "qualificacoes": ["codigo", "descricao"],
    "empresas": [
        "cnpj_basico",
        "razao_social",
        "natureza_juridica",
        "qualificacao_responsavel",
        "capital_social",
        "porte_empresa",
        "ente_federativo_responsavel",
    ],
    "socios": [
        "cnpj_basico",
        "identificador_socio",
        "nome_socio",
        "cnpj_cpf_socio",
        "qualificacao_socio",
        "data_entrada_sociedade",
        "pais",
        "representante_legal",
        "nome_representante",
        "qualificacao_representante_legal",
        "faixa_etaria",
    ],
    "estabelecimentos": [
        "cnpj_basico",
        "cnpj_ordem",
        "cnpj_dv",
        "identificador_matriz_filial",
        "nome_fantasia",
        "situacao_cadastral",
        "data_situacao_cadastral",
        "motivo_situacao_cadastral",
        "nome_cidade_exterior",
        "pais",
        "data_inicio_atividade",
        "cnae_fiscal_principal",
        "cnae_fiscal_secundaria",
        "tipo_logradouro",
        "logradouro",
        "numero",
        "complemento",
        "bairro",
        "cep",
        "uf",
        "municipio",
        "ddd1",
        "telefone1",
        "ddd2",
        "telefone2",
        "ddd_fax",
        "fax",
        "correio_eletronico",
        "situacao_especial",
        "data_situacao_especial",
        "extra_col_1",
        "extra_col_2",
    ],
}


TYPE_ALIASES: Dict[str, str] = {
    "cnae": "cnaes",
    "cnaes": "cnaes",
    "empresa": "empresas",
    "empresas": "empresas",
    "estabelecimento": "estabelecimentos",
    "estabelecimentos": "estabelecimentos",
    "motivo": "motivos",
    "motivos": "motivos",
    "municipio": "municipios",
    "municipios": "municipios",
    "natureza": "naturezas",
    "naturezas": "naturezas",
    "pais": "paises",
    "paises": "paises",
    "qualificacao": "qualificacoes",
    "qualificacoes": "qualificacoes",
    "socio": "socios",
    "socios": "socios",
}


def normalize_file_type(file_type: str) -> str:
    key = (file_type or "").strip().lower()
    return TYPE_ALIASES.get(key, key)


def get_layout_columns(file_type: str) -> List[str]:
    return CNPJ_LAYOUTS.get(normalize_file_type(file_type), [])