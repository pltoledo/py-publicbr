AUX_NAMES = {
    'CNAE': 'df_cnae', 
    'MUNIC': 'df_mun', 
    'NATJU': 'df_natju', 
    'PAIS': 'df_pais', 
    'QUALS': 'df_qual_socio', 
    'MOTI': 'df_motivo'
}
RAW_READ_OPTS = {
        'encoding': 'ISO-8859-1',
        'sep': ';',
    }
SCHEMA_COLS = {
    'simples': [
        'cnpj', 
        'opcao_simples', 
        'data_inclusao_simples',
        'data_exclusao_simples',
        'opcao_mei', 
        'data_inclusao_mei', 
        'data_exclusao_mei'
    ],
    'socios': [
        'cnpj_empresa', 
        'id_socio', 
        'nome_socio', 
        'cpf_cnpj_socio', 
        'cod_quals', 
        'data_entrada_sociedade',
        'cod_pais', 
        'num_rep_legal', 
        'nome_rep_legal', 
        'cod_quals_rep_legal', 
        'faixa_etaria'
    ],
    'empresas': [
        'cnpj', 
        'razao_social', 
        'cod_natju', 
        'cod_quals', 
        'capital_social', 
        'porte', 
        'ente_fed_resp'
    ],
    'estab': [
        'cnpj', 
        'cnpj_ordem', 
        'cnpj_dv', 
        'id_matriz', 
        'nome_fantasia', 
        'situacao_cadastral', 
        'data_situacao_cadastral', 
        'motivo_situacao_cadastral',
        'nome_cidade_ext', 
        'cod_pais', 
        'data_inicio_atividades', 
        'cnae_primario', 
        'cnae_secundario', 
        'tipo_logradouro', 
        'logradouro', 
        'numero', 
        'complemento', 
        'bairro', 
        'cep', 
        'uf', 
        'cod_mun', 
        'ddd_1', 
        'telefone_1', 
        'ddd_2', 
        'telefone_2', 
        'ddd_fax', 
        'fax', 
        'correio_eletronico', 
        'situacao_especial', 
        'data_situacao_especial'
    ],
}