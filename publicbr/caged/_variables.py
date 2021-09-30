AUX_TABLES = {
    'mov': {
        'região': 'regiao.csv',
        'uf': 'uf.csv',
        'município': 'municipio.csv',
        'seção': 'secao.csv',
        'subclasse': 'subclasse.csv',
        'fonte_desl': 'fonte.csv',
        'categoria': 'categoria.csv',
        'cbo2002ocupação': 'cbo_ocupacao.csv',
        'graudeinstrução': 'grau_instrucao.csv',
        'sexo': 'sexo.csv',
        'tipoempregador': 'tipo_empregador.csv',
        'tipoestabelecimento': 'tipo_estab.csv',
        'tipomovimentação': 'tipo_mov.csv',
        'indtrabparcial': 'ind_trab_parcial.csv',
        'indtrabintermitente': 'ind_trab_intermitente.csv',
        'tipodedeficiência': 'tipo_deficiencia.csv',
        'raçacor': 'raca.csv',
        'indicadoraprendiz': 'ind_aprendiz.csv',
        'tamestabjan': 'tamanho_estab.csv'
    },
    'estab': {
        'região': 'regiao.csv',
        'uf': 'uf.csv',
        'município': 'municipio.csv',
        'seção': 'secao.csv',
        'subclasse': 'subclasse.csv',
        'tipoempregador': 'tipo_empregador.csv',
        'tipoestabelecimento': 'tipo_estab.csv',
        'tamestabjan': 'tamanho_estab.csv',
        'fonte_desl': 'fonte.csv'
    },
}
RAW_READ_OPTS = {
        'header': True,
        'sep': ';',
    }
RENAME_DICT = {
    'mov': {
        'competência': 'data',
        'região': 'cod_regiao',
        'uf': 'cod_uf',
        'município': 'cod_municipio',
        'seção': 'cod_secao',
        'subclasse': 'cod_subclasse',
        'saldomovimentação': 'saldo_movimentacao',
        'cbo2002ocupação': 'cod_cbo_ocupacao',
        'categoria': 'cod_categoria',
        'graudeinstrução': 'cod_grau_instrucao',
        'idade': 'idade',
        'horascontratuais': 'horas_contrato',
        'raçacor': 'cod_raca',
        'sexo': 'cod_sexo',
        'tipoempregador': 'cod_tipo_empregador',
        'tipoestabelecimento': 'cod_tipo_estab',
        'tipomovimentação': 'cod_tipo_mov',
        'tipodedeficiência': 'cod_tipo_deficiencia',
        'indtrabintermitente': 'cod_ind_trab_intermitente',
        'indtrabparcial': 'cod_ind_trab_parcial',
        'salário': 'salario',
        'tamestabjan': 'cod_tamanho_estab',
        'indicadoraprendiz': 'cod_ind_aprendiz',
        'fonte': 'cod_fonte'
    },
    'estab': {
        'competência': 'data',
        'região': 'cod_regiao',
        'uf': 'cod_uf',
        'município': 'cod_municipio',
        'seção': 'cod_secao',
        'subclasse': 'cod_subclasse',
        'saldomovimentação': 'saldo_movimentacao',
        'tipoempregador': 'cod_tipo_empregador',
        'tipoestabelecimento': 'cod_tipo_estab',
        'tamestabjan': 'cod_tamanho_estab',
        'fonte_desl': 'cod_fonte'
    }
}