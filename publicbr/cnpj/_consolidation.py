from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as f
from ..utils import *
from ..base import Cleaner
from ._variables import *
import os
import shutil
from typing import List

class AuxCleaner(Cleaner):
    """
    Class used to clean the auxiliary tables that compose the CNPJ data. Currently, they are the following:

    * CNAE
    * Municípios
    * Natureza Jurídica
    * País
    * Qualificação de Sócios
    * Motivo da Situação Cadastral
    
    Parameters
    ----------
    spark_session : pyspark.sql.SparkSession
        Spark Session used to manipulate data

    file_dir : str
        Path to where the raw data is stored.

    save_dir : str
        Path to where the consolidated data should be stored

    Attributes
    -------
    spark : pyspark.sql.SparkSession
        Spark session used in data manipulation

    file_dir : str
        Path to where the raw data is stored.

    save_dir : str
        Path to where the consolidated data should be stored
    
    file_ids : List[str]
        Names used to identify the auxiliary tables
    
    files : str
        Name of the raw files
    
    """

    def __init__(self, spark_session, file_dir, save_dir) -> None:
        super().__init__(spark_session, file_dir, save_dir)
        self.file_ids = ['CNAE', 'MUNIC', 'NATJU', 'PAIS', 'QUALS', 'MOTI']

    def get_files(self) -> List[str]:
        """
        Gets the correct files of the auxiliary tables from the raw directory
        
        Parameters
        ----------    
        Returns
    	-------
        List[str]:
            Name os the auxiliary table files
        """
        all_files = os.listdir(self.file_dir)
        files = [f for n in self.file_ids for f in all_files if f.endswith(n + 'CSV')]
        return files

    def define_schema(self, file_id) -> str:
        """
        Creates schema used in the file reading.
        
        Parameters
        ----------    
        file_id : str
            Name used to identify the auxiliary table
        
        Returns
    	-------
        str:
            String specifying the schema of the DataFrame
        """
        name = AUX_NAMES[file_id]
        cols = [f'cod_{name}', f'nome_{name}']
        schema = ', '.join([c + ' STRING' for c in cols])
        return schema

    def transform_data(self, df) -> DataFrame:
        """
        Performs the necessary transformations to clean the raw data.
        
        Parameters
        ----------    
        df : pyspark.sql.dataframe.DataFrame
            Spark DataFrame of the read raw data
        
        Returns
    	-------
        pyspark.sql.dataframe.DataFrame:
            Spark DataFrame of the consolidated data
        """
        code, description = df.columns
        df_cleaned = (
            df.transform(clean_types('int', code))
            .transform(clean_types('str', description))
        )
        return df_cleaned

    def clean(self, mode = 'error', n_partitions = 32, **kwargs) -> None:
        """
        Wrapper for method execution.
        
        Parameters
        ----------    
        mode : str
            Specify the mode of writing data, if data already exist in the designed path
            * append: Append the contents of the DataFrame to the existing data
            * overwrite: Overwrite existing data
            * ignore: Silently ignores this operation
            * error or errorifexists (default): Raises an error

        n_partitions : int
            Number of data partitions in execution

        **kwargs:
            partition_col : str
                Column to partition DataFrame on writing
            key :
                Other options passed to DataFrameWriter.options
        
        Returns
    	-------
        self:
            returns an instance of the object
        """
        create_dir(self.save_dir)
        files = self.get_files()
        for file, id in zip(files, self.file_ids):
            file_path = join_path(self.file_dir, file)
            schema = self.define_schema(id)
            df = self.read_data(
                file_path, 
                'csv', 
                schema, 
                **RAW_READ_OPTS
            )
            df = self.transform_data(df)
            save_path = join_path(self.save_dir, 'df_' + AUX_NAMES[id])
            self.write_data(df, save_path, mode, n_partitions=n_partitions, encoding = "UTF-8", **kwargs)

class SimplesCleaner(Cleaner):
    """
    Class used to clean the simples table, that contains data of mostly micro and small companies that opted
    to be part of the Simples or MEI category.
    
    Parameters
    ----------
    spark_session : pyspark.sql.SparkSession
        Spark Session used to manipulate data

    file_dir : str
        Path to where the raw data is stored.

    save_dir : str
        Path to where the consolidated data should be stored

    Attributes
    -------
    spark : pyspark.sql.SparkSession
        Spark session used in data manipulation

    file_dir : str
        Path to where the raw data is stored.

    save_dir : str
        Path to where the consolidated data should be stored

    file_path : str
        Path to raw data

    save_path : str
        Path to write cleaned data

    schema : str
        Schema used to read raw data
    
    df : pyspark.sql.dataframe.DataFrame
        Spark DataFrame of raw data

    df_cleaned : pyspark.sql.dataframe.DataFrame
        Spark DataFrame of cleaned data
    """

    def __init__(self, spark_session, file_dir, save_dir) -> None:
        super().__init__(spark_session, file_dir, save_dir)

    def define_schema(self) -> None:
        """
        Creates schema used in the file reading.
        
        Parameters
        ----------    
        Returns
    	-------
        self:
            returns an instance of the object
        """
        cols = SCHEMA_COLS['simples']
        self.schema = ', '.join([c + ' STRING' for c in cols])

    def transform_data(self) -> None:
        """
        Performs the necessary transformations to clean the raw data.
        
        Parameters
        ----------
        Returns
    	-------
        self:
            returns an instance of the object
        """
        cols = self.df.columns
        date_cols = [c for c in cols if c.find("data") != -1]
        self.df_cleaned = (
            self.df
            .withColumn('cnpj', f.lpad(f.col('cnpj'), 8, '0'))
            .transform(clean_types('date', date_cols))
        )
        
    def clean(self, mode = 'error', n_partitions = 32, **kwargs) -> None:
        """
        Wrapper for method execution.
        
        Parameters
        ----------    
        mode : str
            Specify the mode of writing data, if data already exist in the designed path
            * append: Append the contents of the DataFrame to the existing data
            * overwrite: Overwrite existing data
            * ignore: Silently ignores this operation
            * error or errorifexists (default): Raises an error

        n_partitions : int
            Number of data partitions in execution
        
        **kwargs:
            partition_col : str
                Column to partition DataFrame on writing
            key :
                Other options passed to DataFrameWriter.options
        
        Returns
    	-------
        self:
            returns an instance of the object
        """
        create_dir(self.save_dir)
        self.define_schema()
        file_path = join_path(self.file_dir, '*SIMPLES*')
        self.df = self.read_data(
            file_path, 
            'csv', 
            self.schema, 
            **RAW_READ_OPTS
        )
        self.transform_data()
        save_path = join_path(self.save_dir, 'df_simples')
        self.write_data(self.df_cleaned, save_path, mode, n_partitions=n_partitions, encoding = "UTF-8", **kwargs)

class SociosCleaner(Cleaner):
    """
    Class used to clean the table containing information about partners.
    
    Parameters
    ----------
    spark_session : pyspark.sql.SparkSession
        Spark Session used to manipulate data

    file_dir : str
        Path to where the raw data is stored.

    save_dir : str
        Path to where the consolidated data should be stored

    Attributes
    -------
    spark : pyspark.sql.SparkSession
        Spark session used in data manipulation

    file_dir : str
        Path to where the raw data is stored.

    save_dir : str
        Path to where the consolidated data should be stored

    aux_paths : Dict[str]
        Dict with the path to the auxiliary tables used in cleaning

    int_dir
        Path to directory of intermediary tables

    schema : str
        Schema used to read raw data

    int_path
        Path to intermediary table written
    
    df : pyspark.sql.dataframe.DataFrame
        Spark DataFrame of raw data

    df_cleaned : pyspark.sql.dataframe.DataFrame
        Spark DataFrame of cleaned data
    """

    def __init__(self, spark_session, file_dir, save_dir) -> None:
        super().__init__(spark_session, file_dir, save_dir)
        self.aux_paths = {
            'pais': join_path(save_dir, 'df_pais'),
            'quals': join_path(save_dir, 'df_qual_socio')
        }
        self.int_dir = join_path(file_dir, 'int_tables')

    def define_schema(self) -> None:
        """
        Creates schema used in the file reading.
        
        Parameters
        ----------
        Returns
    	-------
        self:
            returns an instance of the object
        """
        cols = SCHEMA_COLS['socios']
        self.schema = ', '.join([c + ' STRING' for c in cols])

    def transform_data(self) -> None:
        """
        Performs the necessary transformations to clean the raw data.
        
        Parameters
        ---------- 
        Returns
    	-------
        self:
            returns an instance of the object
        """
        # Clean types
        cols = self.df.columns
        int_cols = [c for c in cols if c.startswith('cod')] + ['id_socio', 'faixa_etaria']
        string_cols = [c for c in cols if c.startswith('nome')]
        date_cols = [c for c in cols if c.find("data") != -1]
        df_cleaned = (
            self.df
            .transform(clean_types('int', int_cols))
            .transform(clean_types('str', string_cols))
            .transform(clean_types('date', date_cols))
        )
        # Specific Cleaning
        rename_quals = [f.col(c).alias(c + '_rep_legal') for c in self.df_qual_socio.columns]
        predicado = """
                    CASE WHEN faixa_etaria = 0 THEN "Nao se aplica"
                         WHEN faixa_etaria = 1 THEN "0 a 12 anos"
                         WHEN faixa_etaria = 2 THEN "13 a 20 anos"
                         WHEN faixa_etaria = 3 THEN "21 a 30 anos"
                         WHEN faixa_etaria = 4 THEN "31 a 40 anos"
                         WHEN faixa_etaria = 5 THEN "41 a 50 anos"
                         WHEN faixa_etaria = 6 THEN "51 a 60 anos"
                         WHEN faixa_etaria = 7 THEN "61 a 70 anos"
                         WHEN faixa_etaria = 8 THEN "71 a 80 anos"
                         WHEN faixa_etaria = 9 THEN "Mais de 80 anos"
                         ELSE null
                    END
                    """
        self.df_cleaned = (
            df_cleaned
            .withColumn('cnpj_empresa', f.lpad(f.col('cnpj_empresa'), 8, '0'))
            .withColumn('id_socio', f.when(f.col('id_socio') == 1, 'PJ')
                                     .when(f.col('id_socio') == 2, 'PF')
                                     .when(f.col('id_socio') == 3, 'Estrangeiro')
                                     .otherwise(None))
            .withColumn('faixa_etaria', f.expr(predicado))
            .join(f.broadcast(self.df_qual_socio.select(rename_quals)), 'cod_qual_socio_rep_legal', 'left')
            .join(f.broadcast(self.df_qual_socio), 'cod_qual_socio', 'left')
            .join(f.broadcast(self.df_pais), 'cod_pais', 'left')
            .select(
                'cnpj_empresa', 
                'nome_socio', 
                'cpf_cnpj_socio', 
                'id_socio', 
                'faixa_etaria',
                'cod_qual_socio', 
                'nome_qual_socio', 
                'data_entrada_sociedade', 
                'cod_pais', 
                'nome_pais',
                'num_rep_legal',
                'nome_rep_legal', 
                'cod_qual_socio_rep_legal', 
                'nome_qual_socio_rep_legal'
            )
        )

    def clean(self, mode = 'error', n_partitions = 32, **kwargs) -> None:
        """
        Wrapper for method execution.
        
        Parameters
        ----------    
        mode : str
            Specify the mode of writing data, if data already exist in the designed path
            * append: Append the contents of the DataFrame to the existing data
            * overwrite: Overwrite existing data
            * ignore: Silently ignores this operation
            * error or errorifexists (default): Raises an error

        n_partitions : int
            Number of data partitions in execution
        
        **kwargs:
            partition_col : str
                Column to partition DataFrame on writing
            key :
                Other options passed to DataFrameWriter.options
        
        Returns
    	-------
        self:
            returns an instance of the object
        """
        create_dir(self.save_dir)
        create_dir(self.int_dir)
        self.define_schema()
        file_path = join_path(self.file_dir, f'*SOCIOCSV')
        # Saves intermediary table
        int_path = join_path(self.int_dir, 'int_socios')
        self.df_int = self.read_data(
            file_path, 
            'csv', 
            self.schema, 
            **RAW_READ_OPTS
        )
        self.write_data(self.df_int, int_path, 'overwrite', n_partitions=n_partitions, encoding = "UTF-8")
        # Main process
        self.df = self.read_data(int_path, 'parquet')
        self.df_pais = self.read_data(self.aux_paths['pais'], 'parquet')
        self.df_qual_socio = self.read_data(self.aux_paths['quals'], 'parquet')
        self.transform_data()
        save_path = join_path(self.save_dir, 'df_socios')
        self.write_data(self.df_cleaned, save_path, mode, n_partitions=n_partitions, **kwargs)
        shutil.rmtree(self.int_dir)

class EmpresasCleaner(Cleaner):
    """
    Class used to clean the table containing general information about the company, such as share capital.

    Parameters
    ----------
    spark_session : pyspark.sql.SparkSession
        Spark Session used to manipulate data

    file_dir : str
        Path to where the raw data is stored.

    save_dir : str
        Path to where the consolidated data should be stored

    Attributes
    -------
    spark : pyspark.sql.SparkSession
        Spark session used in data manipulation

    file_dir : str
        Path to where the raw data is stored.

    save_dir : str
        Path to where the consolidated data should be stored

    aux_paths : Dict[str]
        Dict with the path to the auxiliary tables used in cleaning

    int_dir
        Path to directory of intermediary tables

    schema : str
        Schema used to read raw data
    
    df : pyspark.sql.dataframe.DataFrame
        Spark DataFrame of raw data

    df_cleaned : pyspark.sql.dataframe.DataFrame
        Spark DataFrame of cleaned data
    """

    def __init__(self, spark_session, file_dir, save_dir) -> None:

        super().__init__(spark_session, file_dir, save_dir)
        self.aux_paths = {
            'natju': join_path(save_dir, 'df_natju'),
            'quals': join_path(save_dir, 'df_qual_socio')
        }
        self.int_dir = join_path(file_dir, 'int_tables')

    def define_schema(self) -> None:
        """
        Creates schema used in the file reading.
        
        Parameters
        ----------
        Returns
    	-------
        self:
            returns an instance of the object
        """
        cols = SCHEMA_COLS['empresas']
        self.schema = ', '.join([c + ' STRING' for c in cols])

    def transform_data(self) -> None:
        """
        Performs the necessary transformations to clean the raw data.
        
        Parameters
        ----------    
        Returns
    	-------
        self:
            returns an instance of the object
        """
        # Clean types
        cols = self.df.columns
        int_cols = [c for c in cols if c.startswith('cod')] + ['porte']
        string_cols = ['razao_social', 'ente_fed_resp']
        df_cleaned = (
            self.df
            .transform(clean_types('int', int_cols))
            .transform(clean_types('str', string_cols))
        )
        # Limpeza Especifica
        predicado = """
                    CASE WHEN porte = 1 THEN "Nao Informado"
                         WHEN porte = 2 THEN "Micro Empresa"
                         WHEN porte = 3 THEN "Empresa de Pequeno Porte"
                         WHEN porte = 5 THEN "Demais"
                         ELSE null
                    END
                    """
        self.df_cleaned = (
            df_cleaned
            .withColumn('cnpj', f.lpad(f.col('cnpj'), 8, '0'))
            .withColumn('capital_social', f.regexp_replace(f.col('capital_social'), ',', '.').cast('float'))
            .withColumn('nome_porte', f.expr(predicado))
            .join(f.broadcast(self.df_natju), 'cod_natju', 'left')
            .join(f.broadcast(self.df_qual_socio), 'cod_qual_socio', 'left')
            .select(
                'cnpj', 
                'razao_social', 
                'capital_social', 
                'porte', 'nome_porte', 
                'ente_fed_resp',
                'cod_natju', 
                'nome_natju', 
                'cod_qual_socio', 
                'nome_qual_socio'
            )
        )

    def clean(self, mode = 'error', n_partitions = 32, **kwargs) -> None:
        """
        Wrapper for method execution.
        
        Parameters
        ----------    
        mode : str
            Specify the mode of writing data, if data already exist in the designed path
            * append: Append the contents of the DataFrame to the existing data
            * overwrite: Overwrite existing data
            * ignore: Silently ignores this operation
            * error or errorifexists (default): Raises an error

        n_partitions : int
            Number of data partitions in execution

        **kwargs:
            partition_col : str
                Column to partition DataFrame on writing
            key :
                Other options passed to DataFrameWriter.options
        
        Returns
    	-------
        self:
            returns an instance of the object
        """
        create_dir(self.save_dir)
        create_dir(self.int_dir)
        self.define_schema()
        file_path = join_path(self.file_dir, f'*EMPRECSV')
        # Saves intermediary table
        int_path = int_path = join_path(self.int_dir, 'int_empresas')
        self.df_int = self.read_data(
            file_path, 
            'csv', 
            self.schema, 
            **RAW_READ_OPTS
        )
        self.write_data(self.df_int, int_path, 'overwrite', n_partitions=n_partitions, encoding = "UTF-8")
        # Main process
        self.df = self.read_data(int_path, 'parquet')
        self.df_natju = self.read_data(self.aux_paths['natju'], 'parquet')
        self.df_qual_socio = self.read_data(self.aux_paths['quals'], 'parquet')
        self.transform_data()
        save_path = join_path(self.save_dir, 'df_empresas')
        self.write_data(self.df_cleaned, save_path, mode, n_partitions=n_partitions, **kwargs)
        shutil.rmtree(self.int_dir)

class EstabCleaner(Cleaner):
    """
    Class used to clean the biggest dataset, that contains all the information of the company at the moment of registration,
    such as main economic activity, location, contacts etc.

    Parameters
    ----------
    spark_session : pyspark.sql.SparkSession
        Spark Session used to manipulate data

    file_dir : str
        Path to where the raw data is stored.

    save_dir : str
        Path to where the consolidated data should be stored

    Attributes
    -------
    spark : pyspark.sql.SparkSession
        Spark session used in data manipulation

    file_dir : str
        Path to where the raw data is stored.

    save_dir : str
        Path to where the consolidated data should be stored

    aux_paths : Dict[str]
        Dict with the path to the auxiliary tables used in cleaning

    int_dir
        Path to directory of intermediary tables

    schema : str
        Schema used to read raw data
    
    df : pyspark.sql.dataframe.DataFrame
        Spark DataFrame of raw data

    df_cleaned : pyspark.sql.dataframe.DataFrame
        Spark DataFrame of cleaned data
    """

    def __init__(self, spark_session, file_dir, save_dir) -> None:

        super().__init__(spark_session, file_dir, save_dir)
        self.aux_paths = {
            'mun': join_path(save_dir, 'df_mun'),
            'pais': join_path(save_dir, 'df_pais')
        }
        self.int_dir = join_path(file_dir, 'int_tables')

    def define_schema(self) -> None:
        """
        Creates schema used in the file reading.
        
        Parameters
        ----------    
        Returns
    	-------
        self:
            returns an instance of the object
        """
        cols = SCHEMA_COLS['estab']
        self.schema = ', '.join([c + ' STRING' for c in cols])

    def transform_data(self) -> None:
        """
        Performs the necessary transformations to clean the raw data.
        
        Parameters
        ----------    
        Returns
    	-------
        self:
            returns an instance of the object
        """
        # Clean types
        cols = self.df.columns
        int_cols = [c for c in cols if c.startswith('cod')] + ['situacao_cadastral', 'motivo_situacao_cadastral']
        string_cols = [c for c in cols if c.startswith('nome')] + ['tipo_logradouro', 'logradouro', 'complemento', 'bairro']
        date_cols = [c for c in cols if c.find("data") != -1]
        df_cleaned =(
            self.df
            .transform(clean_types('int', int_cols))
            .transform(clean_types('str', string_cols))
            .transform(clean_types('date', date_cols))
        )
        # Limpeza Especifica
        self.df_cleaned = (
            df_cleaned
            .withColumn('cnpj', f.lpad(f.col('cnpj'), 8, '0'))
            .withColumn('cnpj_completo', f.concat(f.col('cnpj'), f.col('cnpj_ordem'), f.col('cnpj_dv')))
            .withColumn('cnae_primario', f.lpad(f.col('cnae_primario'), 7, '0'))
            .withColumn('id_matriz', f.when(f.col('id_matriz') == 1, 'Matriz')
                                      .when(f.col('id_matriz') == 2, 'Filial')
                                      .otherwise(None))
            .withColumn('uf', f.trim(f.upper(f.col('uf'))))
            .withColumn('correio_eletronico', f.regexp_replace(f.trim(f.lower(f.col('correio_eletronico'))), "'", "@"))
            .withColumn('tipo_logradouro', f.regexp_replace(f.col('tipo_logradouro'), "ç", "c"))
            .withColumn('numero', f.when(f.col('numero').isin('S/N', 'S/N B'), '').otherwise(f.col('numero')))
            .join(f.broadcast(self.df_mun), 'cod_mun')
            .join(f.broadcast(self.df_pais), 'cod_pais', 'left')
            .select(*cols, *['nome_mun', 'nome_pais'])
        )

    def clean(self, mode = 'error', n_partitions = 32, **kwargs) -> None:
        """
        Wrapper for method execution.
        
        Parameters
        ----------    
        mode : str
            Specify the mode of writing data, if data already exist in the designed path
            * append: Append the contents of the DataFrame to the existing data
            * overwrite: Overwrite existing data
            * ignore: Silently ignores this operation
            * error or errorifexists (default): Raises an error

        n_partitions : int
            Number of data partitions in execution
        
        **kwargs:
            partition_col : str
                Column to partition DataFrame on writing
            key :
                Other options passed to DataFrameWriter.options
        
        Returns
    	-------
        self:
            returns an instance of the object
        """
        create_dir(self.save_dir)
        create_dir(self.int_dir)
        self.define_schema()
        file_path = join_path(self.file_dir, f'*ESTABELE')
         # Saves intermediary table
        int_path = int_path = join_path(self.int_dir, 'int_estab')
        self.df_int = self.read_data(
            file_path, 
            'csv', 
            self.schema, 
            **RAW_READ_OPTS
        )
        self.write_data(self.df_int, int_path, 'overwrite', n_partitions=n_partitions, encoding = "UTF-8")
        # Main process
        self.df = self.read_data(int_path, 'parquet')
        self.df_pais = self.read_data(self.aux_paths['pais'], 'parquet')
        self.df_mun = self.read_data(self.aux_paths['mun'], 'parquet')
        self.transform_data()
        save_path = join_path(self.save_dir, 'df_estab')
        self.write_data(self.df_cleaned, save_path, mode, n_partitions=n_partitions, **kwargs)
        shutil.rmtree(self.int_dir)
