from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from ..utils import clean_types, join_path, create_dir, renamer
from ..base import Cleaner
from ._variables import *

class CagedCleaner(Cleaner):
    """
    Class used to clean the Movimentações and Estabelecimentos CAGED tables, which contains
    individual and per establishment data about layoffs and admissions, respectively.
    
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
    
    caged_levels : List[str]
        The strings used to identify the CAGED tables
    
    file_name : Dict[str]
        The final file names of CAGED tables after consolidation

    """
    def __init__(self, spark_session, file_dir, save_dir) -> None:
        super().__init__(spark_session, file_dir, save_dir)
        self.caged_levels = ['mov', 'estab']
        self.file_name = {
            'mov': 'movimentacoes',
            'estab': 'estabelecimentos'
        }

    def define_schema(self) -> None:
        return super().define_schema()

    def join_aux_tables(self, df, caged_level) -> DataFrame:
        """
        Joins the base CAGED DataFrames and the auxiliary tables extract from the Layout document.
        
        Parameters
        ----------    
        df : pyspark.sql.dataframe.DataFrame
            Spark DataFrame with data to be joined
        
        caged_level : str
            Type of CAGED data. Possibles values are:
            * mov: Data of individual admissions and layoffs in the period considered
            * estab: Data of admissions and layoffs by company in the period considered 
        
        Returns
    	-------
        pyspark.sql.dataframe.Dataframe :
            returns a DataFrame joined with auxiliary tables
        """
        for table in AUX_TABLES[caged_level].values():
            aux_path = join_path(self.file_dir, caged_level, 'aux_tables', table)
            name = table.replace('.csv', '')
            aux_table = (
                self.read_data(aux_path, 'csv', header=True)
                .transform(clean_types('str', [name]))
            )
            key = 'cod_' + name
            df = df.join(aux_table.hint('broadcast'), key, 'left').drop(key)
        return df
    
    def transform_data(self, df, caged_level) -> DataFrame:
        """
        Performs the necessary transformations to clean the raw data.
        
        Parameters
        ----------    
        df : pyspark.sql.dataframe.DataFrame
            Spark DataFrame with data to be cleaned
        
        caged_level : str
            Type of CAGED data. Possibles values are:
            * mov: Data of individual admissions and layoffs in the period considered
            * estab: Data of admissions and layoffs by company in the period considered 
        
        Returns
    	-------
        pyspark.sql.dataframe.Dataframe :
            returns the final cleaned DataFrame
        """
        if caged_level == 'mov':
            int_cols = ['horas_contrato', 'idade', 'saldo_movimentacao']
            df = df.withColumn('salário', f.col('salário').cast('double'))
        elif caged_level == 'estab': 
            int_cols = ['admitidos', 'desligados', 'saldo_movimentacao']
        df_cleaned = (
            df.transform(renamer(RENAME_DICT[caged_level]))
            .withColumn('data', f.to_date('data', 'yyyyMM'))
            .transform(clean_types('int', int_cols))
        )
        df_cleaned = (
            self.join_aux_tables(df_cleaned, caged_level)
            .withColumn('municipio', f.split(f.col("municipio"), '-')[1])
            .withColumn('municipio', f.initcap(f.coalesce(f.col("municipio"), f.lit('Nao Identificado'))))
        )
        final_cols = [c.replace('cod_', '') for c in RENAME_DICT[caged_level].values()]
        return df_cleaned.select(*final_cols)

    def clean(self, mode:str = 'error', **kwargs) -> None:
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
        
        **kwargs:
            n_partitions : int
                Number of DataFrame partitions
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
        for level in self.caged_levels:
            df = self.read_data(join_path(self.file_dir, level, f'*.txt'), 'csv', **RAW_READ_OPTS)
            df_final = self.transform_data(df, level)
            save_path = join_path(self.save_dir, self.file_name[level])
            self.write_data(df_final, save_path, mode, **kwargs)
