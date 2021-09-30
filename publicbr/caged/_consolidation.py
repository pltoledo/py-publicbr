from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from ..utils import join_path, create_dir, renamer
from ..base import Cleaner
from ._variables import *

class CagedCleaner(Cleaner):

    def __init__(self, spark_session: SparkSession, file_dir: str, save_dir: str) -> None:
        super().__init__(spark_session, file_dir, save_dir)

    def define_schema(self) -> None:
        return super().define_schema()
    
    def transform_mov(self) -> None:
        
        df_cleaned = (
            self.df_mov
            .transform(renamer(RENAME_DICT['mov']))
            .withColumn('data', f.to_date('data', 'yyyyMM'))
            .withColumn('salario', f.col('salario').cast('double'))
            .withColumn('horas_contrato', f.col('horas_contrato').cast('int'))
            .withColumn('idade', f.col('idade').cast('int'))
            .withColumn('saldo_movimentacao', f.col('saldo_movimentacao').cast('int'))
        )
        for table in AUX_TABLES['mov'].values():
            aux_path = join_path(self.file_dir, 'mov', 'aux_tables', table)
            aux_table = self.read_data(aux_path, 'csv', header=True)
            key = 'cod_' + table.replace('.csv', '')
            df_cleaned = df_cleaned.join(aux_table, key, 'left').drop(key)

        final_cols = [c.replace('cod_', '') for c in RENAME_DICT['mov'].values()]
        self.df_mov_final = df_cleaned.select(*final_cols)

    def transform_estab(self) -> None:
        
        df_cleaned = (
            self.df_estab
            .transform(renamer(RENAME_DICT['estab']))
            .withColumn('data', f.to_date('data', 'yyyyMM'))
            .withColumn('admitidos', f.col('admitidos').cast('int'))
            .withColumn('desligados', f.col('desligados').cast('int'))
            .withColumn('saldo_movimentacao', f.col('saldo_movimentacao').cast('int'))
        )
        for table in AUX_TABLES['estab'].values():
            aux_path = join_path(self.file_dir, 'estab', 'aux_tables', table)
            aux_table = self.read_data(aux_path, 'csv', header=True)
            key = 'cod_' + table.replace('.csv', '')
            df_cleaned = df_cleaned.join(aux_table, key, 'left').drop(key)

        final_cols = [c.replace('cod_', '') for c in RENAME_DICT['estab'].values()]
        self.df_estab_final = df_cleaned.select(*final_cols)

    def transform_data(self) -> None:

        self.transform_mov()
        self.transform_estab()

    def clean(self, mode:str = 'error', **kwargs) -> None:

        create_dir(self.save_dir)
        self.df_mov = self.read_data(join_path(self.file_dir, 'mov', f'*.txt'), 'csv', **RAW_READ_OPTS)
        self.df_estab = self.read_data(join_path(self.file_dir, 'estab', f'*.txt'), 'csv', **RAW_READ_OPTS)
        self.transform_data()
        save_path = join_path(self.save_dir, 'movimentacoes')
        self.write_data(self.df_mov_final, save_path, mode, **kwargs)
        save_path = join_path(self.save_dir, 'estabelecimentos')
        self.write_data(self.df_estab_final, save_path, mode, **kwargs)
