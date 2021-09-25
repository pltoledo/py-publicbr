from ..base import Cleaner
from pyspark.sql import SparkSession

class BigDataCleaner(Cleaner):

    def __init__(self, spark_session: SparkSession, file_dir: str, save_dir: str) -> None:
        super().__init__(spark_session, file_dir, save_dir)

    def define_schema(self) -> None:
        return super().define_schema()
    
    def transform_data(self) -> None:
        return super().transform_data()

    def clean(self) -> None:
        return super().clean()

    def write_int_table(
        self, 
        int_path,
        file_path, 
        format, 
        schema=None,
        **kwargs
    ) -> None:
        """
        Writes intermediary table in parquet format, to increase performance and reduce memory usage.
        
        Parameters
        ----------    
        None
        
        Returns
    	-------
        self:
            returns an instance of the object
        """
        df = self.spark.read.format(format).options(**kwargs).load(file_path, schema=schema)
        df.write.save(int_path)