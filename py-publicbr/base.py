from abc import ABC, abstractmethod
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from typing import Union
from .utils import join_path, create_dir

from pyspark.sql.dataframe import DataFrame

class Crawler(ABC):
    @abstractmethod
    def get_data(self) -> None:
        """
        Abstract method that is implemented in classes that inherit it
        """
        pass

class Cleaner(ABC):
    """
    Base class for all cleaners.

    Parameters
    ----------
    spark_session : pyspark.sql.SparkSession
        Spark Session used to manipulate data

    file_dir : str
        Path to where the raw data is stored.

    save_dir : str
        Path to where the consolidated data should be stored
    """

    def __init__(self, spark_session: SparkSession, file_dir: str, save_dir: str) -> None:
        self.spark = spark_session
        self.file_dir = file_dir
        self.save_dir = save_dir

    @abstractmethod
    def define_schema(self) -> None:
        """
        Abstract method that is implemented in classes that inherit it
        """
        pass

    @abstractmethod
    def transform_data(self) -> None:
        """
        Abstract method that is implemented in classes that inherit it
        """
        pass

    @abstractmethod
    def clean(self) -> None:
        """
        Abstract method that is implemented in classes that inherit it
        """
        pass

    def read_data(self, file_path: str, format: str, schema: Union[str, StructType] = None, **kwargs) -> DataFrame:
        """
        Reads DataFrame from the specified path

        Parameters
        ----------
        file_path : str
            Path to the data to be read

        format : str
            File format of data to be read

        schema : str or pyspark.sql.types.StructType
            String specifying the schema of the DataFrame
        """
        return self.spark.read.format(format).options(**kwargs).load(file_path, schema=schema)

    def write_data(self, df, save_path, **kwargs) -> None:
        """
        Writes DataFrame as parquet file in the specified destination
        
        Parameters
        ----------
        df : pyspark.sql.dataframe.DataFrame
            Spark DataFrameto be written

        save_path : str
            Path to where data should be written
        """
        df.write.options(**kwargs).save(save_path)

class PublicSource(ABC):
    """
    Base class for all public sources.

    Parameters
    ----------
    spark_session : pyspark.sql.SparkSession
        Spark Session used to manipulate data

    file_dir : str
        Path to where the raw data is stored.

    save_dir : str
        Path to where the consolidated data should be stored
    """

    def __init__(self, spark_session: SparkSession, file_dir: str) -> None:
        self.spark = spark_session
        self.raw_dir = join_path(file_dir, 'data', 'raw')
        self.trusted_dir = join_path(file_dir, 'data', 'trusted')
        create_dir(self.raw_dir)
        create_dir(self.trusted_dir)

    @abstractmethod
    def extract(self) -> None:
        """
        Abstract method that is implemented in classes that inherit it
        """
        pass

    @abstractmethod
    def create(self) -> None:
        """
        Abstract method that is implemented in classes that inherit it
        """
        pass