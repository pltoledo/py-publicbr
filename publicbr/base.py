from abc import ABC, abstractmethod
from .utils import join_path

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

    def __init__(self, spark_session, file_dir, save_dir) -> None:
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

    def read_data(self, file_path, format, schema = None, **kwargs) -> DataFrame:
        """
        Reads DataFrame from the specified path

        Parameters
        ----------
        file_path : str
            Path to the data to be read

        format : str
            File format of data being read

        schema : str | pyspark.sql.types.StructType
            String specifying the schema of the DataFrame
        
        **kwargs: 
            Other options passed to DataFrameReader.options

        """
        return self.spark.read.format(format).options(**kwargs).load(file_path, schema=schema)

    def write_data(
        self, 
        df, 
        save_path,
        mode,
        format = None,
        partition_col = None,
        n_partitions = None,
        **kwargs
    ) -> None:
        """
        Writes DataFrame as parquet file in the specified destination
        
        Parameters
        ----------
        df : pyspark.sql.dataframe.DataFrame
            Spark DataFrameto be written

        save_path : str
            Path to where data should be written

        mode : str
            Specify the mode of writing data, if data already exist in the designed path
            * append: Append the contents of the DataFrame to the existing data
            * overwrite: Overwrite existing data
            * ignore: Silently ignores this operation
            * error or errorifexists (default): Raises an error

        format : str
            File format of data being written
        
        n_partitions : int
            Number of DataFrame partitions
        
        partition_col : str
                Column to partition DataFrame on writing

        **kwargs:
            Other options passed to DataFrameWriter.options

        """
        if n_partitions:
            df_partitions = df.rdd.getNumPartitions()
            if df_partitions >= n_partitions:
                df = df.coalesce(n_partitions)
            else:
                df = df.repartition(n_partitions)
        writer = df.write.options(**kwargs).mode(mode)
        if partition_col:
            writer = writer.partitionBy(partition_col)
        if format:
            writer = writer.format(format)
        writer.save(save_path)

class PublicSource(ABC):
    """
    Base class for all public sources.

    Parameters
    ----------
    spark_session : pyspark.sql.SparkSession
        Spark Session used to manipulate data

    file_dir : str
        Path to where data should be stored.

    """

    def __init__(self, spark_session, file_dir) -> None:
        self.spark = spark_session
        self.raw_dir = join_path(file_dir, 'data', 'raw')
        self.trusted_dir = join_path(file_dir, 'data', 'trusted')

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