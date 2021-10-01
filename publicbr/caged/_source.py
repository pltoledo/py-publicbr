from ntpath import join
from pyspark.sql import SparkSession
from ._crawler import CagedCrawler
from ._consolidation import CagedCleaner
from ..base import PublicSource
from ..utils import join_path, create_dir

import logging
logging.getLogger().setLevel(logging.INFO)

class CagedSource(PublicSource):
    """
    Class used to extract Caged data.
    
    Parameters
    ----------
    spark_session : pyspark.sql.SparkSession
        Spark Session used to manipulate data

    file_dir : str
        Root directory where the data will be saved

    Attributes
    -------
    spark : pyspark.sql.SparkSession
        Spark session used in data manipulation

    raw_dir : str
        Path to the diectory used to store raw data
    
    trusted_dir : str
        Path to the diectory used to store cleaned data

    ftp_folders : Dict[str]
        Dict with the correct folders in the FTP server for each CAGED level

    cleaner : Cleaner
        Object used to consolidate table

    """

    def __init__(self, spark_session: SparkSession, file_dir: str) -> None:
        super().__init__(spark_session, file_dir)
        self.raw_dir = join_path(self.raw_dir, 'caged')
        self.trusted_dir = join_path(self.trusted_dir, 'caged')
        self.ftp_folders = {
            'estab': 'pdet/microdados/NOVO CAGED/Estabelecimentos/',
            'mov': 'pdet/microdados/NOVO CAGED/Movimentações/'
        }
        self.cleaner = CagedCleaner(spark_session, self.raw_dir, self.trusted_dir)

    def extract(self):
        """
        Extract data from public CNPJ data source.
        
        Parameters
        ----------
        Returns
    	-------
        self:
            returns an instance of the object
        """
        logging.info("Extracting data...")
        for table in ['estab', 'mov']:
            table_dir = join_path(self.raw_dir, table)
            crawler = CagedCrawler(table_dir)
            crawler.run(self.ftp_folders[table])

    def transform(self, **kwargs):
        """
        Transform raw data extracted from public CNPJ data source.
        
        Parameters
        ----------    
        **kwargs:
            mode : str
                Specify the mode of writing data, if data already exist in the designed path
                * append: Append the contents of the DataFrame to the existing data
                * overwrite: Overwrite existing data
                * ignore: Silently ignores this operation
                * error or errorifexists (default): Raises an error 
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
        logging.info("Consolidating tables...")
        self.cleaner.clean(**kwargs)

    def create(self, **kwargs):
        """
        Wrapper for method execution.
        
        Parameters
        ----------    
        **kwargs:
            mode : str
                Specify the mode of writing data, if data already exist in the designed path
                * append: Append the contents of the DataFrame to the existing data
                * overwrite: Overwrite existing data
                * ignore: Silently ignores this operation
                * error or errorifexists (default): Raises an error 
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
        create_dir(self.raw_dir)
        create_dir(self.trusted_dir)
        self.extract()
        self.transform(**kwargs)
        logging.info("Success!")

