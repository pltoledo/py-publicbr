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
    
    save_dir : str
        Path to the diectory used to store cleaned data

    crawler : Crawler
        Object used to extract data from the public source

    cleaner : Cleaner
       Object used to consolidate table

    """

    def __init__(self, spark_session: SparkSession, file_dir: str) -> None:
        super().__init__(spark_session, file_dir)
        self.raw_dir = join_path(self.raw_dir, 'caged')
        self.save_dir = join_path(self.trusted_dir, 'caged')
        self.ftp_folders = {
            'estab': 'pdet/microdados/NOVO CAGED/Estabelecimentos/',
            'mov': 'pdet/microdados/NOVO CAGED/Movimentações/'
        }
        self.cleaner = CagedCleaner(spark_session, self.raw_dir, self.save_dir)

    def extract(self):
        """
        Extract data from public CNPJ data source.
        
        Parameters
        ----------    
        None
        
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
        None
        
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
        None

        Returns
    	-------
        self:
            returns an instance of the object
        """
        create_dir(self.raw_dir)
        create_dir(self.save_dir)
        self.extract()
        self.transform(**kwargs)
        logging.info("Success!")

