from ._crawler import CNPJCrawler
from ._consolidation import *
from ..base import PublicSource
from ..utils import join_path, create_dir

import logging
logging.getLogger().setLevel(logging.INFO)

class CNPJSource(PublicSource):
    """
    Class used to extract CNPJ data.
    
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

    crawler : Crawler
        Object used to extract data from the public source

    cleaners : Dict[Cleaner]
        Dict with the cleaners used to consolidate tables

    """

    def __init__(self, spark_session, file_dir) -> None:
        super().__init__(spark_session, file_dir)
        self.raw_dir = join_path(self.raw_dir, 'cnpj')
        self.trusted_dir = join_path(self.trusted_dir, 'cnpj')
        self.crawler = CNPJCrawler(self.raw_dir)
        self.cleaners = {
            'Auxiliary Tables': AuxCleaner,
            'Simples': SimplesCleaner,
            'Socios': SociosCleaner,
            'Empresas': EmpresasCleaner,
            'Estabelecimentos': EstabCleaner
        }

    def extract(self, overwrite):
        """
        Extract data from public CNPJ data source.
        
        Parameters
        ----------    
        overwrite : bool
            Indicator of if the already existing files should be overwritten.
        
        Returns
    	-------
        self:
            returns an instance of the object
        """
        logging.info("Extracting data...")
        self.crawler.run(overwrite)

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
        for name, obj in self.cleaners.items():
            logging.info(f'Cleaning {name}')
            cleaner = obj(self.spark, self.raw_dir, self.trusted_dir)
            cleaner.clean(**kwargs)

    def create(self, download = True, overwrite = True, **kwargs):
        """
        Wrapper for method execution.
        
        Parameters
        ----------    
        download : bool
            Indicator that the raw files must be downloaded

        overwrite : bool
            Indicator of if the already existing files should be overwritten.
        
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
        if download:
            self.extract(overwrite)
        self.transform(**kwargs)
        logging.info("Success!")

