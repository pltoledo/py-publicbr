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
    
    save_dir : str
        Path to the diectory used to store cleaned data

    crawler : Crawler
        Object used to extract data from the public source

    cleaners : Dict[Cleaner]
        Dict with the cleaners used to consolidate tables

    """

    def __init__(self, spark_session: SparkSession, file_dir: str) -> None:
        super().__init__(spark_session, file_dir)
        self.save_dir = join_path(self.trusted_dir, 'cnpj')
        create_dir(self.save_dir)
        self.crawler = CNPJCrawler(self.raw_dir)
        self.cleaners = {
            'Auxiliar Tables': AuxCleaner,
            'Simples': SimplesCleaner,
            'Socios': SociosCleaner,
            'Empresas': EmpresasCleaner,
            'Estabelecimentos': EstabCleaner
        }

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
        self.crawler.run()

    def transform(self):
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
        for name, obj in self.cleaners.items():
            logging.info(f'Cleaning {name}')
            cleaner = obj(self.spark, self.raw_dir, self.save_dir)
            cleaner.clean()

    def create(self, download: bool = True):
        """
        Wrapper for method execution.
        
        Parameters
        ----------    
        download : bool
            Indicator that the raw files must be downloaded

        Returns
    	-------
        self:
            returns an instance of the object
        """
        if download:
            self.extract()
        self.transform()
        logging.info("Success!")

