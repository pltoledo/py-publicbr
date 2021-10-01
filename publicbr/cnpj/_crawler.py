from bs4 import BeautifulSoup
from tqdm import tqdm
from ..utils import join_path, create_dir
from ..base import Crawler
import requests
import os
import zipfile
from contextlib import closing

class CNPJCrawler(Crawler):
    """
    Class used to extract CNPJ data from the public source.
    
    Parameters
    ----------            
    save_dir : str
        Path to where the downloaded data should be stored. It creates a directory if it does not exists already.

    Attributes
    -------
    base_url : str
        Url containing all the files to be downloaded
    
    save_dir : str
        Path to where the consolidated data should be stored

    files : str
        Name of the files to be downloaded
    
    """

    def __init__(self, save_dir) -> None:

        self.base_url = 'http://200.152.38.155/CNPJ/'
        self.save_dir = save_dir
        r = requests.get(self.base_url)
        soup = BeautifulSoup(r.content, 'html.parser')
        self.files = [i['href'] for i in soup.select('a', href=True) 
                      if i['href'].endswith('.zip')]

    def download_url(self, url, save_path) -> None:
        """
        Function that downlads data from the URL.
        
        Parameters
        ----------            
        url : str
            Full url of the file, created by joining the `base_url` and a file name.
        
        save_path : str
            Path of the destination file

        Returns
    	-------
        self:
            returns an instance of the object
        """
        #with requests.get(url, stream=True) as r:    
        #    with open(save_path, 'wb') as fd:
        #        shutil.copyfileobj(r.raw, fd)
        
        with open(save_path, 'wb') as f, closing(requests.get(url, stream=True)) as res:
            for chunk in res.iter_content(chunk_size=1024):
                f.write(chunk)
    
    def get_data(self, overwrite) -> None:
        """
        Wrapper to download each file in `files`.
        
        Parameters
        ----------            
        overwrite : bool
            Indicator of if the already existing files should be overwritten.

        Returns
    	-------
        self:
            returns an instance of the object
        """
        for file in tqdm(self.files):
            url = self.base_url + file
            save_path = join_path(self.save_dir, file)
            path_exist = lambda x: not os.path.exists(x)
            if path_exist(save_path) and path_exist(save_path.replace('.zip', '')):
                self.download_url(url, save_path)
            elif overwrite:
                self.download_url(url, save_path)
            else:
                continue

    def unzip(self) -> None:
        """
        Extract data from the downloaded zipped files.
        
        Parameters
        ----------            
        Returns
    	-------
        self:
            returns an instance of the object
        """
        for file in self.files:   
            filepath = join_path(self.save_dir, file)
            newpath = join_path(self.save_dir, file.replace('.zip', ''))
            if os.path.exists(filepath):
                zip_ref = zipfile.ZipFile(filepath) # create zipfile object
                zip_ref.extractall(newpath) # extract file to dir
                zip_ref.close() # close file
                os.remove(filepath) # delete zipped file
            else:
                pass

    def run(self, overwrite = True) -> None:
        """
        Wrapper for method execution.
        
        Parameters
        ----------    
        overwrite : bool
            Indicator of if the already existing files should be overwritten.
        
        Returns
    	-------
        self:
            returns an instance of the object
        """
        create_dir(self.save_dir)
        self.get_data(overwrite)
        self.unzip()
