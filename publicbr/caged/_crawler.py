from ntpath import join
from ..utils import join_path, create_dir
from ..base import Crawler
from ._variables import AUX_TABLES
from ftplib import FTP
from datetime import datetime
import pandas as pd
import os
import py7zr
import shutil
import re
import openpyxl
from typing import List

class CagedCrawler(Crawler):
    """
    Class used to extract CAGED data from the public FTP server.
    
    Parameters
    ----------            
    save_dir : str
        Path to where the downloaded data should be stored. It creates a directory if it does not exists already.

    Attributes
    -------
    host : str
        Host of the public FTP server.
    
    save_dir : str
        Path to where the consolidated data should be stored
    
    ftp_folders : Dict[str]
        Dict with the correct folders in the FTP server for each CAGED level

    aux_list : List[str]
        Auxiliary list used to extract file information from the FTP server

    ftp: ftplib.FTP
        Object used to access the FTP server.
    
    layout_file: List[str]
        File containing the layout of the CAGED table columns and auxiliary tables

    available_aux: List[str]
        List containing the available auxiliary tables for a given CAGED level
    
    """

    def __init__(self, save_dir) -> None:
        self.host = 'ftp.mtps.gov.br'
        self.save_dir = save_dir
        self.ftp_folders = {
            'estab': 'pdet/microdados/NOVO CAGED/Estabelecimentos/',
            'mov': 'pdet/microdados/NOVO CAGED/Movimentações/'
        }
        self.aux_list = []
        
    def change_ftp_dir(self, folder) -> None:
        """
        Changes the current directory in the FTP server connection based on the `folder`
        
        Parameters
        ----------            
        folder : str
            Folder name in the FTP server

        Returns
    	-------
        self:
            returns an instance of the object
        """
        self.ftp.cwd(folder)
    
    def create_connection(self) -> None:
        """
        Creates a connection with the remote FTP server
        
        Parameters
        ----------            
        Returns
    	-------
        self:
            returns an instance of the object
        """
        self.ftp = FTP(self.host)
        self.ftp.login()
        
    def callback_save(self, x) -> None:
        """
        Callback function used to save file info from the FTP server
        
        Parameters
        ----------            
        x : str
            File info

        Returns
    	-------
        self:
            returns an instance of the object
        """
        self.aux_list.append(x)

    def get_layout_file(self) -> None:
        """
        Gets the layout file for a given CAGED level and its available auxiliary tables
        
        Parameters
        ----------            
        Returns
    	-------
        self:
            returns an instance of the object
        """
        self.layout_file = [i for i in self.ftp.nlst() if i.find('Layout') >= 0]
        self.get_data(self.save_dir, self.layout_file)
        layout_path = join_path(self.save_dir, self.layout_file[0])
        # Rename sheet because sometimes they are in title case
        ss=openpyxl.load_workbook(layout_path)
        self.available_aux = []
        for s in ss.sheetnames:
            ss_sheet = ss[s]
            new_name = s.lower()
            ss_sheet.title = new_name
            ss_sheet.title = new_name
            ss.save(layout_path)
            self.available_aux.append(new_name)
        
    def save_aux_tables(self) -> None:
        """
        Saves the available auxiliary tables
        
        Parameters
        ----------            
        Returns
    	-------
        self:
            returns an instance of the object
        """
        aux_path = join_path(self.save_dir, 'aux_tables')
        create_dir(aux_path)
        for sheet, file in AUX_TABLES['mov'].items():
            if sheet in self.available_aux:
                filepath = join_path(self.save_dir, self.layout_file[0])
                df = pd.read_excel(filepath, sheet)
                name = file.replace('.csv', '')
                cod = 'cod_' + name
                rename_pandas = {'Código': cod, 'Descrição': name}
                df = df.rename(rename_pandas, axis = 1)
                savepath = join_path(aux_path, file)
                df.to_csv(savepath, header=True, index=False)
        
    def get_file_info(self, ) -> List[List[str]]:
        """
        Gets information of all the files available in the current directory in the FTP connection
        
        Parameters
        ----------            
        Returns
    	-------
        List[List[str]]:
            returns a list with information on each file. Each element of the list is a 
            list that contains information on a file.
        """
        self.ftp.retrlines('LIST', callback = self.callback_save)
        file_info = [re.sub(' +', ' ', i).split(' ') for i in self.aux_list]
        self.aux_list = []
        return file_info
    
    def get_most_recent_file(self,) -> str:
        """
        Gets the most recent file from those available in the current directory in the FTP connection
        
        Parameters
        ----------            
        Returns
    	-------
        str:
            returns the name of the most recent file
        """
        file_info = self.get_file_info()
        dict_release = {datetime.strptime(k[0], '%m-%d-%y',):k[-1] for k in file_info}
        max_data = max(dict_release.keys())
        return dict_release[max_data]
    
    def get_download_files(self, ) -> List[str]:
        """
        Gets all the files to be downloaded from the FTP connection
        
        Parameters
        ----------            
        Returns
    	-------
        List[str]:
            returns a list with the file names
        """
        file_info = self.get_file_info()
        return [i[-1] for i in file_info]
    
    def get_data(self, dir, files) -> None:
        """
        Downloads data from the FTP server
        
        Parameters
        ----------            
        dir : str
            Directory where the files should be written
        
        files: List[str]
            List of files to be downloaded from the FTP server

        Returns
    	-------
        self:
            returns an instance of the object
        """
        for file in files:
            savepath = join_path(dir, file)
            with open(savepath, 'wb') as f:
                self.ftp.retrbinary("RETR " + file , f.write)
                
    def unzip_files(self, files) -> None:
        """
        Unzips files downloaded from the FTP connection
        
        Parameters
        ----------            
        files: List[str]
            List of files to be unzipped

        Returns
    	-------
        self:
            returns an instance of the object
        """
        for file in files:   
            filepath = join_path(self.zip_dir, file)
            if os.path.exists(filepath):
                with py7zr.SevenZipFile(filepath) as archive:
                    archive.extractall(self.save_dir)
            else:
                pass
    
    def run(self, caged_level) -> None:
        """
        Wrapper for method execution.
        
        Parameters
        ----------    
        caged_level : str
            Type of CAGED data. Possibles values are:
            * mov: Data of individual admissions and layoffs in the period considered
            * estab: Data of admissions and layoffs by company in the period considered
        
        Returns
    	-------
        self:
            returns an instance of the object
        """
        create_dir(self.save_dir)
        if caged_level not in ['mov', 'estab']:
            raise Exception('Invalid `caged_level`. Please pass one of "mov" or "estab".')
        self.create_connection()
        base_folder = self.ftp_folders[caged_level]
        self.change_ftp_dir(base_folder)
        self.get_layout_file()
        self.save_aux_tables()
        year_folder = self.get_most_recent_file()
        self.change_ftp_dir(year_folder)
        month_folder = self.get_most_recent_file()
        self.change_ftp_dir(month_folder)
        download_files = self.get_download_files()
        self.zip_dir = join_path(self.save_dir, 'zipped')
        create_dir(self.zip_dir)
        self.get_data(self.zip_dir, download_files)
        self.unzip_files(download_files)
        shutil.rmtree(self.zip_dir)
        