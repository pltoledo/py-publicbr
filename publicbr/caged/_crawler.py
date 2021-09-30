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
    
    def __init__(self, save_dir):
        self.host = 'ftp.mtps.gov.br'
        self.save_dir = save_dir
        self.aux_list = []
        
    def change_ftp_dir(self, folder):
        self.ftp.cwd(folder)
    
    def create_connection(self):
        self.ftp = FTP(self.host)
        self.ftp.login()
        
    def callback_save(self, x):
        self.aux_list.append(x)

    def get_layout_file(self):

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
        
    def save_aux_tables(self):

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
        
    def get_file_info(self, ):
        self.ftp.retrlines('LIST', callback = self.callback_save)
        file_info = [re.sub(' +', ' ', i).split(' ') for i in self.aux_list]
        self.aux_list = []
        return file_info
    
    def get_most_recent_file(self,):
        file_info = self.get_file_info()
        dict_release = {datetime.strptime(k[0], '%m-%d-%y',):k[-1] for k in file_info}
        max_data = max(dict_release.keys())
        return dict_release[max_data]
    
    def get_download_files(self, ):
        file_info = self.get_file_info()
        return [i[-1] for i in file_info]
    
    def get_data(self, dir, files):
        
        for file in files:
            savepath = join_path(dir, file)
            with open(savepath, 'wb') as f:
                self.ftp.retrbinary("RETR " + file , f.write)
                
    def unzip_files(self, files):
        
        for file in files:   
            filepath = join_path(self.zip_dir, file)
            if os.path.exists(filepath):
                with py7zr.SevenZipFile(filepath) as archive:
                    archive.extractall(self.save_dir)
            else:
                pass
    
    def run(self, base_folder):
        """
        Extract data from public CNPJ data source.
        
        Parameters
        ----------    
        base_folder : str
            Folder of FTP server that contains desired data.
        
        Returns
    	-------
        self:
            returns an instance of the object
        """
        create_dir(self.save_dir)
        self.create_connection()
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
        