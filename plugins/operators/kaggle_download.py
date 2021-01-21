# -*- coding: utf-8 -*-

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
import kaggle
import logging

class DownloadFromKaggleOperator(BaseOperator):
    
    @apply_defaults
    def __init__(self,
                 dataset:str,
                 *args, **kwargs
                 ):
        super().__init__(*args,**kwargs)
        self.dataset = dataset
        
    def execute(self,context):
        try:
            
            kaggle.api.dataset_download_files(self.dataset,unzip=True)
        except Exception as e:
            logging.error(e)
            return False
        return True