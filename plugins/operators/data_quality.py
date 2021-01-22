#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook

class DataQualityOperator(BaseOperator):
    ui_color = '#ededed'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id:str = '',
                 test_query = '',
                 expected_result = '',
                 *args, **kwargs):
        super().__init__(*args,**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.test_query = test_query
        self.expected_result = expected_result
        
    def execute(self, context):
        
        redshift_hook = PostgresHook(
           postgres_conn_id = self.redshift_conn_id,
           params = {"redshift":True}
            )
        records = redshift_hook.get_first(self.test_query)
        if records[0] != self.expected_result:
            raise ValueError (f"""
                              {records[0]} does not equal {self.expected_result}
                              """)
        else: self.log.info("Data Quality Check Passed")

