# -*- coding: utf-8 -*-
"""
Created on Fri Jul  8 11:54:42 2022

@author: hermann.ngayap
"""
from dash import dcc, html
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.graph_objs as go
from functions import make_dbc_table
from sql_queries_vm import*

cols = ['Years', 'Prod-Merchant', 'HCR', 'Prod-Total', 'HCR']

frames=[query_results_19, query_results_22.iloc[:,1:], query_results_10.iloc[:,1],query_results_7.iloc[:,1]]
df=pd.concat(frames, axis=1, ignore_index=False)

table_header = [
     html.Thead(html.Tr([html.Th(i) for i in cols]))] 

table_body = [
         html.Tbody(
             [
                 html.Tr([html.Td(df.iloc[i][col]) for col in df.columns]) 
                 for i in range(len(df))
                 ]
             )
         ]

prod_merchant_tbl=html.Div(
    children=[
     dbc.Table(
         table_header + table_body,
         bordered=False,
         responsive=True,
         hover=True,
         striped=True,
         #style=table,
         className="table")
     ]
 )



