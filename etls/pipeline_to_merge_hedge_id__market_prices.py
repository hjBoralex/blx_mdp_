# -*- coding: utf-8 -*-
"""
Created on Sun Sep  4 16:50:15 2022

@author: hermann.ngayap
"""

# =============================================================================
# This notebook is to combine hedge data as `projet_id`, `cod`, `date_merchant`, `date_dementelement` and market prices
# - Pull the monthly market prices scrapped from eex and save in the table `DIM_settlement_prices_fr_eex`.
# - Derrived the prices curves accross our time frame 2022-2028.
# - Merge the monthly market prices with hedge_id
# - Export as hedge_settlement_prices
# =============================================================================
import pandas as pd
import numpy as np
import os
from datetime import datetime
import datetime as dt
import pickle
xrange = range

pd.options.display.float_format = '{:.3f}'.format
pd.set_option('display.max_columns', 200)
pd.set_option('display.max_rows', 200)

print("The working directory was: {0}".format(os.getcwd()))
os.chdir("C:/Users/hermann.ngayap/Boralex/Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/etls/asset-hedge/")
print("The current working directory is: {0}".format(os.getcwd()))

path_dir_in='C:/Users/hermann.ngayap/Boralex/Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/in/'
path_dir_temp='C:/Users/hermann.ngayap/Boralex/Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/temp/'

#Open SQL connection to fetch monthly prices data derrived from price curve
import warnings
warnings.filterwarnings("ignore")

import pyodbc
import sqlalchemy
from sqlalchemy import create_engine
from server_credentials import server_credentials

def open_database():
    print('Connecting to SQL Server with ODBC driver')
    connection_string = 'DRIVER={SQL Server};SERVER='+server_credentials['server']+';DATABASE='+server_credentials['database']+';UID='+server_credentials['username']+';Trusted_Connection='+server_credentials['yes']
    cnxn = pyodbc.connect(connection_string)
    print('connected!')

    return cnxn

#windows authentication 
def mssql_engine(): 
    engine = create_engine('mssql+pyodbc://BLX186-SQ1PRO01/StarDust?driver=SQL+Server+Native+Client+11.0') 
    return engine


cnx=open_database()
cursor = cnx.cursor()
query_result=pd.read_sql_query('''
                                SELECT
                                       delivery_period,
                                       settlement_price,
                                       last_update,
                                       DATEPART(YEAR, delivery_period) AS years,
                                       DATEPART(QUARTER, delivery_period) AS quarters,
                                       DATEPART(MONTH, delivery_period) AS months
                                       FROM DIM_settlement_prices_fr_eex 
                                  WHERE current_v=1 AND last_update='2022-08-26';
                                ''', cnx)

market_prices=query_result[['delivery_period', 'settlement_price', 'years', 'quarters', 'months']]

cnx.close()
### MERGE ASSET AND DATE WITH PRICE FROM FUTURES CURVE
#To import asset and hedge data
asset_d=pd.read_excel(path_dir_in+"template_asset.xlsx")
hedge_d=pd.read_excel(path_dir_in+"template_hedge.xlsx")

print(asset_d.shape)
print(hedge_d.shape)

#Extract only projet_id, cod, date_merchant, date_dementelement, date_debut, date_fin
asset_d_=asset_d[['projet_id', 'cod', 'date_merchant', 'date_dementelement']]
hedge_d_=hedge_d[['hedge_id', 'projet_id', 'date_debut', 'date_fin']]

print(hedge_d_.shape)
print(market_prices.shape)


#To multiply hedge df by the len of prices df
n=len(market_prices)
df_hedge = pd.DataFrame(
                np.repeat(hedge_d_.values, n, axis=0),
                columns=hedge_d_.columns,
            )

#To multiply prices df by the len of hedge df
n=len(hedge_d_)
market_prices_=pd.concat([market_prices]*n, ignore_index=True)

print(df_hedge.shape)
print(market_prices_.shape)

#To merge both data frame
frame=[df_hedge, market_prices_]
hedge_market_prices=pd.concat(frame, axis=1, ignore_index=False)

hedge_market_prices.to_excel(path_dir_in+'hedge_settlement_prices.xlsx', sheet_name='hedge_settlement_prices', index=False)

