# -*- coding: utf-8 -*-
"""
Created on Sun Sep  4 16:50:15 2022

@author: hermann.ngayap
"""

# =============================================================================
#- This notebook is to select and combine hedge data columns `projet_id`, `cod`, 
#- `date_merchant`, `date_dementelement` and market prices
# - Pull the monthly market prices scrapped from eex and save in the table `market_prices_fr_eex`.
# - Derrived the prices curves accross our time frame 2022-2028.
# - Merge the monthly market prices with hedge_id
# - Load market prices accross 2022-2028 into the table market_prices
# - Compute mtm historical values and insert in mark_to_market table
# =============================================================================
import pandas as pd
import numpy as np
import os
from datetime import datetime
import datetime as dt
import pickle
xrange = range
import warnings
warnings.filterwarnings("ignore")

pd.options.display.float_format = '{:.3f}'.format
pd.set_option('display.max_columns', 200)
pd.set_option('display.max_rows', 200)

print("The working directory was: {0}".format(os.getcwd()))
os.chdir("C:/Users/hermann.ngayap/Boralex/Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/in/")
print("The current working directory is: {0}".format(os.getcwd()))
#
path_dir_in='C:/Users/hermann.ngayap/Boralex/Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/in/'
path_dir_temp='C:/Users/hermann.ngayap/Boralex/Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/temp/'

#Open SQL connection to fetch monthly prices data derrived from price curve
import pyodbc
import sqlalchemy
from sqlalchemy import create_engine, event
from server_credentials import server_credentials

def open_database():
    print('Connecting to SQL Server with ODBC driver')
    connection_string = 'DRIVER={SQL Server};SERVER='+server_credentials['server']+';DATABASE='+server_credentials['database']+';UID='+server_credentials['username']+';Trusted_Connection='+server_credentials['yes']
    cnxn = pyodbc.connect(connection_string)
    print('connected!')

    return cnxn

#windows authentication 
def mssql_engine(): 
    engine = create_engine('mssql+pyodbc://BLX186-SQ1PRO01/StarDust?driver=SQL+Server+Native+Client+11.0',
                           fast_executemany=True) 
    return engine


#Extract market data to DB
cnx=open_database()
cursor = cnx.cursor()
sql_to_df=pd.read_sql_query('''
                            SELECT
                                delivery_period,
                                settlement_price,
                                cotation_date,
                                DATEPART(YEAR, delivery_period) AS years,
                                DATEPART(QUARTER, delivery_period) AS quarters,
                                DATEPART(MONTH, delivery_period) AS months
                                FROM market_prices_fr_eex 
                            WHERE cotation_date='2022-09-12';
                                ''', cnx)

market_prices=sql_to_df[['delivery_period', 'settlement_price', 'years', 
                            'quarters', 'months']]


### MERGE ASSET AND DATE WITH PRICE FROM FUTURES CURVE
#To import asset and hedge data
asset_d=pd.read_excel(path_dir_in+"template_asset.xlsx")
hedge_d=pd.read_excel(path_dir_in+"template_hedge.xlsx")


#Extract only projet_id, cod, date_merchant, date_dementelement, date_debut, date_fin
asset_d_=asset_d[['projet_id', 'cod', 'date_merchant', 'date_dementelement']]
hedge_d_=hedge_d[['hedge_id', 'projet_id', 'date_debut', 'date_fin']]


#To multiply hedge df by the len of prices df
n=len(market_prices)
df_hedge = pd.DataFrame(
                np.repeat(hedge_d_.values, n, axis=0),
                columns=hedge_d_.columns,
            )

#To multiply prices df by the len of hedge df
n=len(hedge_d_)
market_prices_=pd.concat([market_prices]*n, ignore_index=True)


#To merge both data frame
frame=[df_hedge, market_prices_]
hedge_market_prices=pd.concat(frame, axis=1, ignore_index=False)

hedge_market_prices['surr_id']=0
hedge_market_prices=hedge_market_prices[['surr_id', 'hedge_id', 'projet_id', 'delivery_period', 
                                         'years', 'quarters', 'months', 'settlement_price']]

hedge_market_prices['hedge_id']=pd.to_numeric(hedge_market_prices['hedge_id'])
hedge_market_prices['delivery_period']=pd.to_datetime(hedge_market_prices.delivery_period)
hedge_market_prices['delivery_period']=hedge_market_prices['delivery_period'].dt.date

#insert as csv file
hedge_market_prices.to_excel(path_dir_in+'hedge_settlement_prices.xlsx', sheet_name='hedge_settlement_prices', index=False)
#=======================================================
#== INSERT market_prices into DB market_prices table ===
#=======================================================
table_name='market_prices'
hedge_market_prices.to_sql(table_name, con=mssql_engine(), index=False, if_exists='replace')

#=====================================================
#==== Retrieve prod, market prices and contract prices
#====      data and compute mtm/history      =========
#=====================================================
cnx=open_database()
cursor = cnx.cursor()
sql_to_df2=pd.read_sql_query('''
                                SELECT
                                --h.année, 
                                ---CAST(ROUND(SUM(h.p50), 2) AS DECIMAL(20, 3)) AS prod
                                --,CAST(ROUND(p.prix, 2) AS DECIMAL(10, 2)) AS strike_price
                                ---,CAST(ROUND(pu.settlement_price,2) AS DECIMAL(10, 2)) AS market_price 
                                CAST(ROUND(SUM(-h.p50*(cp.prix_contrat-mp.settlement_price))/1000000, 2) AS DECIMAL(20, 3)) AS mtm
                                FROM p50_p90_hedge AS h
                                INNER JOIN  contracts_prices AS cp
                                ON h.projet_id=cp.projet_id AND h.hedge_id=cp.hedge_id AND h.année=cp.année AND SUBSTRING(h.trimestre, 2, 1)=cp.trimestre AND h.mois=cp.mois
                                INNER JOIN market_prices AS mp
                                ON h.projet_id=mp.projet_id AND h.hedge_id=mp.hedge_id AND h.année=mp.years AND SUBSTRING(h.trimestre, 2, 1)=mp.quarters AND h.mois=mp.months
                                WHERE h.type_hedge = 'PPA' OR h.type_hedge IS NULL 
                                --GROUP BY h.année  
                                --ORDER BY h.année;;
                                ''', cnx)

sql_to_df2['date']=pd.to_datetime(sql_to_df['cotation_date'][0])
sql_to_df2['surr_id']=0
mtm=sql_to_df2[['surr_id', 'date', 'mtm']]
#===========================================================
#==To Insert mtm history in mark to market table in th DB ==
#===========================================================
for index, row in mtm.iterrows(): 
    cursor.execute("INSERT INTO mark_to_market (surr_id, date, mtm) values(?, ?, ?)", row.surr_id, row.date, row.mtm)
cnx.commit()
cnx.close()
