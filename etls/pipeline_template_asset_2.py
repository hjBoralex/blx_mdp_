# -*- coding: utf-8 -*-
"""
Created on Tue Sep 13 13:56:06 2022

@author: hermann.ngayap
"""
import pandas as pd
import numpy as np
from datetime import datetime
import datetime as dt
xrange = range
from pandasql import sqldf
pysqldf = lambda q: sqldf(q, globals())
pd.options.display.float_format = '{:.3f}'.format
pd.set_option('display.max_columns', 200)

import os
print("The working directory was: {0}".format(os.getcwd()))
os.chdir("C:/hjBoralex/etl/gitcwd/blx_mdp_front-end/etls/")
print("The current working directory is: {0}".format(os.getcwd()))

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

#==============================================================================
#==========      Load template asset into asset table      ====================
#==========                 SCD type 1                     ====================
#==============================================================================

#==============================================================================
#=============== To join annual production to asset template   ================
#==============================================================================
asset_vmr_planif=pd.read_excel(path_dir_temp+"asset_vmr_planif.xlsx")
df_prod=pd.read_excel(path_dir_in+"template_prod.xlsx")
prod_id=df_prod.iloc[:,np.r_[0, 2, 3]]
asset_template=pd.merge(asset_vmr_planif, prod_id, how="left", on=['projet_id'])

#Fix data type
asset_template['rw_id']=asset_template['rw_id'].astype(pd.Int64Dtype())
asset_template['asset_id']=asset_template['asset_id'].astype(pd.Int64Dtype())
asset_template['cod']=pd.to_datetime(asset_template.cod)
asset_template['cod']=asset_template['cod'].dt.date

asset_template['date_merchant']=pd.to_datetime(asset_template.date_merchant)
asset_template['date_merchant']=asset_template['date_merchant'].dt.date

asset_template['date_dementelement']=pd.to_datetime(asset_template.date_dementelement)
asset_template['date_dementelement']=asset_template['date_dementelement'].dt.date

asset_template['date_msi']=pd.to_datetime(asset_template.date_msi)
asset_template['date_msi']=asset_template['date_msi'].dt.date
#Export as excel file
asset_template.to_excel(path_dir_in+"template_asset.xlsx", index=False)

#Insert data in DB in asset table
table_name='asset'
asset_template.to_sql(table_name, con=mssql_engine(), index=False, if_exists='replace')
#==============================================================================
#==========   Update asset table with data from template asset ================
#==========                 SCD type 1                     ====================
#==============================================================================
#import source data 
flux_source=pd.read_excel(path_dir_in+"template_asset.xlsx")
#flux_source=asset_template.copy()

#Fix data type
flux_source['rw_id']=flux_source['rw_id'].astype(pd.Int64Dtype())
flux_source['asset_id']=flux_source['asset_id'].astype(pd.Int64Dtype())
flux_source['cod']=pd.to_datetime(flux_source.cod)
flux_source['cod']=flux_source['cod'].dt.date

flux_source['date_merchant']=pd.to_datetime(flux_source.date_merchant)
flux_source['date_merchant']=flux_source['date_merchant'].dt.date

flux_source['date_dementelement']=pd.to_datetime(flux_source.date_dementelement)
flux_source['date_dementelement']=flux_source['date_dementelement'].dt.date

flux_source['date_msi']=pd.to_datetime(flux_source.date_msi)
flux_source['date_msi']=flux_source['date_msi'].dt.date

#rename columns
flux_source.rename(columns={'rw_id':'rw_id_src', 'asset_id':'asset_id_src', 'projet_id':'projet_id_src', 
                          'projet':'projet_src', 'technologie':'technologie_src', 'cod':'cod_src', 'mw':"mw_src", 
                          'taux_succès':'taux_succès_src', 'puissance_installée':'puissance_installée_src', 'eoh':'eoh_src',
                          'date_merchant':'date_merchant_src', 'date_dementelement':'date_dementelement_src', 'repowering':'repowering_src', 
                          'date_msi':'date_msi_src', 'en_planif':'en_planif_src', 'p50':'p50_src', 'p90':'p90_src'},  inplace=True)

#Load target df
cnx=open_database()
cursor = cnx.cursor()
flux_target=pd.read_sql_query('''
                            SELECT *
                            FROM asset;
                            ''', cnx)
#Rename target df
flux_target.rename(columns={'rw_id':'rw_id_tgt', 'asset_id':'asset_id_tgt', 'projet_id':'projet_id_tgt', 
                          'projet':'projet_tgt', 'technologie':'technologie_tgt', 'cod':'cod_tgt', 'mw':"mw_tgt", 
                          'taux_succès':'taux_succès_tgt', 'puissance_installée':'puissance_installée_tgt', 'eoh':'eoh_tgt',
                          'date_merchant':'date_merchant_tgt', 'date_dementelement':'date_dementelement_tgt', 'repowering':'repowering_tgt', 
                          'date_msi':'date_msi_tgt', 'en_planif':'en_planif_tgt', 'p50':'p50_tgt', 'p90':'p90_tgt'},  inplace=True)

#Merge source anr target df
flux_join=pd.merge(flux_source, flux_target, left_on='projet_id_src', 
                   right_on='projet_id_tgt', how='left')

#To create an Insert flag
flux_join['ins_flag']=flux_join.apply(lambda x:'I' if (pd.isnull(x[18]) and pd.isnull(x[19])) else 'N', axis=1)

#To create an Update Flag
flux_join['upd_flag']=flux_join.apply(lambda x:'U' if (
    (x[1]==x[18] and x[2]==x[19]) and ((x[3]!=x[20]) or (x[4]!=x[21]) or (str(x[5])!=str(x[22])) 
                                       or (x[6]!=x[23]) or (x[7]!=x[24]) or (x[8]!=x[25]) 
                                       or (x[9]!=x[26]) or (str(x[10])!=str(x[27]))
                                       or (str(x[11])!=str(x[28])) or (x[12]!=x[29]) or (str(x[13])!=str(x[30]))
                                       or (x[14]!=x[31]) or (x[15]!=x[32]) or (x[16]!=x[33])
                                       )                                     
    ) else 'N', axis=1)


#Prepare and insert updated df
ins_rec=flux_join.loc[flux_join['ins_flag']=='I']
ins_upd=ins_rec[['rw_id_src', 'asset_id_src', 'projet_id_src', 'projet_src',
                 'technologie_src', 'cod_src', 'mw_src', 'taux_succès_src',
                 'puissance_installée_src', 'eoh_src', 'date_merchant_src',
                 'date_dementelement_src', 'repowering_src', 'date_msi_src',
                 'en_planif_src', 'p50_src', 'p90_src']]
#To rename columns
ins_upd.rename(columns={'rw_id_src':'rw_id', 'asset_id_src':'asset_id', 'projet_id_src':'projet_id', 
                          'projet_src':'projet', 'technologie_src':'technologie', 'cod_src':'cod', 
                          'mw_src':'mw', 'taux_succès_src':'taux_succès', 'puissance_installée_src':'puissance_installée',
                          'eoh_src':'eoh', 'date_merchant_src':'date_merchant', 'date_dementelement_src':'date_dementelement', 
                          'repowering_src':'repowering', 'date_msi_src':'date_msi', 'en_planif_src':'en_planif', 
                          'p50_src':'p50', 'p90_src':'p90'
                          }, inplace=True)


#Prepare df for updatd records
upd_df=flux_join.loc[flux_join['upd_flag']=='U']
upd_rec=upd_df[['rw_id_src', 'asset_id_src', 'projet_id_src', 'projet_src',
                 'technologie_src', 'cod_src', 'mw_src', 'taux_succès_src',
                 'puissance_installée_src', 'eoh_src', 'date_merchant_src',
                 'date_dementelement_src', 'repowering_src', 'date_msi_src',
                 'en_planif_src', 'p50_src', 'p90_src']]
#To rename columns
upd_rec.rename(columns={'rw_id_src':'rw_id', 'asset_id_src':'asset_id', 'projet_id_src':'projet_id', 
                          'projet_src':'projet', 'technologie_src':'technologie', 'cod_src':'cod', 
                          'mw_src':'mw', 'taux_succès_src':'taux_succès', 'puissance_installée_src':'puissance_installée',
                          'eoh_src':'eoh', 'date_merchant_src':'date_merchant', 'date_dementelement_src':'date_dementelement', 
                          'repowering_src':'repowering', 'date_msi_src':'date_msi', 'en_planif_src':'en_planif', 
                          'p50_src':'p50', 'p90_src':'p90'
                          }, inplace=True)

#Insert new record to target table
table_name='asset'
ins_upd.to_sql(table_name, con=mssql_engine(), index=False, if_exists='append')

#update new records into target table
metadata=sqlalchemy.MetaData(bind=mssql_engine())
datatable=sqlalchemy.Table('asset', metadata, autoload=True)

#Loop over the target df and update to update records
cnx=open_database()
cursor = cnx.cursor()
for ind, row in upd_rec.iterrows():
    upd=sqlalchemy.sql.update(datatable).values({'rw_id':row.rw_id, 'projet':row.projet, 'technologie':row.technologie,
                                                 'cod':row.cod, 'mw':row.mw, 'taux_succès':row.taux_succès, 'puissance_installée':row.puissance_installée,
                                                 'eoh':row.eoh, 'date_merchant':row.date_merchant, 'date_dementelement':row.date_dementelement,
                                                 'repowering':row.repowering, 'date_msi':row.date_msi, 'en_planif':row.en_planif}) \
        .where(sqlalchemy.and_(datatable.c.projet_id==row.projet_id and datatable.c.asset_id==row.asset_id))
    cursor.execute(upd, )
cnx.flush()
cnx.commit()
cnx.close()

