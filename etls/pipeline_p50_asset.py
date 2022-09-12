# -*- coding: utf-8 -*-
"""
Created on Sun Sep  4 16:07:51 2022

@author: hermann.ngayap
"""

import pandas as pd
import numpy as np
from datetime import datetime
import datetime as dt
xrange = range
from pandasql import sqldf
pysqldf = lambda q: sqldf(q, globals())
import os

pd.options.display.float_format = '{:.3f}'.format
pd.set_option('display.max_columns', 200)

print("The working directory was: {0}".format(os.getcwd()))
os.chdir("C:/Users/hermann.ngayap/Boralex/Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/in/")
print("The current working directory is: {0}".format(os.getcwd()))

path_dir_in='C:/Users/hermann.ngayap/Boralex/Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/in/'
path_dir_temp='C:/Users/hermann.ngayap/Boralex/Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/temp/'


#====================================================
#=========    To change time horizon  ===============
#====================================================
nb_months=12
nb_years=(2028-2022)+1     #2028:is the end year while 2022 represents the starting year.
horizon=nb_months*nb_years #It represents the nber of months between the start date and the end date. 
date_obj="01-01-2022"      #To change the starting date of our horizon ex:To "01-01-2023" if we are in 2023
#====================================================
#================ p50 asset_vmr =====================
#====================================================

prod = pd.read_excel(path_dir_in + "template_prod.xlsx", sheet_name="prod")
prod_perc = pd.read_excel(path_dir_in + "template_prod.xlsx", sheet_name="prod_perc_id")
mean_perc = pd.read_excel(path_dir_in + "template_prod.xlsx", sheet_name="mean_perc")
df1 = pd.read_excel(path_dir_in + "template_asset.xlsx")

#To extract cod date from asset template 
df1 = pd.read_excel(path_dir_in + "template_asset.xlsx", 
                    usecols= ["asset_id", "projet_id", "cod", 
                              "date_dementelement",
                              "date_merchant",
                              "en_planif"])

df1 = df1.loc[df1["en_planif"] == "Non"]
nbr = len(df1.loc[df1["en_planif"] == "Non"]) 


#prod data 
df2 = pd.read_excel(path_dir_in + "template_prod.xlsx", sheet_name="prod")
#prod_perc data
prod_perc = pd.read_excel(path_dir_in + "template_prod.xlsx", sheet_name="prod_perc")

#To change prod df header from projet to projet_id
prod_perc = prod_perc.rename(columns=prod.set_index('projet')['projet_id'])

#To merge cod data and prod data
df3 = df1.merge(df2, on='projet_id')
df3.reset_index(drop=True, inplace=True)

#To define a dict containing prod profil  
d_prod_perc = prod_perc.to_dict()

#This code is to compute monthly p50 and p90.  
start_date = pd.to_datetime([date_obj] * nbr)                 
d = pd.DataFrame()
for i in range(0, horizon):
    df_buffer = df3
    df_buffer["date"] = start_date
    l_p50 = []
    l_p90 = []
    for elm in df_buffer.projet_id:
        try: 
            l_p50.append(d_prod_perc[elm][start_date.month[0]-1]*float(df_buffer[df_buffer.projet_id==elm]["p50"].values[0]))
        except:
            l_p50.append("NA")
        try:
            l_p90.append(d_prod_perc[elm][start_date.month[0]-1]*float(df_buffer[df_buffer.projet_id==elm]["p90"].values[0]))
        except:
            l_p90.append("NA")
    df_buffer["p50_adj"] = l_p50
    df_buffer["p90_adj"] = l_p90
    d = pd.concat([d,df_buffer],axis=0)
    start_date = start_date + pd.DateOffset(months=1)
    
#To create new columns
d["cod"] = pd.to_datetime(d["cod"])
d["date"] = pd.to_datetime(d["date"])
d["date_dementelement"] = pd.to_datetime(d["date_dementelement"])
d["date_merchant"] = pd.to_datetime(d["date_merchant"])

d['année'] = d['date'].dt.year
d['trim'] = d['date'].dt.to_period('Q').dt.strftime("Q%q-%y")
d['mois'] = d['date'].dt.month
d = d[['asset_id', 'projet_id', 'projet', 'cod', 'date', 'date_dementelement', 'date_merchant', 'année', 'trim','mois', 'p50_adj', 'p90_adj']]

#To remove p50 p90 based on date_cod
#Condition:date column is less (in total seconds) than first date for each projet_id's first date_cod value
cond=((d['date'] - d.groupby('projet_id')['cod'].transform('first')).dt.total_seconds())<0
d['p50_adj'] = np.where(cond,'', d['p50_adj'])
d['p90_adj'] = np.where(cond,'', d['p90_adj'])
#To remove p50 p90 based on date_dementelement
cond_2=((d['date'] - d.groupby('projet_id')['date_dementelement'].transform('first')).dt.total_seconds())>0
d['p50_adj'] = np.where(cond_2, '', d['p50_adj'])
d['p90_adj'] = np.where(cond_2, '', d['p90_adj'])

#To reset index
d.reset_index(inplace=True, drop=True)
d = d.assign(rw_id=[1 + i for i in xrange(len(d))])[['rw_id'] + d.columns.tolist()]
d = d[['rw_id', 'asset_id', 'projet_id', 'projet', 'date', 'année', 'trim', 'mois', 'p50_adj', 'p90_adj']]

#To export results as a data frame
d.to_excel(path_dir_temp + 'p50_p90_asset_vmr.xlsx', index=False, float_format="%.3f")

#==============================================================================
#============================= p50 asset_planif ===============================
#==============================================================================

df = pd.read_excel(path_dir_in + "template_asset.xlsx")
df = df.loc[(df["en_planif"] == "Oui") & (df["technologie"] == "éolien")]
mean_perc = pd.read_excel(path_dir_in + "template_prod.xlsx", sheet_name="mean_perc")
print(df.shape)

df1 = pd.read_excel(path_dir_in + "template_asset.xlsx")
df1 = df1.loc[df1["en_planif"] == "Oui"]

mean_perc = pd.read_excel(path_dir_in+"template_prod.xlsx", sheet_name="mean_perc")

#To fill n/a with 0.80
df1["taux_succès"].fillna(0.80, inplace=True)

#To calculate mw 
df1["puissance_installée"] = df1["mw"] * df1["taux_succès"]
df1["date_merchant"].fillna(df1["cod"] + pd.DateOffset(years=20), inplace=True) 

#To select only data with 2023 cod 
filt = df1['cod'] > dt.datetime.today().strftime('%Y-%m-%d') 
df1 = df1.loc[filt]

#df1["puissance_installée"] = df1["mw"] * df1["taux_succès"]

df1.loc[df1['technologie'] == 'éolien', 'p50'] = df1["puissance_installée"] * 8760 * 0.25#calculer le p_50 projet éolien en planification
df1.loc[df1['technologie'] == 'éolien', 'p90'] = df1["puissance_installée"] * 8760 * 0.20#calculer le p_90 projet éolien en planification

df1.loc[df1['technologie'] == 'solaire', 'p50'] = df1["puissance_installée"] * 8760 * 0.15#calculer le p_50 projet solaire en planification
df1.loc[df1['technologie'] == 'solaire', 'p90'] = df1["puissance_installée"] * 8760 * 0.13#calculer le p_90 projet solaire en planification


prod_planif = df1.loc[:,["rw_id", "asset_id", "projet_id", "projet", "technologie", "cod","p50", "p90"]]

prod_planif_solaire = prod_planif.loc[prod_planif['technologie'] == "solaire"]
prod_planif_eolien = prod_planif.loc[prod_planif['technologie'] == "éolien"]

prod_planif_solaire.reset_index(drop = True, inplace=True)
prod_planif_eolien.reset_index(drop = True, inplace=True)

nbr_sol = len(prod_planif_solaire) 
nbr_eol = len(prod_planif_eolien) 

prod_planif_solaire.to_excel(path_dir_temp+"prod_planif_solaire.xlsx", index = False, float_format="%.3f")
prod_planif_eolien.to_excel(path_dir_temp+"prod_planif_eolien.xlsx", index = False, float_format="%.3f")

mean_perc_sol = mean_perc.iloc[:,[0, 1]]
mean_perc_eol = mean_perc.iloc[:,[0,-1]]

#create a dataframe with date from 2022 to 2028 solaire
start_date = pd.to_datetime([date_obj] * nbr_sol)
d1 = pd.DataFrame()
for i in range(0, horizon):
    df_buffer = prod_planif_solaire.copy() 
    df_buffer["date"] = start_date
    d1 = pd.concat([d1, df_buffer],axis=0)
    start_date = start_date + pd.DateOffset(months=1)
    
#reset index    
d1.reset_index(drop=True, inplace=True)
#create a mth column containing number of month
mean_perc_sol = mean_perc_sol.assign(mth=[1 + i for i in xrange(len(mean_perc_sol))])[['mth'] + mean_perc_sol.columns.tolist()]

#To calculate p50 and p90 solar
s = mean_perc_sol.set_index('mth')['m_pct_solaire']
pct = pd.to_datetime(d1['date']).dt.month.map(s)
d1['p50_adj'] = d1['p50'] * pct
d1['p90_adj'] = d1['p90'] * pct

#create a dataframe with date from 2022 to 2028 éolien
start_date = pd.to_datetime([date_obj] * nbr_eol)
d2 = pd.DataFrame()
for i in range(0, horizon):
    df_buffer = prod_planif_eolien.copy() 
    df_buffer["date"] = start_date
    d2 = pd.concat([d2, df_buffer],axis=0)
    start_date = start_date + pd.DateOffset(months=1)

#reset index    
d2.reset_index(drop=True, inplace=True)
#create a mth column containing number of month
mean_perc_eol = mean_perc_eol.assign(mth=[1 + i for i in xrange(len(mean_perc_eol))])[['mth'] + mean_perc_eol.columns.tolist()]

#To calculate p50 and p90 eolien
s2 = mean_perc_eol.set_index('mth')['m_pct_eolien']
pct = pd.to_datetime(d2['date']).dt.month.map(s2)
d2['p50_adj'] = d2['p50'] * pct
d2['p90_adj'] = d2['p90'] * pct

#To create new columns année et mois
d1["cod"] = pd.to_datetime(d1["cod"])
d1["date"] = pd.to_datetime(d1["date"])
d1['année'] = d1['date'].dt.year
d1['trim'] = d1['date'].dt.to_period('Q').dt.strftime("Q%q-%y")
d1['mois'] = d1['date'].dt.month
d1 = d1[['asset_id','projet_id', 'projet', 'cod', 'date', 'année', 'trim', 'mois', 'p50_adj', 'p90_adj']]

#To remove p50 p90 based on date_cod
#Condition:date column is less (in total seconds) than first date for each projet_id's first date_cod value
cond=((d1['date'] - d1.groupby('projet_id')['cod'].transform('first')).dt.total_seconds())<0
d1['p50_adj'] = np.where(cond,'', d1['p50_adj'])
d1['p90_adj'] = np.where(cond,'', d1['p90_adj'])
#To remove p50 p90 based on date_dementelement
#cond_2=((d1['date'] - d1.groupby('projet_id')['date_dementelement'].transform('first')).dt.total_seconds())>0
#d1['p50_adj'] = np.where(cond_2, '', d1['p50_adj'])
#d1['p90_adj'] = np.where(cond_2, '', d1['p90_adj'])

#To reset index
d1.reset_index(inplace=True, drop=True)
d1 = d1.assign(rw_id=[1 + i for i in xrange(len(d1))])[['rw_id'] + d1.columns.tolist()]
d1 = d1[['rw_id', 'asset_id', 'projet_id', 'projet', 'date', 'année', 'trim', 'mois', 'p50_adj', 'p90_adj']]


#To create new columns année et mois
d2["cod"] = pd.to_datetime(d2["cod"])
d2["date"] = pd.to_datetime(d2["date"])
d2['année'] = d2['date'].dt.year
d2['trim'] = d2['date'].dt.to_period('Q').dt.strftime("Q%q-%y")
d2['mois'] = d2['date'].dt.month
d2 = d2[['asset_id','projet_id', 'projet', 'cod', 'date', 'année', 'trim', 'mois', 'p50_adj', 'p90_adj']]

#To remove p50 p90 based on date_cod
#Condition:date column is less (in total seconds) than first date for each projet_id's first date_cod value
cond=((d2['date'] - d2.groupby('projet_id')['cod'].transform('first')).dt.total_seconds())<0
d2['p50_adj'] = np.where(cond,'', d2['p50_adj'])
d2['p90_adj'] = np.where(cond,'', d2['p90_adj'])
#To remove p50 p90 based on date_dementelement
#cond_2=((d2['date'] - d2.groupby('projet_id')['date_dementelement'].transform('first')).dt.total_seconds())>0
#d2['p50_adj'] = np.where(cond_2, '', d2['p50_adj'])
#d2['p90_adj'] = np.where(cond_2, '', d2['p90_adj'])

#To reset index
d2.reset_index(inplace=True, drop=True)
d2 = d2.assign(rw_id=[1 + i for i in xrange(len(d2))])[['rw_id'] + d2.columns.tolist()]
d2 = d2[['rw_id', 'asset_id', 'projet_id', 'projet', 'date', 'année', 'trim', 'mois', 'p50_adj', 'p90_adj']]

#To export as excel files
d1.to_excel(path_dir_temp+'p50_p90_planif_solaire.xlsx', index=False, float_format="%.3f")
d2.to_excel(path_dir_temp+'p50_p90_planif_eolien.xlsx', index=False, float_format="%.3f")

#=======================================================================================
#====================== To merge p50_asset_vmr and p50_asset_palnif ====================
#=======================================================================================

df_1=pd.read_excel(path_dir_temp + 'p50_p90_asset_vmr.xlsx')
df_2=pd.read_excel(path_dir_temp + 'p50_p90_planif_solaire.xlsx')
df_3=pd.read_excel(path_dir_temp + 'p50_p90_planif_eolien.xlsx')

frames=[df_1, df_2, df_3]
p50_p90_vmr_planif =pd.concat(frames, axis= 0, ignore_index=True)
p50_p90_vmr_planif.drop("rw_id", axis=1, inplace=True)
p50_p90_vmr_planif = p50_p90_vmr_planif.assign(rw_id=[1 + i for i in xrange(len(p50_p90_vmr_planif))])[['rw_id'] + p50_p90_vmr_planif.columns.tolist()]

#To export as excel file
p50_p90_vmr_planif.to_excel(path_dir_in + 'p50_p90_asset_vmr_planif.xlsx', index=False, float_format="%.3f")



