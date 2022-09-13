# -*- coding: utf-8 -*-
"""
Created on Sun Sep  4 16:08:19 2022

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
os.chdir("C:/Users/hermann.ngayap/Boralex/Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/etls/asset-hedge/")
print("The current working directory is: {0}".format(os.getcwd()))

path_dir_in='C:/Users/hermann.ngayap/Boralex/Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/in/'
path_dir_temp='C:/Users/hermann.ngayap/Boralex/Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/temp/'

prod = pd.read_excel(path_dir_in + "template_prod.xlsx", sheet_name="prod")
prod_perc = pd.read_excel(path_dir_in + "template_prod.xlsx", sheet_name="prod_perc")
mean_perc = pd.read_excel(path_dir_in + "template_prod.xlsx", sheet_name="mean_perc")

#To import hedge data and split into parcs in planif and in production
df_hedge = pd.read_excel(path_dir_in + "template_hedge.xlsx")
#Hedge data from parcs in production
df_hedge_vmr = df_hedge.loc[df_hedge["en_planif"] == "Non"]
#Hedge data from parcs in planification 
df_hedge_planif = df_hedge.loc[df_hedge["en_planif"] == "Oui"]
#To import template_hedge data
df1 = pd.read_excel(path_dir_in + "template_hedge.xlsx")

#====================================================
#=========    To change time horizon  ===============
#====================================================

nb_months=12
nb_years=(2028-2022)+1     #2028:is the end year while 2022 represents the starting year.
horizon=nb_months*nb_years #It represents the nber of months between the start date and the end date. 
date_obj="01-01-2022"      #To change the starting date of our horizon ex:To "01-01-2023" if we are in 2023

#=====================================================
#=============  To compete hedge planif ==============
#=====================================================
#8760=24*365 Number oh 
#To calculate p50 p90 in mw/h of assets in palnification. equals to mw*8760*factor  
df1.loc[(df1['technologie'] == 'éolien') & (df1['en_planif'] == 'Oui'), 'p50'] = df1["puissance_installée"] * 8760 * 0.25#calculer le p_50 projet éolien en planification
df1.loc[(df1['technologie'] == 'éolien') & (df1['en_planif'] == 'Oui'), 'p90'] = df1["puissance_installée"] * 8760 * 0.20#calculer le p_90 projet éolien en planification

df1.loc[(df1['technologie'] == 'solaire') & (df1['en_planif'] == 'Oui'), 'p50'] = df1["puissance_installée"] * 8760 * 0.15#calculer le p_50 projet solaire en planification
df1.loc[(df1['technologie'] == 'solaire') & (df1['en_planif'] == 'Oui'), 'p90'] = df1["puissance_installée"] * 8760 * 0.13#calculer le p_90 projet solaire en planification

#To calculate p50 p90 adjusted by the pct_couverture
df1["p50"] = df1["p50"] * df1["pct_couverture"]
df1["p90"] = df1["p90"] * df1["pct_couverture"]

prod_planif = df1.copy()
prod_planif_solaire = prod_planif.loc[(prod_planif['technologie'] == "solaire") & (prod_planif['en_planif'] == 'Oui')]
prod_planif_eolien = prod_planif.loc[(prod_planif['technologie'] == "éolien") & (prod_planif['en_planif'] == 'Oui')]

prod_planif_solaire.reset_index(drop = True, inplace=True)
prod_planif_eolien.reset_index(drop = True, inplace=True)

#To determine the number of solar and eolien
n_sol=len(prod_planif_solaire)
n_eol=len(prod_planif_eolien)

prod_planif_solaire.to_excel(path_dir_temp + "prod_planif_solaire_hedge.xlsx", index = False, float_format="%.3f")
prod_planif_eolien.to_excel(path_dir_temp + "prod_planif_eolien_hedge.xlsx", index = False, float_format="%.3f")

mean_perc_sol=mean_perc.iloc[:,[0, 1]]
mean_perc_eol=mean_perc.iloc[:,[0,-1]]

#create a dataframe with date from 2022 to 2028 solaire
start_date=pd.to_datetime([date_obj] * n_sol)
d1=pd.DataFrame()
for i in range(0, horizon):
    df_buffer = prod_planif_solaire.copy() 
    df_buffer["date"] = start_date
    d1 = pd.concat([d1, df_buffer],axis=0)
    start_date = start_date + pd.DateOffset(months=1)

#To reset index    
d1.reset_index(drop=True, inplace=True)

#create a mth column containing number of month
mean_perc_sol = mean_perc_sol.assign(mth=[1 + i for i in xrange(len(mean_perc_sol))])[['mth'] + mean_perc_sol.columns.tolist()]

#To calculate adjusted p50 and p90 solar adusted by the mean
s = mean_perc_sol.set_index('mth')['m_pct_solaire']
pct = pd.to_datetime(d1['date']).dt.month.map(s)
d1['p50_adj'] = -d1['p50'] * pct
d1['p90_adj'] = -d1['p90'] * pct

#create a dataframe with date from 2022 to 2028 éolien
start_date = pd.to_datetime([date_obj] * n_eol)
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
d2['p50_adj'] = -d2['p50'] * pct
d2['p90_adj'] = -d2['p90'] * pct

#==================

#To create new columns année et mois
d1["date_debut"] = pd.to_datetime(d1["date_debut"])
d1["date_fin"] = pd.to_datetime(d1["date_fin"])
d1["date_dementelement"] = pd.to_datetime(d1["date_dementelement"])
d1['année'] = d1['date'].dt.year
d1['trim'] = d1['date'].dt.to_period('Q').dt.strftime("Q%q-%y")
d1['mois'] = d1['date'].dt.month
d1 = d1[['hedge_id','projet_id', 'projet', 'type_hedge', 'date_debut', "date_fin", 'date_dementelement', 'date', 'année', 'trim', 'mois', 'p50_adj', 'p90_adj']]

#To remove p50 p90 based on date_debut
#Condition:date column is less (in total seconds) than first date for each projet_id's first date_debut value
cond=((d1['date'] - d1.groupby(['projet_id', 'hedge_id'])['date_debut'].transform('first')).dt.total_seconds())<0
d1['p50_adj'] = np.where(cond,'', d1['p50_adj'])
d1['p90_adj'] = np.where(cond,'', d1['p90_adj'])
#To remove p50 p90 based on date_debut
d1['type_hedge'] = np.where(cond,'', d1['type_hedge'])

#To remove p50 p90 based on date_fin
cond_2=((d1['date'] - d1.groupby(['projet_id', 'hedge_id'])['date_fin'].transform('first')).dt.total_seconds())>0
d1['p50_adj'] = np.where(cond_2, '', d1['p50_adj'])
d1['p90_adj'] = np.where(cond_2, '', d1['p90_adj'])
#To remove type_hedge based on date_fin
d1['type_hedge'] = np.where(cond_2, '', d1['type_hedge'])

#To reset index
d1.reset_index(inplace=True, drop=False)
d1 = d1.assign(rw_id=[1 + i for i in xrange(len(d1))])[['rw_id'] + d1.columns.tolist()]
d1 = d1[['rw_id', 'hedge_id', 'projet_id', 'projet', 'type_hedge', 'date', 'année', 'trim', 'mois', 'p50_adj', 'p90_adj']]

###############################################################################
###############################################################################
###############################################################################

#To create new columns année et mois
d2["date_debut"] = pd.to_datetime(d2["date_debut"])
d2["date_fin"] = pd.to_datetime(d2["date_fin"])
d2['année'] = d2['date'].dt.year
d2['trim'] = d2['date'].dt.to_period('Q').dt.strftime("Q%q-%y")
d2['mois'] = d2['date'].dt.month
d2 = d2[['hedge_id','projet_id', 'projet', 'type_hedge', 'date_debut', 'date_fin', 'date_dementelement', 'date', 'année', 'trim', 'mois', 'p50_adj', 'p90_adj']]

#To remove p50 p90 based on date_debut
#Condition:date column is less (in total seconds) than first date for each projet_id's first date_cod value
cond=((d2['date'] - d2.groupby(['projet_id', 'hedge_id'])['date_debut'].transform('first')).dt.total_seconds())<0
d2['p50_adj'] = np.where(cond,'', d2['p50_adj'])
d2['p90_adj'] = np.where(cond,'', d2['p90_adj'])
#To remove p50 p90 based on date_debut
d2['type_hedge']=np.where(cond,'', d2['type_hedge'])

#To remove p50 p90 based on date_dementelemnt
cond_2=((d2['date'] - d2.groupby(['projet_id', 'hedge_id'])['date_fin'].transform('first')).dt.total_seconds())>0
d2['p50_adj'] = np.where(cond_2, '', d2['p50_adj'])
d2['p90_adj'] = np.where(cond_2, '', d2['p90_adj'])
#To remove type_hedge based on date_dementelemnt
d2['type_hedge']=np.where(cond_2,'', d2['type_hedge'])

#To reset index
d2.reset_index(inplace=True, drop=False)
d2 = d2.assign(rw_id=[1 + i for i in xrange(len(d2))])[['rw_id'] + d2.columns.tolist()]
d2 = d2[['rw_id','hedge_id', 'projet_id', 'projet', 'type_hedge', 'date', 'année', 'trim', 'mois', 'p50_adj', 'p90_adj']]

#To export as excel files
d1.to_excel(path_dir_temp + 'p50_p90_hedge_planif_solaire.xlsx', index=False, float_format="%.3f")
d2.to_excel(path_dir_temp + 'p50_p90_hedge_planif_eolien.xlsx', index=False, float_format="%.3f")

#To check data
#print(df1.loc[(df1['projet_id'] == 'SE19') & (df1['hedge_id'] == 294)])
#df_temp = d1.loc[(d1['projet_id'] == 'SE19') & (d1['hedge_id'] == 294)]
#df_temp.head(40)

#==============================================================================================
#======  To merge p50_hedge_vmr & p50_hedge_planif_solaire & p50_hedge_planif_eolien ==========
#==============================================================================================

#To import p50_p90
df_1=pd.read_excel(path_dir_temp+'p50_p90_hedge_vmr.xlsx')
df_2=pd.read_excel(path_dir_temp+'p50_p90_hedge_planif_solaire.xlsx')
df_3=pd.read_excel(path_dir_temp+'p50_p90_hedge_planif_eolien.xlsx')

print(df_1.shape)
print(df_2.shape)
print(df_3.shape)

#To merge hedge_vmr and hedge_planif

frames = [df_1, df_2, df_3]
hedge_vmr_planif =pd.concat(frames, axis= 0, ignore_index=True)
hedge_vmr_planif.reset_index(drop=True, inplace=True)

hedge_vmr_planif.drop("rw_id", axis=1, inplace=True)
hedge_vmr_planif = hedge_vmr_planif.assign(rw_id=[1 + i for i in xrange(len(hedge_vmr_planif))])[['rw_id'] + hedge_vmr_planif.columns.tolist()]
hedge_vmr_planif.to_excel(path_dir_in+"p50_p90_hedge_vmr_planif.xlsx", index=False, float_format="%.3f")

#To check the shape
hedge_vmr_planif.shape


