# -*- coding: utf-8 -*-
"""
Created on Sun Sep  4 13:23:00 2022

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
os.chdir("C:/Users/hermann.ngayap/Boralex/Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/etls/asset-hedge/")
print("The current working directory is: {0}".format(os.getcwd()))

path_dir_in='C:/Users/hermann.ngayap/Boralex/Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/in/'
path_dir_temp='C:/Users/hermann.ngayap/Boralex/Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/temp/'


#============================================================
#===============     Asset VMR     ==========================
#============================================================

df=pd.read_excel(path_dir_in+ "Volumes marchés_ Repowering.xlsx", sheet_name="BD_temp_1", header=0)
#Out projects
out_projets = ["Bougainville", "Cham Longe 1", "Evits et Josaphats", "Remise Reclainville", 
               "Evits et Josaphats", "Remise Reclainville", "Maurienne / Gourgançon", "La Bouleste", 
               "Cham Longe 1 - off", "Remise Reclainville - off", "Evits et Josaphats - off", "Bougainville - off", 
               "Maurienne / Gourgançon - off", "Saint-André - off"]

df.rename(columns = {"Alias":"projet", "Technologie":"technologie", "COD":"cod", "MW 100%":"mw", "Taux succès":"taux_succès", 
                     "MW pondérés":"puissance_installée", "EOH":"eoh","Mécanisme":"type_hedge", "Début FiT ajusté":"date_debut", 
                     "Date Merchant":"date_merchant"}, inplace = True)

#Drop rows that contain any value in the list and reset index
df = df[df['Parc '].isin(out_projets) == False]
df.reset_index(inplace=True, drop=True)

#To select rows where Nom is NaN
df = df[df['projet'].notna()]
df.reset_index(inplace=True, drop=True)

#To replace correct eolien & solar writing
df["technologie"] = df["technologie"].str.replace("Eolien", "éolien")
df["technologie"] = df["technologie"].str.replace("PV", "solaire")
#
df["taux_succès"] = 1
#
df["puissance_installée"] = df["mw"] * df["taux_succès"]
#
df["date_dementelement"] = df["date_msi"] - pd.DateOffset(months=6)
#
df["en_planif"] = "Non"
#
df = df.assign(asset_id=[1 + i for i in xrange(len(df))])[['asset_id'] + df.columns.tolist()]
df = df.assign(rw_id=[1 + i for i in xrange(len(df))])[['rw_id'] + df.columns.tolist()]
#
df_asset = df[["rw_id","asset_id", "projet_id", "projet", "technologie", "cod", "mw", "taux_succès", "puissance_installée", 
               "type_hedge", "date_debut", "eoh", "date_merchant", "date_dementelement", "repowering", 
               "date_msi", "en_planif"]]

#To select in planif that should be in volume_repowering 
vmr_to_planif = df_asset[df_asset['cod'] > (dt.datetime.today() + pd.offsets.YearEnd()).strftime('%Y-%m-%d')]

vmr_to_planif = vmr_to_planif[["rw_id", "asset_id", "projet_id", "projet", "technologie", "cod", "mw", "taux_succès", 
                               "puissance_installée", "eoh", "date_merchant", "date_dementelement", 
                               "repowering", "date_msi", "en_planif"]]

#To exclude projects in planif from the one in production ones
df_asset = df_asset[df_asset['cod'] <= (dt.datetime.today() + pd.offsets.YearEnd()).strftime('%Y-%m-%d')]

#
hedge_vmr = df_asset[["rw_id", "projet_id", "projet", "technologie", "type_hedge", "cod", 
                      "date_merchant", "date_dementelement", "puissance_installée", "en_planif"]]
#
hedge_vmr = hedge_vmr.assign(hedge_id=[1 + i for i in xrange(len(hedge_vmr))])[['hedge_id'] + hedge_vmr.columns.tolist()]
#
hedge_vmr = hedge_vmr[["rw_id", "hedge_id", "projet_id", "projet", "technologie", "type_hedge", "cod", 
                       "date_merchant", "date_dementelement", "puissance_installée", "en_planif"]]
#
df_asset = df_asset[["rw_id", "asset_id", "projet_id", "projet", "technologie", "cod", "mw", "taux_succès", "puissance_installée", 
                     "eoh", "date_merchant", "date_dementelement", "repowering", "date_msi", "en_planif"]]

#To make export as excel files
vmr_to_planif.to_excel(path_dir_temp+"asset_vmr_to_planif.xlsx", index=False, float_format="%.3f")
df_asset.to_excel(path_dir_temp + "template_asset_vmr.xlsx", index=False, float_format="%.3f")
df_asset.to_excel(path_dir_temp + "projet_names.xlsx", index=False, columns=["asset_id", "projet_id", "projet"])
hedge_vmr.to_excel(path_dir_temp + "hedge_vmr.xlsx", index=False, float_format="%.3f")


#============================================================
#===============   Asset Planif  ============================
#============================================================

df = pd.read_excel(path_dir_in+"Outils planification agrege 2022-2024.xlsm", sheet_name="Planification", header=20, 
                    usecols=['#', 'Nom', 'Technologie', 'Puissance totale (pour les  repowering)', 
                             'date MSI depl', "date d'entrée dans statut S", 'Taux de réussite'])


hedge_vmr=pd.read_excel(path_dir_temp+"hedge_vmr.xlsx")
asset_vmr=pd.read_excel(path_dir_temp+"template_asset_vmr.xlsx") 

#To drop all optimisation 
rows_to_drop = sqldf("select * from df where Nom like 'optimisation%';", locals())
rows_to_drop = list(rows_to_drop['Nom'])

rows_to_drop2 = sqldf("select * from df where Nom like 'Poste%';", locals())
rows_to_drop2 = list(rows_to_drop2['Nom'])

rows_to_drop3 = sqldf("select * from df where Nom like 'Stockage%';", locals())
rows_to_drop3 = list(rows_to_drop3['Nom'])

rows_to_drop4 = sqldf("select * from df where Nom like 'Régul%';", locals())
rows_to_drop4 = list(rows_to_drop4['Nom'])

#To rename columns
df.rename(columns = {'#':'projet_id', 'Nom':'projet', 'Technologie':'technologie', 
                      'Puissance totale (pour les  repowering)':'mw','date MSI depl':'date_msi', 
                      'Taux de réussite':'taux_succès'}, inplace=True)

#drop optimisation
#df5.loc[df5['Nom'].isin([rows_to_drop])]
df = df[df.projet.isin(rows_to_drop) == False]
#drop poste de...
df = df[df.projet.isin(rows_to_drop2) == False]
#drop poste de...
df = df[df.projet.isin(rows_to_drop3) == False]
#drop poste de...
df = df[df.projet.isin(rows_to_drop4) == False]

df = df.loc[df['technologie'] != 'autre']


df['date_msi'] = pd.to_datetime(df["date_msi"])

#To fill n/a date_msi with with date today + 50 years planif
df["date_msi"].fillna((dt.datetime.today() + pd.DateOffset(years=50)).strftime('%Y-%m-%d'), inplace=True)

#To select projects in planif that should be in production 
df_to_asset_vmr = df[df['date_msi'] < (dt.datetime.today() + pd.offsets.YearEnd()).strftime('%Y-%m-%d')]


#To select only data with cod superior to year's end date
filt = df['date_msi'] > (dt.datetime.today() + pd.offsets.YearEnd()).strftime('%Y-%m-%d') 
df = df.loc[filt]

#To select rows where Nom is NaN
df = df[df['projet'].notna()]
df_to_asset_vmr.reset_index(inplace=True, drop=True)
df.reset_index(inplace=True, drop=True)

df['cod'] = df['date_msi']
df_to_asset_vmr['cod'] = df_to_asset_vmr['date_msi']

#To fill n/a with 0.80
df["taux_succès"].fillna(0.599, inplace=True)
df_to_asset_vmr['taux_succès'].fillna(1, inplace=True)

#To calculate mw100%
df['puissance_installée'] = df['mw'] * df["taux_succès"]
df_to_asset_vmr['puissance_installée'] = df_to_asset_vmr["mw"] * df_to_asset_vmr["taux_succès"]
#eoh
df['eoh'] = np.nan
df_to_asset_vmr['eoh'] = np.nan
#
df['date_merchant'] = df["cod"] + pd.DateOffset(years=20) 
df_to_asset_vmr['date_merchant'] = df_to_asset_vmr["cod"] + pd.DateOffset(years=20) 
#
df['date_dementelement'] = np.nan
df_to_asset_vmr['date_dementelement'] = np.nan
#
df['repowering'] = np.nan
df_to_asset_vmr['repowering'] = np.nan
#
df['en_planif'] = 'Oui'
df_to_asset_vmr['en_planif'] = 'Non'
#
df["technologie"] = df["technologie"].str.replace("éolien ", "éolien")
df_to_asset_vmr["technologie"] = df_to_asset_vmr["technologie"].str.replace("éolien ", "éolien")
#
df = df.assign(rw_id=[1 + i for i in xrange(len(df))])[['rw_id'] + df.columns.tolist()]
df = df.assign(asset_id=[(len(asset_vmr)+1) + i for i in xrange(len(df))])[['asset_id'] + df.columns.tolist()]
df_to_asset_vmr = df_to_asset_vmr.assign(rw_id=[1 + i for i in xrange(len(df_to_asset_vmr))])[['rw_id'] + df_to_asset_vmr.columns.tolist()]
df_to_asset_vmr = df_to_asset_vmr.assign(asset_id=[1 + i for i in xrange(len(df_to_asset_vmr))])[['asset_id'] + df_to_asset_vmr.columns.tolist()]
#
df = df[['rw_id', "asset_id", 'projet_id', 'projet', 'technologie', 'cod', 'mw', 'taux_succès', 
         'puissance_installée', 'eoh', 'date_merchant', 'date_dementelement', 
         'repowering', 'date_msi', 'en_planif']]
#

hedge_planif = df[["rw_id", "projet_id", "projet", "technologie", "cod", "date_merchant", "date_dementelement", 
                   "puissance_installée", "en_planif"]]
hedge_planif = hedge_planif.assign(hedge_id=[(len(hedge_vmr)+1) + i for i in xrange(len(hedge_planif))])[['hedge_id'] + hedge_planif.columns.tolist()] 

hedge_planif = hedge_planif[["rw_id", "hedge_id", "projet_id", "projet", "technologie", "cod", "date_merchant", "date_dementelement", 
                             "puissance_installée", "en_planif"]]
#
df_to_asset_vmr = df_to_asset_vmr[['rw_id', 'projet_id', 'projet', 'technologie', 'cod', 'mw', 'taux_succès', 
                                 'puissance_installée', 'eoh', 'date_merchant', 'date_dementelement', 
                                 'repowering', 'date_msi', 'en_planif']]
#To export as excel
df_to_asset_vmr.to_excel(path_dir_temp+'planif_to_asset_vmr.xlsx', index=False, float_format="%.3f")
hedge_planif.to_excel(path_dir_temp+"hedge_planif.xlsx", index=False, float_format="%.3f")
df.to_excel(path_dir_temp+'template_asset_planif.xlsx', index=False, float_format="%.3f")

#===================== Merging Asset VMR and Asset Planif

df_asset_vmr = pd.read_excel(path_dir_temp+"template_asset_vmr.xlsx")
df_asset_planif = pd.read_excel(path_dir_temp+"template_asset_planif.xlsx")

frames = [df_asset_vmr, df_asset_planif]
asset_vmr_planif = pd.concat(frames)
asset_vmr_planif.reset_index(inplace=True, drop=True)

asset_vmr_planif.drop("rw_id", axis=1, inplace=True)
asset_vmr_planif = asset_vmr_planif.assign(rw_id=[1 + i for i in xrange(len(asset_vmr_planif))])[['rw_id'] + asset_vmr_planif.columns.tolist()]
#asset_vmr_planif.to_excel(path_dir_in+"template_asset.xlsx", index=False, float_format="%.3f")
