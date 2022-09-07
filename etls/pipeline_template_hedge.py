# -*- coding: utf-8 -*-
"""
Created on Sun Sep  4 13:40:00 2022

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

#============================================================
#===============     Hedge VMR     ==========================
#============================================================

#To import data 
df_hedge_vmr=pd.read_excel(path_dir_temp+"hedge_vmr.xlsx")
df_hedge_planif=pd.read_excel(path_dir_temp+"hedge_planif.xlsx")

#To create hedge df with vmr data
df_hedge_vmr["profil"]=np.nan
df_hedge_vmr["pct_couverture"]=np.nan
df_hedge_vmr["contrepartie"]=np.nan
df_hedge_vmr["pays_contrepartie"]=np.nan


df_hedge_vmr = df_hedge_vmr[["rw_id", "hedge_id", "projet_id","projet", "technologie", "type_hedge", 
                             "cod", "date_merchant", "date_dementelement", "puissance_installée", "profil", 
                             "pct_couverture", "contrepartie", "pays_contrepartie", "en_planif"]]

df_hedge_vmr.rename(columns={"cod":"date_debut", "date_merchant":"date_fin"}, inplace = True)

ppa_vmr = ["NIBA" , "CHEP", "ALBE", "ALME", "ALMO", "ALVE", "PLOU"]

df_hedge_vmr["type_hedge"] = df_hedge_vmr["type_hedge"].str.replace("FiT", "OA")
df_hedge_vmr.loc[df_hedge_vmr.projet_id.isin(ppa_vmr) == True, "type_hedge"] = "PPA" 

df_hedge_vmr.loc[df_hedge_vmr['type_hedge'] == "OA", "pct_couverture"] = 1
df_hedge_vmr.loc[df_hedge_vmr['type_hedge'] != "OA", "pct_couverture"] = 1
df_hedge_vmr.loc[df_hedge_vmr['type_hedge'] == "PPA", "pct_couverture"] = 1

df_hedge_vmr.to_excel(path_dir_temp+"template_hedge_vmr.xlsx", index=False, float_format="%.3f")


#To create hedge df with planif data
df_hedge_planif["type_hedge"] = "CR"
df_hedge_planif["profil"] = np.nan
df_hedge_planif["pct_couverture"] = np.nan
df_hedge_planif["contrepartie"] = np.nan
df_hedge_planif["pays_contrepartie"] = np.nan

df_hedge_planif = df_hedge_planif[["rw_id", "hedge_id", "projet_id", "projet", "technologie", "type_hedge", 
                                   "cod", "date_merchant", "date_dementelement", "puissance_installée", 
                                   "profil", "pct_couverture", "contrepartie", "pays_contrepartie", "en_planif"]]

df_hedge_planif.rename(columns={"cod":"date_debut", "date_merchant":"date_fin"}, inplace = True)

ppa_planif = ["SE19", "SE07"]
df_hedge_planif.loc[df_hedge_planif.projet_id.isin(ppa_planif) == True, "type_hedge"] = "PPA"
df_hedge_planif.loc[df_hedge_planif['type_hedge'] == "CR", "pct_couverture"] = 1
df_hedge_planif.loc[df_hedge_planif['type_hedge'] == "PPA", "pct_couverture"] = 1

df_hedge_planif.to_excel(path_dir_temp+"template_hedge_planif.xlsx", index=False, float_format="%.3f")

#To merge both data frame
frames = [df_hedge_vmr, df_hedge_planif]
df_hedge = pd.concat(frames)
df_hedge.reset_index(inplace=True, drop=True)

df_hedge.drop("rw_id", axis=1, inplace=True)
df_hedge = df_hedge.assign(rw_id=[1 + i for i in xrange(len(df_hedge))])[['rw_id'] + df_hedge.columns.tolist()]
df_hedge = df_hedge[["rw_id", "hedge_id", "projet_id", "projet", "technologie", "type_hedge", 
                     "date_debut", "date_fin", "date_dementelement", "puissance_installée", 
                     "profil", "pct_couverture", "contrepartie", "pays_contrepartie", "en_planif"]]

#hedge table
version=2
df_hedge.to_excel(path_dir_in+f"template_hedge_v{version}.xlsx", index=False, float_format="%.3f")