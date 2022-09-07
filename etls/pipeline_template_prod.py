# -*- coding: utf-8 -*-
"""
Created on Sun Sep  4 16:42:07 2022

@author: hermann.ngayap
"""

import pandas as pd
import numpy as np
import pyodbc
from datetime import datetime
import datetime as dt
xrange = range
from pandasql import sqldf
pysqldf = lambda q: sqldf(q, globals())
import xlsxwriter

pd.options.display.float_format = '{:.3f}'.format
pd.set_option('display.max_columns', 200)

import os
print("The working directory was: {0}".format(os.getcwd()))
os.chdir("C:/Users/hermann.ngayap/Boralex/Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/etls/asset-hedge/")
print("The current working directory is: {0}".format(os.getcwd()))

path_dir_in='C:/Users/hermann.ngayap/Boralex/Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/in/'
path_dir_temp='C:/Users/hermann.ngayap/Boralex/Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/temp/'

#==================================================================
#====================   pipeline productible ======================
#==================================================================

#To extract p50, p90 (annual)  
df1 = pd.read_excel(path_dir_in+"Copie de Productibles - Budget 2022 - version 1 loadé du 21 09 2021.xlsx", sheet_name="Budget 2022", header=1)
df1 = df1[["Projet", "Budget 2022 (KWh) - P50", "Budget 2022 (KWh) - P90 "]]
df1 = df1.iloc[0:105,:]
df1[["Budget 2022 (KWh) - P50", "Budget 2022 (KWh) - P90 "]] = df1[["Budget 2022 (KWh) - P50", "Budget 2022 (KWh) - P90 "]]/1000
df1.columns = ["projet", "p50", "p90"]
out_projets = ["Cham Longe Le Courbil (Eole Cevennes)", "Cham Longe Bel Air", "La Bouleste", "CDB Doux le vent",
        "Evits et Josaphats", "Remise Reclainville", "Bougainville", "Renardières mont de Bezard",
        "Blendecques Elec", "Stockage de l'Arce"]


#drop rows that contain any value in the list and reset index
df1 = df1[df1.projet.isin(out_projets) == False]
df1.reset_index(inplace=True, drop=True)

#To extract p50, p90 (percentage per month) 
df2 = pd.read_excel(path_dir_in +  "Copie de Productibles - Budget 2022 - version 1 loadé du 21 09 2021.xlsx", sheet_name="BP2022 - Distribution mensuelle", header=1)
df2 = df2.iloc[0:12, 2:108]
df2.rename(columns = {'% du P50':'month'}, inplace=True)
#drop out parcs
out_projets2 = ["Cham Longe Le Courbil (Eole Cevennes)", "Cham Longe Bel Air", 
                "La Bouleste", "CDB Doux le vent", "Evits et Josaphats", 
                "Remise Reclainville", "Bougainville", "Renardières mont de Bezard", 
                "Blendecques Elec"]

df2.drop(out_projets2, axis=1, inplace=True)


#a list containing solar parcs
solaire = ["Boralex Solaire Les Cigalettes SAS (Montfort)", 
           "Boralex Solaire Lauragais SAS",
           "Saint Christophe (Clé des champs)", 
           "Peyrolles"]
#To calculate the mean perc for solar
df2["m_pct_solaire"] = df2.loc[:,solaire].mean(axis=1)
#To calculate the mean perc for eolien
df2["m_pct_eolien"] = df2.iloc[:,1:].drop(solaire, axis=1).mean(axis=1)

#To create a df containing   
mean_perc = df2.iloc[:,[0,-2,-1]]
prod_perc = df2.iloc[:, 0:-2]

#To rename (add parentheses) on projet names
prod_perc.rename(columns = {'Extension seuil de Bapaume XSB':'Extension seuil de Bapaume (XSB)'}, inplace=True)
prod_perc.rename(columns = {"Extension plaine d'Escrebieux XPE":"Extension plaine d'Escrebieux (XPE)"}, inplace=True)
#To export 
#To export multiple df into one excel file
# Create a Pandas Excel writer using XlsxWriter as the engine.
writer = pd.ExcelWriter(path_dir_in + 'template_prod.xlsx', engine='xlsxwriter')
# Write each dataframe to a different worksheet.
df1.to_excel(writer, sheet_name="prod", float_format="%.3f", index=False)
prod_perc.to_excel(writer, sheet_name="prod_perc", float_format="%.3f", index=False)
mean_perc.to_excel(writer, sheet_name="mean_perc", float_format="%.3f", index=False)
# Close the Pandas Excel writer and output the Excel file.
writer.save()

#===================================================================================
#========================== To add projet_id to prod data ==========================
#===================================================================================
#To import template prod 
prod = pd.read_excel(path_dir_in + "template_prod.xlsx")
prod.sort_values(by=['projet'], inplace=True, ignore_index=True)

prod_perc = pd.read_excel(path_dir_in + "template_prod.xlsx", sheet_name="prod_perc")
prod_perc = prod_perc.iloc[:,1:]

projet_names_id = pd.read_excel(path_dir_in + "template_asset.xlsx", usecols = ["projet_id", "projet", "en_planif"])
projet_names_id = projet_names_id.loc[projet_names_id["en_planif"] == "Non"]
projet_names_id.sort_values(by=['projet'], inplace=True, ignore_index=True)
projet_names_id.drop("en_planif", axis=1, inplace=True)
projet_names_id.rename(columns={"projet_id":"code", "projet":"projet_names"}, inplace=True)

#To join projet_id, projet and prod data frame
frames = [projet_names_id, prod]
df = pd.concat(frames, axis=1, ignore_index=False)

#To create a new column with projet_id
n = 3
df.loc[df['projet'].str[:n] == df['projet_names'].str[:n], 'projet_id'] = df["code"]
df=df[["projet_id", "projet", "p50", "p90"]]

#TO CHANGE PROD_PERC COLUMN NAMES BY PROJET_ID
n = 5
s = (df.assign(names=df['projet'].str[:n])
    .drop_duplicates('names')
    .set_index('names')['projet_id']
)

prod_perc_id = (pd
   .concat([prod_perc.columns.to_frame().T, prod_perc])
   .rename(columns=lambda x: s.loc[x[:n]])

)

prod_perc_id.reset_index(inplace=True, drop=True)
prod_perc_id = prod_perc_id.iloc[1:,:]

#To export multiple df into one excel file
# Create a Pandas Excel writer using XlsxWriter as the engine.
writer = pd.ExcelWriter(path_dir_in + 'template_prod.xlsx', engine='xlsxwriter')
# Write each dataframe to a different worksheet.
df.to_excel(writer, sheet_name="prod", float_format="%.3f", index=False)
prod_perc_id.to_excel(writer, sheet_name="prod_perc_id", float_format="%.3f", index=False)
prod_perc.to_excel(writer, sheet_name="prod_perc", float_format="%.3f", index=False)
mean_perc.to_excel(writer, sheet_name="mean_perc", float_format="%.3f", index=False)
# Close the Pandas Excel writer and output the Excel file.
writer.save()


