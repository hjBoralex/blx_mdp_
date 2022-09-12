# -*- coding: utf-8 -*-
"""
Created on Sun Sep  4 17:19:02 2022

@author: hermann.ngayap
"""
#This script is to merge Asset Hedge contract price

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
os.chdir("C:/Users/hermann.ngayap/Boralex/Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/in")
print("The current working directory is: {0}".format(os.getcwd()))

path_dir_in='C:/Users/hermann.ngayap/Boralex/Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/in/'
path_dir_temp='C:/Users/hermann.ngayap/Boralex/Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/temp/'

#This is to get prod data namely p_50, p_90  for template asset table
#To import projet_id, p_50, p_90 
prod = pd.read_excel(path_dir_in + "template_prod.xlsx", sheet_name="prod", usecols=['projet_id', 'p50', 'p90'])
prod_planif_sol = pd.read_excel(path_dir_temp + "prod_planif_solaire.xlsx", usecols=['projet_id', 'p50', 'p90'])
prod_planif_eol = pd.read_excel(path_dir_temp + "prod_planif_eolien.xlsx", usecols=['projet_id', 'p50', 'p90'])


#To combine p50 & p90 planif_eol, planif_sol
frames = [prod, prod_planif_sol, prod_planif_eol]
prod_asset = pd.concat(frames)
prod_asset.reset_index(inplace=True, drop=True)

#To export prod data for asset tamplate 
prod_asset.to_excel(path_dir_temp + 'prod_asset.xlsx', index=False, float_format="%.3f")