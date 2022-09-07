# -*- coding: utf-8 -*-
"""
Created on Sun Sep  4 22:13:33 2022

@author: hermann.ngayap
"""

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
os.chdir("C:/Users/hermann.ngayap/Boralex/Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/in")
print("The current working directory is: {0}".format(os.getcwd()))

path_dir_in='C:/Users/hermann.ngayap/Boralex/Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/in/'
path_dir_temp='C:/Users/hermann.ngayap/Boralex/Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/temp/'

price=pd.read_excel(path_dir_in+'Production et stats de tous les parcs B22.xlsx', 
                      sheet_name='1-EO_Calcul Reporting', header=10)
#To choose only columns rows and columns with price
price=price.iloc[:106, 80:93]
#To rename columns
price.rename(columns={'Site.4': 'site', 'JAN [€/MWh].3': 'jan', 'FEB [€/MWh].3':'feb', 'MAR [€/MWh].3':'mar', 'APR [€/MWh].3':'apr', 
                      'MAY [€/MWh].3':'may', 'JUNE [€/MWh].3':'june', 'JULY [€/MWh].3':'july', 'AUG [€/MWh].3':'aug', 'SEPT [€/MWh].3':'sep',
                      'OCT [€/MWh].3':'oct', 'NOV [€/MWh].3':'nov', 'DEC [€/MWh].3':'dec'}, inplace=True)
#To create a list containing projets out of service
out_projets = ['Blendecques Elec', 'Bougainville', 'Cham Longe Bel Air', 'CDB Doux le vent' ,'Cham Longe Le Courbil (Eole Cevennes)',
              'Evits et Josaphats', 'La Bouleste', 'Renardières mont de Bezard', 'Remise Reclainville', "Stockage de l'Arce", ]

#To change PBF Blanches Fosses into Blanches Fosses PBF
price.loc[price['site']=='PBF Blanches Fosses', 'site']='Blanches Fosses PBF'
#To drop rows that contain any value in the list and reset index
price = price[price['site'].isin(out_projets) == False]
price.sort_values(by=['site'], inplace=True, ignore_index=True)
price.reset_index(inplace=True, drop=True)
#To export 
price.to_excel(path_dir_temp+'template_price.xlsx', index=False)

#To import projet_id
projet_names_id = pd.read_excel(path_dir_in+"template_asset.xlsx", usecols = ["projet_id", "projet", "en_planif"])
projet_names_id = projet_names_id.loc[projet_names_id["en_planif"] == "Non"]
projet_names_id.sort_values(by=['projet'], inplace=True, ignore_index=True)
projet_names_id.drop("en_planif", axis=1, inplace=True)
projet_names_id.reset_index(drop=True, inplace=True)
projet_names_id.rename(columns={"projet_id":"code"}, inplace=True)


#To join 2 data frame
frame=[projet_names_id, price]
price_id = pd.concat(frame, axis=1, ignore_index=False)

#To create a new column with projet_id
n = 5
price_id.loc[price_id['site'].str[:n] == price_id['projet'].str[:n], 'projet_id'] = price_id["code"]
price_id=price_id[["projet_id", "site", "jan", "feb", "mar", "apr", "may", "june","july", "aug", "sep", "oct", "nov", "dec", ]]
#price_id.to_excel(path_dir_in+'template_price_id.xlsx', index=False)

#PRICES PROJECTS IN PLANIF
#HEDGE DATA
hedge_=pd.read_excel(path_dir_in+"template_hedge.xlsx")
hedge=hedge_.loc[hedge_['en_planif'] == 'Non']
hedge.reset_index(drop=True, inplace=True)
#List containing ppa
ppa=['Ally Bessadous', 'Ally Mercoeur', 'Ally Monteil', 'Ally Verseilles', 'Chépy', 'La citadelle', 
     'Nibas', 'Plouguin', 'Mazagran', 'Pézènes-les-Mines']

hedge_planif_eol=hedge_.loc[(hedge_['en_planif'] == 'Oui') & (hedge_['technologie'] == 'éolien')]
hedge_planif_sol=hedge_.loc[(hedge_['en_planif'] == 'Oui') & (hedge_['technologie'] == 'solaire')]
#To create a subset of df containing only ppa
hedge_ppa=hedge_planif_sol[hedge_planif_sol['projet'].isin(ppa) == True]
#To remove ppa from sol
hedge_planif_sol=hedge_planif_sol[hedge_planif_sol['projet'].isin(ppa) == False]

hedge_planif_sol=hedge_planif_sol.iloc[:,np.r_[1, 2, 3, 5, 6, 7]]
hedge_planif_eol=hedge_planif_eol.iloc[:,np.r_[1, 2, 3, 5, 6, 7]]
hedge_ppa=hedge_ppa.iloc[:,np.r_[1, 2, 3, 5, 6, 7]]

#ASSET DATA
asset_=pd.read_excel(path_dir_in+"template_asset.xlsx")
asset=asset_.loc[asset_['en_planif']=='Non']
asset=asset[['asset_id', 'projet_id', 'cod', 'date_merchant']]
asset.reset_index(drop=True, inplace=True)
#subseting
asset_planif_sol = asset_.loc[(asset_['en_planif'] == 'Oui') & (asset_['technologie'] == 'solaire')]
asset_planif_eol = asset_.loc[(asset_['en_planif'] == 'Oui') & (asset_['technologie'] == 'éolien')]
#list containing ppa
ppa=['Ally Bessadous', 'Ally Mercoeur', 'Ally Monteil', 'Ally Verseilles', 'Chépy', 'La citadelle', 
     'Nibas', 'Plouguin', 'Mazagran', 'Pézènes-les-Mines']

#To filter asset under ppa 
asset_ppa=asset_planif_eol[asset_planif_eol['projet'].isin(ppa) == True]

#subseting: select only asset_id, projet_id, cod, date merchant, date dementelement 
asset_planif_sol=asset_planif_sol.iloc[:,np.r_[1, 2, 3, 5, 10]]
asset_planif_eol=asset_planif_eol.iloc[:,np.r_[1, 2, 3, 5, 10]]

#solaire
time_horizon = 12*7
df1 = hedge_planif_sol.copy()
nbr = len(df1)     
start_date = pd.to_datetime(["2022-01-01"] * nbr)
d1 = pd.DataFrame()
for i in range(0, time_horizon):
    df_buffer= df1 
    df_buffer["date"] = start_date
    d1 = pd.concat([d1, df_buffer], axis=0)
    start_date= start_date + pd.DateOffset(months=1)
#reset index    
d1.reset_index(drop=True, inplace=True)

#éolien
df2 = hedge_planif_eol.copy()
nbr = len(df2)     
start_date = pd.to_datetime(["2022-01-01"] * nbr)
d2 = pd.DataFrame()
for i in range(0, time_horizon):
    df_buffer= df2 
    df_buffer["date"] = start_date
    d2 = pd.concat([d2, df_buffer], axis=0)
    start_date= start_date + pd.DateOffset(months=1)
#reset index    
d2.reset_index(drop=True, inplace=True)

#To create quarter and month columns
d1['année'] = d1['date'].dt.year
d1['trimestre'] = d1['date'].dt.quarter
d1['mois'] = d1['date'].dt.month

d2['année'] = d2['date'].dt.year 
d2['trimestre'] = d2['date'].dt.quarter
d2['mois'] = d2['date'].dt.month

#Create price column
d1.loc[d1['type_hedge']=='CR', 'price'] = 60
d2.loc[d2['type_hedge']=='CR', 'price'] = 70

#To merge wind and solar price data
frame = [d1, d2]
d = pd.concat(frame, axis=0, ignore_index=True)
d.reset_index(inplace=True, drop=True)

#To remove price based on date_debut
#Condition:date column is less (in total seconds) than first date for each projet_id's first date_cod value
cond=((d['date'] - d.groupby(['hedge_id', 'projet_id'])['date_debut'].transform('first')).dt.total_seconds())<0
d['price'] = np.where(cond,'', d['price'])
#To remove price based on date_fin
cond_2=((d['date'] - d.groupby(['hedge_id', 'projet_id'])['date_fin'].transform('first')).dt.total_seconds())>0
d['price'] = np.where(cond_2, '', d['price'])

#To make a copy of df
prices_eol_sol=d.copy()
#Export oa, cr eol solar projects in planif
prices_eol_sol.to_excel(path_dir_temp+'contracts_prices_eol_sol.xlsx', index=False)

#PRICES PROJETS NOT IN PLANIF

'''
projet avec des prix qui commencent en cours de 
l'année:Bois des Fontaines, Repowering Bougainville, Repowering Evit Et Josaphats, Repowering Reclainville
les prix avant la cod sont egales à 0 puis l'année suivanate, les prix sont disponibles toutes l'année. Pour corriger nous devons 
créer un df contenant les prix de 2022 à 2023 puis un second de 2023 à 2028
'''
#To import prices data
#price_=price_id.copy()
price_=pd.read_excel(path_dir_in+'template_price_id.xlsx')
#
out_projets=['Bois des Fontaines', 'Repowering Bougainville', 'Repowering Evit Et Josaphats', 'Repowering Reclainville']

#Drop rows that contain any value in the list and reset index
price__ = price_[price_['site'].isin(out_projets) == False]
price__.reset_index(inplace=True, drop=True)

#
data = {'projet_id': ['GO01', 'KEI3', 'GO02', 'GO03'],
        'site':['Bois des Fontaines', 'Repowering Bougainville', 'Repowering Evit Et Josaphats', 'Repowering Reclainville'],
        'jan': ['67.460', '75.120', '73.790', '73.790'],
        'feb': ['67.460', '75.120', '73.790', '73.790'],
        'mar': ['67.460', '75.120', '73.790', '73.790'],
        'apr': ['67.460', '75.120', '73.790', '73.790'],
        'may': ['67.460', '75.120', '73.790', '73.790'],
        'june': ['67.460', '75.120', '73.790', '73.790'],
        'july': ['67.460', '75.120', '73.790', '73.790'],
        'aug': ['67.460', '75.120', '73.790', '73.790'],
        'sep': ['67.460', '75.120', '73.790', '73.790'],
        'oct': ['67.460', '75.120', '73.790', '73.790'],
        'nov': ['67.460', '75.120', '73.790', '73.790'],
        'dec': ['67.460', '75.120', '73.790', '73.790']
}
price___=pd.DataFrame(data=data)

#To merge df  
frames=[price__, price___]
price_23_28_id=pd.concat(frames, axis=0)
#projet_23_28_id.sort_values(by=['projet'], inplace=True, ignore_index=True)
price_23_28_id.reset_index(inplace=True, drop=True)

#To join hedge, asset and contract price data frames
#data prior 2023
df_temp=pd.merge(hedge, asset, how='inner', on='projet_id')
df=pd.merge(df_temp, price_id, how='inner', on='projet_id')

#post 2022 (2023 to 2028)
df_temp2=pd.merge(hedge, asset, how='inner', on='projet_id')
df2=pd.merge(df_temp2, price_23_28_id, how='inner', on='projet_id')

#To print columns + indexes
#list(enumerate(df.columns))
#Select by range and position   
df_ = df.iloc[:,np.r_[1, 2, 3, 4, 5, 6, 7, 8, 15, 16, 17, 18, 19, 20:31]]
#df subseting
price_oa_cr_temp = df_.loc[df_['type_hedge'] != 'PPA']
price_oa_cr_temp.reset_index(inplace=True, drop=True)#only 2022 prices data 
price_ppa = df_.loc[df_['type_hedge'] == 'PPA']
price_ppa = price_ppa.iloc[:, 0:11]

#df2= data from 2023 to 2028
df2_ = df2.iloc[:,np.r_[1, 2, 3, 4, 5, 6, 7, 8, 15, 16, 17, 18, 19, 20:31]]
#df subseting
price_oa_cr_temp2 = df2_.loc[df2_['type_hedge'] != 'PPA']
price_oa_cr_temp2.reset_index(inplace=True, drop=True)

#OA CR
'''
To divide the data frame in 2 distinct df
price_oa_cr_: contain attributes 
price_oa_cr: contain prices date  
'''
price_oa_cr_temp_ = price_oa_cr_temp.iloc[:, 0:12]
price_oa_cr_temp2_ = price_oa_cr_temp2.iloc[:, 0:12]

#price from Jan to dec 2022
#price jan
price_oa_cr_1 = pd.concat([price_oa_cr_temp_, price_oa_cr_temp.iloc[:, 12]], axis=1)
price_oa_cr_1.rename(columns = {'jan':'price'}, inplace = True)
#price fev
price_oa_cr_2 = pd.concat([price_oa_cr_temp_, price_oa_cr_temp.iloc[:, 13]], axis=1)
price_oa_cr_2.rename(columns = {'feb':'price'}, inplace = True)
#price mar
price_oa_cr_3 = pd.concat([price_oa_cr_temp_, price_oa_cr_temp.iloc[:, 14]], axis=1)
price_oa_cr_3.rename(columns = {'mar':'price'}, inplace = True)
#price avr
price_oa_cr_4 = pd.concat([price_oa_cr_temp_, price_oa_cr_temp.iloc[:, 15]], axis=1)
price_oa_cr_4.rename(columns = {'apr':'price'}, inplace = True)
#price may
price_oa_cr_5 = pd.concat([price_oa_cr_temp_, price_oa_cr_temp.iloc[:, 16]], axis=1)
price_oa_cr_5.rename(columns = {'may':'price'}, inplace = True)
#price jun
price_oa_cr_6 = pd.concat([price_oa_cr_temp_, price_oa_cr_temp.iloc[:, 17]], axis=1)
price_oa_cr_6.rename(columns = {'june':'price'}, inplace = True)
#price jul
price_oa_cr_7 = pd.concat([price_oa_cr_temp_, price_oa_cr_temp.iloc[:, 18]], axis=1)
price_oa_cr_7.rename(columns = {'july':'price'}, inplace = True)
#price aug
price_oa_cr_8 = pd.concat([price_oa_cr_temp_, price_oa_cr_temp.iloc[:, 19]], axis=1)
price_oa_cr_8.rename(columns = {'aug':'price'}, inplace = True)
#price sep
price_oa_cr_9 = pd.concat([price_oa_cr_temp_, price_oa_cr_temp.iloc[:, 20]], axis=1)
price_oa_cr_9.rename(columns = {'sep':'price'}, inplace = True)
#price oct
price_oa_cr_10 = pd.concat([price_oa_cr_temp_, price_oa_cr_temp.iloc[:, 21]], axis=1)
price_oa_cr_10.rename(columns = {'oct':'price'}, inplace = True)
#price nov
price_oa_cr_11 = pd.concat([price_oa_cr_temp_, price_oa_cr_temp.iloc[:, 22]], axis=1)
price_oa_cr_11.rename(columns = {'nov':'price'}, inplace = True)
#price dec
price_oa_cr_12 = pd.concat([price_oa_cr_temp_, price_oa_cr_temp.iloc[:, 23]], axis=1)
price_oa_cr_12.rename(columns = {'dec':'price'}, inplace = True)


#price from Jan 2023 to 2028
#price jan
price_oa_cr_1_ = pd.concat([price_oa_cr_temp2_, price_oa_cr_temp2.iloc[:, 12]], axis=1)
price_oa_cr_1_.rename(columns = {'jan':'price'}, inplace = True)
#price fev
price_oa_cr_2_ = pd.concat([price_oa_cr_temp2_, price_oa_cr_temp2.iloc[:, 13]], axis=1)
price_oa_cr_2_.rename(columns = {'feb':'price'}, inplace = True)
#price mar
price_oa_cr_3_ = pd.concat([price_oa_cr_temp2_, price_oa_cr_temp2.iloc[:, 14]], axis=1)
price_oa_cr_3_.rename(columns = {'mar':'price'}, inplace = True)
#price avr
price_oa_cr_4_ = pd.concat([price_oa_cr_temp2_, price_oa_cr_temp2.iloc[:, 15]], axis=1)
price_oa_cr_4_.rename(columns = {'apr':'price'}, inplace = True)
#price may
price_oa_cr_5_ = pd.concat([price_oa_cr_temp2_, price_oa_cr_temp2.iloc[:, 16]], axis=1)
price_oa_cr_5_.rename(columns = {'may':'price'}, inplace = True)
#price jun
price_oa_cr_6_ = pd.concat([price_oa_cr_temp2_, price_oa_cr_temp2.iloc[:, 17]], axis=1)
price_oa_cr_6_.rename(columns = {'june':'price'}, inplace = True)
#price jul
price_oa_cr_7_ = pd.concat([price_oa_cr_temp2_, price_oa_cr_temp2.iloc[:, 18]], axis=1)
price_oa_cr_7_.rename(columns = {'july':'price'}, inplace = True)
#price aug
price_oa_cr_8_ = pd.concat([price_oa_cr_temp2_, price_oa_cr_temp2.iloc[:, 19]], axis=1)
price_oa_cr_8_.rename(columns = {'aug':'price'}, inplace = True)
#price sep
price_oa_cr_9_ = pd.concat([price_oa_cr_temp2_, price_oa_cr_temp2.iloc[:, 20]], axis=1)
price_oa_cr_9_.rename(columns = {'sep':'price'}, inplace = True)
#price oct
price_oa_cr_10_ = pd.concat([price_oa_cr_temp2_, price_oa_cr_temp2.iloc[:, 21]], axis=1)
price_oa_cr_10_.rename(columns = {'oct':'price'}, inplace = True)
#price nov
price_oa_cr_11_ = pd.concat([price_oa_cr_temp2_, price_oa_cr_temp2.iloc[:, 22]], axis=1)
price_oa_cr_11_.rename(columns = {'nov':'price'}, inplace = True)
#price dec
price_oa_cr_12_ = pd.concat([price_oa_cr_temp2_, price_oa_cr_temp2.iloc[:, 23]], axis=1)
price_oa_cr_12_.rename(columns = {'dec':'price'}, inplace = True)

#Only price_oa_cr__1 for 2022 
frames=[price_oa_cr_1, price_oa_cr_2, price_oa_cr_3, price_oa_cr_4, price_oa_cr_5, price_oa_cr_6, price_oa_cr_7, 
       price_oa_cr_8, price_oa_cr_9, price_oa_cr_10, price_oa_cr_11, price_oa_cr_12]

price_oa_cr__22 = pd.concat(frames, axis=0, ignore_index=False)
price_oa_cr__22.reset_index(inplace=True, drop=True)

#oa cr prices only 
time_horizon = 12*1
df1=pd.DataFrame(index=np.arange(89), columns=['date'])#To create an empty df of shape 89 that will contain date
nbr=price_oa_cr_1.shape[0]    
start_date = pd.to_datetime(["2022-01-01"] * nbr)
d1 = pd.DataFrame()
for i in range(0, time_horizon):
    df_buffer=df1 
    df_buffer["date"] = start_date
    d1 = pd.concat([d1, df_buffer], axis=0)
    start_date= start_date + pd.DateOffset(months=1)
#reset index    
d1.reset_index(drop=True, inplace=True)

#To concat dates df with oa cr price of only 2022
price_oa_cr__22_=pd.concat([price_oa_cr__22, d1], axis=1, ignore_index=False)

#To cretae year, trimestre, mois columns
price_oa_cr__22_['date'] = price_oa_cr__22_['date'].apply(pd.to_datetime)
price_oa_cr__22_['année'] = price_oa_cr__22_['date'].dt.year
price_oa_cr__22_['trimestre'] = price_oa_cr__22_['date'].dt.quarter
price_oa_cr__22_['mois'] = price_oa_cr__22_['date'].dt.month

#Only from price_oa_cr 2023 to 2028
frames=[price_oa_cr_1_, price_oa_cr_2_, price_oa_cr_3_, price_oa_cr_4_, price_oa_cr_5_, price_oa_cr_6_, price_oa_cr_7_, 
       price_oa_cr_8_, price_oa_cr_9_, price_oa_cr_10_, price_oa_cr_11_, price_oa_cr_12_]

price_oa_cr_23_28 = pd.concat(frames, axis=0, ignore_index=False)
price_oa_cr_23_28.reset_index(inplace=True, drop=True)

#To multiply prices df by 6  
price_oa_cr_23_28_=pd.concat([price_oa_cr_23_28]*6, ignore_index=True)

#oa cr prices only 
time_horizon = 12*6
nbr=price_oa_cr_1_.shape[0]
df1=pd.DataFrame(index=np.arange(nbr), columns=['date'])#To create an empty df of shape 89 that will contain date    
start_date = pd.to_datetime(["2023-01-01"] * nbr)#start from 2023 to 2028
d1 = pd.DataFrame()
for i in range(0, time_horizon):
    df_buffer=df1 
    df_buffer["date"] = start_date
    d1 = pd.concat([d1, df_buffer], axis=0)
    start_date= start_date + pd.DateOffset(months=1)

#Reset index    
d1.reset_index(drop=True, inplace=True)

#To concat dates df with oa cr price of only 2022
price_oa_cr_23_28_=pd.concat([price_oa_cr_23_28_, d1], axis=1, ignore_index=False)

#To cretae year, trimestre, mois columns
price_oa_cr_23_28_['date'] = price_oa_cr_23_28_['date'].apply(pd.to_datetime)
price_oa_cr_23_28_['année'] = price_oa_cr_23_28_['date'].dt.year
price_oa_cr_23_28_['trimestre'] = price_oa_cr_23_28_['date'].dt.quarter
price_oa_cr_23_28_['mois'] = price_oa_cr_23_28_['date'].dt.month

#MERGING VMR OA & CR 2022 AND 2023-2028

frame=[price_oa_cr__22_, price_oa_cr_23_28_]

d=pd.concat(frame, axis=0, ignore_index=True)
d.reset_index(inplace=True, drop=True)

#To remove price based on date_debut
#Condition:date column is less (in total seconds) than first date for each projet_id's first date_cod value
cond=((d['date'] - d.groupby(['hedge_id', 'projet_id'])['date_debut'].transform('first')).dt.total_seconds())<0
d['price'] = np.where(cond,'', d['price'])
#To remove price based on date_fin
cond_2=((d['date'] - d.groupby(['hedge_id', 'projet_id'])['date_fin'].transform('first')).dt.total_seconds())>0
d['price'] = np.where(cond_2, '', d['price'])

#To remove price based on date_dementelement
cond_3=((d['date'] - d.groupby(['hedge_id', 'projet_id'])['date_dementelement'].transform('first')).dt.total_seconds())>0
d['price'] = np.where(cond_3, '', d['price'])

#To select columns
prices_oa_cr=d.copy()
prices_oa_cr=prices_oa_cr[['hedge_id', 'projet_id', 'projet', 'type_hedge', 'date_debut', 
                           'date_fin', 'date', 'année', 'trimestre', 'mois', 'price']]

#export oa, cr vmr 
prices_oa_cr.to_excel(path_dir_temp+'contracts_prices_oa_cr_vmr.xlsx', index=False)

#To merge VMR(OA, CR) & PLANIF (CR)
frame=[prices_oa_cr, prices_eol_sol]
prices_oa_cr_vmr_planif=pd.concat(frame, axis=0, ignore_index=True)
prices_oa_cr_vmr_planif.reset_index(inplace=True, drop=True)

#export oa, cr vmr and planif without ppa
prices_oa_cr_vmr_planif.to_excel(path_dir_temp+'contracts_prices_oa_cr_vmr_planif.xlsx', index=False)

#PPA

#export oa, cr vmr and planif without ppa
prices_oa_cr_vmr_planif=pd.read_excel(path_dir_temp+'contracts_prices_oa_cr_vmr_planif.xlsx')
#To import ppa
ppa_= pd.read_excel(path_dir_in + 'ppa.xlsx')
ppa_=ppa_.iloc[:,np.r_[0, 1, 2, 4, 5, 6, -1]]


#Import date cod & date_dementelement from asset
asset_ = pd.read_excel(path_dir_in + "template_asset.xlsx")
asset_=asset_[['asset_id', 'projet_id', 'cod', 'date_dementelement', 'date_merchant']]
asset_.reset_index(drop=True, inplace=True)

ppa=pd.merge(ppa_, asset_, how='left', on=['projet_id'])


#To create a 
time_horizon = 12*7
df = ppa.copy()
nbr = len(df)     
start_date = pd.to_datetime(["01-01-2022"] * nbr)
d = pd.DataFrame()
for i in range(0, time_horizon):
    df_buffer= df 
    df_buffer["date"] = start_date
    d = pd.concat([d, df_buffer], axis=0)
    start_date= start_date + pd.DateOffset(months=1)
    
#reset index    
d.reset_index(drop=True, inplace=True)
#To create quarter and month columns
d['année'] = d['date'].dt.year
d['trimestre'] = d['date'].dt.quarter
d['mois'] = d['date'].dt.month

#To remove price based on date_debut
#Condition:date column is less (in total seconds) than first date for each projet_id's first date_cod value
cond=((d['date'] - d.groupby(['hedge_id', 'projet_id'])['date_debut'].transform('first')).dt.total_seconds())<0
d['price'] = np.where(cond,'', d['price'])
#To remove price based on date_fin
cond_2=((d['date'] - d.groupby(['hedge_id', 'projet_id'])['date_fin'].transform('first')).dt.total_seconds())>0
d['price'] = np.where(cond_2, '', d['price'])
#To remove price based on date_dementelemnt
cond_2=((d['date'] - d.groupby(['hedge_id', 'projet_id'])['date_dementelement'].transform('first')).dt.total_seconds())>0
d['price'] = np.where(cond_2, '', d['price'])

prices_ppa=d[['hedge_id', 'projet_id', 'projet', 'type_hedge', 'date_debut', 
     'date_fin', 'date', 'année', 'trimestre', 'mois', 'price']]

#export oa, cr vmr and planif without ppa
prices_ppa.to_excel(path_dir_temp+'contracts_prices_ppa.xlsx', index=False)

#MERGING VMR(OA, PPA, CR), PLANIF (CR) AND VMR PPA
frame=[prices_oa_cr_vmr_planif, prices_ppa]
prices_oa_cr_ppa= pd.concat(frame, axis=0, ignore_index=True)
prices_oa_cr_ppa.reset_index(inplace=True, drop=True)

prices_oa_cr_ppa=prices_oa_cr_ppa.assign(rw_id=[1 + i for i in xrange(len(prices_oa_cr_ppa))])[['rw_id'] + prices_oa_cr_ppa.columns.tolist()]
prices_oa_cr_ppa=prices_oa_cr_ppa[['rw_id', 'hedge_id', 'projet_id', 'projet', 'date', 'année', 'trimestre', 'mois', 'price']]

prices_oa_cr_ppa.to_excel(path_dir_in+'contracts_prices_oa_cr_ppa.xlsx', index=False)
prices_oa_cr_ppa.shape



