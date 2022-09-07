# -*- coding: utf-8 -*-
"""
Created on Wed Jul 13 11:52:56 2022

@author: hermann.ngayap
"""
import pandas as pd

l_years=[2022, 2023, 2024, 2025, 2026, 2027, 2028]
years= pd.DataFrame(l_years, columns=['years'])

l_quarters =['Q1', 'Q2', 'Q3', 'Q4']
quarters = pd.DataFrame(l_quarters, columns=['quarters'])

l_months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
          'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
months = pd.DataFrame(l_months, columns=['months'])
