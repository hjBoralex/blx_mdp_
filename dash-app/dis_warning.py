# -*- coding: utf-8 -*-
"""
Created on Wed Jun  8 10:13:51 2022

@author: hermann.ngayap
"""

import warnings

def fxn():
    warnings.warn("deprecated", DeprecationWarning)

with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    fxn()