# -*- coding: utf-8 -*-
"""
Created on Sun Sep  4 17:13:42 2022

@author: hermann.ngayap
"""

from scrap_eex_2022 import scrap_eex
import schedule
import time
from datetime import datetime as dt
from datetime import timedelta 


def do_scrap_eex():
    
    print("futures scrapping starts")
    scrap_eex(3)
    print("futures scrapping is done")

if __name__=='__main__':
    schedule.every().day.at(f"{20}:07:00").do(do_scrap_eex)
    
    while 1:
        schedule.run_pending()
        time.sleep(1)
