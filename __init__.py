
import os
import sys
from os import path

import time
import threading
import schedule

import pandas as pd
import numpy as np

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

import yfinance as yf

import FinanceDataReader as fdr

import requests
from public_data_config import apisdata

from slack_message import _post_message
from get_holiday import _get_korea_holiday

class BaseService:
    def __init__(self):
        self.myToken = apisdata["slack"]["token"]
        self.channel = apisdata["slack"]["channel"]

        # 휴장일 데이터 가져오기
        self.mykey = apisdata["holiday"]["decoding_key"]
        self.request_url = apisdata["holiday"]["request_url"]
        self.year = "2022"

    def work(self):
        self.get_settings()
    
    def get_settings(self, **kwargs):
        self.execute_date = datetime.utcnow().date()
        # print("run work")
        # print(self.execute_date)


class Service(BaseService):
    def get_settings(self):
        super().get_settings()



    def work(self):
        super().work()
        # _post_message(self, "test_message %s"%(datetime.now()))

        holiday_list = _get_korea_holiday(self)
        print(len(holiday_list))
        return holiday_list


# holiday_list = Service().work()
schedule.every().day.at("01:17").do(Service().work())



# holiday_list = Service().work()

# def check_holiday(holiday_list):
#     today_date = datetime.now().date()
#     if datetime.strftime(today_date, "%Y-%m-%d") not in holiday_list:
#         print("not_holi_day")

# schedule.every().day.at("01:12").do(check_holiday)

# while True:
#     schedule.run_pending()

    

    
    

        

