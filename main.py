import os
import sys
from os import path
import pause

import threading
from threading import Timer, Thread
import schedule

import pandas as pd
import numpy as np

from datetime import datetime, timedelta, time
import time as ot

import requests
from public_data_config import apisdata

from get_holiday import _get_holiday
from make_stock_list import _get_stock_list
from make_cci_signal import _make_cci_signal
from collect_signal import _collect_cci_signal
from crawler_priority_jungso import _get_priority_house_jungso

from slack_message import _post_message
from slack_sdk_message_post import _post_message_with_slack_sdk

class BaseService:
    def __init__(self):
        self.myToken = apisdata["slack"]["token"]
        self.channel_name = apisdata["slack"]["channel_name"]
        self.channel_id = apisdata["slack"]["channel_id"]

        # 휴장일 데이터 가져오기
        self.mykey = apisdata["holiday"]["decoding_key"]
        self.request_url = apisdata["holiday"]["request_url"]
        self.year = "2022"
        self.market = "kospi"
        self.market_stock_dict = {
            self.market : None,
            }
        self.market_dict = None
        self.config_ndays = 20
        self.financial_engineering_days = 252
        self.start_date = (datetime.now() - timedelta(days=self.financial_engineering_days)).date().strftime("%Y-%m-%d")
        self.ko_holiday_list =  ['2022-01_01','2022-01_31','2022-02_01','2022-02_02','2022-03_01','2022-03_09','2022-05_05','2022-05_08','2022-06_01','2022-06_06','2022-08_15','2022-09_09','2022-09_10','2022-09_11','2022-09_12','2022-10_03','2022-10_09','2022-10_10','2022-12_25']
        self.target_rtn = 3.0
        self.target_days = 5
        self.max_trade_stock_count = 10

    def work(self):
        try:
            market_stock_dict = _get_stock_list(self)
            r_df, buy_stock_list, sell_stock_list, daily_buy_stock_count_mean, daily_sell_stock_count_mean, today_buy_stock_count, today_sell_stock_count, cci_over_count_mean, cci_less_count_mean, today_cci_over_count, today_cci_less_count = _collect_cci_signal(self, market_stock_dict)

            # post a list of stock trade candidates to slack.
            candidate_dict = {
                "type" : "section",
                "text" : {
                    "type" : "mrkdwn",
                    "text" : f"현 시각 매수 후보 주식 : {buy_stock_list}\n\
현 시각 매도 후보 주식 : {sell_stock_list}\n\
현 시각 매수 후보 주식 갯수 : {today_buy_stock_count}\n\
현 시각 매도 후보 주식 갯수 : {today_sell_stock_count}\n\
{self.start_date}일 이후 일평균 매수 추천 주식 갯수 : {daily_buy_stock_count_mean}\n\
{self.start_date}일 이후 일평균 매도 추천 주식 갯수 : {daily_sell_stock_count_mean}\n\
현 시각 CCI 100 초과 주식 갯수 : {today_cci_over_count}\n\
현 시각 CCI -100 미만 주식 갯수 : {today_cci_less_count}\n\
{self.start_date}일 이후 일평균 CCI 100 초과 주식 갯수 : {cci_over_count_mean}\n\
{self.start_date}일 이후 일평균 CCI -100 미만 주식 갯수 : {cci_less_count_mean}"
}
}

            _post_message_with_slack_sdk(self, blocks=[candidate_dict])

            if self.market == "kospi":
                buy_df = r_df[r_df["name"].isin(buy_stock_list)].sort_values(["cci_buy_and_hold_diff_rtn", f"within_{self.target_days}days_expected_rtn"], ascending=False).reset_index(drop=True).iloc[:self.max_trade_stock_count]
                sell_df = r_df[r_df["name"].isin(sell_stock_list)].sort_values(["cci_buy_and_hold_diff_rtn", f"within_{self.target_days}days_expected_rtn"]).reset_index(drop=True).iloc[:self.max_trade_stock_count]
            
            if self.market == "sp500":
                buy_df = r_df[r_df["symbol"].isin(buy_stock_list)].sort_values(["cci_buy_and_hold_diff_rtn", f"within_{self.target_days}days_expected_rtn"], ascending=False).reset_index(drop=True).iloc[:self.max_trade_stock_count]
                sell_df = r_df[r_df["symbol"].isin(sell_stock_list)].sort_values(["cci_buy_and_hold_diff_rtn", f"within_{self.target_days}days_expected_rtn"]).reset_index(drop=True).iloc[:self.max_trade_stock_count]

            stock_info_list = []
            
            if len(buy_df) > 0:
                for i, x in buy_df.iterrows():
                    info_dict = {
                        "type" : "section",
                        "text" : {
                            "type" : "mrkdwn",
                            "text" : f'매매신호 : {x["trade_signal"]}\n\
이름 : {x["name"]}\n\
symbol : {x["symbol"]}\n\
매매신호가격 : {x["signal_price"]}\n\
CCI수익률-구매 후 보유수익률 : {x["cci_buy_and_hold_diff_rtn"]}\n\
{self.start_date}일 이후 CCI수익률 : {x["cci_rtn"]}\n\
{self.start_date}일 구매 후 보유수익률 : {x["buy_and_hold_rtn"]}\n\
{self.target_days}일 이내 평균최고수익률 : {x["max_rtn"]}\n\
{self.target_days}일 이내 구매 후 평균최고수익일수 : {x["max_rtn_day"]}\n\
CCI전략수행시 보유주식수 : {x["remain_holding_shares"]}\n\
CCI전략수행시 보유주식 평균 구매가격: {x["holding_shares_buy_price"]}'
}
}
                    stock_info_list.append(info_dict)
            if len(sell_df) > 0:
                for i, x in sell_df.iterrows():
                    info_dict = {
                        "type" : "section",
                        "text" : {
                            "type" : "mrkdwn",
                            "text" : f'매매신호 : {x["trade_signal"]}\n\
이름 : {x["name"]}\n\
symbol : {x["symbol"]}\n\
매매신호가격 : {x["signal_price"]}\n\
CCI수익률-구매 후 보유수익률 : {x["cci_buy_and_hold_diff_rtn"]}\n\
{self.start_date}일 이후 CCI수익률 : {x["cci_rtn"]}\n\
{self.start_date}일 구매 후 보유수익률 : {x["buy_and_hold_rtn"]}\n\
{self.target_days}일 이내 평균최고수익률 : {x["max_rtn"]}\n\
{self.target_days}일 이내 구매 후 평균최고수익일수 : {x["max_rtn_day"]}\n\
CCI전략수행시 남은 보유주식수 : {x["remain_holding_shares"]}\n\
CCI전략수행시 보유주식 평균 구매가격: {x["holding_shares_buy_price"]}'
}
}
                    stock_info_list.append(info_dict)
            _post_message_with_slack_sdk(self, blocks=stock_info_list)

        except Exception as e:
            _post_message(self, text = e)
            pass

    def run_crawler(self):
        try:
            i_list = _get_priority_house_jungso(self)
        except Exception as e:
            print("run_crawler_error", e)
            pass

    def worker(self):
        ## KOSPI 구동 
        schedule.every().days.at("09:05").do(self.work)
        schedule.every().days.at("15:00").do(self.work)

        ## 중소기업 특공 크롤링 구동
        schedule.every().days.at("09:30").do(self.run_crawler)
        schedule.every().days.at("22:40").do(self.run_crawler)

        ## S&P500 구동
        schedule.every().days.at("09:30").do(self.work)
        schedule.every().days.at("22:35").do(self.work)
        

        while True:
            now = datetime.now()
            kst_now = datetime.utcnow() + timedelta(hours = 9)
            edt_now = datetime.utcnow() - timedelta(hours = 4)

            kst_weekno = kst_now.weekday()
            edt_weekno = edt_now.weekday()
            
            weekno = datetime.today().weekday()
            str_date = datetime.strftime(datetime.utnow().date(), "%Y-%m-%d")

            # 토요일인 경우 이틀 이후 다시 시작
            if weekno == 5:
                future = datetime.combine((datetime.today() + timedelta(days=2)), datetime.strptime("00:00", "%H:%M").time())
                text = "Today is saturday. Next_run_time : %s"%(future)
                print(text)
                _post_message(self, text = text)
                ot.sleep((future - now).total_seconds())

            # 일요일인 경우 하루 뒤 다시 시작
            if weekno == 6:
                future = datetime.combine((datetime.today() + timedelta(days=1)), datetime.strptime("00:00", "%H:%M").time())
                text = "Today is sunday. Next_run_time : %s"%(future)
                print(text)
                _post_message(self, text = text)
                ot.sleep((future - now).total_seconds())

            # 평일이지만 공휴일인 경우 
            if weekno < 5 and str_date in self.ko_holiday_list:
                future = datetime.combine((datetime.today()), datetime.strptime("22:30", "%H:%M").time())
                text = "Today is a Korean holiday. Next_run_time : %s"%(future)
                print(text)
                _post_message(self, text = text)
                ot.sleep((future - now).total_seconds())

            if weekno < 5 and str_date not in self.ko_holiday_list: 
                if now.hour >= 0 and now.hour < 5:
                    until_time = datetime.combine(datetime.today(), datetime.strptime("05:28", "%H:%M").time())
                    text = "Pause untile %s"%(until_time)
                    print(text)
                    _post_message(BaseService(), text = text)
                    pause.until(until_time)
                    
                if (now.time() > time(hour=5, minute=29)) and (now.time() < time(hour=5, minute=31)):
                    text = "Analyzing the SP500. Time. %s"%now.time()
                    print(text)
                    _post_message(BaseService(), text = text)
                    self.market = "sp500"

                if now.hour >= 6 and now.hour < 9:
                    until_time = datetime.combine(datetime.today(), datetime.strptime("09:00", "%H:%M").time())
                    text = "Pause until %s"%(until_time)
                    print(text)
                    _post_message(BaseService(), text = text)
                    pause.until(until_time)

                if (now.time() > time(hour=9, minute=4)) and (now.time() < time(hour=9, minute=6)):
                    text = "Analyzing the KOSPI. Time. %s"%now.time()
                    print(text)
                    _post_message(BaseService(), text = text)
                    self.market = "kospi"

                if now.hour >= 10 and now.hour < 15:
                    until_time = datetime.combine(datetime.today(), datetime.strptime("14:59", "%H:%M").time())
                    text = "Pause until %s"%(until_time)
                    print(text)
                    _post_message(BaseService(), text = text)
                    pause.until(until_time)

                if (now.time() > time(hour=14, minute=59)) and (now.time() < time(hour=15, minute=1)):
                    text = "Analyzing the KOSPI. Time. %s"%now.time()
                    print(text)
                    _post_message(BaseService(), text = text)
                    self.market = "kospi"

                if now.hour >= 16 and now.hour < 22:
                    until_time = datetime.combine(datetime.today(), datetime.strptime("22:32", "%H:%M").time())
                    text = "Pause until %s"%(until_time)
                    print(text)
                    _post_message(BaseService(), text = text)
                    pause.until(until_time)


                if (now.time() > time(hour=22, minute=34)) and (now.time() < time(hour=22, minute=36)):
                    text = "Analyzing the SP500. Time. %s"%now.time()
                    print(text)
                    _post_message(BaseService(), text = text)
                    self.market = "sp500"

                if now.hour >= 23:
                    until_time = datetime.combine(datetime.today(), datetime.strptime("23:59", "%H:%M").time())
                    text = "Pause until %s"%(until_time)
                    print(text)
                    _post_message(BaseService(), text = text)
                    pause.until(until_time)

                str_now = datetime.now().strftime('%m-%d-%y %H:%M:%S')
                print(f"Trading signal generation algorithm running! %s"%str_now)
                schedule.run_pending()    
                ot.sleep(59)

                # self.market = "sp500"
                # schedule.run_pending()
                


if __name__ == "__main__":
    BaseService().worker()
