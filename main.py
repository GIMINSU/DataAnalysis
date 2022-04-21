
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
from dateutil.relativedelta import relativedelta

import yfinance as yf

import FinanceDataReader as fdr

import requests
from public_data_config import apisdata

from slack_message import _post_message
from get_holiday import _get_holiday

# kospi200 회사명 리스트 가져오기
from bs4 import BeautifulSoup
from urllib.request import urlopen

from selenium import webdriver
from selenium.webdriver.common.by import By

# pip install webdriver-manager  ## 항상 최신 버전의 chromedriver를 자동으로 사용
from webdriver_manager.chrome import ChromeDriverManager

import json

class BaseService:
    def __init__(self):
        self.myToken = apisdata["slack"]["token"]
        self.channel = apisdata["slack"]["channel"]

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

    def work(self):
        try:
            self._get_stock_list()
            self._post_signal_to_slack()
        except Exception as e:
            self._post_message(e)
            pass
    
    def _get_stock_list(self):
        print("do_get_stock_list")
        stock_list = []
        if self.market == "kospi":
            for i in range(1, 21):
                page = i
                url = 'https://finance.naver.com/sise/entryJongmok.nhn?&page={page}'.format(page = page)
                source = urlopen(url).read()
                source = BeautifulSoup(source,'lxml')
                source = source.find_all('a',target = '_parent')
                for j in range(len(source)):
                    name = source[j].text
                    stock_list.append(name)

            kospi_df = fdr.StockListing(self.market)
            self.market_dict = kospi_df[kospi_df["Name"].isin(stock_list)].set_index("Symbol")["Name"].to_dict()
            self.market_stock_dict[self.market] = self.market_dict

        elif self.market == "sp500":
            self.market_dict = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0].set_index("Symbol")["Security"].to_dict()
            self.market_stock_dict[self.market] = self.market_dict

    def _post_message(self, text):
        response = requests.post("https://slack.com/api/chat.postMessage",
            headers={"Authorization": "Bearer "+self.myToken},
            data={"channel": self.channel,"text": text}
        )
        if response.ok:
            print("Success. Send message with slack.")
        else:
            print("Fail. Send message with slack.")
            pass

    def _make_signal(self, df):
        try:
            df = df[df["Volume"]>0].reset_index(drop=True)
    
            # 수익률 계산하기
            df["daily_rtn"] = df["Close"].pct_change()  # 퍼센트 변화율
            df["st_rtn"] = (1 + df["daily_rtn"]).cumprod()  # 누적곱 계산함수 return cumulative product over a DataFrame or Series axis.
            gagr = (df.iloc[-1]['st_rtn'] ** (self.financial_engineering_days / len(df.index)) - 1) * 100

            df["TP"] = (df["High"] + df["Low"] + df["Close"]) / 3
            df["sma"] = df["TP"].rolling(self.config_ndays).mean()
            df["mad"] = df["TP"].rolling(self.config_ndays).apply(lambda x: pd.Series(x).mad())
            df["CCI"] = (df["TP"] - df["sma"]) / (0.015 * df["mad"])
            
            df["pre1day_cci"] = df["CCI"].shift(1)
            df["trade"] = None
            df["trade"] = np.where((df["CCI"] >= -100) & (df["pre1day_cci"] < -100), "buy", df["trade"])
            df["trade"] = np.where((df["CCI"] <= 100) & (df["pre1day_cci"] > 100), "sell", df["trade"])
            
            df["total_buy_price"] = 0
            df["shares"] = 0
            df["buy_price"] = 0
            df["sell_price"] = 0
            df["revenue"] = 0
            df["rate"] = 0

            buy_price_list = []
            for i, x in df.iterrows():
                if x["trade"] == "buy":
                    buy_price_list.append(x["Close"])
                    df.loc[i, "total_buy_price"] = np.sum(buy_price_list)
                    df.loc[i, "shares"] = len(buy_price_list)
                    df.loc[i, "buy_price"] = np.mean(buy_price_list)
                elif x["trade"] == None:
                    if i > 0:
                        df.loc[i, "shares"] = df.loc[i-1, "shares"]
                        df.loc[i, "buy_price"] = df.loc[i-1, "buy_price"]
                        df.loc[i, "total_buy_price"] = df.loc[i-1, "total_buy_price"]
                elif x["trade"] == "sell" and df.loc[i-1, "shares"] > 0:
                    first_price = buy_price_list.pop(0)
                    df.loc[i, "revenue"] = df.loc[i, "Close"] - first_price
                    df.loc[i, "rate"] = df.loc[i, "revenue"] / first_price * 100
                    df.loc[i, "sell_price"] = df.loc[i, "Close"]
                    df.loc[i, "shares"] = len(buy_price_list)
                    if len(buy_price_list) > 0:
                        df.loc[i, "total_buy_price"] = np.sum(buy_price_list)
                        df.loc[i, "buy_price"] = np.mean(buy_price_list)
                    else:
                        df.loc[i, "total_buy_price"] = 0
                        df.loc[i, "buy_price"] = 0
            
            r_dict_symbol = "'''%s'''"%df.iloc[-1]["Symbol"]
            name = "'''%s'''"%self.market_dict.get(df.iloc[-1]["Symbol"]).replace("\n", "")
            date = "'''%s'''"%datetime.strftime(df.iloc[-1]["Date"], "%Y-%m-%d").replace("\n", "")
            trade_signal = df.iloc[-1]["trade"]
            price = round(df.iloc[-1]["Close"], 2)
            remain_shares = df.iloc[-1]["shares"]
            holding_shares_buy_price = round(df.iloc[-1]["buy_price"], 2)
            cci_rtn = round(df[df["trade"] == "sell"].iloc[-1]["rate"], 2)
            buy_and_hold_rtn = round(gagr, 2)
            r_dict = {
                "type" : "section",
                "text" : {
                    "type" : "mrkdwn",
                    "text": "*symbol* : %s\n*name* : %s\n*trade_signal* : %s\n*date* : %s\n*price* : %s\n*remain_shares* : %s\n*holding_shares_buy_price* : %s\n*cci_rtn* : %s\n*buy_and_hold_rtn* : %s"%(r_dict_symbol, name, trade_signal, date, price, remain_shares, holding_shares_buy_price, cci_rtn, buy_and_hold_rtn)
                }
            }
            return r_dict, r_dict_symbol, trade_signal
            
        except Exception as e:
            # self._post_message(e)
            print("_make_signal Exception", e)
            pass

    def _post_signal_to_slack(self):
        print("Do _post_signal_to_slack function.")
        df = pd.DataFrame()
        buy_list = []
        sell_list = []
        buy_idata= []
        sell_idata = []

        if self.market == "kospi":
            for symbol in self.market_stock_dict[self.market]:
                ticker = symbol+".KS"
                try:
                    df = yf.download(ticker, start=self.start_date, auto_adjust=True).reset_index()
                    df["Symbol"] = symbol

                    r_dict, r_dict_symbol, trade_signal = self._make_signal(df)
                    if trade_signal == "buy":
                        buy_list.append(self.market_dict.get(symbol))
                        buy_idata.append(r_dict)
                    elif trade_signal == "sell":
                        sell_list.append(self.market_dict.get(symbol))
                        sell_idata.append(r_dict)
                except Exception as e:
                    # self._post_message(e)
                    print("kospi _post_signal_to_slack Exception", e)
                    pass

        elif self.market == "sp500":
            for symbol in self.market_stock_dict[self.market]:
                try:
                    df = yf.download(symbol, start=self.start_date, auto_adjust=True).reset_index()
                    df["Symbol"] = symbol
                    
                    r_dict, r_dict_symbol, trade_signal = self._make_signal(df)

                    if trade_signal == "buy":
                        buy_list.append(r_dict_symbol)
                        buy_idata.append(r_dict)
                    elif trade_signal == "sell":
                        sell_list.append(r_dict_symbol)
                        sell_idata.append(r_dict)

                except Exception as e:
                    # self._post_message(e)
                    print("sp500 _post_signal_to_slack Exception", e)
                    pass

        # 구매 데이터
        if len(buy_list) == 0:
            self._post_message("Today not exists buy stocks")
        if len(buy_list) > 0:
            self._post_message("Buy candidate stocks : %s"%(buy_list))
            info_message = {"blocks":buy_idata}
            print(info_message)
            self._post_message(info_message)

        # 판매 데이터
        if len(sell_list) == 0:
            self._post_message("Today not exists sell stocks")
        if len(sell_list) > 0:
            self._post_message("Sell candidate stocks : %s"%(sell_list))
            info_message = {"blocks":sell_idata}
            print(info_message)
            self._post_message(info_message)
        
        # try:
        #     df = pd.DataFrame.from_dict(buy_idata)
        #     df = df.append(pd.DataFrame.from_dict(sell_idata))
        # except Exception as e:
        #     self._post_message(e)
        #     pass

        # return df
        
    def _get_priority_house_jungso(self):
        driver = webdriver.Chrome(ChromeDriverManager().install())
        # url = "https://www.smes.go.kr/sanhakin/websquare/wq_main.do"
        url = "https://www.mss.go.kr/site/gyeonggi/ex/bbs/List.do?cbIdx=323"
        driver.get(url)
        driver.implicitly_wait(10)
        driver.find_element(By.XPATH, '//*[@id="genTopMenu_2_liTopMenu"]').click()
        driver.implicitly_wait(1)
        driver.find_element(By.XPATH, '//*[@id="genLeftMenu_3_leftMenuGrp"]').click()
        driver.implicitly_wait(1)

        idata = []
        # pagelist1_page_2
        search_date = datetime.today().date()
        str_search_date = datetime.strftime(search_date, "%Y-%m-%d")
        for page in range(1, 3):
            # print(page)
            driver.find_element(By.XPATH, '//*[@id="pagelist1_page_%s"]'%page).click()
            driver.implicitly_wait(10)
            for table_order in range (0, 10):
                title = driver.find_element_by_css_selector('#gridView1_cell_%s_0'%table_order).text
                # title = driver.find_element_by_css_selector('#contents_inner > div > table > tbody > tr:nth-child(%s) > td.mobile > a > div.subject > strong'%(table_order)).text
                str_start_date = driver.find_element_by_css_selector('#gridView1_cell_%s_3'%table_order).text.split(' ')[0]
                str_end_date = driver.find_element_by_css_selector('#gridView1_cell_%s_3'%table_order).text.split(' ')[-1]
                start_date = datetime.strptime(str_start_date, "%Y-%m-%d").date()
                end_date = datetime.strptime(str_end_date, "%Y-%m-%d").date()
                if search_date <= start_date or search_date <= end_date:
                    idict = {
                        "search_date" : str_search_date,
                        "title" : title,
                        "start_date" : str_start_date,
                        "end_date" : str_end_date
                        }
                    idata.append(idict)
            last_date = datetime.strptime(driver.find_element_by_css_selector('#gridView1_cell_9_3').text.split(' ')[0], "%Y-%m-%d").date()
            if search_date > last_date:
                break
        driver.quit()

        if len(idata) > 0:
            text = str(idata)
            _post_message(self, text)
        else:
            text = "유효한 새로운 중소기업 장기근속자 주택 특별공급 없음"
            _post_message(self, text)
        return idata

    def worker(self):
        schedule.every().days.at("09:05").do(self.work)
        schedule.every().days.at("15:00").do(self.work)
        schedule.every().days.at("05:30").do(self.work)
        schedule.every().days.at("22:35").do(self.work)
        try:
            schedule.every().days.at("09:30").do(self._get_priority_house_jungso)
            schedule.every().days.at("22:40").do(self._get_priority_house_jungso)
        except Exception as e:
            print(e)
            pass

        while True:
            now = datetime.now().time()

            weekno = datetime.today().weekday()
            str_date = datetime.strftime(datetime.now().date(), "%Y-%m-%d")

            # 토요일인 경우 이틀 이후 다시 시작
            if weekno == 5:
                future = datetime.combine((datetime.today() + timedelta(days=2)), datetime.strptime("00:00", "%H:%M").time())
                text = "Today is saturday. Next_run_time : %s"%(future)
                print(text)
                _post_message(self, text)
                ot.sleep((future - now).total_seconds())

            # 일요일인 경우 하루 뒤 다시 시작
            if weekno == 6:
                future = datetime.combine((datetime.today() + timedelta(days=1)), datetime.strptime("00:00", "%H:%M").time())
                text = "Today is sunday. Next_run_time : %s"%(future)
                print(text)
                _post_message(self, text)
                ot.sleep((future - now).total_seconds())

            # 평일이지만 공휴일인 경우 
            if weekno < 5 and str_date in self.ko_holiday_list:
                future = datetime.combine((datetime.today()), datetime.strptime("22:30", "%H:%M").time())
                text = "Today is a Korean holiday. Next_run_time : %s"%(future)
                print(text)
                _post_message(self, text)
                ot.sleep((future - now).total_seconds())

            if weekno < 5 and str_date not in self.ko_holiday_list: 
                if now.hour >= 0 and now.hour < 5:
                    until_time = datetime.combine(datetime.today(), datetime.strptime("05:00", "%H:%M").time())
                    text = "Pause %s"%(until_time)
                    print(text)
                    _post_message(self, text)
                    pause.until(until_time)

                if now.hour >= 6 and now.hour < 9:
                    until_time = datetime.combine(datetime.today(), datetime.strptime("09:00", "%H:%M").time())
                    text = "Pause until %s"%(until_time)
                    print(text)
                    _post_message(self, text)
                    pause.until(until_time)

                if now.hour >= 10 and now.hour < 15:
                    until_time = datetime.combine(datetime.today(), datetime.strptime("14:59", "%H:%M").time())
                    text = "Pause until %s"%(until_time)
                    print(text)
                    _post_message(self, text)
                    pause.until(until_time)

                if now.hour >= 16 and now.hour < 22:
                    until_time = datetime.combine(datetime.today(), datetime.strptime("22:32", "%H:%M").time())
                    text = "Pause until %s"%(until_time)
                    print(text)
                    _post_message(self, text)
                    pause.until(until_time)

                if now.hour >= 23:
                    until_time = datetime.combine(datetime.today(), datetime.strptime("24:00", "%H:%M").time())
                    text = "Pause until %s"%(until_time)
                    print(text)
                    _post_message(self, text)
                    pause.until(until_time)

                if (now > time(hour=5, minute=29)) and (now < time(hour=5, minute=31)):
                    text = "Analyzing the SP500. Time. %s"%now
                    print(text)
                    _post_message(self, text)
                    self.market = "sp500"

                if (now > time(hour=9, minute=4)) and (now < time(hour=9, minute=6)):
                    text = "Analyzing the KOSPI. Time. %s"%now
                    print(text)
                    _post_message(self, text)
                    self.market = "kospi"

                if (now > time(hour=14, minute=59)) and (now < time(hour=15, minute=1)):
                    text = "Analyzing the KOSPI. Time. %s"%now
                    print(text)
                    _post_message(self, text)
                    self.market = "kospi"

                if (now > time(hour=22, minute=34)) and (now < time(hour=22, minute=36)):
                    text = "Analyzing the SP500. Time. %s"%now
                    print(text)
                    _post_message(self, text)
                    self.market = "sp500"
                
                schedule.run_pending()    
                ot.sleep(59)

if __name__ == "__main__":
    BaseService().worker()
