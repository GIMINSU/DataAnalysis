
import os
import sys
from os import path

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
        self.start_date = (datetime.now() - timedelta(days=40)).date().strftime("%Y-%m-%d")

    def scheduler(self):
        today_date = datetime.now().date().strftime("%Y-%m-%d")
        try:
            holiday_list = _get_holiday(self)
        except Exception as e:
            print(e)
            holiday_list = []

        if today_date not in holiday_list:
            try:
                self._get_stock_list()

                self._post_signal_to_slack()
            except Exception as e:
                self._post_message(e)
                pass

        else:
            try:
                self._get_stock_list()
                self._post_signal_to_slack()
            except Exception as e:
                self._post_message(e)
                pass
    
    def _get_stock_list(self):
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
            df = df.reset_index()
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
            
            r_dict = {
                "symbol" : df.iloc[-1]["Symbol"],
                "date" : df.iloc[-1]["Date"],
                "trade_signal" : df.iloc[-1]["trade"],
                "price" : df.iloc[-1]["Close"]
            }
            return r_dict
            
        except Exception as e:
            # self._post_message(e)
            print(e)
            pass

    def _post_signal_to_slack(self):
        df = pd.DataFrame()
        buy_list = []
        sell_list = []
        idata= []

        if self.market == "kospi":
            for symbol in self.market_stock_dict[self.market]:
                try:
                    df = fdr.DataReader(symbol, start=self.start_date)
                    df["Symbol"] = symbol

                    r_dict = self._make_signal(df)

                    if r_dict["trade_signal"] == "buy":
                        buy_list.append(self.market_dict.get(r_dict["symbol"]))
                        print(r_dict)
                        idata.append(r_dict)
                    elif r_dict["trade_signal"] == "sell":
                        sell_list.append(self.market_dict.get(r_dict["symbol"]))
                        print(r_dict)
                        idata.append(r_dict)
                except Exception as e:
                    # self._post_message(e)
                    print(e)
                    pass

        elif self.market == "sp500":
            for symbol in self.market_stock_dict[self.market]:
                try:
                    df = yf.download(symbol, start=self.start_date, show_errors=False)
                    df["Symbol"] = symbol
                    
                    r_dict = self._make_signal(df)

                    if r_dict["trade_signal"] == "buy":

                        buy_list.append(r_dict["symbol"])
                        print(r_dict)
                        idata.append(r_dict)
                    elif r_dict["trade_signal"] == "sell":
                        sell_list.append(r_dict["symbol"])
                        print(r_dict)
                        idata.append(r_dict)

                except Exception as e:
                    # self._post_message(e)
                    print(e)
                    pass
            
        # 구매 데이터
        if len(buy_list) == 0:
            self._post_message("Today not exists buy stocks")
        if len(buy_list) > 0:
            self._post_message("Buy_stocks : %s"%(buy_list))

        # 판매 데이터
        if len(sell_list) == 0:
            self._post_message("Today not exists sell stocks")
        if len(sell_list) > 0:
            self._post_message("Sell_stocks : %s"%(sell_list))
        
        try:
            df = pd.DataFrame.from_dict(idata)
        except Exception as e:
            self._post_message(e)
            pass

        return df

    def work(self):
        schedule.every().days.at("15:30").do(self.scheduler)
        schedule.every().days.at("06:30").do(self.scheduler)
        while True:        
            
            print("Ready %s"%(datetime.now()))
            now = datetime.now().time()
            if (now > time(hour=15, minute=29)) and (now < time(hour=15, minute=31)):
                text = "Analyzing the KOSPI. Time. %s"%now
                _post_message(self, text)
                print(text)
                self.market = "kospi"       
            if (now > time(hour=6, minute=29)) and (now < time(hour=6, minute=31)):
                text = "Analyzing the SP500. Time. %s"%now
                _post_message(self, text)
                print(text)
                self.market = "sp500"
            schedule.run_pending()    
            ot.sleep(59)

            # print("Run %s"%datetime.now())
            # now = datetime.now().time()
            # if (now > time(hour=6, minute=00)) and (now < time(hour=7, minute=00)):
            #     self.market = "sp500"
            #     try:
            #         t1 = Thread(target=self.scheduler)
            #         t1.start()
            #         t1.join()
            #     except Exception as e:
            #         print(e)
            # elif (now > time(hour=14, minute=30)) and (now < time(hour=15, minute=30)):
            #     self.market = "kospi"
            #     try:
            #         t2 = Thread(target=self.scheduler)
            #         t2.start()
            #         t2.join()
            #     except Exception as e:
            #         print(e)
            # else:
            #     self._post_message("Run Success. But This is not the time for results.")
            

if __name__ == "__main__":
    BaseService().work()
