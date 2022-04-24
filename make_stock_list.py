# kospi200 회사명 리스트 가져오기
from bs4 import BeautifulSoup
from urllib.request import urlopen

import FinanceDataReader as fdr
import pandas as pd


def _get_stock_list(self):
    print("Do function : _get_stock_list")
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

    return self.market_stock_dict