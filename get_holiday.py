# 휴장일 데이터 가져오기
import requests
from bs4 import BeautifulSoup
from urllib.request import urlopen

def _get_holiday(self):
    loc_date = []

    if self.market == "kospi":
        try:
            # 1월 ~ 12월
            for i in range(1, 13):
                if i >= 10:
                    month = str(i)
                elif i < 10:
                    month = "0" + str(i)

                params = {"serviceKey":self.mykey, "solYear":self.year, "solMonth":month}
                response = requests.get(self.request_url, params=params)

                if response.ok:
                    soup = BeautifulSoup(response.text, "html.parser")
                    item = soup.find_all("item")
                    for i in item:
                        date = i.find("locdate").get_text()
                        loc_date.append(date[:4] + "-" + date[4:6] + "_" + date[6:])
                else:
                    print("공공데이터에서 공휴일 데이터 가져오기 실패.")
                    pass
            
            return loc_date

        except Exception as e:
            print(e)
    elif self.market == "sp500":
        return loc_date