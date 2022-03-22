# 휴장일 데이터 가져오기
import requests
from bs4 import BeautifulSoup
from urllib.request import urlopen

def _get_korea_holiday(self):
    
    loc_date = []
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
    
    print("public_holiday_list_count : ", len(loc_date))

    krx_holiday_list = ["2022-01-31",
    "2022-02-01",
    "2022-02-02",
    "2022-03-01",
    "2022-03-09",
    "2022-05-05",
    "2022-06-01",
    "2022-06-06",
    "2022-08-15",
    "2022-09-09",
    "2022-09-12",
    "2022-10-03",
    "2022-10-10",
    "2022-12-30"]
    print("krx_holiday_list_count : ", len(krx_holiday_list))

    loc_date.extend(krx_holiday_list)
    print("finally_return_holiday_list_count : ", len(loc_date))

    return loc_date