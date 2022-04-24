from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By

# pip install webdriver-manager  ## 항상 최신 버전의 chromedriver를 자동으로 사용
from webdriver_manager.chrome import ChromeDriverManager
from slack_message import _post_message
from slack_sdk_message_post import _post_message_with_slack_sdk

def _get_priority_house_jungso(self):

    idata = []
    search_date = datetime.today().date()
    str_search_date = datetime.strftime(search_date, "%Y-%m-%d")
    try:
        driver = webdriver.Chrome(ChromeDriverManager().install())
        url = "https://www.smes.go.kr/sanhakin/websquare/wq_main.do"
        # url = "https://www.mss.go.kr/site/gyeonggi/ex/bbs/List.do?cbIdx=323"
        driver.get(url)
        driver.implicitly_wait(40)
        driver.find_element(By.XPATH, '//*[@id="genTopMenu_2_liTopMenu"]').click()
        driver.implicitly_wait(40)
        driver.find_element(By.XPATH, '//*[@id="genLeftMenu_3_leftMenuGrp"]').click()
        driver.implicitly_wait(40)

        for page in range(1, 3):
            # print(page)
            driver.find_element(By.XPATH, '//*[@id="pagelist1_page_%s"]'%page).click()
            driver.implicitly_wait(40)
            for table_order in range (0, 10):
                title = driver.find_element_by_css_selector('#gridView1_cell_%s_0'%table_order).text
                # title = driver.find_element_by_css_selector('#contents_inner > div > table > tbody > tr:nth-child(%s) > td.mobile > a > div.subject > strong'%(table_order)).text
                str_start_date = driver.find_element_by_css_selector('#gridView1_cell_%s_3'%table_order).text.split(' ')[0]
                str_end_date = driver.find_element_by_css_selector('#gridView1_cell_%s_3'%table_order).text.split(' ')[-1]
                start_date = datetime.strptime(str_start_date, "%Y-%m-%d").date()
                end_date = datetime.strptime(str_end_date, "%Y-%m-%d").date()
                if search_date <= start_date or search_date <= end_date:
                    idict = {
                            "type" : "section",
                            "text" : {
                                "type" : "mrkdwn",
                                "text": f"*조회일* : {str_search_date}\n*제목* : {title}\n*신청시작일* : {str_start_date}\n*신청종료일* : {str_end_date}"
                            }
                        }
                    idata.append(idict)
            last_date = datetime.strptime(driver.find_element_by_css_selector('#gridView1_cell_9_3').text.split(' ')[0], "%Y-%m-%d").date()
            if search_date > last_date:
                break
        driver.quit()

    except Exception as e:
        _post_message(self, text = e)
        pass

    if len(idata) > 0:
        _post_message_with_slack_sdk(self, blocks=idata)
    else:
        text = "유효한 새로운 중소기업 장기근속자 주택 특별공급 없음"
        _post_message(self, text = text)
    return idata