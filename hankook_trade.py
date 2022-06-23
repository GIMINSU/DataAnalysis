# requests 모듈 설치 필요 (pip install requests)
import requests

url = 'https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/trading/inquire-psbl-order'
params = {
    "CANO": "종합계좌번호",
    "ACNT_PRDT_CD": "계좌상품코드",
    "ACNT_PWD": "계좌비밀번호",
    "PDNO": "상품번호",
    "ORD_UNPR": "주문단가",
    "ORD_DVSN": "주문구분",
    "CMA_EVLU_AMT_ICLD_YN": "CMA평가금액포함여부",
    "OVRS_ICLD_YN": "해외포함여부"
}
headers = {
    "Content-Type": "application/json",
    "authorization": "Bearer {TOKEN}",
    "appKey": "{Client_ID}",
    "appSecret": "{Client_Secret}",
    "personalSeckey": "{personalSeckey}",
    "tr_id": "TTTC8908R",
    "tr_cont": " ",
    "custtype": "법인(B), 개인(P)",
    "seq_no": "법인(01), 개인( )",
    "mac_address": "{Mac_address}",
    "phone_num": "P01011112222",
    "ip_addr": "{IP_addr}",
    "hashkey": "{Hash값}",
    "gt_uid": "{Global UID}"
}

res = requests.get(url, params=params, headers=headers)
rescode = res.status_code
if rescode == 200:
    print(res.headers)
    print(str(rescode) + " | " + res.text)
else:
    print("Error Code : " + str(rescode) + " | " + res.text)