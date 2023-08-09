import requests
from bs4 import BeautifulSoup
from pprint import pprint
import json
import xmltodict
import pandas as pd
import secret


KEY = secret.get_api_key()

startnum = 1
endnum = 3
# 광화문 덕수궁 뿐만 아니라, 다른 데이터도 가지고 올 수 있도록 수정하자.
url = 'http://openapi.seoul.go.kr:8088/61626573486b797538384970536178/xml/citydata/1/2/광화문·덕수궁'
# params = {
#     'KEY': "61626573486b797538384970536178",
#     'TYPE': "xml:xml",
#     'SERVICE': "citydata",
#     'START_INDEX': startnum,
#     'END_INDEX': endnum,
#     'AREA_NM': "광화문·덕수궁"   # gasan digital
# }
res = requests.get(url=url)
content = res.text
xml_obj = xmltodict.parse(content)

"""
CHARGER_STTS : 전기차충전소 현황
STAT_NM : 전기차충전소명
STAT_ID: 전기차충전소ID
STAT_ADDR: 전기차충전소주소
STAT_X: 전기차충전소X좌표(경도)
STAT_Y: 전기차충전소Y좌표(위도)
STAT_USETIME: 전기차충전소 운영시간
STAT_PARKPAY: 전기차충전소 주차료 유무료 여부
STAT_LIMITYN: 전기차충전소 이용자 제한
STAT_LIMITDETAIL	전기차충전소 이용제한 사유
STAT_KINDDETAIL	전기차충전소 상세유형
--------------------------------------------
CHARGER_ID	충전기 ID
CHARGER_TYPE	충전기 타입
CHARGER_STAT	충전기 상태
STATUPDDT	충전기 상태 갱신일시
LASTTSDT	충전기 마지막 충전시작일시
LASTTEDT	충전기 마지막 충전종료일시
NOWTSDT	충전기 충전중 시작일시
OUTPUT	충전기 충전용량
METHOD	충전기 충전방식
"""
charger_stations = xml_obj['SeoulRtd.citydata']['CITYDATA']['CHARGER_STTS']['CHARGER_STTS']

# 충전소 , 충전기 정보를 저장할 데이터프레임 생성
station_data = []
charger_data = []

for station in charger_stations:
    station_info = {
        'STAT_NM': station['STAT_NM'],
        'STAT_ID': station['STAT_ID'],
        'STAT_ADDR': station['STAT_ADDR'],
        'STAT_X': station['STAT_X'],
        'STAT_Y': station['STAT_Y'],
        'STAT_USETIME': station['STAT_USETIME'],
        'STAT_PARKPAY': station['STAT_PARKPAY'],
        'STAT_LIMITYN': station['STAT_LIMITYN'],
        'STAT_LIMITDETAIL': station['STAT_LIMITDETAIL'],
        'STAT_KINDDETAIL': station['STAT_KINDDETAIL']
    }
    station_data.append(station_info)

    charger_details = station['CHARGER_DETAIL']['CHARGER_DETAIL']
    for charger_detail in charger_details:
        try:
            charger_info = {
                'STAT_ID': station['STAT_ID'],  # 연결 정보 추가
                'CHARGER_ID': charger_detail['CHARGER_ID'],
                'CHARGER_TYPE': charger_detail['CHARGER_TYPE'],
                'CHARGER_STAT': charger_detail['CHARGER_STAT'],
                'STATUPDDT': charger_detail['STATUPDDT'],
                'LASTTSDT': charger_detail['LASTTSDT'],
                'LASTTEDT': charger_detail['LASTTEDT'],
                'NOWTSDT': charger_detail['NOWTSDT'],
                'OUTPUT': charger_detail['OUTPUT'],
                'METHOD': charger_detail['METHOD']
            }
            charger_data.append(charger_info)
            
            
            
            
            
            # print("CHARGER_ID:", charger_detail['CHARGER_ID'])
            # print("CHARGER_TYPE:", charger_detail['CHARGER_TYPE'])
            # print("CHARGER_STAT:", charger_detail['CHARGER_STAT'])
            # print("STATUPDDT:", charger_detail['STATUPDDT'])
            # print("LASTTSDT:", charger_detail['LASTTSDT'])
            # print("LASTTEDT:", charger_detail['LASTTEDT'])
            # print("NOWTSDT:", charger_detail['NOWTSDT'])
            # print("OUTPUT:", charger_detail['OUTPUT'])
            # print("METHOD:", charger_detail['METHOD'])
            # print("-" * 40)
        except  TypeError: # 제대로 정보를 받아오지 못하는 경우(정보 누락 발생)
            print("충전기 정보가 없습니다.")
            continue
        
        
station_df = pd.DataFrame(station_data)
charger_df = pd.DataFrame(charger_data)

# 데이터프레임을 CSV 파일로 저장
station_df.to_csv('station_info.csv', index=False)
charger_df.to_csv('charger_info.csv', index=False)