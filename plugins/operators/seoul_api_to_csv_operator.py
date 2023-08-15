from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
import pandas as pd

class SeoulApiToCsvOperator(BaseOperator):
    template_fields = ('endpoint', 'path', 'file_name', 'base_dt')
    
    # 1) 생성자 overriding
    def __init__(self, dataset_nm, path, file_name, base_dt=None, **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = 'openapi.seoul.go.kr'
        self.path = path
        self.file_name = file_name
        self.endpoint = '{{var.value.apikey_openapi_seoul_go_kr}}/json/' + dataset_nm
        self.base_dt = base_dt
    
    # 2) execute overriding (비즈니스 로직 작성)
    def execute(self, context):
        import os
        # url:8080/{api_key}/xml/dataset_nm/start/end/poi_num
        connection = BaseHook.get_connection(self.http_conn_id)
        self.base_url = f'http://{connection.host}:{connection.port}/{self.endpoint}'
        
        total_row_df = pd.DataFrame()
        start_row = 1
        end_row = 1000
        
        while True:
            self.log.info(f'시작: {start_row}')
            self.log.info(f'끝: {end_row}')
            row_df = self.call_api(self.base_url, start_row, end_row)
            total_row_df = pd.concat([total_row_df, row_df])
            
            if len(total_row_df) < 1000:
                break
            else:
                start_row = end_row + 1
                end_row += 1000
                
        # directory가 있는지 검사
        if not os.path.exists(self.path):
            os.system(f'mkdir -p {self.path}')
        total_row_df.to_csv(self.path+'/'+self.file_name, index=False)
    
    def call_api(self, base_url, start_row, end_row):
        import requests
        import json
        
        headers = {
            'Content-Type': 'application/json',
            'charset': 'utf-8',
            'Accept': '*/*'
            }
        request_url = f'{base_url}/{start_row}/{end_row}/'
        if self.base_dt is not None:
            request_url = f'{base_url}/{start_row}/{end_row}/{self.base_dt}'
        response = requests.get(request_url, headers=headers)
        contents = json.loads(response.text)
        key_nm = list(contents.key())[0]
        row_data = contents.get(key_nm).get('row')
        row_df = pd.DataFrame(row_data)
        return row_df
        
        
        
        
        # # 충전소, 충전기 정보
        # station_data, charger_data = [], []
        # # charger_stts가 없는 xml 구조도 있음
        # try:
        #     area_info = area_info = xml_obj['SeoulRtd.citydata']['CITYDATA']['AREA_NM'] # 가산디지털단지역
        #     area_congest_lvl = xml_obj['SeoulRtd.citydata']['CITYDATA']['LIVE_PPLTN_STTS']['LIVE_PPLTN_STTS']['AREA_CONGEST_LVL']
        #     data_update_time = xml_obj['SeoulRtd.citydata']['CITYDATA']['LIVE_PPLTN_STTS']['LIVE_PPLTN_STTS']['PPLTN_TIME']
            
        #     #charger_stations = xml_obj['SeoulRtd.citydata']['CITYDATA']['CHARGER_STTS']['CHARGER_STTS']
        #     charger_stations = xml_obj['SeoulRtd.citydata']['CITYDATA'].get('CHARGER_STTS', [])
        #     if charger_stations != None:
        #         for station in charger_stations['CHARGER_STTS']:
        #             #print(station)
        #             if isinstance(station, dict):
        #                 try:
        #                     station_info = {
        #                         'AREA_INFO': area_info,
        #                         'AREA_CONGEST_LVL': area_congest_lvl,
        #                         'DATA_UPDATE_TIME': data_update_time,
        #                         'STAT_NM': station.get('STAT_NM'),
        #                         'STAT_ID': station.get('STAT_ID'),  # 기본키
        #                         'STAT_ADDR': station.get('STAT_ADDR'),
        #                         'STAT_X': station.get('STAT_X'),
        #                         'STAT_Y': station.get('STAT_Y'),
        #                         'STAT_USETIME': station.get('STAT_USETIME'),
        #                         'STAT_PARKPAY': station.get('STAT_PARKPAY'),
        #                         'STAT_LIMITYN': station.get('STAT_LIMITYN'),
        #                         'STAT_LIMITDETAIL': station.get('STAT_LIMITDETAIL'),
        #                         'STAT_KINDDETAIL': station.get('STAT_KINDDETAIL')
        #                     }
        #                     station_data.append(station_info)
        #                 except TypeError:
        #                     print("충전소 정보가 없습니다.")
        #                     continue

        #                 charger_details = station.get('CHARGER_DETAIL', {}).get('CHARGER_DETAIL', [])
        #                 if isinstance(charger_details, list):
        #                     for charger_detail in charger_details:
        #                         try:
        #                             charger_info = {
        #                                 'STAT_ID': station.get('STAT_ID'),  # 외래키면서 기본키
        #                                 'CHARGER_ID': charger_detail.get('CHARGER_ID'),
        #                                 'CHARGER_TYPE': charger_detail.get('CHARGER_TYPE'),
        #                                 'CHARGER_STAT': charger_detail.get('CHARGER_STAT'),
        #                                 'STATUPDDT': charger_detail.get('STATUPDDT'),
        #                                 'LASTTSDT': charger_detail.get('LASTTSDT'),
        #                                 'LASTTEDT': charger_detail.get('LASTTEDT'),
        #                                 'NOWTSDT': charger_detail.get('NOWTSDT'),
        #                                 'OUTPUT': charger_detail.get('OUTPUT'),
        #                                 'METHOD': charger_detail.get('METHOD')
        #                             }
        #                             charger_data.append(charger_info)
        #                         except TypeError:
        #                             print("충전기 정보가 없습니다.")
        #     else:
        #         print("CHARGER_STTS 데이터가 없습니다.")
                
        # except KeyError:
        #     print("CHARGER_STTS 데이터가 없습니다")
        # charger_stations = []
        
        # df1 = pd.DataFrame(station_data)
        # df2 = pd.DataFrame(charger_data)
        
        # row_df = pd.merge(df1, df2, on='STAT_ID', how='inner')
        # return row_df
    
    