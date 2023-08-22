from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
import pandas as pd

class SeoulPoplulationOperator(BaseOperator):
    template_fields = ('endpoint', 'path', 'file_name', 'base_dt')
    
    def __init__(self, dataset_nm, path, file_name, base_dt=None, **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = 'openapi.seoul.go.kr'
        self.path = path
        self.file_name = file_name
        self.endpoint = '{{var.value.apikey_openapi_seoul_go_kr}}/json/' + dataset_nm
        self.base_dt = base_dt
    
    
    def execute(self, coontext):
        import os
        # url:8080/{api_key}/json/dataset_nm/start/end/poi_num
        connection = BaseHook.get_connection(self.http_conn_id)
        self.base_url = f'http://{connection.host}:{connection.port}/{self.endpoint}'
        
        total_row_df = pd.DataFrame()
        
        nums = []
        def generate_number_string(number):
            if 1 <= number <= 9:
                return f"{number:03}"
            elif 10 <= number <= 99:
                return f"{number:03}"
            elif 100 <= number <= 113:
                return f"{number:03}"
            else:
                return "범위를 벗어난 숫자입니다."
        for num in range(1, 114):
            nums.append(generate_number_string(num))
            
        start_row = 1
        end_row = 1000
        
        for poi_number in nums:
            while True:
                self.log.info(f'Poi_num번호: {poi_number}')
                self.log.info(f'시작: {start_row}')
                self.log.info(f'끝: {end_row}')
                row_df = self.call_api(self.base_url, start_row=start_row, end_row=end_row, poi_number=poi_number)
                total_row_df = pd.concat([total_row_df, row_df])
                
                if len(total_row_df) < 1000:
                    break
                else:
                    start_row = end_row + 1
                    end_row += 1000
        if not os.path.exists(self.path):
            os.system(f'mkdir -p {self.path}')
        total_row_df.to_csv(self.path+'/'+self.file_name, index=False)
                
        
        # for poi_number in nums:
            
        
    
    def call_api(self, base_url, start_row, end_row, poi_number):
        import requests
        import json
        
        headers = {
            'Content-Type': 'application/json',
            'charset': 'utf-8',
            'Accept': '*/*'
        }
        request_url = f'{base_url}/{start_row}/{end_row}/POI{poi_number}'
        if self.base_dt is not None:
            request_url = f'{base_url}/{start_row}/{end_row}/{self.base_dt}/POI{poi_number}'
        response = requests.get(request_url, headers=headers)
        contents = json.loads(response.text)
        record = contents["SeoulRtd.citydata_ppltn"][0]
        main_data = record.copy()
        main_data.pop('FCST_PPLTN')
        # 데이터가 중복됨.
        #main_df = pd.DataFrame([main_data])
        
        fcst_ppltn_list = record['FCST_PPLTN']
        fcst_data_list = []

        for item in fcst_ppltn_list:
            fcst_data = {
                'AREA_NM': record['AREA_NM'],
                'FCST_TIME': item['FCST_TIME'],
                'FCST_CONGEST_LVL' : item['FCST_CONGEST_LVL'],
                'FCST_PPLTN_MIN' : item['FCST_PPLTN_MIN'],
                'FCST_PPLTN_MAX' : item['FCST_PPLTN_MAX']
            }
            fcst_data_list.append(fcst_data)
            
        fcst_df = pd.DataFrame(fcst_data_list)
        return fcst_df