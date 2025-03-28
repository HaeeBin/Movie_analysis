import requests
import pandas as pd
from google.cloud import storage
from datetime import datetime, timedelta
import os   # airflow task에 넣을게 아니기 때문에 .env에 환경변수 설정
from dotenv import load_dotenv

load_dotenv()

GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
BUCKET_NAME = os.getenv('BUCKET_NAME')
DAILY_BOXOFFICE_FOLDER = 'daily_boxoffice'
BOXOFFICE_API_KEY = os.getenv('BOXOFFICE_API_KEY')

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_APPLICATION_CREDENTIALS

def get_date():
    '''
    API로 어제 날짜의 데이터까지만 수집 가능하므로 어제 날짜일자를 구하는 함수입니다.
    '''
    return (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")
    
def request_daily_api(target_date):
    '''
    발급받은 key와 조회하고자 하는 날짜 등 정보를 입력하여 요청해 일별 박스오피스 데이터를 가져오는 함수입니다.
    '''
    
    base_url = 'http://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json'
    params = {
        "key" : BOXOFFICE_API_KEY,
        "targetDt" : target_date,              # 조회하고자 하는 날짜
        # "multiMovieYn" : "N",             # 다양성 영화 : Y, 상업 영화 : N (default: 전체)
        # "repNationCd" : "K",              # 한국 영화 : K, 외국 영화 : F (default: 전체)
        # "wideAreaCd" : "0105000000"       # 지역코드 (공통코드 조회 서비스에서 10자리 숫자로 조회된 지역코드)
    }
    
    response = requests.get(base_url, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"API 요청 실패: {response.status_code}")

def parse_daily_boxoffice_data(data):
    '''
    가져온 박스오피스 데이터 중 필요한 정보들만 모아서 Dataframe으로 변환하는 함수입니다.
    '''     
    box_office_list = data['boxOfficeResult']['dailyBoxOfficeList']
    
    df = pd.DataFrame(box_office_list)
    
    df = df[[
        "rank", "rankInten", "rankOldAndNew",
        "movieCd", "movieNm", "openDt",
        "salesAmt", "salesShare", "salesInten", "salesChange", "salesAcc",
        "audiCnt", "audiInten", "audiChange", "audiAcc",
        "scrnCnt", "showCnt"
    ]]
    
    return df

def upload_to_gcs(data, target_date):
    '''
    DataFrame을 csv로 변환하여 Google Cloud Storage에 업로드하는 함수입니다.
    '''
    #target_date = get_date()
    
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    
    # gcs 파일 경로 설정
    gcs_file_path = f"{DAILY_BOXOFFICE_FOLDER}/daily_box_office_{target_date}.csv"
    blob = bucket.blob(gcs_file_path)
    
    # 데이터 받아서 parse_dataframe(data)함수로 변환
    df = parse_daily_boxoffice_data(data)
    
    # dataframe을 csv 변환 후 gcs 업로드
    csv_data = df.to_csv(index=False, encoding='utf-8-sig')
    blob.upload_from_string(csv_data, content_type="text/csv")
    
    print(f"daily boxOffice 업로드 완료. 날짜 : {target_date}")

if __name__=='__main__':
    
    for i in range(20250201, 20250229):
        data = request_daily_api(i)
        upload_to_gcs(data, i)
        