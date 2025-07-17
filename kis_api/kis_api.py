# dotenv
from dotenv import load_dotenv
import os
import requests
import json
import time
import pandas as pd

load_dotenv()

############################################################

app_key = os.getenv('kis_app_key')
app_secret = os.getenv('kis_app_secret')
url_base = 'https://openapi.koreainvestment.com:9443'  # 실전투자인 경우.

if not app_key or not app_secret:
    print("환경변수 로드 실패: kis_app_key 또는 kis_app_secret이 비어 있습니다.")
else:
    print("환경변수 로드 성공: kis_app_key, kis_app_secret 정상적으로 불러옴")

############################################################

def get_access_token(max_retries=2, retry_delay=60):
    """
    한국투자증권 OpenAPI에서 액세스 토큰을 발급받는 함수입니다.
    분당 1회 제한을 고려하여 실패 시 재시도 로직이 포함되어 있습니다.
    """
    for attempt in range(max_retries + 1):
        try:
            headers = {
                'content-type': 'application/json'
            }

            body = {
                'grant_type': 'client_credentials',
                'appkey': app_key,
                'appsecret': app_secret
            }

            path = 'oauth2/tokenP'
            url = f"{url_base}/{path}"
            res = requests.post(url=url, headers=headers, data=json.dumps(body))
            
            if res.status_code == 200:
                access_token = res.json().get('access_token')
                if access_token:
                    print('액세스 토큰 로드 완료.')
                    return access_token
                else:
                    raise Exception("Error: access_token이 응답에 없습니다.")
            else:
                raise Exception(f"Error: {res.status_code} {res.text}")
                
        except Exception as e:
            if attempt < max_retries:
                attempt_num = attempt + 1
                total_attempts = max_retries + 1
                print(f"토큰 발급 실패 (시도 {attempt_num}/{total_attempts}): {e}")
                print(f"{retry_delay}초 후 재시도합니다...")
                time.sleep(retry_delay)
            else:
                print(f"최대 재시도 횟수 초과. 마지막 오류: {e}")
                raise e

def get_investor_info(stock_code, access_token):
    """
    주식현재가 투자자 정보를 조회하는 함수입니다.
    개인, 외국인, 기관 등 투자 정보를 확인할 수 있습니다.
    """
    headers = {
        'content-type': 'application/json',
        'authorization': f'Bearer {access_token}',
        'appkey': app_key,
        'appsecret': app_secret,
        'tr_id': 'FHKST01010900', # 실전 거래 ID
        'custtype': 'P'  # P: 개인
    }
    
    params = {
        'FID_COND_MRKT_DIV_CODE': 'J',  # J: KRX
        'FID_INPUT_ISCD': stock_code
    }
    
    path = 'uapi/domestic-stock/v1/quotations/inquire-investor'
    url = f"{url_base}/{path}"
    
    res = requests.get(url=url, headers=headers, params=params)
    
    if res.status_code == 200:
        data = res.json()
        if data.get('rt_cd') == '0':
            print(f'종목코드 {stock_code} 투자자 정보 조회 성공')
            return data
        else:
            print(f"API 오류: {data.get('msg1')}")
            return data
    else:
        raise Exception(f"HTTP Error: {res.status_code} {res.text}")

############################################################

# 사용 예시
if __name__ == "__main__":
    try:
        # 1. 액세스 토큰 발급
        token = get_access_token()
        
        # 2. 삼성전자(005930) 투자자 정보 조회
        stock_code = "005930"  # 삼성전자
        investor_data = get_investor_info(stock_code, token)
        
        # 3. DataFrame으로 변환 및 저장
        if investor_data and investor_data.get('output'):
            # output이 리스트 형태임을 가정
            output_list = investor_data['output']
            df = pd.DataFrame(output_list)
            
            # CSV로 저장
            csv_filename = f"{stock_code}_investor_info.csv"
            df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
            print(f"CSV 파일로 저장 완료: {csv_filename}")
            
            # JSON으로 저장
            json_filename = f"{stock_code}_investor_info.json"
            df.to_json(json_filename, orient='records', force_ascii=False, indent=2)
            print(f"JSON 파일로 저장 완료: {json_filename}")
            
            # 주요 정보 출력 (순서지향적으로)
            output = output_list[0]
            print(f"\n=== {stock_code} 투자자 정보 ===")
            print(f"영업일자: {output.get('stck_bsop_date')}")
            print(f"종가: {output.get('stck_clpr')}")
            vrss = output.get('prdy_vrss')
            vrss_sign = output.get('prdy_vrss_sign')
            print(f"전일대비: {vrss} ({vrss_sign})")
            print(f"개인 순매수 수량: {output.get('prsn_ntby_qty')}")
            print(f"외국인 순매수 수량: {output.get('frgn_ntby_qty')}")
            print(f"기관계 순매수 수량: {output.get('orgn_ntby_qty')}")
            print(f"개인 순매수 거래대금: {output.get('prsn_ntby_tr_pbmn')}")
            print(f"외국인 순매수 거래대금: {output.get('frgn_ntby_tr_pbmn')}")
            print(f"기관계 순매수 거래대금: {output.get('orgn_ntby_tr_pbmn')}")
        else:
            print("투자자 정보가 없습니다.")
        
    except Exception as e:
        print(f"오류 발생: {e}")
