# dotenv
from dotenv import load_dotenv
import os
import requests
import json
import time
from datetime import datetime

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

# Element 한글명 매핑 (API 문서의 정확한 한글명 사용)
ELEMENT_KOREAN_MAPPING = {
    # Response Body 필드
    'rt_cd': '성공실패여부',
    'msg_cd': '응답코드',
    'msg1': '응답메세지',
    'output': '응답상세',
    
    # 일자별 정보 필드
    'stck_bsop_date': '주식 영업 일자',
    'stck_oprc': '주식 시가',
    'stck_hgpr': '주식 최고가',
    'stck_lwpr': '주식 최저가',
    'stck_clpr': '주식 종가',
    'acml_vol': '누적 거래량',
    'prdy_vrss_vol_rate': '전일 대비 거래량 비율',
    'prdy_vrss': '전일 대비',
    'prdy_vrss_sign': '전일 대비 부호',
    'prdy_ctrt': '전일 대비율',
    'hts_frgn_ehrt': 'HTS 외국인 소진율',
    'frgn_ntby_qty': '외국인 순매수 수량',
    'flng_cls_code': '락 구분 코드',
    'acml_prtt_rate': '누적 분할 비율',
    
    # 투자자 정보 필드
    'prsn_ntby_qty': '개인 순매수 수량',
    'orgn_ntby_qty': '기관계 순매수 수량',
    'prsn_ntby_tr_pbmn': '개인 순매수 거래 대금',
    'frgn_ntby_tr_pbmn': '외국인 순매수 거래 대금',
    'orgn_ntby_tr_pbmn': '기관계 순매수 거래 대금',
    'prsn_shnu_vol': '개인 매수2 거래량',
    'frgn_shnu_vol': '외국인 매수2 거래량',
    'orgn_shnu_vol': '기관계 매수2 거래량',
    'prsn_shnu_tr_pbmn': '개인 매수2 거래 대금',
    'frgn_shnu_tr_pbmn': '외국인 매수2 거래 대금',
    'orgn_shnu_tr_pbmn': '기관계 매수2 거래 대금',
    'prsn_seln_vol': '개인 매도 거래량',
    'frgn_seln_vol': '외국인 매도 거래량',
    'orgn_seln_vol': '기관계 매도 거래량',
    'prsn_seln_tr_pbmn': '개인 매도 거래 대금',
    'frgn_seln_tr_pbmn': '외국인 매도 거래 대금',
    'orgn_seln_tr_pbmn': '기관계 매도 거래 대금'
}


def convert_to_korean_keys(data):
    """
    API 응답 데이터의 키를 한글명으로 변환하는 함수입니다.
    
    Args:
        data: 변환할 데이터 (dict 또는 list)
    
    Returns:
        변환된 데이터
    """
    if isinstance(data, dict):
        converted = {}
        for key, value in data.items():
            # 키 변환 (한글명이 있으면 변환, 없으면 원래 키 사용)
            korean_key = ELEMENT_KOREAN_MAPPING.get(key, key)
            
            # 값이 dict나 list면 재귀적으로 변환
            if isinstance(value, (dict, list)):
                converted[korean_key] = convert_to_korean_keys(value)
            else:
                converted[korean_key] = value
        return converted
    
    elif isinstance(data, list):
        return [convert_to_korean_keys(item) for item in data]
    
    else:
        return data


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
            res = requests.post(url=url, headers=headers, 
                               data=json.dumps(body))
            
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
        'tr_id': 'FHKST01010900',  # 실전 거래 ID
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


def get_daily_price(stock_code, access_token, period='D', adj_price='1'):
    """
    주식현재가 일자별 정보를 조회하는 함수입니다.
    일/주/월별 주가를 확인할 수 있으며 최근 30일(주,월)로 제한되어 있습니다.
    
    Args:
        stock_code (str): 종목코드 (ex: 005930)
        access_token (str): 액세스 토큰
        period (str): 기간 분류 코드 ('D': 일, 'W': 주, 'M': 월)
        adj_price (str): 수정주가 반영 여부 ('0': 미반영, '1': 반영)
    """
    headers = {
        'content-type': 'application/json',
        'authorization': f'Bearer {access_token}',
        'appkey': app_key,
        'appsecret': app_secret,
        'tr_id': 'FHKST01010400',  # 주식현재가 일자별 TR ID
        'custtype': 'P'  # P: 개인
    }
    
    params = {
        'FID_COND_MRKT_DIV_CODE': 'J',  # J: KRX
        'FID_INPUT_ISCD': stock_code,
        'FID_PERIOD_DIV_CODE': period,
        'FID_ORG_ADJ_PRC': adj_price
    }
    
    path = 'uapi/domestic-stock/v1/quotations/inquire-daily-price'
    url = f"{url_base}/{path}"
    
    res = requests.get(url=url, headers=headers, params=params)
    
    if res.status_code == 200:
        data = res.json()
        if data.get('rt_cd') == '0':
            print(f'종목코드 {stock_code} 일자별 정보 조회 성공')
            return data
        else:
            print(f"API 오류 (종목 {stock_code}): {data.get('msg1')}")
            return data
    else:
        raise Exception(f"HTTP Error: {res.status_code} {res.text}")


def get_multiple_daily_prices(stock_codes, access_token, period='D', 
                              adj_price='1', delay=0.2):
    """
    복수 종목의 일자별 정보를 조회하는 함수입니다.
    
    Args:
        stock_codes (list): 종목코드 리스트
        access_token (str): 액세스 토큰
        period (str): 기간 분류 코드 ('D': 일, 'W': 주, 'M': 월)
        adj_price (str): 수정주가 반영 여부 ('0': 미반영, '1': 반영)
        delay (float): API 호출 간 지연시간 (초)
    """
    results = {}
    
    for i, stock_code in enumerate(stock_codes):
        try:
            print(f"[{i+1}/{len(stock_codes)}] {stock_code} 조회 중...")
            data = get_daily_price(stock_code, access_token, period, adj_price)
            results[stock_code] = data
            
            # API 호출 제한을 고려한 지연
            if i < len(stock_codes) - 1:  # 마지막이 아닌 경우에만 지연
                time.sleep(delay)
                
        except Exception as e:
            print(f"종목 {stock_code} 조회 실패: {e}")
            results[stock_code] = {"error": str(e)}
    
    return results


def calculate_investor_ratios(investor_data):
    """
    투자자 정보 데이터에서 비율을 계산하는 함수입니다.
    
    Args:
        investor_data: 투자자 정보 API 응답 데이터
    
    Returns:
        비율이 계산된 데이터
    """
    if not investor_data or not investor_data.get('응답상세'):
        return None
    
    output_data = investor_data['응답상세'][0]  # 첫 번째 데이터만 사용
    
    # 절댓값으로 변환하여 계산
    def safe_abs_float(value):
        try:
            return abs(float(value))
        except (ValueError, TypeError):
            return 0.0
    
    # 순매수 데이터
    prsn_ntby = safe_abs_float(output_data.get('개인 순매수 수량', 0))
    frgn_ntby = safe_abs_float(output_data.get('외국인 순매수 수량', 0))
    orgn_ntby = safe_abs_float(output_data.get('기관계 순매수 수량', 0))
    total_ntby_qty = prsn_ntby + frgn_ntby + orgn_ntby
    
    prsn_ntby_tr = safe_abs_float(output_data.get('개인 순매수 거래 대금', 0))
    frgn_ntby_tr = safe_abs_float(output_data.get('외국인 순매수 거래 대금', 0))
    orgn_ntby_tr = safe_abs_float(output_data.get('기관계 순매수 거래 대금', 0))
    total_ntby_tr = prsn_ntby_tr + frgn_ntby_tr + orgn_ntby_tr
    
    # 매수2 데이터
    prsn_shnu = safe_abs_float(output_data.get('개인 매수2 거래량', 0))
    frgn_shnu = safe_abs_float(output_data.get('외국인 매수2 거래량', 0))
    orgn_shnu = safe_abs_float(output_data.get('기관계 매수2 거래량', 0))
    total_shnu_vol = prsn_shnu + frgn_shnu + orgn_shnu
    
    prsn_shnu_tr = safe_abs_float(output_data.get('개인 매수2 거래 대금', 0))
    frgn_shnu_tr = safe_abs_float(output_data.get('외국인 매수2 거래 대금', 0))
    orgn_shnu_tr = safe_abs_float(output_data.get('기관계 매수2 거래 대금', 0))
    total_shnu_tr = prsn_shnu_tr + frgn_shnu_tr + orgn_shnu_tr
    
    # 매도 데이터
    prsn_seln = safe_abs_float(output_data.get('개인 매도 거래량', 0))
    frgn_seln = safe_abs_float(output_data.get('외국인 매도 거래량', 0))
    orgn_seln = safe_abs_float(output_data.get('기관계 매도 거래량', 0))
    total_seln_vol = prsn_seln + frgn_seln + orgn_seln
    
    prsn_seln_tr = safe_abs_float(output_data.get('개인 매도 거래 대금', 0))
    frgn_seln_tr = safe_abs_float(output_data.get('외국인 매도 거래 대금', 0))
    orgn_seln_tr = safe_abs_float(output_data.get('기관계 매도 거래 대금', 0))
    total_seln_tr = prsn_seln_tr + frgn_seln_tr + orgn_seln_tr
    
    # 비율 계산 (0으로 나누기 방지)
    def safe_ratio(numerator, denominator):
        return round((numerator / denominator * 100), 2) if denominator > 0 else 0.0
    
    # 비율 데이터 생성
    ratio_data = {
        '주식 영업 일자': output_data.get('주식 영업 일자', ''),
        '주식 종가': output_data.get('주식 종가', ''),
        '전일 대비': output_data.get('전일 대비', ''),
        '전일 대비 부호': output_data.get('전일 대비 부호', ''),
        
        # 순매수 비율
        '개인 순매수 수량 비율(%)': safe_ratio(prsn_ntby, total_ntby_qty),
        '외국인 순매수 수량 비율(%)': safe_ratio(frgn_ntby, total_ntby_qty),
        '기관계 순매수 수량 비율(%)': safe_ratio(orgn_ntby, total_ntby_qty),
        
        '개인 순매수 거래 대금 비율(%)': safe_ratio(prsn_ntby_tr, total_ntby_tr),
        '외국인 순매수 거래 대금 비율(%)': safe_ratio(frgn_ntby_tr, total_ntby_tr),
        '기관계 순매수 거래 대금 비율(%)': safe_ratio(orgn_ntby_tr, total_ntby_tr),
        
        # 매수2 비율
        '개인 매수2 거래량 비율(%)': safe_ratio(prsn_shnu, total_shnu_vol),
        '외국인 매수2 거래량 비율(%)': safe_ratio(frgn_shnu, total_shnu_vol),
        '기관계 매수2 거래량 비율(%)': safe_ratio(orgn_shnu, total_shnu_vol),
        
        '개인 매수2 거래 대금 비율(%)': safe_ratio(prsn_shnu_tr, total_shnu_tr),
        '외국인 매수2 거래 대금 비율(%)': safe_ratio(frgn_shnu_tr, total_shnu_tr),
        '기관계 매수2 거래 대금 비율(%)': safe_ratio(orgn_shnu_tr, total_shnu_tr),
        
        # 매도 비율
        '개인 매도 거래량 비율(%)': safe_ratio(prsn_seln, total_seln_vol),
        '외국인 매도 거래량 비율(%)': safe_ratio(frgn_seln, total_seln_vol),
        '기관계 매도 거래량 비율(%)': safe_ratio(orgn_seln, total_seln_vol),
        
        '개인 매도 거래 대금 비율(%)': safe_ratio(prsn_seln_tr, total_seln_tr),
        '외국인 매도 거래 대금 비율(%)': safe_ratio(frgn_seln_tr, total_seln_tr),
        '기관계 매도 거래 대금 비율(%)': safe_ratio(orgn_seln_tr, total_seln_tr)
    }
    
    return ratio_data


def save_to_json(data, filename, directory="data", convert_korean=True):
    """
    데이터를 JSON 파일로 저장하는 함수입니다.
    
    Args:
        data: 저장할 데이터
        filename (str): 파일명
        directory (str): 저장할 디렉토리
        convert_korean (bool): 한글명으로 변환 여부
    """
    # 디렉토리가 없으면 생성
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"디렉토리 생성: {directory}")
    
    # 한글명으로 변환
    if convert_korean:
        data = convert_to_korean_keys(data)
    
    filepath = os.path.join(directory, filename)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    print(f"JSON 파일 저장 완료: {filepath}")
    return filepath

############################################################

# 기본 종목 코드 (build_persona.py에서 가져온 종목들)
DEFAULT_STOCK_CODES = {
    # KOSPI 종목
    "005930": "삼성전자",
    "373220": "LG에너지솔루션", 
    "035420": "NAVER",
    "035720": "카카오",
    "017670": "SK텔레콤",
    "033780": "KT&G",
    "000660": "SK하이닉스",
    "207940": "삼성바이오로직스",
    "051910": "LG화학",
    "066570": "LG전자",
    "096770": "SK이노베이션",
    "003550": "LG",
    "015760": "한국전력",
    
    # KOSDAQ 종목
    "112040": "위메이드",
    "086520": "에코프로",
    "196170": "알테오젠",
    "028300": "HLB",
    "293490": "카카오게임즈",
    "263750": "펄어비스",
    "096530": "씨젠",
    "086900": "메디톡스"
}


# 사용 예시
if __name__ == "__main__":
    try:
        # 1. 액세스 토큰 발급
        token = get_access_token()
        
        # 2. 종목 코드 입력 (단일 또는 복수)
        print("=== 주식현재가 일자별 조회 ===")
        stock_preview = ', '.join(list(DEFAULT_STOCK_CODES.keys())[:5])
        print(f"기본 종목 ({len(DEFAULT_STOCK_CODES)}개): {stock_preview}...")
        print("엔터: 모든 기본 종목 조회")
        print("특정 종목: 종목코드 입력 (쉼표로 구분)")
        
        input_codes = input("조회할 종목코드를 입력하세요: ").strip()
        
        if not input_codes:
            # 기본 종목들 사용
            stock_codes = list(DEFAULT_STOCK_CODES.keys())
            print(f"기본 종목 {len(stock_codes)}개를 조회합니다.")
        else:
            # 사용자 입력 종목 사용
            stock_codes = [code.strip() for code in input_codes.split(',') 
                           if code.strip()]
        
        print(f"조회 대상 종목: {stock_codes}")
        
        # 3. 일자별 정보 및 투자자 정보 조회
        current_date = datetime.now().strftime("%Y%m%d")
        
        if len(stock_codes) == 1:
            # 단일 종목 조회
            stock_code = stock_codes[0]
            stock_name = DEFAULT_STOCK_CODES.get(stock_code, "알수없음")
            
            # 일자별 정보 조회
            daily_data = get_daily_price(stock_code, token)
            daily_filename = (f"{stock_code}_{stock_name}_"
                             f"일자별_{current_date}.json")
            save_to_json(daily_data, daily_filename, directory="data/daily")
            
            # 투자자 정보 조회 및 비율 계산
            time.sleep(0.2)  # API 호출 간격
            investor_data = get_investor_info(stock_code, token)
            if investor_data and not investor_data.get('error'):
                # 한글명으로 변환
                investor_data_korean = convert_to_korean_keys(investor_data)
                
                # 비율 계산
                ratio_data = calculate_investor_ratios(investor_data_korean)
                if ratio_data:
                    investor_filename = (f"{stock_code}_{stock_name}_"
                                        f"투자자_{current_date}.json")
                    save_to_json(ratio_data, investor_filename, 
                                directory="data/investor", convert_korean=False)
            
        else:
            # 복수 종목 조회 - 각 종목별 개별 파일 저장
            daily_data = get_multiple_daily_prices(stock_codes, token)
            
            # 각 종목별로 일자별 정보 저장
            for stock_code, stock_data in daily_data.items():
                if not stock_data.get('error'):
                    stock_name = DEFAULT_STOCK_CODES.get(stock_code, "알수없음")
                    filename = (f"{stock_code}_{stock_name}_일자별_"
                                f"{current_date}.json")
                    save_to_json(stock_data, filename, directory="data/daily")
                else:
                    print(f"종목 {stock_code} 일자별 정보 저장 실패: "
                          f"{stock_data.get('error')}")
            
            # 투자자 정보 조회 및 저장
            print("\n=== 투자자 정보 조회 시작 ===")
            for i, stock_code in enumerate(stock_codes):
                try:
                    print(f"[{i+1}/{len(stock_codes)}] "
                          f"{stock_code} 투자자 정보 조회 중...")
                    stock_name = DEFAULT_STOCK_CODES.get(stock_code, "알수없음")
                    
                    # 투자자 정보 조회
                    investor_data = get_investor_info(stock_code, token)
                    
                    if investor_data and not investor_data.get('error'):
                        # 한글명으로 변환
                        investor_data_korean = convert_to_korean_keys(investor_data)
                        
                        # 비율 계산
                        ratio_data = calculate_investor_ratios(investor_data_korean)
                        if ratio_data:
                            investor_filename = (f"{stock_code}_{stock_name}_"
                                               f"투자자_{current_date}.json")
                            save_to_json(ratio_data, investor_filename, 
                                        directory="data/investor", 
                                        convert_korean=False)
                    else:
                        print(f"종목 {stock_code} 투자자 정보 조회 실패")
                    
                    # API 호출 제한을 고려한 지연
                    if i < len(stock_codes) - 1:
                        time.sleep(0.2)
                        
                except Exception as e:
                    print(f"종목 {stock_code} 투자자 정보 처리 실패: {e}")
        
        print("\n=== 조회 완료 ===")
        
    except Exception as e:
        print(f"오류 발생: {e}")
