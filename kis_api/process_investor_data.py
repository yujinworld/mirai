import pandas as pd
import os
from datetime import datetime

def process_investor_data(stock_code="005930", stock_name="삼성전자"):
    """
    투자자 정보 데이터를 처리하여 한국어 매핑과 비율 계산을 수행합니다.
    
    Args:
        stock_code (str): 종목코드 (기본값: "005930")
        stock_name (str): 종목명 (기본값: "삼성전자")
    """
    
    # 열 이름 한글 매핑 딕셔너리 정의
    col_map = {
        "stck_bsop_date": "주식 영업 일자",
        "stck_clpr": "주식 종가",
        "prdy_vrss": "전일 대비",
        "prdy_vrss_sign": "전일 대비 부호",
        "prsn_ntby_qty": "개인 순매수 수량",
        "frgn_ntby_qty": "외국인 순매수 수량",
        "orgn_ntby_qty": "기관계 순매수 수량",
        "prsn_ntby_tr_pbmn": "개인 순매수 거래 대금",
        "frgn_ntby_tr_pbmn": "외국인 순매수 거래 대금",
        "orgn_ntby_tr_pbmn": "기관계 순매수 거래 대금",
        "prsn_shnu_vol": "개인 매수2 거래량",
        "frgn_shnu_vol": "외국인 매수2 거래량",
        "orgn_shnu_vol": "기관계 매수2 거래량",
        "prsn_shnu_tr_pbmn": "개인 매수2 거래 대금",
        "frgn_shnu_tr_pbmn": "외국인 매수2 거래 대금",
        "orgn_shnu_tr_pbmn": "기관계 매수2 거래 대금",
        "prsn_seln_vol": "개인 매도 거래량",
        "frgn_seln_vol": "외국인 매도 거래량",
        "orgn_seln_vol": "기관계 매도 거래량",
        "prsn_seln_tr_pbmn": "개인 매도 거래 대금",
        "frgn_seln_tr_pbmn": "외국인 매도 거래 대금",
        "orgn_seln_tr_pbmn": "기관계 매도 거래 대금"
    }
    
    # CSV 파일 읽기
    csv_filename = f"{stock_code}_investor_info.csv"
    
    if not os.path.exists(csv_filename):
        print(f"파일을 찾을 수 없습니다: {csv_filename}")
        return None
    
    df = pd.read_csv(csv_filename)
    print(f"데이터 로드 완료: {len(df)}행")
    
    # 열 이름을 한글로 변경
    df_renamed = df.rename(columns=col_map)
    print("컬럼명 한글 매핑 완료")
    
    # 합계 변수 계산
    total_ntby_qty = df_renamed[["개인 순매수 수량", "외국인 순매수 수량", "기관계 순매수 수량"]].abs().sum(axis=1)
    total_ntby_tr_pbmn = df_renamed[["개인 순매수 거래 대금", "외국인 순매수 거래 대금", "기관계 순매수 거래 대금"]].abs().sum(axis=1)
    total_shnu_vol = df_renamed[["개인 매수2 거래량", "외국인 매수2 거래량", "기관계 매수2 거래량"]].abs().sum(axis=1)
    total_shnu_tr_pbmn = df_renamed[["개인 매수2 거래 대금", "외국인 매수2 거래 대금", "기관계 매수2 거래 대금"]].abs().sum(axis=1)
    total_seln_vol = df_renamed[["개인 매도 거래량", "외국인 매도 거래량", "기관계 매도 거래량"]].abs().sum(axis=1)
    total_seln_tr_pbmn = df_renamed[["개인 매도 거래 대금", "외국인 매도 거래 대금", "기관계 매도 거래 대금"]].abs().sum(axis=1)
    
    # 비율 열 추가
    df_renamed["개인 순매수 수량 비율(%)"] = (df_renamed["개인 순매수 수량"].abs() / total_ntby_qty * 100)
    df_renamed["외국인 순매수 수량 비율(%)"] = (df_renamed["외국인 순매수 수량"].abs() / total_ntby_qty * 100)
    df_renamed["기관계 순매수 수량 비율(%)"] = (df_renamed["기관계 순매수 수량"].abs() / total_ntby_qty * 100)
    
    df_renamed["개인 순매수 거래 대금 비율(%)"] = (df_renamed["개인 순매수 거래 대금"].abs() / total_ntby_tr_pbmn * 100)
    df_renamed["외국인 순매수 거래 대금 비율(%)"] = (df_renamed["외국인 순매수 거래 대금"].abs() / total_ntby_tr_pbmn * 100)
    df_renamed["기관계 순매수 거래 대금 비율(%)"] = (df_renamed["기관계 순매수 거래 대금"].abs() / total_ntby_tr_pbmn * 100)
    
    df_renamed["개인 매수2 거래량 비율(%)"] = (df_renamed["개인 매수2 거래량"].abs() / total_shnu_vol * 100)
    df_renamed["외국인 매수2 거래량 비율(%)"] = (df_renamed["외국인 매수2 거래량"].abs() / total_shnu_vol * 100)
    df_renamed["기관계 매수2 거래량 비율(%)"] = (df_renamed["기관계 매수2 거래량"].abs() / total_shnu_vol * 100)
    
    df_renamed["개인 매수2 거래 대금 비율(%)"] = (df_renamed["개인 매수2 거래 대금"].abs() / total_shnu_tr_pbmn * 100)
    df_renamed["외국인 매수2 거래 대금 비율(%)"] = (df_renamed["외국인 매수2 거래 대금"].abs() / total_shnu_tr_pbmn * 100)
    df_renamed["기관계 매수2 거래 대금 비율(%)"] = (df_renamed["기관계 매수2 거래 대금"].abs() / total_shnu_tr_pbmn * 100)
    
    df_renamed["개인 매도 거래량 비율(%)"] = (df_renamed["개인 매도 거래량"].abs() / total_seln_vol * 100)
    df_renamed["외국인 매도 거래량 비율(%)"] = (df_renamed["외국인 매도 거래량"].abs() / total_seln_vol * 100)
    df_renamed["기관계 매도 거래량 비율(%)"] = (df_renamed["기관계 매도 거래량"].abs() / total_seln_vol * 100)
    
    df_renamed["개인 매도 거래 대금 비율(%)"] = (df_renamed["개인 매도 거래 대금"].abs() / total_seln_tr_pbmn * 100)
    df_renamed["외국인 매도 거래 대금 비율(%)"] = (df_renamed["외국인 매도 거래 대금"].abs() / total_seln_tr_pbmn * 100)
    df_renamed["기관계 매도 거래 대금 비율(%)"] = (df_renamed["기관계 매도 거래 대금"].abs() / total_seln_tr_pbmn * 100)
    
    print("비율 계산 완료")
    
    # 비율 데이터만 추출 (기본 정보 + 비율 데이터)
    ratio_columns = [
        "주식 영업 일자", "주식 종가", "전일 대비", "전일 대비 부호",
        "개인 순매수 수량 비율(%)", "외국인 순매수 수량 비율(%)", "기관계 순매수 수량 비율(%)",
        "개인 순매수 거래 대금 비율(%)", "외국인 순매수 거래 대금 비율(%)", "기관계 순매수 거래 대금 비율(%)",
        "개인 매수2 거래량 비율(%)", "외국인 매수2 거래량 비율(%)", "기관계 매수2 거래량 비율(%)",
        "개인 매수2 거래 대금 비율(%)", "외국인 매수2 거래 대금 비율(%)", "기관계 매수2 거래 대금 비율(%)",
        "개인 매도 거래량 비율(%)", "외국인 매도 거래량 비율(%)", "기관계 매도 거래량 비율(%)",
        "개인 매도 거래 대금 비율(%)", "외국인 매도 거래 대금 비율(%)", "기관계 매도 거래 대금 비율(%)"
    ]
    
    df_ratio = df_renamed[ratio_columns].copy()
    print(f"비율 데이터 추출 완료: {len(ratio_columns)}개 컬럼")
    
    # 데이터 디렉토리 생성
    data_dir = "data"
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
        print(f"디렉토리 생성: {data_dir}")
    
    # 오늘 날짜 가져오기
    today = datetime.now().strftime("%Y%m%d")
    
    # 파일명 생성
    output_filename = f"{data_dir}/{stock_name}_{stock_code}_{today}.json"
    
    # JSON으로 저장
    df_ratio.to_json(output_filename, orient='records', force_ascii=False, indent=2)
    print(f"JSON 파일 저장 완료: {output_filename}")
    
    # 요약 정보 출력
    print(f"\n=== 처리 결과 요약 ===")
    print(f"종목명: {stock_name}")
    print(f"종목코드: {stock_code}")
    print(f"처리 날짜: {today}")
    print(f"데이터 행 수: {len(df_ratio)}")
    print(f"저장 파일: {output_filename}")
    
    return df_ratio

if __name__ == "__main__":
    # 삼성전자 데이터 처리
    result = process_investor_data("005930", "삼성전자")
    
    if result is not None:
        print("\n처리가 성공적으로 완료되었습니다.")
        print(f"첫 번째 행 데이터 예시:")
        print(result.iloc[0].to_dict())
    else:
        print("처리 중 오류가 발생했습니다.") 