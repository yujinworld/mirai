import json
import random
from datetime import datetime, timedelta
import os
import FinanceDataReader as fdr
import time
import sys
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache

# kis_api 모듈 import를 위한 경로 추가
sys.path.append('../kis_api')
from kis_api import get_access_token, get_daily_price

# 글로벌 캐시 딕셔너리
price_cache = {}
local_price_db_file = "data/price_cache.json"


def load_price_cache():
    """로컬 파일에서 주가 캐시 로드"""
    global price_cache
    try:
        if os.path.exists(local_price_db_file):
            with open(local_price_db_file, 'r', encoding='utf-8') as f:
                price_cache = json.load(f)
            print(f"📁 주가 캐시 로드: {len(price_cache)}개 항목")
        else:
            price_cache = {}
            print("📁 새로운 주가 캐시 생성")
    except Exception as e:
        print(f"❌ 주가 캐시 로드 실패: {e}")
        price_cache = {}


def save_price_cache():
    """주가 캐시를 로컬 파일에 저장"""
    try:
        os.makedirs(os.path.dirname(local_price_db_file), exist_ok=True)
        with open(local_price_db_file, 'w', encoding='utf-8') as f:
            json.dump(price_cache, f, ensure_ascii=False, indent=2)
        print(f"💾 주가 캐시 저장: {len(price_cache)}개 항목")
    except Exception as e:
        print(f"❌ 주가 캐시 저장 실패: {e}")


def estimate_historical_price(stock_code, date_str, current_price):
    """실제 API 호출 대신 추정값으로 과거 주가 계산"""
    try:
        # 날짜 차이 계산
        target_date = datetime.strptime(date_str, '%Y%m%d')
        current_date = datetime.now()
        days_diff = (current_date - target_date).days
        
        # 과거일수록 더 큰 변동성 적용
        if days_diff < 30:  # 1개월 미만
            variation_range = 0.15  # ±15%
        elif days_diff < 90:  # 3개월 미만
            variation_range = 0.25  # ±25%
        elif days_diff < 365:  # 1년 미만
            variation_range = 0.35  # ±35%
        else:  # 1년 이상
            variation_range = 0.50  # ±50%
        
        # 랜덤 변동 적용
        variation = random.uniform(-variation_range, variation_range)
        estimated_price = int(current_price * (1 + variation))
        
        # 최소값 보장
        estimated_price = max(estimated_price, 1000)
        
        return estimated_price
    except Exception as e:
        print(f"❌ 추정 주가 계산 실패: {e}")
        return current_price


def get_historical_price(stock_code, date_str, current_price=None, use_cache=True, use_estimation=True):
    """특정 날짜의 주식 가격 조회 (캐싱 및 추정값 지원)"""
    cache_key = f"{stock_code}_{date_str}"
    
    # 1. 캐시에서 확인
    if use_cache and cache_key in price_cache:
        return price_cache[cache_key]
    
    # 2. 추정값 사용 (빠른 처리)
    if use_estimation and current_price:
        estimated_price = estimate_historical_price(stock_code, date_str, current_price)
        if use_cache:
            price_cache[cache_key] = estimated_price
        return estimated_price
    
    # 3. 실제 API 호출 (느린 처리)
    try:
        # 해당 날짜 전후 1주일 데이터 조회
        start_date = (datetime.strptime(date_str, '%Y%m%d') -
                      timedelta(days=7)).strftime('%Y-%m-%d')
        end_date = (datetime.strptime(date_str, '%Y%m%d') +
                    timedelta(days=7)).strftime('%Y-%m-%d')
        
        stock_data = fdr.DataReader(stock_code, start_date, end_date)
        
        if not stock_data.empty:
            # 해당 날짜나 가장 가까운 영업일의 종가 사용
            target_date = (datetime.strptime(date_str, '%Y%m%d')
                           .strftime('%Y-%m-%d'))
            if target_date in stock_data.index:
                price = int(stock_data.loc[target_date]['Close'])
            else:
                # 가장 가까운 날짜의 데이터 사용
                price = int(stock_data['Close'].iloc[-1])
            
            # 캐시에 저장
            if use_cache:
                price_cache[cache_key] = price
            return price
        else:
            return None
    except Exception as e:
        print(f"❌ {stock_code} {date_str} 가격 조회 실패: {e}")
        # 실패시 추정값으로 대체
        if current_price:
            estimated_price = estimate_historical_price(stock_code, date_str, current_price)
            if use_cache:
                price_cache[cache_key] = estimated_price
            return estimated_price
        return None


def get_current_stock_prices():
    """KIS API로 실제 주식 현재가 가져오기 (KOSPI + KOSDAQ)"""
    stock_codes = {
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
        
        # KOSDAQ 종목 추가
        "112040": "위메이드",
        "086520": "에코프로",
        "196170": "알테오젠",
        "028300": "HLB",
        "293490": "카카오게임즈",
        "263750": "펄어비스",
        "096530": "씨젠",
        "086900": "메디톡스"
    }
    
    current_prices = {}
    
    try:
        # KIS API 토큰 발급
        print("🔑 KIS API 토큰 발급 중...")
        token = get_access_token()
        
        # 병렬 처리로 빠르게 가져오기
        print("🚀 병렬 처리로 현재가 조회 중...")
        
        def get_single_price(args):
            code, name = args
            try:
                # 일자별 정보에서 최신 종가 가져오기
                daily_data = get_daily_price(code, token)
                if (daily_data and daily_data.get('rt_cd') == '0' and 
                    daily_data.get('output') and 
                    len(daily_data['output']) > 0):
                    # 첫 번째 데이터가 가장 최신
                    latest_data = daily_data['output'][0]
                    price = int(latest_data.get('stck_clpr', 0))
                    if price > 0:
                        return code, name, price, "성공"
                
                # 데이터가 없거나 가격이 0인 경우 기본값 사용
                kosdaq_codes = ('086520', '112040', '196170', '028300',
                                '293490', '263750', '096530', '086900')
                if code.startswith(kosdaq_codes):
                    default_price = random.randint(20000, 150000)
                else:
                    default_price = random.randint(50000, 300000)
                return code, name, default_price, "기본값"
                
            except Exception as e:
                # 오류시 기본값 사용
                kosdaq_codes = ('086520', '112040', '196170', '028300',
                                '293490', '263750', '096530', '086900')
                if code.startswith(kosdaq_codes):
                    default_price = random.randint(20000, 150000)
                else:
                    default_price = random.randint(50000, 300000)
                return code, name, default_price, f"오류: {e}"
        
        # 병렬 처리로 모든 종목 현재가 조회
        with ThreadPoolExecutor(max_workers=5) as executor:
            args_list = list(stock_codes.items())
            results = list(executor.map(get_single_price, args_list))
        
        # 결과 처리
        for code, name, price, status in results:
            current_prices[code] = price
            if status == "성공":
                print(f"✅ {name}({code}): {price:,}원")
            elif status == "기본값":
                print(f"⚠️  {name}({code}): {price:,}원 (기본값)")
            else:
                print(f"❌ {name}({code}): {price:,}원 ({status})")
        
        print(f"🎯 총 {len(current_prices)}개 종목 현재가 조회 완료")
        
    except Exception as e:
        print(f"❌ KIS API 토큰 발급 실패: {e}")
        print("🔄 기본값으로 대체...")
        
        # 토큰 발급 실패시 모두 기본값 사용
        for code, name in stock_codes.items():
            kosdaq_codes = ('086520', '112040', '196170', '028300',
                            '293490', '263750', '096530', '086900')
            if code.startswith(kosdaq_codes):
                current_prices[code] = random.randint(20000, 150000)
            else:
                current_prices[code] = random.randint(50000, 300000)
            print(f"🔢 {name}({code}): {current_prices[code]:,}원 (기본값)")
    
    return current_prices


def process_single_stock_portfolio(args):
    """단일 종목 포트폴리오 처리 (병렬 처리용)"""
    stock, persona_name, persona_data, stock_prices = args
    
    print(f"📊 {stock['prdt_name']} 거래내역 생성 중...")
    
    # 투자 금액 계산
    investment_amount = int(
        persona_data["total_investment"] * stock["weight"]
    )
    
    # 실제 현재가 사용
    current_price = stock_prices.get(
        stock["pdno"], 
        random.randint(20000, 150000) if stock["market"] == "KOSDAQ" 
        else random.randint(50000, 300000)
    )
    
    # 목표 보유수량 계산 (대략적)
    target_qty = max(1, investment_amount // current_price)
    
    # 실제 거래내역 및 평균 매입가 생성
    (transactions, avg_price, first_purchase_date,
     last_purchase_date) = generate_stock_transactions(
        stock, persona_name, target_qty, current_price
    )
    
    # 실제 보유수량과 매입금액 계산
    holding_qty = sum(t["거래수량"] for t in transactions)
    actual_investment = sum(t["거래금액"] for t in transactions)
    current_value = holding_qty * current_price
    
    # 평가손익 계산
    profit_loss = current_value - actual_investment
    profit_loss_rate = (
        (profit_loss / actual_investment) * 100 
        if actual_investment > 0 else 0
    )
    
    # 전일 대비 등락 (KOSDAQ 변동성이 더 큼)
    if stock["market"] == "KOSDAQ":
        price_change = random.randint(-5000, 5000)
    else:
        price_change = random.randint(-3000, 3000)
    
    change_rate = (
        (price_change / current_price) * 100 
        if current_price > 0 else 0
    )
    
    # 거래 수량 (페르소나별 특성 반영)
    today_buy_qty = 0
    today_sell_qty = 0
    yesterday_buy_qty = 0
    yesterday_sell_qty = 0
    
    if persona_name == "김미래":
        trading_prob = 0.6 if stock["market"] == "KOSDAQ" else 0.5
        if random.random() < trading_prob:
            if random.random() < 0.7:
                today_buy_qty = random.randint(1, 3)
            else:
                today_sell_qty = random.randint(1, min(2, holding_qty))
        
        if random.random() < 0.4:
            if random.random() < 0.6:
                yesterday_buy_qty = random.randint(1, 2)
            else:
                yesterday_sell_qty = random.randint(1, 1)
    
    # output1 필드 구성
    stock_data = {
        "pdno": stock["pdno"],
        "prdt_name": stock["prdt_name"],
        "trad_dvsn_name": "매수",
        "bfdy_buy_qty": str(yesterday_buy_qty),
        "bfdy_sll_qty": str(yesterday_sell_qty),
        "thdt_buyqty": str(today_buy_qty),
        "thdt_sll_qty": str(today_sell_qty),
        "hldg_qty": str(holding_qty),
        "ord_psbl_qty": str(holding_qty),
        "pchs_avg_pric": f"{avg_price:.2f}",
        "pchs_amt": str(actual_investment),
        "prpr": str(current_price),
        "evlu_amt": str(current_value),
        "evlu_pfls_amt": str(profit_loss),
        "evlu_pfls_rt": f"{profit_loss_rate:.2f}",
        "evlu_erng_rt": "0",
        "loan_dt": "",
        "loan_amt": "0",
        "stln_slng_chgs": "0",
        "expd_dt": "",
        "fltt_rt": f"{change_rate:.2f}",
        "bfdy_cprs_icdc": str(price_change),
        "item_mgna_rt_name": "40%",
        "grta_rt_name": "40%",
        "sbst_pric": str(current_price),
        "stck_loan_unpr": "0",
        "pchs_dt": last_purchase_date,
        "frst_pchs_dt": first_purchase_date
    }
    
    return {
        "stock_data": stock_data,
        "current_value": current_value,
        "actual_investment": actual_investment,
        "profit_loss": profit_loss,
        "transactions": transactions
    }


def generate_purchase_dates_and_history(persona_name):
    """페르소나별 매수일자 및 거래내역 생성 (2025년 7월 1일~10일 기간)"""
    # 기준 날짜 설정 (2025년 7월 1일)
    base_date = datetime(2025, 7, 1)
    end_date = datetime(2025, 7, 10)
    
    if persona_name == "김미래":
        # 20대: 단타 거래 위주 (1-3일 보유)
        date_ranges = [
            (base_date, base_date + timedelta(days=1)),  # 7월 1-2일
            (base_date + timedelta(days=2),
             base_date + timedelta(days=3)),  # 7월 3-4일
            (base_date + timedelta(days=4),
             base_date + timedelta(days=5)),  # 7월 5-6일
            (base_date + timedelta(days=6),
             base_date + timedelta(days=7)),  # 7월 7-8일
            (base_date + timedelta(days=8), end_date)  # 7월 9-10일
        ]
    elif persona_name == "이현재":
        # 30대: 단타+중기 투자 위주 (3-7일 보유)
        date_ranges = [
            (base_date, base_date + timedelta(days=2)),  # 7월 1-3일
            (base_date + timedelta(days=3),
             base_date + timedelta(days=5)),  # 7월 4-6일
            (base_date + timedelta(days=6),
             base_date + timedelta(days=8)),  # 7월 7-9일
            (base_date + timedelta(days=9), end_date)  # 7월 10일
        ]
    else:  # 박과거
        # 50대: 가치투자 위주 (장기 보유, 7-10일 보유)
        date_ranges = [
            (base_date, base_date + timedelta(days=3)),  # 7월 1-4일
            (base_date + timedelta(days=4),
             base_date + timedelta(days=6)),  # 7월 5-7일
            (base_date + timedelta(days=7),
             base_date + timedelta(days=9)),  # 7월 8-10일
        ]
    
    return date_ranges


def generate_stock_transactions(stock_info, persona_name, target_holding_qty,
                                current_price):
    """종목별 거래내역 생성 (매수만, 페르소나별 매수일자 범위)"""
    transactions = []
    
    # 페르소나별 거래 특성 및 매수일자 범위
    if persona_name == "김미래":
        # 20대: 단타 거래 위주 (2025-07-01 ~ 2025-07-10)
        start_date = datetime(2025, 7, 1)
        end_date = datetime(2025, 7, 10)
        transaction_counts = random.randint(7, 10)  # 거래 횟수(매일 혹은 하루 2회)
        qty_range = (1, 3)
    elif persona_name == "이현재":
        # 30대: 단타+중기 (2024-07-01 ~ 2025-07-10)
        start_date = datetime(2024, 7, 1)
        end_date = datetime(2025, 7, 10)
        transaction_counts = random.randint(5, 8)
        qty_range = (2, 8)
    else:
        # 50대: 가치투자 (2023-01-01 ~ 2025-07-10)
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2025, 7, 10)
        transaction_counts = random.randint(2, 5)
        qty_range = (5, 15)

    total_qty = 0
    total_amount = 0
    for _ in range(transaction_counts):
        if total_qty >= target_holding_qty:
            break
        # 랜덤 매수일자 생성
        days_diff = (end_date - start_date).days
        random_days = random.randint(0, days_diff)
        transaction_date = start_date + timedelta(days=random_days)
        transaction_date_str = transaction_date.strftime('%Y%m%d')
        # 해당 날짜의 주가 조회 (캐싱 및 추정값 사용)
        historical_price = get_historical_price(
            stock_info["pdno"], 
            transaction_date_str, 
            current_price=current_price, 
            use_cache=True, 
            use_estimation=True
        )
        if historical_price is None:
            price_variation = random.uniform(0.8, 1.2)
            historical_price = int(current_price * price_variation)
        # 매수 수량 결정
        remaining_qty = target_holding_qty - total_qty
        if remaining_qty <= 0:
            break
        
        # 안전한 수량 범위 계산
        min_qty = max(1, qty_range[0])
        max_qty = min(qty_range[1], remaining_qty)
        
        if min_qty > max_qty:
            qty = remaining_qty  # 남은 수량만큼 매수
        else:
            qty = random.randint(min_qty, max_qty)
        
        if qty <= 0:
            continue
        transaction_amount = qty * historical_price
        transaction = {
            "거래일자": transaction_date_str,
                        "거래시간": (f"{random.randint(9, 15):02d}"
                        f"{random.randint(0, 59):02d}"
                        f"{random.randint(0, 59):02d}"),
            "종목코드": stock_info["pdno"],
            "종목명": stock_info["prdt_name"],
            "매매구분": "매수",
            "거래수량": qty,
            "거래단가": historical_price,
            "거래금액": transaction_amount,
            "수수료": int(transaction_amount * 0.00015),
            "세금": 0,
            "정산금액": transaction_amount + int(transaction_amount * 0.00015)
        }
        transactions.append(transaction)
        total_qty += qty
        total_amount += transaction_amount
        if total_qty >= target_holding_qty:
            break
    # 평균 매입가 계산
    avg_price = total_amount / total_qty if total_qty > 0 else current_price
    # 최초 매입일자와 최근 매입일자
    if transactions:
        first_purchase_date = min(t["거래일자"] for t in transactions)
        last_purchase_date = max(t["거래일자"] for t in transactions)
    else:
        first_purchase_date = start_date.strftime('%Y%m%d')
        last_purchase_date = first_purchase_date
    return transactions, avg_price, first_purchase_date, last_purchase_date


def convert_to_korean_api_format(data):
    """한국투자증권 API 응답 형식을 한국어 키값으로 변환"""
    
    # output1 필드 매핑 (보유종목 상세) - 매수일자 필드 추가
    output1_mapping = {
        "pdno": "상품번호",
        "prdt_name": "상품명", 
        "trad_dvsn_name": "매매구분명",
        "bfdy_buy_qty": "전일매수수량",
        "bfdy_sll_qty": "전일매도수량",
        "thdt_buyqty": "금일매수수량",
        "thdt_sll_qty": "금일매도수량",
        "hldg_qty": "보유수량",
        "ord_psbl_qty": "주문가능수량",
        "pchs_avg_pric": "매입평균가격",
        "pchs_amt": "매입금액",
        "prpr": "현재가",
        "evlu_amt": "평가금액",
        "evlu_pfls_amt": "평가손익금액",
        "evlu_pfls_rt": "평가손익율",
        "evlu_erng_rt": "평가수익율",
        "loan_dt": "대출일자",
        "loan_amt": "대출금액",
        "stln_slng_chgs": "대주매각대금",
        "expd_dt": "만기일자",
        "fltt_rt": "등락율",
        "bfdy_cprs_icdc": "전일대비증감",
        "item_mgna_rt_name": "종목증거금율명",
        "grta_rt_name": "보증금율명",
        "sbst_pric": "대용가격",
        "stck_loan_unpr": "주식대출단가",
        "pchs_dt": "매입일자",  # 추가
        "frst_pchs_dt": "최초매입일자"  # 추가
    }
    
    # output2 필드 매핑 (계좌 요약)
    output2_mapping = {
        "dnca_tot_amt": "예수금총금액",
        "nxdy_excc_amt": "익일정산금액",
        "prvs_rcdl_excc_amt": "가수도정산금액",
        "cma_evlu_amt": "CMA평가금액",
        "bfdy_buy_amt": "전일매수금액",
        "thdt_buy_amt": "금일매수금액",
        "nxdy_auto_rdpt_amt": "익일자동상환금액",
        "bfdy_sll_amt": "전일매도금액",
        "thdt_sll_amt": "금일매도금액",
        "d2_auto_rdpt_amt": "D+2자동상환금액",
        "bfdy_tlex_amt": "전일제비용금액",
        "thdt_tlex_amt": "금일제비용금액",
        "tot_loan_amt": "총대출금액",
        "scts_evlu_amt": "유가평가금액",
        "tot_evlu_amt": "총평가금액",
        "nass_amt": "순자산금액",
        "fncg_gld_auto_rdpt_yn": "융자금자동상환여부",
        "pchs_amt_smtl_amt": "매입금액합계금액",
        "evlu_amt_smtl_amt": "평가금액합계금액",
        "evlu_pfls_smtl_amt": "평가손익합계금액",
        "tot_stln_slng_chgs": "총대주매각대금",
        "bfdy_tot_asst_evlu_amt": "전일총자산평가금액",
        "asst_icdc_amt": "자산증감액",
        "asst_icdc_erng_rt": "자산증감수익율"
    }
    
    # 응답 헤더 매핑
    header_mapping = {
        "content-type": "컨텐츠타입",
        "tr_id": "거래ID",
        "tr_cont": "연속거래여부",
        "gt_uid": "GlobalUID"
    }
    
    # 응답 바디 매핑
    body_mapping = {
        "rt_cd": "성공실패여부",
        "msg_cd": "응답코드",
        "msg1": "응답메세지",
        "ctx_area_fk100": "연속조회검색조건100",
        "ctx_area_nk100": "연속조회키100",
        "output1": "응답상세1",
        "output2": "응답상세2"
    }
    
    converted_data = {}
    
    # 헤더 변환
    if "header" in data:
        converted_data["header"] = {}
        for eng_key, kor_key in header_mapping.items():
            if eng_key in data["header"]:
                converted_data["header"][kor_key] = data["header"][eng_key]
    
    # 바디 변환
    if "body" in data:
        converted_data["body"] = {}
        for eng_key, kor_key in body_mapping.items():
            if eng_key in data["body"]:
                if eng_key == "output1":
                    # output1 배열 변환
                    converted_data["body"][kor_key] = []
                    for item in data["body"][eng_key]:
                        kor_item = {}
                        for item_eng_key, item_kor_key in (
                                output1_mapping.items()):
                            if item_eng_key in item:
                                kor_item[item_kor_key] = item[item_eng_key]
                        converted_data["body"][kor_key].append(kor_item)
                elif eng_key == "output2":
                    # output2 배열 변환
                    converted_data["body"][kor_key] = []
                    for item in data["body"][eng_key]:
                        kor_item = {}
                        for item_eng_key, item_kor_key in (
                                output2_mapping.items()):
                            if item_eng_key in item:
                                kor_item[item_kor_key] = item[item_eng_key]
                        converted_data["body"][kor_key].append(kor_item)
                else:
                    converted_data["body"][kor_key] = data["body"][eng_key]
    
    return converted_data


def generate_persona_portfolios():
    """
    3명의 페르소나별 국내주식 전용 포트폴리오 데이터 생성 
    (KOSDAQ 종목 포함, 실제 매수일자 반영, 최적화된 버전)
    """
    
    # 캐시 로드
    print("📁 주가 캐시 로딩 중...")
    load_price_cache()
    
    # 실제 현재가 가져오기
    print("📈 실제 주식 현재가 가져오는 중...")
    print("⏳ API 호출 간격을 두어 안정적으로 데이터를 가져옵니다...")
    stock_prices = get_current_stock_prices()
    print()
    
    # 페르소나별 기본 정보 (국내주식 전용)
    personas = {
        "김미래": {
            "total_investment": 7000000,  # 700만원 (국내주식 전용)
            "profile": {
                "age": 27,
                "job": "IT 스타트업 마케터",
                "income": 42000000,
                "risk_profile": "공격적",
                "investment_style": "숏폼 정보 기반 감정적 거래, 게임/IT 관심"
            }
        },
        "이현재": {
            "total_investment": 36000000,  # 3,600만원 (국내주식 전용)
            "profile": {
                "age": 35,
                "job": "대기업 팀장", 
                "income": 78000000,
                "risk_profile": "중간위험",
                "investment_style": "분산투자, 성장주 선호, 시간 효율성 중시"
            }
        },
        "박과거": {
            "total_investment": 240000000,  # 2억 4천만원 (국내주식 전용)
            "profile": {
                "age": 52,
                "job": "중견기업 임원",
                "income": 120000000,
                "risk_profile": "보수적",
                "investment_style": "장기투자, 배당 중심, 헬스케어 관심"
            }
        }
    }
    
    # 페르소나별 선호 종목 (KOSPI + KOSDAQ, 최대 5개)
    stock_preferences = {
        "김미래": [  # 게임/IT 중심의 공격적 투자
            {"pdno": "035420", "prdt_name": "NAVER", "weight": 0.25,
             "market": "KOSPI"},
            {"pdno": "035720", "prdt_name": "카카오", "weight": 0.20,
             "market": "KOSPI"},
            {"pdno": "112040", "prdt_name": "위메이드", "weight": 0.20,
             "market": "KOSDAQ"},
            {"pdno": "293490", "prdt_name": "카카오게임즈", "weight": 0.20,
             "market": "KOSDAQ"},
            {"pdno": "005930", "prdt_name": "삼성전자", "weight": 0.15,
             "market": "KOSPI"}
        ],
        "이현재": [  # 성장주/미래산업 중심의 균형 투자
            {"pdno": "005930", "prdt_name": "삼성전자", "weight": 0.30,
             "market": "KOSPI"},
            {"pdno": "373220", "prdt_name": "LG에너지솔루션", "weight": 0.25,
             "market": "KOSPI"},
            {"pdno": "000660", "prdt_name": "SK하이닉스", "weight": 0.20,
             "market": "KOSPI"},
            {"pdno": "086520", "prdt_name": "에코프로", "weight": 0.15,
             "market": "KOSDAQ"},
            {"pdno": "196170", "prdt_name": "알테오젠", "weight": 0.10,
             "market": "KOSDAQ"}
        ],
        "박과거": [  # 안정성/배당 중심의 보수적 투자
            {"pdno": "005930", "prdt_name": "삼성전자", "weight": 0.35,
             "market": "KOSPI"},
            {"pdno": "017670", "prdt_name": "SK텔레콤", "weight": 0.25,
             "market": "KOSPI"},
            {"pdno": "033780", "prdt_name": "KT&G", "weight": 0.20,
             "market": "KOSPI"},
            {"pdno": "096530", "prdt_name": "씨젠", "weight": 0.12,
             "market": "KOSDAQ"},
            {"pdno": "028300", "prdt_name": "HLB", "weight": 0.08,
             "market": "KOSDAQ"}
        ]
    }
    
    all_portfolios = {}
    
    for persona_name, persona_data in personas.items():
        print(f"\n=== {persona_name} 포트폴리오 및 거래내역 생성 중... ===")
        stocks = stock_preferences[persona_name]
        
        # 병렬 처리를 위한 인자 준비
        args_list = [
            (stock, persona_name, persona_data, stock_prices) 
            for stock in stocks
        ]
        
        # 병렬 처리로 종목별 포트폴리오 생성
        print(f"🚀 {len(stocks)}개 종목을 병렬 처리로 생성 중...")
        with ThreadPoolExecutor(max_workers=3) as executor:
            results = list(executor.map(process_single_stock_portfolio, args_list))
        
        # 결과 취합
        output1_data = []
        total_portfolio_value = 0
        total_purchase_amount = 0
        total_profit_loss = 0
        all_transactions = []
        
        for result in results:
            output1_data.append(result["stock_data"])
            total_portfolio_value += result["current_value"]
            total_purchase_amount += result["actual_investment"]
            total_profit_loss += result["profit_loss"]
            all_transactions.extend(result["transactions"])
        
        # 거래내역을 시간순으로 정렬
        all_transactions.sort(key=lambda x: (x["거래일자"], x["거래시간"]))
        
        # output2 데이터 생성 (계좌 전체 요약)
        cash_amount = int(persona_data["total_investment"] * 0.15)  # 15%를 현금으로
        total_evaluation = total_portfolio_value + cash_amount
        
        # 전일 총자산 계산
        prev_total_asset = int(total_evaluation * random.uniform(0.96, 1.03))
        asset_change = total_evaluation - prev_total_asset
        asset_change_rate = (
            (asset_change / prev_total_asset) * 100 
            if prev_total_asset > 0 else 0
        )
        
        # 당일 거래금액 계산
        today_buy_amount = sum(
            int(stock["thdt_buyqty"]) * int(stock["prpr"]) 
            for stock in output1_data if int(stock["thdt_buyqty"]) > 0
        )
        today_sell_amount = sum(
            int(stock["thdt_sll_qty"]) * int(stock["prpr"]) 
            for stock in output1_data if int(stock["thdt_sll_qty"]) > 0
        )
        yesterday_buy_amount = sum(
            int(stock["bfdy_buy_qty"]) * int(stock["prpr"]) 
            for stock in output1_data if int(stock["bfdy_buy_qty"]) > 0
        )
        yesterday_sell_amount = sum(
            int(stock["bfdy_sll_qty"]) * int(stock["prpr"]) 
            for stock in output1_data if int(stock["bfdy_sll_qty"]) > 0
        )
        
        output2_data = {
            "dnca_tot_amt": str(cash_amount),
            "nxdy_excc_amt": str(cash_amount),
            "prvs_rcdl_excc_amt": str(cash_amount),
            "cma_evlu_amt": "0",
            "bfdy_buy_amt": str(yesterday_buy_amount),
            "thdt_buy_amt": str(today_buy_amount),
            "nxdy_auto_rdpt_amt": "0",
            "bfdy_sll_amt": str(yesterday_sell_amount),
            "thdt_sll_amt": str(today_sell_amount),
            "d2_auto_rdpt_amt": "0",
            "bfdy_tlex_amt": str(
                int(yesterday_buy_amount * 0.0025)
            ),  # 거래비용 0.25%
            "thdt_tlex_amt": str(int(today_buy_amount * 0.0025)),
            "tot_loan_amt": "0",
            "scts_evlu_amt": str(total_portfolio_value),
            "tot_evlu_amt": str(total_evaluation),
            "nass_amt": str(total_evaluation),
            "fncg_gld_auto_rdpt_yn": "N",
            "pchs_amt_smtl_amt": str(total_purchase_amount),
            "evlu_amt_smtl_amt": str(total_portfolio_value),
            "evlu_pfls_smtl_amt": str(total_profit_loss),
            "tot_stln_slng_chgs": "0",
            "bfdy_tot_asst_evlu_amt": str(prev_total_asset),
            "asst_icdc_amt": str(asset_change),
            "asst_icdc_erng_rt": f"{asset_change_rate:.2f}"
        }
        
        # 한국투자증권 API 응답 형식으로 구성
        api_response = {
            "header": {
                "content-type": "application/json; charset=utf-8",
                "tr_id": "TTTC8434R",
                "tr_cont": "F",
                "gt_uid": ""
            },
            "body": {
                "rt_cd": "0",
                "msg_cd": "MCA00000",
                "msg1": "정상처리 되었습니다.",
                "ctx_area_fk100": "",
                "ctx_area_nk100": "",
                "output1": output1_data,
                "output2": [output2_data]  # output2는 배열 형태
            }
        }
        
        # 한국어 키값으로 변환
        korean_response = convert_to_korean_api_format(api_response)
        
        # 최종 포트폴리오 구성
        portfolio = {
            "persona_name": persona_name,
            "profile": persona_data["profile"],
            "english_api_response": api_response,  # 영어 버전
            "korean_api_response": korean_response,  # 한국어 버전
            "transaction_history": all_transactions,  # 거래내역 추가
            "timestamp": datetime.now().isoformat()
        }
        
        all_portfolios[persona_name] = portfolio

    # 캐시 저장
    print("\n💾 주가 캐시 저장 중...")
    save_price_cache()

    return all_portfolios


def save_persona_json(persona_name, portfolio_data, folder="persona_json"):
    """페르소나별 JSON 저장 (잔고와 거래내역 분리)"""
    if not os.path.exists(folder):
        os.makedirs(folder)
    
    # 1. 잔고 JSON 저장 (API 응답 형식)
    balance_filename = f"{folder}/{persona_name}_잔고.json"
    balance_data = {
        "persona_name": portfolio_data["persona_name"],
        "profile": portfolio_data["profile"],
        "api_response": portfolio_data["korean_api_response"],
        "timestamp": portfolio_data["timestamp"]
    }
    with open(balance_filename, 'w', encoding='utf-8') as f:
        json.dump(balance_data, f, ensure_ascii=False, indent=2)
    
    # 2. 거래내역 JSON 저장
    transaction_filename = f"{folder}/{persona_name}_거래내역.json"
    transaction_data = {
        "persona_name": portfolio_data["persona_name"],
        "profile": portfolio_data["profile"],
        "transaction_history": portfolio_data["transaction_history"],
        "transaction_summary": {
            "총거래횟수": len(portfolio_data["transaction_history"]),
            "총매수금액": sum(t["거래금액"] 
                            for t in portfolio_data["transaction_history"]),
            "총수수료": sum(t["수수료"] 
                          for t in portfolio_data["transaction_history"]),
            "거래기간": {
                "시작일": (min(t["거래일자"] 
                              for t in portfolio_data["transaction_history"]) 
                          if portfolio_data["transaction_history"] else "N/A"),
                "종료일": (max(t["거래일자"] 
                              for t in portfolio_data["transaction_history"]) 
                          if portfolio_data["transaction_history"] else "N/A")
            }
        },
        "timestamp": portfolio_data["timestamp"]
    }
    with open(transaction_filename, 'w', encoding='utf-8') as f:
        json.dump(transaction_data, f, ensure_ascii=False, indent=2)
    
    print(f"✅ {persona_name} JSON 저장:")
    print(f"   💰 잔고: {balance_filename}")
    print(f"   📊 거래내역: {transaction_filename}")


def create_master_tables(portfolios, folder="persona_tables"):
    """전체 통합 테이블 생성 (한국어 버전과 거래내역만) - 주석처리됨"""
    if not os.path.exists(folder):
        os.makedirs(folder)
    
    # 전체 포트폴리오 통합 JSON 저장 (한국어 버전만) - 주석처리
    # all_portfolios_korean = {}
    # all_transactions = {}
    
    # for persona_name, portfolio in portfolios.items():
    #     # 한국어 버전 (모든 정보 포함)
    #     all_portfolios_korean[persona_name] = {
    #         "persona_name": portfolio["persona_name"],
    #         "profile": portfolio["profile"],
    #         "api_response": portfolio["korean_api_response"],
    #         "transaction_history": portfolio["transaction_history"],
    #         "transaction_summary": {
    #             "총거래횟수": len(portfolio["transaction_history"]),
    #             "총매수금액": sum(t["거래금액"] 
    #                            for t in portfolio["transaction_history"]),
    #             "총수수료": sum(t["수수료"] 
    #                          for t in portfolio["transaction_history"]),
    #             "거래기간": {
    #                 "시작일": (min(t["거래일자"] 
    #                               for t in portfolio["transaction_history"]) 
    #                           if portfolio["transaction_history"] else "N/A"),
    #                 "종료일": (max(t["거래일자"] 
    #                               for t in portfolio["transaction_history"]) 
    #                           if portfolio["transaction_history"] else "N/A")
    #             }
    #         },
    #         "timestamp": portfolio["timestamp"]
    #     }
        
    #     # 거래내역 통합
    #     all_transactions[persona_name] = portfolio["transaction_history"]
    
    # # 한국어 버전 저장 (주석처리)
    # korean_filename = f"{folder}/all_personas.json"
    # with open(korean_filename, 'w', encoding='utf-8') as f:
    #     json.dump(all_portfolios_korean, f, ensure_ascii=False, indent=2)
    
    # # 전체 거래내역 통합 저장 (주석처리)
    # transactions_filename = f"{folder}/all_transactions.json"
    # with open(transactions_filename, 'w', encoding='utf-8') as f:
    #     json.dump(all_transactions, f, ensure_ascii=False, indent=2)
    
    # print("\n📊 통합 JSON 저장:")
    # print(f"   🇰🇷 전체 페르소나: {korean_filename}")
    # print(f"   📈 전체 거래내역: {transactions_filename}")
    
    print("\n📊 통합 JSON 저장: (주석처리됨)")
    print("   🇰🇷 전체 페르소나: all_personas.json (생성 안함)")
    print("   📈 전체 거래내역: all_transactions.json (생성 안함)")


# def convert_investor_data_to_korean(investor_data):
#     """
#     투자자 정보의 영어 필드명을 한글명으로 변환하는 함수 (주석처리)
#     """
#     if not investor_data or 'output' not in investor_data:
#         return investor_data
#     
#     # 투자자 정보 필드 매핑
#     field_mapping = {
#         "stck_bsop_date": "주식영업일자",
#         "stck_clpr": "주식종가",
#         "prdy_vrss": "전일대비",
#         "prdy_vrss_sign": "전일대비부호",
#         "prsn_ntby_qty": "개인순매수수량",
#         "frgn_ntby_qty": "외국인순매수수량",
#         "orgn_ntby_qty": "기관순매수수량",
#         "prsn_ntby_tr_pbmn": "개인순매수거래대금",
#         "frgn_ntby_tr_pbmn": "외국인순매수거래대금",
#         "orgn_ntby_tr_pbmn": "기관순매수거래대금",
#         "prsn_shnu_vol": "개인매수거래량",
#         "frgn_shnu_vol": "외국인매수거래량",
#         "orgn_shnu_vol": "기관매수거래량",
#         "prsn_shnu_tr_pbmn": "개인매수거래대금",
#         "frgn_shnu_tr_pbmn": "외국인매수거래대금",
#         "orgn_shnu_tr_pbmn": "기관매수거래대금",
#         "prsn_seln_vol": "개인매도거래량",
#         "frgn_seln_vol": "외국인매도거래량",
#         "orgn_seln_vol": "기관매도거래량",
#         "prsn_seln_tr_pbmn": "개인매도거래대금",
#         "frgn_seln_tr_pbmn": "외국인매도거래대금",
#         "orgn_seln_tr_pbmn": "기관매도거래대금"
#     }
#     
#     converted_data = investor_data.copy()
#     converted_data['output'] = []
#     
#     for item in investor_data['output']:
#         converted_item = {}
#         for eng_key, kor_key in field_mapping.items():
#             if eng_key in item:
#                 converted_item[kor_key] = item[eng_key]
#         converted_data['output'].append(converted_item)
#     
#     return converted_data


# def calculate_investor_ratios(investor_data):
#     """
#     투자자 정보에 비율값을 계산하여 추가하는 함수 (주석처리)
#     """
#     if not investor_data or 'output' not in investor_data:
#         return investor_data
#     
#     for item in investor_data['output']:
#         # 순매수 수량 비율 계산
#         total_net_qty = (abs(int(item.get('prsn_ntby_qty', 0))) + 
#                          abs(int(item.get('frgn_ntby_qty', 0))) + 
#                          abs(int(item.get('orgn_ntby_qty', 0))))
#         if total_net_qty > 0:
#             item['prsn_ntby_qty_ratio'] = round(
#                 (abs(int(item.get('prsn_ntby_qty', 0))) / total_net_qty) * 100,
#                 10)
#             item['frgn_ntby_qty_ratio'] = round(
#                 (abs(int(item.get('frgn_ntby_qty', 0))) / total_net_qty) * 100,
#                 10)
#             item['orgn_ntby_qty_ratio'] = round(
#                 (abs(int(item.get('orgn_ntby_qty', 0))) / total_net_qty) * 100,
#                 10)
#         else:
#             item['prsn_ntby_qty_ratio'] = 0
#             item['frgn_ntby_qty_ratio'] = 0
#             item['orgn_ntby_qty_ratio'] = 0
#         
#         # 순매수 거래대금 비율 계산
#         total_net_amount = (abs(int(item.get('prsn_ntby_tr_pbmn', 0))) + 
#                             abs(int(item.get('frgn_ntby_tr_pbmn', 0))) + 
#                             abs(int(item.get('orgn_ntby_tr_pbmn', 0))))
#         if total_net_amount > 0:
#             item['prsn_ntby_tr_pbmn_ratio'] = round(
#                 (abs(int(item.get('prsn_ntby_tr_pbmn', 0))) / 
#                  total_net_amount) * 100, 10)
#             item['frgn_ntby_tr_pbmn_ratio'] = round(
#                 (abs(int(item.get('frgn_ntby_tr_pbmn', 0))) / 
#                  total_net_amount) * 100, 10)
#             item['orgn_ntby_tr_pbmn_ratio'] = round(
#                 (abs(int(item.get('orgn_ntby_tr_pbmn', 0))) / 
#                  total_net_amount) * 100, 10)
#         else:
#             item['prsn_ntby_tr_pbmn_ratio'] = 0
#             item['frgn_ntby_tr_pbmn_ratio'] = 0
#             item['orgn_ntby_tr_pbmn_ratio'] = 0
#         
#         # 매수 거래량 비율 계산
#         total_buy_vol = (int(item.get('prsn_shnu_vol', 0)) + 
#                          int(item.get('frgn_shnu_vol', 0)) + 
#                          int(item.get('orgn_shnu_vol', 0)))
#         if total_buy_vol > 0:
#             item['prsn_shnu_vol_ratio'] = round(
#                 (int(item.get('prsn_shnu_vol', 0)) / total_buy_vol) * 100, 10)
#             item['frgn_shnu_vol_ratio'] = round(
#                 (int(item.get('frgn_shnu_vol', 0)) / total_buy_vol) * 100, 10)
#             item['orgn_shnu_vol_ratio'] = round(
#                 (int(item.get('orgn_shnu_vol', 0)) / total_buy_vol) * 100, 10)
#         else:
#             item['prsn_shnu_vol_ratio'] = 0
#             item['frgn_shnu_vol_ratio'] = 0
#             item['orgn_shnu_vol_ratio'] = 0
#         
#         # 매수 거래대금 비율 계산
#         total_buy_amount = (int(item.get('prsn_shnu_tr_pbmn', 0)) + 
#                             int(item.get('frgn_shnu_tr_pbmn', 0)) + 
#                             int(item.get('orgn_shnu_tr_pbmn', 0)))
#         if total_buy_amount > 0:
#             item['prsn_shnu_tr_pbmn_ratio'] = round(
#                 (int(item.get('prsn_shnu_tr_pbmn', 0)) / 
#                  total_buy_amount) * 100, 10)
#             item['frgn_shnu_tr_pbmn_ratio'] = round(
#                 (int(item.get('frgn_shnu_tr_pbmn', 0)) / 
#                  total_buy_amount) * 100, 10)
#             item['orgn_shnu_tr_pbmn_ratio'] = round(
#                 (int(item.get('orgn_shnu_tr_pbmn', 0)) / 
#                  total_buy_amount) * 100, 10)
#         else:
#             item['prsn_shnu_tr_pbmn_ratio'] = 0
#             item['frgn_shnu_tr_pbmn_ratio'] = 0
#             item['orgn_shnu_tr_pbmn_ratio'] = 0
#         
#         # 매도 거래량 비율 계산
#         total_sell_vol = (int(item.get('prsn_seln_vol', 0)) + 
#                           int(item.get('frgn_seln_vol', 0)) + 
#                           int(item.get('orgn_seln_vol', 0)))
#         if total_sell_vol > 0:
#             item['prsn_seln_vol_ratio'] = round(
#                 (int(item.get('prsn_seln_vol', 0)) / total_sell_vol) * 100, 10)
#             item['frgn_seln_vol_ratio'] = round(
#                 (int(item.get('frgn_seln_vol', 0)) / total_sell_vol) * 100, 10)
#             item['orgn_seln_vol_ratio'] = round(
#                 (int(item.get('orgn_seln_vol', 0)) / total_sell_vol) * 100, 10)
#         else:
#             item['prsn_seln_vol_ratio'] = 0
#             item['frgn_seln_vol_ratio'] = 0
#             item['orgn_seln_vol_ratio'] = 0
#         
#         # 매도 거래대금 비율 계산
#         total_sell_amount = (int(item.get('prsn_seln_tr_pbmn', 0)) + 
#                              int(item.get('frgn_seln_tr_pbmn', 0)) + 
#                              int(item.get('orgn_seln_tr_pbmn', 0)))
#         if total_sell_amount > 0:
#             item['prsn_seln_tr_pbmn_ratio'] = round(
#                 (int(item.get('prsn_seln_tr_pbmn', 0)) / 
#                  total_sell_amount) * 100, 10)
#             item['frgn_seln_tr_pbmn_ratio'] = round(
#                 (int(item.get('frgn_seln_tr_pbmn', 0)) / 
#                  total_sell_amount) * 100, 10)
#             item['orgn_seln_tr_pbmn_ratio'] = round(
#                 (int(item.get('orgn_seln_tr_pbmn', 0)) / 
#                  total_sell_amount) * 100, 10)
#         else:
#             item['prsn_seln_tr_pbmn_ratio'] = 0
#             item['frgn_seln_tr_pbmn_ratio'] = 0
#             item['orgn_seln_tr_pbmn_ratio'] = 0
#     
#     return investor_data


# def collect_all_investor_info():
#     """
#     build_persona.py에서 사용되는 모든 종목의 투자자 정보를 수집 (주석처리)
#     """
#     print("🚀 모든 종목 투자자 정보 수집 시작... (주석처리됨)")
#     print("✅ 투자자 정보 수집 생략")
#     return None


if __name__ == "__main__":
    print("🚀 국내주식 페르소나 포트폴리오 및 투자자 정보 통합 생성...")
    
    try:
        # 1단계: 페르소나 포트폴리오 생성
        print("\n=== 1단계: 페르소나 포트폴리오 생성 ===")
        portfolios = generate_persona_portfolios()
        
        print("\n📁 페르소나별 JSON 파일 생성...")
        
        # 페르소나별 개별 JSON 저장
        for persona_name, portfolio_data in portfolios.items():
            print(f"\n=== {persona_name} ===")
            save_persona_json(persona_name, portfolio_data)
        
        # 통합 JSON 생성
        create_master_tables(portfolios)
        
        # 2단계: 투자자 정보 수집 (주석처리)
        # print("\n=== 2단계: 투자자 정보 수집 ===")
        # collect_all_investor_info()
        print("\n=== 2단계: 투자자 정보 수집 (생략) ===")
        print("✅ 투자자 정보 수집 단계를 생략합니다.")
        
        print("\n✨ 모든 작업 완료!")
        print("📂 생성된 파일:")
        print("   📁 persona_json/ - 페르소나별 개별 파일")
        print("   📁 persona_tables/ - 통합 테이블 (생략됨)")
        print("   📁 data/ - 투자자 정보 (생략됨)")
        
        # 생성된 데이터 요약
        print("\n=== 📈 국내주식 포트폴리오 요약 ===")
        for persona_name, portfolio in portfolios.items():
            profile = portfolio["profile"]
            # 한국어 버전에서 데이터 추출
            output1 = portfolio["korean_api_response"]["body"]["응답상세1"]
            output2 = portfolio["korean_api_response"]["body"]["응답상세2"][0]
            transactions = portfolio["transaction_history"]
            
            kospi_codes = ["005930", "373220", "035420", "035720", "017670",
                           "033780", "000660", "207940", "051910", "066570",
                           "096770", "003550", "015760"]
            kospi_count = sum(1 for stock in output1 
                              if stock["상품번호"] in kospi_codes)
            kosdaq_count = len(output1) - kospi_count
            
            profit_loss = int(output2["평가손익합계금액"])
            profit_rate = (
                (profit_loss / int(output2["매입금액합계금액"]) * 100) 
                if int(output2["매입금액합계금액"]) > 0 else 0
            )
            
            # 거래내역 요약
            total_transactions = len(transactions)
            total_fees = sum(t["수수료"] for t in transactions)
            if transactions:
                first_trade = min(t["거래일자"] for t in transactions)
                last_trade = max(t["거래일자"] for t in transactions)
            else:
                first_trade = last_trade = "N/A"
            
            print(f"\n{persona_name} ({profile['age']}세)")
            print(f"  💼 {profile['job']}")
            print(f"  📊 {profile['risk_profile']} 투자성향")
            print(f"  💰 주식평가액: {int(output2['유가평가금액']):,}원")
            print(f"  📈 평가손익: {profit_loss:,}원 ({profit_rate:+.2f}%)")
            print(f"  💵 보유현금: {int(output2['예수금총금액']):,}원")
            print(f"  🏢 보유종목: KOSPI {kospi_count}개, "
                  f"KOSDAQ {kosdaq_count}개")
            print(f"  📝 투자스타일: {profile['investment_style']}")
            print(f"  🔄 총거래횟수: {total_transactions}회")
            print(f"  💸 총수수료: {total_fees:,}원")
            print(f"  📅 거래기간: {first_trade} ~ {last_trade}")
            
            # 보유 종목 상세 (매수일자 포함)
            print("  📋 보유종목:")
            for stock in output1:
                kosdaq_codes = ["086520", "112040", "196170", "028300",
                                "293490", "263750", "096530", "086900"]
                market_type = ("📊 KOSDAQ" if stock["상품번호"] in kosdaq_codes 
                               else "🏛️ KOSPI")
                profit_loss_stock = int(stock["평가손익금액"])
                first_purchase = stock["최초매입일자"]
                last_purchase = stock["매입일자"]
                avg_price = float(stock["매입평균가격"])
                print(f"     {market_type} {stock['상품명']}: "
                      f"{profit_loss_stock:+,}원")
                print(f"        📅 매수기간: {first_purchase} ~ "
                      f"{last_purchase}")
                print(f"        💱 평균매입가: {avg_price:,.0f}원")
            
    except KeyboardInterrupt:
        print("\n\n⏹️  사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")