#!/usr/bin/env python3
"""
간단한 페르소나 포트폴리오 생성 테스트 스크립트
"""

from build_persona import (generate_persona_portfolios, save_persona_json,
                          create_master_tables)

if __name__ == "__main__":
    print("🚀 페르소나 포트폴리오 테스트 생성...")
    
    # 포트폴리오 생성
    portfolios = generate_persona_portfolios()
    
    # 페르소나별 개별 JSON 저장 (잔고와 거래내역 분리)
    for persona_name, portfolio_data in portfolios.items():
        save_persona_json(persona_name, portfolio_data)
    
    # 통합 JSON 생성
    create_master_tables(portfolios)
    
    print("\n✨ 테스트 완료!")
    print("📂 'persona_json' 폴더에서 개별 파일들을 확인하세요")
    print("📂 'persona_tables' 폴더에서 통합 파일을 확인하세요") 