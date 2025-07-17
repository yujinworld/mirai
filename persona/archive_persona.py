import json
import os
import shutil
from datetime import datetime


class PersonaArchiver:
    """페르소나별 데이터를 개별 파일로 아카이빙하는 클래스"""
    
    def __init__(self):
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.persona_json_dir = os.path.join(self.base_dir, "persona_json")
        self.kis_data_dir = os.path.join(self.base_dir, "../kis_api/data")
        self.output_dir = os.path.join(self.base_dir, "archived")
        
        # 출력 디렉토리 생성
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            print(f"📁 아카이브 디렉토리 생성: {self.output_dir}")
    
    def get_persona_stocks(self, balance_data):
        """페르소나가 보유한 종목 코드 리스트 추출"""
        if not balance_data or 'api_response' not in balance_data:
            return []
        
        stocks = []
        holdings = balance_data['api_response']['body']['응답상세1']
        for holding in holdings:
            stock_code = holding['상품번호']
            stock_name = holding['상품명']
            stocks.append({
                'code': stock_code,
                'name': stock_name
            })
        return stocks
    
    def archive_persona_data(self, persona_name):
        """특정 페르소나의 데이터를 개별 파일로 아카이빙"""
        print(f"\n=== {persona_name} 데이터 아카이빙 중... ===")
        
        # 페르소나별 폴더 생성 (기존 폴더 삭제 후 재생성)
        persona_dir = os.path.join(self.output_dir, persona_name)
        if os.path.exists(persona_dir):
            shutil.rmtree(persona_dir)
        os.makedirs(persona_dir)
        
        # 1. 잔고 파일 복사
        balance_file = os.path.join(self.persona_json_dir, f"{persona_name}_잔고.json")
        if os.path.exists(balance_file):
            shutil.copy2(balance_file, os.path.join(persona_dir, f"{persona_name}_잔고.json"))
            print(f"  📊 잔고 파일 복사 완료")
            
            # 잔고에서 보유 종목 추출
            with open(balance_file, 'r', encoding='utf-8') as f:
                balance_data = json.load(f)
            held_stocks = self.get_persona_stocks(balance_data)
        else:
            print(f"  ❌ 잔고 파일을 찾을 수 없습니다: {balance_file}")
            return
        
        # 2. 거래내역 파일 복사
        transaction_file = os.path.join(self.persona_json_dir, f"{persona_name}_거래내역.json")
        if os.path.exists(transaction_file):
            shutil.copy2(transaction_file, os.path.join(persona_dir, f"{persona_name}_거래내역.json"))
            print(f"  📈 거래내역 파일 복사 완료")
        else:
            print(f"  ❌ 거래내역 파일을 찾을 수 없습니다: {transaction_file}")
        
        # 3. 보유 종목별 일자별 데이터 복사
        daily_copied = 0
        for stock in held_stocks:
            stock_code = stock['code']
            stock_name = stock['name']
            
            daily_source = os.path.join(self.kis_data_dir, "daily", 
                                       f"{stock_code}_{stock_name}_일자별_20250717.json")
            if os.path.exists(daily_source):
                daily_dest = os.path.join(persona_dir, f"{stock_code}_{stock_name}_일자별.json")
                shutil.copy2(daily_source, daily_dest)
                daily_copied += 1
        
        print(f"  📅 일자별 데이터 {daily_copied}/{len(held_stocks)}개 복사 완료")
        
        # 4. 보유 종목별 투자자 정보 복사
        investor_copied = 0
        for stock in held_stocks:
            stock_code = stock['code']
            stock_name = stock['name']
            
            investor_source = os.path.join(self.kis_data_dir, "investor", 
                                          f"{stock_code}_{stock_name}_투자자_20250717.json")
            if os.path.exists(investor_source):
                investor_dest = os.path.join(persona_dir, f"{stock_code}_{stock_name}_투자자.json")
                shutil.copy2(investor_source, investor_dest)
                investor_copied += 1
        
        print(f"  👥 투자자 정보 {investor_copied}/{len(held_stocks)}개 복사 완료")
        
        # 5. 아카이브 정보 파일 생성
        archive_info = {
            "아카이브_생성시간": datetime.now().isoformat(),
            "페르소나": persona_name,
            "보유종목수": len(held_stocks),
            "파일현황": {
                "잔고파일": 1,
                "거래내역파일": 1,
                "일자별데이터": daily_copied,
                "투자자정보": investor_copied
            },
            "보유종목목록": [
                {
                    "종목코드": stock['code'],
                    "종목명": stock['name'],
                    "일자별데이터": os.path.exists(os.path.join(persona_dir, f"{stock['code']}_{stock['name']}_일자별.json")),
                    "투자자정보": os.path.exists(os.path.join(persona_dir, f"{stock['code']}_{stock['name']}_투자자.json"))
                }
                for stock in held_stocks
            ]
        }
        
        info_file = os.path.join(persona_dir, f"{persona_name}_아카이브정보.json")
        with open(info_file, 'w', encoding='utf-8') as f:
            json.dump(archive_info, f, ensure_ascii=False, indent=2)
        
        print(f"  📋 아카이브 정보 파일 생성: {info_file}")
        print(f"✅ {persona_name} 아카이빙 완료")
        
        return archive_info
    
    def archive_all_personas(self):
        """모든 페르소나의 데이터를 아카이빙"""
        personas = ["김미래", "이현재", "박과거"]
        
        print("🚀 페르소나별 데이터 아카이빙 시작...")
        print(f"📂 아카이브 디렉토리: {self.output_dir}")
        
        results = {}
        for persona in personas:
            try:
                result = self.archive_persona_data(persona)
                if result:
                    results[persona] = result
            except Exception as e:
                print(f"❌ {persona} 아카이빙 중 오류: {e}")
        
        print(f"\n✨ 모든 페르소나 데이터 아카이빙 완료!")
        print(f"📁 결과 위치: {self.output_dir}")
        
        return results
    
    def show_archive_structure(self):
        """아카이브 구조 출력"""
        print("\n" + "="*60)
        print("📁 아카이브 디렉토리 구조")
        print("="*60)
        
        for root, dirs, files in os.walk(self.output_dir):
            level = root.replace(self.output_dir, '').count(os.sep)
            indent = ' ' * 2 * level
            folder_name = os.path.basename(root)
            if level == 0:
                print(f"📂 {folder_name}/")
            else:
                print(f"{indent}📁 {folder_name}/")
            
            subindent = ' ' * 2 * (level + 1)
            for file in files:
                if file.endswith('.json'):
                    print(f"{subindent}📄 {file}")


def main():
    """메인 실행 함수"""
    archiver = PersonaArchiver()
    
    print("=" * 60)
    print("📦 페르소나 데이터 아카이빙 도구")
    print("=" * 60)
    
    # 모든 페르소나 데이터 아카이빙
    results = archiver.archive_all_personas()
    
    # 아카이브 구조 출력
    archiver.show_archive_structure()
    
    # 결과 요약
    print("\n" + "=" * 60)
    print("📊 아카이빙 결과 요약")
    print("=" * 60)
    
    total_files = 0
    for persona_name, info in results.items():
        files_count = (info["파일현황"]["잔고파일"] + 
                      info["파일현황"]["거래내역파일"] + 
                      info["파일현황"]["일자별데이터"] + 
                      info["파일현황"]["투자자정보"] + 1)
        
        total_files += files_count
        
        print(f"📁 {persona_name}:")
        print(f"   💼 보유종목: {info['보유종목수']}개")
        print(f"   📄 총파일: {files_count}개")
        print(f"   📅 일자별데이터: {info['파일현황']['일자별데이터']}개")
        print(f"   👥 투자자정보: {info['파일현황']['투자자정보']}개")
    
    print(f"\n🎉 총 {len(results)}명의 페르소나, {total_files}개 파일 아카이빙 완료!")


if __name__ == "__main__":
    main() 