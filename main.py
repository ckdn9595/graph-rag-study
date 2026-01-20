#!/usr/bin/env python3
"""Cost Analytics Agent CLI 엔트리포인트

Claude Agent SDK 기반으로 동작합니다.
- API 키 불필요 (로컬 Claude CLI 사용)
- Claude Pro/Team 구독 필요
"""

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

# 프로젝트 루트를 path에 추가
sys.path.insert(0, str(Path(__file__).parent))

from src.agent.agent import CostAnalyticsAgent
from src.agent.tools import init_handler, close_handler


def load_config():
    """환경변수에서 설정 로드"""
    load_dotenv()

    db_config = {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", "3306")),
        "user": os.getenv("DB_USER", "root"),
        "password": os.getenv("DB_PASSWORD", ""),
        "database": os.getenv("DB_NAME", ""),
        "charset": "utf8mb4",
    }

    return {
        "db_config": db_config,
        "default_method": os.getenv("DEFAULT_METHOD", "yaml"),
        "max_validation_retries": int(os.getenv("MAX_VALIDATION_RETRIES", "3")),
        "max_turns": int(os.getenv("MAX_TURNS", "10")),
    }


def main():
    parser = argparse.ArgumentParser(
        description="Cost Analytics Agent - 자연어 질문을 SQL로 변환하여 실행",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예시:
  # YAML 모드로 질문 실행
  python main.py --method yaml --question "지난달 A고객 EC2 비용은?"

  # Graph 모드로 대화형 실행
  python main.py --method graph --interactive

  # 기본 모드로 질문 실행
  python main.py -q "2024년 12월 전체 비용 요약"
        """,
    )

    parser.add_argument(
        "-m", "--method",
        type=str,
        choices=["yaml", "graph"],
        help="컨텍스트 조회 방식 (yaml: 메타데이터, graph: NetworkX)",
    )

    parser.add_argument(
        "-q", "--question",
        type=str,
        help="실행할 질문",
    )

    parser.add_argument(
        "-i", "--interactive",
        action="store_true",
        help="대화형 모드 실행",
    )

    parser.add_argument(
        "--metadata",
        type=str,
        default="./data/schema_metadata.yaml",
        help="스키마 메타데이터 파일 경로 (기본: ./data/schema_metadata.yaml)",
    )

    parser.add_argument(
        "--output-dir",
        type=str,
        default="./output",
        help="CSV 출력 디렉토리 (기본: ./output)",
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="상세 출력 모드",
    )

    args = parser.parse_args()

    # 설정 로드
    config = load_config()

    # 메서드 결정 (인자 > 환경변수 > 기본값)
    method = args.method or config["default_method"]

    # 메타데이터 파일 확인
    metadata_path = Path(args.metadata)
    if not metadata_path.exists():
        print(f"오류: 메타데이터 파일을 찾을 수 없습니다: {metadata_path}")
        print("data/schema_metadata.yaml 파일을 생성하세요.")
        sys.exit(1)

    # DB 설정 확인
    if not config["db_config"]["database"]:
        print("오류: DB_NAME 환경변수가 설정되지 않았습니다.")
        print(".env 파일을 확인하세요.")
        sys.exit(1)

    if args.verbose:
        print(f"설정:")
        print(f"  - 모드: {method}")
        print(f"  - 메타데이터: {metadata_path}")
        print(f"  - DB: {config['db_config']['host']}:{config['db_config']['port']}/{config['db_config']['database']}")
        print(f"  - 출력 디렉토리: {args.output_dir}")
        print()

    try:
        # Tool 핸들러 초기화
        init_handler(
            context_method=method,
            metadata_path=str(metadata_path),
            db_config=config["db_config"],
            output_dir=args.output_dir,
        )

        # Agent 생성
        agent = CostAnalyticsAgent(
            context_method=method,
            max_turns=config["max_turns"],
            max_validation_retries=config["max_validation_retries"],
        )

        # 실행 모드 결정
        if args.interactive:
            agent.run_interactive()

        elif args.question:
            print(f"질문: {args.question}\n")
            print("처리 중...\n")

            result = agent.run(args.question)

            print(f"답변:\n{result['answer']}\n")

            if args.verbose:
                print(f"\n[디버그] Tool 호출 {len(result['tool_calls'])}회:")
                for i, call in enumerate(result["tool_calls"], 1):
                    print(f"  {i}. {call['tool']}")

        else:
            parser.print_help()
            print("\n오류: --question 또는 --interactive 옵션이 필요합니다.")
            sys.exit(1)

    except NotImplementedError as e:
        print(f"\n오류: {e}")
        print("--method yaml 옵션을 사용해주세요.")
        sys.exit(1)

    except KeyboardInterrupt:
        print("\n\n중단됨")

    except Exception as e:
        print(f"\n오류: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)

    finally:
        close_handler()


if __name__ == "__main__":
    main()
