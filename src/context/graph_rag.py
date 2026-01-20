"""NetworkX 기반 스키마 그래프 모듈 (인터페이스만)"""

from typing import Optional


class SchemaGraph:
    """
    NetworkX 기반 스키마 그래프

    테이블을 노드로, FK 관계를 엣지로 표현하여
    복잡한 조인 경로 탐색을 지원합니다.

    [TODO] 이 클래스는 인터페이스만 정의되어 있습니다.
    실제 구현은 NetworkX를 사용하여 추후 완성 예정입니다.
    """

    def __init__(self, metadata_path: str):
        """
        스키마 메타데이터를 로드하여 그래프 구축

        Args:
            metadata_path: schema_metadata.yaml 파일 경로
        """
        raise NotImplementedError(
            "Graph RAG는 아직 구현되지 않았습니다. "
            "--method yaml 옵션을 사용해주세요."
        )

    def get_table_info(self, table_name: str) -> Optional[dict]:
        """
        테이블 정보 + 연결된 테이블 정보 반환

        Args:
            table_name: 조회할 테이블명

        Returns:
            {
                "table": str,
                "description": str,
                "columns": list,
                "connected_tables": [
                    {
                        "table": str,
                        "join_condition": str,
                        "relationship": str
                    }
                ]
            }
        """
        raise NotImplementedError("Graph RAG는 아직 구현되지 않았습니다.")

    def find_join_path(self, from_table: str, to_table: str) -> Optional[dict]:
        """
        두 테이블 간 최단 조인 경로 찾기

        Args:
            from_table: 시작 테이블
            to_table: 목표 테이블

        Returns:
            {
                "path": ["table1", "table2", "table3"],
                "joins": [
                    {
                        "from": "table1",
                        "to": "table2",
                        "condition": "table1.col = table2.col"
                    }
                ]
            }
        """
        raise NotImplementedError("Graph RAG는 아직 구현되지 않았습니다.")

    def list_tables(self) -> list:
        """
        모든 테이블 목록 반환

        Returns:
            [{"name": str, "description": str}, ...]
        """
        raise NotImplementedError("Graph RAG는 아직 구현되지 않았습니다.")

    def get_all_schema_context(self) -> str:
        """
        전체 스키마를 컨텍스트 문자열로 반환

        Returns:
            LLM에 전달할 스키마 정보 문자열
        """
        raise NotImplementedError("Graph RAG는 아직 구현되지 않았습니다.")

    def search_tables_by_keyword(self, keyword: str) -> list:
        """
        키워드로 관련 테이블 검색

        Args:
            keyword: 검색할 키워드

        Returns:
            관련 테이블 정보 리스트
        """
        raise NotImplementedError("Graph RAG는 아직 구현되지 않았습니다.")

    def get_multi_hop_path(self, tables: list) -> Optional[dict]:
        """
        여러 테이블을 연결하는 최적 경로 찾기

        Args:
            tables: 연결할 테이블 목록

        Returns:
            최적 조인 경로 정보
        """
        raise NotImplementedError("Graph RAG는 아직 구현되지 않았습니다.")
