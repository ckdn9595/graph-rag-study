"""YAML 메타데이터 기반 스키마 조회 모듈"""

import yaml
from pathlib import Path
from typing import Optional


class MetadataRAG:
    """YAML 메타데이터에서 스키마 정보를 조회하는 클래스"""

    def __init__(self, metadata_path: str):
        """
        Args:
            metadata_path: schema_metadata.yaml 파일 경로
        """
        self.metadata_path = Path(metadata_path)
        self.metadata = self._load_metadata()

    def _load_metadata(self) -> dict:
        """YAML 파일 로드"""
        if not self.metadata_path.exists():
            raise FileNotFoundError(f"메타데이터 파일을 찾을 수 없습니다: {self.metadata_path}")

        with open(self.metadata_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def get_table_info(self, table_name: str) -> Optional[dict]:
        """
        테이블 정보 조회

        Args:
            table_name: 조회할 테이블명

        Returns:
            테이블 정보 dict 또는 None
        """
        tables = self.metadata.get("tables", {})

        if table_name not in tables:
            return None

        table_data = tables[table_name]

        # 해당 테이블과 관련된 관계 정보 추출
        relationships = self._get_relationships_for_table(table_name)

        return {
            "table": table_name,
            "description": table_data.get("description", ""),
            "source": table_data.get("source", ""),
            "columns": table_data.get("columns", []),
            "relationships": relationships,
        }

    def _get_relationships_for_table(self, table_name: str) -> list:
        """테이블과 관련된 모든 관계 정보 추출"""
        relationships = self.metadata.get("relationships", [])
        related = []

        for rel in relationships:
            from_table = rel["from"].split(".")[0]
            to_table = rel["to"].split(".")[0]

            if from_table == table_name or to_table == table_name:
                related.append({
                    "from": rel["from"],
                    "to": rel["to"],
                    "type": rel.get("type", ""),
                    "description": rel.get("description", ""),
                })

        return related

    def list_tables(self) -> list:
        """모든 테이블 목록 반환"""
        tables = self.metadata.get("tables", {})
        return [
            {
                "name": name,
                "description": info.get("description", ""),
            }
            for name, info in tables.items()
        ]

    def get_all_schema_context(self) -> str:
        """
        전체 스키마를 컨텍스트 문자열로 반환
        (LLM에 전달할 용도)
        """
        lines = ["# 데이터베이스 스키마 정보\n"]

        # 테이블 정보
        tables = self.metadata.get("tables", {})
        for table_name, table_info in tables.items():
            lines.append(f"## 테이블: {table_name}")
            lines.append(f"설명: {table_info.get('description', '')}")
            lines.append(f"소스: {table_info.get('source', '')}")
            lines.append("컬럼:")

            for col in table_info.get("columns", []):
                lines.append(f"  - {col['name']} ({col['type']}): {col.get('description', '')}")

            lines.append("")

        # 관계 정보
        relationships = self.metadata.get("relationships", [])
        if relationships:
            lines.append("## 테이블 관계")
            for rel in relationships:
                lines.append(f"  - {rel['from']} → {rel['to']} ({rel.get('type', '')})")
                if rel.get("description"):
                    lines.append(f"    설명: {rel['description']}")
            lines.append("")

        # 비즈니스 용어
        glossary = self.metadata.get("business_glossary", {})
        if glossary:
            lines.append("## 비즈니스 용어 매핑")
            for term, mapping in glossary.items():
                lines.append(f"  - \"{term}\" → {mapping}")

        return "\n".join(lines)

    def search_tables_by_keyword(self, keyword: str) -> list:
        """
        키워드로 관련 테이블 검색

        Args:
            keyword: 검색할 키워드

        Returns:
            관련 테이블 정보 리스트
        """
        keyword_lower = keyword.lower()
        results = []

        tables = self.metadata.get("tables", {})
        for table_name, table_info in tables.items():
            # 테이블명, 설명, 컬럼명/설명에서 검색
            matched = False

            if keyword_lower in table_name.lower():
                matched = True
            elif keyword_lower in table_info.get("description", "").lower():
                matched = True
            else:
                for col in table_info.get("columns", []):
                    if keyword_lower in col["name"].lower():
                        matched = True
                        break
                    if keyword_lower in col.get("description", "").lower():
                        matched = True
                        break

            if matched:
                results.append(self.get_table_info(table_name))

        # 비즈니스 용어 매핑도 검색
        glossary = self.metadata.get("business_glossary", {})
        for term, mapping in glossary.items():
            if keyword_lower in term.lower():
                # 매핑된 테이블/컬럼 정보 추가
                results.append({
                    "type": "glossary",
                    "term": term,
                    "mapping": mapping,
                })

        return results

    def get_join_hint(self, table1: str, table2: str) -> Optional[dict]:
        """
        두 테이블 간의 조인 힌트 반환

        Args:
            table1: 첫 번째 테이블명
            table2: 두 번째 테이블명

        Returns:
            조인 조건 정보 또는 None
        """
        relationships = self.metadata.get("relationships", [])

        for rel in relationships:
            from_table = rel["from"].split(".")[0]
            to_table = rel["to"].split(".")[0]
            from_col = rel["from"].split(".")[1]
            to_col = rel["to"].split(".")[1]

            # 양방향으로 체크
            if (from_table == table1 and to_table == table2) or \
               (from_table == table2 and to_table == table1):
                return {
                    "join_condition": f"{rel['from']} = {rel['to']}",
                    "type": rel.get("type", ""),
                    "description": rel.get("description", ""),
                }

        return None
