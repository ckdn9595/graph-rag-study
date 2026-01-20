"""Claude Agent SDK Tool 정의 및 MCP 서버 생성"""

from typing import Any, Optional

from claude_agent_sdk import tool, create_sdk_mcp_server

from ..context.metadata_rag import MetadataRAG
from ..context.graph_rag import SchemaGraph
from ..sql.validator import SQLValidator
from ..sql.executor import ParallelExecutor
from ..export.csv_exporter import CSVExporter


class ToolHandler:
    """Tool 실행 핸들러 - MCP 서버 도구들의 상태를 관리"""

    def __init__(
        self,
        context_method: str,
        metadata_path: str,
        db_config: dict,
        output_dir: str = "./output",
    ):
        """
        Args:
            context_method: "yaml" 또는 "graph"
            metadata_path: 스키마 메타데이터 파일 경로
            db_config: DB 연결 설정
            output_dir: CSV 출력 디렉토리
        """
        self.context_method = context_method
        self.db_config = db_config

        # 컨텍스트 시스템 초기화
        if context_method == "yaml":
            self.context = MetadataRAG(metadata_path)
        elif context_method == "graph":
            self.context = SchemaGraph(metadata_path)
        else:
            raise ValueError(f"지원하지 않는 context_method: {context_method}")

        # SQL 모듈 초기화
        self.validator = SQLValidator(db_config)
        self.executor = ParallelExecutor(db_config)
        self.exporter = CSVExporter(output_dir)

        # 마지막 쿼리 결과 저장 (export_csv에서 사용)
        self._last_result: list = []

    def close(self):
        """리소스 정리"""
        self.validator.close()


# 전역 핸들러 인스턴스
_handler: Optional[ToolHandler] = None


def init_handler(
    context_method: str,
    metadata_path: str,
    db_config: dict,
    output_dir: str = "./output",
):
    """전역 핸들러 초기화"""
    global _handler
    _handler = ToolHandler(
        context_method=context_method,
        metadata_path=metadata_path,
        db_config=db_config,
        output_dir=output_dir,
    )


def close_handler():
    """전역 핸들러 종료"""
    global _handler
    if _handler:
        _handler.close()
        _handler = None


def get_handler() -> ToolHandler:
    """전역 핸들러 반환"""
    if _handler is None:
        raise RuntimeError("ToolHandler가 초기화되지 않았습니다. init_handler()를 먼저 호출하세요.")
    return _handler


# =============================================================================
# MCP Tool 정의 (@tool 데코레이터 사용)
# =============================================================================

@tool(
    name="list_tables",
    description="사용 가능한 모든 테이블 목록을 조회합니다. 먼저 이 도구로 어떤 테이블이 있는지 확인하세요.",
    schema={}
)
async def list_tables(args: dict[str, Any]) -> dict[str, Any]:
    """테이블 목록 조회"""
    handler = get_handler()
    tables = handler.context.list_tables()

    result = {
        "tables": tables,
        "count": len(tables),
    }

    return {
        "content": [{"type": "text", "text": str(result)}]
    }


@tool(
    name="get_schema_info",
    description="특정 테이블의 스키마 정보(컬럼, 타입, 설명)와 다른 테이블과의 관계를 조회합니다.",
    schema={"table_name": str}
)
async def get_schema_info(args: dict[str, Any]) -> dict[str, Any]:
    """테이블 스키마 정보 조회"""
    handler = get_handler()
    table_name = args.get("table_name")

    if not table_name:
        return {
            "content": [{"type": "text", "text": "오류: table_name이 필요합니다."}],
            "is_error": True
        }

    info = handler.context.get_table_info(table_name)
    if info is None:
        return {
            "content": [{"type": "text", "text": f"오류: 테이블을 찾을 수 없습니다: {table_name}"}],
            "is_error": True
        }

    return {
        "content": [{"type": "text", "text": str(info)}]
    }


@tool(
    name="search_schema",
    description="키워드로 관련 테이블과 컬럼을 검색합니다. 어떤 테이블을 사용해야 할지 모를 때 유용합니다.",
    schema={"keyword": str}
)
async def search_schema(args: dict[str, Any]) -> dict[str, Any]:
    """키워드로 스키마 검색"""
    handler = get_handler()
    keyword = args.get("keyword")

    if not keyword:
        return {
            "content": [{"type": "text", "text": "오류: keyword가 필요합니다."}],
            "is_error": True
        }

    results = handler.context.search_tables_by_keyword(keyword)
    result = {
        "results": results,
        "count": len(results),
    }

    return {
        "content": [{"type": "text", "text": str(result)}]
    }


@tool(
    name="get_join_hint",
    description="두 테이블 간의 조인 조건을 조회합니다.",
    schema={"table1": str, "table2": str}
)
async def get_join_hint(args: dict[str, Any]) -> dict[str, Any]:
    """조인 힌트 조회"""
    handler = get_handler()
    table1 = args.get("table1")
    table2 = args.get("table2")

    if not table1 or not table2:
        return {
            "content": [{"type": "text", "text": "오류: table1과 table2가 필요합니다."}],
            "is_error": True
        }

    # MetadataRAG의 경우
    if hasattr(handler.context, "get_join_hint"):
        hint = handler.context.get_join_hint(table1, table2)
    # SchemaGraph의 경우
    elif hasattr(handler.context, "find_join_path"):
        hint = handler.context.find_join_path(table1, table2)
    else:
        return {
            "content": [{"type": "text", "text": "오류: 조인 힌트를 조회할 수 없습니다."}],
            "is_error": True
        }

    if hint is None:
        return {
            "content": [{"type": "text", "text": f"오류: {table1}과 {table2} 사이의 관계를 찾을 수 없습니다."}],
            "is_error": True
        }

    return {
        "content": [{"type": "text", "text": str(hint)}]
    }


@tool(
    name="validate_sql",
    description="SQL 쿼리의 문법과 테이블/컬럼 존재 여부를 검증합니다. 실행 전에 반드시 이 도구로 검증하세요.",
    schema={"sql": str}
)
async def validate_sql(args: dict[str, Any]) -> dict[str, Any]:
    """SQL 검증"""
    handler = get_handler()
    sql = args.get("sql")

    if not sql:
        return {
            "content": [{"type": "text", "text": "오류: sql이 필요합니다."}],
            "is_error": True
        }

    result = handler.validator.validate(sql)

    # 에러 시 수정 제안 추가
    if not result["is_valid"] and result["errors"]:
        error_type = result["errors"]["type"]
        error_msg = result["errors"]["message"]
        result["suggestion"] = handler.validator.get_error_suggestion(
            error_type, error_msg
        )

    return {
        "content": [{"type": "text", "text": str(result)}]
    }


@tool(
    name="execute_sql",
    description="검증된 SQL 쿼리를 실행합니다. 대용량 데이터는 자동으로 병렬 처리됩니다.",
    schema={"sql": str, "parallel": bool}
)
async def execute_sql(args: dict[str, Any]) -> dict[str, Any]:
    """SQL 실행"""
    handler = get_handler()
    sql = args.get("sql")

    if not sql:
        return {
            "content": [{"type": "text", "text": "오류: sql이 필요합니다."}],
            "is_error": True
        }

    parallel = args.get("parallel", True)

    result = handler.executor.execute(sql, parallel=parallel)

    # 성공 시 결과 저장 (export_csv용)
    if result["success"]:
        handler._last_result = result["data"]

    # 결과가 크면 요약
    if result["success"] and len(result["data"]) > 10:
        summary = {
            "success": True,
            "row_count": result["row_count"],
            "preview": result["data"][:10],
            "message": f"총 {result['row_count']}건 조회됨. 상위 10건만 표시."
        }
        return {
            "content": [{"type": "text", "text": str(summary)}]
        }

    return {
        "content": [{"type": "text", "text": str(result)}]
    }


@tool(
    name="export_csv",
    description="쿼리 결과를 CSV 파일로 저장합니다.",
    schema={"filename": str}
)
async def export_csv(args: dict[str, Any]) -> dict[str, Any]:
    """CSV 내보내기"""
    handler = get_handler()

    if not handler._last_result:
        return {
            "content": [{"type": "text", "text": "오류: 내보낼 데이터가 없습니다. 먼저 execute_sql을 실행하세요."}],
            "is_error": True
        }

    filename = args.get("filename", "query_result")

    result = handler.exporter.export(
        data=handler._last_result,
        filename=filename,
        include_timestamp=True,
    )

    return {
        "content": [{"type": "text", "text": str(result)}]
    }


# =============================================================================
# MCP 서버 생성 함수
# =============================================================================

def create_cost_analytics_mcp_server():
    """Cost Analytics MCP 서버 생성"""
    return create_sdk_mcp_server(
        name="cost_analytics",
        version="1.0.0",
        tools=[
            list_tables,
            get_schema_info,
            search_schema,
            get_join_hint,
            validate_sql,
            execute_sql,
            export_csv,
        ]
    )


# MCP Tool 이름 목록 (allowed_tools 설정용)
MCP_TOOL_NAMES = [
    "mcp__cost_analytics__list_tables",
    "mcp__cost_analytics__get_schema_info",
    "mcp__cost_analytics__search_schema",
    "mcp__cost_analytics__get_join_hint",
    "mcp__cost_analytics__validate_sql",
    "mcp__cost_analytics__execute_sql",
    "mcp__cost_analytics__export_csv",
]
