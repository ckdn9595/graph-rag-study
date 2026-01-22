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

        # 실행 결과 추적 (agent에서 결과 구조 생성 시 사용)
        self._last_result: list = []
        self._last_executed_sql: str = ""
        self._last_csv_path: Optional[str] = None

        # 디버그용: 모든 실행 쿼리 추적
        self._all_executed_queries: list = []  # [{"sql": str, "success": bool, "row_count": int, "data": list}]

    def reset_query_history(self):
        """쿼리 히스토리 초기화 (새 질문 시작 시 호출)"""
        self._all_executed_queries = []
        self._last_result = []
        self._last_executed_sql = ""
        self._last_csv_path = None

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

@tool("list_tables", "사용 가능한 모든 테이블 목록을 조회합니다. 먼저 이 도구로 어떤 테이블이 있는지 확인하세요.", {})
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


@tool("get_schema_info", "특정 테이블의 스키마 정보(컬럼, 타입, 설명)와 다른 테이블과의 관계를 조회합니다.", {"table_name": str})
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


@tool("search_schema", "키워드로 관련 테이블과 컬럼을 검색합니다. 어떤 테이블을 사용해야 할지 모를 때 유용합니다.", {"keyword": str})
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


@tool("get_join_hint", "두 테이블 간의 조인 조건을 조회합니다.", {"table1": str, "table2": str})
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


@tool("validate_sql", "SQL 쿼리의 문법과 테이블/컬럼 존재 여부를 검증합니다. 실행 전에 반드시 이 도구로 검증하세요.", {"sql": str})
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


@tool("execute_sql", "검증된 SQL 쿼리를 실행합니다. 대용량 데이터는 자동으로 병렬 처리됩니다.", {"sql": str, "parallel": bool})
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

    # 모든 실행 쿼리를 히스토리에 추가 (디버그용)
    handler._all_executed_queries.append({
        "sql": sql,
        "success": result["success"],
        "row_count": result.get("row_count", 0),
        "data": result.get("data", []),
        "error": result.get("error") if not result["success"] else None,
    })

    # 성공 시 결과 저장
    if result["success"]:
        handler._last_result = result["data"]
        handler._last_executed_sql = sql

    # 결과가 크면 요약
    if result["success"] and len(result["data"]) > 20:
        summary = {
            "success": True,
            "row_count": result["row_count"],
            "preview": result["data"][:20],
            "message": f"총 {result['row_count']}건 조회됨. 상위 20건만 표시."
        }
        return {
            "content": [{"type": "text", "text": str(summary)}]
        }

    return {
        "content": [{"type": "text", "text": str(result)}]
    }


@tool("export_csv", "쿼리 결과를 CSV 파일로 저장합니다.", {"filename": str})
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

    # CSV 경로 저장
    if result["success"]:
        handler._last_csv_path = result["file_path"]

    return {
        "content": [{"type": "text", "text": str(result)}]
    }


@tool(
    "get_optimal_join_path",
    "여러 테이블을 조인할 최적 경로를 찾습니다. (Graph 모드 전용) 3개 이상의 테이블을 조인해야 할 때 사용하세요. 최단 경로와 조인 조건을 반환합니다.",
    {"tables": list}
)
async def get_optimal_join_path(args: dict[str, Any]) -> dict[str, Any]:
    """
    여러 테이블 간 최적 조인 경로 찾기 (Graph 모드 전용)

    NetworkX 그래프 기반으로 Steiner Tree 근사 알고리즘을 사용하여
    주어진 테이블들을 모두 연결하는 최적 경로를 찾습니다.

    Args:
        tables: 조인할 테이블 목록 (예: ["tbil_cmpn_l", "tbil_aws_ak_l", "t_aws_mart_shard_l"])

    Returns:
        최적 조인 경로, 조인 조건, SQL JOIN 절 예시
    """
    handler = get_handler()

    # Graph 모드 전용 체크
    if handler.context_method != "graph":
        return {
            "content": [{
                "type": "text",
                "text": "오류: get_optimal_join_path는 Graph 모드에서만 사용 가능합니다. YAML 모드에서는 get_join_hint를 테이블 쌍마다 호출하세요."
            }],
            "is_error": True
        }

    tables = args.get("tables")

    if not tables:
        return {
            "content": [{"type": "text", "text": "오류: tables 목록이 필요합니다."}],
            "is_error": True
        }

    if not isinstance(tables, list) or len(tables) < 2:
        return {
            "content": [{"type": "text", "text": "오류: tables는 2개 이상의 테이블명 리스트여야 합니다."}],
            "is_error": True
        }

    # SchemaGraph의 get_multi_hop_path 호출
    path_info = handler.context.get_multi_hop_path(tables)

    if path_info is None:
        # 어떤 테이블이 없는지 확인
        missing = [t for t in tables if t not in handler.context.graph]
        if missing:
            return {
                "content": [{
                    "type": "text",
                    "text": f"오류: 다음 테이블을 찾을 수 없습니다: {missing}"
                }],
                "is_error": True
            }
        return {
            "content": [{
                "type": "text",
                "text": f"오류: {tables} 사이에 연결 경로가 없습니다. 관계가 정의되지 않았거나 연결되지 않은 테이블이 있습니다."
            }],
            "is_error": True
        }

    # SQL JOIN 절 예시 생성
    join_clauses = []
    base_table = path_info["path"][0]

    for join in path_info["joins"]:
        # from: table1.col → to: table2.col 형태의 condition
        condition = join["condition"]
        join_clauses.append(f"JOIN {join['to']} ON {condition}")

    join_sql_example = f"FROM {base_table}\n" + "\n".join(join_clauses)

    result = {
        "requested_tables": tables,
        "optimal_path": path_info["path"],
        "total_hops": path_info["total_hops"],
        "joins": path_info["joins"],
        "join_sql_example": join_sql_example,
        "note": "실제 쿼리 시 필요한 WHERE 조건(site_id, ym 등)을 추가하세요."
    }

    return {
        "content": [{"type": "text", "text": str(result)}]
    }


# =============================================================================
# MCP 서버 생성 함수
# =============================================================================

def create_text_to_sql_mcp_server():
    """text_to_sql MCP 서버 생성"""
    return create_sdk_mcp_server(
        name="text_to_sql",
        version="1.0.0",
        tools=[
            list_tables,
            get_schema_info,
            search_schema,
            get_join_hint,
            get_optimal_join_path,  # Graph 모드 전용
            validate_sql,
            execute_sql,
            export_csv,
        ]
    )


# MCP Tool 이름 목록 (allowed_tools 설정용)
MCP_TOOL_NAMES = [
    "mcp__text_to_sql__list_tables",
    "mcp__text_to_sql__get_schema_info",
    "mcp__text_to_sql__search_schema",
    "mcp__text_to_sql__get_join_hint",
    "mcp__text_to_sql__get_optimal_join_path",  # Graph 모드 전용
    "mcp__text_to_sql__validate_sql",
    "mcp__text_to_sql__execute_sql",
    "mcp__text_to_sql__export_csv",
]
