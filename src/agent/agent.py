"""Claude Agent SDK 기반 Cost Analytics Agent"""

import anyio

from claude_agent_sdk import (
    ClaudeSDKClient,
    ClaudeAgentOptions,
    AssistantMessage,
    ResultMessage,
    TextBlock,
    ToolUseBlock,
)

from .tools import create_text_to_sql_mcp_server, MCP_TOOL_NAMES


class TextToSqlAgent:
    """Cost Analytics SQL Agent - Claude Agent SDK 사용"""

    def __init__(
        self,
        context_method: str = "yaml",
        max_turns: int = 10,
        max_validation_retries: int = 3,
    ):
        """
        Args:
            context_method: "yaml" 또는 "graph"
            max_turns: 최대 대화 턴 수
            max_validation_retries: SQL 검증 최대 재시도 횟수
        """
        self.context_method = context_method
        self.max_turns = max_turns
        self.max_validation_retries = max_validation_retries

        # MCP 서버 생성
        self.mcp_server = create_text_to_sql_mcp_server()

        # 시스템 프롬프트
        self.system_prompt = self._build_system_prompt()

    def _build_system_prompt(self) -> str:
        """시스템 프롬프트 생성"""
        return f"""당신은 Text to SQL Agent입니다.

## 역할
- 사용자의 자연어 질문을 분석하여 적절한 SQL 쿼리를 생성합니다.
- RDB에서 쿼리를 실행하고 결과를 반환합니다.

## 작업 흐름
1. **테이블 파악**: 먼저 `list_tables`로 사용 가능한 테이블을 확인합니다.
2. **스키마 조회**: `get_schema_info`로 관련 테이블의 컬럼 정보를 확인합니다.
3. **관계 확인**: 여러 테이블 조인이 필요하면 `get_join_hint`로 조인 조건을 확인합니다.
4. **SQL 생성**: 정보를 바탕으로 SQL 쿼리를 작성합니다.
5. **검증**: `validate_sql`로 쿼리를 검증합니다. 실패 시 수정하여 재시도합니다 (최대 {self.max_validation_retries}회).
6. **실행**: 검증 통과 후 `execute_sql`로 실행합니다.
7. **저장**: 필요시 `export_csv`로 결과를 CSV로 저장합니다.

## 규칙
- SELECT 쿼리만 생성하세요. INSERT, UPDATE, DELETE는 금지입니다.
- 반드시 validate_sql로 검증 후 execute_sql을 실행하세요.
- 검증 실패 시 에러 메시지와 suggestion을 참고하여 수정하세요.
- 사용자가 요청하면 결과를 CSV로 저장하세요.

## 컨텍스트 조회 방식
현재 모드: **{self.context_method}**
- yaml: YAML 메타데이터 문서에서 스키마 정보 조회
- graph: NetworkX 그래프에서 테이블 관계 탐색 (조인 경로 추론 가능)

## 응답 형식
- 실행한 SQL 쿼리를 명시하세요.
- 쿼리 결과의 주요 내용을 요약해서 설명하세요.
- 데이터가 많으면 상위 몇 개만 보여주고 전체 건수를 알려주세요.
- CSV로 저장한 경우 파일 경로를 알려주세요.
"""

    def _create_options(self) -> ClaudeAgentOptions:
        """ClaudeAgentOptions 생성"""
        return ClaudeAgentOptions(
            system_prompt=self.system_prompt,
            max_turns=self.max_turns,
            mcp_servers={"text_to_sql": self.mcp_server},
            allowed_tools=MCP_TOOL_NAMES,
            permission_mode="acceptEdits",  # 도구 자동 실행
        )

    async def _run_async(self, question: str) -> dict:
        """
        비동기 Agent 실행

        Args:
            question: 사용자 질문

        Returns:
            {
                "queries": [  # 실행된 쿼리 목록 (validation 실패는 제외)
                    {
                        "sql": str,
                        "success": bool,
                        "row_count": int,
                        "data": list[dict]
                    }
                ],
                "summary": str,  # Claude가 생성한 요약
                "csv_path": str | None,  # CSV 저장 시 경로
                "cost_usd": float | None,  # 비용 (USD)
            }
        """
        options = self._create_options()
        queries = []
        summary_parts = []
        csv_path = None
        cost_usd = None

        # Tool 호출 추적용
        pending_sql = {}  # tool_use_id -> sql

        async with ClaudeSDKClient(options=options) as client:
            await client.connect()
            await client.query(question)

            async for message in client.receive_response():
                if isinstance(message, AssistantMessage):
                    for block in message.content:
                        if isinstance(block, TextBlock):
                            summary_parts.append(block.text)
                        elif isinstance(block, ToolUseBlock):
                            # execute_sql 호출 시 sql 저장
                            if block.name.endswith("__execute_sql"):
                                pending_sql[block.id] = block.input.get("sql", "")

                elif isinstance(message, ResultMessage):
                    cost_usd = message.total_cost_usd

        # Tool 결과에서 실행된 쿼리 정보 추출
        # (ClaudeSDKClient는 tool result를 별도로 제공하지 않으므로
        #  ToolHandler에서 마지막 실행 결과를 가져옴)
        from .tools import get_handler
        try:
            handler = get_handler()
            if handler._last_result:
                # 마지막 실행된 쿼리 결과 추가
                queries.append({
                    "sql": handler._last_executed_sql if hasattr(handler, '_last_executed_sql') else "",
                    "success": True,
                    "row_count": len(handler._last_result),
                    "data": handler._last_result,
                })
            if hasattr(handler, '_last_csv_path') and handler._last_csv_path:
                csv_path = handler._last_csv_path
        except RuntimeError:
            pass  # Handler not initialized

        return {
            "queries": queries,
            "summary": "\n".join(summary_parts).strip(),
            "csv_path": csv_path,
            "cost_usd": cost_usd,
        }

    def run(self, question: str) -> dict:
        """
        동기 Agent 실행 (anyio.run 래퍼)

        Args:
            question: 사용자 질문

        Returns:
            {
                "queries": list,  # 실행된 쿼리 목록
                "summary": str,   # Claude 요약
                "csv_path": str | None,
                "cost_usd": float | None,
            }
        """
        return anyio.run(self._run_async, question)

    async def _run_interactive_async(self):
        """비동기 대화형 모드 실행"""
        print(f"Text to Sql Agent 시작 (모드: {self.context_method})")
        print("질문을 입력하세요. 종료하려면 'exit' 또는 'quit'를 입력하세요.\n")

        options = self._create_options()

        async with ClaudeSDKClient(options=options) as client:
            await client.connect()

            while True:
                try:
                    # 동기 input을 비동기 컨텍스트에서 사용
                    question = await anyio.to_thread.run_sync(
                        lambda: input("질문> ").strip()
                    )

                    if not question:
                        continue

                    if question.lower() in ("exit", "quit", "q"):
                        print("Agent를 종료합니다.")
                        break

                    print("\n처리 중...\n")

                    await client.query(question)

                    tool_count = 0
                    async for message in client.receive_response():
                        if isinstance(message, AssistantMessage):
                            for block in message.content:
                                if isinstance(block, TextBlock):
                                    print(f"\n답변:\n{block.text}")
                                elif isinstance(block, ToolUseBlock):
                                    tool_count += 1

                        elif isinstance(message, ResultMessage):
                            if message.total_cost_usd:
                                print(f"\n(비용: ${message.total_cost_usd:.4f})")

                    print(f"(Tool 호출 {tool_count}회)")
                    print("-" * 50)

                except KeyboardInterrupt:
                    print("\n\nAgent를 종료합니다.")
                    break
                except Exception as e:
                    print(f"\n오류 발생: {e}\n")

    def run_interactive(self):
        """동기 대화형 모드 실행"""
        anyio.run(self._run_interactive_async)
