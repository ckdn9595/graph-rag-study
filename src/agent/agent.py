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

from .tools import create_text_to_sql_mcp_server, MCP_TOOL_NAMES, get_handler
from .prompts import PromptBuilder, create_default_prompt_builder


class TextToSqlAgent:
    """Cost Analytics SQL Agent - Claude Agent SDK 사용"""

    def __init__(
        self,
        context_method: str = "yaml",
        max_turns: int = 100,
        max_validation_retries: int = 3,
        prompt_builder: PromptBuilder | None = None,
    ):
        """
        Args:
            context_method: "yaml" 또는 "graph"
            max_turns: 최대 대화 턴 수
            max_validation_retries: SQL 검증 최대 재시도 횟수
            prompt_builder: 커스텀 PromptBuilder (None이면 기본값 사용)
        """
        self.context_method = context_method
        self.max_turns = max_turns
        self.max_validation_retries = max_validation_retries

        # MCP 서버 생성
        self.mcp_server = create_text_to_sql_mcp_server()

        # 프롬프트 빌더 설정
        if prompt_builder is None:
            self.prompt_builder = create_default_prompt_builder(
                context_method=context_method,
                max_validation_retries=max_validation_retries,
                include_mart_rules=True,
            )
        else:
            self.prompt_builder = prompt_builder

        # 시스템 프롬프트 빌드
        self.system_prompt = self.prompt_builder.build()

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

    def _handle_slash_command(self, command: str) -> bool:
        """
        슬래시 명령어 처리

        Args:
            command: 사용자 입력 (예: "/graph", "/tables")

        Returns:
            True: 명령어 처리됨 (질문으로 전달하지 않음)
            False: 일반 질문으로 처리
        """
        from .tools import get_handler

        cmd = command.lower().strip()

        if cmd == "/help":
            print("""
사용 가능한 명령어:
  /graph    - 스키마 그래프를 HTML로 시각화 (브라우저에서 열림)
  /tables   - 테이블 목록 출력
  /schema   - 전체 스키마 정보 출력
  /help     - 이 도움말 출력
  exit, quit, q - 종료
""")
            return True

        if cmd == "/graph":
            if self.context_method != "graph":
                print("오류: /graph 명령은 graph 모드에서만 사용 가능합니다.")
                print("  실행 시 -m graph 옵션을 사용하세요.")
                return True

            try:
                handler = get_handler()
                output_path = handler.context.visualize()
                print(f"그래프 시각화 파일 생성: {output_path}")
            except Exception as e:
                print(f"오류: 그래프 시각화 실패 - {e}")
            return True

        if cmd == "/tables":
            try:
                handler = get_handler()
                tables = handler.context.list_tables()
                print(f"\n테이블 목록 ({len(tables)}개):")
                for t in tables:
                    print(f"  - {t['name']}: {t['description']}")
                print()
            except Exception as e:
                print(f"오류: {e}")
            return True

        if cmd == "/schema":
            try:
                handler = get_handler()
                schema_context = handler.context.get_all_schema_context()
                print(schema_context)
            except Exception as e:
                print(f"오류: {e}")
            return True

        return False  # 일반 질문으로 처리

    async def _run_interactive_async(self):
        """비동기 대화형 모드 실행"""
        print(f"Text to Sql Agent 시작 (모드: {self.context_method})")
        print("질문을 입력하세요. 명령어는 /help 를 입력하세요.\n")

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

                    # 슬래시 명령어 처리
                    if question.startswith("/"):
                        self._handle_slash_command(question)
                        continue

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

                    # 실행된 쿼리 정보 출력
                    try:
                        handler = get_handler()
                        if hasattr(handler, '_last_executed_sql') and handler._last_executed_sql:
                            print(f"\n[실행된 쿼리]")
                            print(f"SQL: {handler._last_executed_sql}")
                            if handler._last_result:
                                print(f"결과: {len(handler._last_result)}건")
                        if hasattr(handler, '_last_csv_path') and handler._last_csv_path:
                            print(f"CSV 저장: {handler._last_csv_path}")
                    except RuntimeError:
                        pass  # Handler not initialized

                    print(f"\n(Tool 호출 {tool_count}회)")
                    print("-" * 50)

                except KeyboardInterrupt:
                    print("\n\nAgent를 종료합니다.")
                    break
                except Exception as e:
                    print(f"\n오류 발생: {e}\n")

    def run_interactive(self):
        """동기 대화형 모드 실행"""
        anyio.run(self._run_interactive_async)
