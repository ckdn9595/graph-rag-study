"""Cost Analytics Agent - Streamlit UI

자연어로 비용 데이터를 조회하는 채팅 인터페이스
"""

import os
import sys
from pathlib import Path

import streamlit as st
import pandas as pd
from dotenv import load_dotenv

# 프로젝트 루트를 path에 추가
sys.path.insert(0, str(Path(__file__).parent))

from src.agent.agent import TextToSqlAgent
from src.agent.tools import init_handler, close_handler, get_handler


# =============================================================================
# 설정
# =============================================================================

def load_config():
    """환경변수에서 설정 로드"""
    load_dotenv()

    return {
        "db_config": {
            "host": os.getenv("DB_HOST", "localhost"),
            "port": int(os.getenv("DB_PORT", "3306")),
            "user": os.getenv("DB_USER", "root"),
            "password": os.getenv("DB_PASSWORD", ""),
            "database": os.getenv("DB_NAME", ""),
            "charset": "utf8mb4",
        },
        "max_validation_retries": int(os.getenv("MAX_VALIDATION_RETRIES", "3")),
        "max_turns": int(os.getenv("MAX_TURNS", "100")),
        "metadata_path": os.getenv("METADATA_PATH", "./data/schema_metadata.yaml"),
        "output_dir": os.getenv("OUTPUT_DIR", "./output"),
    }


# =============================================================================
# 페이지 설정
# =============================================================================

st.set_page_config(
    page_title="Cost Analytics Agent",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded",
)

# 커스텀 CSS
st.markdown("""
<style>
    .stChatMessage {
        padding: 1rem;
    }
    .sql-code {
        background-color: #1e1e1e;
        color: #d4d4d4;
        padding: 1rem;
        border-radius: 0.5rem;
        font-family: 'Consolas', monospace;
        font-size: 0.85rem;
        overflow-x: auto;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        text-align: center;
    }
    .metric-value {
        font-size: 1.5rem;
        font-weight: bold;
        color: #1f77b4;
    }
</style>
""", unsafe_allow_html=True)


# =============================================================================
# 세션 상태 초기화
# =============================================================================

def init_session_state():
    """세션 상태 초기화"""
    if "initialized" not in st.session_state:
        st.session_state.initialized = False
    if "context_method" not in st.session_state:
        st.session_state.context_method = None
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "agent" not in st.session_state:
        st.session_state.agent = None
    if "last_result" not in st.session_state:
        st.session_state.last_result = None
    if "tables_cache" not in st.session_state:
        st.session_state.tables_cache = None


def initialize_agent(context_method: str, config: dict):
    """Agent 초기화"""
    try:
        # 기존 핸들러 정리
        try:
            close_handler()
        except:
            pass

        # 새 핸들러 초기화
        init_handler(
            context_method=context_method,
            metadata_path=config["metadata_path"],
            db_config=config["db_config"],
            output_dir=config["output_dir"],
        )

        # Agent 생성
        agent = TextToSqlAgent(
            context_method=context_method,
            max_turns=config["max_turns"],
            max_validation_retries=config["max_validation_retries"],
        )

        return agent
    except Exception as e:
        st.error(f"Agent 초기화 실패: {e}")
        return None


# =============================================================================
# 사이드바
# =============================================================================

def render_sidebar(config: dict):
    """사이드바 렌더링"""
    with st.sidebar:
        st.title("⚙️ 설정")

        # 모드 선택 (초기화 전에만 변경 가능)
        st.subheader("Context 모드")

        if not st.session_state.initialized:
            context_method = st.radio(
                "스키마 조회 방식을 선택하세요:",
                options=["yaml", "graph"],
                index=0,
                format_func=lambda x: "📄 YAML (메타데이터)" if x == "yaml" else "🔗 Graph (NetworkX)",
                help="YAML: 메타데이터 파일 기반 | Graph: NetworkX 그래프 탐색 기반"
            )

            if st.button("시작하기", type="primary", use_container_width=True):
                with st.spinner("Agent 초기화 중..."):
                    agent = initialize_agent(context_method, config)
                    if agent:
                        st.session_state.agent = agent
                        st.session_state.context_method = context_method
                        st.session_state.initialized = True
                        st.session_state.messages = [{
                            "role": "assistant",
                            "content": f"**{context_method.upper()}** 모드로 text to sql이 시작되었습니다.\n\n"
                        }]
                        st.rerun()
        else:
            st.info(f"현재 모드: **{st.session_state.context_method.upper()}**")

            if st.button("🔄 모드 변경", use_container_width=True):
                st.session_state.initialized = False
                st.session_state.agent = None
                st.session_state.messages = []
                st.session_state.last_result = None
                st.session_state.tables_cache = None
                try:
                    close_handler()
                except:
                    pass
                st.rerun()

        st.divider()

        # Graph 시각화 버튼 (Graph 모드일 때만)
        if st.session_state.initialized and st.session_state.context_method == "graph":
            st.subheader("🔗 스키마 그래프")

            if st.button("그래프 DB UI HTML 다운", use_container_width=True):
                try:
                    handler = get_handler()
                    output_path = handler.context.visualize(open_browser=False)

                    # HTML 파일 읽기
                    with open(output_path, "r", encoding="utf-8") as f:
                        html_content = f.read()

                    # 다운로드 버튼 제공
                    st.download_button(
                        label="📥 schema_graph.html 다운로드",
                        data=html_content,
                        file_name="schema_graph.html",
                        mime="text/html",
                        use_container_width=True,
                    )

                    st.info("다운로드 후 브라우저에서 열어보세요!")
                except Exception as e:
                    st.error(f"시각화 실패: {e}")

            st.divider()

        # CSV 다운로드 (결과가 있을 때만)
        if st.session_state.last_result and st.session_state.last_result.get("queries"):
            st.subheader("📥 데이터 내보내기")

            for i, query in enumerate(st.session_state.last_result["queries"]):
                if query.get("data"):
                    df = pd.DataFrame(query["data"])
                    csv = df.to_csv(index=False, encoding="utf-8-sig")

                    st.download_button(
                        label=f"CSV 다운로드 ({len(query['data'])}건)",
                        data=csv,
                        file_name=f"query_result_{i+1}.csv",
                        mime="text/csv",
                        use_container_width=True,
                    )

        st.divider()



# =============================================================================
# 메인 채팅 영역
# =============================================================================

def render_chat():
    """채팅 영역 렌더링"""
    st.title("text to sql POC")

    if not st.session_state.initialized:
        # 미리보기
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("""
            ### 📄 YAML 모드
            - 메타데이터 파일 기반 스키마 조회
            - 빠른 키워드 검색
            - 단순한 조인 힌트 제공
            """)
        with col2:
            st.markdown("""
            ### 🔗 Graph 모드
            - 그래프 기반 탐색
            - 최단 조인 경로 자동 탐색
            - 다중 테이블 연결 경로 분석
            """)
        return

    # 메시지 히스토리 표시
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

            # 모든 쿼리 표시 (디버그용)
            if message.get("all_queries"):
                with st.expander(f"🔍 실행된 SQL 쿼리 ({len(message['all_queries'])}개)", expanded=False):
                    for i, query in enumerate(message["all_queries"], 1):
                        st.markdown(f"**쿼리 {i}** {'✅' if query['success'] else '❌'}")
                        st.code(query["sql"], language="sql")
                        if query["success"]:
                            st.caption(f"결과: {query['row_count']}건")
                        else:
                            st.error(f"에러: {query.get('error', 'Unknown error')}")
                        if i < len(message["all_queries"]):
                            st.divider()

            # 데이터 테이블 표시
            if message.get("data_preview"):
                with st.expander("📊 데이터 미리보기", expanded=True):
                    df = pd.DataFrame(message["data_preview"])
                    st.dataframe(df, use_container_width=True)

    # 채팅 입력
    if prompt := st.chat_input("질문을 입력하세요...", key="chat_input"):
        # 사용자 메시지 추가
        st.session_state.messages.append({"role": "user", "content": prompt})

        with st.chat_message("user"):
            st.markdown(prompt)

        # Agent 실행 (스트리밍)
        with st.chat_message("assistant"):
            try:
                # 진행 상황 표시 영역
                status_placeholder = st.empty()
                progress_container = st.container()

                tool_calls = []  # Tool 호출 기록
                final_result = None
                text_parts = []

                # 스트리밍 실행
                for event in st.session_state.agent.run_streaming(prompt):
                    if event["type"] == "status":
                        status_placeholder.info(event["message"])

                    elif event["type"] == "tool_call":
                        tool_name = event["name"]
                        tool_input = event["input"]
                        tool_calls.append({"name": tool_name, "input": tool_input})

                        # Tool 호출 표시
                        with progress_container:
                            tool_icon = {
                                "list_tables": "📋",
                                "get_schema_info": "🔍",
                                "search_schema": "🔎",
                                "get_join_hint": "🔗",
                                "get_optimal_join_path": "🛤️",
                                "validate_sql": "✅",
                                "execute_sql": "▶️",
                                "export_csv": "💾",
                            }.get(tool_name, "🔧")

                            # 간략한 입력 표시
                            if tool_name == "execute_sql":
                                sql_preview = tool_input.get("sql", "")[:100]
                                if len(tool_input.get("sql", "")) > 100:
                                    sql_preview += "..."
                                st.markdown(f"{tool_icon} **{tool_name}**")
                                st.code(sql_preview, language="sql")
                            elif tool_name == "get_schema_info":
                                st.markdown(f"{tool_icon} **{tool_name}**: `{tool_input.get('table_name', '')}`")
                            elif tool_name == "get_optimal_join_path":
                                tables = tool_input.get("tables", [])
                                st.markdown(f"{tool_icon} **{tool_name}**: `{' → '.join(tables)}`")
                            elif tool_name == "validate_sql":
                                st.markdown(f"{tool_icon} **{tool_name}**: SQL 검증 중...")
                            else:
                                st.markdown(f"{tool_icon} **{tool_name}**: {tool_input}")

                    elif event["type"] == "text":
                        text_parts.append(event["content"])

                    elif event["type"] == "done":
                        final_result = event["result"]
                        status_placeholder.empty()  # 상태 메시지 제거

                # 최종 결과 처리
                if final_result:
                    st.session_state.last_result = final_result

                    # 응답 메시지 구성
                    response_message = {
                        "role": "assistant",
                        "content": final_result["summary"] if final_result["summary"] else "처리가 완료되었습니다."
                    }

                    # 모든 쿼리 정보 추가 (디버그용)
                    if final_result["queries"]:
                        response_message["all_queries"] = final_result["queries"]

                        # 마지막 성공 쿼리의 데이터 미리보기 (최대 10건)
                        for query in reversed(final_result["queries"]):
                            if query.get("success") and query.get("data"):
                                response_message["data_preview"] = query["data"][:10]
                                break

                    st.session_state.messages.append(response_message)

                    # 구분선
                    st.divider()

                    # 응답 표시
                    st.markdown("### 📝 답변")
                    st.markdown(response_message["content"])

                    # 모든 실행된 쿼리 표시 (디버그용)
                    if response_message.get("all_queries"):
                        with st.expander(f"🔍 실행된 SQL 쿼리 ({len(response_message['all_queries'])}개)", expanded=False):
                            for i, query in enumerate(response_message["all_queries"], 1):
                                st.markdown(f"**쿼리 {i}** {'✅' if query['success'] else '❌'}")
                                st.code(query["sql"], language="sql")
                                if query["success"]:
                                    st.caption(f"결과: {query['row_count']}건")
                                else:
                                    st.error(f"에러: {query.get('error', 'Unknown error')}")
                                if i < len(response_message["all_queries"]):
                                    st.divider()

                    if response_message.get("data_preview"):
                        with st.expander("📊 데이터 미리보기", expanded=True):
                            df = pd.DataFrame(response_message["data_preview"])
                            st.dataframe(df, use_container_width=True)

            except NotImplementedError as e:
                error_msg = f"⚠️ 아직 구현되지 않은 기능입니다: {e}\n\nYAML 모드로 변경해주세요."
                st.error(error_msg)
                st.session_state.messages.append({"role": "assistant", "content": error_msg})

            except Exception as e:
                error_msg = f"❌ 오류가 발생했습니다: {e}"
                st.error(error_msg)
                st.session_state.messages.append({"role": "assistant", "content": error_msg})


# =============================================================================
# 메인
# =============================================================================

def main():
    """메인 함수"""
    init_session_state()
    config = load_config()

    # 설정 검증
    if not config["db_config"]["database"]:
        st.error("⚠️ DB_NAME 환경변수가 설정되지 않았습니다. .env 파일을 확인하세요.")
        st.stop()

    metadata_path = Path(config["metadata_path"])
    if not metadata_path.exists():
        st.error(f"⚠️ 메타데이터 파일을 찾을 수 없습니다: {metadata_path}")
        st.stop()

    # UI 렌더링
    render_sidebar(config)
    render_chat()


if __name__ == "__main__":
    main()
