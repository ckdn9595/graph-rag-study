"""SQL 검증 모듈 - EXPLAIN 기반"""

import pymysql
from typing import Optional


class SQLValidator:
    """SQL 쿼리 검증 클래스"""

    def __init__(self, connection_config: dict):
        """
        Args:
            connection_config: PyMySQL 연결 설정
                {
                    "host": str,
                    "port": int,
                    "user": str,
                    "password": str,
                    "database": str
                }
        """
        self.config = connection_config
        self._conn: Optional[pymysql.Connection] = None

    def _get_connection(self) -> pymysql.Connection:
        """DB 연결 반환 (lazy connection)"""
        if self._conn is None or not self._conn.open:
            self._conn = pymysql.connect(**self.config)
        return self._conn

    def validate(self, sql: str) -> dict:
        """
        SQL 쿼리 검증 (문법 + 테이블/컬럼 존재 여부)

        Args:
            sql: 검증할 SQL 쿼리

        Returns:
            {
                "is_valid": bool,
                "errors": None | {
                    "code": int,
                    "message": str,
                    "type": str
                }
            }
        """
        # SELECT 쿼리만 허용
        sql_stripped = sql.strip().upper()
        if not sql_stripped.startswith("SELECT"):
            return {
                "is_valid": False,
                "errors": {
                    "code": -1,
                    "message": "SELECT 쿼리만 허용됩니다.",
                    "type": "not_select",
                },
            }

        try:
            conn = self._get_connection()
            with conn.cursor() as cursor:
                # EXPLAIN으로 검증 (실제 실행 X)
                cursor.execute(f"EXPLAIN {sql}")
                return {
                    "is_valid": True,
                    "errors": None,
                }

        except pymysql.err.ProgrammingError as e:
            error_code, error_msg = e.args
            return {
                "is_valid": False,
                "errors": {
                    "code": error_code,
                    "message": error_msg,
                    "type": self._classify_error(error_msg),
                },
            }

        except pymysql.err.OperationalError as e:
            error_code, error_msg = e.args
            return {
                "is_valid": False,
                "errors": {
                    "code": error_code,
                    "message": error_msg,
                    "type": "operational",
                },
            }

        except Exception as e:
            return {
                "is_valid": False,
                "errors": {
                    "code": -1,
                    "message": str(e),
                    "type": "unknown",
                },
            }

    def _classify_error(self, msg: str) -> str:
        """에러 메시지 유형 분류"""
        msg_lower = msg.lower()

        if "unknown column" in msg_lower:
            return "unknown_column"
        elif "table" in msg_lower and "doesn't exist" in msg_lower:
            return "unknown_table"
        elif "syntax" in msg_lower:
            return "syntax_error"
        elif "ambiguous" in msg_lower:
            return "ambiguous_column"
        elif "access denied" in msg_lower:
            return "access_denied"
        else:
            return "other"

    def get_error_suggestion(self, error_type: str, error_msg: str) -> str:
        """
        에러 유형별 수정 제안 반환

        Args:
            error_type: 에러 유형
            error_msg: 에러 메시지

        Returns:
            수정 제안 문자열
        """
        suggestions = {
            "unknown_column": "컬럼명을 확인하세요. 테이블 alias를 사용했다면 올바른지 확인하세요.",
            "unknown_table": "테이블명을 확인하세요. 스키마 정보를 다시 조회해보세요.",
            "syntax_error": "SQL 문법을 확인하세요. 괄호, 따옴표, 예약어 사용을 점검하세요.",
            "ambiguous_column": "여러 테이블에 동일한 컬럼이 있습니다. 테이블명.컬럼명 형식으로 명시하세요.",
            "access_denied": "해당 테이블에 대한 접근 권한이 없습니다.",
            "not_select": "SELECT 쿼리만 실행 가능합니다.",
        }

        return suggestions.get(error_type, f"에러를 확인하세요: {error_msg}")

    def close(self):
        """연결 종료"""
        if self._conn and self._conn.open:
            self._conn.close()
            self._conn = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
