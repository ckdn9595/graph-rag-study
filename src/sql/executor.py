"""SQL 실행 모듈 - 병렬 처리 지원"""

import re
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import pymysql


class ParallelExecutor:
    """병렬 SQL 실행 클래스"""

    def __init__(self, connection_config: dict, max_workers: int = 4):
        """
        Args:
            connection_config: PyMySQL 연결 설정
            max_workers: 최대 병렬 워커 수
        """
        self.config = connection_config
        self.max_workers = max_workers

    def execute(self, sql: str, parallel: bool = True) -> dict:
        """
        SQL 쿼리 실행

        Args:
            sql: 실행할 SQL 쿼리
            parallel: 병렬 실행 여부

        Returns:
            {
                "success": bool,
                "data": list[dict],  # 결과 행들
                "row_count": int,
                "error": str | None,
                "execution_info": {
                    "parallel": bool,
                    "partitions": int,
                    "elapsed_ms": float
                }
            }
        """
        start_time = datetime.now()

        try:
            if not parallel:
                data = self._execute_single(sql)
                elapsed = (datetime.now() - start_time).total_seconds() * 1000

                return {
                    "success": True,
                    "data": data,
                    "row_count": len(data),
                    "error": None,
                    "execution_info": {
                        "parallel": False,
                        "partitions": 1,
                        "elapsed_ms": elapsed,
                    },
                }

            # 날짜 기반 파티션 감지
            partitions = self._detect_partitions(sql)

            if len(partitions) <= 1:
                data = self._execute_single(sql)
                elapsed = (datetime.now() - start_time).total_seconds() * 1000

                return {
                    "success": True,
                    "data": data,
                    "row_count": len(data),
                    "error": None,
                    "execution_info": {
                        "parallel": False,
                        "partitions": 1,
                        "elapsed_ms": elapsed,
                    },
                }

            # 병렬 실행
            data = self._execute_parallel(sql, partitions)
            elapsed = (datetime.now() - start_time).total_seconds() * 1000

            return {
                "success": True,
                "data": data,
                "row_count": len(data),
                "error": None,
                "execution_info": {
                    "parallel": True,
                    "partitions": len(partitions),
                    "elapsed_ms": elapsed,
                },
            }

        except Exception as e:
            elapsed = (datetime.now() - start_time).total_seconds() * 1000

            return {
                "success": False,
                "data": [],
                "row_count": 0,
                "error": str(e),
                "execution_info": {
                    "parallel": parallel,
                    "partitions": 0,
                    "elapsed_ms": elapsed,
                },
            }

    def _detect_partitions(self, sql: str) -> list:
        """
        SQL에서 날짜 범위를 감지하여 파티션 목록 생성

        지원하는 패턴:
        - BETWEEN 'YYYY-MM-DD' AND 'YYYY-MM-DD'
        - >= 'YYYY-MM-DD' AND <= 'YYYY-MM-DD'
        """
        # BETWEEN 패턴
        between_pattern = r"BETWEEN\s+'(\d{4}-\d{2}-\d{2})'\s+AND\s+'(\d{4}-\d{2}-\d{2})'"
        match = re.search(between_pattern, sql, re.IGNORECASE)

        if not match:
            # >= AND <= 패턴
            range_pattern = r">=\s*'(\d{4}-\d{2}-\d{2})'\s+AND\s+\w+\s*<=\s*'(\d{4}-\d{2}-\d{2})'"
            match = re.search(range_pattern, sql, re.IGNORECASE)

        if not match:
            return [None]

        start = datetime.strptime(match.group(1), "%Y-%m-%d")
        end = datetime.strptime(match.group(2), "%Y-%m-%d")

        # 날짜 범위가 30일 이하면 파티션 안 함
        if (end - start).days <= 30:
            return [None]

        # 월별 파티션 생성
        partitions = []
        current = start.replace(day=1)

        while current <= end:
            # 다음 달 1일
            if current.month == 12:
                next_month = current.replace(year=current.year + 1, month=1)
            else:
                next_month = current.replace(month=current.month + 1)

            # 파티션 종료일 (다음달 1일 - 1일 또는 end 중 작은 값)
            partition_end = min(next_month - timedelta(days=1), end)

            # 파티션 시작일 (current 또는 start 중 큰 값)
            partition_start = max(current, start)

            partitions.append({
                "start": partition_start.strftime("%Y-%m-%d"),
                "end": partition_end.strftime("%Y-%m-%d"),
            })

            current = next_month

        return partitions

    def _execute_parallel(self, sql: str, partitions: list) -> list:
        """파티션별 병렬 실행"""
        results = []
        errors = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {}

            for partition in partitions:
                partition_sql = self._replace_date_range(
                    sql,
                    partition["start"],
                    partition["end"],
                )
                future = executor.submit(self._execute_single, partition_sql)
                futures[future] = partition

            for future in as_completed(futures):
                partition = futures[future]
                try:
                    result = future.result()
                    results.extend(result)
                except Exception as e:
                    errors.append(f"Partition {partition}: {e}")

        if errors:
            # 일부 파티션 실패 시 경고 로깅 (결과는 반환)
            print(f"[Warning] 일부 파티션 실행 실패: {errors}")

        return results

    def _replace_date_range(self, sql: str, start: str, end: str) -> str:
        """SQL의 날짜 범위를 파티션 범위로 치환"""
        # BETWEEN 패턴
        between_pattern = r"BETWEEN\s+'(\d{4}-\d{2}-\d{2})'\s+AND\s+'(\d{4}-\d{2}-\d{2})'"
        replaced = re.sub(
            between_pattern,
            f"BETWEEN '{start}' AND '{end}'",
            sql,
            flags=re.IGNORECASE,
        )

        if replaced != sql:
            return replaced

        # >= AND <= 패턴
        range_pattern = r"(>=\s*)'(\d{4}-\d{2}-\d{2})'(\s+AND\s+\w+\s*<=\s*)'(\d{4}-\d{2}-\d{2})'"
        return re.sub(
            range_pattern,
            rf"\g<1>'{start}'\g<3>'{end}'",
            sql,
            flags=re.IGNORECASE,
        )

    def _execute_single(self, sql: str) -> list:
        """단일 쿼리 실행"""
        conn = pymysql.connect(**self.config)
        try:
            with conn.cursor(pymysql.cursors.DictCursor) as cursor:
                cursor.execute(sql)
                return list(cursor.fetchall())
        finally:
            conn.close()

    def execute_with_limit(self, sql: str, limit: int = 1000) -> dict:
        """
        LIMIT을 적용하여 실행 (미리보기용)

        Args:
            sql: 실행할 SQL
            limit: 최대 행 수

        Returns:
            execute()와 동일한 형식
        """
        # 이미 LIMIT이 있는지 확인
        if not re.search(r"\bLIMIT\s+\d+", sql, re.IGNORECASE):
            sql = f"{sql.rstrip(';')} LIMIT {limit}"

        return self.execute(sql, parallel=False)
