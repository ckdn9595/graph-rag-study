"""CSV 내보내기 모듈"""

import os
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd


class CSVExporter:
    """쿼리 결과를 CSV로 내보내는 클래스"""

    def __init__(self, output_dir: str = "./output"):
        """
        Args:
            output_dir: CSV 파일 저장 디렉토리
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def export(
        self,
        data: list,
        filename: Optional[str] = None,
        include_timestamp: bool = True,
    ) -> dict:
        """
        데이터를 CSV 파일로 내보내기

        Args:
            data: 내보낼 데이터 (list of dict)
            filename: 파일명 (확장자 제외). None이면 자동 생성
            include_timestamp: 파일명에 타임스탬프 포함 여부

        Returns:
            {
                "success": bool,
                "file_path": str,
                "row_count": int,
                "file_size_bytes": int,
                "error": str | None
            }
        """
        try:
            if not data:
                return {
                    "success": False,
                    "file_path": None,
                    "row_count": 0,
                    "file_size_bytes": 0,
                    "error": "내보낼 데이터가 없습니다.",
                }

            # 파일명 생성
            if filename is None:
                filename = "query_result"

            if include_timestamp:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"{filename}_{timestamp}"

            file_path = self.output_dir / f"{filename}.csv"

            # DataFrame 변환 및 저장
            df = pd.DataFrame(data)
            df.to_csv(file_path, index=False, encoding="utf-8-sig")

            file_size = file_path.stat().st_size

            return {
                "success": True,
                "file_path": str(file_path.absolute()),
                "row_count": len(data),
                "file_size_bytes": file_size,
                "error": None,
            }

        except Exception as e:
            return {
                "success": False,
                "file_path": None,
                "row_count": 0,
                "file_size_bytes": 0,
                "error": str(e),
            }

    def export_with_summary(
        self,
        data: list,
        filename: Optional[str] = None,
        summary_columns: Optional[list] = None,
    ) -> dict:
        """
        데이터와 요약 정보를 함께 내보내기

        Args:
            data: 내보낼 데이터
            filename: 파일명
            summary_columns: 요약할 숫자 컬럼 목록

        Returns:
            export() 반환값 + summary 정보
        """
        result = self.export(data, filename)

        if not result["success"] or not data:
            return result

        # 요약 정보 생성
        df = pd.DataFrame(data)

        summary = {
            "total_rows": len(df),
            "columns": list(df.columns),
        }

        # 숫자 컬럼 통계
        if summary_columns:
            numeric_summary = {}
            for col in summary_columns:
                if col in df.columns:
                    try:
                        numeric_summary[col] = {
                            "sum": float(df[col].sum()),
                            "mean": float(df[col].mean()),
                            "min": float(df[col].min()),
                            "max": float(df[col].max()),
                        }
                    except (TypeError, ValueError):
                        pass
            summary["numeric_stats"] = numeric_summary

        result["summary"] = summary
        return result

    def list_exports(self, pattern: str = "*.csv") -> list:
        """
        내보낸 파일 목록 조회

        Args:
            pattern: 파일 패턴 (기본: *.csv)

        Returns:
            파일 정보 리스트
        """
        files = []
        for file_path in self.output_dir.glob(pattern):
            stat = file_path.stat()
            files.append({
                "filename": file_path.name,
                "path": str(file_path.absolute()),
                "size_bytes": stat.st_size,
                "created_at": datetime.fromtimestamp(stat.st_ctime).isoformat(),
                "modified_at": datetime.fromtimestamp(stat.st_mtime).isoformat(),
            })

        # 최신순 정렬
        files.sort(key=lambda x: x["modified_at"], reverse=True)
        return files

    def cleanup_old_files(self, keep_count: int = 10) -> dict:
        """
        오래된 파일 정리

        Args:
            keep_count: 유지할 최근 파일 수

        Returns:
            {
                "deleted_count": int,
                "deleted_files": list[str]
            }
        """
        files = self.list_exports()

        if len(files) <= keep_count:
            return {
                "deleted_count": 0,
                "deleted_files": [],
            }

        files_to_delete = files[keep_count:]
        deleted_files = []

        for file_info in files_to_delete:
            try:
                os.remove(file_info["path"])
                deleted_files.append(file_info["filename"])
            except Exception as e:
                print(f"파일 삭제 실패: {file_info['filename']} - {e}")

        return {
            "deleted_count": len(deleted_files),
            "deleted_files": deleted_files,
        }
