"""시스템 프롬프트 모듈 - 모듈화된 프롬프트 관리"""

from typing import Optional
from dataclasses import dataclass, field


@dataclass
class PromptSection:
    """프롬프트 섹션 정의"""
    name: str
    content: str
    enabled: bool = True
    order: int = 0  # 낮을수록 먼저 출력


class PromptBuilder:
    """
    모듈화된 시스템 프롬프트 빌더

    사용 예시:
        builder = PromptBuilder()
        builder.add_section("role", "## 역할\\n당신은 SQL Agent입니다.")
        builder.add_section("rules", "## 규칙\\n- SELECT만 허용")
        prompt = builder.build()
    """

    def __init__(self):
        self._sections: dict[str, PromptSection] = {}
        self._order_counter = 0

    def add_section(
        self,
        name: str,
        content: str,
        enabled: bool = True,
        order: Optional[int] = None
    ) -> "PromptBuilder":
        """
        프롬프트 섹션 추가

        Args:
            name: 섹션 이름 (고유 키)
            content: 섹션 내용
            enabled: 활성화 여부
            order: 출력 순서 (None이면 추가 순서)

        Returns:
            self (체이닝 지원)
        """
        if order is None:
            order = self._order_counter
            self._order_counter += 1

        self._sections[name] = PromptSection(
            name=name,
            content=content,
            enabled=enabled,
            order=order
        )
        return self

    def enable_section(self, name: str) -> "PromptBuilder":
        """섹션 활성화"""
        if name in self._sections:
            self._sections[name].enabled = True
        return self

    def disable_section(self, name: str) -> "PromptBuilder":
        """섹션 비활성화"""
        if name in self._sections:
            self._sections[name].enabled = False
        return self

    def update_section(self, name: str, content: str) -> "PromptBuilder":
        """섹션 내용 업데이트"""
        if name in self._sections:
            self._sections[name].content = content
        return self

    def append_to_section(self, name: str, content: str) -> "PromptBuilder":
        """기존 섹션에 내용 추가"""
        if name in self._sections:
            self._sections[name].content += "\n" + content
        return self

    def get_section(self, name: str) -> Optional[str]:
        """섹션 내용 조회"""
        if name in self._sections:
            return self._sections[name].content
        return None

    def build(self) -> str:
        """
        활성화된 섹션들을 순서대로 조합하여 최종 프롬프트 생성

        Returns:
            조합된 시스템 프롬프트 문자열
        """
        enabled_sections = [
            s for s in self._sections.values() if s.enabled
        ]
        sorted_sections = sorted(enabled_sections, key=lambda s: s.order)

        return "\n\n".join(s.content for s in sorted_sections)


# ============================================
# 기본 프롬프트 섹션 정의
# ============================================

SECTION_ROLE = """당신은 Text to SQL Agent입니다.

## 역할
- 사용자의 자연어 질문을 분석하여 적절한 SQL 쿼리를 생성합니다.
- RDB에서 쿼리를 실행하고 결과를 반환합니다."""


SECTION_WORKFLOW = """## 작업 흐름
1. **테이블 파악**: 먼저 `list_tables`로 사용 가능한 테이블을 확인합니다.
2. **스키마 조회**: `get_schema_info`로 관련 테이블의 컬럼 정보를 확인합니다.
3. **관계 확인**: 여러 테이블 조인이 필요하면 `get_join_hint`로 조인 조건을 확인합니다.
4. **SQL 생성**: 정보를 바탕으로 SQL 쿼리를 작성합니다.
5. **검증**: `validate_sql`로 쿼리를 검증합니다. 실패 시 수정하여 재시도합니다.
6. **실행**: 검증 통과 후 `execute_sql`로 실행합니다.
7. **저장**: 필요시 `export_csv`로 결과를 CSV로 저장합니다."""


SECTION_RULES = """## 규칙
- SELECT 쿼리만 생성하세요. INSERT, UPDATE, DELETE는 금지입니다.
- 반드시 validate_sql로 검증 후 execute_sql을 실행하세요.
- 검증 실패 시 에러 메시지와 suggestion을 참고하여 수정하세요.
- 사용자가 요청하면 결과를 CSV로 저장하세요."""


SECTION_RESPONSE_FORMAT = """## 응답 형식
- 실행한 SQL 쿼리를 명시하세요.
- 쿼리 결과의 주요 내용을 요약해서 설명하세요.
- 데이터가 많으면 상위 몇 개만 보여주고 전체 건수를 알려주세요.
- CSV로 저장한 경우 파일 경로를 알려주세요."""


# ============================================
# 비즈니스 로직 섹션 (마트 테이블 선택 규칙 등)
# ============================================

SECTION_MART_TABLE_SELECTION = """## 마트 테이블 선택 규칙

비용 데이터 조회 시 아래 규칙에 따라 적절한 마트 테이블을 선택하세요:

| 조건 | 사용 테이블 | 설명 |
|------|------------|------|
| tags 필터 AND serviceGroup 필터 | `t_aws_use_cost_rsrc_pv` | 가장 상세한 리소스 레벨 |
| serviceGroup 필터 AND viewBy='tag' | `t_aws_use_cost_rsrc_pv` | 서비스그룹 내 태그 조회 |
| serviceGroup 필터 AND viewBy!='tag' | `t_aws_use_cost_svc_grp_pv` | 서비스그룹 집계 |
| tags 필터 AND viewBy='serviceGroup' | `t_aws_use_cost_rsrc_pv` | 태그 기반 서비스그룹 |
| tags 필터 AND viewBy!='serviceGroup' | `t_aws_use_cost_tag_pv` | 태그 집계 |
| 필터 없음 AND viewBy='tag' | `t_aws_use_cost_tag_pv` | 전체 태그 집계 |
| 필터 없음 AND viewBy='serviceGroup' | `t_aws_use_cost_svc_grp_pv` | 전체 서비스그룹 집계 |
| 필터 없음 AND viewBy 기타 | `t_aws_use_cost_pv` | 기본 비용 집계 |
| 상세 데이터 조회 | `t_aws_use_cost_rsrc_pv` | 리소스 레벨 상세 |
| 이메일 다운로드 | `t_aws_use_cost_l` | 원본 데이터 |

**참고**:
- `_pv` 접미사: Pivot 테이블 (집계된 데이터)
- `_l` 접미사: List 테이블 (원본 상세 데이터)
- `rsrc`: Resource (리소스 레벨)
- `svc_grp`: Service Group (서비스그룹 레벨)"""


SECTION_SHARD_TABLE_WORKFLOW = """## 샤드 테이블 조회 워크플로우 (중요!)

AWS 비용 마트 테이블들(`t_aws_use_cost_pv`, `t_aws_use_cost_l`, `t_aws_use_cost_rsrc_pv`, `t_aws_use_cost_svc_grp_pv`, `t_aws_use_cost_tag_pv`, `t_aws_tag_map_l`)은 **샤드 테이블**입니다.

**실제 데이터는 prefix가 붙은 샤드 테이블에 있습니다** (예: `c0_t_aws_use_cost_pv`, `f0_t_aws_use_cost_pv`).

### 조회 순서

1. **먼저 `t_aws_mart_shard_l`에서 실제 테이블 위치를 조회**:
```sql
SELECT DISTINCT table_loc, shard_id
FROM bill_mart.t_aws_mart_shard_l
WHERE payr_acc_id = '{payer_account_id}'
  AND tgt_tbl_nm = '{target_table_name}'  -- 예: 't_aws_use_cost_pv'
  AND ym = '{year_month}'
  AND status_cd = 'COMPLETED'
```

2. **조회된 `table_loc`(예: `bill_mart.c0_t_aws_use_cost_pv`)와 `shard_id`로 실제 데이터 조회**:
```sql
SELECT *
FROM bill_mart.c0_t_aws_use_cost_pv  -- table_loc에서 얻은 실제 테이블명
WHERE shard_id = '{shard_id}'        -- shard_l에서 얻은 shard_id
  AND ym = '{year_month}'
  AND cmpn_id = '{company_id}'
```

### 샤드 테이블 조회 시 필수 조건

- **shard_id**: 반드시 `t_aws_mart_shard_l`에서 조회한 값 사용
- **ym**: 조회 년월
- **cmpn_id/site_id**: 회사 또는 사이트 ID

### Payer Account ID 조회 방법

회사명으로 payer_acc_id를 찾으려면:
```sql
SELECT ak.PAYR_ACC_ID
FROM bill_new.tbil_cmpn_l c
JOIN bill_new.tbil_aws_ak_l ak ON c.CMPN_ID = ak.CMPN_ID
WHERE c.CMPN_NM LIKE '%{회사명}%'
  AND ak.PAYR_YN = 'Y'
```

### 샤드 prefix 종류
- `c0_`: 현재(current) 데이터
- `f0_`, `f1_`: 이전(former) 데이터 또는 대용량 분할

**주의**: 기본 테이블명(예: `t_aws_use_cost_pv`)을 직접 조회하면 데이터가 없습니다!"""


# ============================================
# 팩토리 함수
# ============================================

def create_default_prompt_builder(
    context_method: str = "yaml",
    max_validation_retries: int = 3,
    include_mart_rules: bool = True,
) -> PromptBuilder:
    """
    기본 설정의 PromptBuilder 생성

    Args:
        context_method: 컨텍스트 조회 방식 (yaml/graph)
        max_validation_retries: SQL 검증 최대 재시도 횟수
        include_mart_rules: 마트 테이블 선택 규칙 포함 여부

    Returns:
        설정된 PromptBuilder 인스턴스
    """
    builder = PromptBuilder()

    # 기본 섹션 추가
    builder.add_section("role", SECTION_ROLE, order=0)
    builder.add_section("workflow", SECTION_WORKFLOW, order=10)
    builder.add_section("rules", SECTION_RULES, order=20)

    # 컨텍스트 모드 섹션
    context_section = f"""## 컨텍스트 조회 방식
현재 모드: **{context_method}**
- yaml: YAML 메타데이터 문서에서 스키마 정보 조회
- graph: NetworkX 그래프에서 테이블 관계 탐색 (조인 경로 추론 가능)"""
    builder.add_section("context_mode", context_section, order=30)

    # 마트 테이블 선택 규칙 (선택적)
    builder.add_section(
        "mart_rules",
        SECTION_MART_TABLE_SELECTION,
        enabled=include_mart_rules,
        order=40
    )

    # 샤드 테이블 워크플로우 (선택적, 마트 규칙과 함께 사용)
    builder.add_section(
        "shard_workflow",
        SECTION_SHARD_TABLE_WORKFLOW,
        enabled=include_mart_rules,
        order=45
    )

    # 응답 형식
    builder.add_section("response_format", SECTION_RESPONSE_FORMAT, order=50)

    # 검증 재시도 횟수 동적 추가
    if max_validation_retries != 3:
        builder.append_to_section(
            "workflow",
            f"\n**참고**: SQL 검증 최대 재시도 횟수는 {max_validation_retries}회입니다."
        )

    return builder


def create_minimal_prompt_builder(context_method: str = "yaml") -> PromptBuilder:
    """
    최소 구성의 PromptBuilder 생성 (테스트/디버깅용)
    """
    builder = PromptBuilder()
    builder.add_section("role", SECTION_ROLE, order=0)
    builder.add_section("rules", SECTION_RULES, order=10)

    context_section = f"## 모드\n현재 모드: **{context_method}**"
    builder.add_section("context_mode", context_section, order=20)

    return builder
