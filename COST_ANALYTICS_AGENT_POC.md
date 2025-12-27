# Cost Analytics Agent PoC 계획

> AWS CUR 기반 Cost Analytics 리포트 생성을 위한 Agent PoC

## 배경

### 현재 상황
- AWS CUR 기반 고객 사용량/요금 분석 서비스 운영 중
- 계약, 조정 비용, 크레딧 등 복잡한 비즈니스 로직 존재
- 데이터 적재: S3, RDB
- 조회: Athena, BigQuery 등 활용
- 고객의 다양한 리포트 요청에 수동 대응 중

### 목표
- 고객 질문 → 자동으로 적절한 데이터소스 조회 → 리포트 생성하는 Agent 구축
- Agent가 DB 스키마/관계를 이해하고 정확한 쿼리 생성

---

## PoC 비교 실험

### 접근법 A: 스키마 메타데이터 + Text-to-SQL Agent

```
고객 질문 → LLM (스키마 Context) → SQL 생성 → 실행 → 결과
```

**구성 요소**:
1. 스키마 메타데이터 문서 (YAML/JSON)
2. 테이블 관계 정의
3. 비즈니스 용어 → 기술 용어 매핑
4. Text-to-SQL Agent (LangChain/LlamaIndex)

**장점**:
- 구축 빠름 (1-2일)
- 유지보수 단순
- 비용 낮음

**단점**:
- 테이블 많아지면 Context 길어짐
- 복잡한 관계 추론 한계

### 접근법 B: Graph RAG 기반 Agent

```
고객 질문 → Graph 탐색 (관계 파악) → LLM (SQL 생성) → 실행 → 결과
```

**구성 요소**:
1. Graph DB (Neo4j 또는 NetworkX)
2. 스키마를 그래프로 모델링 (테이블=노드, FK=엣지)
3. 비즈니스 엔티티 관계 그래프
4. Graph RAG Agent

**장점**:
- 복잡한 관계 탐색 용이
- 멀티홉 조인 경로 추론 가능
- 스키마 확장에 유연

**단점**:
- 구축 복잡 (3-5일+)
- Graph DB 운영 필요
- 오버엔지니어링 가능성

---

## 비교 평가 기준

| 평가 항목 | 측정 방법 |
|-----------|-----------|
| 정확도 | 생성된 SQL의 정확성 (수동 검증) |
| 응답 시간 | 질문 → 결과까지 소요 시간 |
| 복잡한 질문 처리 | 멀티 테이블 조인 질문 성공률 |
| 구축 비용 | 개발 시간 + API 비용 |
| 유지보수성 | 스키마 변경 시 수정 범위 |

### 테스트 질문 예시

```
# 단순 (1-2 테이블)
"지난달 A 고객의 EC2 사용량은?"

# 중간 (3-4 테이블 조인)
"B 고객의 계약 할인율 적용 후 실제 청구 금액은?"

# 복잡 (5+ 테이블, 비즈니스 로직)
"크레딧 적용 전후로 C 고객의 월별 비용 추이와 계약 조건 변경 이력을 보여줘"
```

---

## 구현 계획

### Phase 1: 스키마 메타데이터 준비 (공통)

```yaml
# schema_metadata.yaml 예시
tables:
  cur_line_items:
    description: "AWS CUR 원본 사용량 데이터"
    source: "S3/Athena"
    columns:
      - name: line_item_usage_amount
        type: decimal
        description: "사용량"
      - name: line_item_unblended_cost
        type: decimal
        description: "비용 (할인 전)"
      - name: account_id
        type: string
        description: "AWS 계정 ID"

  customer_contracts:
    description: "고객 계약 정보"
    source: "RDB"
    columns:
      - name: customer_id
        type: string
        description: "고객 식별자"
      - name: discount_rate
        type: decimal
        description: "계약 할인율"
      - name: contract_start_date
        type: date
        description: "계약 시작일"

  credits:
    description: "크레딧 적용 내역"
    source: "RDB"
    columns:
      - name: customer_id
        type: string
      - name: credit_amount
        type: decimal
      - name: applied_date
        type: date

relationships:
  - from: cur_line_items.account_id
    to: customer_contracts.customer_id
    type: "many-to-one"
    description: "사용량 → 계약 매핑"

  - from: customer_contracts.customer_id
    to: credits.customer_id
    type: "one-to-many"
    description: "고객 → 크레딧 내역"
```

### Phase 2-A: Text-to-SQL Agent 구현

```python
# 예시 구조
from langchain.agents import create_sql_agent
from langchain.sql_database import SQLDatabase

# 1. 스키마 메타데이터 로드
schema_context = load_yaml("schema_metadata.yaml")

# 2. System Prompt 구성
system_prompt = f"""
당신은 Cost Analytics SQL Agent입니다.

[데이터소스 선택 규칙]
- CUR 원본 조회 (대용량): Athena 사용
- 계약/크레딧 조회: RDB 사용
- 복합 질문: 각각 조회 후 병합

[스키마 정보]
{schema_context}

[SQL 생성 규칙]
- 항상 customer_id로 필터링
- 날짜 범위 명시
- 집계 시 GROUP BY 확인
"""

# 3. Agent 생성
agent = create_sql_agent(
    llm=llm,
    db=db,
    system_prompt=system_prompt
)
```

### Phase 2-B: Graph RAG Agent 구현

```python
# 예시 구조
from neo4j import GraphDatabase
from langchain.graphs import Neo4jGraph

# 1. 스키마를 그래프로 구축
def build_schema_graph():
    # 테이블을 노드로
    graph.run("""
        CREATE (t:Table {name: 'cur_line_items', source: 'Athena'})
        CREATE (t:Table {name: 'customer_contracts', source: 'RDB'})
        CREATE (t:Table {name: 'credits', source: 'RDB'})
    """)

    # 관계를 엣지로
    graph.run("""
        MATCH (a:Table {name: 'cur_line_items'})
        MATCH (b:Table {name: 'customer_contracts'})
        CREATE (a)-[:JOINS_TO {key: 'account_id = customer_id'}]->(b)
    """)

# 2. Graph RAG Agent
def query_with_graph(question):
    # 관련 테이블/관계 탐색
    relevant_schema = graph.query("""
        MATCH path = (t1:Table)-[r:JOINS_TO*1..3]-(t2:Table)
        WHERE t1.name CONTAINS $keyword OR t2.name CONTAINS $keyword
        RETURN path
    """, keyword=extract_keyword(question))

    # LLM에게 SQL 생성 요청
    sql = llm.generate_sql(question, relevant_schema)
    return execute(sql)
```

---

## 일정 (예상)

| 주차 | 작업 |
|------|------|
| Week 1 | 스키마 메타데이터 정리, 테스트 질문 준비 |
| Week 2 | 접근법 A (Text-to-SQL) 구현 |
| Week 3 | 접근법 B (Graph RAG) 구현 |
| Week 4 | 비교 평가 및 결론 도출 |

---

## 참고 자료

### Graph RAG
- Microsoft GraphRAG: https://github.com/microsoft/graphrag
- Neo4j + LangChain: https://python.langchain.com/docs/integrations/graphs/neo4j

### Text-to-SQL
- LangChain SQL Agent: https://python.langchain.com/docs/tutorials/sql_qa
- Vanna.ai: https://vanna.ai (Text-to-SQL 특화)

### 관련 개념
- Vector RAG: 임베딩 유사도 기반 검색
- Graph RAG: 엔티티-관계 그래프 기반 검색
- Text-to-SQL: 자연어 → SQL 변환

---

## 핵심 판단 기준

```
Graph RAG 선택 조건:
□ 테이블이 50개 이상인가?
□ 조인 경로가 3단계 이상 자주 필요한가?
□ "A와 B의 관계가 뭐야?" 류의 질문이 많은가?
□ 스키마가 자주 변경되는가?

3개 이상 Yes → Graph RAG 고려
그 외 → Text-to-SQL로 충분
```

---

## 메모

- 우선 테이블 규모/복잡도 파악 필요
- 실제 고객 질문 패턴 수집해서 테스트 케이스 구성
- 두 접근법 모두 구현해보고 실제 성능 비교
- 참고 유튜브 - https://www.youtube.com/watch?v=H2OMM6GOP3g

---

*마지막 업데이트: 2025-12-28*
