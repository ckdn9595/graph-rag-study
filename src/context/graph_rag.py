"""NetworkX 기반 스키마 그래프 모듈"""

import yaml
from pathlib import Path
from typing import Optional

import networkx as nx


class SchemaGraph:
    """
    NetworkX 기반 스키마 그래프

    테이블을 노드로, FK 관계를 엣지로 표현하여
    복잡한 조인 경로 탐색을 지원합니다.

    YAML 메타데이터와 동일한 소스를 사용하지만,
    그래프 구조를 통해 다음 기능을 추가로 제공:
    - 최단 조인 경로 탐색 (shortest_path)
    - 다중 테이블 연결 경로 (multi-hop)
    - 연결된 테이블 탐색 (neighbors)
    """

    def __init__(self, metadata_path: str):
        """
        스키마 메타데이터를 로드하여 그래프 구축

        Args:
            metadata_path: schema_metadata.yaml 파일 경로
        """
        self.metadata_path = Path(metadata_path)
        self.metadata = self._load_metadata()
        self.graph = self._build_graph()

    def _load_metadata(self) -> dict:
        """YAML 파일 로드"""
        if not self.metadata_path.exists():
            raise FileNotFoundError(f"메타데이터 파일을 찾을 수 없습니다: {self.metadata_path}")

        with open(self.metadata_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def _build_graph(self) -> nx.DiGraph:
        """
        메타데이터로부터 NetworkX 그래프 구축

        - 노드: 테이블 (속성: description, columns, source)
        - 엣지: FK 관계 (속성: from_col, to_col, type, description)
        """
        G = nx.DiGraph()

        # 테이블을 노드로 추가
        tables = self.metadata.get("tables", {})
        for table_name, table_info in tables.items():
            G.add_node(
                table_name,
                description=table_info.get("description", ""),
                source=table_info.get("source", ""),
                columns=table_info.get("columns", []),
            )

        # 관계를 엣지로 추가
        relationships = self.metadata.get("relationships", [])
        for rel in relationships:
            from_parts = rel["from"].split(".")
            to_parts = rel["to"].split(".")

            from_table = from_parts[0]
            from_col = from_parts[1] if len(from_parts) > 1 else ""
            to_table = to_parts[0]
            to_col = to_parts[1] if len(to_parts) > 1 else ""

            # 양방향 엣지 추가 (조인은 양방향 가능)
            edge_attrs = {
                "from_col": from_col,
                "to_col": to_col,
                "type": rel.get("type", ""),
                "description": rel.get("description", ""),
                "join_condition": f"{rel['from']} = {rel['to']}",
            }

            G.add_edge(from_table, to_table, **edge_attrs)
            # 역방향 엣지 (컬럼명 스왑)
            G.add_edge(
                to_table,
                from_table,
                from_col=to_col,
                to_col=from_col,
                type=rel.get("type", ""),
                description=rel.get("description", ""),
                join_condition=f"{rel['to']} = {rel['from']}",
            )

        return G

    def get_table_info(self, table_name: str) -> Optional[dict]:
        """
        테이블 정보 + 연결된 테이블 정보 반환

        Args:
            table_name: 조회할 테이블명

        Returns:
            테이블 정보 dict 또는 None
        """
        if table_name not in self.graph:
            return None

        node_data = self.graph.nodes[table_name]

        # 연결된 테이블 정보
        connected_tables = []
        for neighbor in self.graph.neighbors(table_name):
            edge_data = self.graph.edges[table_name, neighbor]
            connected_tables.append({
                "table": neighbor,
                "join_condition": edge_data.get("join_condition", ""),
                "relationship": edge_data.get("type", ""),
            })

        return {
            "table": table_name,
            "description": node_data.get("description", ""),
            "source": node_data.get("source", ""),
            "columns": node_data.get("columns", []),
            "connected_tables": connected_tables,
        }

    def find_join_path(self, from_table: str, to_table: str) -> Optional[dict]:
        """
        두 테이블 간 최단 조인 경로 찾기

        Args:
            from_table: 시작 테이블
            to_table: 목표 테이블

        Returns:
            조인 경로 정보 또는 None
        """
        if from_table not in self.graph or to_table not in self.graph:
            return None

        try:
            # 최단 경로 탐색
            path = nx.shortest_path(self.graph, from_table, to_table)
        except nx.NetworkXNoPath:
            return None

        # 경로 상의 조인 조건 추출
        joins = []
        for i in range(len(path) - 1):
            edge_data = self.graph.edges[path[i], path[i + 1]]
            joins.append({
                "from": path[i],
                "to": path[i + 1],
                "condition": edge_data.get("join_condition", ""),
                "type": edge_data.get("type", ""),
            })

        return {
            "path": path,
            "joins": joins,
            "hop_count": len(path) - 1,
        }

    def list_tables(self) -> list:
        """
        모든 테이블 목록 반환

        Returns:
            [{"name": str, "description": str}, ...]
        """
        tables = []
        for node in self.graph.nodes:
            node_data = self.graph.nodes[node]
            tables.append({
                "name": node,
                "description": node_data.get("description", ""),
            })
        return tables

    def get_all_schema_context(self) -> str:
        """
        전체 스키마를 컨텍스트 문자열로 반환

        Returns:
            LLM에 전달할 스키마 정보 문자열
        """
        lines = ["# 데이터베이스 스키마 정보 (Graph 기반)\n"]

        # 테이블 정보
        for node in self.graph.nodes:
            node_data = self.graph.nodes[node]
            lines.append(f"## 테이블: {node}")
            lines.append(f"설명: {node_data.get('description', '')}")
            lines.append(f"소스: {node_data.get('source', '')}")
            lines.append("컬럼:")

            for col in node_data.get("columns", []):
                lines.append(f"  - {col['name']} ({col['type']}): {col.get('description', '')}")

            # 연결된 테이블
            neighbors = list(self.graph.neighbors(node))
            if neighbors:
                lines.append(f"연결된 테이블: {', '.join(neighbors)}")

            lines.append("")

        # 그래프 통계
        lines.append("## 그래프 통계")
        lines.append(f"  - 테이블 수: {self.graph.number_of_nodes()}")
        lines.append(f"  - 관계 수: {self.graph.number_of_edges() // 2}")  # 양방향이므로 /2

        # 비즈니스 용어
        glossary = self.metadata.get("business_glossary", {})
        if glossary:
            lines.append("\n## 비즈니스 용어 매핑")
            for term, mapping in glossary.items():
                lines.append(f"  - \"{term}\" → {mapping}")

        return "\n".join(lines)

    def search_tables_by_keyword(self, keyword: str) -> list:
        """
        키워드로 관련 테이블 검색

        Args:
            keyword: 검색할 키워드

        Returns:
            관련 테이블 정보 리스트
        """
        keyword_lower = keyword.lower()
        results = []

        for node in self.graph.nodes:
            node_data = self.graph.nodes[node]
            matched = False

            # 테이블명 검색
            if keyword_lower in node.lower():
                matched = True
            # 설명 검색
            elif keyword_lower in node_data.get("description", "").lower():
                matched = True
            # 컬럼명/설명 검색
            else:
                for col in node_data.get("columns", []):
                    if keyword_lower in col["name"].lower():
                        matched = True
                        break
                    if keyword_lower in col.get("description", "").lower():
                        matched = True
                        break

            if matched:
                results.append(self.get_table_info(node))

        # 비즈니스 용어 매핑도 검색
        glossary = self.metadata.get("business_glossary", {})
        for term, mapping in glossary.items():
            if keyword_lower in term.lower():
                results.append({
                    "type": "glossary",
                    "term": term,
                    "mapping": mapping,
                })

        return results

    def get_multi_hop_path(self, tables: list) -> Optional[dict]:
        """
        여러 테이블을 연결하는 최적 경로 찾기

        Steiner Tree 근사 알고리즘을 사용하여
        주어진 모든 테이블을 연결하는 최소 비용 경로를 찾습니다.

        Args:
            tables: 연결할 테이블 목록

        Returns:
            최적 조인 경로 정보 또는 None
        """
        if len(tables) < 2:
            return None

        # 모든 테이블이 그래프에 존재하는지 확인
        for table in tables:
            if table not in self.graph:
                return None

        # 간단한 접근: 첫 번째 테이블에서 시작해서 순차적으로 연결
        # (최적은 아니지만 실용적)
        all_joins = []
        visited_tables = set()
        ordered_path = [tables[0]]
        visited_tables.add(tables[0])

        remaining = set(tables[1:])

        while remaining:
            best_next = None
            best_path = None
            best_length = float("inf")

            # 현재까지 방문한 테이블에서 남은 테이블로 가는 최단 경로 찾기
            for visited in visited_tables:
                for target in remaining:
                    try:
                        path = nx.shortest_path(self.graph, visited, target)
                        if len(path) < best_length:
                            best_length = len(path)
                            best_next = target
                            best_path = path
                    except nx.NetworkXNoPath:
                        continue

            if best_next is None:
                # 연결 불가능한 테이블 존재
                return None

            # 경로 상의 조인 조건 추출
            for i in range(len(best_path) - 1):
                from_t, to_t = best_path[i], best_path[i + 1]
                if (from_t, to_t) not in [(j["from"], j["to"]) for j in all_joins]:
                    edge_data = self.graph.edges[from_t, to_t]
                    all_joins.append({
                        "from": from_t,
                        "to": to_t,
                        "condition": edge_data.get("join_condition", ""),
                        "type": edge_data.get("type", ""),
                    })

            # 경로 상의 모든 테이블을 visited에 추가
            for t in best_path:
                visited_tables.add(t)
                if t not in ordered_path:
                    ordered_path.append(t)

            remaining.remove(best_next)

        return {
            "tables": tables,
            "path": ordered_path,
            "joins": all_joins,
            "total_hops": len(all_joins),
        }

    def get_join_hint(self, table1: str, table2: str) -> Optional[dict]:
        """
        두 테이블 간의 조인 힌트 반환 (MetadataRAG와 호환용)

        Args:
            table1: 첫 번째 테이블명
            table2: 두 번째 테이블명

        Returns:
            조인 조건 정보 또는 None
        """
        path_info = self.find_join_path(table1, table2)
        if path_info is None:
            return None

        # 직접 연결인 경우
        if path_info["hop_count"] == 1:
            join = path_info["joins"][0]
            return {
                "join_condition": join["condition"],
                "type": join["type"],
                "description": f"직접 연결: {table1} → {table2}",
            }

        # 다중 홉인 경우
        conditions = [j["condition"] for j in path_info["joins"]]
        return {
            "join_condition": " AND ".join(conditions),
            "type": "multi-hop",
            "description": f"경로: {' → '.join(path_info['path'])}",
            "path": path_info["path"],
            "joins": path_info["joins"],
        }

    def visualize(self, output_path: str = "./output/schema_graph.html", open_browser: bool = True) -> str:
        """
        Pyvis를 사용하여 그래프를 HTML로 시각화

        Args:
            output_path: 출력할 HTML 파일 경로
            open_browser: 생성 후 브라우저에서 자동으로 열지 여부

        Returns:
            생성된 HTML 파일 경로
        """
        from pyvis.network import Network
        import webbrowser
        from pathlib import Path

        # 출력 디렉토리 생성
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        # Pyvis 네트워크 생성
        net = Network(
            height="750px",
            width="100%",
            bgcolor="#ffffff",
            font_color="#333333",
            directed=True,
            notebook=False,
        )

        # 물리 엔진 설정 (노드 배치)
        net.set_options("""
        {
            "nodes": {
                "shape": "box",
                "font": {"size": 14, "face": "arial"},
                "borderWidth": 2,
                "shadow": true
            },
            "edges": {
                "arrows": {"to": {"enabled": true, "scaleFactor": 0.5}},
                "color": {"color": "#848484", "highlight": "#1E90FF"},
                "font": {"size": 10, "align": "middle"},
                "smooth": {"type": "curvedCW", "roundness": 0.2}
            },
            "physics": {
                "enabled": true,
                "barnesHut": {
                    "gravitationalConstant": -3000,
                    "centralGravity": 0.3,
                    "springLength": 200,
                    "springConstant": 0.04
                }
            },
            "interaction": {
                "hover": true,
                "tooltipDelay": 100
            }
        }
        """)

        # 노드 추가 (테이블)
        for node in self.graph.nodes:
            node_data = self.graph.nodes[node]
            columns = node_data.get("columns", [])

            # 툴팁에 컬럼 정보 표시
            column_info = "\n".join([
                f"• {col['name']} ({col['type']})"
                for col in columns[:10]  # 최대 10개만
            ])
            if len(columns) > 10:
                column_info += f"\n... 외 {len(columns) - 10}개"

            tooltip = f"<b>{node}</b>\n{node_data.get('description', '')}\n\n<b>컬럼:</b>\n{column_info}"

            net.add_node(
                node,
                label=node,
                title=tooltip,
                color="#4A90D9",
                size=30,
            )

        # 엣지 추가 (관계) - 중복 방지를 위해 한 방향만
        added_edges = set()
        for edge in self.graph.edges:
            from_table, to_table = edge
            edge_key = tuple(sorted([from_table, to_table]))

            if edge_key not in added_edges:
                edge_data = self.graph.edges[edge]
                join_condition = edge_data.get("join_condition", "")
                rel_type = edge_data.get("type", "")

                # 엣지 라벨 (컬럼명만 표시)
                label = join_condition.split("=")[0].split(".")[-1].strip() if join_condition else ""

                net.add_edge(
                    from_table,
                    to_table,
                    title=f"{join_condition}\n({rel_type})",
                    label=label,
                    color="#848484",
                )
                added_edges.add(edge_key)

        # HTML 파일 생성
        net.write_html(str(output_file))

        # 브라우저에서 열기
        if open_browser:
            webbrowser.open(f"file://{output_file.absolute()}")

        return str(output_file.absolute())
