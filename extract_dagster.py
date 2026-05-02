#!/usr/bin/env python3
"""
Dagster to IDL extractor.

Extracts asset definitions from Dagster code and converts to IDL graph format.
Focuses on DQ1 (variance) and DQ5 (fan strategy) validation.
"""

import ast
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Set


class DagsterAssetExtractor(ast.NodeVisitor):
    """Extract Dagster asset definitions via AST."""

    def __init__(self):
        self.assets: Dict[str, Dict[str, Any]] = {}
        self.multi_assets: Dict[str, Dict[str, Any]] = {}
        self.current_file: Optional[str] = None

    def extract_from_file(self, filepath: Path) -> None:
        """Extract assets from a Python file."""
        self.current_file = str(filepath)
        with open(filepath, "r") as f:
            tree = ast.parse(f.read(), filename=str(filepath))
        self.visit(tree)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        """Visit function definitions looking for @asset or @multi_asset decorators."""
        for decorator in node.decorator_list:
            if isinstance(decorator, ast.Name):
                if decorator.id == "asset":
                    self._extract_asset(node, {})
                elif decorator.id == "multi_asset":
                    self._extract_multi_asset(node, {})
            elif isinstance(decorator, ast.Call):
                if isinstance(decorator.func, ast.Name):
                    if decorator.func.id == "asset":
                        kwargs = self._parse_decorator_kwargs(decorator)
                        self._extract_asset(node, kwargs)
                    elif decorator.func.id == "multi_asset":
                        kwargs = self._parse_decorator_kwargs(decorator)
                        self._extract_multi_asset(node, kwargs)
        self.generic_visit(node)

    def _parse_decorator_kwargs(self, call_node: ast.Call) -> Dict[str, Any]:
        """Parse keyword arguments from decorator call."""
        kwargs = {}
        for keyword in call_node.keywords:
            if keyword.arg:
                kwargs[keyword.arg] = self._eval_node(keyword.value)
        return kwargs

    def _eval_node(self, node: ast.AST) -> Any:
        """Evaluate simple AST nodes to Python values."""
        if isinstance(node, ast.Constant):
            return node.value
        elif isinstance(node, ast.Name):
            return f"<ref:{node.id}>"
        elif isinstance(node, ast.List):
            return [self._eval_node(elt) for elt in node.elts]
        elif isinstance(node, ast.Dict):
            return {
                self._eval_node(k): self._eval_node(v)
                for k, v in zip(node.keys, node.values)
            }
        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name):
                return f"<call:{node.func.id}>"
            return "<call:unknown>"
        return None

    def _extract_asset(self, node: ast.FunctionDef, kwargs: Dict[str, Any]) -> None:
        """Extract single asset definition."""
        asset_name = node.name

        # Extract upstream dependencies
        deps = kwargs.get("deps", [])
        if isinstance(deps, list):
            upstream = [dep.split(":")[-1].strip(">") for dep in deps if isinstance(dep, str)]
        else:
            upstream = []

        # Also check function parameters for upstream assets
        for arg in node.args.args:
            if arg.arg not in ["context", "self"]:
                upstream.append(arg.arg)

        # Extract return type annotation
        return_type = None
        if node.returns:
            return_type = ast.unparse(node.returns)

        # Extract partitioning
        partitions_def = kwargs.get("partitions_def")
        is_partitioned = partitions_def is not None

        self.assets[asset_name] = {
            "name": asset_name,
            "type": "asset",
            "upstream": list(set(upstream)),
            "return_type": return_type,
            "is_partitioned": is_partitioned,
            "partitions_def": str(partitions_def) if partitions_def else None,
            "metadata": kwargs.get("metadata", {}),
            "source_file": self.current_file,
        }

    def _extract_multi_asset(self, node: ast.FunctionDef, kwargs: Dict[str, Any]) -> None:
        """Extract multi_asset definition (fan-out: 1→N)."""
        multi_asset_name = kwargs.get("name", node.name)

        # Extract outputs
        outs = kwargs.get("outs", {})
        output_names = []
        if isinstance(outs, dict):
            output_names = list(outs.keys())

        # Extract return type to infer output count
        return_type = None
        if node.returns:
            return_type = ast.unparse(node.returns)

        # Extract upstream dependencies from function args
        upstream = []
        for arg in node.args.args:
            if arg.arg not in ["context", "self"]:
                upstream.append(arg.arg)

        self.multi_assets[multi_asset_name] = {
            "name": multi_asset_name,
            "type": "multi_asset",
            "outputs": output_names,
            "upstream": upstream,
            "return_type": return_type,
            "metadata": kwargs.get("metadata", {}),
            "source_file": self.current_file,
        }

    def to_idl_graph(self) -> Dict[str, Any]:
        """Convert extracted assets to IDL graph format."""
        nodes = []
        edges = []
        node_id_map = {}

        # Process single assets
        for asset_name, asset_data in self.assets.items():
            node_id = f"asset_{asset_name}"
            node_id_map[asset_name] = node_id

            # Determine cardinality
            input_cardinality = "many" if asset_data["is_partitioned"] else "one"
            output_cardinality = "many" if asset_data["is_partitioned"] else "one"

            nodes.append({
                "id": node_id,
                "label": asset_name,
                "node_type": "computation",
                "properties": {
                    "dagster_type": "asset",
                    "is_partitioned": asset_data["is_partitioned"],
                    "partitions_def": asset_data["partitions_def"],
                    "return_type": asset_data["return_type"],
                    "input_cardinality": input_cardinality,
                    "output_cardinality": output_cardinality,
                    "source_file": asset_data["source_file"],
                }
            })

            # Add edges for upstream dependencies
            for upstream_name in asset_data["upstream"]:
                upstream_id = node_id_map.get(upstream_name, f"asset_{upstream_name}")
                
                # Determine fan strategy
                fan_strategy = self._infer_fan_strategy(
                    upstream_name, asset_name, asset_data
                )
                
                edges.append({
                    "source": upstream_id,
                    "target": node_id,
                    "edge_type": "dataflow",
                    "properties": {
                        "fan_strategy": fan_strategy,
                    }
                })

        # Process multi_assets (fan-out)
        for multi_name, multi_data in self.multi_assets.items():
            # Create a node for the multi_asset computation
            multi_node_id = f"multi_{multi_name}"
            nodes.append({
                "id": multi_node_id,
                "label": multi_name,
                "node_type": "computation",
                "properties": {
                    "dagster_type": "multi_asset",
                    "output_count": len(multi_data["outputs"]),
                    "return_type": multi_data["return_type"],
                    "source_file": multi_data["source_file"],
                }
            })

            # Add edges from upstream to multi_asset
            for upstream_name in multi_data["upstream"]:
                upstream_id = node_id_map.get(upstream_name, f"asset_{upstream_name}")
                edges.append({
                    "source": upstream_id,
                    "target": multi_node_id,
                    "edge_type": "dataflow",
                    "properties": {"fan_strategy": "broadcast"}
                })

            # Create output nodes and edges (fan-out)
            for output_name in multi_data["outputs"]:
                output_node_id = f"asset_{output_name}"
                node_id_map[output_name] = output_node_id

                nodes.append({
                    "id": output_node_id,
                    "label": output_name,
                    "node_type": "data",
                    "properties": {
                        "dagster_type": "asset_output",
                        "parent_multi_asset": multi_name,
                    }
                })

                # Fan-out edge: multi_asset → output
                edges.append({
                    "source": multi_node_id,
                    "target": output_node_id,
                    "edge_type": "dataflow",
                    "properties": {
                        "fan_strategy": "partition",  # multi_asset splits output
                    }
                })

        return {
            "schema_version": "0.1.4",
            "graph": {
                "nodes": nodes,
                "edges": edges,
            },
            "metadata": {
                "extractor": "dagster-idl-extractor",
                "version": "0.1.0",
                "extraction_date": "2025-01-01",
            }
        }

    def _infer_fan_strategy(
        self, upstream_name: str, downstream_name: str, downstream_data: Dict
    ) -> str:
        """
        Infer fan strategy for an edge.
        
        DQ5 validation: classify as broadcast, partition, or routed.
        """
        upstream_data = self.assets.get(upstream_name)
        
        if not upstream_data:
            return "broadcast"  # default
        
        upstream_partitioned = upstream_data.get("is_partitioned", False)
        downstream_partitioned = downstream_data.get("is_partitioned", False)
        
        if upstream_partitioned and downstream_partitioned:
            # Both partitioned → partition strategy (each partition flows independently)
            return "partition"
        elif upstream_partitioned and not downstream_partitioned:
            # Many→1: downstream aggregates all partitions → broadcast (all data)
            return "broadcast"
        elif not upstream_partitioned and downstream_partitioned:
            # 1→Many: upstream broadcast to all partitions
            return "broadcast"
        else:
            # 1→1: simple broadcast
            return "broadcast"


def main():
    """Extract Dagster assets and generate IDL graph."""
    extractor = DagsterAssetExtractor()

    # Extract from source files
    sources_dir = Path(__file__).parent / "intent" / "sources"
    for py_file in sources_dir.glob("*.py"):
        print(f"Extracting from {py_file.name}...")
        extractor.extract_from_file(py_file)

    print(f"\nExtracted {len(extractor.assets)} assets")
    print(f"Extracted {len(extractor.multi_assets)} multi_assets")

    # Generate IDL graph
    idl_graph = extractor.to_idl_graph()

    # Write to file
    output_path = Path(__file__).parent / "intent" / "extracted" / "dagster-graph.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(idl_graph, f, indent=2)

    print(f"\nWrote IDL graph to {output_path}")
    print(f"  Nodes: {len(idl_graph['graph']['nodes'])}")
    print(f"  Edges: {len(idl_graph['graph']['edges'])}")


if __name__ == "__main__":
    main()
