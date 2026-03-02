# Python Compiling Service Architecture

## Runtime Call Flow

```mermaid
flowchart TD
    A["HTTP Client"] --> B["udf_compiling_service.py\n/compile"]
    B --> C["compiler.common.infer_line_number_from_code"]
    B --> D["compiler.facade.compile_udf"]

    D --> E["compiler.facade.compile_udf_legacy"]

    E --> F{"first line == '#baseline'?"}
    F -->|Yes| G["compiler.baseline.compile_baseline_mode"]
    F -->|No| H["compiler.orchestrator.compile_non_baseline"]

    H --> I["compiler.pipeline.split_imports_and_function_code"]
    H --> J["compiler.preprocessing.preprocess_code"]
    H --> K["compiler.type_inference.infer_types_from_code"]
    H --> L["compiler.ssa_core.SSA"]
    H --> M["compiler.ssa_transform.convert_ssa_to_self"]
    H --> N["compiler.dependency_graph.VariableDependencyGraph"]
    N --> O["compiler.graph + compiler.cut_strategy\nfind_valid_cuts/rank_cuts"]
    H --> P["compiler.splitter.generate_process_tables_and_split"]
    P --> Q["loop_transformer.py\n(optional loop rewrite)"]
    H --> T["compiler.port_coloring.infer_port_assignments\n(experimental metadata)"]
    H --> R["compiler.pipeline.assemble_compile_result"]

    G --> R
    R --> D
    D --> B
    B --> S["operator_class text response"]
```

## Module Layering

```mermaid
flowchart LR
    subgraph Entry["Entry Layer"]
      A1["udf_compiling_service.py"]
      A2["compiler.facade.py"]
    end

    subgraph Core["Compiler Core"]
      B1["orchestrator.py"]
      B2["pipeline.py"]
      B3["baseline.py"]
      B4["splitter.py"]
      B5["ssa_core.py"]
      B6["ssa_transform.py"]
      B7["dependency_graph.py"]
      B8["graph.py"]
      B9["cut_strategy.py"]
      B10["type_inference.py"]
      B11["preprocessing.py"]
      B12["common.py"]
      B13["models.py"]
      B14["config.py"]
    end

    subgraph Side["Side Tools (separate concern)"]
      C1["port_optimizer.py"]
      C2["port_detector.py"]
    end

    A1 --> A2
    A2 --> B1
    B1 --> B2
    B1 --> B3
    B1 --> B4
    B1 --> B5
    B1 --> B6
    B1 --> B7
    B7 --> B8
    B7 --> B9
    B1 --> C2
```

## Recommended Use Case

For a concrete compile input and expected output shape, use:

- `core/python_compiling_service/docs/good_use_case.md`
- `core/python_compiling_service/examples/good_use_case.py`
