import webbrowser
import os

diagram = """
flowchart TD
    A[Kaggle Dataset] -->|Download via API| B[Local Storage]
    B -->|Process Daily Files| C[CSV Daily Files]
    C -->|Mount Storage| D[Azure Data Lake]
    D -->|Ingest to| E[Databricks Delta Lake]
    E -->|Daily Update Process| F[Merge & Upsert Data]
    F -->|Retention Management| G[Clean Old Data]

    subgraph Azure Configuration
        H[Azure Storage Account]
        I[Databricks Workspace]
    end

    D -.-> H
    E -.-> I

    classDef cloud fill:#f9f,stroke:#333,stroke-width:4px;
    classDef process fill:#bbf,stroke:#333,stroke-width:2px;

    class H,I cloud;
    class A,B,C,D,E,F,G process;
"""

# Crear HTML con Mermaid.js
html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <script src="https://cdn.jsdelivr.net/npm/mermaid@10.6.1/dist/mermaid.min.js"></script>
    <script>mermaid.initialize({{startOnLoad:true}})</script>
</head>
<body>
    <div class="mermaid">
        {diagram}
    </div>
</body>
</html>
"""

file_path = os.path.abspath('diagram.html')
with open(file_path, 'w') as f:
    f.write(html_content)

webbrowser.open(f'file://{file_path}')