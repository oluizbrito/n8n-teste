# Projeto de Automacao de Dados

Pipeline de ingestao e transformacao de dados com Python, PySpark e workflow para n8n.

## Como rodar
1. Entre em `observatorio_banda_larga_fixa`.
2. Crie e ative ambiente virtual Python.
3. Instale dependencias com `pip install -r requirements.txt`.
4. Execute `python scripts/ingest.py` para baixar dados em `lake/raw`.
5. Execute `python scripts/transform.py` para gerar saidas em `lake/curated`.
6. Importe `n8n_workflow_export.json` no n8n para orquestrar o fluxo.
