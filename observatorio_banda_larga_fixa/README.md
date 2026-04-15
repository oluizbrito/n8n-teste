# Projeto de Automacao de Dados

Este diretorio contem os artefatos do fluxo (scripts Python e workflow do n8n).

## Como rodar
1. Crie e ative ambiente virtual Python.
2. Instale dependencias com `pip install -r requirements.txt`.
3. Rode `python scripts/ingest.py`.
4. Rode `python scripts/transform.py`.
5. Importe `n8n_workflow_export.json` no n8n.
