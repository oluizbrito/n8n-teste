#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Ingestão do dataset "Acessos – Banda Larga Fixa" via CKAN do dados.gov.br.
- Busca o pacote por nome
- Localiza recursos CSV
- Baixa o mais recente e salva em lake/raw por ano de referência
"""

import os, re, json, hashlib, datetime as dt
import requests
from dotenv import load_dotenv

load_dotenv()
BASE_DIR = os.getenv("BASE_DIR") or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LAKE_RAW = os.path.join(BASE_DIR, "lake", "raw")
os.makedirs(LAKE_RAW, exist_ok=True)

CKAN = "https://dados.gov.br/api/3/action"

def ckan_call(action, **params):
    url = f"{CKAN}/{action}"
    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    if not data.get("success"):
        raise RuntimeError(f"CKAN error in {action}: {data}")
    return data["result"]

def search_package(query):
    res = ckan_call("package_search", q=query)
    if res["count"] == 0:
        raise RuntimeError("Dataset não encontrado no dados.gov.br")
    return res["results"][0]

def best_csv_resource(pkg):
    csvs = [r for r in pkg.get("resources", []) if (r.get("format") or "").lower() == "csv"]
    if not csvs:
        csvs = [r for r in pkg.get("resources", []) if "csv" in (r.get("mimetype") or "").lower() or "csv" in (r.get("name") or "").lower()]
    if not csvs:
        raise RuntimeError("Nenhum recurso CSV encontrado no pacote")
    def sortkey(r):
        return r.get("last_modified") or r.get("created") or ""
    csvs.sort(key=sortkey, reverse=True)
    return csvs[0]

def year_from_text(s):
    m = re.search(r"(20\d{2})", s or "")
    if m:
        return int(m.group(1))
    return dt.datetime.utcnow().year

def download(url):
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    return r.content

def main():
    pkg = search_package("Acessos - Banda Larga Fixa")
    res = best_csv_resource(pkg)
    url = res.get("url")
    name = res.get("name") or "acessos_banda_larga_fixa.csv"
    lastmod = res.get("last_modified") or res.get("created") or ""
    year = year_from_text((name or "") + " " + (lastmod or ""))
    content = download(url)
    sha = hashlib.sha256(content).hexdigest()[:12]

    year_dir = os.path.join(LAKE_RAW, f"ano={year}")
    os.makedirs(year_dir, exist_ok=True)
    fname = f"acessos_banda_larga_fixa_{year}_{sha}.csv"
    fpath = os.path.join(year_dir, fname)
    with open(fpath, "wb") as f:
        f.write(content)

    meta = {
        "downloaded_at": dt.datetime.utcnow().isoformat(),
        "package_id": pkg.get("id"),
        "resource_id": res.get("id"),
        "resource_url": url,
        "resource_name": name,
        "last_modified": lastmod,
        "sha256": sha,
        "saved_to": fpath
    }
    with open(os.path.join(year_dir, fname + ".meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print(json.dumps({"status":"ok","file":fpath,"meta":meta}, ensure_ascii=False))

if __name__ == "__main__":
    main()
