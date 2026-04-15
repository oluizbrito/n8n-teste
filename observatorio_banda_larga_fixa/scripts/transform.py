#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Transformações em PySpark para responder:
Q1: total de acessos por região no último ano disponível
Q2: evolução do número de acessos por tecnologia nos últimos 3 anos
"""

import os, json, glob, datetime as dt
from dotenv import load_dotenv

load_dotenv()
BASE_DIR = os.getenv("BASE_DIR") or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LAKE_RAW = os.path.join(BASE_DIR, "lake", "raw")
LAKE_CUR = os.path.join(BASE_DIR, "lake", "curated")
os.makedirs(LAKE_CUR, exist_ok=True)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, upper, trim, sum as _sum, max as _max, year, to_date, lit
from pyspark.sql.types import IntegerType, StringType, DoubleType

REGIAO_UF = {
    "N": {"AC","AP","AM","PA","RO","RR","TO"},
    "NE":{"AL","BA","CE","MA","PB","PE","PI","RN","SE"},
    "CO":{"DF","GO","MT","MS"},
    "SE":{"ES","MG","RJ","SP"},
    "S": {"PR","RS","SC"}
}

def most_recent_year(df):
    if "Ano" in df.columns:
        return df.agg(_max(col("Ano")).alias("y")).collect()[0]["y"]
    if "ano" in df.columns:
        return df.agg(_max(col("ano")).alias("y")).collect()[0]["y"]
    if "Data" in df.columns:
        return df.agg(_max(year(col("Data"))).alias("y")).collect()[0]["y"]
    if "data" in df.columns:
        return df.agg(_max(year(col("data"))).alias("y")).collect()[0]["y"]
    years = []
    for p in glob.glob(os.path.join(LAKE_RAW, "ano=*")):
        try:
            y = int(p.split("ano=")[-1])
            years.append(y)
        except: pass
    return max(years) if years else None

def normalize_columns(df):
    for c in df.columns:
        c_norm = c.strip().lower().replace(" ", "_")
        if c_norm != c:
            df = df.withColumnRenamed(c, c_norm)
    return df

def load_latest_csvs(spark):
    paths = glob.glob(os.path.join(LAKE_RAW, "ano=*/*.csv"))
    if not paths:
        raise RuntimeError("Nenhum CSV encontrado em lake/raw. Rode scripts/ingest.py antes.")
    df = spark.read.option("header", True).option("inferSchema", True).csv(paths)
    return normalize_columns(df)

def prepare(df):
    if "uf" in df.columns:
        df = df.withColumn("uf", upper(trim(col("uf"))))
    if "acessos" not in df.columns:
        for cand in ["total_acessos","quantidade","qtd","acessos_totais","total"]:
            if cand in df.columns:
                df = df.withColumnRenamed(cand, "acessos")
                break
    if "tecnologia" not in df.columns:
        for cand in ["tec","tipo_tecnologia","tipo"]:
            if cand in df.columns:
                df = df.withColumnRenamed(cand, "tecnologia")
                break
    if "ano" not in df.columns:
        if "data" in df.columns:
            df = df.withColumn("ano", year(to_date(col("data"))))
        else:
            df = df.withColumn("ano", lit(None).cast(IntegerType()))
    if "acessos" in df.columns:
        df = df.withColumn("acessos", regexp_replace(col("acessos").cast(StringType()), r"[^0-9]", "").cast(DoubleType()))
    return df

def compute_q1(df):
    y = most_recent_year(df)
    d = df.filter(col("ano")==y)
    # uf->região
    spark = df.sparkSession
    uf_rows = []
    for reg, ufs in REGIAO_UF.items():
        for u in ufs:
            uf_rows.append((u, reg))
    uf_map = spark.createDataFrame(uf_rows, ["uf","regiao"])
    d = d.join(uf_map, on="uf", how="left")
    agg = d.groupBy("regiao").agg(_sum(col("acessos")).alias("total_acessos"))
    return agg.orderBy("regiao"), y

def compute_q2(df):
    y = most_recent_year(df)
    d = df.filter((col("ano") >= (y-2)) & (col("ano") <= y))
    agg = d.groupBy("ano","tecnologia").agg(_sum(col("acessos")).alias("total_acessos"))
    return agg.orderBy("ano","tecnologia"), (y-2, y)

def save_outputs(df_q1, year_q1, df_q2, span_q2):
    out1 = os.path.join(LAKE_CUR, "q1_total_por_regiao")
    out2 = os.path.join(LAKE_CUR, "q2_evolucao_por_tecnologia")
    df_q1.write.mode("overwrite").parquet(out1 + "/parquet")
    df_q2.write.mode("overwrite").parquet(out2 + "/parquet")
    df_q1.coalesce(1).write.mode("overwrite").option("header", True).csv(out1 + "/csv")
    df_q2.coalesce(1).write.mode("overwrite").option("header", True).csv(out2 + "/csv")
    meta = {
        "q1_year": year_q1,
        "q2_years": {"from": span_q2[0], "to": span_q2[1]},
        "generated_at": dt.datetime.utcnow().isoformat()
    }
    with open(os.path.join(LAKE_CUR, "transform_meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    print(json.dumps({"status":"ok","meta":meta}, ensure_ascii=False))

def main():
    spark = SparkSession.builder.appName("banda_larga_fixa_transform").getOrCreate()
    df = load_latest_csvs(spark)
    df = prepare(df)
    df_q1, y = compute_q1(df)
    df_q2, span = compute_q2(df)
    save_outputs(df_q1, y, df_q2, span)
    spark.stop()

if __name__ == "__main__":
    main()
