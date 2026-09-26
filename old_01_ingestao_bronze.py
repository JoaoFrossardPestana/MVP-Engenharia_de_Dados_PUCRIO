# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ## Notebook 01_ingestao_bronze — Ingestão de dados do Datajud
# MAGIC
# MAGIC
# MAGIC
# MAGIC #### Objetivo
# MAGIC Este notebook realiza a coleta dos processos da Recuperação Judicial,
# MAGIC Recuperação Extrajudicial e Falência diretamente da API pública do Datajud (CNJ) e os
# MAGIC persiste na camada **Bronze** do lakehouse (`default.bronze_datajud_raw`), sem qualquer
# MAGIC transformação — o dado bruto é o "cofre de evidências" da arquitetura medalhão.
# MAGIC
# MAGIC #### Entradas e saídas
# MAGIC - **Entrada**: endpoints públicos `https://api-publica.datajud.cnj.jus.br/api_publica_{tribunal}/_search` (27 tribunais estaduais + TJDFT)
# MAGIC - **Saída**: tabela Delta `default.bronze_datajud_raw` com 3 colunas:
# MAGIC   - `raw_payload` (string) — o JSON do processo exatamente como recebido da API
# MAGIC   - `tribunal_origem` (string) — metadado de fonte
# MAGIC   - `ingestion_timestamp` (timestamp) — metadado de controle da carga
# MAGIC
# MAGIC #### Decisões de desenho
# MAGIC 1. **Recorte definido pelas perguntas de negócio**: filtro por assunto pai 4993 - recuperação judicial, falência. Esse assunto pai engloba os assuntos filhos: 4992 -faLência (código antigo); 4994 recuperação extrajudicial; 4995 - concordata preventiva (código antigo); 4996 concordata suspensiva (código antigo),;  4997 falência fraudulenta (código antigo);
# MAGIC
# MAGIC 2. **Recorte definido pelo tempo para o download**: `dataAjuizamento >= 01/01/1995`. Mais tarde decidiu-se analisar processos a partir de 9/06/2005 (vigência da atual Lei 11.101/2005 que regula a recuperação judicial, extrajudicial e a falência de empresários e sociedades empresárias). 
# MAGIC
# MAGIC 3. **Dado bruto imutável dentro do registro**: o JSON é salvo como string, sem tipagem nem
# MAGIC    flattening — toda estruturação acontece na Silver.
# MAGIC
# MAGIC 4. **Persistência incremental por tribunal**: cada tribunal é gravado em lote ao término da
# MAGIC    sua paginação, liberando a memória do driver antes do próximo — evita OOM em coletas longas.
# MAGIC
# MAGIC 5. **Full-refresh da rotina**: o `DROP TABLE IF EXISTS` no início do código significa que a execução
# MAGIC    completa reconstrói a Bronze inteira. Esse procedimento foi utilizado para viabilizar as múltiplas tentativas de captura dos dados junto ao CNJ; o `append` dentro do loop é apenas o mecanismo de gravação por lote de vários tribunais. 

# COMMAND ----------

# DBTITLE 1,Ingestão Multi-Tribunal
# Databricks Notebook: 01_ingestao_bronze
import requests
import json
import time
from pyspark.sql.functions import current_timestamp, lit

API_KEY = "cDZHYzlZa0JadVREZDJCendQbXY6SkJlTzNjLV9TRENyQk1RdnFKZGRQdw=="

# Substituição da sigla do DF (tjdft em vez de tjdf)
TRIBUNAIS = [
    "tjsp", "tjrj", "tjmg", "tjrs", "tjpr", "tjsc", "tjba", "tjpe", "tjce", 
    "tjdft", "tjes", "tjgo", "tjmt", "tjms", "tjpa", "tjpb", "tjrn", "tjro", 
    "tjse", "tjto", "tjal", "tjam", "tjap", "tjac", "tjrr", "tjpi", "tjma"
]

headers = {
    "Authorization": f"APIKey {API_KEY}",
    "Content-Type": "application/json"
}

payload_base = {
    "size": 10000,
    "query": {
        "bool": {
            "must": [
                {"range": {"dataAjuizamento": {"gte": "1995-01-01T00:00:00.000Z"}}},
                {"terms": {"assuntos.codigo": [4992, 4993, 4994, 4995, 4996, 4997]}}
            ]
        }
    },
    "sort": [{"@timestamp": {"order": "asc"}}, {"id.keyword": {"order": "asc"}}]
}

MAX_RETRIES = 5
INITIAL_BACKOFF = 3

print("=== INICIANDO INGESTÃO MULTI-TRIBUNAL COM PERSISTÊNCIA EM LOTE ===", flush=True)

# 1. Limpa ou recria a tabela Bronze no início da rotina
spark.sql("DROP TABLE IF EXISTS default.bronze_datajud_raw")

total_geral_processos = 0

for tribunal in TRIBUNAIS:
    url = f"https://api-publica.datajud.cnj.jus.br/api_publica_{tribunal}/_search"
    search_after = None
    page = 1
    total_tribunal = 0
    hits_tribunal_batch = []
    
    print(f"\n--- Processando Tribunal: {tribunal.upper()} ---", flush=True)

    while True:
        payload = payload_base.copy()
        if search_after:
            payload["search_after"] = search_after

        success = False
        print(f"-> [{tribunal.upper()}] Pág {page}... ", end="", flush=True)

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = requests.post(url, json=payload, headers=headers, timeout=(10, 120))

                if response.status_code == 200:
                    success = True
                    print("[OK]", flush=True)
                    break

                elif response.status_code in [429, 500, 502, 503, 504]:
                    wait_time = INITIAL_BACKOFF * (2 ** (attempt - 1))
                    print(f"\n⚠️ HTTP {response.status_code}. Aguardando {wait_time}s... (Tentativa {attempt}/{MAX_RETRIES})", flush=True)
                    time.sleep(wait_time)
                else:
                    print(f"\n❌ Erro HTTP {response.status_code}: {response.text}", flush=True)
                    break

            except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
                wait_time = INITIAL_BACKOFF * (2 ** (attempt - 1))
                print(f"\n⚠️ Timeout/Falha Conexão. Aguardando {wait_time}s... (Tentativa {attempt}/{MAX_RETRIES})", flush=True)
                time.sleep(wait_time)

        if not success:
            print(f"\n❌ Falha ao buscar dados do {tribunal.upper()} na página {page}. Avançando para próximo tribunal.", flush=True)
            break

        data = response.json()
        hits = data.get("hits", {}).get("hits", [])

        if not hits:
            print(f"✅ Concluído {tribunal.upper()}! Total do tribunal: {total_tribunal:,} processos.", flush=True)
            break

        for hit in hits:
            hits_tribunal_batch.append((json.dumps(hit["_source"]), tribunal.upper()))

        total_tribunal += len(hits)
        print(f"   L Registros obtidos: +{len(hits)} (Acumulado no tribunal: {total_tribunal:,})", flush=True)

        search_after = hits[-1].get("sort")
        page += 1

        time.sleep(1)

    # 2. Persiste os dados do tribunal corrente no Delta Lake e libera a memória local
    if len(hits_tribunal_batch) > 0:
        df_batch = spark.createDataFrame(hits_tribunal_batch, ["raw_payload", "tribunal_origem"]) \
                        .withColumn("ingestion_timestamp", current_timestamp())
        
        df_batch.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable("default.bronze_datajud_raw")
            
        total_geral_processos += len(hits_tribunal_batch)
        print(f"💾 {len(hits_tribunal_batch):,} registros do {tribunal.upper()} gravados na Bronze.", flush=True)
        
        # Limpa o lote da memória do driver
        del hits_tribunal_batch

print(f"\n=== INGESTÃO FINALIZADA. TOTAL GERAL SALVO NA BRONZE: {total_geral_processos:,} ===", flush=True)

# COMMAND ----------

# DBTITLE 1,Output: Ingestão Multi-Tribunal
# MAGIC %md
# MAGIC ### Output da execução — Ingestão Multi-Tribunal (02/09/2026)
# MAGIC
# MAGIC ```
# MAGIC === INICIANDO INGESTÃO MULTI-TRIBUNAL COM PERSISTÊNCIA EM LOTE ===
# MAGIC
# MAGIC --- Processando Tribunal: TJSP ---
# MAGIC -> [TJSP] Pág 1... [OK]
# MAGIC    L Registros obtidos: +10000 (Acumulado no tribunal: 10,000)
# MAGIC -> [TJSP] Pág 2... [OK]
# MAGIC    L Registros obtidos: +10000 (Acumulado no tribunal: 20,000)
# MAGIC -> [TJSP] Pág 3... [OK]
# MAGIC    L Registros obtidos: +10000 (Acumulado no tribunal: 30,000)
# MAGIC -> [TJSP] Pág 4... [OK]
# MAGIC    L Registros obtidos: +10000 (Acumulado no tribunal: 40,000)
# MAGIC -> [TJSP] Pág 5... [OK]
# MAGIC    L Registros obtidos: +10000 (Acumulado no tribunal: 50,000)
# MAGIC -> [TJSP] Pág 6... [OK]
# MAGIC    L Registros obtidos: +10000 (Acumulado no tribunal: 60,000)
# MAGIC -> [TJSP] Pág 7... [OK]
# MAGIC    L Registros obtidos: +10000 (Acumulado no tribunal: 70,000)
# MAGIC -> [TJSP] Pág 8... [OK]
# MAGIC    L Registros obtidos: +10000 (Acumulado no tribunal: 80,000)
# MAGIC -> [TJSP] Pág 9... [OK]
# MAGIC    L Registros obtidos: +10000 (Acumulado no tribunal: 90,000)
# MAGIC -> [TJSP] Pág 10... [OK]
# MAGIC    L Registros obtidos: +10000 (Acumulado no tribunal: 100,000)
# MAGIC -> [TJSP] Pág 11... [OK]
# MAGIC    L Registros obtidos: +10000 (Acumulado no tribunal: 110,000)
# MAGIC -> [TJSP] Pág 12... [OK]
# MAGIC    L Registros obtidos: +10000 (Acumulado no tribunal: 120,000)
# MAGIC -> [TJSP] Pág 13... [OK]
# MAGIC    L Registros obtidos: +2738 (Acumulado no tribunal: 122,738)
# MAGIC -> [TJSP] Pág 14... [OK]
# MAGIC ✅ Concluído TJSP! Total do tribunal: 122,738 processos.
# MAGIC 💾 122,738 registros do TJSP gravados na Bronze.
# MAGIC
# MAGIC --- Processando Tribunal: TJRJ ---
# MAGIC -> [TJRJ] Pág 1... [OK]
# MAGIC    L Registros obtidos: +10000 (Acumulado no tribunal: 10,000)
# MAGIC -> [TJRJ] Pág 2... [OK]
# MAGIC    L Registros obtidos: +10000 (Acumulado no tribunal: 20,000)
# MAGIC -> [TJRJ] Pág 3... [OK]
# MAGIC    L Registros obtidos: +10000 (Acumulado no tribunal: 30,000)
# MAGIC -> [TJRJ] Pág 4... [OK]
# MAGIC    L Registros obtidos: +10000 (Acumulado no tribunal: 40,000)
# MAGIC -> [TJRJ] Pág 5... [OK]
# MAGIC    L Registros obtidos: +10000 (Acumulado no tribunal: 50,000)
# MAGIC -> [TJRJ] Pág 6... [OK]
# MAGIC    L Registros obtidos: +10000 (Acumulado no tribunal: 60,000)
# MAGIC -> [TJRJ] Pág 7... [OK]
# MAGIC    L Registros obtidos: +2289 (Acumulado no tribunal: 62,289)
# MAGIC -> [TJRJ] Pág 8... [OK]
# MAGIC ✅ Concluído TJRJ! Total do tribunal: 62,289 processos.
# MAGIC 💾 62,289 registros do TJRJ gravados na Bronze.
# MAGIC
# MAGIC --- Processando Tribunal: TJMG ---
# MAGIC -> [TJMG] Pág 1... 
# MAGIC ⚠️ HTTP 504. Aguardando 3s... (Tentativa 1/5)
# MAGIC ⚠️ HTTP 504. Aguardando 6s... (Tentativa 2/5)
# MAGIC ⚠️ HTTP 504. Aguardando 12s... (Tentativa 3/5)
# MAGIC ⚠️ HTTP 504. Aguardando 24s... (Tentativa 4/5)
# MAGIC ⚠️ HTTP 504. Aguardando 48s... (Tentativa 5/5)
# MAGIC ❌ Falha ao buscar dados do TJMG na página 1. Avançando para próximo tribunal.
# MAGIC
# MAGIC --- Processando Tribunal: TJRS ---
# MAGIC -> [TJRS] Pág 1... [OK]
# MAGIC    L Registros obtidos: +10000 (Acumulado no tribunal: 10,000)
# MAGIC -> [TJRS] Pág 2... [OK]
# MAGIC    L Registros obtidos: +2031 (Acumulado no tribunal: 12,031)
# MAGIC -> [TJRS] Pág 3... [OK]
# MAGIC ✅ Concluído TJRS! Total do tribunal: 12,031 processos.
# MAGIC 💾 12,031 registros do TJRS gravados na Bronze.
# MAGIC
# MAGIC --- Processando Tribunal: TJPR ---
# MAGIC -> [TJPR] Pág 1... [OK]
# MAGIC    L Registros obtidos: +5649 (Acumulado no tribunal: 5,649)
# MAGIC -> [TJPR] Pág 2... [OK]
# MAGIC ✅ Concluído TJPR! Total do tribunal: 5,649 processos.
# MAGIC 💾 5,649 registros do TJPR gravados na Bronze.
# MAGIC
# MAGIC --- Processando Tribunal: TJSC ---
# MAGIC -> [TJSC] Pág 1... [OK]
# MAGIC    L Registros obtidos: +10000 (Acumulado no tribunal: 10,000)
# MAGIC -> [TJSC] Pág 2... [OK]
# MAGIC    L Registros obtidos: +2846 (Acumulado no tribunal: 12,846)
# MAGIC -> [TJSC] Pág 3... [OK]
# MAGIC ✅ Concluído TJSC! Total do tribunal: 12,846 processos.
# MAGIC 💾 12,846 registros do TJSC gravados na Bronze.
# MAGIC
# MAGIC --- Processando Tribunal: TJBA ---
# MAGIC -> [TJBA] Pág 1... [OK]
# MAGIC    L Registros obtidos: +533 (Acumulado no tribunal: 533)
# MAGIC -> [TJBA] Pág 2... [OK]
# MAGIC ✅ Concluído TJBA! Total do tribunal: 533 processos.
# MAGIC 💾 533 registros do TJBA gravados na Bronze.
# MAGIC
# MAGIC --- Processando Tribunal: TJPE ---
# MAGIC -> [TJPE] Pág 1... [OK]
# MAGIC    L Registros obtidos: +1497 (Acumulado no tribunal: 1,497)
# MAGIC -> [TJPE] Pág 2... [OK]
# MAGIC ✅ Concluído TJPE! Total do tribunal: 1,497 processos.
# MAGIC 💾 1,497 registros do TJPE gravados na Bronze.
# MAGIC
# MAGIC --- Processando Tribunal: TJCE ---
# MAGIC -> [TJCE] Pág 1... [OK]
# MAGIC    L Registros obtidos: +527 (Acumulado no tribunal: 527)
# MAGIC -> [TJCE] Pág 2... [OK]
# MAGIC ✅ Concluído TJCE! Total do tribunal: 527 processos.
# MAGIC 💾 527 registros do TJCE gravados na Bronze.
# MAGIC
# MAGIC --- Processando Tribunal: TJDFT ---
# MAGIC -> [TJDFT] Pág 1... [OK]
# MAGIC    L Registros obtidos: +407 (Acumulado no tribunal: 407)
# MAGIC -> [TJDFT] Pág 2... [OK]
# MAGIC ✅ Concluído TJDFT! Total do tribunal: 407 processos.
# MAGIC 💾 407 registros do TJDFT gravados na Bronze.
# MAGIC
# MAGIC --- Processando Tribunal: TJES ---
# MAGIC -> [TJES] Pág 1... [OK]
# MAGIC    L Registros obtidos: +485 (Acumulado no tribunal: 485)
# MAGIC -> [TJES] Pág 2... [OK]
# MAGIC ✅ Concluído TJES! Total do tribunal: 485 processos.
# MAGIC 💾 485 registros do TJES gravados na Bronze.
# MAGIC
# MAGIC --- Processando Tribunal: TJGO ---
# MAGIC -> [TJGO] Pág 1... [OK]
# MAGIC    L Registros obtidos: +1494 (Acumulado no tribunal: 1,494)
# MAGIC -> [TJGO] Pág 2... [OK]
# MAGIC ✅ Concluído TJGO! Total do tribunal: 1,494 processos.
# MAGIC 💾 1,494 registros do TJGO gravados na Bronze.
# MAGIC
# MAGIC --- Processando Tribunal: TJMT ---
# MAGIC -> [TJMT] Pág 1... [OK]
# MAGIC    L Registros obtidos: +5275 (Acumulado no tribunal: 5,275)
# MAGIC -> [TJMT] Pág 2... [OK]
# MAGIC ✅ Concluído TJMT! Total do tribunal: 5,275 processos.
# MAGIC 💾 5,275 registros do TJMT gravados na Bronze.
# MAGIC
# MAGIC --- Processando Tribunal: TJMS ---
# MAGIC -> [TJMS] Pág 1... [OK]
# MAGIC    L Registros obtidos: +120 (Acumulado no tribunal: 120)
# MAGIC -> [TJMS] Pág 2... [OK]
# MAGIC ✅ Concluído TJMS! Total do tribunal: 120 processos.
# MAGIC 💾 120 registros do TJMS gravados na Bronze.
# MAGIC
# MAGIC --- Processando Tribunal: TJPA ---
# MAGIC -> [TJPA] Pág 1... [OK]
# MAGIC    L Registros obtidos: +206 (Acumulado no tribunal: 206)
# MAGIC -> [TJPA] Pág 2... [OK]
# MAGIC ✅ Concluído TJPA! Total do tribunal: 206 processos.
# MAGIC 💾 206 registros do TJPA gravados na Bronze.
# MAGIC
# MAGIC --- Processando Tribunal: TJPB ---
# MAGIC -> [TJPB] Pág 1... [OK]
# MAGIC    L Registros obtidos: +250 (Acumulado no tribunal: 250)
# MAGIC -> [TJPB] Pág 2... [OK]
# MAGIC ✅ Concluído TJPB! Total do tribunal: 250 processos.
# MAGIC 💾 250 registros do TJPB gravados na Bronze.
# MAGIC
# MAGIC --- Processando Tribunal: TJRN ---
# MAGIC -> [TJRN] Pág 1... [OK]
# MAGIC    L Registros obtidos: +464 (Acumulado no tribunal: 464)
# MAGIC -> [TJRN] Pág 2... [OK]
# MAGIC ✅ Concluído TJRN! Total do tribunal: 464 processos.
# MAGIC 💾 464 registros do TJRN gravados na Bronze.
# MAGIC
# MAGIC --- Processando Tribunal: TJRO ---
# MAGIC -> [TJRO] Pág 1... [OK]
# MAGIC    L Registros obtidos: +180 (Acumulado no tribunal: 180)
# MAGIC -> [TJRO] Pág 2... [OK]
# MAGIC ✅ Concluído TJRO! Total do tribunal: 180 processos.
# MAGIC 💾 180 registros do TJRO gravados na Bronze.
# MAGIC
# MAGIC --- Processando Tribunal: TJSE ---
# MAGIC -> [TJSE] Pág 1... [OK]
# MAGIC    L Registros obtidos: +100 (Acumulado no tribunal: 100)
# MAGIC -> [TJSE] Pág 2... [OK]
# MAGIC ✅ Concluído TJSE! Total do tribunal: 100 processos.
# MAGIC 💾 100 registros do TJSE gravados na Bronze.
# MAGIC
# MAGIC --- Processando Tribunal: TJTO ---
# MAGIC -> [TJTO] Pág 1... [OK]
# MAGIC    L Registros obtidos: +195 (Acumulado no tribunal: 195)
# MAGIC -> [TJTO] Pág 2... [OK]
# MAGIC ✅ Concluído TJTO! Total do tribunal: 195 processos.
# MAGIC 💾 195 registros do TJTO gravados na Bronze.
# MAGIC
# MAGIC --- Processando Tribunal: TJAL ---
# MAGIC -> [TJAL] Pág 1... [OK]
# MAGIC    L Registros obtidos: +475 (Acumulado no tribunal: 475)
# MAGIC -> [TJAL] Pág 2... [OK]
# MAGIC ✅ Concluído TJAL! Total do tribunal: 475 processos.
# MAGIC 💾 475 registros do TJAL gravados na Bronze.
# MAGIC
# MAGIC --- Processando Tribunal: TJAM ---
# MAGIC -> [TJAM] Pág 1... [OK]
# MAGIC    L Registros obtidos: +1439 (Acumulado no tribunal: 1,439)
# MAGIC -> [TJAM] Pág 2... [OK]
# MAGIC ✅ Concluído TJAM! Total do tribunal: 1,439 processos.
# MAGIC 💾 1,439 registros do TJAM gravados na Bronze.
# MAGIC
# MAGIC --- Processando Tribunal: TJAP ---
# MAGIC -> [TJAP] Pág 1... [OK]
# MAGIC    L Registros obtidos: +27 (Acumulado no tribunal: 27)
# MAGIC -> [TJAP] Pág 2... [OK]
# MAGIC ✅ Concluído TJAP! Total do tribunal: 27 processos.
# MAGIC 💾 27 registros do TJAP gravados na Bronze.
# MAGIC
# MAGIC --- Processando Tribunal: TJAC ---
# MAGIC -> [TJAC] Pág 1... [OK]
# MAGIC    L Registros obtidos: +111 (Acumulado no tribunal: 111)
# MAGIC -> [TJAC] Pág 2... [OK]
# MAGIC ✅ Concluído TJAC! Total do tribunal: 111 processos.
# MAGIC 💾 111 registros do TJAC gravados na Bronze.
# MAGIC
# MAGIC --- Processando Tribunal: TJRR ---
# MAGIC -> [TJRR] Pág 1... [OK]
# MAGIC    L Registros obtidos: +22 (Acumulado no tribunal: 22)
# MAGIC -> [TJRR] Pág 2... [OK]
# MAGIC ✅ Concluído TJRR! Total do tribunal: 22 processos.
# MAGIC 💾 22 registros do TJRR gravados na Bronze.
# MAGIC
# MAGIC --- Processando Tribunal: TJPI ---
# MAGIC -> [TJPI] Pág 1... [OK]
# MAGIC    L Registros obtidos: +85 (Acumulado no tribunal: 85)
# MAGIC -> [TJPI] Pág 2... [OK]
# MAGIC ✅ Concluído TJPI! Total do tribunal: 85 processos.
# MAGIC 💾 85 registros do TJPI gravados na Bronze.
# MAGIC
# MAGIC --- Processando Tribunal: TJMA ---
# MAGIC -> [TJMA] Pág 1... [OK]
# MAGIC    L Registros obtidos: +102 (Acumulado no tribunal: 102)
# MAGIC -> [TJMA] Pág 2... [OK]
# MAGIC ✅ Concluído TJMA! Total do tribunal: 102 processos.
# MAGIC 💾 102 registros do TJMA gravados na Bronze.
# MAGIC
# MAGIC === INGESTÃO FINALIZADA. TOTAL GERAL SALVO NA BRONZE: 229,547 ===
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Ingestão Complementar TJMG
# Databricks Notebook: Ingestao_Complementar_TJMG
import requests
import json
import time
from pyspark.sql.functions import current_timestamp

API_KEY = "cDZHYzlZa0JadVREZDJCendQbXY6SkJlTzNjLV9TRENyQk1RdnFKZGRQdw=="
TRIBUNAL = "tjmg"
URL = f"https://api-publica.datajud.cnj.jus.br/api_publica_{TRIBUNAL}/_search"

headers = {
    "Authorization": f"APIKey {API_KEY}",
    "Content-Type": "application/json"
}

# Redução do tamanho da página para evitar erro 504 no TJMG
PAGE_SIZE = 2500

payload_base = {
    "size": PAGE_SIZE,
    "query": {
        "bool": {
            "must": [
                {"range": {"dataAjuizamento": {"gte": "1995-01-01T00:00:00.000Z"}}},
                {"terms": {"assuntos.codigo": [4992, 4993, 4994, 4995, 4996, 4997]}}
            ]
        }
    },
    "sort": [{"@timestamp": {"order": "asc"}}, {"id.keyword": {"order": "asc"}}]
}

MAX_RETRIES = 5
INITIAL_BACKOFF = 4

print(f"=== INICIANDO INGESTÃO COMPLEMENTAR: {TRIBUNAL.upper()} ===", flush=True)

search_after = None
page = 1
total_tjmg = 0

while True:
    payload = payload_base.copy()
    if search_after:
        payload["search_after"] = search_after

    success = False
    print(f"-> [{TRIBUNAL.upper()}] Pág {page}... ", end="", flush=True)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(URL, json=payload, headers=headers, timeout=(15, 180))

            if response.status_code == 200:
                success = True
                print("[OK]", flush=True)
                break

            elif response.status_code in [429, 500, 502, 503, 504]:
                wait_time = INITIAL_BACKOFF * (2 ** (attempt - 1))
                print(f"\n⚠️ HTTP {response.status_code}. Aguardando {wait_time}s... (Tentativa {attempt}/{MAX_RETRIES})", flush=True)
                time.sleep(wait_time)
            else:
                print(f"\n❌ Erro HTTP {response.status_code}: {response.text}", flush=True)
                break

        except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
            wait_time = INITIAL_BACKOFF * (2 ** (attempt - 1))
            print(f"\n⚠️ Timeout/Falha Conexão. Aguardando {wait_time}s... (Tentativa {attempt}/{MAX_RETRIES})", flush=True)
            time.sleep(wait_time)

    if not success:
        print(f"\n❌ Falha persistente ao buscar dados do {TRIBUNAL.upper()} na página {page}.", flush=True)
        break

    data = response.json()
    hits = data.get("hits", {}).get("hits", [])

    if not hits:
        print(f"✅ Concluído {TRIBUNAL.upper()}! Total de processos coletados: {total_tjmg:,}.", flush=True)
        break

    # Converte e grava cada página diretamente no Delta Lake existente em modo APPEND
    hits_batch = [(json.dumps(hit["_source"]), TRIBUNAL.upper()) for hit in hits]
    
    df_page = spark.createDataFrame(hits_batch, ["raw_payload", "tribunal_origem"]) \
                   .withColumn("ingestion_timestamp", current_timestamp())
    
    df_page.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("default.bronze_datajud_raw")

    total_tjmg += len(hits)
    print(f"   L Gravados na Bronze: +{len(hits):,} (Acumulado TJMG: {total_tjmg:,})", flush=True)

    search_after = hits[-1].get("sort")
    page += 1

    del hits_batch
    time.sleep(1)

print(f"\n=== PROCESSAMENTO DO TJMG CONCLUÍDO. REGISTROS ADICIONADOS: {total_tjmg:,} ===", flush=True)

# COMMAND ----------

# DBTITLE 1,Output: Ingestão Complementar TJMG
# MAGIC %md
# MAGIC ### Output da execução — Ingestão Complementar TJMG (02/09/2026)
# MAGIC
# MAGIC ```
# MAGIC === INICIANDO INGESTÃO COMPLEMENTAR: TJMG ===
# MAGIC -> [TJMG] Pág 1... [OK]
# MAGIC    L Gravados na Bronze: +2,500 (Acumulado TJMG: 2,500)
# MAGIC -> [TJMG] Pág 2... [OK]
# MAGIC    L Gravados na Bronze: +2,500 (Acumulado TJMG: 5,000)
# MAGIC -> [TJMG] Pág 3... [OK]
# MAGIC    L Gravados na Bronze: +2,500 (Acumulado TJMG: 7,500)
# MAGIC -> [TJMG] Pág 4... [OK]
# MAGIC    L Gravados na Bronze: +2,500 (Acumulado TJMG: 10,000)
# MAGIC -> [TJMG] Pág 5... [OK]
# MAGIC    L Gravados na Bronze: +2,500 (Acumulado TJMG: 12,500)
# MAGIC -> [TJMG] Pág 6... [OK]
# MAGIC    L Gravados na Bronze: +1,784 (Acumulado TJMG: 14,284)
# MAGIC -> [TJMG] Pág 7... [OK]
# MAGIC ✅ Concluído TJMG! Total de processos coletados: 14,284.
# MAGIC
# MAGIC === PROCESSAMENTO DO TJMG CONCLUÍDO. REGISTROS ADICIONADOS: 14,284 ===
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC #### Comentários sobre o código do notebook 01_ingestao_bronze
# MAGIC ##### Configuração da coleta
# MAGIC
# MAGIC - **Autenticação**: a API do Datajud usa o header `Authorization: APIKey {chave}`.
# MAGIC - **Filtro Elasticsearch**: o corpo da requisição é uma query `bool` com duas cláusulas
# MAGIC   `must` — intervalo de `dataAjuizamento` e lista de códigos de assunto (`terms`).
# MAGIC - **Paginação com `search_after`**: em vez de páginas numeradas (que degradam em bases
# MAGIC   grandes), a API do Datajud usa o cursor `search_after` do Elasticsearch: cada resposta
# MAGIC   retorna um array `sort`, que é repassado na requisição seguinte. O loop `while True`
# MAGIC   encerra quando a página vem vazia (`hits == []`).
# MAGIC - **Ordenação determinística** por `@timestamp` e `id.keyword` — garante que a paginação
# MAGIC   não pule nem repita registros.
# MAGIC
# MAGIC ##### Resiliência: retry com backoff exponencial
# MAGIC
# MAGIC Cada página passa por até `MAX_RETRIES = 5` tentativas. Em erros transitórios (HTTP 429
# MAGIC limite de taxa, 5xx do servidor ou timeout de rede), a rotina aguarda
# MAGIC `INITIAL_BACKOFF * 2^(tentativa-1)` segundos — 3s, 6s, 12s, 24s, 48s — antes de repetir.
# MAGIC Erros definitivos (outros códigos HTTP) interrompem o tribunal corrente e avançam para o
# MAGIC próximo, sem abortar a coleta inteira — o log registra a falha para auditoria.
# MAGIC
# MAGIC **Comando-chave**: `requests.post(url, json=payload, headers=headers, timeout=(10, 120))` —
# MAGIC o timeout em tupla separa o tempo de conexão (10s) do tempo de leitura (120s), necessário
# MAGIC páginas de 10.000 registros.
# MAGIC
# MAGIC ##### Persistência na Bronze (Load)
# MAGIC
# MAGIC Cada tribunal finaliza com a gravação do seu lote no Delta Lake:
# MAGIC
# MAGIC - `spark.createDataFrame(hits_tribunal_batch, [...])` — materializa as tuplas
# MAGIC   `(payload_json, tribunal)` coletadas no driver;
# MAGIC - `.withColumn("ingestion_timestamp", current_timestamp())` — **metadado de controle**:
# MAGIC   carimbo de data/hora da ingestão em cada registro;
# MAGIC - `.write.format("delta").mode("append").saveAsTable("default.bronze_datajud_raw")` —
# MAGIC   gravação no formato Delta com append; a tabela é criada no primeiro tribunal e
# MAGIC   estendida nos demais;
# MAGIC - `del hits_tribunal_batch` — libera a memória do driver entre lotes, permitindo coletar
# MAGIC   milhões de registros sem estourar o nó.
# MAGIC
# MAGIC A camada Bronze está fechada: nenhum notebook das camadas seguintes escreve
# MAGIC nesta tabela — apenas leem.

# COMMAND ----------

# Validação final da camada Bronze (default.bronze_datajud_raw)
df_resumo = spark.sql("""
    SELECT 
        tribunal_origem, 
        COUNT(*) AS total_processos,
        MIN(ingestion_timestamp) AS primeira_ingestao,
        MAX(ingestion_timestamp) AS ultima_ingestao
    FROM default.bronze_datajud_raw
    GROUP BY tribunal_origem
    ORDER BY total_processos DESC
""")

display(df_resumo)

# Total geral
total_bronze = spark.table("default.bronze_datajud_raw").count()
print(f"Total absoluto de registros na Bronze: {total_bronze:,}")

# COMMAND ----------

# ==============================================================================
# QA ADITIVO — valida a Bronze ANTES de seguir para o notebook 002
# (apenas leitura e print — não grava tabelas, não altera o código acima)
# ==============================================================================
bronze_qa = spark.table("default.bronze_datajud_raw")

# 1. Schema esperado (3 colunas, tipos corretos)
esperado = [("raw_payload", "string"), ("tribunal_origem", "string"), ("ingestion_timestamp", "timestamp")]
schema_ok = [(f.name, f.dataType.simpleString()) for f in bronze_qa.schema.fields] == esperado
print(f"[QA 1/5] Schema da Bronze: {'OK' if schema_ok else 'DIVERGENTE -> ' + str([(f.name, f.dataType.simpleString()) for f in bronze_qa.schema.fields])}")

# 2. Nulos nos metadados de controle (esperado: 0)
nulos = bronze_qa.filter(
    bronze_qa.raw_payload.isNull() | bronze_qa.tribunal_origem.isNull() | bronze_qa.ingestion_timestamp.isNull()
).count()
print(f"[QA 2/5] Registros com payload/tribunal/timestamp nulos: {nulos} (esperado: 0)")
assert nulos == 0, "QA falhou: existem nulos nos metadados da Bronze"

# 3. Payload JSON válido em todos os registros (esperado: 0 inválidos)
from pyspark.sql.functions import from_json, col
invalidos = bronze_qa.filter(from_json(col("raw_payload"), "map<string,string>").isNull()).count()
print(f"[QA 3/5] Payloads JSON inválidos: {invalidos} (esperado: 0)")
assert invalidos == 0, "QA falhou: payload bruto não-parseável detectado"

# 4. Contagem por tribunal — cruza com o log da rotina
print("[QA 4/5] Contagem por tribunal na Bronze:")
bronze_qa.groupBy("tribunal_origem").count().orderBy(col("count").desc()).show(30)

# 5. Contagem total de processos
total_bronze = spark.table("default.bronze_datajud_raw").count()
print(f"[QA 5/5] Contagem de processos na Bronze: {total_bronze:,}")

print("\n=== QA DA BRONZE CONCLUÍDO: pronta para leitura pelo 02_silver_transformacao ===")

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC #### QA de ingestão — verificação da Bronze (controle de passagem)
# MAGIC
# MAGIC Verificações pós-carga, apenas de leitura e log: contagem por tribunal (consistência com
# MAGIC o log da rotina), total geral, existência de nulos nos metadados e amostra do schema.
# MAGIC Nada aqui grava tabelas — é controle de passagem para a Silver.

# COMMAND ----------

# MAGIC %md
# MAGIC **Obs:**
# MAGIC 1. Esse notebook foi executado em outra pasta, por isso seus printouts estão no formato %md
# MAGIC
# MAGIC 2. O download do TJMG não foi bem sucedido na primeira tentativa, por isso houve uma ingestão complementar.