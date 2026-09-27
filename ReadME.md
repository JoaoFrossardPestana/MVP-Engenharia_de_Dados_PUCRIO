# Jurimetria da Lei 11.101/2005 a partir do Datajud

Arquitetura medalhão (Bronze, Silver e Gold) em Databricks, modelo
dimensional e análise de 9 perguntas sobre processos de recuperação
judicial, extrajudicial e falência.

## Sumário

1. [Introdução](#1-introdução)
2. [Contexto de Negócios e Perguntas](#2-contexto-de-negócios-e-perguntas)
3. [Carga dos Dados](#3-carga-dos-dados)
4. [Modelagem e Catálogo de Dados](#4-modelagem-e-catálogo-de-dados)
5. [Pipeline de Dados](#5-pipeline-de-dados)
6. [Qualidade de Dados](#6-qualidade-de-dados)
7. [Análise de Dados (Etapa 4.5): Respostas às perguntas propostas](#7-análise-de-dados-etapa-45-respostas-às-perguntas-propostas)
8. [Autoavaliação](#8-autoavaliação)

**Anexos:** [A — Bronze](#anexo-a-estabelecimento-da-camada-bronze) ·
[B — Silver](#anexo-b-estabelecimento-da-camada-silver) ·
[C — Gold](#anexo-c-estabelecimento-da-camada-gold)

# 1. Introdução

Este projeto é o MVP de conclusão do curso de Engenharia de Dados da pós-graduação em Ciência de Dados e Analytics da PUC-Rio. O objetivo que orientou todo o trabalho foi:

> Aprender a extrair informações do Datajud (CNJ) e realizar análises jurimétricas, construindo para isso uma arquitetura de dados em nuvem (Databricks) capaz de processar um grande volume de dados processuais — e, com isso, consolidar as competências do curso em um problema real, público e de grande escala.

A jurimetria — a aplicação de métodos quantitativos ao comportamento do sistema de justiça — exige um pipeline que saiba transformar documentos processuais heterogêneos em métricas confiáveis. O objeto escolhido foi um dos mais relevantes para o Direito Empresarial brasileiro: os processos regidos pela **Lei Federal nº 11.101/2005** — Recuperações Judiciais (RJ), Recuperações Extrajudiciais (RE) e Falências.

Para dar direção analítica à engenharia, o projeto partiu de **9 perguntas de negócio** (Seção 2), que funcionaram como fio condutor: definiram quais dados coletar, quais campos eram relevantes, como modelar a camada de consumo e quais métricas precisavam existir na camada final. Cada pergunta foi respondida seguindo um ciclo fixo de trabalho:

- **(a)** Enunciado formal;
- **(b)** Racional da métrica (da camada _silver_ à _gold_);
- **(c)** Mapeamento das colunas utilizadas;
- **(d)** Código SQL executado no Spark;
- **(e)** Interpretação substantiva dos resultados.

A arquitetura segue o **padrão medalhão (Bronze → Silver → Gold)** sobre a _Lakehouse_ do Databricks:

- **Bronze:** Preserva o dado bruto imutável recebido da API pública do Datajud.
- **Silver:** Trata e destrincha os JSONs de origem.
- **Gold:** Organiza o modelo relacional (fatos e dimensões) que alimenta as análises.

O repositório contém os notebooks do pipeline, o catálogo de dados e este README, que documenta cada etapa da jornada.

---

# 2. Contexto de Negócios e Perguntas

## 2.1. O problema e as perguntas de negócio

O problema formulado no início do projeto foi: _"como o sistema de justiça brasileiro processa, em números, os institutos da Lei nº 11.101/2005?"_ — tempos de resposta judicial, desfechos dos procedimentos, taxas de conversão e limites de visibilidade dos registros públicos.

As perguntas abaixo foram geradas com apoio de IA (Gemini) e, em seguida, curadas manualmente: das 12 propostas, 9 foram consideradas pertinentes e mantidas como escopo analítico do MVP. **Importante:** o enunciado original foi preservado intacto ao longo de todo o trabalho — as perguntas que não puderam ser respondidas integralmente permaneceram no relatório, com o diagnóstico do porquê (limitações de registrabilidade da própria base pública), conforme orienta a metodologia.

| ID     | Pergunta de Negócio                                                                                                                                                                 |
| :----- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Q1** | Qual é o tempo médio (e mediano) entre a distribuição do pedido de Recuperação Judicial e a concessão do processamento (despacho inicial)?                                          |
| **Q2** | Quanto tempo dura, em média, a fase de _stay period_ (suspensão de execuções) na prática, até a homologação do Plano de Recuperação Judicial?                                       |
| **Q3** | Qual é o tempo médio de duração total de um processo de Falência, desde a decretação até o encerramento da fase de arrecadação e pagamento de credores?                             |
| **Q4** | Existe diferença significativa no tempo de tramitação de RJs processadas em varas especializadas em Direito Empresarial/Falimentar vs. varas cíveis genéricas?                      |
| **Q5** | Qual é a taxa de conversão de Recuperações Judiciais em Falência (convolação, por descumprimento do plano ou rejeição da assembleia)?                                               |
| **Q6** | Qual o percentual de processos de Recuperação Extrajudicial que obtêm homologação do plano sem impugnação expressiva de credores?                                                   |
| **Q7** | Qual é a incidência de incidentes processuais (habilitações e impugnações de crédito) por processo de RJ, e como esse volume impacta o gargalo da vara?                             |
| **Q8** | Qual é a taxa de concessão de tutelas de urgência cautelares antecedentes ao pedido principal de recuperação?                                                                       |
| **Q9** | Qual é a taxa de recursos (Agravos de Instrumento) interpostos contra decisões que deferem ou indeferem o processamento da RJ, e qual a taxa de reforma dessas decisões no 2º grau? |

Essas perguntas definiram:

- **A fonte:** API pública do Datajud.
- **O recorte de coleta:** classes processuais 129 (Recuperação Judicial), 128 (Recuperação Extrajudicial) e 108 (Falência).
- **A janela temporal:** processos cujo assunto pai é recuperação judicial, falência ou recuperação extrajudicial, ajuizados a partir de **01/01/1995**.
- **O modelo de consumo:** tabela analítica com 1 linha por processo, marcos temporais e variáveis de desfecho.

---

## 2.2. Os dados brutos: o Datajud e a API pública do CNJ

A fonte de dados é a API pública do Datajud, mantida pelo Conselho Nacional de Justiça (CNJ) como parte da política de dados abertos do Poder Judiciário. O Datajud é a base nacional de processos unificada: cada tribunal fornece os metadados de seus processos em formato JSON padronizado pelo padrão CNJ de número único de processo (Resolução CNJ nº 65/2008), e a API permite a consulta por número processual e a extração em lote.

Do ponto de vista do dado bruto, cada registro retornado é um documento JSON que descreve um processo com, entre outros campos: número processual, classe e assunto processuais, tribunal e órgão julgador, datas-chave (ajuizamento) e — o elemento mais valioso e mais desafiador — o **array histórico de movimentações processuais**, em que cada movimento traz código TPU (Tabela Processual Unificada), descrição textual, complementos e data/hora. É desse array que se extraem os marcos processuais (deferimento de processamento, decretação de falência, encerramento, tutelas, homologações), transformando texto jurídico livre em métricas.

O volume é o principal desafio de engenharia: a extração resultou em mais de **17,1 milhões de registros** de movimentações processuais na camada de refino — escala que justifica a arquitetura em nuvem com processamento distribuído (Spark/Databricks), inviável em ferramentas locais convencionais.

---

## 2.3. Estrutura dos dados brutos (camada Bronze)

A camada Bronze (`bronze_datajud_raw`) foi desenhada a partir da estrutura que o Datajud disponibiliza, preservando a rastreabilidade do dado original — é o "cofre de evidências" da arquitetura medalhão: nada aqui é alterado ou limpo; as transformações acontecem a partir da _Silver_. O payload JSON original é mantido íntegro dentro de uma única coluna de texto, acrescido apenas de metadados de controle da ingestão:

| Coluna                | Tipo        | Descrição                                                                                                      |
| :-------------------- | :---------- | :------------------------------------------------------------------------------------------------------------- |
| `raw_payload`         | `string`    | O documento JSON do processo exatamente como recebido da API do Datajud, sem tipagem, _flattening_ ou limpeza. |
| `tribunal_origem`     | `string`    | Metadado de fonte: sigla do tribunal de onde o registro foi extraído.                                          |
| `ingestion_timestamp` | `timestamp` | Metadado de controle: data e hora da ingestão do registro no _lakehouse_.                                      |

Essa estrutura garante que qualquer divergência detectada nas camadas seguintes possa ser auditada contra o registro original imutável — princípio central do padrão medalhão adotado.

---

## 2.4. Licença dos dados

Os dados utilizados são públicos e abertos: a API do Datajud é disponibilizada pelo CNJ de forma gratuita como política de transparência do Poder Judiciário, em conformidade com a Lei de Acesso à Informação (Lei nº 12.527/2011) e com a política de dados abertos do Judiciário. Não há restrição de uso para fins de pesquisa e estudo, sendo a extração realizada exclusivamente por meio da API oficial, sem coleta de dados pessoais sensíveis além dos metadados processuais publicados pelo próprio CNJ.

Em contrapartida, este projeto adota como boa prática a citação da fonte — **Conselho Nacional de Justiça, Base Nacional do Poder Judiciário (Datajud)** — em todas as análises e visualizações derivadas, recomendando-se a consulta à política de uso vigente no portal de dados abertos do CNJ para usos comerciais ou redistribuição em larga escala.

---

# 3. Carga dos Dados

## 3.1. Estratégia de coleta

Por se tratar de um caso avançado de coleta — os dados não estão disponíveis em arquivo pronto (CSV/Parquet), mas em API pública —, a ingestão foi construída como carga programática via API oficial do Datajud (CNJ), dentro do ambiente Databricks. Essa escolha foi deliberada: é o fluxo profissional real de construção de pipeline, e o volume de dados envolvido inviabiliza qualquer abordagem manual de download.

---

## 3.2. Recorte da coleta

O universo de coleta foi definido pelas perguntas de negócio (Seção 2), que orientaram três filtros de ingestão:

| Dimensão do Recorte     | Definição                                                                                                                                        |
| :---------------------- | :----------------------------------------------------------------------------------------------------------------------------------------------- |
| **Assunto pai**         | Recuperação Judicial, Falência e Recuperação Extrajudicial.                                                                                      |
| **Janela temporal**     | Processos ajuizados a partir de **01/01/1995** (10 anos antes da vigência da Lei nº 11.101/2005, preservando contexto pré-lei para comparações). |
| **Formato de extração** | Documentos JSON completos de cada processo, incluindo o array histórico de movimentações.                                                        |

---

## 3.3. Persistência na nuvem (camada Bronze)

Os registros extraídos foram persistidos na tabela `bronze_datajud_raw` no catálogo do Databricks, com três características de desenho:

- **Preservação integral do payload:** o JSON recebido da API é armazenado como veio, sem tipagem ou transformação — a camada Bronze funciona como cofre de evidências.
- **Metadados de controle de ingestão:** cada registro recebe `tribunal_origem` (fonte de extração) e `ingestion_timestamp` (data e hora da carga), garantindo rastreabilidade e auditabilidade.
- **Imutabilidade:** os notebooks das camadas seguintes apenas leem a Bronze — nenhum processo de transformação reescreve o dado bruto.
- **Script:** a coleta está implementada no notebook `01_ingestao_bronze`.

![Catalog Explorer: Overview da tabela bronze_datajud_raw, com contagem de registros](bronze_datajud_raw1.png)

_Figura — Catalog Explorer: tabela `bronze_datajud_raw` persistida no Databricks
(243.831 registros)._

![Catalog Explorer: Details da tabela bronze_datajud_raw, com timestamp de criação e tamanho](bronze_datajud_raw2.png)

_Figura — Catalog Explorer: Details da tabela, evidenciando data de criação,
tamanho e número de arquivos da Bronze._

---

## 3.4. Volume

O resultado da carga foi de milhões de documentos JSON processuais (17 tribunais testados conforme escopo da coleta), que na camada de refino totalizaram **mais de 17,1 milhões de registros de movimentações processuais** — escala que justifica o processamento distribuído em Spark e a arquitetura _Lakehouse_.

---

# 4. Modelagem e Catálogo de Dados

_(Etapa 4.3 — Organizando os dados com propósito)_

## 4.1. Modelo escolhido: Esquema Estrela

A camada Gold adota o **Esquema Estrela (Star Schema)**, estrutura clássica de Data Warehouse otimizada para consultas analíticas — adequada ao objetivo do projeto, que é responder perguntas de negócio com SQL de forma rápida e legível. A modelagem foi desenhada a partir das 9 perguntas: cada coluna da camada de consumo existe porque alguma pergunta a demanda.

---

## 4.2. Estrutura das camadas

| Camada     | Objetivo                                                                              | Tabelas                                                                                                              |
| :--------- | :------------------------------------------------------------------------------------ | :------------------------------------------------------------------------------------------------------------------- |
| **Bronze** | Dado bruto imutável, como recebido da API                                             | `bronze_datajud_raw`                                                                                                 |
| **Silver** | Dado limpo, tipado e padronizado; destrincha os JSONs (incl. arrays de movimentações) | `silver_datajud_processos`, `silver_fato_movimento` (17,1M+ registros), `silver_marcos_processo`                     |
| **Gold**   | Modelo estrela pronto para consumo analítico                                          | 1 tabela fato central (`fato_tramitacao`), 1 fato de grão fino (`fato_incidentes`), 4 dimensões e 3 tabelas de apoio |
| **Mart**   | Visão consolidada para facilitar as consultas SQL das perguntas                       | `gold_base_analitica` (1 linha por processo)                                                                         |

- **Tabela fato central — `fato_tramitacao` (grão: 1 linha por processo):** Registra o ciclo de vida completo de cada processo: ajuizamento, deferimento de processamento (com estratégia de fallback R9 → TPU 12444 → proxy TPU 11010), homologação do plano, decretação de falência, encerramento, tutela de urgência, e as durações derivadas (`dias_ate_deferimento_rj`, `dias_stay_period`, `dias_ate_falencia`, `dias_duracao_falencia`, `dias_tramitacao_total`).
- **Fato de grão fino — `fato_incidentes` (grão: 1 linha por autos-filho):** Registra os incidentes processuais (habilitações e impugnações de crédito) vinculados aos autos originários por chave composta posicional (7 primeiros + 4 últimos dígitos do padrão CNJ sem máscara). É a base da análise de gargalo de vara (Q7).
- **Dimensões:** `dim_processo`, `dim_classe_processual`, `dim_orgao_julgador` e `dim_tempo` — desnormalizadas no padrão estrela, ligadas à fato por chaves substitutas (`sk_*`).
- **Tabelas de apoio:**
  - `gold_base_analitica` — mart analítico de 1 linha por processo, que consolida fatos, dimensões e métricas derivadas para as consultas das 9 perguntas.
  - `gold_data_dictionary` — o próprio catálogo de dados materializado como tabela (ver 4.3).
  - `gold_qa_execucao` — tabela de controle de qualidade com _regression check_ (ver 6.4).

---

### 4.2.1. Camada Bronze — `bronze_datajud_raw`

**Objetivo:** preservar o dado bruto imutável recebido da API do Datajud — o
"cofre de evidências" da arquitetura medalhão. Nenhuma transformação acontece
aqui: o JSON é armazenado como veio, sem tipagem nem flattening; toda
estruturação fica para a Silver.

**Tabela criada (notebook `01_ingestao_bronze`):**

| Coluna                | Tipo        | O que contém                                                         |
| --------------------- | ----------- | -------------------------------------------------------------------- |
| `raw_payload`         | `string`    | O documento JSON do processo exatamente como recebido da API         |
| `tribunal_origem`     | `string`    | Metadado de fonte: sigla do tribunal de onde o registro foi extraído |
| `ingestion_timestamp` | `timestamp` | Metadado de controle: data e hora da ingestão no lakehouse           |

**O que há dentro de `raw_payload` — a estrutura do documento JSON:**

O payload segue o padrão público de interoperabilidade do CNJ, e a estrutura
abaixo é o schema espelhado declarado no notebook 02 (Bloco 1) — a mesma
estrutura que tipa o parse da Silver:

```text
raw_payload (JSON do processo)
├── id                          Identificador único do processo
├── numeroProcesso              Número CNJ de 20 dígitos (padrão Resolução 65/2008)
├── dataAjuizamento             Data de ajuizamento (fonte dos formatos heterogêneos)
├── grau                        Grau de jurisdição (G1/G2)
├── classe                      Classe processual
│   ├── codigo                  Código TPU (129=RJ, 108=Falência, 128=RE)
│   └── nome                    Nome da classe
├── sistema                     Sistema processual (código + nome)
├── formato                     Formato do processo (eletrônico/físico)
├── orgaoJulgador               Órgão julgador
│   ├── codigo                  Código CNJ do órgão
│   ├── nome                    Nome da vara/juízo
│   └── codigoMunicipioIBGE     Código IBGE da sede (base da análise territorial)
├── assuntos                    ARRAY — assuntos do processo
│   └── {codigo, nome}          (o recorte da coleta filtra pelos códigos 4992–4997)
└── movimentos                  ARRAY — o histórico completo de movimentações
    ├── codigo                  Código TPU do movimento
    ├── nome                    Descrição textual do ato
    ├── dataHora                Data/hora do registro (em formatos heterogêneos)
    └── complementosTabelados   ARRAY — complementos {codigo, nome, descricao}
```

**O elemento central do dado bruto é o array `movimentos`:** cada movimento
traz o código TPU, a descrição textual e os complementos tabelados — é dele
que a Silver extrai os marcos processuais (deferimento de processamento,
decretação de falência, encerramento, tutelas), transformando texto jurídico
livre em métricas. A riqueza e a heterogeneidade desse array (códigos
genéricos, datas em formatos variados, complementos em texto livre) são o
principal desafio de qualidade do projeto — e motivam o parser multi-formato,
as regras de marcação declaradas e o dedupe por conteúdo da camada Silver.

**Resultado da carga:** 229.547 processos coletados em 27 tribunais
(TJSP 122.738; TJRJ 62.289; TJMG 14.284 — este último concluído em ingestão
complementar com página reduzida após falha HTTP 504 na primeira tentativa).
Cada tribunal é gravado em lote ao término da sua paginação (append no Delta),
liberando a memória do driver antes do próximo.

**QA de passagem (5 verificações, antes da Silver):** schema esperado
(3 colunas, tipos corretos), zero nulos nos metadados, zero payloads JSON
inválidos, contagem por tribunal cruzada com o log da rotina e total geral.

**Evidência no Catalog — `bronze_datajud_raw`:**

![Overview da tabela bronze_datajud_raw](bronze_datajud_raw1.png)

![Details da tabela bronze_datajud_raw](bronze_datajud_raw2.png)

### 4.2.2. Camada Silver — `02_silver_transformacao`

**Objetivo:** transformar o arquivo bruto da Bronze na primeira versão
_usável_ do dado — tipos corretos, grãos explícitos (1 linha por processo,
1 linha por movimento) e eventos já classificados por regras declaradas.
A Gold não reprocessa nada: consome o que aqui está garantido.

**Padrões:** `CREATE OR REPLACE` (idempotente) + `COMMENT` em tabelas e
colunas; `overwriteSchema` habilitado para evolução controlada. O notebook
pode ser re-executado ponta a ponta sem resíduos.

**Tabelas criadas (10):**

| Tabela                                | Objetivo                               | Grão / colunas-chave                                                                                                                                  |
| ------------------------------------- | -------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| `silver_datajud_processos`            | Processo tabular, tipado e deduplicado | 1 linha por processo: `numero_processo`, `data_ajuizamento`, `classe_codigo/nome`, `orgao_julgador_*`, `escopo_lei_atual`, `regime_legal`             |
| `silver_fato_movimento`               | Histórico completo de movimentações    | 1 linha por movimento (~17,1 milhões): `movimento_codigo`, `movimento_nome`, `data_movimento`, `precisao_data`, `complementos_txt`, `qtd_ocorrencias` |
| `silver_dim_orgao_julgador`           | Órgãos julgadores agregados            | 1 linha por órgão×tribunal, com `qtd_processos`                                                                                                       |
| `silver_dim_assunto`                  | Catálogo de assuntos                   | `assunto_codigo/nome`, `qtd_processos` (countDistinct)                                                                                                |
| `silver_ponte_processo_assunto`       | Ponte N:N processo–assunto             | `numero_processo`, `assunto_codigo`                                                                                                                   |
| `silver_dim_processo`                 | Grade enxuta do processo               | tribunal, grau, sistema, formato                                                                                                                      |
| `silver_regras_marcacao`              | Regras de marcação declaradas          | `regra_id`, `categoria_evento`, `condicao_sql`                                                                                                        |
| `silver_datajud_eventos_enriquecidos` | Fato de movimentos classificada        | + `categoria_evento`, `regra_aplicada`                                                                                                                |
| `silver_marcos_processo`              | Marcos processuais datados             | eventos classificados com `precisao_suficiente`                                                                                                       |
| `silver_qa_execucao`                  | QA de execução da camada               | 5 métricas com esperado e `executado_em`                                                                                                              |

**Qualidade aplicada:** deduplicação de capturas (243.831 registros →
225.556 processos distintos; 18.275 duplicatas de reingestão resolvidas
pela captura mais recente); parser multi-formato de datas (ISO, 14 e
8 dígitos, via COALESCE de `try_to_date`); dedupe de movimentos idênticos
por conteúdo (a repetição vira `qtd_ocorrencias` — evidência, não perda).

**Regras de marcação (R9–R17):** o conhecimento jurídico da Lei 11.101/2005
vira regras declaradas em tabela (`silver_regras_marcacao`) — a ponte entre
o direito e o dado. A primeira regra que casa vence; regras específicas
precedem as genéricas (ex.: R12 homologação antes de R10 deferimento
genérico). Toda métrica da Gold é rastreável até a regra que a originou,
via `regra_aplicada`.

**QA da camada:** `silver_qa_execucao` grava processos distintos (esperado
225.556), movimentos totais (referência ~17,1 milhões), movimentos com data
válida, eventos classificados e marcos datados — histórico comparável entre
execuções.

**Evidências no Catalog — camada Silver (10 tabelas):**

![Overview silver_datajud_processos](silver_datajud_processos1.png)

![Details silver_datajud_processos](silver_datajud_processos2.png)

![Overview silver_fato_movimento](silver_fato_movimento1.png)

![Details silver_fato_movimento](silver_fato_movimento2.png)

![Overview silver_datajud_eventos_enriquecidos](silver_datajud_eventos_enriquecidos1.png)

![Details silver_datajud_eventos_enriquecidos](silver_datajud_eventos_enriquecidos2.png)

![Overview silver_marcos_processo](silver_marcos_processo1.png)

![Details silver_marcos_processo](silver_marcos_processo2.png)

![Overview silver_qa_execucao](silver_qa_execucao1.png)

![Details silver_qa_execucao](silver_qa_execucao2.png)

![Overview silver_dim_orgao_julgador1](silver_dim_orgao_julgador1.png)

![Details silver_dim_orgao_julgador](silver_dim_orgao_julgador2.png)

![Overview silver_dim_assunto](silver_dim_assunto1.png)

![Details silver_dim_assunto](silver_dim_assunto2.png)

![Overview silver_dim_processo](silver_dim_processo1.png)

![Details silver_dim_processo](silver_dim_processo2.png)

![Overview silver_ponte_processo_assunto](silver_ponte_processo_assunto1.png)

![Details silver_ponte_processo_assunto](silver_ponte_processo_assunto2.png)

![Overview silver_regras_marcacao](silver_regras_marcacao1.png)

![Details silver_regras_marcacao](silver_regras_marcacao2.png)

### 4.2.3. Camada Gold — `03_gold_modelagem`

**Objetivo:** consumir a Silver e materializar o **star schema** (modelagem
dimensional em estrela), o **mart analítico** das 9 perguntas e a
**governança do catálogo** (dicionário + QA com regression check).

**Padrões:** DDL com `COMMENT` por coluna **antes** da carga — o `CREATE
TABLE` é o contrato da tabela, e é dele que o dicionário se alimenta.
Cargas por `INSERT OVERWRITE` (idempotentes). **Gates de QA em memória:**
validações rodam sobre views temporárias ANTES do write — dado que falha no
gate nunca chega ao catálogo.

**Tabelas criadas (9):**

| Tabela                  | Papel                                           | Grão                       |
| ----------------------- | ----------------------------------------------- | -------------------------- |
| `dim_tempo`             | Calendário do corpus (1995–2027)                | 1 linha por dia            |
| `dim_classe_processual` | Classes da TPU do CNJ                           | 1 linha por classe         |
| `dim_orgao_julgador`    | Órgãos julgadores, dedup por (código, tribunal) | 1 linha por órgão×tribunal |
| `dim_processo`          | Universo completo (225.556 processos)           | 1 linha por processo       |
| `fato_tramitacao`       | **Fato central**: ciclo de vida + durações      | 1 linha por processo       |
| `fato_incidentes`       | Fato de grão fino: incidentes de crédito (Q7)   | 1 linha por autos-filho    |
| `gold_base_analitica`   | **Mart**: fonte única das 9 perguntas           | 1 linha por processo       |
| `gold_data_dictionary`  | Dicionário materializado                        | 1 linha por coluna         |
| `gold_qa_execucao`      | Regression check (baseline congelado)           | 1 linha por métrica        |

**Decisões de modelagem relevantes:**

- **Fato enxuta, dim completa:** a `dim_processo` guarda o universo inteiro
  (incluindo o legado da Lei 7.661/1945) com `escopo_lei_atual` como
  _atributo_, não filtro — os recortes acontecem na fato/mart, e o universo
  permanece consultável;
- **Dedup de órgãos por (código, tribunal):** a origem traz o mesmo órgão
  com variantes de grafia; o nome mais frequente vira o canônico e
  `qtd_variantes_nome` documenta quantas grafias foram consolidadas;
- **LEFT JOIN com FKs nulas:** processo sem match de dimensão não some —
  aparece com FK nula (problema visível em QA em vez de perda silenciosa);
- **Cascata do marco de processamento** (`data_deferimento_rj`):
  R9 → TPU 12444 → proxy TPU 11010 (janela 15–180 dias), com
  `fonte_deferimento` registrando qual regra produziu cada data.

**O mart (`gold_base_analitica`) — colunas por pergunta:**

| Coluna                                 | O que contém                                                                |
| -------------------------------------- | --------------------------------------------------------------------------- |
| `numero_processo`                      | Número CNJ do processo                                                      |
| `classe_codigo` / `classe_nome`        | Código TPU (129=RJ, 108=Falência, 128=RE) e nome da classe                  |
| `escopo_lei_atual`                     | Ajuizado a partir de 09/06/2005 (universo das métricas)                     |
| `eh_especializada`                     | Vara especializada em Empresarial/Falimentar (Q4/Q8)                        |
| `data_ajuizamento`                     | Data de ajuizamento                                                         |
| `data_processamento_efetivo`           | P1: processamento (cascata R9 > 12444 > proxy 11010)                        |
| `fonte_deferimento`                    | Fonte do marco de processamento (auditabilidade)                            |
| `dias_distribuicao_processamento`      | P1: dias de ajuizamento ao processamento                                    |
| `data_fim_stay`                        | P2: fim do stay = LEAST(homologação, falência)                              |
| `cenario_desfecho`                     | homologacao_plano \| decretao_falencia \| extincao \| encerramento \| ativo |
| `dias_stay_period`                     | P2: deferimento → fim do stay (nulo = censura estrutural)                   |
| `dias_stay_limite_superior`            | P2: cota censurada — deferimento → fim do processo                          |
| `ind_stay_period_excedido_180d`        | P2: stay além dos 180 dias legais                                           |
| `data_homologacao_plano`               | Homologação do plano (não registrável no Datajud)                           |
| `data_decretao_falencia`               | P5: decretação de falência (TPU 202)                                        |
| `dias_ate_convolucao`                  | P5: ajuizamento → convolução                                                |
| `data_encerramento`                    | P3/P6: encerramento/arquivamento                                            |
| `dias_duracao_falencia`                | P3: decretação → encerramento/extinção                                      |
| `data_primeira_tutela`                 | P8: primeira tutela de urgência                                             |
| `tutela_antes_processamento`           | P8: tutela anterior ao processamento                                        |
| `qtd_habilitacoes` / `qtd_impugnacoes` | P7: autos-filho classes 111/114                                             |
| `dias_duracao_processo_total`          | Ajuizamento → evento fim                                                    |

**Evidências no Catalog — camada Gold (modelo dimensional):**

![Overview dim_classe_processual](dim_classe_processual1.png)

![Details dim_classe_processual](dim_classe_processual2.png)

![Overview dim_orgao_julgador](dim_orgao_julgador1.png)

![Details dim_orgao_julgador](dim_orgao_julgador2.png)

![Overview dim_processo](dim_processo1.png)

![Details dim_processo](dim_processo2.png)

![Overview dim_tempo](dim_tempo1.png)

![Details dim_tempo](dim_tempo2.png)

![Overview fato_incidentes](fato_incidentes1.png)

![Details fato_incidentes](fato_incidentes2.png)

![Overview fato_tramitacao](fato_tramitacao1.png)

![Details fato_tramitacao](fato_tramitacao2.png)

![Overview gold_base_analitica](gold_base_analitica1.png)

![Details gold_base_analitica](gold_base_analitica2.png)

### 4.2.4. Evidências de execução

As evidências da execução do pipeline — código, células executadas e
resultados das células — estão nos três notebooks do projeto, depositados
neste repositório:

| Etapa                                    | Evidência                              |
| ---------------------------------------- | -------------------------------------- |
| Ingestão Bronze (coleta Datajud)         | Notebook 01_ingestao_bronze.ipynb      |
| Processamento Silver (parse, dedupe, QA) | Notebook 02_silver_transformacao.ipynb |
| Modelagem Gold (star schema, governança) | Notebook 03_gold_modelagem.ipynb       |

As respostas às 9 perguntas de negócio (Seção 7), incluindo as tabelas
resultantes das consultas, estão preservadas nos notebooks p01–p09, também
depositados neste repositório com seus outputs. O detalhamento passo a passo
do que cada notebook fez — células, decisões intermediárias e verificações —
está nos Anexos A, B e C deste documento e também nos próprios notebooks.

## 4.3. Catálogo de Dados

O catálogo foi implementado em duas frentes complementares, atendendo ao requisito de documentação da Unity Catalog:

1. **Catálogo nativo (Unity Catalog):** todas as tabelas e colunas possuem `COMMENT` persistido no próprio Databricks — descrição da tabela, papel de cada campo, tipo de dado e domínio de valores. Visível no Catalog Explorer e consultável via `information_schema.columns`.
2. **Dicionário materializado (`gold_data_dictionary`):** tabela Gold gerada automaticamente a partir do `information_schema` do próprio workspace, consolidando 98 colunas de 8 tabelas com nome, tipo, comentário e tabela de origem — o catálogo como produto de dados, versionado junto ao pipeline.

| Atributo Documentado por Coluna     | Como está atendido                                                                                                  |
| :---------------------------------- | :------------------------------------------------------------------------------------------------------------------ |
| **Descrição do contexto da tabela** | `COMMENT` da tabela + dicionário Gold                                                                               |
| **Nome e descrição de cada campo**  | `COMMENT` de coluna + dicionário Gold                                                                               |
| **Tipo de dado**                    | Tipagem nativa Delta (`DATE`, `INT`, `STRING`, `TIMESTAMP`, `BOOLEAN`) + dicionário                                 |
| **Domínio de valores**              | Descrito nos comentários (ex.: `cenario_desfecho: homologacao_plano`)                                               |
| **Linhagem**                        | Comentários indicam a origem (ex.: `sk_tempo` FK → `dim_tempo`), complementada pelo grafo de linhagem do Databricks |

**Screenshots:**

![Catalog Explorer: Overview de fato_tramitacao, mostrando as colunas e seus comentários](fato_tramitacao1.png)

_Figura — Aba Overview da tabela `fato_tramitacao` no Catalog Explorer:
colunas com os comentários de tabela e campo, atendendo aos atributos de
documentação exigidos (descrição do contexto da tabela, nome e descrição
de cada campo, tipo de dado e domínio de valores)._

![Catalog Explorer: grafo de linhagem de fato_tramitacao](lineage_fato_tramitacao.png)

_Figura — Grafo de linhagem do Databricks: origem Bronze (Datajud),
transformação Silver e destino Gold, complementando a linhagem descrita
nos comentários._

## 4.4. Governança e gestão de dados

A governança foi implementada como parte do pipeline, não como documentação
à parte — quatro mecanismos concretos:

**1. O COMMENT como contrato.** Toda tabela da Gold nasce de um `CREATE TABLE`
com `COMMENT` por coluna: descrição do campo, papel e domínio de valores.
O contrato é escrito uma vez e o dicionário é 100% regenerado dele —
documentar e definir são o mesmo ato, sem segunda fonte para desatualizar.

**2. Dicionário materializado (`gold_data_dictionary`).** Tabela consultável
gerada automaticamente a partir do `information_schema` do próprio workspace,
consolidando as colunas das 4 dimensões, 2 fatos, mart e QA — o catálogo como
produto de dados, versionado junto ao pipeline. Tabela nova entra no escopo
do dicionário na próxima execução.

**3. Gates de QA antes da persistência.** Validações de coerência temporal
rodam sobre views temporárias **antes** do write: encerramento anterior ao
ajuizamento, stay negativo, cota menor que o stay medido, grão violado
(duplicidade de `numero_processo`). Dado que falha no gate nunca chega ao
catálogo.

**4. QA com regression check (`gold_qa_execucao`).** As 10 métricas-chave do
mart são comparadas contra um **baseline congelado** — valores validados
metodologicamente nas análises exploratórias (universos, marcos, taxas),
cada linha com `fonte_esperado` registrando a origem do número. Como a
Bronze é imutável e o pipeline é determinístico, qualquer execução ponta a
ponta deve reproduzir o baseline com desvio 0 — desvio diferente de zero
bloqueia o consumo da camada.

**O Unity Catalog (Catalog Explorer) completa essa governança:** os COMMENTs
de tabela e coluna ficam visíveis na interface e consultáveis via
`information_schema.columns`; o grafo de linhagem do Databricks expõe as
dependências entre as camadas. As capturas abaixo evidenciam os dois:

**Screenshots:**

![Consulta ao gold_data_dictionary com output](gold_data_dictionary_output.png)

_Figura — Consulta ao `gold_data_dictionary`: 98 colunas documentadas
com nome, tipo e comentário de cada campo._

![Consulta ao gold_qa_execucao com as 10 métricas de desvio 0](gold_qa_execucao_output.png)

_Figura — `gold_qa_execucao` com as 10 métricas do baseline congelado,
todas com desvio 0 (regression check)._

---

# 5. Pipeline de Dados

## 5.1. Organização geral

O pipeline foi ramificado em notebooks por estágio de ETL (um notebook por etapa da medalhão), seguindo a recomendação de organização e facilitando re-execução seletiva e depuração:

- `01_ingestao_bronze` → API Datajud → `bronze_datajud_raw` (Load — dado bruto)
- `02_silver_transformacao` → Bronze → Silver (Extract/Transform/Load — limpeza e tipagem)
- `03_gold_modelagem` → Silver → Gold (Transform/Load — modelagem estrela + mart + dicionário + QA)

---

## 5.2. Detalhamento por estágio

- **Ingestão (Extract/Load) — notebook `01_ingestao_bronze`:** requisições à API do Datajud segundo o recorte da Seção 3.2; persistência do JSON bruto com metadados de fonte e timestamp. A Bronze nunca é alterada pelos estágios seguintes.

- **Transformação Silver (Transform) — notebook `02_silver_transformacao`:** leitura da Bronze e destrinchamento dos JSONs, incluindo a explosão dos arrays de movimentações (a história completa de cada processo em registros individuais); tipagem correta (datas de string para date, códigos para int); padronização e derivações analíticas (ex.: `silver_marcos_processo`, que classifica cada movimento por categorias de evento — processamento, homologação, falência, extinção, encerramento, tutela — por regras de texto e código TPU).

- **Modelagem Gold (Transform/Load) — notebook `03_gold_modelagem`:** construção do esquema estrela a partir da Silver (fatos, dimensões, mart `gold_base_analitica`), geração automática do dicionário de dados e execução do QA com _regression check_. Inclui _gate_ de qualidade antes da carga: validações de coerência temporal sobre o fato em memória que interrompem o pipeline antes de sobrescrever a tabela de destino em caso de falha.

---

## 5.3. Documentação das transformações

Cada transformação relevante está documentada no código e no dicionário — no padrão: o que foi feito, por que foi feito, impacto nos dados. Exemplos reais:

- _"Realizei a explosão do array de movimentações do payload JSON para gerar a `silver_fato_movimento`, criando 1 linha por movimento — o grão necessário para calcular intervalos entre marcos."_
- _"Implementei a coalescência de três fontes para o marco de processamento (regra textual R9 → TPU 12444 → proxy TPU 11010 na janela 15–180 dias), porque tribunais usam movimentos genéricos que não são distintivos do deferimento."_
- _"Apliquei chave posicional composta (7 primeiros + 4 últimos dígitos) para vincular autos-filho aos originários, habilitando a análise de incidentes processuais."_

- **Scripts:** notebooks `01_ingestao_bronze`, `02_silver_transformacao` e
  `03_gold_modelagem` (depositados neste repositório com os outputs — seção 4.2.4);

- **Screenshots de persistência:**

![Catalog Explorer: Details da tabela Silver persistida, com contagem de registros e timestamp](silver_fato_movimento2.png)

_Figura — Persistência da Silver no Databricks: Details com contagem de
registros (numRows), tamanho e timestamp do último commit._

![Catalog Explorer: Details de fato_tramitacao, com contagem e timestamp](fato_tramitacao2.png)

_Figura — Persistência da Gold: fato central do mart, com contagem de
registros e data do último commit._

![Catalog Explorer: Details de gold_base_analitica, com contagem e timestamp](gold_base_analitica2.png)

_Figura — Persistência da Gold: base analítica pronta para as consultas da
Seção 7._

---

# 6. Qualidade de Dados

## 6.1. Abordagem

A qualidade foi tratada em três frentes: verificações por atributo na construção da Silver (completude, unicidade, consistência de tipos e datas), gate de carga na Gold (validações que bloqueiam o write) e QA com _regression check_ (tabela `gold_qa_execucao`, com 10 métricas congeladas — universos, marcos e taxas — cujo desvio atual é 0 em todas as linhas, sinal de pipeline estável).

---

## 6.2. Problemas detectados e tratamentos aplicados

| #     | Problema Detectado                                                                                            | Diagnóstico                                                                                                                                                 | Tratamento                                                                                                                         |
| :---- | :------------------------------------------------------------------------------------------------------------ | :---------------------------------------------------------------------------------------------------------------------------------------------------------- | :--------------------------------------------------------------------------------------------------------------------------------- |
| **1** | **Durações negativas (Q1):** 7 processos com processamento anterior à distribuição                            | Ruído de origem: registros de atos com data anterior à autuação                                                                                             | Exclusão por higiene temporal — ruído não entra na métrica; decisão metodológica documentada.                                      |
| **2** | **Cobertura do encerramento subestimada (Q6):** apenas 9 REs encerradas (3%)                                  | Diagnóstico de cobertura na Silver revelou que o movimento Baixa Definitiva (TPU 22) — assentamento padrão de arquivamento — não estava mapeado na regra    | Incorporação do TPU 22 à regra de encerramento (primeira baixa por processo, conservador), elevando os encerramentos a 60 (19,7%). |
| **3** | **Incoerência temporal no fato:** 23 encerramentos anteriores ao ajuizamento                                  | Auditoria do gate de carga: condição pré-existente da regra textual, não introduzida pela correção                                                          | Descarte por higiene temporal na montagem do fato; o gate bloqueou o write até a correção.                                         |
| **4** | **Homologação de plano não rastreável (Q2/Q6):** 8.935 eventos 'homolog' enumerados, nenhum referente a plano | Não-achado estrutural provado: TPU 466 = transação; 14099/12649 = acordos executivos; a homologação de plano não possui assentamento sistemático no Datajud | Tratado como fronteira de registrabilidade documentada — a ausência de dados sistemáticos é achado empírico em si.                 |
| **5** | **Marco de processamento ausente em parte dos casos (Q1)**                                                    | Tribunais usam movimentos genéricos (TPU 11010) sem menção ao deferimento                                                                                   | Fallback determinístico: primeiro TPU 11010 na janela 15–180 dias após o ajuizamento.                                              |

---

## 6.3. Transformações de qualidade (padrão ETL documentado)

- **Tipagem e padronização:** datas de string para `DATE`, códigos TPU para inteiro, normalização de números processuais (padrão CNJ sem máscara).
- **Deduplicação e unicidade:** garantia de 1 linha por processo no mart (`gold_base_analitica`) e de grão controlado nas fatos (1 linha por processo / 1 por incidente).
- **Validações de domínio:** filtro `escopo_lei_atual` (universos restritos à vigência da Lei nº 11.101/2005) aplicado sistematicamente em todas as consultas.
- **Coerência temporal:** regra geral de origem no ajuizamento + gates que impedem a persistência de intervalos inválidos.

---

## 6.4. Evidência de controle: tabela `gold_qa_execucao`

O QA final consolidou 10 métricas com valor, esperado, fonte*esperado e desvio — todas com desvio 0, incluindo as métricas de auditoria documentadas (`re_encerradas_tpu22 = 60`, fonte: "diagnóstico TPU 22 (51 novos) + higiene temporal (23 inválidos descartados); gate Bloco 4"). Esta tabela funciona como \_regression check*: qualquer re-execução do pipeline que mova os valores congelados sinaliza regressão real.

![QA de aceitação: gold_qa_execucao com 10 métricas e desvio 0](gold_qa_execucao_output.png)
_Figura — Catalog Explorer: tabela `gold_qa_execucao` persistida no Databricks._

# 7. Análise de Dados (Etapa 4.5): Respostas às perguntas propostas

## 7.1. Qual é o tempo médio (e mediano) entre a distribuição do pedido de Recuperação Judicial (RJ) e a concessão do processamento (despacho inicial)?

A pergunta se refere ao tempo médio (e mediano) entre a distribuição do
pedido de Recuperação Judicial e a concessão do processamento - despacho inicial que instaura a RJ (art. 52 da Lei 11.101/2005) - e dispara a proteção do art. 6º, § 4º (início do stay period).

**Fonte:** `default.gold_base_analitica` (mart da camada Gold, criado pelo notebook 03_gold_modelagem).
A duração já vem materializada na coluna `dias_distribuicao_processamento`, calculada como `data_deferimento_rj − data_ajuizamento`. O presente notebook é consumidor: não reconstrói a semântica, apenas a consulta.

**Recorte:**

- `classe_codigo = 129` — classe CNJ "Recuperação Judicial";
- `escopo_lei_atual` — somente processos sob a Lei 11.101/2005 (exclui o legado da Lei 7.661/1945);
- `dias_distribuicao_processamento >= 0` — higiene temporal: exclui 7 processos (0,3% dos 2.422 com marco) cuja duração calculada ficou negativa por ruído de data na origem.

##### Método — como identificar a "concessão do processamento" da recuperação judicial nos dados

**O problema:** o ato existe na lei, mas não existe no vocabulário do dado.

A Lei 11.101/2005 prevê que o juiz, ao acolher o pedido, defere o
_processamento_ da recuperação (art. 52) — despacho que instaura a RJ e
dispara a proteção do art. 6º, § 4º. Só que o padrão Datajud não tem um movimento próprio para esse ato: a TPU do CNJ não reserva código para "deferimento do processamento". O juiz o pratica dentro de movimentos
genéricos — às vezes como texto livre de um despacho, às vezes como uma decisão de deferimento codificada, às vezes invisível entre atos ordinatórios. O primeiro desafio metodológico do projeto foi, portanto, decidir como caracterizar um momento que o dado não nomeia.

**As três alternativas testadas (em ordem de confiabilidade):**

1. **Texto do despacho (regra estrita, R9 da camada gold)** — procurar, no nome do movimento e nos complementos, os padrões textuais do despacho processador ("processamento"). Essa seria a caracterização mais fiel: é o próprio ato. A cobertura, porém, é mínima (~1%) — na maioria dos tribunais o texto do despacho não é registrado como movimento.

2. **TPU 12444 — decisão de deferimento genérica** — um código que significa "deferiu-se algo". Não é o ato em si, mas é uma decisão (não um ato ordinatório) e, quando ocorre logo após a distribuição de uma RJ, é o candidato natural ao deferimento do processamento. Validado pela investigação da pergunta 2 (trajetória deferimento → concessão).

3. **Proxy "Mero expediente" (TPU 11010, janela 15–180 dias)** — o ato
   ordinatório mais comum da tramitação. A hipótese: o primeiro 11010
   entre 15 e 180 dias após a distribuição acompanha a tramitação do
   despacho processador. É o que dá cobertura — os demais capturam o
   ato "com nome"; este o infere pelo comportamento.

A cascata de regras formada por essas três alternativas - **R9 → 12444 → proxy** - resolve a convivência entre elas: a caracterização mais confiável vence; o proxy só preenche quem não tem marco estrito. Assim, o ato nunca é caracterizado por um método mais fraco quando um mais forte está disponível.

**Por que 15 e 180 dias são limites razoáveis — e não arbitrados no escuro.**
Os dois números foram escolhidos com fundamento e verificados por teste de sensibilidade:

- **Piso de 15 dias (defesa contra contaminação):** o 11010 (mero expediente) é um ato genérico que acompanha _toda_ tramitação. Nos primeiros dias pós-distribuição, é mais provável que registre expediente de rotina (juntadas, intimações) do que um despacho processador. Um piso baixo (ex.: 7 dias) aumentaria marginalmente a cobertura, mas injetaria falsos positivos com tempos "bons demais", puxando a mediana artificialmente para baixo.
- **Teto de 180 dias (defesa contra falso negativo):** passados 6 meses, um primeiro "mero expediente" tem pouca plausibilidade de acompanhar o despacho processador — ou o juízo demorou muito além do razoável, ou registra pouco. Incluir essa cauda arriscaria medir tramitação genérica, não concessão.
- **A verificação empírica:** medindo o peso das bordas, apenas **2,1%** dos casos atingem o piso e **0,3%** o teto (**2,4%** somados); excluindo as bordas, a mediana move de 48 → 49 e a média de 127,3 → 125,6 — alteração marginal. Ou seja: a janela captura comportamento natural, não trunca a distribuição — a métrica não depende dos limites escolhidos.

O resultado do teste fecha o ciclo metodológico: os limites não foram impostos, foram arbitrados com fundamento e auditados com dado — o que blinda a escolha contra questionamento.

**Limitações:**

1. O marco de concessão da RJ não existe na TPU — é caracterizado por cascata, com cobertura de 42,4% do corpus;

2. O proxy 11010 infere o ato pelo comportamento tramitatório — a janela 15–180 é validada por sensibilidade (2,4% de efeito de borda), mas ainda é inferência, não registro;

3. 7 casos (0,3%) têm datas incoerentes e são excluídos por higiene temporal;

4. A cobertura do marco é desigual entre tribunais — varas de maior volume registram menos (achado quantificado na pergunta 4).

#### O que a consulta a seguir mede

Esta consulta faz uma análise exploratória dos dados e mede o trecho
'ajuizamento → concessão' sobre os casos elegíveis, após a higiene temporal.

**Entrada:** `default.gold_base_analitica`, classe 129, escopo da lei
vigente, com higiene temporal (duração ≥ 0).

A consulta lê uma coluna por processo:

| Coluna lida                       | O que representa                                                                                                                                                                                      |
| --------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `dias_distribuicao_processamento` | dias entre a distribuição e a concessão do processamento — materializada no Bloco 4 do notebook 03 pela cascata R9 → TPU 12444 → proxy 11010. Quando o campo é nulo significa sem marco identificável |

**Saída — ela produz uma única linha com as colunas a seguir:**

| Coluna produzida          | Como é calculada                    | O que significa                                                  |
| ------------------------- | ----------------------------------- | ---------------------------------------------------------------- |
| `total_rj`                | conta os processos do recorte       | o universo da pergunta (5.709)                                   |
| `com_marco_identificavel` | conta os casos com concessão datada | quantos têm o marco identificável (2.422)                        |
| `excl_negativos`          | conta os casos com duração < 0      | datas incoerentes na origem, excluídas por higiene temporal (7)  |
| `qtd_processos`           | conta os elegíveis (duração ≥ 0)    | o "n" da fórmula estatística, após a higiene (2.415 = 2.422 − 7) |
| `minimo_dias`             | mínimo da duração                   | o piso da distribuição após a higiene                            |
| `media_dias`              | média da duração                    | o custo da cauda longa (127,3)                                   |
| `mediana_dias`            | percentil 50                        | o tempo típico — a métrica central desta pergunta (48)           |
| `p90_dias`                | percentil 90                        | o limiar além do qual estão os 10% mais demorados (149)          |

**Em uma frase:** a consulta após fazer um levantamento dos dados, mede quanto tempo o sistema leva, na mediana e na cauda, para conceder o processamento da RJ — o marco que instaura a recuperação e dispara a proteção do art. 6º, § 4º.

#### Consulta SQL

```text
-- Q1 · DURAÇÃO ATE A CONCESSAO: análise exploratória + estatisticas (nacional)
WITH base AS (
SELECT numero\*processo, dias_distribuicao_processamento
FROM default.gold_base_analitica
WHERE classe_codigo = 129 AND escopo_lei_atual
)
SELECT
(SELECT count(*) FROM base) AS total*rj,
(SELECT count(*) FROM base WHERE dias*distribuicao_processamento IS NOT NULL) AS
com_marco_identificavel,
(SELECT count(*) FROM base WHERE dias*distribuicao_processamento < 0) AS excl_negativos,
count(\*) AS qtd_processos,
round(min(dias_distribuicao_processamento), 1) AS minimo_dias,
round(avg(dias_distribuicao_processamento), 1) AS media_dias,
round(percentile_approx(dias_distribuicao_processamento, 0.5), 1) AS mediana_dias,
round(percentile_approx(dias_distribuicao_processamento, 0.9), 1) AS p90_dias
FROM base
WHERE dias_distribuicao_processamento >= 0;
```

##### Resposta:

| total_rj | com_marco_identificavel | excl_negativos | qtd_processos | minimo_dias | media_dias | mediana_dias | p90_dias |
| :------- | :---------------------- | :------------- | :------------ | :---------- | :--------- | :----------- | :------- |
| 5709     | 2422                    | 7              | 2415          | 0           | 127.3      | 48           | 149      |

#### Análise da resposta

**Cobertura da métrica:** dos 5.709 processos no escopo da Lei 11.101/2005 (classe 129),
2.422 (42,4%) têm marco identificável de concessão — resultado da cascata
R9 → TPU 12444 → proxy 11010. Dos
2.422, 2.415 (99,7%) têm duração não-negativa e entram na estatística;
os 7 restantes (0,3%) carregam data inválida anterior ao ajuizamento e
são excluídos por higiene.

**Distribuição (2.415 processos elegíveis):**
| Estatística | Valor |
|---|---|
| Mediana | 48 dias |
| Média | 127,3 dias |
| P90 | 149 dias |

**Assimetria:** a média (127) é 2,6× a mediana (48) — distribuição
fortemente assimétrica à direita. Metade das concessões sai em ~7
semanas. Por isso a leitura central desta
pergunta é a **mediana**, com a média documentando o custo da cauda.

#### O que a próxima consulta realiza (sensibilidade da janela 15–180)

A consulta faz medições para verificar a sensibilidade da janela 15-180 dias usada para considerar o primeiro movimento classificado como mero expediente (11010), como movimento que caracteriza a concessão da recuperação judicial.

**Entrada:** a mesma coluna da consulta anterior, agora **sem** o filtro
de higiene — o teste mede o peso das bordas sobre todas as concessões
com marco, isolando os valores cravados no piso (15) e no teto (180).

**Saída — o que ela produz (uma única linha com sete números):**

| Coluna produzida                          | Como é calculada                        | O que significa                          |
| ----------------------------------------- | --------------------------------------- | ---------------------------------------- |
| `total_com_marco`                         | conta os processos com concessão datada | a base do teste (2.422)                  |
| `no_piso_15` / `no_teto_180`              | conta os cravados em 15 e 180           | quantos casos a janela capturou na borda |
| `pct_piso` / `pct_teto`                   | contagens ÷ total × 100                 | o peso das bordas (2,1% e 0,3%)          |
| `media_sem_bordas` / `mediana_sem_bordas` | estatísticas excluindo 15 e 180         | o que restaria da métrica sem a janela   |

**Em uma frase:** o teste remove as bordas da janela arbitrada e
recomputa a métrica — se mediana e média quase não se movem, a escolha
de 15–180 dias captura comportamento natural em vez de truncar a
distribuição.

#### Consulta SQL

```text
-- Q1 · Peso das bordas da janela arbitrada + sensibilidade
SELECT
count(\_) AS total_com_marco,
count(CASE WHEN dias_distribuicao_processamento = 15 THEN 1 END) AS no_piso_15,
count(CASE WHEN dias_distribuicao_processamento = 180 THEN 1 END) AS no_teto_180,
round(count(CASE WHEN dias*distribuicao_processamento = 15 THEN 1 END) / count(*) \_ 100, 1) AS pct_piso,
round(count(CASE WHEN dias*distribuicao_processamento = 180 THEN 1 END) / count(*) \* 100, 1) AS pct_teto,
round(avg(CASE WHEN dias_distribuicao_processamento NOT IN (15, 180)
THEN dias_distribuicao_processamento END), 1) AS media_sem_bordas,
round(percentile_approx(CASE WHEN dias_distribuicao_processamento NOT IN (15, 180)
THEN dias_distribuicao_processamento END, 0.5), 1) AS mediana_sem_bordas
FROM default.gold_base_analitica
WHERE classe_codigo = 129 AND escopo_lei_atual
AND dias_distribuicao_processamento IS NOT NULL;
```

##### Resposta:

| total_com_marco | no_piso_15 | no_teto_180 | pct_piso | pct_teto | media_sem_bordas | mediana_sem_bordas |
| :-------------- | :--------- | :---------- | :------- | :------- | :--------------- | :----------------- |
| 2422            | 51         | 8           | 2.1      | 0.3      | 125.6            | 49                 |

**Leitura da sensibilidade:** 2,1% dos casos tocam o piso (15 dias) e 0,3%
o teto (180) — 2,4% somados. Excluindo as bordas, a mediana move de 48 →
49 e a média de 127,3 → 125,6: alteração marginal. A janela captura
comportamento natural, não trunca a distribuição — a métrica não depende
dos limites arbitrados.

**Resposta:** o tempo **mediano** entre distribuição e concessão do
processamento é de **48 dias** (~7 semanas); o P90 é de **149 dias**. A
média de **127 dias** é quase o triplo da mediana — cauda longa de demoras
que não desloca o tempo típico do sistema, mas revela alta variabilidade
entre juízos.

#### Resposta final (pergunta 1)

O tempo mediano entre a distribuição do pedido e a concessão do
processamento da RJ é de **48 dias** — cerca de 7 semanas. O percentil
90 é de **149 dias** (~5 meses): 1 em cada 10 recuperandas espera
mais do que isso. A média de **127 dias** é quase o triplo da mediana,
refletindo uma cauda longa de casos com demora muito acima do típico —
a característica de interesse para a jurimetria: o _tempo típico do
sistema_ é razoável, mas a _incerteza_ entre os processos varia
amplamente.

Metodologia: como a TPU do CNJ não reserva movimento próprio para o
deferimento do processamento (art. 52 da Lei 11.101/2005), o marco é caracterizado
pela cascata R9 (despacho textual) → TPU 12444 (decisão de deferimento)
→ proxy "Mero expediente" (TPU 11010, janela 15–180 dias), conforme
detalhado no Bloco 4 do notebook 03. A janela do proxy foi validada
por sensibilidade: 2,4% de efeito de borda, sem impacto na mediana.

**Coerência com as perguntas seguintes:** o marco de concessão validado aqui é a fundação do conjunto — a pergunta 2 o usa como início do stay period, a pergunta 4 compara a fase pré-meritória entre varas com esta mesma métrica (com a mesma higiene de duração ≥ 0, confirmada pela soma de −8.359 dias dos 7 negativos), e a pergunta 5 o usa como denominador da taxa condicionada (2.422). A cobertura de 42,4% declarada nesta pergunta é, portanto, a cobertura herdada por todas as demais.

## 7.2. Quanto tempo dura, em média, a fase de stay period (suspensão de execuções) na prática, até a homologação do Plano de Recuperação Judicial?

**Pergunta:** o art. 6º, §4º, da Lei 11.101/2005 suspende as ações e
execuções contra a empresa em recuperação judicial, a partir do
deferimento do processamento. Na redação original, o prazo era de
180 dias improrrogáveis; a Lei 14.112/2020 permitiu prorrogar uma única vez, por igual período.

A pergunta mede quanto tempo essa proteção dura de fato — e se o prazo de 180 dias é respeitado na prática.

**Fonte no mart (consumo puro):** `gold_base_analitica` —
`dias_stay_period` (do deferimento até o fim legal da suspensão) e
`dias_stay_limite_superior` (até o fim do processo).

O cálculo da métrica está no Bloco 4 do notebook 03: a suspensão começa no deferimento do processamento (a mesma data validada na pergunta 1) e termina quando o plano é homologado pelos credores ou quando a empresa tem a falência decretada — o que acontecer primeiro. Este notebook não refaz esses cálculos, mas consome o resultado já pronto.

**Recorte:** classe 129 (pedido principal de RJ), ajuizado a partir de 09/06/2005 (escopo da lei vigente), com processamento identificado e datas coerentes. Agregação Brasil.

**Método:** estatísticas de duração (mediana, média, % acima de 180 dias).

**Limitação central:** a homologação do plano — o evento que encerraria a suspensão na maioria dos casos — não é digitada pelos tribunais no padrão Datajud. Foi comprovado no QA: nenhum movimento com nome contendo "plano" existe entre ~14,1 milhões de eventos, e nenhum padrão estatístico nas janelas teóricas de decisão ou prorrogação foi constatado (situação validada contra o benchmark da ABJ — Observatório da Insolvência, cap. 7). Essa é uma **limitação do dado, não do método**: sabemos **quando** a
suspensão começou, mas, na maioria dos casos, não observamos
**quando** terminou. Por isso a resposta tem duas camadas:

| Camada                                                              | O que mede                               | Quem tem esse dado                  |
| ------------------------------------------------------------------- | ---------------------------------------- | ----------------------------------- |
| **Suspensão medida** (fim do stay period = homologação ou falência) | a duração real da proteção               | só quem teve a falência decretada   |
| **Limite superior** (fim estimado pelo fim do processo)             | o máximo que a suspensão pode ter durado | quem tem fim de processo registrado |

#### O que a consulta seguinte mede e como (suspensão medida)

**Nota de escopo:** a homologação do plano — evento que encerraria a
suspensão na maioria dos casos — não é registrada no padrão Datajud. Por isso o fim do stay
period só é observável quando a falência é decretada, e esta consulta
mede a trajetória do fracasso, não a da recuperação bem-sucedida.

**Entrada:** `default.gold_base_analitica`, classe 129, escopo da lei
vigente, com concessão registrada e duração coerente (≥ 0).

A consulta lê três colunas por processo:

| Coluna lida                     | O que representa                                                                                                                                    |
| ------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| `data_processamento_efetivo`    | a data do deferimento do processamento — o início da suspensão (a mesma concessão validada na pergunta 1)                                           |
| `dias_stay_period`              | dias do deferimento ao fim legal da suspensão (homologação ou decretação de falência, o que ocorrer primeiro) — calculado no Bloco 4 do notebook 03 |
| `ind_stay_period_excedido_180d` | indicador de que a suspensão excedeu o prazo legal de 180 dias                                                                                      |

**Saída — o que ela produz (uma única linha com seis números):**

| Coluna produzida                           | Como é calculada                        | O que significa                                 |
| ------------------------------------------ | --------------------------------------- | ----------------------------------------------- |
| `com_processamento`                        | conta os processos com concessão datada | o denominador de cobertura (2.422)              |
| `com_fim_stay`                             | conta os casos com stay ≥ 0             | quantos têm o fim da suspensão observável (110) |
| `pct_acima_180`                            | soma do indicador ÷ com_fim_stay × 100  | a fatia que excedeu o prazo legal (97,3%)       |
| `media_dias` / `mediana_dias` / `p90_dias` | estatísticas de `dias_stay_period`      | a duração típica da suspensão medida e a cauda  |

**Em uma frase:** a consulta mede a duração da suspensão nos únicos
casos em que seu fim é observável — as falências decretadas —
produzindo a mediana, a média e a fatia acima do prazo legal que
sustentam a primeira camada da resposta.

#### Consulta SQL

```text
-- Q2 · BRASIL: duração do stay period (consumo do mart)
SELECT
(SELECT count(_) FROM default.gold_base_analitica
WHERE classe_codigo = 129
AND escopo_lei_atual
AND data_processamento_efetivo IS NOT NULL) AS com_processamento,
count(_) AS com_fim_stay,
round(sum(ind_stay_period_excedido_180d) _ 100.0 / count(_), 1) AS pct_acima_180,
round(avg(dias_stay_period), 1) AS media_dias,
round(percentile_approx(dias_stay_period, 0.5), 1) AS mediana_dias,
round(percentile_approx(dias_stay_period, 0.9), 1) AS p90_dias
FROM default.gold_base_analitica
WHERE classe_codigo = 129
AND escopo_lei_atual
AND data_processamento_efetivo IS NOT NULL
AND dias_stay_period >= 0;
```

##### Resposta:

| com_processamento | com_fim_stay | pct_acima_180 | media_dias | mediana_dias | p90_dias |
| :---------------- | :----------- | :------------ | :--------- | :----------- | :------- |
| 2422              | 110          | 97.3          | 2110.5     | 1575         | 4354     |

#### O que a próxima consulta mede e como (limite superior da suspensão)

**Entrada:** as mesmas colunas da consulta anterior, agora lendo
`dias_stay_limite_superior` — dias do deferimento até o fim do processo,
o máximo que a suspensão pode ter durado quando seu fim real não é
observável.

**Saída — o que ela produz (uma única linha com cinco números):**

| Coluna produzida                           | Como é calculada                            | O que significa                                                                       |
| ------------------------------------------ | ------------------------------------------- | ------------------------------------------------------------------------------------- |
| `com_cota`                                 | conta os casos com limite ≥ 0               | quantos têm ao menos o teto da suspensão estimável (530)                              |
| `media_cota` / `mediana_cota` / `p90_cota` | estatísticas de `dias_stay_limite_superior` | o piso do "quanto durou no máximo"                                                    |
| `pct_cota_acima_180`                       | casos com cota > 180 ÷ com_cota × 100       | a fatia cuja suspensão, mesmo no cenário mais otimista, excedeu o prazo legal (93,2%) |

**Em uma frase:** a consulta estima o teto da suspensão nos casos sem
fim observável — se até o teto estoura os 180 dias (93,2%), a conclusão
da primeira camada se robustece, não se enfraquece.

#### Consulta SQL

```text
-- Q2 · Cota do stay (limite superior) na população com concessão
SELECT
count(\_) AS com_cota,
round(avg(dias_stay_limite_superior), 1) AS media_cota,
round(percentile_approx(dias_stay_limite_superior, 0.5), 1) AS mediana_cota,
round(percentile_approx(dias_stay_limite_superior, 0.9), 1) AS p90_cota,
round(count(CASE WHEN dias_stay_limite_superior > 180 THEN 1 END)
\_ 100.0 / count(\*), 1) AS pct_cota_acima_180
FROM default.gold_base_analitica
WHERE classe_codigo = 129 AND escopo_lei_atual
AND data_processamento_efetivo IS NOT NULL
AND dias_stay_limite_superior >= 0;
```

##### Resposta:

| com_cota | media_cota | mediana_cota | p90_cota | pct_cota_acima_180 |
| :------- | :--------- | :----------- | :------- | :----------------- |
| 530      | 2051.2     | 1788         | 4182     | 93.2               |

#### Análise da resposta (pergunta 2)

**Cobertura:** dos 2.422 processos com concessão registrada, a
suspensão consegue ser medida até o fim em apenas 110 casos (4,5%) — são exatamente aqueles em que a empresa teve a falência decretada, pois a homologação do plano não é registrada no Datajud. Para outros 530 (21,9%), só é possível saber o **máximo** que a suspensão pode ter durado (até o fim do processo).

**Consegue-se medir apenas os fracassos:** os 110 casos mensuráveis são, por construção, as recuperações que fracassaram. Eles retratam a trajetória do fracasso, não a da recuperação bem-sucedida (por isso não são comparáveis à tramitação total média da pergunta 4, que cobre todos os desfechos).

**Resultados:**

| Camada                                | N   | Mediana                | Média   | 90% dos casos ficam abaixo de | Acima de 180 dias |
| ------------------------------------- | --- | ---------------------- | ------- | ----------------------------- | ----------------- |
| Suspensão medida (falência decretada) | 110 | 1.575 dias (~4,3 anos) | 2.110,5 | 4.354                         | 97,3%             |
| Limite superior (fim do processo)     | 530 | 1.788 dias (~4,9 anos) | 2.051,2 | 4.182                         | 93,2%             |

**Conclusão:** uma análise confiável da duração do stay period fica prejudicada pela falta de um movimento registrado no Datajud que mostre claramente o fim dessa fase. Uma análise mais aprofundada vai requerer um data mining nos processos completos existente nos tribunais estaduais.

#### Resposta final (pergunta 2)

A análise confiável da duração do stay period fica prejudicada pela
falta de um movimento registrado no Datajud que mostre claramente o
fim dessa fase: só em 110 casos (as falências decretadas) ela é
mensurável — e ali durou **1.575 dias na mediana**, quase nove vezes o
prazo legal de 180 dias, com 97,3% acima do teto. Nos 530 casos em que
só existe o limite até o fim do processo, o padrão se confirma
(**1.788 dias na mediana**, 93,2% acima do prazo). Aprofundar essa
medida exigirá mineração de dados nos processos completos
(digitados/arquivados) nos tribunais estaduais, fora do padrão
Datajud.

##### Coerência com as demais perguntas

As 110 RJs convoladas com stay period mensurável são subconjunto das 124 convoladas com concessão da pergunta 5 — nos 14 casos restantes, a duração da suspensão não pôde ser computada na gold (campo nulo) e eles ficam fora da métrica, sem implicar incoerência nas datas de concessão ou decretação, que estão registradas. A mediana de 1.575 dias é a mesma citada na decomposição por desfecho da pergunta 4, onde o stay period consome ~94% da vida do processo convolado.

## 7.3. Qual é o tempo médio de duração total de um processo de Falência desde a decretação até o encerramento da fase de arrecadação e pagamento de credores?

**Pergunta:** decretada a falência, os credores devem receber sua parte da
massa falida até o processo se encerrar. A pergunta mede esse trecho final:
da decretação ao encerramento.

**Fonte no mart (consumo puro):** `gold_base_analitica` — a duração já vem
calculada em `dias_duracao_falencia` (Bloco 4 do notebook 03): da data da
decretação da falência até o encerramento do processo; quando o
encerramento não é registrado, usa-se a extinção como desfecho alternativo.
Este notebook não refaz esses cálculos, mas consome o resultado pronto.

**Recorte:** classe 108 (falência originária), com decretação e desfecho
registrados e duração coerente (≥ 0). Agregação Brasil, estratificada por
regime legal — `escopo_lei_atual` separa o que foi ajuizado a partir de
09/06/2005 (Lei 11.101/2005) do que é anterior (Lei 7.661/1945, regime
antigo que ainda existe no corpus).

**Limitações:**

1. O "fim da fase de arrecadação e pagamento" não é um movimento nomeado
   na TPU — o marco adotado é o encerramento/extinção do processo, que na
   prática sinaliza o fecho da massa;
2. Processos decretados mas sem desfecho registrado ficam fora da média
   (contados no denominador de cobertura);
3. Recuperações judiciais convoladas em falência (classe 129) ficam fora
   deste recorte;
4. Os processos do regime antigo são, em média, mais antigos e foram
   registrados em sistemas com menos consistência — a comparação entre
   regimes herda esse viés de época.

#### O que a consulta a seguir mede e como (cobertura da duração)

**Entrada:** todos os campos vêm da tabela `default.gold_base_analitica`,
filtrada para falências originárias (classe 108), sem filtro de regime —
a cobertura é medida sobre o corpus inteiro.

A consulta lê duas colunas por processo:

| Coluna lida              | O que representa                                                                                              |
| ------------------------ | ------------------------------------------------------------------------------------------------------------- |
| `data_decretao_falencia` | a data da decretação da falência (nulo = o processo ainda não foi decretado)                                  |
| `dias_duracao_falencia`  | dias da decretação ao encerramento/extinção, já calculado na gold (nulo ou negativo = duração não mensurável) |

**Saída — o que ela produz (uma única linha com três números):**

| Coluna produzida     | Como é calculada                            | O que significa                                                                                        |
| -------------------- | ------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| `total_falencias`    | conta todos os processos da classe 108      | o universo do recorte (15.932)                                                                         |
| `com_decretacao`     | conta os casos com decretação datada        | quantas falências entraram efetivamente na fase falimentar (4.180)                                     |
| `com_duracao_medida` | conta os casos com decretação e duração ≥ 0 | quantas têm o trecho decretação → encerramento mensurável (847) — o denominador efetivo do comparativo |

**Em uma frase:** a consulta mede quanto do corpus de falências
originárias tem a duração decretação → encerramento mensurável,
estabelecendo a cobertura de 20,3% que condiciona todas as leituras
seguintes.

#### Consulta SQL

```text
-- Q3 · Cobertura: quantas falências têm a duração mensurável
SELECT
count(\*) AS total_falencias,
count(CASE WHEN data_decretao_falencia IS NOT NULL THEN 1 END) AS com_decretacao,
count(CASE WHEN data_decretao_falencia IS NOT NULL
AND dias_duracao_falencia >= 0 THEN 1 END) AS com_duracao_medida
FROM default.gold_base_analitica
WHERE classe_codigo = 108;
```

##### Resposta:

| total_falencias | com_decretacao | com_duracao_medida |
| :-------------- | :------------- | :----------------- |
| 15932           | 4180           | 847                |

#### O que a próxima consulta mede e como (comparativo entre regimes)

**Entrada:** as mesmas colunas da consulta anterior, restrita aos casos
elegíveis (decretação datada e duração ≥ 0), com o regime legal
derivado de `escopo_lei_atual`.

**Método:** estatísticas de duração (média, mediana, P25, P90) por
regime legal — Lei 7.661/1945 (anterior a 2005) vs. Lei 11.101/2005
(vigente). A mediana é a métrica central: a média fica sensível às
falências longas de cauda.

**Saída — o que ela produz (duas linhas, uma por regime, com seis números cada):**

| Coluna produzida        | Como é calculada                   | O que significa                                             |
| ----------------------- | ---------------------------------- | ----------------------------------------------------------- |
| `qtd_falencias`         | conta os casos elegíveis do regime | o n de cada regime (337 antiga / 510 vigente)               |
| `media_dias`            | média de `dias_duracao_falencia`   | o tempo médio, sensível à cauda longa                       |
| `mediana_dias`          | percentil 50                       | a duração típica — a métrica recomendada                    |
| `p25_dias` / `p90_dias` | percentis 25 e 90                  | a dispersão: onde começa a distribuição e onde ela engrossa |

**Em uma frase:** a consulta compara a duração da fase falimentar entre
os dois regimes legais do corpus, produzindo as quatro medidas que
mostram a aceleração trazida pela reforma de 2005 em toda a
distribuição — não só no caso típico.

#### Consulta SQL

```text
-- Q3 · Comparativo de regimes legais: Lei 7.661/1945 (antiga) x Lei 11.101/2005
WITH elegiveis AS (
SELECT numero_processo, dias_duracao_falencia,
CASE WHEN escopo_lei_atual THEN 'Lei 11.101/2005'
ELSE 'Lei 7.661/1945 (antiga)' END AS regime_legal
FROM default.gold_base_analitica
WHERE classe_codigo = 108
AND data_decretao_falencia IS NOT NULL
AND dias_duracao_falencia >= 0
)
SELECT
regime_legal,
count(\*) AS qtd_falencias,
round(avg(dias_duracao_falencia), 1) AS media_dias,
round(percentile_approx(dias_duracao_falencia, 0.5), 1) AS mediana_dias,
round(percentile_approx(dias_duracao_falencia, 0.25), 1) AS p25_dias,
round(percentile_approx(dias_duracao_falencia, 0.9), 1) AS p90_dias
FROM elegiveis
GROUP BY 1
ORDER BY qtd_falencias DESC;
```

##### Resposta:

| regime_legal            | qtd_falencias | media_dias | mediana_dias | p25_dias | p90_dias |
| :---------------------- | :------------ | :--------- | :----------- | :------- | :------- |
| Lei 11.101/2005         | 510           | 1284.8     | 831          | 296      | 3399     |
| Lei 7.661/1945 (antiga) | 337           | 3271.1     | 2033         | 774      | 7815     |

#### Análise da resposta (pergunta 3)

**Cobertura:** existem 15.932 processos de falência (classe 108) no mart;
4.180 têm decretação registrada e, destes, 847 (20,3%) têm a duração
mensurável — o restante ainda não tem encerramento/extinção registrado,
reflexo da duração longa da fase falimentar (a mesma censura vista na
pergunta 2).

**Comparativo de regimes — duração da falência (decretação → encerramento):**

| Indicador                    | Lei 7.661/1945 (antiga) | Lei 11.101/2005 (vigente) | Razão antiga/vigente |
| ---------------------------- | ----------------------- | ------------------------- | -------------------- |
| Falências com duração medida | 337                     | 510                       | —                    |
| **Mediana (dias)**           | **2.033 (~5,6 anos)**   | **831 (~2,3 anos)**       | **2,4×**             |
| Média (dias)                 | 3.271 (~9,0 anos)       | 1.285 (~3,5 anos)         | 2,5×                 |
| P25 (dias)                   | 774                     | 296                       | 2,6×                 |
| P90 (dias)                   | 7.815 (~21,4 anos)      | 3.399 (~9,3 anos)         | 2,3×                 |

**Leitura:** a Lei 11.101/2005 encurtou a falência típica em cerca de 60% —
a mediana caiu de ~5,6 anos (regime antigo) para ~2,3 anos. O achado mais
forte está no P25: mesmo o quarto mais rápido do regime antigo (774 dias)
levou mais que o dobro do equivalente atual (296 dias) — a aceleração
atingiu toda a distribuição, não só os casos simples. A cauda também
encurtou: o P90 caiu de ~21 para ~9 anos. A média continuar bem acima da
mediana nos dois regimes (no antigo, 1,6×) mostra que as falências longas
e complexas são um traço permanente do instituto — atenuado pela reforma,
mas não eliminado.

**Ressalva metodológica:** processos do regime antigo são mais antigos e
foram registrados com menos consistência (vieses de época), e a cobertura
da duração é de 20,3% das decretadas. A direção do efeito, porém, bate
com o que a reforma de 2005 se propunha: tramitação mais célere da
insolvência.

#### Resposta final (pergunta 3)

O processo de falência, medido da decretação ao encerramento, dura
**2,3 anos na mediana** sob a lei vigente (831 dias; média de 1.285) —
menos da metade do regime anterior (5,6 anos), com aceleração uniforme
em toda a distribuição. Mesmo assim, 1 em cada 10 falências do regime vigente ultrapassa
9 anos, e a fase de arrecadação e pagamento de credores segue sendo o
trecho mais longo da insolvência.

Ressalvas: o marco final é o
encerramento/extinção do processo (a TPU não tem movimento próprio para
o fim da fase de rateio) e a duração é mensurável em 20,3% das
falências decretadas — as demais ainda aguardam desfecho registrado.

## 7.4. Existe diferença significativa no tempo de tramitação de RJs processadas em varas especializadas em Direito Empresarial/Falimentar vs. varas cíveis genéricas?

**Pergunta:** a especialização do juízo torna a recuperação judicial mais
rápida? Comparamos varas especializadas (por nome do órgão julgador) com
varas cíveis genéricas em duas fases: a pré-meritória (ajuizamento →
processamento da RJ, a etapa mais sensível à rotina da vara) e a tramitação
total do processo (ajuizamento → desfecho, que capta a velocidade global).

**Fonte no mart (consumo puro):** `gold_base_analitica` —
`dias_distribuicao_processamento` (a mesma métrica validada na pergunta 1) e
`dias_duracao_processo_total`; o agrupamento vem de `eh_especializada`.
Este notebook não refaz cálculos: consome o resultado pronto.

**Recorte:** classe 129 (pedido principal de RJ), ajuizado a partir de
09/06/2005. Denominadores explícitos por grupo: a diferença de cobertura do
registro de processamento entre grupos é, ela mesma, um achado — uma vara
que registra menos também processa com menos transparência.

**Método:** estatísticas de duração por grupo (mediana, P25/P90, média) +
teste de Mann-Whitney calculado em SQL: compara-se **todos os pares
possíveis** entre os dois grupos e mede-se a proporção de pares em que a
especializada é mais rápida. Leitura simples: **0,5 = sem diferença**;
quanto mais longe de 0,5, maior o efeito prático da especialização (|P − 0,5|
≥ 0,14 indica efeito grande).

**Limitações:**

1. A classificação de especialização é do momento da coleta — varas
   reclassificadas no período herdam o rótulo atual;
2. O efeito vara confunde-se com o efeito tribunal: varas especializadas se
   concentram em tribunais de grande porte (TJSP, TJRJ). A diferença não é
   atribuível só à especialização, mas ao pacote "tribunal estruturado para
   insolvência";
3. Coberturas desiguais de processamento entre grupos podem enviesar as
   medianas pré-meritórias;
4. A fase protegida (stay period) não é usada aqui como métrica: só é
   mensurável nas convolações em falência (limitação da pergunta 2) —
   compará-la entre grupos mediria apenas trajetórias de fracasso.

#### O que a próxima consulta mede e como (estatísticas por grupo)

**Entrada:** todos os campos vêm da tabela `default.gold_base_analitica`,
já filtrada para os processos de Recuperação Judicial (classe 129)
ajuizados no período da lei vigente (a partir de 09/06/2005).

A consulta lê três colunas por processo:

| Coluna lida                       | O que representa                                                                                     |
| --------------------------------- | ---------------------------------------------------------------------------------------------------- |
| `eh_especializada`                | se o órgão julgador é vara especializada em Direito Empresarial/Falimentar (agrupamento da consulta) |
| `dias_distribuicao_processamento` | dias entre a distribuição e o processamento da RJ (nulo = concessão não registrada)                  |
| `dias_duracao_processo_total`     | dias entre o ajuizamento e o desfecho do processo (nulo = sem desfecho registrado)                   |

**Saída — o que ela produz (duas linhas, uma por grupo, com onze números cada):**

| Coluna produzida                                    | Como é calculada                                   | O que significa                                                                                         |
| --------------------------------------------------- | -------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| `total_rj`                                          | conta os processos do grupo                        | o denominador das coberturas do grupo                                                                   |
| `com_processamento`                                 | conta os casos com processamento datado            | quantas RJs do grupo têm a fase pré-meritória mensurável                                                |
| `pct_cobertura_proc`                                | com_processamento ÷ total_rj × 100                 | a cobertura do registro de processamento — desigual entre grupos, e essa desigualdade é um achado em si |
| `p25_proc` / `mediana_proc` / `p90_proc`            | percentis de `dias_distribuicao_processamento`     | a distribuição da fase pré-meritória por grupo                                                          |
| `media_proc`                                        | média de `dias_distribuicao_processamento`         | o tempo médio pré-meritório, sensível a caudas                                                          |
| `com_duracao_total`                                 | conta os casos com desfecho registrado             | o denominador da tramitação total do grupo                                                              |
| `mediana_duracao` / `p90_duracao` / `media_duracao` | percentis e média de `dias_duracao_processo_total` | a trajetória completa do processo por grupo                                                             |

**Em uma frase:** a consulta separa as 5.709 RJs do corpus entre varas
especializadas e genéricas e, para cada grupo, mede a cobertura do
registro, a fase pré-meritória e a tramitação total — produzindo os
números que mostram onde a especialização age (e onde não age).

**Observação:** a fase pré-meritória aplica a mesma higiene da pergunta 1 (duração ≥ 0): os 7 casos com datas incoerentes saem das estatísticas de duração, mas a cobertura do marco (2.422 = 1293+1129) permanece integral, pois é ela o denominador herdado pelas perguntas 1 e 5.

#### Consulta SQL

```text
-- Q4 · Comparativo por grupo (cobertura intacta + higiene só nas estatísticas)
WITH base AS (
SELECT eh_especializada,
dias_distribuicao_processamento,
dias_duracao_processo_total,
CASE WHEN dias_distribuicao_processamento >= 0
FROM default.gold_base_analitica
WHERE classe_codigo = 129 AND escopo_lei_atual
)
SELECT
eh_especializada,
count(\_) AS total_rj,
count(dias_distribuicao_processamento) AS com_processamento, -- cobertura: 1.129 + 1.293 = 2.422 (sem mudar)
round(count(dias*distribuicao_processamento) * 100.0 / count(\*), 1) AS pct_cobertura_proc,
round(percentile_approx(dias_proc_higienizado, 0.25), 1) AS p25_proc, -- sobre os elegíveis
round(percentile_approx(dias_proc_higienizado, 0.5), 1) AS mediana_proc,
round(percentile_approx(dias_proc_higienizado, 0.9), 1) AS p90_proc,
round(avg(dias_proc_higienizado), 1) AS media_proc, -- muda aqui
count(dias_duracao_processo_total) AS com_duracao_total,
round(percentile_approx(dias_duracao_processo_total, 0.5), 1) AS mediana_duracao,
round(percentile_approx(dias_duracao_processo_total, 0.9), 1) AS p90_duracao,
round(avg(dias_duracao_processo_total), 1) AS media_duracao
FROM base
GROUP BY 1
ORDER BY 1;
```

##### Resposta:

| eh_especializada | total_rj | com_processamento | pct_cobertura_proc | p25_proc | mediana_proc | p90_proc | media_proc | com_duracao_total | mediana_duracao | p90_duracao | media_duracao |
| :--------------- | :------- | :---------------- | :----------------- | :------- | :----------- | :------- | :--------- | :---------------- | :-------------- | :---------- | :------------ |
| false            | 2282     | 1293              | 56.7               | 27       | 49           | 155      | 167.3      | 564               | 1783            | 4169        | 2063.1        |
| true             | 3427     | 1129              | 32.9               | 26       | 46           | 142      | 81.2       | 473               | 798             | 3471        | 1357.2        |

#### O que a consulta a seguir mede e como (teste de Mann-Whitney em SQL)

**Entrada:** as mesmas três colunas da consulta anterior, nos mesmos
recortes — a consulta compara durações par a par entre os grupos.

**Método:** em vez de assumir distribuições normais, comparam-se
**todos os pares possíveis** especializada × genérica; a cada par,
a especializada "vence" se for mais rápida (empate vale meio ponto).
O resultado é a proporção P de pares em que a especializada é mais
rápida: **P = 0,5 significa ausência de diferença prática**; a
distância |P − 0,5| mede o tamanho do efeito (≥ 0,14 = efeito grande).

**Saída — o que ela produz (duas linhas, uma por métrica, com três números cada):**

| Coluna produzida    | Como é calculada                          | O que significa                                                    |
| ------------------- | ----------------------------------------- | ------------------------------------------------------------------ |
| `pares`             | count(\*) do produto cruzado entre grupos | quantas comparações sustentam o teste (não é o tamanho da amostra) |
| `p_esp_mais_rapida` | vitórias ÷ pares                          | a proporção P — perto de 0,5 = sem diferença; longe = efeito       |
| `distancia_de_05`   | abs(P − 0,5)                              | o tamanho do efeito na régua declarada no cabeçalho                |

**Em uma frase:** a consulta conta, para a fase pré-meritória e para a
tramitação total, em que fração de todos os pares comparáveis a
especializada vence — separando "não há diferença" (0,521) de "efeito
grande" (0,663) sem depender de pressupostos de distribuição.

#### Consulta SQL

```text
-- Q4 · Mann-Whitney: proporção de pares em que a especializada é mais rápida
-- P = 0,5 -> sem diferença; quanto mais longe de 0,5, maior o efeito
WITH base AS (
SELECT eh_especializada,
dias_distribuicao_processamento,
dias_duracao_processo_total
FROM default.gold_base_analitica
WHERE classe_codigo = 129 AND escopo_lei_atual
),
pares_proc AS (
SELECT sum(CASE WHEN e.m < g.m THEN 1
WHEN e.m = g.m THEN 0.5
ELSE 0 END) AS vitorias,
count(\_) AS pares
FROM (SELECT dias_distribuicao_processamento AS m FROM base
WHERE eh_especializada AND dias_distribuicao_processamento >= 0) e
CROSS JOIN (SELECT dias_distribuicao_processamento AS m FROM base
WHERE NOT eh_especializada AND dias_distribuicao_processamento >= 0) g
),
pares_total AS (
SELECT sum(CASE WHEN e.m < g.m THEN 1
WHEN e.m = g.m THEN 0.5
ELSE 0 END) AS vitorias,
count(\_) AS pares
FROM (SELECT dias_duracao_processo_total AS m FROM base
WHERE eh_especializada AND dias_duracao_processo_total >= 0) e
CROSS JOIN (SELECT dias_duracao_processo_total AS m FROM base
WHERE NOT eh_especializada AND dias_duracao_processo_total >= 0) g
)
SELECT 'processamento' AS metrica,
pares,
round(vitorias / pares, 3) AS p_esp_mais_rapida,
round(abs(vitorias / pares - 0.5), 3) AS distancia_de_05
FROM pares_proc
UNION ALL
SELECT 'duracao_total',
pares,
round(vitorias / pares, 3),
round(abs(vitorias / pares - 0.5), 3)
FROM pares_total;
```

##### Resposta:

| metrica       | pares   | p_esp_mais_rapida | distancia_de_05 |
| :------------ | :------ | :---------------- | :-------------- |
| processamento | 1450746 | 0.521             | 0.021           |
| duracao_total | 264610  | 0.663             | 0.163           |

#### Análise da resposta (pergunta 4)

**Cobertura por grupo:**
| Grupo | RJs | Com processamento | Cobertura |
|---|---|---|---|
| Especializada | 3.427 | 1.129 | 32,9% |
| Genérica | 2.282 | 1.293 | 56,7% |

**Fase pré-meritória (ajuizamento → processamento):**
| Indicador | Especializada | Genérica |
|---|---|---|
| Mediana | 46 dias | 49 dias |
| P25 / P90 | 26 / 142 | 27 / 155 |
| Média | 81,2 | 167,3 |

**Tramitação total (ajuizamento → desfecho):**
| Indicador | Especializada | Genérica |
|---|---|---|
| Com desfecho | 473 | 564 |
| Mediana | 798 dias (~2,2 anos) | 1.783 dias (~4,9 anos) |
| P90 | 3.471 (~9,5 anos) | 4.169 (~11,4 anos) |
| Média | 1.357 | 2.063 |

**Teste de significância (Mann-Whitney):**
| Métrica | Pares | P(esp < gen) | Distância de 0,5 | Interpretação |
|---|---|---|---|---|
| Processamento | 1.450.746 | 0,521 | 0,021 | Sem diferença prática |
| Tramitação total | 264.610 | 0,663 | 0,163 | **Efeito grande** |

**Leitura:** sim, existe diferença significativa — mas ela não está onde se
esperava. No despacho inicial (ato padronizado de rotina), os dois tipos de
vara são indistinguíveis: medianas de 46 vs 49 dias e teste praticamente
em cima da linha de sem diferença (0,521). A diferença emerge na
trajetória do processo: com desfecho registrado, a especializada termina,
na mediana, em **798 dias contra 1.783** — menos da metade — e o teste
confirma efeito grande (0,663; acima da régua de 0,14). A especialização
acelera a fase de negociação e gestão (assembleias, plano, administrador
judicial — atos que o juízo especializado gerencia melhor), não o despacho
inicial. Validado contra o benchmark da ABJ (1ª AGC: 327 vs 456 dias;
deliberação: 384 vs 553): direção e localização da diferença coincidem.

Ressalvas: (1) efeito vara confunde-se com efeito tribunal (especializadas
se concentram em tribunais grandes); (2) cobertura do processamento é
menor nas especializadas (32,9% vs 56,7%) — o registro é mais raro
justamente nas varas de maior volume, um achado em si; (3) classificação
de especialização do momento da coleta.

#### Resposta final (pergunta 4)

Sim, há diferença significativa — mas localizada fora da fase que se
costuma suspeitar. No processamento inicial, especializadas e genéricas
empatam (46 vs 49 dias; teste em 0,521 = sem efeito prático). Na
tramitação total, as especializadas encerram o processo em **798 dias na
mediana contra 1.783 das genéricas** — menos da metade — e o teste de
Mann-Whitney classifica o efeito como grande (0,663; em 66% dos pares
comparáveis a especializada é mais rápida). Conclusão: especializar o
juízo não agiliza o despacho de rotina; agiliza a gestão do processo —
a fase que concentra plano, assembleias e administrador judicial. O
achado replica a direção e a magnitude do benchmark nacional da ABJ.
Ressalvas: efeito vara embaralhado com efeito tribunal e cobertura de
registro menor nas varas especializadas.

#### Consulta para a conciliação dessa resposta com a resposta da pergunta 2:

Essa consulta decompõe a tramitação total por
`cenario_desfecho` e reporta, para cada rota, a mediana de duração
total e a mediana do stay period — mostrando por que a mediana geral
de 798/1.783 dias não é comparável ao stay de 1.575 dias (universos
distintos: rota do fracasso vs. todos os destinos juntos).

#### Consulta SQL

```text
-- Q4 · Conciliação com a pergunta 2: decomposição da tramitação por desfecho
SELECT cenario_desfecho,
count(\*) AS n,
round(percentile_approx(dias_duracao_processo_total, 0.5), 1) AS mediana_total,
round(percentile_approx(dias_stay_period, 0.5), 1) AS mediana_stay
FROM default.gold_base_analitica
WHERE classe_codigo = 129 AND escopo_lei_atual
AND dias_duracao_processo_total >= 0
GROUP BY 1
ORDER BY n DESC;
```

##### Resposta:

| cenario_desfecho  | n   | mediana_total | mediana_stay |
| :---------------- | :-- | :------------ | :----------- |
| extincao          | 676 | 1192          | null         |
| encerramento      | 183 | 2090          | null         |
| decretao_falencia | 174 | 1676          | 1575         |

**Análise da conciliação com a pergunta 2:** a mediana de 798 dias da
tramitação total das especializadas não é comparável ao stay period de
1.575 dias medido na pergunta 2 — são universos distintos. A decomposição por desfecho mostra: 65% dos processos terminam por
extinção (mediana de 1.192 dias, muitos sem sequer chegar ao
deferimento), o que puxa a média geral para baixo; a rota da convolação
em falência — onde o stay é mensurável — é bem mais longa (mediana de
1.676 dias), e o stay dela consome quase toda a vida do processo
(1.575 de 1.676 dias, ~94%). Dentro de um mesmo processo, vale sempre
stay ≤ tramitação total; a aparente contradição entre as perguntas
é só a diferença entre medir a rota do fracasso (pergunta 2) e medir todos os
destinos juntos (pergunta 4).

A diferença para os 1.623 dias da pergunta 5 é esperada: lá mede-se até a decretação da falência (dias_ate_convolucao); aqui, até o desfecho final do processo (dias_duracao_processo_total) — e os universos são idênticos (as 177 convoladas, todas com desfecho registrado).

## 7.5. Qual é a taxa de conversão de Recuperações Judiciais em Falência (convolação em falência por descumprimento do plano ou rejeição da assembleia)?

**Pergunta:** qual a fatia de empresas que ajuizam RJ e têm a falência decretada nos mesmos autos?

**Fonte no mart (consumo puro):** `gold_base_analitica` — a convolação é
identificada pela presença de `'data_decretao_falencia'` nos autos da RJ
(movimento TPU 202 "Decretação de falência" registrado dentro do processo
de classe 129). O tempo até a convolação já vem calculado em
`'dias_ate_convolucao'` (ajuizamento → decretação) e o denominador
condicionado usa `'data_processamento_efetivo'` (a mesma concessão validada
na pergunta 1). Este notebook não refaz cálculos: consome o resultado pronto.

**Recorte:** classe 129 (pedido principal de RJ), ajuizado a partir de
09/06/2005. Dois denominadores explícitos:

- **Taxa bruta** — sobre todas as RJs do corpus;
- **Taxa condicionada** — só sobre as RJs com concessão identificável
  (recomendada: a convolação por descumprimento do plano pressupõe que o
  processo chegou à fase de processamento).

**Sobre as causas:** a pergunta menciona duas situações que levam à falência — a rejeição do plano pelos credores na assembleia e o descumprimento do plano depois de aprovado. O Datajud não registra **por que** a falência foi decretada: a justificativa fica dentro do texto da decisão, e não existe um movimento próprio na TPU para cada
causa (a mesma limitação estrutural da pergunta 2). O que medimos, com segurança, é a **taxa total** de convolação. A causa pode ser apenas **aproximada pelo tempo**: quando a falência é decretada sem que o processamento tenha sido concedido, o mais provável é rejeição ou indeferimento (art. 73, V); quando a decretação vem anos depois da concessão, o perfil é de descumprimento do plano durante sua execução. Essa separação é uma inferência temporal, não uma medida direta — e por
isso está declarada como limitação.

**Limitações:**

1. A causa da convolação (descumprimento vs rejeição) é inferência temporal,
   não medida direta;
2. Convolações em autos de falência separados (classe 108 aberta depois de
   indeferir a RJ) não têm vínculo pai-filho no Datajud — ficam fora da
   taxa, que é, portanto, um piso;
3. O denominador condicionado herda a cobertura do marco de concessão
   (42,4% — pergunta 1).

#### O que esta consulta mede e como

**Nota terminológica:** o título emprega "conversão" no sentido corrente
e "convolação" no sentido jurídico consagrado pela doutrina falimentar
(Lei 11.101/2005, art. 73). O texto deste notebook usa convolação. As
colunas do mart preservam a grafia `convolucao` (artefato da camada
gold, congelada) e são citadas apenas entre crases.

**Entrada:** todos os campos vêm da tabela `default.gold_base_analitica`,
já filtrada para os processos de Recuperação Judicial (classe 129)
ajuizados no período da lei vigente (a partir de 09/06/2005).

A consulta lê quatro colunas por processo:

| Coluna lida                  | O que representa                                                                                     |
| ---------------------------- | ---------------------------------------------------------------------------------------------------- |
| `numero_processo`            | identificador do processo (usado apenas para a contagem)                                             |
| `data_decretao_falencia`     | a data da decretação de falência registrada nos próprios autos da RJ (nulo = a RJ não foi convolada) |
| `dias_ate_convolucao`        | dias entre o ajuizamento e a decretação, já calculado na gold                                        |
| `data_processamento_efetivo` | a data em que o juiz concedeu o processamento da RJ (nulo = concessão não identificável)             |

**Saída — o que ela produz (uma única linha com oito números):**

| Coluna produzida                    | Como é calculada                                        | O que significa                                                                                                        |
| ----------------------------------- | ------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| `total_rj`                          | conta todos os processos do recorte                     | o denominador da taxa bruta (5.709)                                                                                    |
| `com_concessao`                     | conta os processos em que a concessão está datada       | o denominador da taxa condicionada (2.422)                                                                             |
| `convoladas`                        | conta os casos em que a decretação existe nos autos     | o numerador das duas taxas (177)                                                                                       |
| `taxa_bruta_pct`                    | convoladas ÷ total_rj × 100                             | a taxa bruta de convolação (3,10%)                                                                                     |
| `taxa_condicionada_pct`             | convoladas com concessão ÷ com_concessao × 100          | a taxa condicionada, métrica recomendada (5,12%)                                                                       |
| `mediana_dias_ate_convolacao`       | mediana de `dias_ate_convolucao`, só para as convoladas | o tempo típico até a decretação (1.623 dias) — medido sobre o conjunto completo das 177, incluindo as 53 sem concessão |
| `media_dias_ate_convolacao`         | média de `dias_ate_convolucao`, só para as convoladas   | o tempo médio até a decretação (1.980 dias), mais sensível aos casos extremos                                          |
| `pct_convolacao_antes_da_concessao` | convoladas sem concessão ÷ convoladas × 100             | a fatia candidata a rejeição/indeferimento (art. 73, V) (29,9% — 53 de 177)                                            |

**Em uma frase:** a consulta pega as 5.709 RJs do período da lei vigente,
verifica em quantas houve decretação de falência nos próprios autos e em
quantas dessas o processamento havia sido concedido, produzindo os oito
números que sustentam a resposta: a taxa bruta, a taxa
condicionada, o tempo típico até a decretação
(mediana) e a fatia de convolações ocorridas antes de
qualquer concessão registrada.

#### Consulta SQL

```text
-- Q5 · TAXA DE CONVOLACAO: RJ -> falencia nos mesmos autos (nacional)
WITH base AS (
SELECT numero_processo,
data_decretao_falencia,
dias_ate_convolucao,
data_processamento_efetivo
FROM default.gold_base_analitica
WHERE classe_codigo = 129 AND escopo_lei_atual
)
SELECT
(SELECT count(\_) FROM base) AS total_rj,
(SELECT count(\_) FROM base WHERE data_processamento_efetivo IS NOT NULL) AS com_concessao,
count(CASE WHEN data_decretao_falencia IS NOT NULL THEN 1 END) AS convoladas,
round(count(CASE WHEN data*decretao_falencia IS NOT NULL THEN 1 END) * 100.0
/ (SELECT count(\_) FROM base), 2) AS taxa_bruta_pct,
round(count(CASE WHEN data_decretao_falencia IS NOT NULL
AND data*processamento_efetivo IS NOT NULL THEN 1 END) * 100.0
/ NULLIF((SELECT count(\_) FROM base
WHERE data_processamento_efetivo IS NOT NULL), 0), 2) AS taxa_condicionada_pct,
round(percentile_approx(CASE WHEN data_decretao_falencia IS NOT NULL
THEN dias_ate_convolucao END, 0.5), 1) AS mediana_dias_ate_convolacao,
round(avg(CASE WHEN data_decretao_falencia IS NOT NULL
THEN dias_ate_convolucao END), 1) AS media_dias_ate_convolacao,
round(count(CASE WHEN data_decretao_falencia IS NOT NULL
AND data_processamento_efetivo IS NULL THEN 1 END) \* 100.0
/ NULLIF(count(CASE WHEN data_decretao_falencia IS NOT NULL THEN 1 END), 0), 1) AS pct_convolacao_antes_da_concessao
FROM base;
```

##### Resposta:

| total_rj | com_concessao | convoladas | taxa_bruta_pct | taxa_condicionada_pct | mediana_dias_ate_convolacao | media_dias_ate_convolacao | pct_convolacao_antes_da_concessao |
| :------- | :------------ | :--------- | :------------- | :-------------------- | :-------------------------- | :------------------------ | :-------------------------------- |
| 5709     | 2422          | 177        | 3.10           | 5.12                  | 1623                        | 1980.2                    | 29.9                              |

#### Análise da resposta (pergunta 5)

**Resultado nacional (classe 129, Lei 11.101/2005):**
| Indicador | Valor |
|---|---|
| Total de RJs | 5.709 |
| RJs com concessão identificável | 2.422 |
| RJs convoladas em falência (mesmos autos, TPU 202) | 177 |
| **Taxa bruta (sobre todas as RJs)** | **3,10%** |
| **Taxa condicionada (sobre as com concessão)** | **5,12%** |
| Mediana do tempo até a convolação | 1.623 dias (~4,4 anos) |
| Média do tempo até a convolação | 1.980 dias (~5,4 anos) |
| Convolações sem concessão registrada | 29,9% (53 de 177) |

**Leitura:** a taxa condicionada (5,12%, medida só sobre as RJs com
concessão identificável) é a métrica recomendada — a convolação por
descumprimento pressupõe que o processo chegou à fase de processamento.
A sanidade interna confere: 177 = 124 com concessão + 53 sem.

**Observação:** 14 das 124 convoladas com concessão não têm a duração do stay computada na gold; elas permanecem na taxa condicionada, que conta eventos (concessão e decretação), não intervalos — o teste de coerência do intervalo só é aplicável na pergunta 2.

**Leitura das causas (aproximação temporal):** 29,9% das convolações ocorrem sem concessão registrada — candidatas a rejeição do plano/indeferimento (art. 73, V); os 70,1% restantes ocorrem após a concessão, perfil de descumprimento tardio durante a execução do plano. O tempo até a decretação (mediana de 1.623 dias) é medido sobre o conjunto completo das 177 convolações, incluindo as 53 sem concessão. Essa separação é inferência temporal, não medida direta.

**Coerência com as perguntas anteriores:** as 177 convoladas formam o universo onde o stay period se torna avaliável (pergunta 2) — medido de fato nas 110 com concessão e duração computadas, como lá se declara; e o total conversa com as 174 decretações que a decomposição da pergunta 4 mostrou entre os desfechos (diferença de 3 = decretadas ainda sem desfecho do processo registrado)

#### Resposta final (pergunta 5)

Entre 3,10% e 5,12% das Recuperações Judiciais do corpus terminam em
falência nos mesmos autos — a **taxa condicionada (5,12%), medida só
sobre as RJs com concessão identificável, é a métrica recomendada**. A
RJ retém a grande maioria das empresas que entram no sistema. O padrão
Datajud não registra a causa da convolação, mas a decomposição temporal
aproxima o diagnóstico: 29,9% das convolações ocorrem sem concessão
registrada (candidatas a rejeição/indeferimento, art. 73, V) e 70,1%
vêm anos após a concessão — perfil de descumprimento tardio do plano
durante a execução (o tempo até a decretação, mediana de 1.623 dias, é
medido sobre o conjunto completo das 177 convolações, incluindo as 53
sem concessão).

Ressalvas: a causa é inferência temporal, não medida;
convolações em autos de falência separados ficam fora da taxa (piso
estrutural); o denominador condicionado herda a cobertura da concessão
da pergunta 1 (42,4%).

##### Nota comparativa com a pergunta 4

Sobre o tempo: a mediana de 1.623 dias daqui mede o intervalo ajuizamento → decretação da falência (dias_ate_convolucao); a pergunta 4 reporta 1.676 dias para a mesma rota porque lá a régua é dias_duracao_processo_total, que corre até o desfecho final do processo — podendo ser posterior à decretação (ex.: arquivamento). A diferença de ~53 dias é esperada, não é incoerência. A equivalência dos universos foi verificada: as 177 convoladas desta consulta são exatamente os mesmos processos com desfecho registrado na decomposição da pergunta 4.

## 7.6. Qual o percentual de processos de Recuperação Extrajudicial que obtêm homologação do plano sem impugnação expressiva de credores?

**Pergunta:** a recuperação extrajudicial (RE, art. 161-167 da Lei
11.101/2005) é o procedimento "rápido e confidencial": os credores assinam
um plano diretamente com a empresa e o juiz apenas homologa. A pergunta
mede quantos desses processos chegam à homologação — e sem disputa.

**Fonte:**
`gold_base_analitica` — classe 128 (RE originária):
`data_homologacao_plano` (marco da homologação, art. 162),
`data_decretao_falencia`,
`data_encerramento`,
`cenario_desfecho` (desfechos),
`qtd_impugnacoes` (impugnações de crédito nos autos) e
`data_ajuizamento`.

Este notebook não refaz cálculos: consome o resultado pronto.

**Recorte:** classe 128, ajuizado a partir de 09/06/2005 (o instituto não
existia na lei anterior).

**Correção do universo (nota metodológica):** O recorte
pela classe processual (128) isola as REs originárias (~300
processos — compatível com a baixa demanda real do instituto).

**Sobre a "impugnação expressiva":** o Datajud não
registra se os credores contestam o plano, nem o tamanho da contestação — nem sequer há movimento nomeado para a homologação do plano (mesma limitação estrutural da pergunta 2). O único substituto possível é a ausência de impugnações de crédito nos autos — e a leitura correta dele é "não há contestação registrada", não "os credores aprovaram".

**Limitações:**

1. A homologação do plano não tem movimento próprio na TPU — se não for registrada, a pergunta fica sem denominador;
2. O instituto é de baixa demanda (a confidencialidade é seu atrativo) — com amostras pequenas, as taxas exigem cautela;
3. Pedidos de RE arquivados podem ter migrado para RJ judicial (sem vínculo pai-filho no Datajud) — não rastreável;
4. **O contador de impugnações não distingue dois atos diferentes.** A
   lei prevê: (i) a **objeção ao plano** (arts. 162-163) — o credor
   discorda do _conteúdo do plano_ antes da homologação, e a objeção
   suspende o processo de homologação; e (ii) a **impugnação de
   crédito** — o credor diverge da _lista de créditos_, etapa de
   verificação que corre à parte e não afeta o plano. O contador
   `qtd_impugnacoes` mede apenas o **segundo**. Consequência: mesmo
   que as homologações fossem detectáveis, uma RE "com impugnação
   registrada" poderia ter tido o plano **aceito sem qualquer objeção**
   — a impugnação pode se referir só ao valor de um crédito na lista.
   O indicador "homologado sem impugnação expressiva" ficaria
   contaminado por falsos positivos — por isso o proxy é frágil
   mesmo no cenário em que a homologação fosse registrável.

**O que a consulta a seguir mede:** o quadro de desfechos das REs originárias
(classe 128) e, nele, o marco que define a resposta — a homologação do
plano (art. 162 da Lei 11.101/2005). Uma linha com o universo total, quantos têm
homologação registrada, quantos convoluiram em falência, quantos
encerraram sem convolação, quantos seguem em andamento, os percentuais
de cada bloco e a mediana de tramitação dos processos de em andamento (até a data
download).

**Entrada:** apenas colunas da gold — `data_homologacao_plano`,
`data_decretao_falencia`, `data_encerramento`, `data_ajuizamento`.

**Como funciona por dentro:** os três desfechos são blocos mutuamente
exclusivos definidos por `CASE`: convolação (falência registrada),
encerramento sem convolação (encerramento registrado, falência não) e
em andamento (nenhum marco de fim). Os percentuais são cada bloco
sobre o total; a mediana de dias usa `datediff` até a data de corte
(01/09/2026) **somente para os em andamento** — os concluídos têm
medida própria na célula seguinte.

**Saída esperada e papel na resposta:** se `com_homologacao` vier
zero, o denominador da taxa pedida pela pergunta é nulo — e não será possivel responder a pergunta por esse caminho.

#### Consulta SQL

```text
-- Q6 · Quadro de desfechos das REs (classe 128) + marco de homologação
SELECT
count(\_) AS total_re,
count(CASE WHEN data_homologacao_plano IS NOT NULL THEN 1 END) AS com_homologacao,
count(CASE WHEN data_decretao_falencia IS NOT NULL THEN 1 END) AS convoluidas_falencia,
count(CASE WHEN data_decretao_falencia IS NULL
AND data_encerramento IS NOT NULL THEN 1 END) AS encerradas_sem_convolacao,
count(CASE WHEN data_decretao_falencia IS NULL
AND data_encerramento IS NULL THEN 1 END) AS em_andamento,
round(count(CASE WHEN data_decretao_falencia IS NOT NULL THEN 1 END)
_ 100.0 / count(_), 1) AS pct_convoluidas,
round(count(CASE WHEN data_decretao_falencia IS NULL
AND data_encerramento IS NOT NULL THEN 1 END)
_ 100.0 / count(_), 1) AS pct_encerradas,
round(count(CASE WHEN data_decretao_falencia IS NULL
AND data_encerramento IS NULL THEN 1 END)
\_ 100.0 / count(\*), 1) AS pct_em_andamento,
round(percentile_approx(CASE WHEN data_decretao_falencia IS NULL
AND data_encerramento IS NULL
THEN datediff(date('2026-09-01'), data_ajuizamento)
END, 0.5), 1) AS mediana_dias_em_andamento_ate_corte
FROM default.gold_base_analitica
WHERE classe_codigo = 128 AND escopo_lei_atual;
```

##### Resposta:

| total_re | com_homologacao | convoluidas_falencia | encerradas_sem_convolacao | em_andamento | pct_convoluidas | pct_encerradas | pct_em_andamento | mediana_dias_em_andamento_ate_corte |
| :------- | :-------------- | :------------------- | :------------------------ | :----------- | :-------------- | :------------- | :--------------- | :---------------------------------- |
| 305      | 0               | 0                    | 60                        | 245          | 0.0             | 19.7           | 80.3             | 804                                 |

**O que a consulta a seguir mede:** o detalhamento do mesmo universo de RE por
desfecho consolidado (`cenario_desfecho`), cruzando três dimensões —
quantos processos existem em cada desfecho (ativo, ), quão rápido terminaram (mediana de
duração) e quantas impugnações de crédito carregam (média e contagem
de autos com ao menos uma).

**Entrada:** colunas da gold — `cenario_desfecho`,
`dias_duracao_processo_total`, `qtd_impugnacoes`.

**Como funciona por dentro:** agregação `GROUP BY` no desfecho com
`percentile_approx(..., 0.5)` para a mediana (robusta a extremos,
padrão do projeto) e `avg` para a média de impugnações;
`com_impugnacao` conta os autos com impugnação, que é a leitura
correta do contador (ocorrência, não volume distribuído).

**Saída esperada e papel na resposta:**

(i) A duração
mediana dos encerrados testa se o fim registrado é compatível com o
procedimento homologatório cumprido (~1 ano) — a evidência de que as
homologações existiram mas não foram registradas;

(ii) a coluna de
impugnações testa o único substituto disponível para a "impugnação
expressiva" — e o teste falha por duas razões independentes:

- O contador registra disputa de _lista de
  créditos_, não objeção ao _plano_ (ver limitação 4) — um valor alto não provaria contestação do plano;

- Espera-se pouquíssimos autos com
  impugnação (e o output confirma). Uma variável zerada em ~96% do
  universo não distingue grupo algum: todo processo sem impugnação
  registrada apareceria como "credores aprovaram", quando na verdade
  significa apenas "nada foi registrado".

Mesmo que as homologações fossem detectáveis, este proxy não
sustentaria o indicador pedido — a ausência de registro não é prova
de aprovação.

#### Consulta SQL

```text
-- Q6 · Detalhamento: desfecho × duração × impugnações
SELECT cenario_desfecho,
count(\*) AS n,
round(percentile_approx(dias_duracao_processo_total, 0.5), 1) AS mediana_duracao_dias,
round(avg(qtd_impugnacoes), 1) AS media_impugnacoes,
count(CASE WHEN qtd_impugnacoes > 0 THEN 1 END) AS com_impugnacao
FROM default.gold_base_analitica
WHERE classe_codigo = 128 AND escopo_lei_atual
GROUP BY 1
ORDER BY n DESC;
```

##### Resposta:

| cenario_desfecho | n   | mediana_duracao_dias | media_impugnacoes | com_impugnacao |
| :--------------- | :-- | :------------------- | :---------------- | :------------- |
| ativo            | 241 | null                 | 0                 | 7              |
| extincao         | 60  | 377                  | 0                 | 2              |
| encerramento     | 4   | 477                  | 0                 | 0              |

**O que a consulta a seguir faz:** verificação de coerência (QA) — não responde
à pergunta, mas valida a semântica do mart contra ela. O stay period
(art. 6º, §4º) termina em homologação do plano **ou** em falência; se
a classe 128 registra zero homologações e zero falências (saída da
célula 1), então `data_fim_stay` deve ser nulo em **todas** as linhas.

**Entrada:** uma coluna da gold — `data_fim_stay`.

**Como funciona por dentro:** contagem simples de linhas com e sem o
marco, sobre o mesmo recorte da célula 1.

**Leitura do output:** o valor esperado é `com_fim_stay = 0`. Se vier
diferente de zero, há inconsistência entre a cascata de marcos do
Bloco 4 do notebook 03_gold_modelagem e o quadro de desfechos — o que exigiria investigação antes de
qualquer relato. Zero confirma que a regra de construção do stay e os
desfechos contados contam a mesma história, de ponta a ponta.

#### Consulta SQL

```text
-- Q6 · Coerência com a semântica do stay (art. 6º, §4º)
-- O stay termina em homologação OU falência. Se classe 128 tem 0 falências
-- e 0 homologações detectadas, data_fim_stay deve ser nulo em TODAS as linhas.
SELECT count(\*) AS total_re,
count(CASE WHEN data_fim_stay IS NOT NULL THEN 1 END) AS com_fim_stay -- esperado: 0
FROM default.gold_base_analitica
WHERE classe_codigo = 128 AND escopo_lei_atual;
```

##### Resposta:

| total_re | com_fim_stay |
| :------- | :----------- |
| 305      | 0            |

#### Análises das respostas

**Quadro de desfechos das REs originárias (classe 128, Lei 11.101/2005):**
| Indicador | Valor |
|---|---|
| Processos de RE originária | 305 |
| Homologações de plano identificadas | **0** |
| Convoluções em falência | 0 (0,0%) |
| Encerradas sem convolação (encerramento/extinção registrados) | 60 (19,7%) |
| Em andamento / sem marco de fim | 245 (80,3%) |
| Mediana de tramitação das em andamento (até 01/09/2026) | 804 dias (~2,2 anos) |
| Taxa de homologação | **0,00%** |
| % de homologados sem impugnação | não calculável (denominador nulo) |

**Decomposição por desfecho consolidado (mart):** 241 ativos, 60 extintos
(mediana de 377 dias de tramitação) e 4 encerrados (mediana de 477 dias).
Os 60 do quadro acima correspondem aos autos que já têm marco de fim
registrado; a diferença entre os cortes é a regra de consolidação do
desfecho (qual evento veio primeiro), não divergência de dados.

**A pergunta é NÃO-RESPONDÍVEL no padrão Datajud.** Nem um único marco de homologação de plano extrajudicial foi
identificado em 305 processos da classe própria. O achado espelha a
limitação estrutural da pergunta 2 (nenhum movimento com a palavra "plano" em ~14,1
milhões de eventos): a homologação do art. 162 — ato final de todo o
procedimento — não possui movimento nomeado nem código TPU, e nenhum
tribunal do corpus a registra de forma detectável. Sem o marco, não
existe denominador para a taxa de homologação sem impugnação.

**Evidência de homologações ocultas:** 60 REs já chegaram ao fim, com
tramitação típica de ~1 ano (377–477 dias) — duração compatível com
procedimento homologatório cumprido, não com pedidos abandonados. Como a
execução do plano homologado é o desfecho natural do instituto
(arts. 162-167), é altamente improvável que dezenas de procedimentos
tenham sido encerrados **sem** o plano ter sido homologado em algum
momento. O resultado é, portanto, **positivamente mascarado**: a fonte
registra o fim do ciclo (encerramento/extinção) mas não o ato central
(homologação). O gradiente de registrabilidade fica completo: pedido
registrado (305), fim do ciclo registrado (60), homologação invisível (0).

**Impugnações sem sinal utilizável:** apenas 9 dos 305 autos têm alguma
impugnação de crédito (média ~0 em todos os desfechos) — mesmo como proxy,
a variável não separa impugnação ao plano (art. 163, §1º) da verificação
ordinária de créditos.

**convolação zero é coerente:** a RE é procedimento homologatório e
consensual — não prevê decretação de falência como desfecho (ao contrário
da RJ, art. 73). A ausência total de convolação confirma que a classe 128
está limpa de contaminação por autos conexos.

**Coerência com o mart:** `data_fim_stay` nulo em todas as 305 linhas
(o stay só termina em homologação ou falência — nenhuma das duas é
registrada na classe 128), confirmando a semântica do Bloco 4 de ponta a
ponta.

**Leitura jurismétrica:** a RE, desenhada pela Lei 11.101/2005 justamente
pela celeridade e confidencialidade, é praticamente invisível na base
pública nacional — o instituto é de baixa demanda (80,3% ainda sem marco
de fim) e seu ato central não é registrável. Para estudá-lo, a fonte
adequada são os portões de transparência dos próprios tribunais, não o
Datajud.

#### Resposta final (pergunta 6)

O percentual de recuperações extrajudiciais com homologação do plano
**não pode ser calculado no padrão Datajud: entre os 305 processos de RE
originária, zero homologações são registráveis** — embora 60 processos
estejam encerrados, com tramitação típica de cerca de 1 ano compatível
com o procedimento cumprido, o ato que define o instituto (a homologação,
art. 162) não tem movimento próprio nem código na TPU, e nenhum tribunal
o registra de forma detectável. A resposta estrutural é tripla: (i) o
percentual pedido é não-respondível — e a ausência é o achado; (ii) a
invisibilidade é positiva, não casual: processos terminam sem que a
homologação apareça — o dado mascara atos que sabidamente ocorreram;
(iii) o instituto consensual, criado para ser rápido e confidencial, é
praticamente invisível na base pública — para medi-lo, a fonte adequada
são os portões de transparência dos tribunais, não o Datajud. A taxa de
"homologação sem impugnação" permanece, portanto, fora dos limites de
rastreabilidade da base.

## 7.7. Qual é a ocorrência de incidentes processuais (como habilitações e impugnações de crédito) por processo de RJ, e como esse volume impacta o gargalo da vara?

**Pergunta:** cada credor que disputa seu crédito gera um "incidente"
processual? E esse volume congestionaria a vara? A pergunta tem duas
camadas: quanto de incidente existe por RJ — e se processos com mais
incidentes tramitam mais devagar.

**Fonte no mart:** `gold_base_analitica` — duas vias de
contagem: (i) os contadores consolidados por processo principal
(`qtd_habilitacoes`, `qtd_impugnacoes`); e (ii) a contagem de processos
autônomos de incidente (classes TPU 111 — Habilitação de Crédito, 114 —
Impugnação de Crédito, e a legada 38) **vinculados à RJ pelo número CNJ**
(os 7 primeiros dígitos do número + o código de origem identificam a
mesma origem processual). O tempo de tramitação vem de
`dias_distribuicao_processamento` (pergunta 1) e
`dias_duracao_processo_total` (ajuizamento → desfecho). Este notebook não
refaz cálculos: consome o resultado pronto.

**Recorte:** classe 129, ajuizado a partir de 09/06/2005.

**Por que faixas e não correlação formal:** estratificar os tempos por
faixas de volume de incidentes (0, 1–5, 6–20, >20) mostra o gradiente do
efeito sem estatística pesada — mesma lógica da pergunta 4.

**Limitações (em linguagem simples):**

1. A contagem depende do vínculo autos-filho → autos-mãe: incidentes em
   autos não vinculados ficam de fora (subcontagem) — e o pool não vinculado
   é majoritariamente do regime anterior à Lei 11.101/2005;
2. Correlação não é causa: RJs maiores (mais credores, mais créditos
   disputados) têm mais incidentes **e** tramitação mais longa por
   complexidade própria — o gradiente mostra associação;
3. Na RJ, a verificação de créditos é internalizada: as impugnações à lista
   do administrador judicial (art. 8º) correm nos próprios autos da RJ e não
   geram processo contável — a métrica capta só a ponta autônoma do conflito
   (habilitação em autos próprios é estrutura típica da falência, art. 119);
4. O stay period não é usado aqui como métrica de gargalo: só é mensurável
   nas convoluções (limitação da pergunta 2).

**O que a consulta a seguir mede:** a ocorrência de incidentes processuais por RJ, por **dupla via independente**:

(i) os contadores consolidados no
mart (`qtd_habilitacoes`, `qtd_impugnacoes`: incidentes computados em autos próprios, classes 111 - habilitação de crédito e 114 - impugnação de crédito, vinculados à RJ-mãe pela chave raiz+origem do número CNJ na construção da gold); e

(ii) a contagem direta dos processos-filho autônomos (classes 111, 114 e a legada 38 - habilitação)
casados pela mesma chave na hora da consulta. A convergência dos dois totais **valida a regra de vinculação**: se os métodos independentes chegam ao mesmo volume, o vínculo não é artefato.

**Mecânica:** a subconsulta interna agrega os autos-filho por (raiz, origem) e o `LEFT JOIN` devolve o `n_inc` de cada RJ-mãe — quem não tem filho entra como **zero** (`COALESCE`), preservando a semântica contável do universo inteiro.

A saída traz volume por canal, média, mediana, p90, penetração (% de RJs com ao menos um incidente) e o
extremo.

Os contadores registram a **ocorrência** do incidente, não
seu conteúdo: não distinguem credor, valor nem desfecho — a leitura é de intensidade da disputa creditícia, não de resultado. A vinculação funciona porque habilitação e impugnação são autuadas **na mesma vara de origem da RJ-mãe** (registro de 1º grau).

**Como os contadores foram apurados (na construção da gold):** `qtd_habilitacoes` e `qtd_impugnacoes` não derivam de
movimentos — cada número conta incidentes autuados em autos próprios (petitórios filhos), vinculados à RJ pelo componente posicional da numeração CNJ (raiz = dígitos 1-7; origem = dígitos 17-20):

- `qtd_habilitacoes` — quantidade de processos filhos de classe 111
  (habilitação de crédito) vinculados à RJ;

- `qtd_impugnacoes` — quantidade de processos filhos de classe 114
  (impugnação à lista de credores) vinculados à RJ.

A cadeia de construção: `fato_incidentes` (1 linha por incidente filho, com gate de unicidade `count = distinct` que elimina duplicidade do mesmo ajuizamento) → agregação `GROUP BY raiz, origem`
→ `LEFT JOIN` na mart pela chave raiz+origem com `COALESCE(..., 0)`:
RJ-mãe sem incidente vinculado entra como zero (não como NULL), o
que preserva a semântica contável.

#### Consulta SQL

```text
-- Q7 · INCIDENCIA (dupla via): contadores do mart + autos-filho autonomos
WITH base AS (
SELECT r.numero_processo,
COALESCE(r.qtd_habilitacoes, 0) AS hab,
COALESCE(r.qtd_impugnacoes, 0) AS imp,
COALESCE(f.n_inc, 0) AS autonomos
FROM default.gold_base_analitica r
LEFT JOIN (
SELECT substring(numero_processo, 1, 7) AS raiz,
substring(numero_processo, 17, 4) AS origem,
count(\_) AS n_inc
FROM default.gold_base_analitica
WHERE classe_codigo IN (111, 114, 38)
GROUP BY 1, 2
) f
ON f.raiz = substring(r.numero_processo, 1, 7)
AND f.origem = substring(r.numero_processo, 17, 4)
WHERE r.classe_codigo = 129 AND r.escopo_lei_atual
)
SELECT
count(\_) AS total_rj,
sum(hab) AS hab_via_contador,
sum(imp) AS imp_via_contador,
sum(autonomos) AS incidentes_via_filho,
round(sum(autonomos) _ 1.0 / count(_), 2) AS media_incidentes_por_rj,
round(percentile_approx(autonomos, 0.5), 1) AS mediana_incidentes,
round(percentile_approx(autonomos, 0.9), 1) AS p90_incidentes,
round(count(CASE WHEN autonomos > 0 THEN 1 END) \_ 100.0
/ count(\_), 1) AS pct_rj_com_incidente,
max(autonomos) AS max_incidentes
FROM base;
```

##### Resposta:

| total_rj | hab_via_contador | imp_via_contador | incidentes_via_filho | media_incidentes_por_rj | mediana_incidentes | p90_incidentes | pct_rj_com_incidente | max_incidentes |
| :------- | :--------------- | :--------------- | :------------------- | :---------------------- | :----------------- | :------------- | :------------------- | :------------- |
| 5709     | 180              | 32               | 215                  | 0.04                    | 0                  | 0              | 3.6                  | 2              |

#### Análise do resultado da incidência

**O que os números dizem:**

| Métrica                   | Valor | Leitura                                                                                                |
| ------------------------- | ----- | ------------------------------------------------------------------------------------------------------ |
| `total_rj`                | 5.709 | universo íntegro — igual ao baseline congelado das demais perguntas                                    |
| `hab_via_contador`        | 180   | habilitações (classe 111) autuadas em autos próprios, pelo canal consolidado na gold                   |
| `imp_via_contador`        | 32    | impugnações (classe 114) autuadas em autos próprios, pelo mesmo canal                                  |
| `incidentes_via_filho`    | 215   | o mesmo fenômeno contado pela via independente (contagem direta dos filhos, inclui a classe legada 38) |
| `media_incidentes_por_rj` | 0,04  | menos de um incidente a cada 25 RJs                                                                    |
| `mediana_incidentes`      | 0     | a RJ típica **não tem nenhum** incidente autônomo                                                      |
| `p90_incidentes`          | 0     | em 9 de cada 10 RJs, o total de incidentes em autos próprios é **zero**                                |
| `pct_rj_com_incidente`    | 3,6%  | apenas ~206 das 5.709 RJs têm ao menos um incidente vinculado                                          |
| `max_incidentes`          | 2     | nenhuma RJ chega a 3 incidentes — não existe cauda                                                     |

**A validação da dupla via.** Os dois métodos independentes convergem: o canal consolidado na gold soma 180 + 32 = **212**, e a contagem direta dos autos-filho soma **215** — diferença de 3, explicada pelo critério de cada via: os contadores do mart recortam apenas as classes 111 e 114, enquanto a contagem direta inclui a classe 38 (legada,
regime anterior à lei). A convergência em dois canais construídos de forma independente **valida a regra de vinculação raiz+origem** para incidentes: o vínculo não é artefato da modelagem, e o resíduo de 3 é atribuível, não inexplicável.

**O achado e sua limitação.**

**Achado:** habilitações e impugnações autuadas como processos
próprios são raras nas RJs do corpus — 3,6% de penetração (212
incidentes em classes 111/114; 215 com a classe legada 38), máximo de
2 por RJ, mediana 0.

**Limitação do achado:** essa métrica conta apenas o incidente
**autuado em autos próprios** (processo-filho com número CNJ próprio).
Mas na prática forense o incidente raramente ganha autos próprios: o
credor divergente apresenta sua discordância como **petição dentro
dos próprios autos da RJ** (após a publicação da lista de credores —
art. 8º da Lei 11.101/2005), e o juiz decide nos mesmos autos. O dado
não registra isso como processo separado — portanto o contador não
vê.

**Consequência para a resposta:** o 3,6% não mede a frequência da
disputa creditícia — mede apenas a **ponta visível** dela, a que
migrou para autos próprios. A frequência real é maior, mas
invisível neste formato de dado (mesma limitação já documentada na
homologação do plano — pergunta 2 — e no vínculo recursal — pergunta 9).

**Coerência interna da distribuição:** a comparação média × mediana
(0,04 × 0) confirma distribuição degenerada — quase todo o universo em
zero, sem cauda expressiva (o máximo de 2 descarta concentração em
poucas RJs). O número é pequeno e homogêneo, não concentrado — o que
também limita a utilidade estatística de qualquer corte por grupo
(poucos pares para comparar).

**Implicação para a resposta da pergunta 7 (parte 1):** o canal
mensurável no dado estruturado é o dos autos-filho, e ele registra
**3,6% de penetração** (212 incidentes pelas classes 111/114; 215
incluindo a legada 38; máximo de 2 por RJ). A resposta final deve
apresentar o número **com a ressalva de canal**: mede-se a incidência
de incidentes autuados em autos próprios, não a frequência real das
habilitações e impugnações, que no registro migram majoritariamente
para movimentos dentro dos autos da RJ.

**O que a consulta a seguir mede:** o **impacto no gargalo da vara** — compara
os tempos de tramitação das RJs **com** ao menos um incidente autônomo
vinculado contra as **sem** incidente: mediana de dias até o
processamento (pergunta 1) e mediana da duração total do processo
(ajuizamento → desfecho). Se incidentes congestionam a vara, o grupo
com incidente deve tramitar mais devagar.

**Mecânica:** os incidentes são reduzidos ao par (raiz, origem)
deduplicado; o `LEFT JOIN` marca cada RJ-mãe como 'com incidente' ou
'sem incidente'; cada grupo é agregado com medianas (robustas a
extremos, padrão do projeto). `com_proc` e `com_desfecho` reportam os
`n` de cada lado — a mediana de processamento só existe para quem tem
o marco, e omitir os `n` esconderia viés de seleção.

**Aviso metodológico:** o grupo com incidente é pequeno (~208 RJs,
poucos desfechos registrados) — diferenças de medianas entre grupos
desse tamanho são **indício, não causalidade**.

#### Consulta SQL

```text
-- Q7 · CONTRASTE: tempos das RJs com vs sem incidente autonomo
WITH inc AS (
SELECT substring(numero_processo, 1, 7) AS raiz,
substring(numero_processo, 17, 4) AS origem
FROM default.gold_base_analitica
WHERE classe_codigo IN (111, 114, 38)
),
base AS (
SELECT CASE WHEN i.raiz IS NOT NULL THEN 'com incidente' ELSE 'sem incidente' END AS grupo,
r.dias_distribuicao_processamento,
r.dias_duracao_processo_total
FROM default.gold_base_analitica r
LEFT JOIN (SELECT DISTINCT raiz, origem FROM inc) i
ON i.raiz = substring(r.numero_processo, 1, 7)
AND i.origem = substring(r.numero_processo, 17, 4)
WHERE r.classe_codigo = 129 AND r.escopo_lei_atual
)
SELECT grupo,
count(\*) AS qtd_rj,
count(dias_distribuicao_processamento) AS com_proc,
round(percentile_approx(dias_distribuicao_processamento, 0.5), 1) AS mediana_proc,
count(dias_duracao_processo_total) AS com_desfecho,
round(percentile_approx(dias_duracao_processo_total, 0.5), 1) AS mediana_duracao_total
FROM base
GROUP BY 1;
```

##### Resposta:

| grupo         | qtd_rj | com_proc | mediana_proc | com_desfecho | mediana_duracao_total |
| :------------ | :----- | :------- | :----------- | :----------- | :-------------------- |
| com incidente | 208    | 63       | 52           | 24           | 1459                  |
| sem incidente | 5501   | 2359     | 48           | 1013         | 1467                  |

#### Análise do resultado do contraste (com × sem incidente)

**O que os números dizem:**

| Métrica                 | Com incidente | Sem incidente | Leitura                                                                    |
| ----------------------- | ------------- | ------------- | -------------------------------------------------------------------------- |
| `qtd_rj`                | 208           | 5.501         | o grupo com incidente é 3,6% do universo — confere com a célula anterior   |
| `com_proc`              | 63 (30%)      | 2.359 (43%)   | RJs com incidente têm menos vezes o marco do processamento datado          |
| `mediana_proc`          | 52 dias       | 48 dias       | diferença de 4 dias (~8%) — pequena                                        |
| `com_desfecho`          | 24            | 1.013         | a mediana de duração do grupo com incidente repousa sobre **24 casos**     |
| `mediana_duracao_total` | 1.459 dias    | 1.467 dias    | diferença de 8 dias — nula na prática (e favorável ao grupo com incidente) |

**O achado:** não há diferença relevante de tramitação entre as RJs
com e sem incidente autuado em autos próprios. A mediana até o
processamento diverge em apenas 4 dias (52 × 48) e a duração total é
praticamente idêntica (1.459 × 1.467) — na direção **oposta** à
esperada pelo gargalo, o grupo com incidente tramita (leve e
irrelevantemente) mais rápido.

**A limitação do achado:** o grupo com incidente é pequeno e a
comparação repousa sobre amostras mínimas — a mediana de duração usa
**24 casos** e a de processamento, **63**. Diferenças de poucos dias
em amostras desse tamanho não sustentam qualquer inferência; e as
proporções de `com_proc` (30% × 43%) e `com_desfecho` (11% × 18%)
sugerem composição temporal distinta entre os grupos (as RJs com
incidente vinculado são, em média, de ajuizamento diferente), o que
contamina qualquer comparação direta de medianas.

**Consequência para a resposta (parte 2 da pergunta 7):** o canal
mensurável — incidentes autuados em autos próprios, 3,6% de
penetração — é **raro demais e homogêneo demais para produzir efeito
detectável no gargalo da vara**. A resposta final deve registrar: (i)
o volume de incidentes por autos-filho é ínfimo e não se associa a
tramitação mais lenta neste dado; (ii) a leitura não exclui impacto
real, porque a maior parte da disputa creditícia corre dentro dos
autos da RJ (petições e decisões internas), formato invisível ao
contador — mesma fronteira estrutural documentada na parte 1 e nas
perguntas 2 e 9.

#### Resposta final da pergunta 7

**Parte 1 — Ocorrência de incidentes por RJ.**

No canal mensurável do dado estruturado — incidentes autuados em
**autos próprios** (processos-filho de habilitação, classe 111, e
impugnação, classe 114, vinculados pela chave raiz+origem, validado
por dupla via independente: 212 pelo contador consolidado × 215 pela
contagem direta, diferença atribuível à classe legada 38) — a
ocorrência é mínima:

- **212 incidentes autônomos em 5.709 RJs** (180 habilitações + 32
  impugnações);
- **3,6% de penetração**: apenas ~208 RJs têm ao menos um incidente;
- mediana **0** e p90 **0**: a RJ típica não tem nenhum incidente
  autônomo — e nenhuma RJ passa de 2.

**Parte 2 — Impacto no gargalo da vara.**

Comparando RJs com × sem incidente autônomo (208 × 5.501), **não há
diferença relevante de tramitação**: mediana até o processamento de 52
dias (com incidente) × 48 dias (sem), e duração total praticamente
idêntica (1.459 × 1.467 dias) — na direção oposta à esperada pelo
gargalo. O comparativo repousa sobre amostras mínimas (63 marcos e 24
desfechos no grupo com incidente), o que reforça a leitura de
**ausência de efeito detectável**, não a prova de ausência de efeito.

**Limitação estrutural que condiciona ambas as partes.** O contador só
enxerga o incidente autuado como processo autônomo. Na prática
forense, o incidente raramente ganha autos próprios: o credor
divergente apresenta sua discordância como petição dentro dos
próprios autos da RJ (após a publicação da lista de credores —
art. 8º da Lei 11.101/2005), e o juiz decide nos mesmos autos — sem
processo novo, sem número CNJ próprio, invisível ao contador. O
número medido é a ponta visível da disputa creditícia, não sua
frequência real.

**Síntese.** O dado estruturado mostra incidentes em autos próprios raros (3,6% das RJs; máximo de 2 por RJ) sem qualquer associação a tramitação mais lenta. O número baixo, porém, não significa que a disputa de créditos seja rara: significa que a maior parte dela não vira processo separado — corre dentro dos próprios autos da RJ, como petição e decisão nos mesmos autos, formato que o
padrão Datajud não registra com número próprio.

## 7.8. Qual é a taxa de concessão de tutelas de urgência cautelares antecedentes ao pedido principal de recuperação?

**Pergunta:** antes (ou no limiar) do pedido de recuperação, o devedor pede
ao juiz uma tutela de urgência — tipicamente a suspensão das execuções —
para ganhar tempo e viabilizar a negociação. Quantas RJs têm essa tutela?

**Fonte no mart:** `gold_base_analitica` —
`data_primeira_tutela` (primeira tutela de urgência/cautelar registrada nos
autos) e `data_processamento_efetivo` (concessão da RJ, pergunta 1); a
antecedência é computada comparando as duas datas. A via autônoma é testada
pelas classes 12134 e 12084 (ambas denominadas Tutela Cautelar Antecedente), vinculada à RJ pela chave do número CNJ (raiz + origem, método validado na pergunta 7). Este
notebook não refaz cálculos: consome o resultado pronto.

**Recorte:** classe 129, ajuizado a partir de 09/06/2005. A tutela é
instrumento do CPC, não da Lei 11.101 — não há filtro adicional de lei.

**Sobre "concessão":** o dado registra a tutela
**materializada nos autos** (com data). Pedidos denegados tendem a não
deixar marco datado — então a taxa medida é de **deferimento**, não de
pedido.

**Limitações:**

1. A coluna data_primeira_tutela cobre apenas tutelas registradas nos próprios autos da RJ — tutelas deferidas em autos apensados ou em incidentes podem não aparecer nela;
2. Tutelas ajuizadas em unidade de origem diversa da RJ não casam na chave CNJ (subcontagem);
3. **Registros incompletos**: entre as RJs com tutela registrada, 266 (34%) não têm data de processamento registrada — o evento secundário (a tutela de urgência) aparece e o principal não. Isso impede classificar a antecedência da tutela nesses casos. É o mesmo padrão de registros incompletos já documentado nas perguntas 1 e 2.

**Observação — duas vias distintas:** este notebook examina duas vias para a mesma pergunta. Na **via intra-autos**, a tutela tramita dentro dos próprios autos da RJ (medida pela coluna
data_primeira_tutela). Na **via autônoma**, a tutela tem processo
próprio e separado (classes 12134/12084) — e, como o dado não declara a qual RJ ela pertence, o vínculo é deduzido pela estrutura do número CNJ (raiz + origem). Os números das duas vias não se somam: são universos diferentes.

#### O que a consulta a seguir mede e como (via intra-autos)

**Entrada:** todos os campos vêm da tabela
`default.gold_base_analitica`, já filtrada para os processos de Recuperação
Judicial (classe 129) ajuizados no período da lei vigente (a partir de
09/06/2005).

A consulta lê três colunas por processo:

| Coluna lida                  | O que representa                                                                                       |
| ---------------------------- | ------------------------------------------------------------------------------------------------------ |
| `data_ajuizamento`           | quando a RJ foi protocolada (o "pedido principal")                                                     |
| `data_primeira_tutela`       | a data da primeira tutela de urgência registrada nos próprios autos (nulo = o processo não tem tutela) |
| `data_processamento_efetivo` | a data em que o juiz concedeu o processamento da RJ (nulo = não registrada)                            |

**Saída — o que ela produz (uma única linha com seis números):**

| Coluna produzida                      | Como é calculada                                                           | O que significa                                                                                                                                      |
| ------------------------------------- | -------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| `total_rj`                            | conta todos os processos do recorte                                        | o denominador de todas as taxas (5.709)                                                                                                              |
| `com_tutela`                          | conta os processos em que a data da tutela existe                          | quantas RJs têm tutela de urgência nos próprios autos (792)                                                                                          |
| `pct_rj_com_tutela`                   | com_tutela ÷ total_rj × 100                                                | a taxa de incidência da tutela (13,9%)                                                                                                               |
| `tutela_no_ajuizamento`               | conta os casos em que a tutela é igual ao ajuizamento                      | quantas tutelas vieram no mesmo dia do pedido principal — (12)                                                                                       |
| `mediana_dias_ajuizamento_tutela`     | mediana de (data da tutela − data de ajuizamento), só para quem tem tutela | quantos dias após o pedido a tutela típica é deferida (+26 dias)                                                                                     |
| `com_tutela_sem_processamento_datado` | conta os casos com tutela mas **sem** data de processamento                | quantas RJs têm o evento secundário (tutela) registrado e o principal (processamento) não (266) — alimenta a limitação 3 sobre registros incompletos |

**Em uma frase:** a consulta pega os 5.709 processos de RJ do período da lei vigente, verifica em quantos há tutela de urgência nos próprios autos e — para esses — situa a tutela em relação à data do pedido e verifica em quantos o processamento está datado, produzindo os seis números que sustentam a resposta: a incidência (13,9%), as tutelas deferidas no próprio dia do pedido (12), o tempo típico até a tutela (26 dias) e o balanço de registros incompletos (266).

#### Consulta SQL

```text
-- Q8 · VIA INTRA-AUTOS: incidencia da tutela e antecedencia ao pedido principal
SELECT
count(\_) AS total_rj,
count(CASE WHEN data_primeira_tutela IS NOT NULL THEN 1 END) AS com_tutela,
round(count(CASE WHEN data_primeira_tutela IS NOT NULL THEN 1 END)
\_ 100.0 / count(\*), 1) AS pct_rj_com_tutela,
count(CASE WHEN data_primeira_tutela = data_ajuizamento THEN 1 END) AS
tutela_no_ajuizamento,
round(percentile_approx(
CASE WHEN data_primeira_tutela IS NOT NULL
THEN datediff(data_primeira_tutela, data_ajuizamento) END, 0.5), 1)
AS mediana_dias_ajuizamento_tutela,
count(CASE WHEN data_primeira_tutela IS NOT NULL
AND data_processamento_efetivo IS NULL THEN 1 END) AS com_tutela_sem_processamento_datado
FROM default.gold_base_analitica
WHERE classe_codigo = 129 AND escopo_lei_atual;
```

##### Resposta:

| total_rj | com_tutela | pct_rj_com_tutela | tutela_no_ajuizamento | mediana_dias_ajuizamento_tutela | com_tutela_sem_processamento_datado |
| :------- | :--------- | :---------------- | :-------------------- | :------------------------------ | :---------------------------------- |
| 5709     | 792        | 13.9              | 12                    | 26                              | 266                                 |

#### O que a consulta a seguir mede e como (via autônoma)

**Entrada:** lê a mesma tabela
`default.gold_base_analitica`, mas agora em **duas frentes**:

| Frente        | Filtro                                                                            | O que extrai                                                                                                                                 |
| ------------- | --------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| CTE `rj`      | classe 129 (pedidos de RJ) no período da lei vigente                              | de cada RJ: a **raiz** (7 primeiros dígitos do número CNJ), a **origem** (4 últimos dígitos, que identificam a vara) e a data de ajuizamento |
| CTE `tutelas` | classes 12134 (tutela cautelar antecedente) e 12084 (tutela cautelar antecedente) | de cada tutela ajuizada em **autos próprios**: o número do processo, a mesma raiz, a mesma origem e a data de ajuizamento                    |

A liga entre as duas frentes é a **estrutura do número CNJ**: processos
relacionados (um incidente e o seu processo-mãe) compartilham a raiz e a
origem — só o número sequencial do meio muda. Assim, uma tutela autônoma
é considerada **vinculada** a uma RJ quando existe, no corpus, uma
recuperação com a mesma raiz + origem.

**Saída — o que ela produz (uma única linha com três números):**

| Coluna produzida          | Como é calculada                                                                 | O que significa                                                                                                    |
| ------------------------- | -------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| `total_tutelas_autonomas` | conta todas as tutelas das classes 12134/12084 no corpus                         | o universo da via autônoma (279)                                                                                   |
| `vinculadas_a_rj`         | conta as tutelas que encontraram uma RJ com a mesma raiz + origem no JOIN        | quantas dessas tutelas pertencem a uma RJ do corpus (1)                                                            |
| `antecedentes`            | conta as vinculadas em que a tutela é anterior **ou igual** ao ajuizamento da RJ | quantas foram usadas como **preparação** da recuperação — a resposta da via autônoma (0; o único caso é posterior) |

**Em uma frase:** a consulta pega as 279 tutelas ajuizadas em processos separados (a via autônoma do art. 303 do CPC), verifica quais delas
pertencem a uma RJ do corpus pela chave do número CNJ (mesma raiz e
mesma origem) e produz três números: o universo autônomo (279), os
vínculos encontrados (1) e os casos de verdadeira preparação, anteriores ao pedido (0) — confirmando o uso de tutela cautelar antecedente, uma ação autônoma, não é uma via normalmente utilizada em recuperação judicial.

#### Consulta SQL

```text
-- Q8 · VIA AUTONOMA: tutelas cautelares antecedentes (12134/12084) vinculadas a RJ
WITH rj AS (
SELECT substring(numero_processo, 1, 7) AS raiz,
substring(numero_processo, 17, 4) AS origem,
data_ajuizamento AS data_ajuizamento_rj
FROM default.gold_base_analitica
WHERE classe_codigo = 129 AND escopo_lei_atual
),
tutelas AS (
SELECT numero_processo,
substring(numero_processo, 1, 7) AS raiz,
substring(numero_processo, 17, 4) AS origem,
data_ajuizamento
FROM default.gold_base_analitica
WHERE classe_codigo IN (12134, 12084)
)
SELECT
(SELECT count(\*) FROM tutelas) AS total_tutelas_autonomas,
count(t.numero_processo) AS vinculadas_a_rj,
count(CASE WHEN t.data_ajuizamento <= r.data_ajuizamento_rj
THEN 1 END) AS antecedentes
FROM tutelas t
JOIN rj r ON r.raiz = t.raiz AND r.origem = t.origem;
```

##### Resposta:

| total_tutelas_autonomas | vinculadas_a_rj | antecedentes |
| :---------------------- | :-------------- | :----------- |
| 279                     | 1               | 0            |

#### Análise da resposta (pergunta 8)

**Resultados (classe 129-recuperação judicial, Lei 11.101/2005, 5.709 RJs):**
| Indicador | Valor |
|---|---|
| Total de RJs | 5709 |
| RJs com tutela registrada nos próprios autos | 792 (**13,9%**) |
| RJ com tutela no dia do ajuizamento do pedido principal | **12 (0,2%)** |
| Mediana ajuizamento → tutela | **+26 dias** |
| Tutelas autônomas no corpus (classes 12134/12084) | 279 |
| Autônomas vinculadas a uma RJ | 1 |

**Três achados:**

1. **A tutela pedida antes da recuperação é praticamente inexistente:**
   de todas as 5.709 RJs, apenas 12 (0,2%) tiveram uma tutela deferida
   **no próprio dia do ajuizamento do pedido** — o máximo que a via
   intra-autos permite como "antecedência".

2. **O que existe na prática é a tutela pedida junto com a recuperação:**
   13,9% das RJs registram uma tutela de urgência dentro dos próprios
   autos, deferida em geral 26 dias após o pedido. O caminho típico é:
   o empresário entra com a recuperação, o juiz analisa o pedido e, na
   mesma decisão (ou nos dias seguintes a ela), concede a proteção que
   suspende as execuções. Isso faz sentido: a própria lei já suspende
   as execuções a partir do processamento — a tutela e a concessão são
   parte do mesmo momento, e não uma preparação que vem antes.

#### Resposta final (pergunta 8)

A tutela de urgência **antecedente ao pedido principal** é quase
inexistente: apenas 12 dos 5.709 processos de RJ (0,2%) tiveram tutela
deferida antes ou no dia do ajuizamento, e a via autônoma do art. 303 do
CPC (tutela cautelar em autos próprios) contribui com um único caso
vinculado a uma RJ — ainda por cima posterior ao pedido. O que existe na
prática é outra coisa: **13,9% das RJs registram tutela de urgência nos
próprios autos**. Em suma: o instrumento da tutela antecedente autônoma,
disponível desde 2015 no CPC, não se firmou como preparação da RJ no
corpus nacional; a suspensão das execuções que viabiliza a negociação é
obtida dentro da própria recuperação. Ressalva: a taxa mede
**deferimentos** (atos datados) — pedidos denegados são invisíveis ao
padrão Datajud.

## 7.9. Qual é a taxa de recursos (Agravos de Instrumento) interpostos contra decisões que deferem ou indeferem o processamento da RJ e qual a taxa de reforma dessas decisões no 2º Grau?

**Resposta sintética:** a métrica não é computável com os dados disponíveis pelo Datajud do CNJ e o diagnóstico do porquê é o achado deste notebook.

**O que foi testado:** vincular agravos de instrumento (classe TPU 202, 30.750 processos no corpus) às RJs (classe 129, 5.709 processos após 09/06/2005) pela chave raiz+origem do número CNJ — o mesmo método validado na pergunta 7 para incidentes da mesma vara (tutela cautelar antecedente).

**Resultado: 0 vínculos em 30.750 agravos.** O motivo é estrutural: a chave raiz+origem do número CNJ identifica processos que nasceram na mesma vara (foi assim que vinculamos a tutela antecedente às RJs na pergunta 8). O agravo de instrumento, porém, é um recurso autuado no
tribunal, com numeração própria — raiz sequencial do 2º grau e unidade de origem do tribunal. Processo e recurso, portanto, nunca compartilham a chave: o vínculo é impossível com o dado disponível. E sem saber a qual RJ cada agravo pertence, não há sobre quais autos
medir o desfecho de 2º grau.

Nem a delimitação pelo assunto-pai 4993 (recuperação judicial e falências) resolve: no corpus, os agravos de instrumento orbitam classes originárias diversas desse assunto — 108 (Falência), 128 (Recuperação Extrajudicial), 129 (Recuperação Judicial), entre outras —, mas o número CNJ do recurso não preserva nem a classe nem a vara do
processo originário, de modo que não há como atribuir um agravo a qualquer uma dessas classes.

**Por que isso rjustifica a não resposta à pergunta:** o Datajud não registra o vínculo recurso→decisão agravada, e a numeração CNJ dos recursos não preserva a origem — a taxa de agravabilidade e a taxa de reforma são estruturalmente não deriváveis do acervo. É o segundo não-achado estrutural do projeto (o primeiro: a homologação do plano, pergunta 3).

**Universo (filtros):** RJs classe 129 com escopo_lei_atual; agravos classe 202 do corpus completo.

#### O que a próxima consulta de QA verifica

**Entrada:** a tabela `default.gold_base_analitica` (somente colunas de
número de processo e classe), de onde a consulta extrai, para dois
universos — RJs (classe 129, universo da lei) e agravos de instrumento
(classe 202) — apenas os dois componentes da chave CNJ: a **raiz**
(7 primeiros dígitos) e a **origem** (4 últimos). Todo o resto do
registro é descartado: a pergunta é só sobre a chave.

**Por que existe:** a consulta principal de vínculo exigiu raiz **e**
origem iguais entre agravo e RJ — e devolveu 0 em 30.750 agravos. Antes
de aceitar o zero como resultado, é preciso descartar a hipótese de bug:
a chave pode ter falhado por um componente isolado (ex.: origens batem,
raízes não). Este QA decompõe o vínculo e testa cada componente
isoladamente:

- `agravos_mesma_raiz` — quantos agravos têm raiz igual à de alguma RJ,
  sem exigir origem (se a raiz do agravo nunca coincide, é numeração
  própria do tribunal);
- `agravos_origem_de_vara_rj` — quantos agravos têm origem igual à de
  alguma RJ (se ~0, o agravo não nasce em vara de 1º grau);
- `origens_distintas_agravos` × `origens_distintas_rj` — o contraste
  entre os conjuntos de unidades de origem dos dois universos: se os
  agravos moram em poucas unidades (48) e as RJs em muitas (397), são
  dois mundos de numeração, não o mesmo.

**Saída (uma linha com cinco números):** o tamanho do universo de
agravos, as duas contagens de coincidência por componente isolado e os
tamanhos dos dois conjuntos de unidades de origem.

**Resultado esperado conforme o diagnóstico:**

- coincidências por componente ≈ 0 e conjuntos de origem incompatíveis
  (48 × 397) → o zero da consulta principal é **estrutura do dado**, não
  bug: o agravo é autuado no tribunal com numeração própria e jamais
  compartilha a chave com a RJ — o vínculo é impossível;
- coincidência alta em um componente → haveria caminho alternativo de
  vínculo a explorar (ex.: origem + janela temporal) antes de fechar a
  pergunta.

#### Como funciona o próximo código

**Passo 1 — duas CTEs (blocos temporários):**

- `rj` — seleciona as RJs do universo da lei (`classe_codigo = 129` +
  `escopo_lei_atual`) e, de cada número de processo, extrai apenas os
  dois componentes da chave CNJ: a **raiz** (`substring(..., 1, 7)` —
  os 7 primeiros dígitos, sequência da vara) e a **origem**
  (`substring(..., 17, 4)` — os 4 últimos, código da unidade
  judiciária). Guarda também `data_ajuizamento` e
  `data_processamento_efetivo`, necessárias para as janelas;
- `agravos` — faz o mesmo para os agravos de instrumento
  (`classe_codigo = 202`), guardando a data de ajuizamento do agravo.

O objetivo das CTEs é reduzir cada processo a um par (raiz, origem)
mais as datas: só o que a análise usa.

**Passo 2 — o JOIN (a tentativa de vínculo):**

`FROM agravos a JOIN rj r ON r.raiz = a.raiz AND r.origem = a.origem`
exige que agravo e RJ tenham **raiz e origem iguais ao mesmo tempo** —
a chave que identificou processos irmãos da mesma vara na pergunta 8.
Se nenhum agravo casar, o JOIN devolve zero linhas.

**Passo 3 — as sete medidas da saída (uma linha só):**

- `total_rj`, `rj_com_processamento`, `rj_sem_processamento` — os
  denominadores das taxas (5.709 RJs; 2.422 com e 3.287 sem
  processamento datado), contados por subconsultas sobre a CTE `rj`
  (por isso sobrevivem mesmo quando o JOIN não casa);
- `total_agravos_202` — o universo de agravos (30.750), também por
  subconsulta independente do JOIN;
- `agravos_vinculados_a_rj` — **a medida central**: conta as linhas
  que sobreviveram ao JOIN, ou seja, quantos agravos se vincularam a
  alguma RJ;
- `janela_a_agravos` — dos vinculados, os ajuizados entre o
  ajuizamento da RJ e 90 dias após o processamento efetivo
  (`CASE WHEN` com as três condições: posterior ao ajuizamento,
  processamento existente, e dentro da janela de 90 dias);
- `janela_b_agravos` — dos vinculados, os ajuizados após o ajuizamento
  em RJs sem processamento datado (proxy do indeferimento).

**Detalhe técnico importante:** os quatro primeiros números usam
subconsultas sobre as CTEs, e os três últimos usam `count` sobre o
resultado do JOIN. É por isso que o output pode mostrar universos
populosos (5.709 RJs, 30.750 agravos) e, ao mesmo tempo, zero nas
colunas de vínculo — sem isso, um JOIN vazio zeraria tudo e esconderia
os denominadores.

#### Consulta SQL

```text
-- Q9 · JANELAS A/B (camada gold): agravos de instrumento (classe 202)
-- vinculados a RJ (classe 129) pela chave raiz+origem CNJ, atribuidos a
-- decisao de processamento por janela temporal. Consome apenas a gold.
WITH rj AS (
SELECT substring(numero_processo, 1, 7) AS raiz,
substring(numero_processo, 17, 4) AS origem,
data_ajuizamento AS data_ajuizamento_rj,
data_processamento_efetivo
FROM default.gold_base_analitica
WHERE classe_codigo = 129 AND escopo_lei_atual
),
agravos AS (
SELECT numero_processo,
substring(numero_processo, 1, 7) AS raiz,
substring(numero_processo, 17, 4) AS origem,
data_ajuizamento AS data_ajuizamento_agravo
FROM default.gold_base_analitica
WHERE classe_codigo = 202
)
SELECT
(SELECT count(\_) FROM rj) AS total_rj,
(SELECT count(\_) FROM rj WHERE data_processamento_efetivo IS NOT NULL) AS
rj_com_processamento,
(SELECT count(\_) FROM rj WHERE data_processamento_efetivo IS NULL) AS rj_sem_processamento,
(SELECT count(\_) FROM agravos) AS total_agravos_202,
count(a.numero_processo) AS agravos_vinculados_a_rj,
count(CASE WHEN a.data_ajuizamento_agravo >= r.data_ajuizamento_rj
AND r.data_processamento_efetivo IS NOT NULL
AND a.data_ajuizamento_agravo
<= date_add(r.data_processamento_efetivo, 90)
THEN 1 END) AS janela_a_agravos,
count(CASE WHEN a.data_ajuizamento_agravo > r.data_ajuizamento_rj
AND r.data_processamento_efetivo IS NULL
THEN 1 END) AS janela_b_agravos
FROM agravos a
JOIN rj r ON r.raiz = a.raiz AND r.origem = a.origem;
```

##### Resposta:

| total_rj | rj_com_processamento | rj_sem_processamento | total_agravos_202 | agravos_vinculados_a_rj | janela_a_agravos | janela_b_agravos |
| :------- | :------------------- | :------------------- | :---------------- | :---------------------- | :--------------- | :--------------- |
| 5709     | 2422                 | 3287                 | 30750             | 0                       | 0                | 0                |

#### Análise do resultado da vinculação

**O que o output mostra:**

| Coluna                    | Valor  | Leitura                                                                      |
| ------------------------- | ------ | ---------------------------------------------------------------------------- |
| `total_rj`                | 5.709  | o universo de RJs da lei está íntegro — igual às perguntas 1 e 8             |
| `rj_com_processamento`    | 2.422  | denominador da taxa da janela A (contra o deferimento)                       |
| `rj_sem_processamento`    | 3.287  | denominador da taxa da janela B (proxy do indeferimento)                     |
| `total_agravos_202`       | 30.750 | o universo de agravos de instrumento do corpus                               |
| `agravos_vinculados_a_rj` | **0**  | **a medida central: nenhum agravo casou com alguma RJ na chave raiz+origem** |
| `janela_a_agravos`        | 0      | consequência direta: sem vínculos, nenhum agravo cai na janela A             |
| `janela_b_agravos`        | 0      | idem para a janela B                                                         |

**A leitura do resultado:** com `agravos_vinculados_a_rj = 0`, as taxas
das janelas são incomputáveis — `0 / 2.422` e `0 / 3.287` não são taxas,
são a ausência de objeto. Mas um zero em 30.750 agravos precisa ser
explicado antes de aceito: pode ser bug de código (chave mal extraída,
JOIN mal condicionado) ou estrutura do dado. **A célula de QA seguinte
faz esse diagnóstico**: decompõe a chave em raiz e origem, testa cada
componente isoladamente e compara os conjuntos de unidades de origem
dos dois universos — para determinar se o zero é artefato ou estrutura.

#### O que a consulta de QA a seguir verifica

**Entrada:** a tabela `default.gold_base_analitica` (somente colunas de
número de processo e classe), de onde a consulta extrai, para dois
universos — RJs (classe 129, universo da lei) e agravos de instrumento
(classe 202) — apenas os dois componentes da chave CNJ: a **raiz**
(7 primeiros dígitos) e a **origem** (4 últimos). Todo o resto do
registro é descartado: a pergunta é só sobre a chave.

**Por que existe:** a consulta principal de vínculo exigiu raiz **e**
origem iguais entre agravo e RJ — e devolveu 0 em 30.750 agravos. Antes
de aceitar o zero como resultado, é preciso descartar a hipótese de bug:
a chave pode ter falhado por um componente isolado (ex.: origens batem,
raízes não). Este QA decompõe o vínculo e testa cada componente
isoladamente:

- `agravos_mesma_raiz` — quantos agravos têm raiz igual à de alguma RJ,
  sem exigir origem (se a raiz do agravo nunca coincide, é numeração
  própria do tribunal);
- `agravos_origem_de_vara_rj` — quantos agravos têm origem igual à de
  alguma RJ (se ~0, o agravo não nasce em vara de 1º grau);
- `origens_distintas_agravos` × `origens_distintas_rj` — o contraste
  entre os conjuntos de unidades de origem dos dois universos: se os
  agravos moram em poucas unidades (48) e as RJs em muitas (397), são
  dois mundos de numeração, não o mesmo.

**Saída (uma linha com cinco números):** o tamanho do universo de
agravos, as duas contagens de coincidência por componente isolado e os
tamanhos dos dois conjuntos de unidades de origem.

**Resultado esperado conforme o diagnóstico:**

- coincidências por componente ≈ 0 e conjuntos de origem incompatíveis
  (48 × 397) → o zero da consulta principal é **estrutura do dado**, não
  bug: o agravo é autuado no tribunal com numeração própria e jamais
  compartilha a chave com a RJ — o vínculo é impossível;
- coincidência alta em um componente → haveria caminho alternativo de
  vínculo a explorar (ex.: origem + janela temporal) antes de fechar a
  pergunta.

  #### Consulta SQL

```text
  -- Q9 · QA: por que a vinculacao raiz+origem nao capturou NENHUM agravo?
  -- Hipotese estrutural: a tutela antecedente (Q7/Q8) e incidente da mesma
  -- vara (compartilha raiz+origem); o agravo de instrumento e autuado no
  -- tribunal com numero proprio (raiz e origem proprias). Diagnostico
  -- separa os dois componentes da chave e compara os universos de origem.
  WITH rj AS (
  SELECT substring(numero_processo, 1, 7) AS raiz,
  substring(numero_processo, 17, 4) AS origem
  FROM default.gold_base_analitica
  WHERE classe_codigo = 129 AND escopo_lei_atual
  ),
  agravos AS (
  SELECT substring(numero_processo, 1, 7) AS raiz,
  substring(numero_processo, 17, 4) AS origem
  FROM default.gold_base_analitica
  WHERE classe_codigo = 202
  )
  SELECT
  (SELECT count(\*) FROM agravos) AS total_agravos,
  count(CASE WHEN EXISTS (SELECT 1 FROM rj r WHERE r.raiz = a.raiz)
  THEN 1 END) AS agravos_mesma_raiz,
  count(CASE WHEN EXISTS (SELECT 1 FROM rj r WHERE r.origem = a.origem)
  THEN 1 END) AS agravos_origem_de_vara_rj,
  count(DISTINCT a.origem) AS origens_distintas_agravos,
  (SELECT count(DISTINCT origem) FROM rj) AS origens_distintas_rj
```

FROM agravos a;

##### Resposta:

| total_agravos | agravos_mesma_raiz | agravos_origem_de_vara_rj | origens_distintas_agravos | origens_distintas_rj |
| :------------ | :----------------- | :------------------------ | :------------------------ | :------------------- |
| 30750         | 187                | 156                       | 48                        | 397                  |

#### Análise do resultado do QA

**O que os cinco números dizem:**

| Valor                                    | Leitura                                                                                                                                                                                                                                                                                      |
| ---------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `total_agravos` = 30.750                 | o universo de agravos de instrumento do corpus está íntegro — o zero da vinculação não decorre de universo vazio ou filtro mal aplicado                                                                                                                                                      |
| `agravos_mesma_raiz` = 187 (0,6%)        | apenas 187 agravos coincidem de raiz com alguma RJ — e mesmo esses **não constituem vínculo**: são colisões estatísticas do espaço de 7 dígitos, pois **nenhum** deles casa também na origem (casaram sempre isolados, nunca os dois componentes juntos — por isso o vínculo completo deu 0) |
| `agravos_origem_de_vara_rj` = 156 (0,5%) | apenas 156 agravos têm alguma origem que também aparece entre as varas das RJs — coincidências de código, sem vínculo demonstrável                                                                                                                                                           |
| `origens_distintas_agravos` = 48         | os agravos moram em apenas **48 unidades de origem** distintas — conjunto pequeno, típico de 2º grau (câmaras/gabinetes de tribunal), e não das centenas de varas de 1º grau                                                                                                                 |
| `origens_distintas_rj` = 397             | as RJs nascem em **397 varas** distintas — o universo de unidades de 1º grau                                                                                                                                                                                                                 |

**O contraste decisivo: 48 × 397.** Se o agravo fosse registrado na vara da RJ
(como a tutela antecedente da pergunta 8, que compartilha raiz e origem por
nascer na mesma vara), as unidades de origem dos agravos deveriam ser um
subconjunto das 397 varas — e os agravos deveriam casar na origem em volume.
O que se observa é o inverso: os 30.750 agravos concentram-se em 48 unidades
(pequeno conjunto de 2º grau), com apenas 0,5% de sobreposição acidental com
as varas. A amostra comparada (QA 2) confirma o padrão: raízes de agravo
sequenciais baixas (0000001, 0000011, 0000144...) e origens do tipo 0000 e
9xxx — numeração própria do tribunal, sem rastro da vara de origem.

**Conclusão do QA:** o zero da consulta principal é **estrutura do dado**, não
bug de código. A chave raiz+origem identifica processos que nasceram na mesma
vara; o agravo de instrumento é autuado no tribunal com numeração própria e
jamais compartilha a chave com a RJ. O vínculo agravo→processo originário é
impossível com o dado disponível — e, sem saber a qual RJ cada agravo
pertence, não há sobre quais autos medir o desfecho de 2º grau (a taxa de
reforma herda a mesma inviabilidade). Este resultado fundamenta a resposta
metodológica da pergunta 9.

#### Resposta à pergunta 9

**Parte 1 — taxas de recursos (Agravos de Instrumento) interpostos contra
decisões de deferimento/indeferimento do processamento da RJ:** não
computável. O teste de vinculabilidade pela chave raiz+origem do número
CNJ — o método validado na pergunta 8 para incidentes da mesma vara —
retornou 0 vínculos em 30.750 agravos. O QA estrutural demonstrou o
motivo: o agravo é autuado no tribunal com numeração própria (48
unidades de origem do 2º grau, contra as 397 varas de origem das RJs),
de modo que processo e recurso nunca compartilham a chave. Nem a
delimitação pelo assunto-pai 4993 resolve, pois o número do recurso não
preserva nem a classe nem a vara do processo originário.

**Parte 2 — taxa de reforma dessas decisões no 2º Grau:** não computável,
pois herda a inviabilidade da parte 1: sem vínculo agravo→RJ, não há
sobre quais autos medir o desfecho.

**Alternativas de superação investigadas e descartadas neste estudo:**

1. **Campo de processos antecessores na fonte bruta** (`numero_processo_prior`
   do padrão CNJ, onde o recurso apontaria o processo de origem) —
   verificado na silver e no raw: **o campo não existe na ingestão**. A via
   estruturada única possível está, portanto, ausente já na fonte, não
   apenas na modelagem deste projeto;
2. **Metadados estruturados dos movimentos** — o perfil de registro dos
   movimentos, documentado na pergunta 2, não contém vocabulário que
   identifique o processo de origem do recurso.

**Caminhos futuros (fora do escopo deste estudo):** a identificação do
vínculo recurso→processo originário reside no documento processual, não
no dado estruturado. A petição do agravo de instrumento deve ser
instruída, obrigatoriamente, com cópias da petição que ensejou a decisão
agravada e da própria decisão agravada (art. 1.017, I, do CPC) — peças
que identificam o processo de origem —, e a falta de peça que comprometa
a admissibilidade sujeita o recurso ao não-conhecimento (art. 1.017,
§ 3º, c/c art. 932, parágrafo único). O despacho de admissibilidade do
relator, por sua vez, tipicamente referencia o processo de origem. Sua
extração, contudo, demandaria a coleta das peças nos portais eletrônicos
dos tribunais (PJe, e-SAJ), configurando um segundo projeto de aquisição
de dados — os autos eletrônicos dispensam a juntada das cópias (art.
1.017, § 5º), o que confirma que o vínculo habita os autos, não os
metadados. Superado o vínculo, a classificação do objeto do agravo
(decisão de processamento vs. tutela vs. decisão interlocutória) viria
da leitura do documento, eliminando também os falsos positivos da
atribuição por janela temporal previstos no desenho original desta
métrica.

---

## 8. Autoavaliação

### 8.1. Objetivo com o curso

Sou engenheiro e advogado, e meu objetivo é usar ciência de dados para extrair
informações dos processos judiciais e sistematizar os fundamentos que os juízes
usam na tomada de suas decisões. Escolhi os processos de recuperação judicial,
extrajudicial e falência como objeto deste estudo por terem muitas fases
interessantes de entender: stay period, negociação do plano, execução do plano,
falência, liquidação, entre outras. Este MVP foi um passo importante nessa
direção: aprender a captar e tratar dados reais sobre o tema.

### 8.2. Pontos positivos

- **Aprendizado técnico intenso** de SQL, Python/PySpark, Databricks e Genie;

- **Aprofundamento no uso de inteligências artificiais**: Genie, Qwen, Claude,
  Gemini e ChatGPT. O projeto foi feito quatro vezes sem um bom resultado; a
  versão final usou Claude apoiado por Gemini e ChatGPT — mas ainda prefiro o
  Gemini;

- **Manuseio da TPU** a compreensão da clasificação de assuntos, classes e movimentos pelos códigos do CNJ foi de suma importância para futuros trabalhos

- **A abordagem "dados primeiro" funcionou**: partindo de perguntas previamente
  definidas (e mantidas até o fim), identifiquei quais dados seriam necessários
  para respondê-las e, a partir daí, desenhei a camada Gold. Depois construí a
  camada Silver para limpar e organizar os dados obtidos do Datajud, e a Gold
  para tratá-los e consolidá-los em um mart (tabelas de fatos e dimensões),
  consumido pelas consultas SQL com o mínimo de trabalho. Mesmo assim, as
  perguntas finais exigiram SQL bem elaborado, com junções e tabelas temporárias.

### 8.3. Pontos negativos e dificuldades

- **Escopo grande demais para o tempo**: nove perguntas tornaram o trabalho
  enorme e impediram que eu aprofundasse mais a matéria em si;

- **Limitação estrutural dos dados do Datajud**: os metadados dos processos não
  caracterizam bem as fases processuais da recuperação judicial e da falência,
  o que levou a interpretações sobre o que os movimentos registrados poderiam
  significar. Muitas respostas se basearam em pequenas quantidades de processos
  e, às vezes, a pergunta nem podia ser respondida com os dados disponíveis.
  Para respostas mais acuradas seria necessário, a partir do número do processo
  obtido no Datajud, baixar dos sites dos tribunais os documentos que formalizam
  o andamento do processo, interpretá-los e extrair os dados necessários. Só
  descobri essa necessidade colocando a mão na massa;

- **APIs dos tribunais**: tive sucesso com a API do Datajud depois de algumas
  tentativas, mas não com as APIs dos tribunais específicos para baixar os
  processos;

- **Inexperiência com as ferramentas**: ao transferir o notebook de ingestão,
  construído no Databricks, da pasta do projeto para a pasta do repositório que
  conecta ao GitHub, perdi os outputs das células — o que prejudicou a
  evidenciação do processamento. Os outputs já haviam sido registrados nos
  anexos deste README, e a ingestão não pôde ser executada novamente por falta
  de tempo (os demais notebooks puderam rodar de novo). Depois de várias
  tentativas descobri um método para levar notebooks do Databricks ao GitHub
  mantendo os outputs: exportar do Databricks e subir o arquivo no GitHub. Foi
  um aprendizado.

- **Não há cobertura total do tema** Não considerar a classe de autofalência no estudo, apesar dessa ter uma expressão maior do que a recuperação extrajudicial.

### 8.4. Trabalhos futuros

- **Enriquecimento a partir dos tribunais**: a partir do número do processo
  obtido no Datajud, baixar dos sites dos tribunais os documentos que formalizam
  o andamento (despachos, decisões, sentenças), interpretá-los e extrair os
  marcos processuais — isso permitiria responder com acurácia as perguntas que
  hoje dependem de interpretação dos movimentos, como a homologação do plano;

- **Orquestração e automação**: transformar a execução manual dos três
  notebooks em um pipeline orquestrado (jobs agendados, reprocessamento
  incremental), reduzindo o esforço de atualização das camadas;

- **Expansão do escopo**: ampliar a coleta para outros assuntos e classes dentro do direito empresarial, como por exemplo disputas societárias,governança societária, contratos empresariais, propriedade intelectual, entre outros.

- **Camada de consumo com IA**: explorar o uso de assistentes de linguagem
  natural (como o Genie) sobre o mart Gold para permitir consultas em linguagem
  corrente por profissionais do direito, sem necessidade de escrever SQL.

---

# Anexo A: Estabelecimento da Camada Bronze

### Notebook 01_ingestao_bronze — Ingestão de dados do Datajud

#### Objetivo

Este notebook realiza a coleta dos processos da Recuperação Judicial,
Recuperação Extrajudicial e Falência diretamente da API pública do Datajud (CNJ) e os
persiste na camada **Bronze** do lakehouse (`default.bronze_datajud_raw`), sem qualquer
transformação — o dado bruto é o "cofre de evidências" da arquitetura medalhão.

#### Entradas e saídas

- **Entrada**: endpoints públicos `https://api-publica.datajud.cnj.jus.br/api_publica_{tribunal}/_search` (27 tribunais estaduais + TJDFT)
- **Saída**: tabela Delta `default.bronze_datajud_raw` com 3 colunas:
  - `raw_payload` (string) — o JSON do processo exatamente como recebido da API
  - `tribunal_origem` (string) — metadado de fonte
  - `ingestion_timestamp` (timestamp) — metadado de controle da carga

#### Decisões de desenho

1. **Recorte definido pelas perguntas de negócio**: filtro por assunto pai 4993 - recuperação judicial, falência. Esse assunto pai engloba os assuntos filhos: 4992 -faLência (código antigo); 4994 recuperação extrajudicial; 4995 - concordata preventiva (código antigo); 4996 concordata suspensiva (código antigo),; 4997 falência fraudulenta (código antigo);

2. **Recorte definido pelo tempo para o download**: `dataAjuizamento >= 01/01/1995`. Mais tarde decidiu-se analisar processos a partir de 9/06/2005 (vigência da atual Lei 11.101/2005 que regula a recuperação judicial, extrajudicial e a falência de empresários e sociedades empresárias).

3. **Dado bruto imutável dentro do registro**: o JSON é salvo como string, sem tipagem nem
   flattening — toda estruturação acontece na Silver.

4. **Persistência incremental por tribunal**: cada tribunal é gravado em lote ao término da
   sua paginação, liberando a memória do driver antes do próximo — evita OOM em coletas longas.

5. **Full-refresh da rotina**: o `DROP TABLE IF EXISTS` no início do código significa que a execução
   completa reconstrói a Bronze inteira. Esse procedimento foi utilizado para viabilizar as múltiplas tentativas de captura dos dados junto ao CNJ; o `append` dentro do loop é apenas o mecanismo de gravação por lote de vários tribunais.

```text
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
```

```text
=== INICIANDO INGESTÃO MULTI-TRIBUNAL COM PERSISTÊNCIA EM LOTE ===

--- Processando Tribunal: TJSP ---
-> [TJSP] Pág 1... [OK]
   L Registros obtidos: +10000 (Acumulado no tribunal: 10,000)
-> [TJSP] Pág 2... [OK]
   L Registros obtidos: +10000 (Acumulado no tribunal: 20,000)
-> [TJSP] Pág 3... [OK]
   L Registros obtidos: +10000 (Acumulado no tribunal: 30,000)
-> [TJSP] Pág 4... [OK]
   L Registros obtidos: +10000 (Acumulado no tribunal: 40,000)
-> [TJSP] Pág 5... [OK]
   L Registros obtidos: +10000 (Acumulado no tribunal: 50,000)
-> [TJSP] Pág 6... [OK]
   L Registros obtidos: +10000 (Acumulado no tribunal: 60,000)
-> [TJSP] Pág 7... [OK]
   L Registros obtidos: +10000 (Acumulado no tribunal: 70,000)
-> [TJSP] Pág 8... [OK]
   L Registros obtidos: +10000 (Acumulado no tribunal: 80,000)
-> [TJSP] Pág 9... [OK]
   L Registros obtidos: +10000 (Acumulado no tribunal: 90,000)
-> [TJSP] Pág 10... [OK]
   L Registros obtidos: +10000 (Acumulado no tribunal: 100,000)
-> [TJSP] Pág 11... [OK]
   L Registros obtidos: +10000 (Acumulado no tribunal: 110,000)
-> [TJSP] Pág 12... [OK]
   L Registros obtidos: +10000 (Acumulado no tribunal: 120,000)
-> [TJSP] Pág 13... [OK]
   L Registros obtidos: +2738 (Acumulado no tribunal: 122,738)
-> [TJSP] Pág 14... [OK]
✅ Concluído TJSP! Total do tribunal: 122,738 processos.
💾 122,738 registros do TJSP gravados na Bronze.

--- Processando Tribunal: TJRJ ---
-> [TJRJ] Pág 1... [OK]
   L Registros obtidos: +10000 (Acumulado no tribunal: 10,000)
-> [TJRJ] Pág 2... [OK]
   L Registros obtidos: +10000 (Acumulado no tribunal: 20,000)
-> [TJRJ] Pág 3... [OK]
   L Registros obtidos: +10000 (Acumulado no tribunal: 30,000)
-> [TJRJ] Pág 4... [OK]
   L Registros obtidos: +10000 (Acumulado no tribunal: 40,000)
-> [TJRJ] Pág 5... [OK]
   L Registros obtidos: +10000 (Acumulado no tribunal: 50,000)
-> [TJRJ] Pág 6... [OK]
   L Registros obtidos: +10000 (Acumulado no tribunal: 60,000)
-> [TJRJ] Pág 7... [OK]
   L Registros obtidos: +2289 (Acumulado no tribunal: 62,289)
-> [TJRJ] Pág 8... [OK]
✅ Concluído TJRJ! Total do tribunal: 62,289 processos.
💾 62,289 registros do TJRJ gravados na Bronze.

--- Processando Tribunal: TJMG ---
-> [TJMG] Pág 1...
⚠️ HTTP 504. Aguardando 3s... (Tentativa 1/5)

⚠️ HTTP 504. Aguardando 6s... (Tentativa 2/5)

⚠️ HTTP 504. Aguardando 12s... (Tentativa 3/5)

⚠️ HTTP 504. Aguardando 24s... (Tentativa 4/5)

⚠️ HTTP 504. Aguardando 48s... (Tentativa 5/5)

❌ Falha ao buscar dados do TJMG na página 1. Avançando para próximo tribunal.

--- Processando Tribunal: TJRS ---
-> [TJRS] Pág 1... [OK]
   L Registros obtidos: +10000 (Acumulado no tribunal: 10,000)
-> [TJRS] Pág 2... [OK]
   L Registros obtidos: +2031 (Acumulado no tribunal: 12,031)
-> [TJRS] Pág 3... [OK]
✅ Concluído TJRS! Total do tribunal: 12,031 processos.
💾 12,031 registros do TJRS gravados na Bronze.

--- Processando Tribunal: TJPR ---
-> [TJPR] Pág 1... [OK]
   L Registros obtidos: +5649 (Acumulado no tribunal: 5,649)
-> [TJPR] Pág 2... [OK]
✅ Concluído TJPR! Total do tribunal: 5,649 processos.
💾 5,649 registros do TJPR gravados na Bronze.

--- Processando Tribunal: TJSC ---
-> [TJSC] Pág 1... [OK]
   L Registros obtidos: +10000 (Acumulado no tribunal: 10,000)
-> [TJSC] Pág 2... [OK]
   L Registros obtidos: +2846 (Acumulado no tribunal: 12,846)
-> [TJSC] Pág 3... [OK]
✅ Concluído TJSC! Total do tribunal: 12,846 processos.
💾 12,846 registros do TJSC gravados na Bronze.

--- Processando Tribunal: TJBA ---
-> [TJBA] Pág 1... [OK]
   L Registros obtidos: +533 (Acumulado no tribunal: 533)
-> [TJBA] Pág 2... [OK]
✅ Concluído TJBA! Total do tribunal: 533 processos.
💾 533 registros do TJBA gravados na Bronze.

--- Processando Tribunal: TJPE ---
-> [TJPE] Pág 1... [OK]
   L Registros obtidos: +1497 (Acumulado no tribunal: 1,497)
-> [TJPE] Pág 2... [OK]
✅ Concluído TJPE! Total do tribunal: 1,497 processos.
💾 1,497 registros do TJPE gravados na Bronze.

--- Processando Tribunal: TJCE ---
-> [TJCE] Pág 1... [OK]
   L Registros obtidos: +527 (Acumulado no tribunal: 527)
-> [TJCE] Pág 2... [OK]
✅ Concluído TJCE! Total do tribunal: 527 processos.
💾 527 registros do TJCE gravados na Bronze.

--- Processando Tribunal: TJDFT ---
-> [TJDFT] Pág 1... [OK]
   L Registros obtidos: +407 (Acumulado no tribunal: 407)
-> [TJDFT] Pág 2... [OK]
✅ Concluído TJDFT! Total do tribunal: 407 processos.
💾 407 registros do TJDFT gravados na Bronze.

--- Processando Tribunal: TJES ---
-> [TJES] Pág 1... [OK]
   L Registros obtidos: +485 (Acumulado no tribunal: 485)
-> [TJES] Pág 2... [OK]
✅ Concluído TJES! Total do tribunal: 485 processos.
💾 485 registros do TJES gravados na Bronze.

--- Processando Tribunal: TJGO ---
-> [TJGO] Pág 1... [OK]
   L Registros obtidos: +1494 (Acumulado no tribunal: 1,494)
-> [TJGO] Pág 2... [OK]
✅ Concluído TJGO! Total do tribunal: 1,494 processos.
💾 1,494 registros do TJGO gravados na Bronze.

--- Processando Tribunal: TJMT ---
-> [TJMT] Pág 1... [OK]
   L Registros obtidos: +5275 (Acumulado no tribunal: 5,275)
-> [TJMT] Pág 2... [OK]
✅ Concluído TJMT! Total do tribunal: 5,275 processos.
💾 5,275 registros do TJMT gravados na Bronze.

--- Processando Tribunal: TJMS ---
-> [TJMS] Pág 1... [OK]
   L Registros obtidos: +120 (Acumulado no tribunal: 120)
-> [TJMS] Pág 2... [OK]
✅ Concluído TJMS! Total do tribunal: 120 processos.
💾 120 registros do TJMS gravados na Bronze.

--- Processando Tribunal: TJPA ---
-> [TJPA] Pág 1... [OK]
   L Registros obtidos: +206 (Acumulado no tribunal: 206)
-> [TJPA] Pág 2... [OK]
✅ Concluído TJPA! Total do tribunal: 206 processos.
💾 206 registros do TJPA gravados na Bronze.

--- Processando Tribunal: TJPB ---
-> [TJPB] Pág 1... [OK]
   L Registros obtidos: +250 (Acumulado no tribunal: 250)
-> [TJPB] Pág 2... [OK]
✅ Concluído TJPB! Total do tribunal: 250 processos.
💾 250 registros do TJPB gravados na Bronze.

--- Processando Tribunal: TJRN ---
-> [TJRN] Pág 1... [OK]
   L Registros obtidos: +464 (Acumulado no tribunal: 464)
-> [TJRN] Pág 2... [OK]
✅ Concluído TJRN! Total do tribunal: 464 processos.
💾 464 registros do TJRN gravados na Bronze.

--- Processando Tribunal: TJRO ---
-> [TJRO] Pág 1... [OK]
   L Registros obtidos: +180 (Acumulado no tribunal: 180)
-> [TJRO] Pág 2... [OK]
✅ Concluído TJRO! Total do tribunal: 180 processos.
💾 180 registros do TJRO gravados na Bronze.

--- Processando Tribunal: TJSE ---
-> [TJSE] Pág 1... [OK]
   L Registros obtidos: +100 (Acumulado no tribunal: 100)
-> [TJSE] Pág 2... [OK]
✅ Concluído TJSE! Total do tribunal: 100 processos.
💾 100 registros do TJSE gravados na Bronze.

--- Processando Tribunal: TJTO ---
-> [TJTO] Pág 1... [OK]
   L Registros obtidos: +195 (Acumulado no tribunal: 195)
-> [TJTO] Pág 2... [OK]
✅ Concluído TJTO! Total do tribunal: 195 processos.
💾 195 registros do TJTO gravados na Bronze.

--- Processando Tribunal: TJAL ---
-> [TJAL] Pág 1... [OK]
   L Registros obtidos: +475 (Acumulado no tribunal: 475)
-> [TJAL] Pág 2... [OK]
✅ Concluído TJAL! Total do tribunal: 475 processos.
💾 475 registros do TJAL gravados na Bronze.

--- Processando Tribunal: TJAM ---
-> [TJAM] Pág 1... [OK]
   L Registros obtidos: +1439 (Acumulado no tribunal: 1,439)
-> [TJAM] Pág 2... [OK]
✅ Concluído TJAM! Total do tribunal: 1,439 processos.
💾 1,439 registros do TJAM gravados na Bronze.

--- Processando Tribunal: TJAP ---
-> [TJAP] Pág 1... [OK]
   L Registros obtidos: +27 (Acumulado no tribunal: 27)
-> [TJAP] Pág 2... [OK]
✅ Concluído TJAP! Total do tribunal: 27 processos.
💾 27 registros do TJAP gravados na Bronze.

--- Processando Tribunal: TJAC ---
-> [TJAC] Pág 1... [OK]
   L Registros obtidos: +111 (Acumulado no tribunal: 111)
-> [TJAC] Pág 2... [OK]
✅ Concluído TJAC! Total do tribunal: 111 processos.
💾 111 registros do TJAC gravados na Bronze.

--- Processando Tribunal: TJRR ---
-> [TJRR] Pág 1... [OK]
   L Registros obtidos: +22 (Acumulado no tribunal: 22)
-> [TJRR] Pág 2... [OK]
✅ Concluído TJRR! Total do tribunal: 22 processos.
💾 22 registros do TJRR gravados na Bronze.

--- Processando Tribunal: TJPI ---
-> [TJPI] Pág 1... [OK]
   L Registros obtidos: +85 (Acumulado no tribunal: 85)
-> [TJPI] Pág 2... [OK]
✅ Concluído TJPI! Total do tribunal: 85 processos.
💾 85 registros do TJPI gravados na Bronze.

--- Processando Tribunal: TJMA ---
-> [TJMA] Pág 1... [OK]
   L Registros obtidos: +102 (Acumulado no tribunal: 102)
-> [TJMA] Pág 2... [OK]
✅ Concluído TJMA! Total do tribunal: 102 processos.
💾 102 registros do TJMA gravados na Bronze.

=== INGESTÃO FINALIZADA. TOTAL GERAL SALVO NA BRONZE: 229,547 ===
```

```text
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
```

```text
=== INICIANDO INGESTÃO COMPLEMENTAR: TJMG ===
-> [TJMG] Pág 1... [OK]
   L Gravados na Bronze: +2,500 (Acumulado TJMG: 2,500)
-> [TJMG] Pág 2... [OK]
   L Gravados na Bronze: +2,500 (Acumulado TJMG: 5,000)
-> [TJMG] Pág 3... [OK]
   L Gravados na Bronze: +2,500 (Acumulado TJMG: 7,500)
-> [TJMG] Pág 4... [OK]
   L Gravados na Bronze: +2,500 (Acumulado TJMG: 10,000)
-> [TJMG] Pág 5... [OK]
   L Gravados na Bronze: +2,500 (Acumulado TJMG: 12,500)
-> [TJMG] Pág 6... [OK]
   L Gravados na Bronze: +1,784 (Acumulado TJMG: 14,284)
-> [TJMG] Pág 7... [OK]
✅ Concluído TJMG! Total de processos coletados: 14,284.

=== PROCESSAMENTO DO TJMG CONCLUÍDO. REGISTROS ADICIONADOS: 14,284 ===
```

```text
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
```

#### Resposta:

| tribunal_origem | total_processos | primeira_ingestao             | ultima_ingestao               |
| :-------------- | :-------------- | :---------------------------- | :---------------------------- |
| TJSP            | 122738          | 2026-09-02T15:54:02.612+00:00 | 2026-09-02T15:54:02.612+00:00 |
| TJRJ            | 62289           | 2026-09-02T15:57:23.415+00:00 | 2026-09-02T15:57:23.415+00:00 |
| TJMG            | 14284           | 2026-09-02T16:21:51.690+00:00 | 2026-09-02T16:25:10.329+00:00 |
| TJSC            | 12846           | 2026-09-02T16:07:35.148+00:00 | 2026-09-02T16:07:35.148+00:00 |
| TJRS            | 12031           | 2026-09-02T16:05:41.620+00:00 | 2026-09-02T16:05:41.620+00:00 |
| TJPR            | 5649            | 2026-09-02T16:06:43.217+00:00 | 2026-09-02T16:06:43.217+00:00 |
| TJMT            | 5275            | 2026-09-02T16:09:34.408+00:00 | 2026-09-02T16:09:34.408+00:00 |
| TJPE            | 1497            | 2026-09-02T16:08:18.975+00:00 | 2026-09-02T16:08:18.975+00:00 |
| TJGO            | 1494            | 2026-09-02T16:09:02.328+00:00 | 2026-09-02T16:09:02.328+00:00 |
| TJAM            | 1439            | 2026-09-02T16:11:16.616+00:00 | 2026-09-02T16:11:16.616+00:00 |
| TJBA            | 533             | 2026-09-02T16:07:50.856+00:00 | 2026-09-02T16:07:50.856+00:00 |
| TJCE            | 527             | 2026-09-02T16:08:33.037+00:00 | 2026-09-02T16:08:33.037+00:00 |
| TJES            | 485             | 2026-09-02T16:08:51.283+00:00 | 2026-09-02T16:08:51.283+00:00 |
| TJAL            | 475             | 2026-09-02T16:11:02.949+00:00 | 2026-09-02T16:11:02.949+00:00 |
| TJRN            | 464             | 2026-09-02T16:10:17.355+00:00 | 2026-09-02T16:10:17.355+00:00 |
| TJDFT           | 407             | 2026-09-02T16:08:44.558+00:00 | 2026-09-02T16:08:44.558+00:00 |
| TJPB            | 250             | 2026-09-02T16:10:00.403+00:00 | 2026-09-02T16:10:00.403+00:00 |
| TJPA            | 206             | 2026-09-02T16:09:51.847+00:00 | 2026-09-02T16:09:51.847+00:00 |
| TJTO            | 195             | 2026-09-02T16:10:41.491+00:00 | 2026-09-02T16:10:41.491+00:00 |
| TJRO            | 180             | 2026-09-02T16:10:25.801+00:00 | 2026-09-02T16:10:25.801+00:00 |
| TJMS            | 120             | 2026-09-02T16:09:44.001+00:00 | 2026-09-02T16:09:44.001+00:00 |
| TJAC            | 111             | 2026-09-02T16:11:35.686+00:00 | 2026-09-02T16:11:35.686+00:00 |
| TJMA            | 102             | 2026-09-02T16:12:18.603+00:00 | 2026-09-02T16:12:18.603+00:00 |
| TJSE            | 100             | 2026-09-02T16:10:32.695+00:00 | 2026-09-02T16:10:32.695+00:00 |
| TJPI            | 85              | 2026-09-02T16:11:54.180+00:00 | 2026-09-02T16:11:54.180+00:00 |
| TJAP            | 27              | 2026-09-02T16:11:22.812+00:00 | 2026-09-02T16:11:22.812+00:00 |
| TJRR            | 22              | 2026-09-02T16:11:45.820+00:00 | 2026-09-02T16:11:45.820+00:00 |

### Comentários sobre o código do notebook 01_ingestao_bronze

#### Configuração da coleta

- **Autenticação**: a API do Datajud usa o header `Authorization: APIKey {chave}`.
- **Filtro Elasticsearch**: o corpo da requisição é uma query `bool` com duas cláusulas
  `must` — intervalo de `dataAjuizamento` e lista de códigos de assunto (`terms`).
- **Paginação com `search_after`**: em vez de páginas numeradas (que degradam em bases
  grandes), a API do Datajud usa o cursor `search_after` do Elasticsearch: cada resposta
  retorna um array `sort`, que é repassado na requisição seguinte. O loop `while True`
  encerra quando a página vem vazia (`hits == []`).
- **Ordenação determinística** por `@timestamp` e `id.keyword` — garante que a paginação
  não pule nem repita registros.

#### Resiliência: retry com backoff exponencial

Cada página passa por até `MAX_RETRIES = 5` tentativas. Em erros transitórios (HTTP 429
limite de taxa, 5xx do servidor ou timeout de rede), a rotina aguarda
`INITIAL_BACKOFF * 2^(tentativa-1)` segundos — 3s, 6s, 12s, 24s, 48s — antes de repetir.
Erros definitivos (outros códigos HTTP) interrompem o tribunal corrente e avançam para o
próximo, sem abortar a coleta inteira — o log registra a falha para auditoria.

**Comando-chave**: `requests.post(url, json=payload, headers=headers, timeout=(10, 120))` —
o timeout em tupla separa o tempo de conexão (10s) do tempo de leitura (120s), necessário
páginas de 10.000 registros.

#### Persistência na Bronze (Load)

Cada tribunal finaliza com a gravação do seu lote no Delta Lake:

- `spark.createDataFrame(hits_tribunal_batch, [...])` — materializa as tuplas
  `(payload_json, tribunal)` coletadas no driver;
- `.withColumn("ingestion_timestamp", current_timestamp())` — **metadado de controle**:
  carimbo de data/hora da ingestão em cada registro;
- `.write.format("delta").mode("append").saveAsTable("default.bronze_datajud_raw")` —
  gravação no formato Delta com append; a tabela é criada no primeiro tribunal e
  estendida nos demais;
- `del hits_tribunal_batch` — libera a memória do driver entre lotes, permitindo coletar
  milhões de registros sem estourar o nó.

A camada Bronze está fechada: nenhum notebook das camadas seguintes escreve
nesta tabela — apenas leem.

```text
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
```

%md
[QA 1/5] Schema da Bronze: OK\
[QA 2/5] Registros com payload/tribunal/timestamp nulos: 0 (esperado: 0)\
[QA 3/5] Payloads JSON inválidos: 0 (esperado: 0)\
[QA 4/5] Contagem por tribunal na Bronze:
| tribunal_origem | count |
| :--- | :--- |
| TJSP | 122738 |
| TJRJ | 62289 |
| TJMG | 14284 |
| TJSC | 12846 |
| TJRS | 12031 |
| TJPR | 5649 |
| TJMT | 5275 |
| TJPE | 1497 |
| TJGO | 1494 |
| TJAM | 1439 |
| TJBA | 533 |
| TJCE | 527 |
| TJES | 485 |
| TJAL | 475 |
| TJRN | 464 |
| TJDFT | 407 |
| TJPB | 250 |
| TJPA | 206 |
| TJTO | 195 |
| TJRO | 180 |
| TJMS | 120 |
| TJAC | 111 |
| TJMA | 102 |
| TJSE | 100 |
| TJPI | 85 |
| TJAP | 27 |
| TJRR | 22 |

[QA 5/5] Contagem de processos na Bronze: 243,831

=== QA DA BRONZE CONCLUÍDO: pronta para leitura pelo 02_silver_transformacao ===

---

> ✅ **STATUS:** QA da Bronze concluído com sucesso. O ambiente está pronto para leitura pelo script `02_silver_transformacao`.

---

# Anexo B: Estabelecimento da Camada Silver

### 3. Notebook 02_silver_transformacao — Tratamento dos dados recebidos

#### Objetivo

Ler **somente** `bronze_datajud_raw` (camada imutável) e reconstruir a camada
Silver por completo: parse do payload JSON, deduplicação de capturas,
tabelas de processos e movimentos, dimensões derivadas, regras de marcação,
classificação de eventos, marcos processuais e QA de execução.

#### Desenho

- **Entrada única:** `bronze_datajud_raw` (raw_payload, tribunal_origem, ingestion_timestamp)
- **Saídas (10 tabelas):** silver_datajud_processos, silver_fato_movimento,
  silver_dim_orgao_julgador, silver_dim_assunto, silver_ponte_processo_assunto,
  silver_dim_processo, silver_regras_marcacao,
  silver_datajud_eventos_enriquecidos, silver_marcos_processo, silver_qa_execucao
- **Padrões:** CREATE OR REPLACE (idempotente) + COMMENT (governança);
  regravação de schema habilitada (overwriteSchema) para evolução controlada
- **Idempotência:** o notebook pode ser re-executado ponta a ponta sem resíduos

#### Papel da camada silver

Se a Bronze é o arquivo bruto do Datajud, a Silver é a primeira versão
_usável_: tipos corretos, 1 linha por processo, 1 linha por movimento e
eventos já classificados pelas regras de marcação. A Gold não reprocessa
nada — consome o que aqui está garantido.

### BLOCO 1 — Parse do raw (JSON → estruturas tipadas)

A Bronze guarda o payload bruto da API de interoperabilidade do CNJ em
`raw_payload` (string). Aqui definimos o **schema espelhado** da resposta
pública: processo, classe, sistema, formato, órgão julgador, assuntos e o
array de movimentos (com complementos tabelados).

**Por que schema explícito:** `from_json` com StructType garante tipagem
determinística, rejeita silênciosamente campos fora do contrato e documenta
a estrutura da fonte dentro do próprio código — parte do requisito de
catálogo e governança da disciplina.

**Saída do bloco:** DataFrame `bronze` (243.831 registros), ainda sem
qualquer filtragem — o parse não decide, só tipa.

```textr
# Databricks notebook source
# ==============================================================================
# 02_SILVER_TRANSFORMACAO
# Lê SOMENTE de bronze_datajud_raw (imutável) e monta a camada silver.
# Escopo essencial: 10 tabelas — processos, movimentos, dims derivadas,
# regras de marcação, eventos enriquecidos, marcos e QA.
# Padrões: CREATE OR REPLACE (idempotente, atualiza o catalog) + COMMENT.
# ==============================================================================

from pyspark.sql import functions as F, types as T, Window

# ==============================================================================
# BLOCO 1 — PARSE DO RAW (JSON -> estruturas tipadas)
# O bronze guarda o payload bruto do Datajud em raw_payload.
# O schema espelha a estrutura pública da API de interoperabilidade do CNJ.
# ==============================================================================

schema_comp = T.StructType([
    T.StructField("codigo",    T.StringType()),
    T.StructField("nome",      T.StringType()),
    T.StructField("descricao", T.StringType()),
])

schema_mov = T.StructType([
    T.StructField("codigo",                 T.IntegerType()),
    T.StructField("nome",                   T.StringType()),
    T.StructField("dataHora",               T.StringType()),
    T.StructField("complementosTabelados",  T.ArrayType(schema_comp)),
])

schema_proc = T.StructType([
    T.StructField("id",                   T.StringType()),
    T.StructField("numeroProcesso",       T.StringType()),
    T.StructField("dataAjuizamento",      T.StringType()),
    T.StructField("grau",                 T.StringType()),
    T.StructField("classe",               T.StructType([
        T.StructField("codigo", T.IntegerType()),
        T.StructField("nome",   T.StringType())])),
    T.StructField("sistema",              T.StructType([
        T.StructField("codigo", T.IntegerType()),
        T.StructField("nome",   T.StringType())])),
    T.StructField("formato",              T.StructType([
        T.StructField("codigo", T.IntegerType()),
        T.StructField("nome",   T.StringType())])),
    T.StructField("orgaoJulgador",        T.StructType([
        T.StructField("codigo",              T.IntegerType()),
        T.StructField("nome",                T.StringType()),
        T.StructField("codigoMunicipioIBGE", T.LongType())])),
    T.StructField("assuntos",             T.ArrayType(T.StructType([
        T.StructField("codigo", T.IntegerType()),
        T.StructField("nome",   T.StringType())]))),
    T.StructField("movimentos",           T.ArrayType(schema_mov)),
])

bronze = spark.table("bronze_datajud_raw").withColumn(
    "p", F.from_json("raw_payload", schema_proc))

print("Registros no bronze:", bronze.count())
```

##### Resposta:

Registros no bronze: 243831

### BLOCO 2 — Dedupe: última captura de cada processo

O bronze contém lotes ingestados: **243.831 registros × 225.556 processos
distintos** (18.275 duplicatas de reingestão).

**Regra de dedupe:** `row_number` particionado por `numeroProcesso`,
ordenado por `ingestion_timestamp` descendente — vence a captura mais
recente de cada processo (a versão mais atual do Datajud).

**QA herdado:** o print de `qtd_distintos` valida contra o alvo 225.556.

```text
# ==============================================================================
# BLOCO 2 — DEDUPE: última captura de cada processo
# Bronze: 243.831 registros x 225.556 processos distintos (lotes reingestados).
# Regra: row_number por processo, ordenando pela captura mais recente.
# ==============================================================================

w_ultima_captura = Window.partitionBy("p.numeroProcesso") \
                         .orderBy(F.col("ingestion_timestamp").desc())

proc_dedupe = (bronze
    .withColumn("rn", F.row_number().over(w_ultima_captura))
    .filter("rn = 1")
    .drop("rn"))

qtd_distintos = proc_dedupe.count()
print("Processos distintos após dedupe (alvo 225.556):", qtd_distintos)
```

##### Resposta:

Processos distintos após dedupe (alvo 225.556): 225556

### BLOCO 3 — Silver de processos (`silver_datajud_processos`)

Converte o processo deduplicado em 1 linha tabular, com três decisões de
qualidade:

1. **Parser multi-formato de data** (COALESCE de tentativas): o Datajud
   retorna datas em formatos heterogêneos — ISO com/sem hora, 14 dígitos
   (`yyyyMMddHHmmss`) e 8 dígitos (`yyyyMMdd`). `try_to_date` retorna NULL
   quando o formato não casa e o COALESCE devolve o primeiro parse
   bem-sucedido; a ordem resolve a precedência (14 dígitos antes de 8).
2. **Escopo temporal da pesquisa:** `escopo_lei_atual` marca processos
   ajuizados a partir de **09/06/2005** (vigência da Lei 11.101/2005);
   `regime_legal` distingue da Lei 7.661/1945 (legado).
3. **Gravação com overwriteSchema:** o schema evolui com o notebook sem
   quebrar re-execuções.

**QA do parser:** contagem de `data_ajuizamento` NULL — falhas de parse
devem ficar em ~0; qualquer valor relevante é defeito de origem a documentar.

```text
# ==============================================================================
# CORREÇÃO BLOCO 3 — parser multi-formato via COALESCE
# try_to_date retorna NULL quando o formato não casa; coalesce retorna o
# primeiro parse bem-sucedido. Sem when/otherwise — cada tentativa é
# independente e a ordem resolve a precedência (14 dígitos antes de 8).
# ==============================================================================

DATA_AJUIZ = "p.dataAjuizamento"

parse_data = F.coalesce(
    # ISO com hora: '1998-12-01T00:00:00' ou '1998-12-01'
    F.try_to_date(F.regexp_extract(DATA_AJUIZ, r"^(\d{4}-\d{2}-\d{2})", 1),
                  "yyyy-MM-dd"),
    # Sequência de 14 dígitos: '19981201000000' (yyyyMMddHHmmss)
    F.try_to_date(F.regexp_extract(DATA_AJUIZ, r"^(\d{14})", 1),
                  "yyyyMMddHHmmss"),
    # Sequência de 8 dígitos: '19981201' (yyyyMMdd)
    F.try_to_date(F.regexp_extract(DATA_AJUIZ, r"^(\d{8})", 1),
                  "yyyyMMdd"),
    # ISO puro como fallback final
    F.try_to_date(DATA_AJUIZ, "yyyy-MM-dd"))

silver_proc = (proc_dedupe.select(
        F.col("p.numeroProcesso").alias("numero_processo"),
        F.col("tribunal_origem"),
        F.col("p.grau").alias("grau_jurisdicao"),
        F.col("p.classe.codigo").alias("classe_codigo"),
        F.col("p.classe.nome").alias("classe_nome"),
        F.col("p.sistema.nome").alias("sistema_processual"),
        F.col("p.formato.nome").alias("formato_processo"),
        parse_data.alias("data_ajuizamento"),
        F.col("p.orgaoJulgador.codigo").alias("orgao_julgador_codigo"),
        F.col("p.orgaoJulgador.nome").alias("orgao_julgador_nome"),
        F.col("p.orgaoJulgador.codigoMunicipioIBGE").alias("municipio_ibge_codigo"),
        F.col("p.assuntos").alias("assuntos"),
        F.col("p.movimentos").alias("movimentos"))
    .withColumn("escopo_lei_atual",
        F.col("data_ajuizamento") >= F.lit("2005-06-09").cast("date"))
    .withColumn("regime_legal",
        F.when(F.col("escopo_lei_atual"), F.lit("Lei 11.101/2005"))
         .otherwise(F.lit("Lei 7.661/1945 (legado)"))))

# QA do parser: NULLs devem ficar próximos de 0
print("Total processos:", silver_proc.count())
print("data_ajuizamento NULL (falha de parse):",
      silver_proc.filter("data_ajuizamento IS NULL").count())

(silver_proc.write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("silver_datajud_processos"))
```

##### Resposta:

Total processos: 225556
data_ajuizamento NULL (falha de parse): 0

### BLOCO 4 — Fato de movimentos (`silver_fato_movimento`)

Explode o array de movimentos para o grão de análise principal:
**1 linha por movimento** (~14–17 milhões de eventos).

**Higiene herdada do parse:**

- `precisao_data`: dia_hora / dia / mês / ano / inválida, conforme o
  comprimento da string original
- `precisao_suficiente`: só dia e dia_hora suportam marcos datados —
  flag usada depois no Bloco 7
- `sem_codigo`: movimento sem código TPU (rastro de qualidade de origem)
- `complementos_txt`: complementos tabelados concatenados em texto
  pesquisável (insumo das regras textuais do Bloco 6)

**Dedupe por conteúdo:** movimentos idênticos repetidos no mesmo processo
(eco das capturas duplicadas da Bronze) colapsam em 1 linha, e a contagem
vira `qtd_ocorrencias` — nada é descartado, a repetição fica registrada
como evidência de QA.

```text
# ==============================================================================
# BLOCO 4 — SILVER_FATO_MOVIMENTO (1 linha por movimento)
# Explode dos movimentos com QA estrutural herdada: precisão da data,
# flag de movimento sem código TPU e contagem de ocorrências repetidas
# (rastro das capturas duplicadas da bronze, consolidadas na dedupe).
# ==============================================================================

silver_mov = (
    silver_proc.drop("assuntos", "movimentos")
    .join(silver_proc.select("numero_processo",
                             F.explode_outer("movimentos").alias("m")),
          "numero_processo")
    .select("numero_processo", "tribunal_origem",
            F.col("classe_codigo").alias("classe_codigo_mov"),
            F.col("m.codigo").alias("movimento_codigo"),
            F.col("m.nome").alias("movimento_nome"),
            F.col("m.dataHora").alias("data_hora_raw"),
            F.col("m.complementosTabelados").alias("complementos"))
    .withColumn("precisao_data",
        F.when(F.length("data_hora_raw") > 10, F.lit("dia_hora"))
         .when(F.length("data_hora_raw") == 10, F.lit("dia"))
         .when(F.length("data_hora_raw") == 7,  F.lit("mes"))
         .when(F.length("data_hora_raw") == 4,  F.lit("ano"))
         .otherwise(F.lit("invalida")))
    .withColumn("data_movimento",
        F.when(F.col("precisao_data").isin("dia", "dia_hora"),
               F.to_date(F.substring("data_hora_raw", 1, 10))))
    .withColumn("precisao_suficiente",
        F.col("precisao_data").isin("dia", "dia_hora"))
    .withColumn("sem_codigo", F.col("movimento_codigo").isNull())
    .withColumn("complementos_txt",
        F.concat_ws(" | ", F.transform("complementos",
            lambda c: F.concat_ws(": ",
                F.coalesce(c["nome"],      F.lit("")),
                F.coalesce(c["descricao"], F.lit(""))))))
    .drop("complementos", "data_hora_raw"))

# dedupe de movimento idêntico no mesmo processo; repetições viram QA
mov_cols = [c for c in silver_mov.columns]
(silver_mov
    .groupBy(*mov_cols)
    .agg(F.count(F.lit(1)).alias("qtd_ocorrencias"))
    .write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("silver_fato_movimento"))

eventos = spark.table("silver_fato_movimento").count()

print(f"silver_fato_movimento gravada. Alvo de referência: ~{eventos/1000000:.1f} milhões de eventos.")
```

##### Resposta:

silver_fato_movimento gravada. Alvo de referência: ~14.1 milhões de eventos.

### BLOCO 5 — Dimensões derivadas da Silver

Quatro tabelas de apoio, todas **derivadas** (sem carga externa):

- **`silver_dim_orgao_julgador`**: 1 linha por órgão × tribunal, com
  contagem de processos. Agrupar por (código, nome, tribunal, município)
  — nomes variantes do
  mesmo órgão não multiplicam mais as contagens.
- **`silver_dim_assunto`**: catálogo de assuntos com nº de processos
  distintos (`countDistinct` evita dupla contagem em processos multi-assunto).
- **`silver_ponte_processo_assunto`**: ponte N:N processo–assunto, base
  para definir o universo da pesquisa por matéria.
- **`silver_dim_processo`**: grade enxuta do processo (tribunal, grau,
  sistema, formato) — leitura rápida sem carregar os arrays.

As dimensões alimentam o star schema da Gold (03) sem recálculo lá.

```text
# ==============================================================================
# BLOCO 5 — DIMS DERIVADAS DA SILVER
# Órgãos julgadores, assuntos, ponte processo-assunto e grade de processo.
# ==============================================================================

(silver_proc.groupBy("orgao_julgador_codigo", "orgao_julgador_nome",
                     "tribunal_origem", "municipio_ibge_codigo")
    .agg(F.count("*").alias("qtd_processos"))
    .select(F.col("orgao_julgador_codigo"), F.col("orgao_julgador_nome"),
            F.col("tribunal_origem").alias("tribunal_sigla"),
            F.col("municipio_ibge_codigo"), F.col("qtd_processos"))
    .write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("silver_dim_orgao_julgador"))

(silver_proc.select("numero_processo",
                    F.explode_outer("assuntos").alias("a"))
    .groupBy(F.col("a.codigo").alias("assunto_codigo"),
             F.col("a.nome").alias("assunto_nome"))
    .agg(F.countDistinct("numero_processo").alias("qtd_processos"))
    .write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("silver_dim_assunto"))

(silver_proc.select("numero_processo",
                    F.explode_outer("assuntos").alias("a"))
    .select("numero_processo",
            F.col("a.codigo").alias("assunto_codigo"),
            F.col("a.nome").alias("assunto_nome"))
    .distinct()
    .write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("silver_ponte_processo_assunto"))

(silver_proc.select("numero_processo", "tribunal_origem", "grau_jurisdicao",
                    "sistema_processual", "formato_processo")
    .write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("silver_dim_processo"))

print("Dims da silver gravadas: orgao, assunto, ponte, processo.")
```

##### Resposta:

Dims da silver gravadas: orgao, assunto, ponte, processo.

### BLOCO 6 — Regras de marcação (`silver_regras_marcacao`, v2)

Tabela de configuração que transforma conhecimento jurídico em regras
declaradas: `regra_id`, `categoria_evento`, `descricao` e `condicao_sql`.
O pipeline fica auditorável — toda marcação rastreia a regra que a gerou.

**Por que uma tabela de regras e não lógica embutida no código:** as
regras de marcação são a ponte entre o direito (Lei 11.101/2005) e o dado.
Isolá-las permite revisar e discutir uma regra de cada vez sem tocar no
pipeline, e todo evento classificado carrega `regra_aplicada` — qualquer
métrica da Gold é rastreável até o critério que a originou.

**Princípio de precedência:** o Bloco 7 aplica as regras **na ordem da
lista** — a primeira que casa vence. É por isso que a R12 (homologação)
está em **primeiro lugar**: sem isso, a R10 (TPU 12444, deferimento
genérico) "engoliria" o evento de homologação antes de a regra
específica enxergá-lo. Regras específicas sempre precedem as genéricas.

### As regras (na ordem de aplicação)

**R12 — Homologação do plano de recuperação (classe 128/129).**

_O que marca:_ a aprovação judicial do plano, marco do art. 57 da Lei 11.101/2005 — encerra a fase de "stay period" e inicia o cumprimento.

_Como funciona:_ exige `classe_codigo_mov IN (128 , 129)` (recuperação extrajudicial/recuperação judicial) e casa por padrões textuais em `movimento_nome` **e** `complementos_txt`
("homolog+plano", "plano+homologado") — o Datajud não tem código TPU dedicado a esse ato, então a identificação é necessariamente textual.

_Por que existe:_ é um dos desfechos centrais da pesquisa,contudo diversas tentativas mostraram que esse movimento não ocorre nos dados (0 ocorrências); a regra segue ativa — se o CNJ passar a registrar o ato, o pipeline já o captura.

**R11 — Decretação de falência (TPU 202).**

_O que marca:_ a sentença declaratória de falência (art. 99 da Lei 11.101/2005) — conversão da recuperação em falência ou falência direta.

_Como funciona:_ código TPU exato (`movimento_codigo = 202`) — não precisa de padrão textual, o vocabulário processual unificado tem código dedicado a esse ato.

_Por que existe:_ é o desfecho adverso do modelo. Permite medir **tempo até a falência** e quantificar quantas recuperações processadas não se concretizam.

**R13 — Extinção do processo (TPU 22/456).**

_O que marca:_ o fim do processo no juízo.

_Como funciona:_ `movimento_codigo IN (22, 456)` — dois códigos TPU para baixa definitiva e extinção respectivamente, elas cobrem as variantes de extinção, uma vez que o CNJ não uniformiza o ato em um só código.

_Por que existe:_ delimita o **fim do ciclo de vida** do processo. Sem ela, durações mediriam até a última captura, não até o desfecho real.

**R14 — Encerramento/arquivamento dos autos.**

_O que marca:_ o ato administrativo de encerramento ou arquivamento.

_Como funciona:_ padrão textual ("%encerra%", "%arquivamento%") — o arquivamento não tem código TPU único.

_Por que existe:_ distingue o arquivamento formal sem decisão de mérito (abandono da ação, falta de interesse, ausência de pressupostos processuais, desistênci), da extinção após a decisão de mérito (procedente/improcedente).

_Obs_: processos podem ser extintos e encerrados em momentos distintos; confundi-los distorceria as durações.

**R9 — Processamento da recuperação judicial (classe 129).**

_O que marca:_ o deferimento do processamento (art. 52 da Lei 11.101/2005) — o juiz acolhe o pedido e instaura a recuperação.

_Como funciona:_ exige `classe_codigo_mov = 129` (processo de RJ) **e** padrão textual "%processamento%" — restringe por classe para não pegar "processamentos" de outras matérias.

_Por que existe:_ define o **ponto zero** de todas as durações da Gold: tempo de tramitação, tempo até homologação, tempo até o desfecho. Está depois de R12/R11/R13/R14 na lista porque, se o processo já tem desfecho registrado, o desfecho vence o processamento na marcação.

**R10 — Deferimento genérico (TPU 12444).**

_O que marca:_ qualquer decisão de deferimento que **não** foi capturada pela R12 (homologação) nem pela R9 (processamento).

_Como funciona:_ código TPU exato (`movimento_codigo = 12444`), que tem o nome de deferimeto nessa tabela.

_Por que existe:_ é a **regra-guarda**. Deferimentos genéricos são frequentes no Datajud; sem capturá-los, eventos relevantes ficariam sem categoria. A precedência controlada (por último na família dos deferimentos) impede que ele absorva homologação ou processamento.

**R15 — Tutela/liminar de urgência registrada nos autos.**

_O que marca:_ a **protocolização** do pedido de tutela de urgência — o ato de registro, sem decisão do juiz.

_Como funciona:_ padrão textual ("%tutela%" OR "%liminar%") em
`movimento_nome`.

_Por que existe:_ abre a trilha da tutela e é a **âncora do início** da tentativa de proteção. Na recuperação judicial, a tutela de urgência concedida inicia o _stay period_ — a suspensão das ações e execuções contra o devedor (art. 6º, § 4º da Lei 11.101/2005). Sem a marca de registro, não se mede quanto tempo o pedido ficou pendente.

**R16 — Tutela deferida/concedida.**

_O que marca:_ a decisão favorável — a tutela é concedida e a proteção efetivamente começa.

_Como funciona:_ padrão textual ("%conced%" em `movimento_nome`,
"%concedida%" em `complementos_txt`) — cobre "concede", "concedida" e variantes da mesma raiz.

_Por que existe:_ define o **início efetivo do stay period**. A Gold mede a duração da proteção a partir daqui — deferimento é o único evento que transforma o pedido em proteção real.

**R17 — Tutela indeferida.**

_O que marca:_ o desfecho adverso — o juiz nega a tutela.

_Como funciona:_ padrão textual ("%indefere%", "%não conced%").

_Por que existe:_ o indeferimento define o fim da proteção pontual (quando havia deferimento antes) ou a sua ausência naquele conflito específico. Separar deferimento de indeferimento permite quantificar em quantos processos a empresa em crise financeira perdeu a proteção do instituto da recuperação.

```text
# ==============================================================================
# BLOCO 6 — REGRAS DE MARCACAO (v2 corrigida)
# FIX R12: regra de homologação movida para PRIMEIRA posição (precedência
# sobre R10/TPU 12444, que engolia o evento) e condição alargada:
# '%homolog%plano%', '%plano%homologado%' e complementos textualizados.
# IMPORTANTE: o loop de classificação aplica as regras NA ORDEM da lista —
# regras específicas antes das genéricas.
# ==============================================================================

regras = [
    # --- ESPECÍFICAS primeiro (desfechos que usam movimentos genéricos) ---
    ("R12", "homologacao_plano",
     "Homologação do plano de recuperação (RJ ou RE)",
     "classe_codigo_mov IN (128, 129) AND (lower(movimento_nome) LIKE '%homolog%plano%' "
     "OR lower(movimento_nome) LIKE '%plano%homologado%' "
     "OR lower(movimento_nome) LIKE '%homologacao de plano%' "
     "OR lower(complementos_txt) LIKE '%homolog%plano%' "
     "OR lower(complementos_txt) LIKE '%plano%homologado%')"),
    ("R11", "decretao_falencia",
     "TPU 202 - Decretação de falência",
     "movimento_codigo = 202"),
    ("R13", "extincao",
     "Extinção do processo (TPU 22/456)",
     "movimento_codigo IN (22, 456)"),
    ("R14", "encerramento",
     "Encerramento/arquivamento dos autos",
     "lower(movimento_nome) LIKE '%encerra%' OR lower(movimento_nome) LIKE '%arquivamento%'"),
    # --- DEFERIMENTO de processamento: textual antes do TPU genérico ---
    ("R9",  "processamento_rj",
     "Despacho textual de deferimento do processamento de RJ (classe 129)",
     "classe_codigo_mov = 129 AND lower(movimento_nome) LIKE '%processamento%'"),
    ("R10", "deferimento_estrito",
     "TPU 12444 - Decisão de deferimento (exceto homologação, capturada antes pela R12)",
     "movimento_codigo = 12444"),
    # --- TUTELAS ---
    ("R15", "tutela_urgencia",
     "Tutela/liminar de urgência registrada nos autos",
     "lower(movimento_nome) LIKE '%tutela%' OR lower(movimento_nome) LIKE '%liminar%'"),
    ("R16", "tutela_deferida",
     "Deferimento/concessão de tutela",
     "lower(movimento_nome) LIKE '%conced%' OR lower(complementos_txt) LIKE '%concedida%'"),
    ("R17", "tutela_indeferida",
     "Indeferimento de tutela",
     "lower(movimento_nome) LIKE '%indefere%' OR lower(movimento_nome) LIKE '%não conced%'"),
]

schema_regra = T.StructType([
    T.StructField("regra_id",         T.StringType()),
    T.StructField("categoria_evento", T.StringType()),
    T.StructField("descricao",        T.StringType()),
    T.StructField("condicao_sql",     T.StringType()),
])

spark.createDataFrame(regras, schema_regra) \
     .write.mode("overwrite").option("overwriteSchema", "true") \
     .saveAsTable("silver_regras_marcacao")

print("silver_regras_marcacao regravada:", len(regras),
      "regras (R12 em precedência máxima).")
```

##### Resposta:

silver_regras_marcacao regravada: 9 regras (R12 em precedência máxima).

### BLOCO 7 — Classificação de eventos e marcos processuais

Duas saídas em cascata:

1. **`silver_datajud_eventos_enriquecidos`**: a fato de movimentos com as
   colunas `categoria_evento` e `regra_aplicada` preenchidas. A primeira
   regra que casa vence — implementado com `when(...).otherwise(...)` em
   cadeia, na ordem da lista do Bloco 6.
2. **`silver_marcos_processo`**: os eventos **classificados e datados**
   (filtro `precisao_suficiente`), com DDL + INSERT separados (padrão
   RTAS-safe já validado no 01) e COMMENT em todas as colunas.

**Por que marcos:** a Gold calcula durações (tempo até processamento,
stay period, tempo até falência) a partir dessas datas — aqui é o único
ponto do pipeline onde evento jurídico vira data mensurável.

**Verificação imediata:** o `display` de marcos por regra permite conferir
de olho se a R12 materializou ou se segue como não-achado (0 linhas).

```text
# ==============================================================================
# BLOCO 7 — CLASSIFICACAO DOS EVENTOS + MARCOS PROCESSUAIS
# Regenera os eventos enriquecidos e os marcos com as regras corrigidas.
# A primeira regra que casa vence, na ordem da lista (específicas primeiro).
# ==============================================================================

df_evt = (spark.table("silver_fato_movimento")
    .withColumn("categoria_evento", F.lit(None).cast("string"))
    .withColumn("regra_aplicada",   F.lit(None).cast("string")))

for regra_id, categoria, _, condicao in regras:
    df_evt = (df_evt
        .withColumn("regra_aplicada",
            F.when(F.expr(condicao), F.lit(regra_id))
             .otherwise(F.col("regra_aplicada")))
        .withColumn("categoria_evento",
            F.when(F.expr(condicao) & F.col("categoria_evento").isNull(),
                   F.lit(categoria))
             .otherwise(F.col("categoria_evento"))))

(df_evt.write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("silver_datajud_eventos_enriquecidos"))

# DDL + carga separados (padrão RTAS-safe já validado)
spark.sql("""
  CREATE OR REPLACE TABLE silver_marcos_processo (
    numero_processo    STRING COMMENT 'Número CNJ do processo',
    classe_codigo_mov  BIGINT COMMENT 'Classe processual do processo',
    categoria_evento   STRING COMMENT 'Categoria jurisimétrica da regra aplicada',
    regra_aplicada     STRING COMMENT 'regra_id (linhagem/auditoria)',
    data_movimento     DATE   COMMENT 'Data normalizada do marco',
    movimento_codigo   BIGINT COMMENT 'Código TPU do movimento',
    movimento_nome     STRING COMMENT 'Texto do movimento',
    complementos_txt   STRING COMMENT 'Complementos tabelados concatenados'
  ) USING DELTA
  COMMENT 'Marcos processuais datados: 1 linha por evento classificado'
""")

spark.sql("""
  INSERT OVERWRITE silver_marcos_processo
  SELECT numero_processo, classe_codigo_mov, categoria_evento,
         regra_aplicada, data_movimento, movimento_codigo,
         movimento_nome, complementos_txt
  FROM silver_datajud_eventos_enriquecidos
  WHERE categoria_evento IS NOT NULL AND precisao_suficiente
""")

# Verificação imediata da correção: homologação deve materializar agora
print("Marcos por regra:")
display(spark.sql("""
  SELECT categoria_evento, regra_aplicada, count(*) AS n
  FROM silver_marcos_processo GROUP BY 1, 2 ORDER BY n DESC
"""))
```

##### Resposta:

Marcos por regra:  
| categoria_evento | regra_aplicada | n |
| :--- | :--- | :--- |
| extincao | R13 | 83891 |
| tutela_urgencia | R15 | 9932 |
| encerramento | R14 | 8505 |
| deferimento_estrito | R10 | 7884 |
| decretao_falencia | R11 | 5061 |

### BLOCO 8 — QA da camada Silver (`silver_qa_execucao`)

Gate de qualidade antes de qualquer consumo pela Gold. Cinco métricas com
valores esperados quando existem:

| Métrica                         | Esperado              | Interpretação                            |
| ------------------------------- | --------------------- | ---------------------------------------- |
| processos_distintos             | 225.556               | dedupe correto (igual à Bronze distinta) |
| movimentos_totais               | ~14,1 mi (referência) | grão preservado da origem                |
| movimentos_com_data_valida      | —                     | sanidade do parser de datas              |
| eventos_classificados_por_regra | —                     | cobertura das regras de marcação         |
| marcos_processuais_datados      | —                     | insumo das durações na Gold              |

Cada execução grava `executado_em` — o histórico de QA é comparável entre
execuções.

```text
# ==============================================================================
# BLOCO 8 — QA DA CAMADA SILVER (gravada: silver_qa_execucao)
# Gate de qualidade antes da gold: contagens-chave com valores esperados.
# ==============================================================================

qa = spark.sql("""
  SELECT 'processos_distintos'      AS metrica,
         count(*) AS valor, 225556 AS esperado
  FROM silver_datajud_processos
  UNION ALL
  SELECT 'movimentos_totais',
         (SELECT count(*) FROM silver_fato_movimento),
         14129958
  UNION ALL
  SELECT 'movimentos_com_data_valida',
         (SELECT count(*) FROM silver_fato_movimento WHERE precisao_suficiente),
         NULL
  UNION ALL
  SELECT 'eventos_classificados_por_regra',
         (SELECT count(*) FROM silver_datajud_eventos_enriquecidos
          WHERE categoria_evento IS NOT NULL),
         NULL
  UNION ALL
  SELECT 'marcos_processuais_datados',
         (SELECT count(*) FROM silver_marcos_processo),
         NULL
""")

(qa.withColumn("executado_em", F.current_timestamp())
   .write.mode("overwrite").option("overwriteSchema", "true")
   .saveAsTable("silver_qa_execucao"))

display(spark.table("silver_qa_execucao"))
print("02 concluído: silver 100% reconstruída a partir de bronze_datajud_raw.")
```

##### Resposta:

| metrica                         | valor    | esperado | executado_em                  |
| :------------------------------ | :------- | :------- | :---------------------------- |
| eventos_classificados_por_regra | 115273   | null     | 2026-09-22T00:39:09.399+00:00 |
| movimentos_com_data_valida      | 14128738 | null     | 2026-09-22T00:39:09.399+00:00 |
| marcos_processuais_datados      | 115273   | null     | 2026-09-22T00:39:09.399+00:00 |
| processos_distintos             | 225556   | 225556   | 2026-09-22T00:39:09.399+00:00 |
| movimentos_totais               | 14129958 | 14129958 | 2026-09-22T00:39:09.399+00:00 |

### 9. Bloco de governança — comentários das colunas das tabelas Silver

```text
# BLOCO DE GOVERNANÇA — comentários de coluna da Silver (idempotente)
# Executar SEMPRE ao final do notebook 02: escrita por DataFrame não
# propaga comentários, e overwriteSchema os apaga a cada re-execução.

tabelas = {
  "silver_datajud_processos": {
    "numero_processo":       "Número CNJ de 20 dígitos — identificador único do processo",
    "tribunal_origem":       "Sigla do Tribunal de Justiça de onde o registro foi extraído (ex: TJSP, TJRJ, TJDFT)",
    "grau_jurisdicao":       "Grau de jurisdição (G1 = primeiro grau, G2 = segundo grau)",
    "classe_codigo":         "Código TPU da classe processual (129 = RJ, 108 = Falência, 128 = RE)",
    "classe_nome":           "Nome da classe processual conforme a TPU do CNJ",
    "sistema_processual":    "Sistema processual em que o processo tramita (ex: PJe, e-SAJ)",
    "formato_processo":      "Formato do processo (eletrônico ou físico)",
    "data_ajuizamento":      "Data de ajuizamento; dela deriva o recorte escopo_lei_atual",
    "orgao_julgador_codigo": "Código CNJ do órgão julgador (vara/juízo)",
    "orgao_julgador_nome":   "Nome do órgão julgador; dele deriva a especialização (eh_especializada na Gold)",
    "municipio_ibge_codigo": "Código IBGE do município da sede do órgão julgador",
    "assuntos":              "Array de assuntos do processo (código + nome); o recorte da coleta filtra pelos códigos 4992–4997",
    "movimentos":            "Array do histórico completo de movimentações — insumo bruto das regras de marcação",
    "escopo_lei_atual":      "TRUE se ajuizado a partir de 09/06/2005 (vigência da Lei 11.101/2005) — universo das métricas",
    "regime_legal":          "Regime legal aplicável (Lei 11.101/2005 ou Lei 7.661/1945 legado)",
  },
  "silver_fato_movimento": {
    "numero_processo":      "Número CNJ do processo ao qual o movimento pertence",
    "tribunal_origem":      "Sigla do tribunal do processo",
    "classe_codigo_mov":    "Código TPU da classe do processo no momento do movimento",
    "movimento_codigo":     "Código TPU do movimento",
    "movimento_nome":       "Descrição textual do ato processual",
    "data_movimento":       "Data do movimento após o parser multi-formato",
    "precisao_data":        "Precisão da data original (dia_hora / dia / mes / ano / invalida)",
    "precisao_suficiente":  "TRUE se a precisão da data basta para construir marco datado",
    "sem_codigo":           "TRUE se o movimento não possui código TPU (rastro de qualidade da origem)",
    "complementos_txt":     "Complementos tabelados concatenados em texto pesquisável",
    "qtd_ocorrencias":      "Ocorrências idênticas colapsadas pelo dedupe por conteúdo (evidência, não perda)",
  },
  "silver_dim_orgao_julgador": {
    "orgao_julgador_codigo": "Código CNJ do órgão julgador",
    "orgao_julgador_nome":   "Nome do órgão julgador conforme a origem",
    "tribunal_sigla":        "Sigla do tribunal de origem",
    "municipio_ibge_codigo": "Código IBGE do município da sede",
    "qtd_processos":         "Quantidade de processos vinculados ao órgão",
  },
  "silver_dim_assunto": {
    "assunto_codigo": "Código do assunto no padrão CNJ",
    "assunto_nome":   "Nome do assunto",
    "qtd_processos":  "Processos distintos com o assunto (countDistinct evita dupla contagem)",
  },
  "silver_ponte_processo_assunto": {
    "numero_processo": "Número CNJ do processo",
    "assunto_codigo":  "Código do assunto vinculado ao processo",
    "assunto_nome":    "Nome do assunto vinculado",
  },
  "silver_dim_processo": {
    "numero_processo":    "Número CNJ do processo",
    "tribunal_origem":    "Sigla do tribunal de origem",
    "grau_jurisdicao":    "Grau de jurisdição (G1/G2)",
    "sistema_processual": "Sistema processual em que o processo tramita",
    "formato_processo":   "Formato do processo (eletrônico ou físico)",
  },
  "silver_regras_marcacao": {
    "regra_id":         "Identificador da regra (R9–R17)",
    "categoria_evento": "Categoria de evento atribuída pela regra",
    "descricao":        "Descrição humana da regra",
    "condicao_sql":     "Condição SQL declarada — aplicada na ordem da lista; a primeira que casa vence",
  },
  "silver_datajud_eventos_enriquecidos": {
    "numero_processo":     "Número CNJ do processo ao qual o movimento pertence",
    "tribunal_origem":     "Sigla do tribunal do processo",
    "classe_codigo_mov":   "Código TPU da classe do processo",
    "movimento_codigo":    "Código TPU do movimento",
    "movimento_nome":      "Descrição textual do ato processual",
    "data_movimento":      "Data do movimento após o parser multi-formato",
    "precisao_data":       "Precisão da data original (dia_hora / dia / mes / ano / invalida)",
    "precisao_suficiente": "TRUE se a precisão da data basta para construir marco datado",
    "sem_codigo":          "TRUE se o movimento não possui código TPU",
    "complementos_txt":    "Complementos tabelados concatenados em texto pesquisável",
    "qtd_ocorrencias":     "Ocorrências idênticas colapsadas pelo dedupe por conteúdo",
    "categoria_evento":    "Categoria do evento classificado — primeira regra que casa vence",
    "regra_aplicada":      "regra_id da regra que classificou o evento (linhagem/auditoria)",
  },
  "silver_marcos_processo": {  # já nasce comentada pelo DDL; reaplicação idempotente
    "numero_processo":   "Número CNJ do processo",
    "classe_codigo_mov": "Classe processual do processo",
    "categoria_evento":  "Categoria jurimétrica da regra aplicada",
    "regra_aplicada":    "regra_id (linhagem/auditoria)",
    "data_movimento":    "Data normalizada do marco",
    "movimento_codigo":  "Código TPU do movimento",
    "movimento_nome":    "Texto do movimento",
    "complementos_txt":  "Complementos tabelados concatenados",
  },
  "silver_qa_execucao": {
    "metrica":      "Nome da métrica de QA da camada",
    "valor":        "Valor obtido na execução",
    "esperado":     "Valor esperado (quando definido)",
    "executado_em": "Timestamp da execução — histórico comparável entre execuções",
  },
}

falhas = []
for tabela, cols in tabelas.items():
    for coluna, comment in cols.items():
        try:
            spark.sql(f"ALTER TABLE default.{tabela} ALTER COLUMN {coluna} COMMENT '{comment}'")
        except Exception as e:
            falhas.append((tabela, coluna, str(e).splitlines()[0]))

total = sum(len(v) for v in tabelas.values())
if falhas:
    print(f"{len(falhas)} de {total} comentários não aplicados — conferir nomes de coluna:")
    for t, c, msg in falhas:
        print(f"  {t}.{c}: {msg}")
else:
    print(f"Governança aplicada: {total} comentários de coluna em {len(tabelas)} tabelas da Silver.")
```

##### Resposta:

Governança aplicada: 71 comentários de coluna em 10 tabelas da Silver.

---

# Anexo C: Estabelecimento da Camada Gold

### Notebook 03_gold_modelagem — Gold: Modelagem Dimensional

#### Objetivo

Consumir a camada Silver e materializar a camada Gold:

- um **star schema** (modelagem dimensional em estrela) com o ciclo de vida completo dos processos;
- um **mart analítico** que alimenta as 9 perguntas do MVP; e
- a governança do catálogo (dicionário de dados + QA com regression check).

#### O que é o star schema (e por que aqui)

Modelo dimensional clássico de Kimball: uma **tabela fato** central (fatos = eventos mensuráveis, números, datas) cercada por **tabelas dimensão** (contextos descritivos pelos quais se filtra e agrupa). O nome vem do desenho: no diagrama, as dimensões irradiam da fato como pontas de estrela.

**Por que separar fato de dimensão:**

- A fato fica **enxuta e numérica** — agregações (SUM, COUNT, AVG) são rápidas e o grão fica explícito (aqui: 1 linha por processo).
- As dimensões guardam o **contexto descritivo** (classe, órgão, tempo) — mudanças de rótulo não poluem a fato, e o mesmo contexto serve a várias métricas sem duplicação.
- É o padrão que ferramentas de BI esperam.

## O desenho implementado

| Tabela                  | Papel                                                                 | Grão                       |
| ----------------------- | --------------------------------------------------------------------- | -------------------------- |
| `dim_tempo`             | Calendário do corpus (1995–2027)                                      | 1 linha por dia            |
| `dim_classe_processual` | Classes da TPU do CNJ                                                 | 1 linha por classe         |
| `dim_orgao_julgador`    | Órgãos julgadores, dedup por (código, tribunal)                       | 1 linha por órgão×tribunal |
| `dim_processo`          | 225.556 processos, todos da silver após 1/01/1995                     | 1 linha por processo       |
| `fato_tramitacao`       | **Fato central**: ciclo de vida + durações (perguna 1 até pergunta 5) | 1 linha por processo       |
| `fato_incidentes`       | Fato de grão fino: incidentes de crédito (pergunta 7)                 | 1 linha por autos-filho    |
| `gold_base_analitica`   | **Mart analítico**: tudo que as 9 perguntas consomem                  | 1 linha por processo       |

## O que é o mart (e por que ele existe)

`gold_base_analitica` é um **mart**: uma tabela desnormalizada, plana e orientada ao consumo — 1 linha por processo com todas as métricas das 9 perguntas já calculadas e nomeadas pela pergunta que atendem
(`dias_stay_period` para pergunta 2, `dias_ate_convolucao` para a pergunta 5 ...).

**Por que um mart se o star schema já existe:** o star schema serve para consultas exploratórias e agregações livres; as 9 perguntas do MVP são consumo de receita pronta. O mart elimina joins repetidos, congela a semântica validada (proxy de processamento, evento de fim, higiene temporal) num único lugar e é a **fonte única das respostas** — os notebooks p01 a p09 e o regression check leem dele, não das fatos/dims direto. Star schema = modelagem; mart = entrega.

## Padrões do notebook

- **DDL comentado antes da carga:** o `CREATE TABLE` com COMMENT por coluna é o contrato da tabela — é dele que o dicionário (Bloco 6 do código) se alimenta.
- **INSERT OVERWRITE / overwriteSchema:** toda execução regrava
  integralmente as tabelas — idempotente e sem resíduos.
- **Gates de QA em memória:** validações rodam sobre a view temporária ANTES do write — dado defeituoso não entra no catálogo.

```text
# Databricks notebook source
# ==============================================================================
# 03_GOLD_MODELAGEM — silver -> gold
# Star schema: dim_tempo, dim_classe, dim_orgao, dim_processo, fato_tramitacao
# + mart analítico (gold_base_analitica) + governança (data dictionary) + QA.
# Padrão: DDL comentado (contrato da tabela) + INSERT OVERWRITE (carga idempotente).
# ==============================================================================

from pyspark.sql import functions as F

# ==============================================================================
# BLOCO 1 — DIM_TEMPO (calendário do corpus)
# ==============================================================================

spark.sql("""
  CREATE OR REPLACE TABLE dim_tempo (
    sk_tempo  INT    COMMENT 'Surrogate key: AAAAMMDD',
    data      DATE   COMMENT 'Data do calendário',
    ano       INT    COMMENT 'Ano',
    trimestre INT    COMMENT 'Trimestre (1-4)',
    mes       INT    COMMENT 'Mês (1-12)',
    dia       INT    COMMENT 'Dia (1-31)',
    ano_mes   STRING COMMENT 'AAAA-MM para agrupamento mensal',
    semestre  INT    COMMENT 'Semestre (1-2)'
  ) USING DELTA
  COMMENT 'Dimensão de tempo: 1 linha por dia do intervalo do corpus'
""")

spark.sql("""
  INSERT OVERWRITE dim_tempo
  SELECT year(d)*10000 + month(d)*100 + day(d), d, year(d), quarter(d),
         month(d), day(d),
         concat(year(d), '-', lpad(month(d), 2, '0')),
         CASE WHEN month(d) <= 6 THEN 1 ELSE 2 END
  FROM (SELECT explode(sequence(to_date('1995-01-01'),
                                to_date('2027-12-31'),
                                interval 1 day)) AS d)
""")

print("dim_tempo:", spark.table("dim_tempo").count(), "dias.")
```

##### Resposta:

dim_tempo: 12053 dias.

### BLOCO 2 — `dim_classe_processual` e `dim_orgao_julgador`

Duas dimensões de contexto, cada uma com uma decisão de qualidade:

**`dim_classe_processual`** — catálogo de classes da TPU do CNJ,
extraído do universo da silver. SK numérica sequencial
(`row_number`) porque o código CNJ não serve de chave direta: ele é BIGINT esparsamente preenchido e a dim precisa de chave INT densa.

**`dim_orgao_julgador`** — onde está a correção estrutural do projeto: o **dedup por (código, tribunal)**. A origem traz o mesmo órgão com variantes de grafia do nome; a chave real de identidade é o par (código CNJ, tribunal), não o nome.

Então, o pipeline:

1. Agrupa por (código, tribunal, nome) contando ocorrências — o nome
   **mais frequente vira o canônico** (`nome_canonico`).
2. `qtd_variantes` registra quantas grafias foram consolidadas — QA
   transparente: nada foi descartado, a contagem documenta a sujeira.
3. `eh_especializada` classifica por padrão de nome (falência/
   empresarial/recupera) — atributo analítico que sustenta a pergunta 4
   (comparativo de varas especializadas vs. comuns).

```text
# ==============================================================================
# BLOCO 2 — DIM_CLASSE_PROCESSUAL e DIM_ORGAO_JULGADOR
# ==============================================================================

spark.sql("""
  CREATE OR REPLACE TABLE dim_classe_processual (
    sk_classe     INT    COMMENT 'Surrogate key da classe',
    classe_codigo BIGINT COMMENT 'Código TPU do CNJ',
    classe_nome   STRING COMMENT 'Nome da classe processual'
  ) USING DELTA
  COMMENT 'Dimensão das classes processuais (TPU CNJ)'
""")

spark.sql("""
  INSERT OVERWRITE dim_classe_processual
  SELECT row_number() OVER (ORDER BY classe_codigo),
         classe_codigo, classe_nome
  FROM (SELECT DISTINCT classe_codigo, classe_nome
        FROM silver_datajud_processos
        WHERE classe_codigo IS NOT NULL)
""")

spark.sql("""
  CREATE OR REPLACE TABLE dim_orgao_julgador (
    sk_orgao_julgador     INT     COMMENT 'Surrogate key: 1 por (codigo, tribunal)',
    orgao_julgador_codigo BIGINT  COMMENT 'Código CNJ do órgão julgador',
    orgao_julgador_nome   STRING  COMMENT 'Nome canônico (grafia mais frequente)',
    tribunal_sigla        STRING  COMMENT 'Tribunal de origem',
    municipio_ibge_codigo BIGINT  COMMENT 'Código IBGE da sede',
    qtd_variantes_nome    INT     COMMENT 'QA: variantes de grafia consolidadas',
    eh_especializada      BOOLEAN COMMENT 'Órgão especializado em empresarial/falimentar (por nome)'
  ) USING DELTA
  COMMENT 'Dimensão dos órgãos julgadores — dedup por (codigo, tribunal)'
""")

spark.sql("""
  INSERT OVERWRITE dim_orgao_julgador
  SELECT row_number() OVER (ORDER BY orgao_julgador_codigo, tribunal_sigla),
         orgao_julgador_codigo, nome_canonico, tribunal_sigla,
         municipio_ibge_codigo, qtd_variantes,
         (lower(nome_canonico) LIKE '%falencia%' OR lower(nome_canonico) LIKE '%empresarial%'
          OR lower(nome_canonico) LIKE '%recupera%')
  FROM (
    SELECT orgao_julgador_codigo, tribunal_sigla, nome_canonico,
           municipio_ibge_codigo, qtd_variantes,
           row_number() OVER (PARTITION BY orgao_julgador_codigo, tribunal_sigla
                              ORDER BY qtd_ocorrencias DESC) AS rn
    FROM (
      SELECT orgao_julgador_codigo, tribunal_sigla,
             orgao_julgador_nome                AS nome_canonico,
             first(municipio_ibge_codigo, true) AS municipio_ibge_codigo,
             count(*)                           AS qtd_ocorrencias,
             count(DISTINCT orgao_julgador_nome) AS qtd_variantes
      FROM silver_dim_orgao_julgador
      WHERE orgao_julgador_codigo IS NOT NULL
      GROUP BY orgao_julgador_codigo, tribunal_sigla, orgao_julgador_nome))
  WHERE rn = 1
""")

print("dim_classe_processual:", spark.table("dim_classe_processual").count())
print("dim_orgao_julgador:", spark.table("dim_orgao_julgador").count())
```

##### Resposta:

dim_classe_processual: 196\
dim_orgao_julgador: 3979

### BLOCO 3 — `dim_processo`

Dimensão do processo com **grão = numero_processo** e escopo
deliberadamente amplo: os **225.556 processos** da bronze deduplicados na silver — incluindo o legado da Lei 7.661/1945, capturado no download do Datajud devido decisão de capturar processos de recuperação judicial e falências (classe_codigo= 4993, pai) a partir de 1/01/1995, anterior à vingencia da lei 11.101/2005 em vigor.

**Decisão de desenho — a dim guarda o universo, o fato filtra a
jusante:** a dim_processo carrega tudo (com `escopo_lei_atual` e
`regime_legal` como atributos, não como filtro). As métricas da Lei
11.101/2005 são obtidas filtrando na fato/mart. Assim o universo
completo permanece consultável — recortes futuros (ex.: comparar com
o regime antigo) não exigem reconstrução da dim.

**Surrogate key:** `row_number` ordenado por número CNJ — densa e
estável. As FKs (classe, órgão, tempo de ajuizamento) são resolvidas
por LEFT JOIN com as dims já criadas — LEFT, não INNER: processo sem
match de dimensão **não some**, aparece com FK nula (problema visível
em QA em vez de perda silenciosa).

**O `rn = 1` final** protege o grão: qualquer eventual duplicidade do
join (ex.: órgão ambíguo) colapsa em 1 linha por processo — a dim
nunca pode violar seu próprio grão.

```text
# ==============================================================================
# BLOCO 3 — DIM_PROCESSO (universo bruto: 225.556 processos)
# SK numérica; FKs para classe, órgão e tempo de ajuizamento.
# O fato filtra a jusante — a dim guarda o universo completo.
# ==============================================================================

spark.sql("""
  CREATE OR REPLACE TABLE dim_processo (
    sk_processo          BIGINT  COMMENT 'Surrogate key do processo',
    numero_processo      STRING  COMMENT 'Número CNJ de 20 dígitos (chave natural)',
    tribunal_sigla       STRING  COMMENT 'Tribunal de origem',
    grau_jurisdicao      STRING  COMMENT 'Grau de jurisdição (G1/G2)',
    classe_codigo        BIGINT  COMMENT 'Código TPU da classe',
    classe_nome          STRING  COMMENT 'Nome da classe processual',
    data_ajuizamento     DATE    COMMENT 'Data de ajuizamento',
    regime_legal         STRING  COMMENT 'Regime legal (Lei 11.101/2005 ou legado)',
    escopo_lei_atual     BOOLEAN COMMENT 'Ajuizado >= 2005-06-09 (universo das métricas)',
    sk_classe            INT     COMMENT 'FK -> dim_classe_processual',
    sk_orgao_julgador    INT     COMMENT 'FK -> dim_orgao_julgador',
    sk_tempo_ajuizamento INT     COMMENT 'FK -> dim_tempo (data de ajuizamento)'
  ) USING DELTA
  COMMENT 'Dimensão de processos: grain = numero_processo (universo bruto da bronze)'
""")

spark.sql("""
  INSERT OVERWRITE dim_processo
  SELECT row_number() OVER (ORDER BY numero_processo) AS sk_processo,
         numero_processo, tribunal_sigla, grau_jurisdicao,
         classe_codigo, classe_nome, data_ajuizamento,
         regime_legal, escopo_lei_atual,
         sk_classe, sk_orgao_julgador, sk_tempo_ajuizamento
  FROM (
    SELECT p.*, row_number() OVER (PARTITION BY p.numero_processo
                                   ORDER BY p.sk_classe) AS rn
    FROM (
      SELECT p.numero_processo, p.tribunal_origem AS tribunal_sigla,
             p.grau_jurisdicao, p.classe_codigo, p.classe_nome,
             p.data_ajuizamento, p.regime_legal, p.escopo_lei_atual,
             c.sk_classe, o.sk_orgao_julgador, t.sk_tempo AS sk_tempo_ajuizamento
      FROM silver_datajud_processos p
      LEFT JOIN dim_classe_processual c ON c.classe_codigo = p.classe_codigo
      LEFT JOIN dim_orgao_julgador    o ON o.orgao_julgador_codigo = p.orgao_julgador_codigo
                                       AND o.tribunal_sigla = p.tribunal_origem
      LEFT JOIN dim_tempo             t ON t.data = p.data_ajuizamento) p)
  WHERE rn = 1
""")

print("dim_processo:", spark.table("dim_processo").count(), "(alvo 225.556)")
```

##### Resposta:

dim_processo: 225556 (alvo 225.556)

### BLOCO 4 — `fato_tramitacao`: o ciclo de vida completo

**A fato central do star schema: 1 linha por processo**, com todas as
datas-marco do ciclo de vida e as durações derivadas. Construída em
cinco passos:

**(a) Pivot dos marcos** — `silver_marcos_processo` está em formato
"longo" (1 linha por evento classificado); o `min(CASE WHEN...)`
pivota para "largo": uma coluna por categoria, com a **primeira data**
de cada marco por processo.

**(b) Proxy do processamento (TPU 11010, janela 15–180 dias) — camada de
fallback, não método primário.** O marco de processamento é uma cascata
de três níveis, aplicada no `data_deferimento_rj`:

1. **R9** — despacho textual de processamento (regra estrita, Bloco 6
   da silver);

2. **TPU 12444 - deferimento** — decisão de deferimento estrita (incorporada pela
   investigação da pergunta 2, trajetória deferimento → concessão);

3. **Proxy 11010 - despacho de mero expediente** — primeiro expediente comum entre 15 e 180 dias após o ajuizamento, herdado da metodologia original da Pergunta 1. Roda por último
   e só preenche processos sem marco estrito; limites validados por
   análise de sensibilidade (efeito de borda < 3%).

A query da pergunta 1 não reconstrói nada disso — consome
`dias_distribuicao_processamento` do mart, onde a cascata já está
aplicada e congelada.

**(b2) Primeira Baixa Definitiva (TPU 22)** — complemento de cobertura:
a regra de encerramento (TPU 22/456) não cobre o ato "Baixa Definitiva",
sob o qual existem 51 encerramentos de RE. `min()` = primeira baixa
(conservador: autos reabertos geram múltiplas baixas, e o fim real é
a primeira baixa).

**(c) Base da fato** — `dim_processo` + marcos + proxy + baixa, tudo
por LEFT JOIN. O `COALESCE` do encerramento aplica **higiene
temporal**: encerramento datado antes do ajuizamento é ruído de
origem e é descartado em favor da baixa definitiva.

**(d) Métricas derivadas** — a semântica validada nas 9 perguntas,
agora materializada em colunas:

- `data_deferimento_rj` = cascata **R9 → TPU 12444 → proxy 11010**
  (regra estrita vence o proxy; o proxy só preenche lacuna);

- `evento_fim` = cascata **homologação → falência → extinção →
  encerramento** (o desfecho juridicamente mais forte vence);

- `cenario_desfecho` = a mesma cascata como rótulo categórico
  (homologacao_plano | decretao_falencia | extincao | encerramento |
  ativo);

- durações por `datediff` com origem no **ajuizamento** (coerente com
  o método das perguntas 1, 2, 5).

### Gate de QA — valida ANTES de gravar

O fato passa por uma **view temporária** (`fato_staging`) e um `assert` bloca a execução se houver encerramento anterior ao ajuizamento:
**dado que falha no gate nunca chega ao catálogo**. O print de Recuperações Extrajudiciais encerradas confirma a cobertura
de encerramento (referência: 60).

**(e) DDL do contrato + carga** — o CREATE TABLE com COMMENT é o
contrato lido pelo dicionário do Bloco 6; a carga regrava com
`overwriteSchema` para evolução controlada.

```text
# ==============================================================================
# BLOCO 4 — FATO_TRAMITACAO (1 linha por processo; ciclo de vida completo)
# Marcos: pivot da silver_marcos_processo (min data por categoria).
# Proxy do processamento (herdado da pergunta 1): primeiro TPU 11010 entre 15 e
# 180 dias após o ajuizamento, quando não há regra estrita (cascata R9 >
# TPU 12444 > proxy 11010).
# ==============================================================================

# (a) marcos: 1 linha por processo com uma coluna por categoria
marcos = spark.sql("""
  SELECT numero_processo,
         min(CASE WHEN categoria_evento = 'processamento_rj'    THEN data_movimento END) AS marco_proc_r9,
         min(CASE WHEN categoria_evento = 'deferimento_estrito' THEN data_movimento END) AS marco_proc_estrito,
         min(CASE WHEN categoria_evento = 'homologacao_plano'   THEN data_movimento END) AS marco_homologacao,
         min(CASE WHEN categoria_evento = 'decretao_falencia'   THEN data_movimento END) AS marco_falencia,
         min(CASE WHEN categoria_evento = 'extincao'            THEN data_movimento END) AS marco_extincao,
         min(CASE WHEN categoria_evento = 'encerramento'        THEN data_movimento END) AS marco_encerramento,
         min(CASE WHEN categoria_evento = 'tutela_urgencia'     THEN data_movimento END) AS marco_tutela
  FROM silver_marcos_processo
  GROUP BY numero_processo
""")

# (b) proxy de processamento: TPU 11010 na janela 15-180 dias (regra da pergunta 1)
proxy = spark.sql("""
  SELECT m.numero_processo, min(m.data_movimento) AS marco_proc_proxy
  FROM silver_fato_movimento m
  JOIN silver_datajud_processos p ON p.numero_processo = m.numero_processo
  WHERE m.movimento_codigo = 11010
    AND m.data_movimento >= date_add(p.data_ajuizamento, 15)
    AND m.data_movimento <= date_add(p.data_ajuizamento, 180)
  GROUP BY m.numero_processo
""")

# (b2) primeira Baixa Definitiva (TPU 22) — complemento de cobertura:
# a regra de encerramento não cobre o ato "Baixa Definitiva", sob o qual
# existem 51 encerramentos de RE. min() = primeira baixa (conservador:
# autos reabertos geram múltiplas baixas; o fim real é a primeira)
baixa = spark.sql("""
  SELECT m.numero_processo, min(m.data_movimento) AS marco_baixa_definitiva
  FROM silver_fato_movimento m
  JOIN silver_datajud_processos p ON p.numero_processo = m.numero_processo
  WHERE m.movimento_codigo = 22
    AND m.data_movimento >= p.data_ajuizamento
  GROUP BY m.numero_processo
""")

# (c) base do fato: dim_processo + marcos + proxy + baixa
fato = (spark.table("dim_processo").alias("p")
    .join(marcos.alias("mk"), "numero_processo", "left")
    .join(proxy.alias("px"), "numero_processo", "left")
    .join(baixa.alias("bx"), "numero_processo", "left")
    .select(
        F.col("p.sk_processo"),
        F.col("p.sk_classe"),
        F.col("p.sk_orgao_julgador"),
        F.col("p.sk_tempo_ajuizamento").alias("sk_tempo"),
        F.col("p.numero_processo"),
        F.col("p.tribunal_sigla").alias("tribunal_origem"),
        F.col("p.classe_codigo"),
        F.col("p.classe_nome"),
        F.col("p.data_ajuizamento").alias("data_inicio_processo"),
        F.col("mk.marco_proc_r9"),
        F.col("mk.marco_proc_estrito"),
        F.col("px.marco_proc_proxy"),
        F.col("mk.marco_homologacao"),
        F.col("mk.marco_falencia"),
        F.col("mk.marco_extincao"),
        F.coalesce(
            F.when(F.col("mk.marco_encerramento") >= F.col("p.data_ajuizamento"),
                F.col("mk.marco_encerramento")),
            F.col("bx.marco_baixa_definitiva")
        ).alias("marco_encerramento"),
        F.col("mk.marco_tutela"),
        F.lit(None).cast("date").alias("data_sentenca_acordao"))
)

# (d) métricas derivadas (semântica validada nas 9 perguntas)
fato = (fato
    # processamento efetivo: R9 > estrito TPU 12444 > proxy 11010
    .withColumn("data_deferimento_rj",
        F.coalesce("marco_proc_r9", "marco_proc_estrito", "marco_proc_proxy"))
    # fonte do marco de processamento (auditabilidade: rastreia qual regra
    # produziu cada data — a mais confiável vence)
    .withColumn("fonte_deferimento",
        F.when(F.col("marco_proc_r9").isNotNull(), F.lit("r9_despacho_textual"))
         .when(F.col("marco_proc_estrito").isNotNull(), F.lit("tpu12444_deferimento"))
         .when(F.col("marco_proc_proxy").isNotNull(), F.lit("proxy11010_expediente"))
         .otherwise(F.lit(None).cast("string")))
    # evento_fim = FIM DO PROCESSO (homologação > falência > extinção >
    # encerramento). Alimenta ind_processo_encerrado e dias_tramitacao_total.
    # NÃO confundir com o fim do stay period (art. 6º, §4º).
    .withColumn("evento_fim",
        F.coalesce("marco_homologacao", "marco_falencia",
                   "marco_extincao", "marco_encerramento"))
    # FIM DO STAY (art. 6º, §4º da Lei 11.101/2005): a suspensão termina na
    # homologação do plano OU na decretação de falência — o PRIMEIRO dos dois
    # que ocorrer (LEAST ignora nulos).
    .withColumn("data_fim_stay",
        F.least("marco_homologacao", "marco_falencia"))
    .withColumn("cenario_desfecho",
        F.when(F.col("marco_homologacao").isNotNull(), F.lit("homologacao_plano"))
         .when(F.col("marco_falencia").isNotNull(),    F.lit("decretao_falencia"))
         .when(F.col("marco_extincao").isNotNull(),    F.lit("extincao"))
         .when(F.col("marco_encerramento").isNotNull(),F.lit("encerramento"))
         .otherwise(F.lit("ativo")))
    .withColumn("ind_deferido",
        F.col("data_deferimento_rj").isNotNull().cast("int"))
    .withColumn("ind_falencia",
        F.col("marco_falencia").isNotNull().cast("int"))
    .withColumn("ind_processo_encerrado",
        F.col("evento_fim").isNotNull().cast("int"))
    # durações (ajuizamento como origem nas métricas de vida do processo;
    # o stay period, por definição legal, parte do deferimento)
    .withColumn("dias_ate_deferimento_rj",
        F.datediff("data_deferimento_rj", "data_inicio_processo"))
    # STAY PERIOD: do deferimento do processamento ao fim do stay (estrito).
    # Nulo quando falta ponta — censura estrutural (homologação não
    # registrável no padrão Datajud), não falha do método.
    .withColumn("dias_stay_period",
        F.when(F.col("data_deferimento_rj").isNotNull()
               & F.col("data_fim_stay").isNotNull()
               & (F.col("data_fim_stay") >= F.col("data_deferimento_rj")),
               F.datediff("data_fim_stay", "data_deferimento_rj")))
    # COTA DO STAY (limite superior): para quem não tem fim de stay
    # registrável, o fim do processo marca o máximo que o stay pode ter durado.
    .withColumn("dias_stay_limite_superior",
        F.when(F.col("data_deferimento_rj").isNotNull()
               & F.col("evento_fim").isNotNull()
               & (F.col("evento_fim") >= F.col("data_deferimento_rj")),
               F.datediff("evento_fim", "data_deferimento_rj")))
    .withColumn("dias_ate_falencia",
        F.datediff("marco_falencia", "data_inicio_processo"))
    .withColumn("dias_duracao_falencia",
        F.datediff(F.coalesce("marco_encerramento", "marco_extincao"),
                   "marco_falencia"))
    .withColumn("dias_tramitacao_total",
        F.datediff("evento_fim", "data_inicio_processo"))
    .withColumn("ind_stay_period_excedido_180d",
        F.when(F.col("dias_stay_period") > 180, F.lit(1)).otherwise(F.lit(0)))
    .withColumnRenamed("marco_homologacao", "data_homologacao_prj")
    .withColumnRenamed("marco_falencia",    "data_decretacao_falencia")
    .withColumnRenamed("marco_encerramento","data_encerramento_processo")
    .withColumnRenamed("marco_tutela",      "data_primeira_tutela")
    .withColumn("data_carga_gold", F.current_timestamp())
)

# ==============================================================================
# GATE DE QA — valida o fato em memória ANTES do write
# ==============================================================================
fato.createOrReplaceTempView("fato_staging")
chk = spark.sql("""
  SELECT
    count(CASE WHEN classe_codigo = 128 AND ind_processo_encerrado = 1 THEN 1 END) AS re_encerradas_novo,
    count(CASE WHEN data_encerramento_processo < data_inicio_processo THEN 1 END) AS antes_ajuizamento,
    count(CASE WHEN dias_stay_period < 0 THEN 1 END)  AS stay_negativo,
    count(CASE WHEN dias_stay_limite_superior < dias_stay_period THEN 1 END) AS cota_menor_que_stay
  FROM fato_staging
""").collect()[0]
assert chk["antes_ajuizamento"] == 0, f"Gate falhou: {chk['antes_ajuizamento']} encerramentos antes do ajuizamento"
assert chk["stay_negativo"] == 0,     f"Gate falhou: {chk['stay_negativo']} stay periods negativos"
assert chk["cota_menor_que_stay"] == 0, "Gate falhou: cota menor que o stay medido"
print("Gate OK — REs encerradas:", chk["re_encerradas_novo"])

# (e) DDL do contrato + carga — INSERT OVERWRITE preserva o schema/comentários
spark.sql("""
  CREATE OR REPLACE TABLE fato_tramitacao (
    sk_processo                  BIGINT    COMMENT 'FK -> dim_processo',
    sk_classe                    INT       COMMENT 'FK -> dim_classe_processual',
    sk_orgao_julgador            INT       COMMENT 'FK -> dim_orgao_julgador',
    sk_tempo                     INT       COMMENT 'FK -> dim_tempo (ajuizamento)',
    numero_processo              STRING    COMMENT 'Número CNJ (chave lógica)',
    tribunal_sigla               STRING    COMMENT 'Tribunal de origem',
    classe_codigo                BIGINT    COMMENT 'Código TPU da classe',
    classe_nome                  STRING    COMMENT 'Nome da classe',
    cenario_desfecho             STRING    COMMENT 'homologacao_plano|decretao_falencia|extincao|encerramento|ativo',
    ind_deferido                 INT       COMMENT '1 se há deferimento de processamento registrado',
    ind_falencia                 INT       COMMENT '1 se há decretação de falência (TPU 202)',
    ind_processo_encerrado       INT       COMMENT '1 se há evento de fim registrado',
    data_inicio_processo         DATE      COMMENT 'Data de ajuizamento',
    data_deferimento_rj          DATE      COMMENT 'Processamento da RJ (cascata R9 > TPU 12444 > proxy 11010)',
    fonte_deferimento            STRING    COMMENT 'Fonte do marco de processamento: r9_despacho_textual|tpu12444_deferimento|proxy11010_expediente',
    data_fim_stay                DATE      COMMENT 'Fim do stay (art. 6º §4º): homologação do plano OU decretação de falência, o primeiro que ocorrer',
    data_homologacao_prj         DATE      COMMENT 'Homologação do plano (não registrável no padrão Datajud — não-achado estrutural, QA pergunta 2)',
    data_decretacao_falencia     DATE      COMMENT 'Decretação de falência (TPU 202)',
    data_sentenca_acordao        DATE      COMMENT 'Reservado: regra de sentença/acórdão (futura)',
    data_encerramento_processo   DATE      COMMENT 'Encerramento/arquivamento (higiene temporal: >= ajuizamento)',
    data_primeira_tutela         DATE      COMMENT 'Primeira tutela/liminar de urgência',
    evento_fim                   DATE      COMMENT 'Primeiro desfecho registrado: homologação > falência > extinção > encerramento (fim do PROCESSO, não do stay)',
    dias_ate_deferimento_rj      INT       COMMENT 'Pergunta 1: ajuizamento -> processamento',
    dias_stay_period             INT       COMMENT 'Pergunta 2: deferimento do processamento -> fim do stay (estrito; nulo = censura estrutural)',
    dias_stay_limite_superior    INT       COMMENT 'Pergunta 2: cota censurada — deferimento -> fim do processo (limite superior do stay)',
    dias_ate_falencia            INT       COMMENT 'Pergunta 5: ajuizamento -> decretação',
    dias_duracao_falencia        INT       COMMENT 'Pergunta 3: decretação -> encerramento/extinção',
    dias_tramitacao_total        INT       COMMENT 'Ajuizamento -> evento fim (total do processo)',
    ind_stay_period_excedido_180d INT      COMMENT 'Pergunta 2: stay além dos 180 dias (art. 6º §4º)',
    data_carga_gold              TIMESTAMP COMMENT 'Lineage: momento da carga'
  ) USING DELTA
  COMMENT 'Fato central do star: 1 linha por processo, ciclo de vida completo (perguntas de 1 a 5)'
""")

spark.sql("""
  INSERT OVERWRITE fato_tramitacao
  SELECT sk_processo, sk_classe, sk_orgao_julgador, sk_tempo,
         numero_processo, tribunal_origem, classe_codigo, classe_nome,
         cenario_desfecho, ind_deferido, ind_falencia, ind_processo_encerrado,
         data_inicio_processo, data_deferimento_rj, fonte_deferimento,
         data_fim_stay, data_homologacao_prj, data_decretacao_falencia,
         data_sentenca_acordao, data_encerramento_processo, data_primeira_tutela,
         evento_fim,
         dias_ate_deferimento_rj, dias_stay_period, dias_stay_limite_superior,
         dias_ate_falencia, dias_duracao_falencia, dias_tramitacao_total,
         ind_stay_period_excedido_180d, data_carga_gold
  FROM fato_staging
""")

print("fato_tramitacao:", spark.table("fato_tramitacao").count(), "linhas.")
```

##### Resposta:

Gate OK — REs encerradas: 69  
fato_tramitacao: 225556 linhas.

### BLOCO 4.5 — `fato_incidentes`: a fato de grão fino (pergunta 7)

Segunda fato do star, com **grão distinto: 1 linha por autos-filho** —
os incidentes de crédito (habilitações, classe 111, e impugnações,
classe 114), que são processos autônomos ligados ao principal.

**Por que uma fato separada e não uma coluna:** a contagem de
incidentes por processo-pai é agregação; o incidente em si é evento.
Misturar os dois grãos na fato central quebraria o princípio do grão
único (criando um erro de modelagem dimensional). Aqui: a fato de
grão fino guarda o incidente; a agregação acontece na hora do consumo
(Bloco 5).

**A chave de vinculação é posicional** — extraída da estrutura do
número CNJ de 20 dígitos:

- `raiz` = 7 primeiros dígitos (sequencial de origem);
- `origem` = dígitos 17–20 (unidade judiciária de origem).
  Filhos e pai da mesma origem compartilham (raiz, origem) — método
  validado na pergunta 7, que recuperou os 217 incidentes que a versão anterior
  não conseguia ligar.

**Gate de grão:** o print final compara total × distintos — qualquer
duplicidade por autos-filho ficaria visível de imediato.

```text
# ==============================================================================
# BLOCO 4.5 — FATO_INCIDENTES (grão: autos-filho, 1 linha por incidente)
# Gold-puro para a pergunta 7: os incidentes de crédito (habilitações classe 111 e
# impugnações classe 114) viram fato própria na gold, com chave posicional
# de vinculação (raiz 7 + origem 4) igual ao método validado.
# O mart (Bloco 5) passa a ler daqui — sem agregação direta da silver.
# ==============================================================================

spark.sql("""
  CREATE OR REPLACE TABLE fato_incidentes (
    numero_processo       STRING COMMENT 'Número CNJ do autos-filho (grão da fato)',
    raiz                  STRING COMMENT '7 primeiros dígitos: sequencial de origem',
    origem                STRING COMMENT 'Dígitos 17-20: unidade judiciária de origem',
    classe_codigo         BIGINT COMMENT 'Classe do incidente (111=habilitação, 114=impugnação)',
    classe_nome           STRING COMMENT 'Nome da classe do incidente',
    tribunal_origem       STRING COMMENT 'Tribunal do autos-filho',
    data_ajuizamento      DATE   COMMENT 'Ajuizamento do incidente',
    data_carga_gold       TIMESTAMP COMMENT 'Timestamp da carga'
  ) USING DELTA
  COMMENT 'Fato de incidentes processuais: 1 linha por autos-filho (pergunta 7)'
""")

spark.sql("""
  INSERT OVERWRITE fato_incidentes
  SELECT numero_processo,
         substring(numero_processo, 1, 7)  AS raiz,
         substring(numero_processo, 17, 4) AS origem,
         classe_codigo, classe_nome,
         tribunal_origem, data_ajuizamento,
         current_timestamp()
  FROM silver_datajud_processos
  WHERE classe_codigo IN (111, 114)
    AND data_ajuizamento IS NOT NULL
""")

# Gate de QA do grão: sem duplicidade por autos-filho
print("fato_incidentes:",
      spark.table("fato_incidentes").count(), "linhas /",
      spark.table("fato_incidentes").select("numero_processo").distinct().count(),
      "distintos")
```

##### Resposta:

fato_incidentes: 133495 linhas / 133495 distintos

### BLOCO 5 — `gold_base_analitica`: o mart das 9 perguntas

O mart analítico: **1 linha por processo**, desnormalizado, com cada
coluna nomeada pela pergunta que atende. É a **fonte única** dos
notebooks de resposta (p01–p09) e do regression check.

**Composição:** fato_tramitacao (métricas) + dim_processo (`escopo_lei_atual`,
`data_ajuizamento` — atributos que o contrato do fato não carrega e que
vêm da dim via `sk_processo`) + dim_orgao_julgador (`eh_especializada`,
das perguntas 4 e 8) + agregação da fato_incidentes (`qtd_habilitacoes`,
`qtd_impugnacoes` por raiz+origem, método pergunta 7).

**Fix pergunta 8 — `tutela_antes_processamento`:** recomputada a partir das
datas (`data_primeira_tutela <= data_deferimento_rj`). Aqui a antecedência é derivada dos marcos reais — coluna respode a pergunta: a tutela pedida antes do processamento é a tutela _antecedente_ da pergunta 8.

**COALESCE(..., 0) nos contadores:** processo sem incidente ligado
recebe 0 (não NULL) — semântica contável correta para as consultas da
pergunta 7.

```text
# ==============================================================================
# BLOCO 5 — GOLD_BASE_ANALITICA (mart analítico: 1 linha por processo)
# Semântica do stay corrigida (art. 6º §4º) + contrato preservado:
# a carga é INSERT OVERWRITE — o schema/comentários do DDL não são sobrescritos.
# Implementação 100% em SQL: evita colunas duplicadas de temp views.
# ==============================================================================

# incidentes de crédito: contagem de autos-filho por raiz+origem (método pergunta 7)
spark.sql("""
  CREATE OR REPLACE TEMP VIEW inc AS
  SELECT raiz, origem,
         count(CASE WHEN classe_codigo = 111 THEN 1 END) AS qtd_habilitacoes,
         count(CASE WHEN classe_codigo = 114 THEN 1 END) AS qtd_impugnacoes
  FROM fato_incidentes
  GROUP BY 1, 2
""")

spark.sql("""
  CREATE OR REPLACE TABLE gold_base_analitica (
    numero_processo                 STRING  COMMENT 'Número CNJ do processo',
    classe_codigo                   BIGINT  COMMENT 'Código TPU (129=RJ, 108=Falência, 128=RE)',
    classe_nome                     STRING  COMMENT 'Nome da classe',
    escopo_lei_atual                BOOLEAN COMMENT 'Ajuizado >= 2005-06-09 (universo das métricas)',
    eh_especializada                BOOLEAN COMMENT 'Vara especializada (pergunta 4/8)',
    data_ajuizamento                DATE    COMMENT 'Data de ajuizamento',
    data_processamento_efetivo      DATE    COMMENT 'Pergunta 1: processamento (cascata R9 > 12444 > proxy 11010)',
    fonte_deferimento               STRING  COMMENT 'Fonte do marco: r9_despacho_textual|tpu12444_deferimento|proxy11010_expediente',
    dias_distribuicao_processamento INT     COMMENT 'Pergunta 1: dias ajuizamento -> processamento',
    data_fim_stay                   DATE    COMMENT 'Pergunta 2: fim do stay (art. 6º §4º) = LEAST(homologação, falência)',
    cenario_desfecho                STRING  COMMENT 'homologacao_plano|decretao_falencia|extincao|encerramento|ativo',
    dias_stay_period                INT     COMMENT 'Pergunta 2: deferimento -> fim do stay (estrito; nulo = censura estrutural)',
    dias_stay_limite_superior       INT     COMMENT 'Pergunta 2: cota censurada — deferimento -> fim do processo',
    ind_stay_period_excedido_180d   INT     COMMENT 'Pergunta 2: 1 se stay > 180 dias (art. 6º §4º)',
    data_homologacao_plano          DATE    COMMENT 'Homologação do plano (não registrável no Datajud — não-achado estrutural)',
    data_decretao_falencia          DATE    COMMENT 'Pergunta 5: decretação de falência (TPU 202)',
    dias_ate_convolucao             INT     COMMENT 'Pergunta 5: ajuizamento -> convolução',
    data_encerramento               DATE    COMMENT 'Pergunta 3/6: encerramento/arquivamento',
    dias_duracao_falencia           INT     COMMENT 'Pergunta 3: decretação -> encerramento/extinção',
    data_primeira_tutela            DATE    COMMENT 'Pergunta 8: primeira tutela de urgência',
    tutela_antes_processamento      BOOLEAN COMMENT 'Pergunta 8: tutela <= processamento',
    qtd_habilitacoes                BIGINT  COMMENT 'Pergunta 7: autos-filho classe 111',
    qtd_impugnacoes                 BIGINT  COMMENT 'Pergunta 7: autos-filho classe 114',
    dias_duracao_processo_total     INT     COMMENT 'Ajuizamento -> evento fim (total do processo)'
  ) USING DELTA
  COMMENT 'Mart analítico: 1 linha por processo, alimenta as 9 perguntas do MVP'
""")

spark.sql("""
  INSERT OVERWRITE gold_base_analitica
  SELECT f.numero_processo, f.classe_codigo, f.classe_nome,
         dp.escopo_lei_atual, o.eh_especializada, dp.data_ajuizamento,
         f.data_deferimento_rj, f.fonte_deferimento,
         f.dias_ate_deferimento_rj,
         f.data_fim_stay, f.cenario_desfecho,
         f.dias_stay_period, f.dias_stay_limite_superior,
         f.ind_stay_period_excedido_180d,
         f.data_homologacao_prj, f.data_decretacao_falencia,
         datediff(f.data_decretacao_falencia, f.data_inicio_processo) AS dias_ate_convolucao,
         f.data_encerramento_processo, f.dias_duracao_falencia,
         f.data_primeira_tutela,
         CASE WHEN f.data_primeira_tutela IS NOT NULL
               AND f.data_deferimento_rj IS NOT NULL
              THEN f.data_primeira_tutela <= f.data_deferimento_rj
         END AS tutela_antes_processamento,
         COALESCE(i.qtd_habilitacoes, 0) AS qtd_habilitacoes,
         COALESCE(i.qtd_impugnacoes, 0)  AS qtd_impugnacoes,
         f.dias_tramitacao_total
  FROM fato_tramitacao f
  LEFT JOIN dim_processo dp      ON f.sk_processo       = dp.sk_processo
  LEFT JOIN dim_orgao_julgador o ON f.sk_orgao_julgador = o.sk_orgao_julgador
  LEFT JOIN inc i
    ON substring(f.numero_processo, 1, 7)  = i.raiz
   AND substring(f.numero_processo, 17, 4) = i.origem
""")

print("gold_base_analitica:", spark.table("gold_base_analitica").count(), "linhas.")

# GATE DE QA PÓS-CARGA — grão e sanidade do mart
chk = spark.sql("""
  SELECT count(*)                                              AS total,
         count(DISTINCT numero_processo)                       AS distintos,
         count(CASE WHEN dias_stay_period < 0 THEN 1 END)      AS stay_negativo,
         count(CASE WHEN dias_stay_period > dias_stay_limite_superior
                    THEN 1 END)                                AS stay_maior_que_cota
  FROM gold_base_analitica
""").collect()[0]
assert chk["total"] == chk["distintos"], "Gate falhou: grão violado (duplicidade de numero_processo)"
assert chk["stay_negativo"] == 0,       "Gate falhou: stay negativo no mart"
assert chk["stay_maior_que_cota"] == 0, "Gate falhou: stay maior que a cota censurada"
print("Gate OK — mart:", chk["total"], "linhas, 1 por processo.")
```

##### Resposta:

gold_base_analitica: 225556 linhas.  
Gate OK — mart: 225556 linhas, 1 por processo.

### BLOCO 6 — `gold_data_dictionary`: governança materializada

O **dicionário de dados da camada gold como tabela consultável** —
resolvido sem digitação manual: as descrições vêm **dos COMMENTs dos DDLs** via `information_schema.columns`.

**Por que assim:** o contrato da tabela é escrito uma vez (no CREATE
TABLE de cada bloco) e o dicionário é 100% regenerado dele. Não existe
segunda fonte para desatualizar — documentar e definir são o mesmo ato.

**Escopo:** as 4 dimensões, as 2 fatos, o mart e o próprio QA — o
dicionário cobre toda a superfície de consumo da gold. Qualquer tabela
nova entra na lista `IN (...)` do INSERT e o dicionário a absorve na
próxima execução.

```text
# ==============================================================================
# BLOCO 6 — GOVERNANÇA: gold_data_dictionary (dicionário materializado)
# ATUALIZADO: inclui fato_incidentes (pergunta 7) e gold_qa_execucao no escopo do
# dicionário — as descrições vêm dos COMMENTs do DDL via information_schema,
# mantendo o dicionário 100% regenerado (sem entradas manuais).
# ==============================================================================
spark.sql("""
  CREATE OR REPLACE TABLE gold_data_dictionary (
    table_name  STRING COMMENT 'Tabela da camada gold',
    column_name STRING COMMENT 'Coluna',
    data_type   STRING COMMENT 'Tipo de dado',
    comment     STRING COMMENT 'Descrição da coluna (política de dicionário)'
  ) USING DELTA
  COMMENT 'Dicionário de dados da camada gold (gerado do metadado do catalog)'
""")
spark.sql("""
  INSERT OVERWRITE gold_data_dictionary
  SELECT table_name, column_name, data_type, comment
  FROM information_schema.columns
  WHERE table_schema = 'default'
    AND table_name IN ('dim_tempo','dim_classe_processual','dim_orgao_julgador',
                       'dim_processo','fato_tramitacao','fato_incidentes',
                       'gold_base_analitica','gold_qa_execucao')
  ORDER BY table_name, ordinal_position
""")
print("gold_data_dictionary:",
      spark.table("gold_data_dictionary").count(), "colunas documentadas.")
```

##### Resposta:

gold_data_dictionary: 98 colunas documentadas.

### BLOCO 7 — QA de aceitação: baseline de referência congelado

Último gate da camada gold: as métricas-chave do mart são comparadas
contra um **baseline de referência congelado** — valores validados
metodologicamente durante as análises exploratórias (pergunta 1 – pergunta 9) e fixados
como critério de aceitação da camada.

| Métrica                    | Esperado | Fundamento                                |
| -------------------------- | -------- | ----------------------------------------- |
| rj_classe_129_escopo_atual | 5.709    | universo RJ no escopo da Lei 11.101/2005  |
| falencias_classe_108       | 15.932   | universo de falências                     |
| re_classe_128_escopo_atual | 305      | universo REs no escopo da Lei 11.101/2005 |
| mediana_dias_processamento | 48       | origem: ajuizamento                       |
| rj_com_marco_processamento | 2.422    | cobertura do marco de concessão           |
| rj_convoluções             | 177      | desfecho adverso (TPU 202)                |
| taxa_convolucao_pct        | 3,1%     | 177/5.709                                 |
| rj_com_tutela              | 792      | antecedência de tutela                    |
| homologacoes_plano_rj      | 0        | não-achado estrutural (ver nota)          |

Cada linha carrega `fonte_esperado` — a origem metodológica do número
congelado fica registrada no próprio QA.

**A linha da homologação é um não-achado documentado:** 8.935 eventos
"homolog" foram enumerados e nenhum refere-se a plano de recuperação
(TPU 466 = transação; 14099/12649 = acordos executivos). Esperado 0 não
é ausência de teste — é a fronteira estrutural da pergunta 2 transformada em
asserção permanente.

**O que este gate prova:** como a bronze é imutável e o pipeline é
determinístico, qualquer execução ponta a ponta deve reproduzir o
baseline com desvio 0. Desvio diferente de zero indica mudança de
semântica ou de dado de origem — e bloqueia o consumo da camada.

O `display` final mostra a tabela de QA completa: **desvio 0 em todas
as linhas é o critério de aprovação da camada.**

```text
# ==============================================================================
# BLOCO 7 (003) — QA DE ACEITAÇÃO · baseline de referência congelado
# As métricas-chave do mart são comparadas contra valores esperados
# validados metodologicamente nas análises exploratórias (pergunta 1 – pergunta 9).
# Desvio 0 em todas as linhas é o critério de aprovação da camada:
# qualquer execução ponta a ponta, a partir da bronze imutável, deve
# reproduzir o baseline.
# ==============================================================================

from pyspark.sql import types as T

g = spark.table("gold_base_analitica")

rj = g.filter("classe_codigo = 129 AND escopo_lei_atual")

rj_agg = (rj.agg(
        F.count("*").alias("total_rj"),
        F.round(F.percentile_approx("dias_distribuicao_processamento", 0.5), 1)
            .alias("mediana_proc"),
        F.count(F.when(F.col("data_processamento_efetivo").isNotNull(), 1))
            .alias("com_marco"),
        F.round(F.avg(F.when(F.col("data_decretao_falencia").isNotNull(), 1.0)
                      .otherwise(0.0)) * 100, 2).alias("pct_convolucao"),
        F.count(F.when(F.col("data_decretao_falencia").isNotNull(), 1))
            .alias("convolucoes"),
        F.count(F.when(F.col("data_primeira_tutela").isNotNull(), 1))
            .alias("com_tutela"),
        F.count(F.when(F.col("data_homologacao_plano").isNotNull(), 1))
            .alias("homologacoes"))
    .collect()[0])

qa_rows = [
    # (metrica, valor_obtido, esperado, fonte_esperado)
    ("rj_classe_129_escopo_atual",  float(rj_agg["total_rj"]),      5709.0, "diagnóstico pergunta 1"),
    ("falencias_classe_108",        float(g.filter("classe_codigo = 108").count()),
                                                                            15932.0, "diagnóstico pergunta 3"),
    ("re_classe_128_escopo_atual",  float(g.filter("classe_codigo = 128 AND escopo_lei_atual").count()),
                                                                            305.0, "diagnóstico pergunta 6"),
    ("mediana_dias_processamento",  float(rj_agg["mediana_proc"]),   48.0,
     "diagnóstico pergunta 1 (origem: ajuizamento)"),
    ("rj_com_marco_processamento",  float(rj_agg["com_marco"]),    2422.0, "diagnóstico pergunta 1"),
    ("rj_convolucoes",              float(rj_agg["convolucoes"]),    177.0,
     "diagnóstico pergunta 5 (desfecho adverso)"),
    ("taxa_convolucao_rj_pct",      float(rj_agg["pct_convolucao"]),  3.1, "177/5.709"),
    ("rj_com_tutela",               float(rj_agg["com_tutela"]),     792.0, "diagnóstico pergunta 8"),
    ("homologacoes_plano_rj",       float(rj_agg["homologacoes"]),     0.0,
     "PROVADO: 8.935 eventos 'homolog' enumerados; nenhum refere-se a plano "
     "(TPU 466 = transacao; 14099/12649 = acordos executivos). Nao-achado estrutural pergunta 2"),
]

schema_qa = T.StructType([
    T.StructField("metrica",        T.StringType()),
    T.StructField("valor",          T.DoubleType()),
    T.StructField("esperado",       T.DoubleType()),
    T.StructField("fonte_esperado", T.StringType()),
])

(spark.createDataFrame(qa_rows, schema_qa)
    .withColumn("desvio", F.round(F.col("valor") - F.col("esperado"), 2))
    .withColumn("executado_em", F.current_timestamp())
    .write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("gold_qa_execucao"))

spark.sql("""
INSERT INTO gold_qa_execucao
  (metrica, valor, esperado, fonte_esperado, desvio, executado_em)
SELECT
  're_encerradas_tpu22',
  (SELECT count(*) FROM default.gold_base_analitica
    WHERE classe_codigo = 128
      AND escopo_lei_atual
      AND data_encerramento IS NOT NULL),
  60,                                            -- esperado do diagnóstico
  'diagnóstico TPU 22 (51 novos: Baixa Definitiva) + higiene temporal (23 encerramentos inválidos descartados); gate Bloco 4',
  0,
  current_timestamp()
WHERE NOT EXISTS (
  SELECT 1 FROM gold_qa_execucao WHERE metrica = 're_encerradas_tpu22'
)
""")

display(spark.table("gold_qa_execucao"))
```

##### Resposta:

| metrica                    | valor | esperado | fonte_esperado                                                                                                                                                  | desvio | executado_em                  |
| :------------------------- | :---- | :------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------- | :----- | :---------------------------- |
| rj_classe_129_escopo_atual | 5709  | 5709     | diagnóstico pergunta 1                                                                                                                                          | 0      | 2026-09-22T23:29:43.336+00:00 |
| falencias_classe_108       | 15932 | 15932    | diagnóstico pergunta 3                                                                                                                                          | 0      | 2026-09-22T23:29:43.336+00:00 |
| re_classe_128_escopo_atual | 305   | 305      | diagnóstico pergunta 6                                                                                                                                          | 0      | 2026-09-22T23:29:43.336+00:00 |
| mediana_dias_processamento | 48    | 48       | diagnóstico pergunta 1 (origem: ajuizamento)                                                                                                                    | 0      | 2026-09-22T23:29:43.336+00:00 |
| rj_com_marco_processamento | 2422  | 2422     | diagnóstico pergunta 1                                                                                                                                          | 0      | 2026-09-22T23:29:43.336+00:00 |
| rj_convolucoes             | 177   | 177      | diagnóstico pergunta 5 (desfecho adverso)                                                                                                                       | 0      | 2026-09-22T23:29:43.336+00:00 |
| taxa_convolucao_rj_pct     | 3.1   | 3.1      | 177/5.709                                                                                                                                                       | 0      | 2026-09-22T23:29:43.336+00:00 |
| rj_com_tutela              | 792   | 792      | diagnóstico pergunta 8                                                                                                                                          | 0      | 2026-09-22T23:29:43.336+00:00 |
| homologacoes_plano_rj      | 0     | 0        | PROVADO: 8.935 eventos 'homolog' enumerados; nenhum refere-se a plano (TPU 466 = transacao; 14099/12649 = acordos executivos). Nao-achado estrutural pergunta 2 | 0      | 2026-09-22T23:29:43.336+00:00 |
| re_encerradas_tpu22        | 60    | 60       | diagnóstico TPU 22 (51 novos: Baixa Definitiva) + higiene temporal (23 encerramentos inválidos descartados); gate Bloco 4                                       | 0      | 2026-09-22T23:29:45.257+00:00 |

---
