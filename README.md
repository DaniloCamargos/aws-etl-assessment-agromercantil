# 🌾 aws-etl-assessment-agromercantil

Pipeline completo de **coleta, tratamento, modelagem e análise de dados de commodities agrícolas**, desenvolvido como avaliação técnica para vaga de **Analista de Dados (Web Scraping)**.

---

## 🧩 Mapeamento da Avaliação Técnica

Abaixo está a relação entre cada etapa solicitada na avaliação e sua implementação no projeto:

---

### 1. Coleta de Dados (Web Scraping)

📍 **Arquivo:**
- `src/extract_commodity.py`

📍 **Saída:**
- `inputs/csv/`
- `inputs/json/`

📍 **Descrição:**
- scraping com selenium
- tratamento de cloudflare
- extração de tabelas fragmentadas

---

### 2. Estruturação da Camada Raw

📍 **Diretório:**
- `inputs/csv`

📍 **Documentação:**
- `respostas/csv_json_parquet.txt`

📍 **Descrição:**
- armazenamento em csv e json
- explicação sobre formatos e uso em data lake

---

### 3. Criação de Tabelas no PostgreSQL

📍 **Arquivo:**
- `src/normalizar_commodity.py`

📍 **Banco:**
- schema `analytics`

📍 **Descrição:**
- criação de tabelas dimensionais e fato
- definição de pk e fk

---

### 4. Tratamento e ETL

📍 **Arquivo:**
- `src/insert_commodity.py`

📍 **Banco:**
- schema `processed`

📍 **Descrição:**
- limpeza de dados
- padronização
- conversão de tipos
- carga no banco

---

### 5. Estruturação do Data Lake

📍 **Documentação:**
- `respostas/estrutura_data_lake.txt`

📍 **Diretórios:**
- `inputs/` → raw
- `src/` → processed
- `outputs/` → curated

---

### 6. Análises SQL

📍 **Arquivos:**
- `db/6a.SQL`
- `db/6b.SQL`
- `db/6c.SQL`

📍 **Evidências:**
- `db/print_query_6a.png`
- `db/print_query_6b.png`
- `db/print_query_6c.png`

📍 **Descrição:**
- média mensal com lag
- top commodities
- detecção de anomalias

---

### 7. Otimização e Indexação

📍 **Documentação:**
- `respostas/otimizacao_indexacao.txt`

---

### 8. Análise Exploratória (Pandas)

📍 **Notebook:**
- `notebooks/analise_exploratoria_pandas.ipynb`

📍 **Saídas:**
- `outputs/*.png`
- `outputs/*.csv`

📍 **Descrição:**
- estatísticas descritivas
- detecção de outliers
- visualizações

---

### 9. Visualização (Streamlit)

📍 **Arquivo:**
- `src/app.py`

📍 **Descrição:**
- dashboard interativo
- filtros e gráficos

---

### 10. Insights e Documentação

📍 **Arquivo:**
- `README.md`

📍 **Descrição:**
- padrões encontrados
- aplicações no agronegócio
- limitações da fonte

---

## 🎯 Objetivo

Este projeto tem como objetivo demonstrar na prática:

- Coleta de dados via web scraping
- Estruturação em camadas (raw, processed, curated)
- Modelagem relacional no PostgreSQL
- Análises SQL e exploração com Pandas
- Visualização de dados com gráficos e Streamlit
- Documentação e geração de insights

---

## ⚠️ Avisos importantes

> **Variáveis de ambiente**: o arquivo `.env` não está oculto, para validação da criação das variáveis de ambiente.

> **Cloudflare**: o site do CEPEA possui proteção anti-bot, sendo necessário uso de automação com Selenium.

> **Tempo de execução**: a coleta completa pode levar ~5 minutos.

> **Tempo de execução**: todo o projeto respeita as normas LGPD. (https://www.cepea.org.br/br/licenca-de-uso-de-dados.aspx)

---

## 🗂️ Estrutura de pastas do projeto


```
aws-etl-assessment-agromercantil/

├── inputs/ # camada raw (dados brutos)
│ ├── csv/
│ └── json/

├── outputs/ # camada curated (dados analisados e visualizações)
│ ├── *.png
│ ├── *.csv

├── src/ # camada processed (etl e tratamento)
│ ├── extract_commodity.py
│ ├── insert_commodity.py
│ ├── normalizar_commodity.py
│ └── app.py

├── notebooks/ # analise exploratoria
├── db/ # consultas sql
├── respostas/ # documentacao teorica
├── .env
├── requirements.txt
└── README.md
```

---

## 🧱 Estrutura do Data Lake

| Camada     | Descrição |
|------------|----------|
| **raw**     | dados brutos em csv/json (sem tratamento) |
| **processed** | dados tratados e padronizados |
| **curated** | dados prontos para análise e visualização |

---

## 🚀 Como executar

```bash
# 1. Criar e ativar o ambiente virtual
python -m venv venv
venv\Scripts\activate        # Windows
source venv/bin/activate     # Linux/Mac

# 2. Instalar dependências
pip install -r requirements.txt

# 3. Extração dos dados (Etapas 1 e 2)
python src/extracao_commodities.py

# 4. ETL e carga na camada processed (Etapa 4)
python src/insert_todos_csv.py

# 5. Criação das tabelas normalizadas (Etapa 3)
python src/normalizar_inserir.py
```

---

## 📦 Stack utilizada

| Tecnologia | Uso |
|---|---|
| `Python 3.13` | Linguagem principal |
| `Selenium + undetected-chromedriver` | Automação do navegador e bypass do Cloudflare |
| `BeautifulSoup4` | Parsing do HTML |
| `Pandas` | Manipulação e transformação dos dados |
| `SQLAlchemy` | Conexão e inserção no PostgreSQL |
| `PostgreSQL` | Banco de dados relacional |
| `python-dotenv` | Gerenciamento de variáveis de ambiente |

---

## 📋 Etapas do desafio

### Etapa 1 e 2 — Captura de dados (`src/extracao_commodities.py`)

Coleta cotações históricas das 21 commodities disponíveis no **CEPEA/ESALQ-USP** (`https://www.cepea.org.br/br/indicador/`).

**Desafios de scraping tratados:**
- Proteção **Cloudflare** — contornada com `undetected-chromedriver` e perfil temporário isolado
- **Tabelas fragmentadas** — cada commodity tem até 2 tabelas com regiões distintas (ex: Paranaguá e Paraná para soja)
- **Extração do local** — capturado do elemento `div.imagenet-table-titulo` (ex: `INDICADOR DA SOJA CEPEA/ESALQ - PARANAGUÁ`)
- **Reinicialização automática** — se o Chrome travar ou a janela fechar, o driver é recriado e retoma de onde parou
- Saída em `.csv` e `.json` na pasta `inputs/`

---

### Etapa 3 — Criação de tabelas normalizadas (`src/normalizar_inserir.py`)

Cria o schema `analytics` com 3 tabelas normalizadas:

```
analytics.dim_commodity   → cadastro único de cada commodity
analytics.dim_regiao      → cadastro único de cada região/local
analytics.fato_preco      → cotações diárias com FKs para dimensões
```

**Justificativa das chaves:**

| Tabela | PK | Justificativa |
|---|---|---|
| `dim_commodity` | `id_commodity` SERIAL | Surrogate key desacopla FK do nome, que pode mudar |
| `dim_regiao` | `id_regiao` SERIAL | Surrogate key evita repetir strings longas na tabela fato |
| `fato_preco` | `id` SERIAL | Chave natural `(commodity, regiao, data)` pode ter duplicatas se o site atualizar o mesmo dia duas vezes |
| `fato_preco` | FK `id_commodity` | Garante integridade — não insere preço de commodity inexistente |
| `fato_preco` | FK `id_regiao` | Garante integridade — não insere preço de região inexistente |

---

### Etapa 4 — Tratamento e ETL (`src/insert_todos_csv.py`)

Lê todos os `.csv` de `inputs/csv/`, aplica transformações e carrega na camada `processed`.

**Transformações aplicadas:**

| Requisito | Implementação |
|---|---|
| Corrigir tipos de dados | `pd.to_datetime`, `pd.to_numeric`, `.astype(float)` |
| Tratar valores ausentes | `fillna(0)` para numéricos, `fillna('nao informado')` para texto, `dropna(how='all')` |
| Padronizar categorias | `.str.strip()`, `.title()` nos campos `cultivo` e `local` |
| Carregar no PostgreSQL | `df.to_sql(..., schema='processed', if_exists='replace')` |

---

### Etapa 5 — Insights e Documentação

#### Padrões identificados nos dados

- **Soja — Paranaguá vs Paraná**: a cotação de Paranaguá é consistentemente superior à do Paraná (diferença média de ~R$ 7/saca), refletindo o custo de frete do interior ao porto
- **Boi Gordo**: apresenta menor volatilidade diária em comparação com grãos, com variações geralmente abaixo de 1% ao dia
- **Variação USD**: commodities com maior exposição ao mercado externo (soja, milho, café) apresentam correlação mais forte entre `val_usd` e eventos cambiais
- **Sazonalidade**: os dados históricos do CEPEA mostram padrão de alta nos preços de soja entre março e maio, período de colheita no Brasil, quando há pressão de venda pelos produtores
- **Valores ausentes**: commodities como `hortifruti`, `florestal` e `ovinos` apresentam maior frequência de valores nulos em `val_usd`, pois são mercados predominantemente domésticos

#### Aplicações práticas para o agronegócio

- **Alertas de preço**: monitorar variações diárias acima de 2% para acionar notificações a produtores e tradings
- **Análise de basis**: diferença entre preço Paranaguá e Paraná pode indicar momento ideal de venda no interior vs porto
- **Hedge e contratos futuros**: histórico de variação diária alimenta modelos de risco para decisão de proteção via BM&F
- **Planejamento de plantio**: cruzar cotações históricas com dados climáticos para estimar rentabilidade por safra
- **Dashboard de gestão**: alimentar BI com a camada `analytics` para acompanhamento em tempo real por diretores rurais

#### Limitações da fonte (CEPEA/ESALQ-USP)

| Limitação | Impacto |
|---|---|
| Apenas ~15 registros por página sem paginação completa | Histórico limitado por extração — para séries longas é necessário baixar os `.xlsx` do CEPEA manualmente |
| Proteção Cloudflare | Extração frágil — mudanças no JS do site podem quebrar o scraper |
| Sem dados de volume negociado | Não é possível saber liquidez do mercado, apenas preço |
| Atualizações apenas em dias úteis | Fins de semana e feriados geram gaps na série temporal |
| Algumas commodities sem cotação USD | Limita comparações internacionais para produtos domésticos |

---

## 🗄️ Modelo de dados (star schema ou snowflake schema a depender da evolução e complexidade dos dados)

```
analytics.dim_commodity          analytics.dim_regiao
┌──────────────────────┐         ┌─────────────────────┐
│ id_commodity (PK)    │         │ id_regiao (PK)       │
│ nome                 │         │ nome                 │
│ slug                 │         │ estado               │
└──────────┬───────────┘         └──────────┬──────────┘
           │                                │
           └──────────────┬─────────────────┘
                          │
               analytics.fato_preco
               ┌─────────────────────────┐
               │ id (PK)                 │
               │ id_commodity (FK)       │
               │ id_regiao (FK)          │
               │ dt_ref                  │
               │ val_brl                 │
               │ val_usd                 │
               │ pct_var_dia             │
               │ dt_ext                  │
               └─────────────────────────┘
```

---

## 📄 Licença

Projeto desenvolvido para fins de avaliação técnica. Dados fornecidos pelo **CEPEA/ESALQ-USP** — uso acadêmico e de pesquisa (dados públicos, sem comprometimento das normas LGPD).
