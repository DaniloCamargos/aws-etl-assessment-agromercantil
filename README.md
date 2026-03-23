# 🌾 aws-etl-assessment-agromercantil

Pipeline de coleta, tratamento e normalização de dados de commodities agrícolas do **CEPEA/ESALQ-USP**, desenvolvido como desafio técnico de ETL.

---

## ⚠️ Avisos importantes

> **Variáveis de ambiente**: o arquivo `.env` **não está versionado** por conter credenciais de banco de dados. Consulte a seção [Configuração](#configuração) para montar o seu.

> **Cloudflare**: o site do CEPEA utiliza proteção anti-bot. O script de extração usa `undetected-chromedriver` para contornar o bloqueio. É necessário ter o **Google Chrome instalado** na máquina.

> **Tempo de execução**: a extração completa das 21 commodities leva aproximadamente **90 minutos** devido aos sleeps necessários para evitar bloqueios.

---

## 🗂️ Estrutura do projeto

```
aws-etl-assessment-agromercantil/
├── src/
│   ├── extracao_commodities.py     # Etapa 1 e 2 — coleta via Selenium
│   ├── insert_todos_csv.py         # Etapa 4 — ETL e carga na camada processed
│   └── normalizar_inserir.py       # Etapa 3 — tabelas normalizadas (analytics)
├── inputs/
│   ├── csv/                        # Arquivos .csv gerados pela extração
│   └── json/                       # Arquivos .json gerados pela extração
├── outputs/                        # Arquivos de saída adicionais
├── respostas/                      # Documentação e respostas das etapas
├── notebooks/                      # Exploração e testes
├── db/                             # Scripts SQL auxiliares
├── .env                            # ⚠️ Não versionado — ver seção Configuração
├── .gitignore
├── requirements.txt
└── venv/
```

---

## ⚙️ Configuração

Crie um arquivo `.env` na raiz do projeto com as seguintes variáveis:

```env
CONEXAO=postgresql://usuario:senha@localhost:5432/postgres
CSV_DIR=C:\caminho\para\inputs\csv
JSON_DIR=C:\caminho\para\inputs\json
COMMODITIES=acucar,algodao,arroz,bezerro,boi-gordo,cafe,citros,etanol,feijao,florestal,frango,hortifruti,leite,mandioca,milho,ovinos,ovos,soja,suino,tilapia,trigo
SLEEP_CLOUDFLARE=12
SLEEP_RENDER=2
SLEEP_REINICIO=5
WEBDRIVER_TIMEOUT=20
SCHEMA=processed
```

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

## 🗄️ Modelo de dados

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

Projeto desenvolvido para fins de avaliação técnica. Dados fornecidos pelo **CEPEA/ESALQ-USP** — uso acadêmico e de pesquisa.