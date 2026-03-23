#importando as bibliotecas
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()

db_url  = os.getenv("CONEXAO")
csv_dir = os.getenv("CSV_DIR")

if not db_url:  raise ValueError("variável CONEXAO não definida no .env")
if not csv_dir: raise ValueError("variável CSV_DIR não definida no .env")

engine = create_engine(db_url)

with engine.connect() as conn:
    print("conectado")

def criar_tabelas():
    """
    cria o schema normalizado com 3 tabelas:

    dim_commodity (dimensão)
        pk: id_commodity — surrogate key gerada pelo banco (serial)
        Justificativa: cada commodity é uma entidade única identificada
        pelo slug (ex: 'soja', 'milho'). Usamos surrogate key para
        desacoplar a FK da fato_preco de mudanças no nome.

    dim_regiao (dimensão)
        pk: id_regiao — surrogate key gerada pelo banco (serial)
        Justificativa: o local (ex: 'Paranaguá', 'Paraná') é uma
        entidade reutilizável por diferentes commodities. Surrogate
        key evita duplicar strings longas na tabela fato.

        
    fato_preco (fato)
        pk: id — surrogate key gerada pelo banco (serial)
        fk: id_commodity → dim_commodity.id_commodity
        fk: id_regiao    → dim_regiao.id_regiao
        justificativa: a granularidade é (commodity, regiao, data).
        As FKs garantem integridade referencial — não é possível
        inserir um preço de uma commodity ou região inexistente.
    """
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS analytics;"))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS analytics.dim_commodity (
                id_commodity SERIAL PRIMARY KEY,
                nome VARCHAR(100) NOT NULL,
                slug VARCHAR(100) NOT NULL UNIQUE
            );
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS analytics.dim_regiao (
                id_regiao SERIAL PRIMARY KEY,
                nome VARCHAR(100) NOT NULL,
                estado VARCHAR(100)
            );
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS analytics.fato_preco (
                id SERIAL PRIMARY KEY,
                id_commodity INT  NOT NULL REFERENCES analytics.dim_commodity(id_commodity),
                id_regiao INT  NOT NULL REFERENCES analytics.dim_regiao(id_regiao),
                dt_ref DATE NOT NULL,
                val_brl FLOAT,
                val_usd FLOAT,
                pct_var_dia FLOAT,
                dt_ext TIMESTAMP
            );
        """))

        conn.commit()
        print("tabelas criadas no schema analytics.")

def limpar_numero(texto):
    if pd.isna(texto): return None
    texto_limpo = str(texto).replace('.', '').replace(',', '.').replace('%', '').strip()
    try:
        return float(texto_limpo)
    except ValueError:
        return None

def inserir_normalizado():

    arquivos = [f for f in os.listdir(csv_dir) if f.endswith('.csv')] #type:ignore

    if not arquivos:
        print("nenhum arquivo csv encontrado.")
        return

    print(f"foram encontrados {len(arquivos)} arquivos para normalizar.")

    for arquivo in arquivos:
        caminho     = os.path.join(csv_dir, arquivo) #type:ignore
        nome_tabela = os.path.splitext(arquivo)[0]

        #extrair slug e sufixo do nome do arquivo (ex: raw_boi_gordo_1)
        slug = nome_tabela.replace('raw_', '')
        slug = '_'.join([p for p in slug.split('_') if not p.isdigit()])

        try:
            df = pd.read_csv(
                caminho
                ,sep=';'
                ,encoding='utf-8'
                ,on_bad_lines='skip'
                ,engine='python'
            )

            if df.empty:
                print(f"arquivo vazio, pulando: {arquivo}")
                continue

            #normalizar colunas
            df.columns = [c.lower().strip() for c in df.columns]

            if 'data_referencia' in df.columns:
                df['data_referencia'] = pd.to_datetime(df['data_referencia'], dayfirst=True, errors='coerce')
            if 'data_extracao' in df.columns:
                df['data_extracao'] = pd.to_datetime(df['data_extracao'], errors='coerce')
            if 'valor_brl' in df.columns:
                df['valor_brl'] = pd.to_numeric(df['valor_brl'], errors='coerce')
            if 'valor_usd' in df.columns:
                df['valor_usd'] = pd.to_numeric(df['valor_usd'], errors='coerce')
            if 'variacao_diaria_pct' in df.columns:
                df['variacao_diaria_pct'] = pd.to_numeric(df['variacao_diaria_pct'], errors='coerce')

            #remover linhas completamente vazias
            df = df.dropna(how='all').reset_index(drop=True)

            with engine.connect() as conn:

                #inserir ou recuperar dim_commodity
                nome_commodity = slug.replace('_', ' ').title()
                conn.execute(text("""
                    INSERT INTO analytics.dim_commodity (nome, slug)
                    VALUES (:nome, :slug)
                    ON CONFLICT (slug) DO NOTHING;
                """), {"nome": nome_commodity, "slug": slug})
                conn.commit()

                id_commodity = conn.execute(text(
                    "SELECT id_commodity FROM analytics.dim_commodity WHERE slug = :slug"
                ), {"slug": slug}).scalar()

                #inserir ou recuperar dim_regiao por linha
                local_col = 'local' if 'local' in df.columns else None

                for _, row in df.iterrows():
                    local = row[local_col] if local_col else 'nao informado'
                    if pd.isna(local): local = 'nao informado'

                    conn.execute(text("""
                        INSERT INTO analytics.dim_regiao (nome, estado)
                        VALUES (:nome, :estado)
                        ON CONFLICT DO NOTHING;
                    """), {"nome": str(local), "estado": str(local)})
                    conn.commit()

                    id_regiao = conn.execute(text(
                        "SELECT id_regiao FROM analytics.dim_regiao WHERE nome = :nome LIMIT 1"
                    ), {"nome": str(local)}).scalar()

                    #inserir fato_preco
                    conn.execute(text("""
                        INSERT INTO analytics.fato_preco
                            (id_commodity, id_regiao, dt_ref, val_brl, val_usd, pct_var_dia, dt_ext)
                        VALUES
                            (:id_commodity, :id_regiao, :dt_ref, :val_brl, :val_usd, :pct_var_dia, :dt_ext)
                        ON CONFLICT DO NOTHING;
                    """), {
                        "id_commodity": id_commodity,
                        "id_regiao": id_regiao,
                        "dt_ref": row.get('data_referencia'),
                        "val_brl": row.get('valor_brl'),
                        "val_usd": row.get('valor_usd'),
                        "pct_var_dia": row.get('variacao_diaria_pct'),
                        "dt_ext": row.get('data_extracao'),
                    })
                    conn.commit()

            print(f"inserido: {nome_tabela} ({len(df)} linhas)")

        except Exception as e:
            print(f"erro ao processar {arquivo}: {e}")
            continue

    print("normalização concluída.")

if __name__ == "__main__":
    criar_tabelas()
    inserir_normalizado()