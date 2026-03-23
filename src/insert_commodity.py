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

def tratar_dados(df):

    if df is None or df.empty:
        return None
    
    #renomeando colunas
    df.rename(columns={
        "data_referencia": "dt_ref"
        ,"valor_brl": "val_brl"
        ,"variacao_diaria_pct": "pct_var_dia"
        ,"valor_usd": "val_usd"
        ,"local":"local"
        ,"data_extracao": "dt_ext"
    }, inplace=True)

    if 'data_ref' in df.columns:
        df['data_ref'] = pd.to_datetime(df['data_ref'], dayfirst=True, errors='coerce')
    if 'data_ext' in df.columns:
        df['data_ext'] = pd.to_datetime(df['data_ext'], errors='coerce')
    if 'val_brl' in df.columns:
        df['val_brl'] = pd.to_numeric(df['val_brl'], errors='coerce')
    if 'val_usd' in df.columns:
        df['val_usd'] = pd.to_numeric(df['val_usd'], errors='coerce')
    if 'pct_var_dia' in df.columns:
        df['pct_var_dia'] = pd.to_numeric(df['pct_var_dia'], errors='coerce')

    #tratamento de valores nulos ou ausentes
    for col in df.select_dtypes(include=['float64', 'int64']).columns:
        df[col] = df[col].fillna(0)

    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].fillna('nao informado').str.strip()

    for col in df.select_dtypes(include=['datetime64']).columns:
        df[col] = df[col].fillna(pd.NaT)

    #remover linhas onde todas as colunas são nulas
    df = df.dropna(how='all')

    #ordenar por data de referência
    if 'data_ref' in df.columns:
        df = df.sort_values('data_ref', ascending=False)

    #resetar índice
    df = df.reset_index(drop=True)

    return df

def insert_todos():

    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw_commodity;"))
        conn.commit()

    #lista todos os arquivos csv da pasta
    arquivos = [f for f in os.listdir(csv_dir) if f.endswith('.csv')] #type:ignore

    if not arquivos:
        print("nenhum arquivo csv encontrado na pasta.")
        return

    print(f"foram encontrados {len(arquivos)} arquivos para inserir.")

    #contador global para garantir ids únicos entre todos os arquivos
    contador = 1

    for arquivo in arquivos:
        caminho     = os.path.join(csv_dir, arquivo) #type:ignore
        nome_tabela = os.path.splitext(arquivo)[0]

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

            #aplicar tratamentos
            df = tratar_dados(df)

            if df is None:
                print(f"dataframe vazio após tratamento, pulando: {arquivo}")
                continue

            #gerar id sequencial global, sem repetição entre arquivos
            df.insert(0, 'commodity_id', range(contador, contador + len(df)))
            contador += len(df)

            #extrair nome do cultivo do nome do arquivo, como é global, fica em insert_todos()
            cultivo = nome_tabela.replace('raw_', '').split('_')[0]
            df.insert(1, 'commodity_name', cultivo)

            df.to_sql(
                name=nome_tabela
                ,con=engine
                ,schema='processed_commodity'
                ,if_exists='replace'
                ,index=False
            )

            print(f"inserido: {nome_tabela} ({len(df)} linhas)")

        except Exception as e:
            print(f"erro ao processar {arquivo}: {e}")
            continue

    print("processo concluído.")

if __name__ == "__main__":
    insert_todos()