#importando as bibliotecas
import pandas as pd
from datetime import datetime
import time
import undetected_chromedriver as uc
import multiprocessing
import os
import tempfile
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

#configura o chrome para passar pelo bloqueio
def criar_driver():
    #cria um diretório temporário único para o perfil do Chrome
    #isso engana o Cloudflare como se fosse uma instalação limpa
    perfil_dir = os.path.join(tempfile.gettempdir(), 'cepea_profile_' + str(time.time()))

    options = uc.ChromeOptions()
    options.add_argument(f'--user-data-dir={perfil_dir}')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    #argumento crítico: evita que o site detecte o protocolo de automação
    options.add_argument('--disable-blink-features=AutomationControlled')

    #pra não crashar no terminal
    driver = uc.Chrome(options=options, use_subprocess=True)
    return driver

#limpando e convertendo os números
def limpar_numero(texto):
    texto_limpo = texto.replace('.', '').replace(',', '.').replace('%', '').strip()
    try:
        return float(texto_limpo)
    except ValueError:
        #retorna nulo para o pandas se é '-' ou 'nd'
        return None

def executar_extracao_total():

    #carregando configurações do .env
    CONEXAO          = os.getenv("CONEXAO")
    CSV_DIR          = os.getenv("CSV_DIR")
    JSON_DIR         = os.getenv("JSON_DIR")
    SLEEP_CLOUDFLARE = int(os.getenv("SLEEP_CLOUDFLARE", 12))
    SLEEP_RENDER     = int(os.getenv("SLEEP_RENDER", 2))
    SLEEP_REINICIO   = int(os.getenv("SLEEP_REINICIO", 5))
    TIMEOUT          = int(os.getenv("WEBDRIVER_TIMEOUT", 20))

    if not CONEXAO:  raise ValueError("variável CONEXAO não definida no .env")
    if not CSV_DIR:  raise ValueError("variável CSV_DIR não definida no .env")
    if not JSON_DIR: raise ValueError("variável JSON_DIR não definida no .env")

    #lista completa de commodities disponíveis no CEPEA
    COMMODITIES = os.getenv("COMMODITIES", "").split(",")
    if not COMMODITIES or COMMODITIES == [""]:
        raise ValueError("variável COMMODITIES não definida no .env")

    if not os.path.exists(CSV_DIR):  os.makedirs(CSV_DIR)
    if not os.path.exists(JSON_DIR): os.makedirs(JSON_DIR)

    engine = create_engine(CONEXAO)

    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw_commodity;"))
        conn.commit()

    #loop principal: só para quando a lista de commodities esvaziar
    while COMMODITIES:
        driver = None
        try:
            print(f"iniciando motor. restam {len(COMMODITIES)} commodities para processar.")
            driver = criar_driver()

            #usamos list(COMMODITIES) para poder remover itens da original durante o loop
            for item in list(COMMODITIES):
                try:
                    url = f"https://www.cepea.org.br/br/indicador/{item}.aspx"
                    print(f"--- processando: {item.upper()} ---")

                    driver.get(url)

                    #aguarda o cloudflare: o sleep dá tempo do sistema de segurança validar o uc.Chrome
                    time.sleep(SLEEP_CLOUDFLARE)

                    #tenta rolar a página para parecer humano
                    driver.execute_script("window.scrollTo(0, 400);")

                    #esperar até a tabela aparecer, garante que o cloudflare já carregou
                    WebDriverWait(driver, TIMEOUT).until(
                        EC.presence_of_element_located((By.CLASS_NAME, "imagenet-table"))
                    )

                    #pausa para renderizar os dados
                    time.sleep(SLEEP_RENDER)

                    #captura o html final renderizado
                    html = driver.page_source

                    #referenciando as tabelas
                    soup    = BeautifulSoup(html, 'html.parser')
                    tabelas = soup.find_all('table', class_='imagenet-table')

                    for i, tabela in enumerate(tabelas, 1):
                        corpo = tabela.find('tbody')
                        if not corpo: continue

                        #extrair o local do título da tabela (ex: PARANAGUÁ, PARANÁ)
                        bloco  = tabela.find_parent().find_parent().find_parent()
                        titulo = bloco.find('div', class_='imagenet-table-titulo') if bloco else None
                        if titulo:
                            texto_titulo = titulo.get_text(strip=True)
                            local = texto_titulo.split('-')[-1].strip().title()
                        else:
                            local = 'nao informado'

                        linhas      = corpo.find_all('tr')
                        lista_bruta = []

                        print(f"foram encontradas {len(linhas)} linhas de dados ({item} - {local} - tabela {i}).")

                        for linha in linhas:
                            #procura tanto <td> quanto <th> para evitar problemas estruturais
                            cols = linha.find_all(['td', 'th'])

                            if len(cols) < 4:
                                continue

                            data_str  = cols[0].get_text(strip=True)
                            valor_brl = limpar_numero(cols[1].get_text(strip=True))
                            variacao  = limpar_numero(cols[2].get_text(strip=True))
                            valor_usd = limpar_numero(cols[3].get_text(strip=True))

                            #só insere a linha se tiver pelo menos a data válida e o valor em R$
                            if valor_brl is not None and data_str != "":
                                lista_bruta.append({
                                    'data_referencia'     : data_str
                                    ,'valor_brl'           : valor_brl
                                    ,'variacao_diaria_pct' : variacao
                                    ,'valor_usd'           : valor_usd
                                    ,'local'               : local
                                    ,'data_extracao'       : datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                })

                        if lista_bruta:
                            df = pd.DataFrame(lista_bruta)

                            #convertendo tipos antes de enviar ao banco
                            df['data_referencia']     = pd.to_datetime(df['data_referencia'], dayfirst=True)
                            df['data_extracao']       = pd.to_datetime(df['data_extracao'])
                            df['valor_brl']           = df['valor_brl'].astype(float)
                            df['valor_usd']           = df['valor_usd'].astype(float)
                            df['variacao_diaria_pct'] = df['variacao_diaria_pct'].astype(float)

                            sufixo      = f"_{i}" if len(tabelas) > 1 else ""
                            nome_tabela = f"raw_{item.replace('-', '_')}{sufixo}"

                            #exportando os dados brutos na camada raw
                            arquivo_csv  = f"{nome_tabela}.csv"
                            arquivo_json = f"{nome_tabela}.json"

                            df.to_csv(
                                os.path.join(CSV_DIR, arquivo_csv)
                                ,index=False
                                ,sep=';'
                                ,encoding='utf-8'
                            )

                            df.to_json(
                                os.path.join(JSON_DIR, arquivo_json)
                                ,orient='records'
                                ,force_ascii=False
                                ,indent=4
                            )

                            df.to_sql(
                                name=nome_tabela
                                ,con=engine
                                ,schema='raw_commodity'
                                ,if_exists='replace'
                                ,index=False
                            )

                            print(f"dados salvos em '{arquivo_csv}' e '{arquivo_json}'.")

                    #se chegou aqui sem erro, remove da lista de pendências
                    COMMODITIES.remove(item)

                except Exception as e:
                    #se o erro for de janela fechada, força a reinicialização do driver
                    if "no such window" in str(e).lower() or "target window already closed" in str(e).lower():
                        print(f"janela fechada {item}. reiniciando")
                        raise ConnectionError("Browser Crash")

                    print(f"erro em {item}: {e}")
                    continue

        except (ConnectionError, Exception) as e:
            print(f"reiniciando instância do Chrome após erro fatal: {e}")
            if driver:
                try: driver.quit()
                except: pass
            if not COMMODITIES:
                break
            time.sleep(SLEEP_REINICIO)
            continue
        finally:
            #garante que o navegador será fechado mesmo se der erro
            if driver:
                try: driver.quit()
                except: pass

        #sai do while se a lista estiver vazia
        if not COMMODITIES:
            break

    print(f"todas as commodities foram processadas e salvas")

if __name__ == "__main__":
    multiprocessing.freeze_support()
    executar_extracao_total()