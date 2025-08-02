import pandas as pd
import requests
import io
import logging
import json
import concurrent.futures
from typing import List, Dict, Optional

# --- Configurações e Constantes ---
BASE_URL = 'https://www.sped.fazenda.gov.br/spedtabelas/appconsulta/obterTabelaExterna.aspx'
API_URL = 'http://localhost:8080/codigos-ajustes-apuracao/sincronizar'
OUTPUT_FILENAME = 'codigos_ajustes_apuracao_final.csv'

# Constantes para limpeza de dados
DEFAULT_DATA_FIM = '31129999'
COLUNAS = ['cod_aj_apur', 'descricao', 'data_inicio', 'data_fim']

# Constantes para requisições
REQUEST_TIMEOUT = 30

DADOS_ESTADUAIS = [
    {"idPacote": 11, "UF": "Bahia", "idTabela": 190},
    {"idPacote": 21, "UF": "Paraiba", "idTabela": 46},
    {"idPacote": 8, "UF": "Alagoas", "idTabela": 33},
    {"idPacote": 15, "UF": "Goias", "idTabela": 37},
    {"idPacote": 19, "UF": "Minas Gerais", "idTabela": 43},
    {"idPacote": 22, "UF": "Pernambuco", "idTabela": 212},
    {"idPacote": 29, "UF": "Rondonia", "idTabela": 53},
    {"idPacote": 28, "UF": "Roraima", "idTabela": None},  # idTabela ausente
    {"idPacote": 30, "UF": "Santa Catarina", "idTabela": 54},
    {"idPacote": 31, "UF": "Sao Paulo", "idTabela": 247},
    {"idPacote": 32, "UF": "Sergipe", "idTabela": 56},
    {"idPacote": 33, "UF": "Tocantins", "idTabela": 59},
    {"idPacote": 7,  "UF": "Acre", "idTabela": 727},
    {"idPacote": 10, "UF": "Amapa", "idTabela": 842},
    {"idPacote": 9,  "UF": "Amazonas", "idTabela": 220},
    {"idPacote": 12, "UF": "Ceara", "idTabela": 36},
    {"idPacote": 13, "UF": "Distrito Federal", "idTabela": 838},
    {"idPacote": 14, "UF": "Espirito Santo", "idTabela": 171},
    {"idPacote": 16, "UF": "Maranhao", "idTabela": 122},
    {"idPacote": 17, "UF": "Mato Grosso", "idTabela": 131},
    {"idPacote": 18, "UF": "Mato Grosso do Sul", "idTabela": 42},
    {"idPacote": 20, "UF": "Para", "idTabela": 123},
    {"idPacote": 23, "UF": "Parana", "idTabela": 50},
    {"idPacote": 24, "UF": "Piaui", "idTabela": 127},
    {"idPacote": 25, "UF": "Rio de Janeiro", "idTabela": 52},
    {"idPacote": 26, "UF": "Rio Grande do Norte", "idTabela": 126},
    {"idPacote": 27, "UF": "Rio Grande do Sul", "idTabela": 173}
]

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_state_data(estado: Dict) -> Optional[pd.DataFrame]:
    """
    Busca e processa os dados de um único estado a partir da URL.

    Args:
        estado (Dict): Dicionário contendo informações do estado (UF, idTabela, idPacote).

    Returns:
        Optional[pd.DataFrame]: Um DataFrame com os dados do estado ou None em caso de falha.
    """
    id_tabela = estado.get("idTabela")
    uf_nome = estado.get("UF", "Desconhecido")

    if id_tabela is None:
        logging.warning(f"Ignorando {uf_nome} (sem idTabela)")
        return None

    params = {"idTabela": id_tabela, "idPacote": estado["idPacote"]}

    try:
        response = requests.get(BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        if not response.text.strip():
            logging.warning(f"Recebida resposta vazia para {uf_nome}.")
            return None

        data_io = io.StringIO(response.text)

        df = pd.read_csv(
            data_io,
            sep='|',
            encoding='cp1252',
            skiprows=1,
            header=None,
            dtype=str
        )

        logging.info(f"✅ Sucesso ao processar: {uf_nome}")
        return df

    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Erro de rede ao buscar dados para {uf_nome}: {e}")
    except pd.errors.ParserError as e:
        logging.error(f"❌ Erro de parsing do CSV para {uf_nome}: {e}")
    except Exception as e:
        logging.error(f"❌ Erro inesperado em {uf_nome}: {e}")

    return None

def process_and_clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica as transformações e limpeza necessárias no DataFrame consolidado.
    """
    logging.info("Aplicando transformações e limpeza nos dados...")
    df.columns = COLUNAS

    # Preenche data_fim nula ou vazia com um valor padrão
    df['data_fim'] = df['data_fim'].fillna(DEFAULT_DATA_FIM).replace(['nan', 'NaN', ''], DEFAULT_DATA_FIM)
    
    # Escapa aspas simples na descrição para compatibilidade com SQL
    df['descricao'] = df['descricao'].str.replace("'", "''", regex=False)

    df['uf'] = df['cod_aj_apur'].str[:2]
    return df

def enviar_dados_para_api(df: pd.DataFrame):
    """
    Converte o DataFrame para JSON e o envia para a API via POST.
    """
    if df.empty:
        logging.warning("DataFrame está vazio. Nenhum dado para enviar à API.")
        return

    logging.info(f"Preparando para enviar {len(df)} registros para a API em {API_URL}...")

    # Converte o DataFrame para uma lista de dicionários, que é o formato JSON nativo.
    payload = df.to_dict(orient='records')

    headers = {
        'Content-Type': 'application/json'
    }

    try:
        response = requests.post(API_URL, json=payload, headers=headers, timeout=120)
        response.raise_for_status()  # Lança um erro para status HTTP 4xx ou 5xx

        resultado = response.json()
        logging.info(f"✅ Sucesso! Resposta da API: {resultado}")

    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Erro ao enviar dados para a API: {e}")
        if e.response is not None:
            logging.error(f"Detalhes da resposta: {e.response.text}")

def main():

    logging.info("Iniciando o processo de extração de dados estaduais...")

    # Usa ThreadPoolExecutor para buscar dados de vários estados em paralelo
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        # Mapeia a função fetch_state_data para cada estado na lista
        results = executor.map(fetch_state_data, DADOS_ESTADUAIS)
        # Filtra os resultados que retornaram None (erros ou estados ignorados)
        all_dfs = [df for df in results if df is not None and not df.empty]

    if not all_dfs:
        logging.warning("Nenhum dado foi baixado. Encerrando o processo.")
        return

    logging.info("Consolidando todos os DataFrames...")
    df_final = pd.concat(all_dfs, ignore_index=True)

    # Aplica a limpeza e transformação
    df_final = process_and_clean_data(df_final)

    df_final.to_csv(OUTPUT_FILENAME, index=False, encoding='utf-8-sig')
    logging.info(f"Arquivo final salvo em: {OUTPUT_FILENAME}")

    enviar_dados_para_api(df_final)

    logging.info("🎉 Processo concluído!")

if __name__ == "__main__":
    main()