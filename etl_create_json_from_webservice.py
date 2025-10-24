import requests
import json
import pandas as pd

def consultar_auditoria(numero_inscricao):
    """
    Queries the webservice to get audit data for a specific registration number.
    Returns a dictionary with the status ('success' or 'failed') and the data or error message.
    """
    url = f'https://sistemas.sefaz.go.gov.br/gre-service/v1/relatorio/consulta-publica-auditorias/0/{numero_inscricao}'
    print(f"    Querying URL for {numero_inscricao}: {url}")
    try:
        resposta = requests.get(url, timeout=30)
        resposta.raise_for_status() # Raises an HTTPError for bad status codes (4xx or 5xx)
        return {"status": "success", "data": resposta.json()}
    except requests.exceptions.Timeout:
        return {"status": "failed", "error": f"Timeout exceeded while querying registration {numero_inscricao}."}
    except requests.exceptions.HTTPError as e:
        # HTTP error, but there might be useful content in the response body
        error_msg = f"HTTP Error for registration {numero_inscricao}: {e}. Status: {e.response.status_code}"
        try:
            error_details = e.response.json()
            error_msg += f". Details: {json.dumps(error_details, ensure_ascii=False)}"
        except json.JSONDecodeError:
            error_msg += f". Raw response: {e.response.text}"
        return {"status": "failed", "error": error_msg, "raw_response": e.response.text}
    except requests.exceptions.ConnectionError as e:
        return {"status": "failed", "error": f"Connection error for registration {numero_inscricao}: {e}. Check your internet or the webservice server."}
    except requests.exceptions.RequestException as e:
        return {"status": "failed", "error": f"Request error for registration {numero_inscricao}: {e}."}
    except json.JSONDecodeError as e:
        # If the response is not valid JSON, but the request was 2xx
        raw_response = resposta.text if 'resposta' in locals() else 'N/A'
        return {"status": "failed", "error": f"Error decoding JSON for registration {numero_inscricao}: {e}.", "raw_response": raw_response}
    except Exception as e:
        return {"status": "failed", "error": f"Unexpected error for registration {numero_inscricao}: {e}."}

def ler_inscricoes_xls(caminho_xls, nome_coluna='NUMR_INSCRICAO', sheet_name=0):
    """
    Reads registration numbers from an .xls file.
    - caminho_xls: full path of the .xls file
    - nome_coluna: exact name of the header containing the registration numbers
    - sheet_name: sheet index (0, 1, 2, ...) or sheet name
    """
    try:
        # engine="xlrd" ensures .xls reading, dtype={nome_coluna: str} ensures the column is read as string
        df = pd.read_excel(caminho_xls, sheet_name=sheet_name, dtype={nome_coluna: str}, engine="xlrd")
    except ImportError as e:
        raise ImportError("Missing dependency. Install with: pip install pandas xlrd") from e
    except FileNotFoundError:
        raise FileNotFoundError(f"Error: XLS file not found at path: {caminho_xls}")
    except ValueError as e:
        raise ValueError(f"Error reading the XLS file (check sheet name or format): {e}")
    except Exception as e:
        raise Exception(f"Unexpected error reading the XLS file: {e}")

    if nome_coluna not in df.columns:
        available_columns = ", ".join(map(str, df.columns))
        raise ValueError(f"Error: Column '{nome_coluna}' not found in the file. Available columns: {available_columns}")

    # Converts to string, removes empty values and 'nan' (which pandas uses for Not a Number/empty in string)
    series = df[nome_coluna].astype(str).str.strip()
    inscricoes = [s for s in series if s and s.lower() != 'nan']
    return inscricoes

def consultar_varias_inscricoes(inscricoes, arquivo_saida='C:/Users/marke/GNRE/auditorias_completas_por_inscricao.json'):
    """
    Queries multiple registration numbers and saves the data to a single JSON file.
    Ensures that EACH queried registration number has an entry in the output JSON,
    indicating success, failure, or absence of data, and that nested list fields
    like 'CampoPersonalizadoTermoBeneficioList' and 'Auditorias' are always present as lists.
    """
    todos_resultados_por_inscricao = []
    
    for i, numero in enumerate(inscricoes):
        print(f"\nProcessing registration {i+1}/{len(inscricoes)}: {numero}")
        
        # Calls the modified query function
        consulta_resultado = consultar_auditoria(numero)
        
        # Prepares the entry for this registration number in the final JSON
        entrada_inscricao = {
            "numero_inscricao_consultado": numero,
            "status": consulta_resultado["status"],
            "auditorias": [] # Initializes as an empty list
        }

        if consulta_resultado["status"] == "success":
            dados_retornados = consulta_resultado["data"]
            
            # Normalizes to a list if not already (the webservice might return a single dict or a list of dicts)
            if not isinstance(dados_retornados, list):
                dados_retornados = [dados_retornados]
            
            registros_processados = []
            if dados_retornados: # If there is data returned (the list is not empty)
                for record in dados_retornados:
                    # Ensures 'CampoPersonalizadoTermoBeneficioList' is always present as a list
                    if "CampoPersonalizadoTermoBeneficioList" not in record:
                        record["CampoPersonalizadoTermoBeneficioList"] = []
                    
                    # Ensures 'Auditorias' is always present as a list
                    if "Auditorias" not in record:
                        record["Auditorias"] = []
                    
                    registros_processados.append(record)
                
                entrada_inscricao["auditorias"] = registros_processados
            else: # The query was successful, but the webservice returned no data for this registration
                entrada_inscricao["status"] = "no_data_returned"
                print(f"    No audit data returned by the webservice for registration {numero}.")

        else: # status is "failed"
            entrada_inscricao["error"] = consulta_resultado["error"]
            if "raw_response" in consulta_resultado:
                entrada_inscricao["raw_response"] = consulta_resultado["raw_response"]
            print(f"    Error querying registration {numero}: {entrada_inscricao['error']}")

        todos_resultados_por_inscricao.append(entrada_inscricao)

    if todos_resultados_por_inscricao:
        with open(arquivo_saida, 'w', encoding='utf-8') as f:
            json.dump(todos_resultados_por_inscricao, f, ensure_ascii=False, indent=2)
        print(f"\n--- Processing Completed ---")
        print(f"File '{arquivo_saida}' successfully saved with {len(todos_resultados_por_inscricao)} registration entries!")
    else:
        print(f"\n--- Processing Completed ---")
        print(f"No results collected to save to file '{arquivo_saida}'.")


if __name__ == "__main__":
    try:
        # --- Configurations ---
        CAMINHO_XLS = r'C:\Users\marke\GNRE\ies_progoias.xls' # ATTENTION: Adjust this path to your XLS file
        NOME_COLUNA_INSCRICOES = 'NUMR_INSCRICAO'             # ATTENTION: Adjust if the column name is different
        SHEET_NAME_XLS = 0                                    # 0 for the first tab, or the tab name (e.g., 'Sheet1')
        ARQUIVO_SAIDA_JSON = 'C:/Users/marke/GNRE/auditorias_completas_por_inscricao.json' # Output JSON file name
        # --- End of Configurations ---

        print("Starting to read registrations from the XLS file...")
        inscricoes_lidas = ler_inscricoes_xls(
            CAMINHO_XLS,
            nome_coluna=NOME_COLUNA_INSCRICOES,
            sheet_name=SHEET_NAME_XLS
        )
        print(f"Total of {len(inscricoes_lidas)} registrations read from the XLS file.")
        
        consultar_varias_inscricoes(inscricoes_lidas, arquivo_saida=ARQUIVO_SAIDA_JSON)
        
    except (ImportError, FileNotFoundError, ValueError, Exception) as e:
        print(f"\n--- CRITICAL ERROR ---")
        print(f"An error occurred that prevented processing: {e}")
        print("Please check the configurations (XLS path, column name, sheet name) and dependencies (pandas, xlrd, requests).")
