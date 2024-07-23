import pandas as pd
import sqlite3
import os

def extract_data(file_path):
    """Função para extrair dados de um arquivo CSV"""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")
    
    if os.stat(file_path).st_size == 0:
        raise ValueError(f"O arquivo está vazio: {file_path}")
    
    return pd.read_csv(file_path)

def transform_data(df):
    """Função para transformar dados"""
    # Exemplo de transformação: remover linhas com valores nulos e converter todas as strings para minúsculas
    df.dropna(inplace=True)
    df = df.applymap(lambda s: s.lower() if type(s) == str else s)
    return df

def load_data(df, db_path, table_name):
    """Função para carregar dados em um banco de dados SQLite"""
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()

def main():
    """Função principal para executar o pipeline ETL"""
    # Caminhos dos arquivos
    csv_file_path = 'data/source_data.csv'
    db_file_path = 'data/transformed_data.db'
    table_name = 'transformed_table'

    # Execução das etapas do ETL
    try:
        data = extract_data(csv_file_path)
        transformed_data = transform_data(data)
        load_data(transformed_data, db_file_path, table_name)
        print("ETL concluído com sucesso!")
    except FileNotFoundError as e:
        print(e)
    except ValueError as e:
        print(e)
    except Exception as e:
        print(f"Ocorreu um erro: {e}")

if __name__ == '__main__':
    main()
