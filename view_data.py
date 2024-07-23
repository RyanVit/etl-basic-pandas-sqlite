import sqlite3

def view_data(db_path, table_name):
    """
    Função para visualizar dados de uma tabela em um banco de dados SQLite.

    Parâmetros:
    db_path (str): Caminho para o arquivo do banco de dados SQLite.
    table_name (str): Nome da tabela a ser visualizada.
    """
    # Conectar ao banco de dados SQLite
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Construir a consulta SQL para selecionar todos os dados da tabela especificada
    query = f"SELECT * FROM {table_name}"
    cursor.execute(query)
    
    # Recuperar todos os resultados da consulta
    rows = cursor.fetchall()
    
    # Imprimir cada linha de dados
    for row in rows:
        print(row)
    
    # Fechar a conexão com o banco de dados
    conn.close()

if __name__ == '__main__':
    # Caminho para o arquivo do banco de dados SQLite
    db_path = 'data/transformed_data.db'
    
    # Nome da tabela a ser visualizada
    table_name = 'transformed_table'
    
    # Chamar a função para visualizar os dados da tabela
    view_data(db_path, table_name)
