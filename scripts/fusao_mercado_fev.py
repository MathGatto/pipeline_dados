from processamento_dados import Dados

# Definindo os endereços
path_json = '../pipeline_dados/data_raw/dados_empresaA.json' # Endereçando dados json
path_csv = '../pipeline_dados/data_raw/dados_empresaB.csv' # Endereçando os dados CSV


# == EXTRACT ==

dados_empresaA = Dados(path_json, 'json')
print(f'Nome Colunas Empresa A RAW: {dados_empresaA.nome_colunas}')
print(f'Numero de Linhas da EMPRESA A: {dados_empresaA.qtd_linhas}')

dados_empresaB = Dados(path_csv, 'csv')
print(f'Nome Colunas Empresa B RAW: {dados_empresaB.nome_colunas}')
print(f'Numero de Linhas da EMPRESA B: {dados_empresaB.qtd_linhas}')


# == TRANSFORM ==

# Criando um dicionário para fazer a mudança de colunas de forma eficiente, pois os dados estão em um formato Lista de Dicionários
key_mapping = {'Nome do Item': 'Nome do Produto',
               'ClassificaÃ§Ã£o do Produto': 'Categoria do Produto',
                'Valor em Reais (R$)': 'Preço do Produto (R$)',
                'Quantidade em Estoque': 'Quantidade em Estoque',
                'Nome da Loja': 'Filial',
                'Data da Venda': 'Data da Venda'}

print('')
dados_empresaB.rename_columns(key_mapping)
print(f'Nome Colunas Empresa B TRANSFORMADAS: {dados_empresaB.nome_colunas}')


print('')
dados_fusao = Dados.join(dados_empresaA, dados_empresaB)
print(f'Nome Colunas Fusão: {dados_fusao.nome_colunas}')
print(f'Numero de Linhas da Fusão: {dados_fusao.qtd_linhas}')


# == LOAD == 

path_dados_combinados = '../pipeline_dados/data_processed/dados_combinados.csv'
dados_fusao.salvando_dados(path_dados_combinados)
print('')
print(f'Caminho do arquivo Salvo: {path_dados_combinados}')

