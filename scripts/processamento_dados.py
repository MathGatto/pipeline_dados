import json
import csv


class Dados:

    def __init__(self, path, tipo_dados):
        self.__path = path
        self.__tipo_dados = tipo_dados
        self.dados = self.__leitura_dados()
        self.nome_colunas = self.__get_columns()
        self.qtd_linhas = self.__size_data()

    # Método Privado
    def __leitura_json(self): # Método para ler um arquivo JSON usando JSON LOAD
        dados_json = []
        with open(self.__path, 'r') as file:
            dados_json = json.load(file)
        return dados_json


    # Método Privado
    def __leitura_csv(self): # Método para ler um arquivo CSV usando DICTREADER
        dados_csv = []
        with open(self.__path,'r') as file:
            spamreader = csv.DictReader(file, delimiter=',')
            for row in spamreader:
                dados_csv.append(row)
        
        return dados_csv


    # Método Privado

    def __leitura_dados(self): # Método para leitura de Dados Geral
        dados = []

        if self.__tipo_dados == 'csv':
            dados = self.__leitura_csv()
        
        elif self.__tipo_dados == 'json':
            dados = self.__leitura_json()

        elif self.__tipo_dados == 'list':
            dados = self.__path
            self.__path = 'lista em memoria'

        return dados


    # Método Privado
    def __get_columns(self): # Método que retorna o nome das Colunas (Header) do database
        return list(self.dados[-1].keys())


    # Método Publico   
    def rename_columns(self, key_mapping): # Método que renomeia as colunas para trazer um padrão ás duas bases de dados, usando Key mapping
        new_dados = []

        for old_dict in self.dados:
            dict_temp = {} 
            for old_key, value in old_dict.items():
                dict_temp[key_mapping[old_key]] = value 
            new_dados.append(dict_temp)

        self.dados = new_dados
        self.nome_colunas = self.__get_columns()


    # Método Privado   
    def __size_data(self): # Método que devolver a qtd de linhas do database
        return len(self.dados)
    

    # Método Publico
    def join(dadosA, dadosB): # Método que junta LISTAS
        combined_list = []
        combined_list.extend(dadosA.dados)
        combined_list.extend(dadosB.dados)

        return Dados(combined_list, 'list')
    
    # Método Privado   
    def __transformando_dados_tabela(self): # Método que transforma uma lista de dicionários em uma lista de listas

        dados_combinados_tabela = [self.nome_colunas]

        for row in self.dados:
            linha = []
            for coluna in self.nome_colunas:
                linha.append(row.get(coluna, 'Indisponível'))
            dados_combinados_tabela.append(linha)
            
        return dados_combinados_tabela
    
    # Método Publico
    def salvando_dados(self, path): # Método para salvar o arquivo novo

        dados_combinados_tabela = self.__transformando_dados_tabela() # Faz uso do método de transformação
    
        with open(path, 'w') as file:
            writer = csv.writer(file)
            writer.writerows(dados_combinados_tabela)