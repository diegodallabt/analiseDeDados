import pandas as pd
from sqlalchemy import create_engine
import psycopg2

# Conectando ao banco de dados
engine=create_engine('postgresql+psycopg2://postgres:SENHA@localhost:5432/DB')

# Populando o banco de dados
## VENDAS 2017
with pd.ExcelFile('vendas2017.xlsx') as xls: # lê o arquivo vendas2017.xlsx
    df = pd.read_excel(xls) # lê o arquivo vendas2017.xlsx
    df.to_sql(name='vendas2017', con=engine, if_exists='replace', index=False) # insere os dados no banco de dados

## VENDAS 2018
with pd.ExcelFile('vendas2018.xlsx') as xls: # lê o arquivo vendas2018.xlsx
    df = pd.read_excel(xls) # lê o arquivo vendas2018.xlsx
    df.to_sql(name='vendas2018', con=engine, if_exists='replace', index=False) # insere os dados no banco de dados

## VENDAS 2019
with pd.ExcelFile('vendas2019.xlsx') as xls: # lê o arquivo vendas2019.xlsx
    df = pd.read_excel(xls) # lê o arquivo vendas2019.xlsx
    df.to_sql(name='vendas2019', con=engine, if_exists='replace', index=False) # insere os dados no banco de dados

## METAS
with pd.ExcelFile('metas.xlsx') as xls: # lê o arquivo metas.xlsx
    df = pd.read_excel(xls) # lê o arquivo metas.xlsx
    df.to_sql(name='metas', con=engine, if_exists='append', index=False) # insere os dados no banco de dados

## DIMENSÕES
def sheets(data, file): # função para ler as planilhas do arquivo dimensoes.xlsx
    if(data=='Cliente'): 
        df=pd.read_excel(file, sheet_name='Cliente') # lê a planilha Cliente
        df.to_sql(name='clientes', con=engine, if_exists='append', index=False) # insere os dados no banco de dados
    elif(data=='Produto'):
        df=pd.read_excel(file, sheet_name='Produto') # lê a planilha Produto
        df.to_sql(name='produtos', con=engine, if_exists='append', index=False) # insere os dados no banco de dados
    elif(data=='Vendedor'):
        df=pd.read_excel(file, sheet_name='Vendedor') # lê a planilha Vendedor
        df.to_sql(name='vendedores', con=engine, if_exists='append', index=False) # insere os dados no banco de dados
    elif(data=='GrupoProduto'):
        df=pd.read_excel(file, sheet_name='GrupoProduto') # lê a planilha GrupoProduto
        df.to_sql(name='grupoProdutos', con=engine, if_exists='append', index=False) # insere os dados no banco de dados
    elif(data=='Data'):
        df=pd.read_excel(file, sheet_name='Data') # lê a planilha Data
        df.to_sql(name='datas', con=engine, if_exists='append', index=False) # insere os dados no banco de dados

with pd.ExcelFile('dimensoes.xlsx') as xls:
    for sheet_name in xls.sheet_names: # lê as planilhas do arquivo dimensoes.xlsx
        sheets(sheet_name, 'dimensoes.xlsx') # chama a função sheets para cada planilha
