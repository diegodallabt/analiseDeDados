import pandas as pd
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine

# Altera precisão do tipo double para duas casas decimais
# pd.set_option("display.precision", 0)

# Conectando ao banco de dados
engine = create_engine('postgresql://postgres:SENHA@localhost/DB')

# Estrutura das querys
query_vendas2017 = """
    SELECT * FROM vendas2017
"""

query_vendas2018 = """
    SELECT * FROM vendas2018
"""

query_vendas2019 = """
    SELECT * FROM vendas2019
"""

query_metas = """
    SELECT * FROM metas
"""

query_produtos = """
    SELECT * FROM produtos
"""

query_vendedores = """
    SELECT * FROM vendedores
"""

query_clientes = """
    SELECT * FROM clientes
"""

query_grupoProdutos = """
    SELECT * FROM "grupoprodutos"
"""

query_datas = """
    SELECT * FROM datas
"""

query_faturamento_ano_e_trimestre = """
SELECT 
    CAST(EXTRACT(YEAR FROM vendas2017.data_emissao) AS INTEGER) AS Ano, 
    CONCAT('T', EXTRACT(QUARTER FROM vendas2017.data_emissao)) AS Trimestre, 
    SUM(vendas2017.qtd_itens * vendas2017.valor_unitario) AS Faturamento 
FROM 
    vendas2017 
GROUP BY 
    CAST(EXTRACT(YEAR FROM vendas2017.data_emissao) AS INTEGER), 
    CONCAT('T', EXTRACT(QUARTER FROM vendas2017.data_emissao))
	
UNION ALL

SELECT 
    CAST(EXTRACT(YEAR FROM vendas2018.data_emissao) AS INTEGER) AS Ano, 
    CONCAT('T', EXTRACT(QUARTER FROM vendas2018.data_emissao)) AS Trimestre, 
    SUM(vendas2018.qtd_itens * vendas2018.valor_unitario) AS Faturamento 
FROM 
    vendas2018 
GROUP BY 
    CAST(EXTRACT(YEAR FROM vendas2018.data_emissao) AS INTEGER), 
    CONCAT('T', EXTRACT(QUARTER FROM vendas2018.data_emissao))

UNION ALL

SELECT 
    CAST(EXTRACT(YEAR FROM vendas2019.data_emissao) AS INTEGER) AS Ano, 
    CONCAT('T', EXTRACT(QUARTER FROM vendas2019.data_emissao)) AS Trimestre, 
    SUM(vendas2019.qtd_itens * vendas2019.valor_unitario) AS Faturamento 
FROM 
    vendas2019 
GROUP BY 
    CAST(EXTRACT(YEAR FROM vendas2019.data_emissao) AS INTEGER), 
    CONCAT('T', EXTRACT(QUARTER FROM vendas2019.data_emissao))

ORDER BY Ano, Trimestre;
"""

query_faturamento_por_categoria = """
SELECT 
    Categoria, 
    SUM(Faturamento) AS Faturamento
FROM (
    SELECT 
        c.categoria AS Categoria,
        SUM(v.qtd_itens * v.valor_unitario) AS Faturamento
    FROM 
        vendas2017 v 
        JOIN clientes c ON v.cd_cliente = c.cd_cliente
    GROUP BY 
        c.categoria

    UNION ALL

    SELECT 
        c.categoria AS Categoria,
        SUM(v.qtd_itens * v.valor_unitario) AS Faturamento
    FROM 
        vendas2018 v 
        JOIN clientes c ON v.cd_cliente = c.cd_cliente
    GROUP BY 
        c.categoria

    UNION ALL

    SELECT 
        c.categoria AS Categoria,
        SUM(v.qtd_itens * v.valor_unitario) AS Faturamento
    FROM 
        vendas2019 v 
        JOIN clientes c ON v.cd_cliente = c.cd_cliente
    GROUP BY 
        c.categoria
) AS subquery
GROUP BY 
    Categoria
ORDER BY 
    Categoria;
"""

query_faturamento_por_estado = """
SELECT
    c.uf AS Estado,
    CAST(SUM(v.qtd_itens * v.valor_unitario) AS NUMERIC(20,2)) AS Faturamento
FROM
    (SELECT cd_cliente, qtd_itens, valor_unitario FROM vendas2017
     UNION ALL
     SELECT cd_cliente, qtd_itens, valor_unitario FROM vendas2018
     UNION ALL
     SELECT cd_cliente, qtd_itens, valor_unitario FROM vendas2019) v
    JOIN clientes c ON v.cd_cliente = c.cd_cliente
GROUP BY
    c.uf
ORDER BY
    Estado;
"""

query_faturamento_por_gerente_e_vendedor = """
SELECT 
    v.gerente AS Gerente,
    v.vendedor AS Vendedor,
    SUM(qtd_itens * valor_unitario) AS Faturamento
FROM 
    (
        SELECT * FROM vendas2017
        UNION ALL
        SELECT * FROM vendas2018
        UNION ALL
        SELECT * FROM vendas2019
    ) AS vendas
    JOIN vendedores v ON vendas.cd_vendedor = v.cd_vendedor
GROUP BY 
    v.gerente, 
    v.vendedor 
ORDER BY 
    v.gerente, 
    v.vendedor;
"""

query_faturamento_vs_meta = """
SELECT
    f.vendedor AS Vendedor,
    SUM(v.qtd_itens * v.valor_unitario) AS Faturamento,
    CAST(m.total AS NUMERIC(20, 2)) AS Meta
FROM
    (SELECT * FROM vendas2017
        UNION ALL
        SELECT * FROM vendas2018
        UNION ALL
        SELECT * FROM vendas2019) v
    JOIN vendedores f ON v.cd_vendedor = f.cd_vendedor
    JOIN metas m ON v.cd_vendedor = m.cd_vendedor
GROUP BY
    f.vendedor,
	m.total
ORDER BY
    f.vendedor;
"""

query_porcentagem_de_vendas_gerente = """
SELECT
    f.gerente AS Gerente,
    CAST(SUM(v.qtd_itens * v.valor_unitario) AS NUMERIC(20, 2)) AS Faturamento,
	CAST(SUM(v.qtd_itens * v.valor_unitario) / SUM(SUM(v.qtd_itens * v.valor_unitario)) OVER () * 100 AS NUMERIC(20,2)) AS Porcentagem
FROM
    (SELECT * FROM vendas2017
        UNION ALL
        SELECT * FROM vendas2018
        UNION ALL
        SELECT * FROM vendas2019) v
    JOIN vendedores f ON v.cd_vendedor = f.cd_vendedor
GROUP BY
    f.gerente
ORDER BY
    f.gerente;
"""

query_vendedores_atingiram_meta_por_mes = """
SELECT 
    DISTINCT (f.vendedor),
	f.gerente,
    EXTRACT(MONTH FROM v.data_emissao) AS mes,
    EXTRACT(YEAR FROM v.data_emissao) AS ano
FROM 
   (
        SELECT * FROM vendas2017
        UNION ALL
        SELECT * FROM vendas2018
    ) AS v
    JOIN vendedores AS f ON v.cd_vendedor = f.cd_vendedor
    JOIN metas_view AS metas ON metas.cd_vendedor = f.cd_vendedor
WHERE
    EXTRACT(MONTH FROM v.data_emissao) = EXTRACT(MONTH FROM metas.data) AND
    EXTRACT(YEAR FROM v.data_emissao) = EXTRACT(YEAR FROM metas.data)
GROUP BY
	f.gerente,
    f.vendedor,
    EXTRACT(MONTH FROM v.data_emissao),
    EXTRACT(YEAR FROM v.data_emissao),
	metas.meta
HAVING
    SUM(v.qtd_itens * v.valor_unitario) >= metas.meta
ORDER BY
    EXTRACT(YEAR FROM v.data_emissao),
    EXTRACT(MONTH FROM v.data_emissao)
    ASC;
"""

query_vendas_por_categoria = """
SELECT
    c.categoria AS Categoria,
    COUNT (*) AS Vendas
FROM
    (SELECT cd_cliente, qtd_itens FROM vendas2017
        UNION ALL
        SELECT cd_cliente, qtd_itens FROM vendas2018
        UNION ALL
        SELECT cd_cliente, qtd_itens FROM vendas2019) v
    JOIN clientes c ON v.cd_cliente = c.cd_cliente
GROUP BY
    c.categoria
ORDER BY
    Vendas
DESC;
"""

query_porcentagem_vendas_por_categoria = """
SELECT
    c.categoria AS Categoria,
    ROUND(SUM(v.qtd_itens) / SUM(SUM(v.qtd_itens)) OVER () * 100, 2) AS Porcentagem
FROM
    (SELECT cd_cliente, qtd_itens FROM vendas2017
        UNION ALL
        SELECT cd_cliente, qtd_itens FROM vendas2018
        UNION ALL
        SELECT cd_cliente, qtd_itens FROM vendas2019) v
    JOIN clientes c ON v.cd_cliente = c.cd_cliente
GROUP BY
    c.categoria
ORDER BY
    Porcentagem
DESC;
"""

query_ticket_medio_por_mes_e_ano = """
SELECT
    CAST(EXTRACT(YEAR FROM v.data_emissao) AS INTEGER) AS Ano,
    CAST(EXTRACT(MONTH FROM v.data_emissao) AS INTEGER) AS Mes,
    CAST(SUM(v.qtd_itens * v.valor_unitario) / COUNT(DISTINCT v.cd_cliente) AS NUMERIC(20,2)) AS TicketMedio
FROM
    (SELECT cd_cliente, qtd_itens, valor_unitario, data_emissao FROM vendas2017
        UNION ALL
        SELECT cd_cliente, qtd_itens, valor_unitario, data_emissao FROM vendas2018
        UNION ALL
        SELECT cd_cliente, qtd_itens, valor_unitario, data_emissao FROM vendas2019) v
GROUP BY
    CAST(EXTRACT(YEAR FROM v.data_emissao) AS INTEGER),
    CAST(EXTRACT(MONTH FROM v.data_emissao) AS INTEGER)
ORDER BY
    Ano,
    Mes;
"""

query_top_5_clientes_faturamento_por_mes = """
SELECT DISTINCT
    c.razao_social AS Cliente,
    CAST(SUM(v.qtd_itens * v.valor_unitario) AS NUMERIC(20,2)) AS Faturamento
FROM
    (SELECT cd_cliente, qtd_itens, valor_unitario, data_emissao FROM vendas2017
        UNION ALL
        SELECT cd_cliente, qtd_itens, valor_unitario, data_emissao FROM vendas2018
        UNION ALL
        SELECT cd_cliente, qtd_itens, valor_unitario, data_emissao FROM vendas2019) v
    JOIN clientes c ON v.cd_cliente = c.cd_cliente
GROUP BY
    c.razao_social,
    CAST(EXTRACT(MONTH FROM v.data_emissao) AS INTEGER)
ORDER BY
    Faturamento DESC
LIMIT 5;
"""

query_top_5_vendedor_em_faturamento_por_mes = """
SELECT
    vendedores.vendedor,
    CAST(SUM(v.qtd_itens * v.valor_unitario) AS NUMERIC(20,2)) AS Faturamento
FROM
    (SELECT cd_vendedor, qtd_itens, valor_unitario, data_emissao FROM vendas2017
        UNION ALL
        SELECT cd_vendedor, qtd_itens, valor_unitario, data_emissao FROM vendas2018
        UNION ALL
        SELECT cd_vendedor, qtd_itens, valor_unitario, data_emissao FROM vendas2019) v
    JOIN vendedores ON v.cd_vendedor = vendedores.cd_vendedor
GROUP BY
    vendedores.vendedor,
    CAST(EXTRACT(MONTH FROM v.data_emissao) AS INTEGER)
ORDER BY
    Faturamento DESC
LIMIT 5;
"""

query_peso_vendido_por_vendedor = """
SELECT
  vendedores.vendedor,
  SUM((v.qtd_itens * v.peso_liquido)) AS Peso
FROM
  (
        SELECT * FROM vendas2017
        UNION ALL
        SELECT * FROM vendas2018
        UNION ALL
        SELECT * FROM vendas2019
    ) AS v
  INNER JOIN vendedores ON v.cd_vendedor = vendedores.cd_vendedor
GROUP BY
  vendedores.vendedor
ORDER BY
    Peso
DESC;
"""

# Executando as querys básicas e armazenando o resultado em seus respectivos dataframes
vendas2017 = pd.read_sql_query(sql=query_vendas2017, con=engine)
vendas2018 = pd.read_sql_query(sql=query_vendas2018, con=engine) 
vendas2019 = pd.read_sql_query(sql=query_vendas2019, con=engine)
metas = pd.read_sql_query(sql=query_metas, con=engine)
produtos = pd.read_sql_query(sql=query_produtos, con=engine)
vendedores = pd.read_sql_query(sql=query_vendedores, con=engine)
clientes = pd.read_sql_query(sql=query_clientes, con=engine)
grupoProdutos = pd.read_sql_query(sql=query_grupoProdutos, con=engine)
datas = pd.read_sql_query(sql=query_datas, con=engine)

# Imprimindo os resultados das querys básicas
print("# VENDAS 2017 #\n{}\n".format(vendas2017.loc[0:9]))
print("# VENDAS 2018 #\n{}\n".format(vendas2018.loc[0:9]))
print("# VENDAS 2019 #\n{}\n".format(vendas2019.loc[0:9]))
print("# METAS #\n{}\n".format(metas.loc[0:9]))
print("# PRODUTOS #\n{}\n".format(produtos.loc[0:9]))
print("# VENDEDORES #\n{}\n".format(vendedores.loc[0:9]))
print("# CLIENTES #\n{}\n".format(clientes.loc[0:9]))
print("# GRUPO DE PRODUTOS #\n{}\n".format(grupoProdutos.loc[0:9]))
print("# DATAS #\n{}".format(datas.loc[0:9]))

# Executando a querys mais complexas sobre os dados e armazenando o resultado em seus respectivos dataframes
faturamento_ano_e_trimestre = pd.read_sql_query(sql=query_faturamento_ano_e_trimestre, con=engine)
faturamento_por_categoria = pd.read_sql_query(sql=query_faturamento_por_categoria, con=engine)
faturamento_por_estado = pd.read_sql_query(sql=query_faturamento_por_estado, con=engine)
faturamento_por_gerente_e_vendedor = pd.read_sql_query(sql=query_faturamento_por_gerente_e_vendedor, con=engine)
faturamento_vs_meta = pd.read_sql_query(sql=query_faturamento_vs_meta, con=engine)
porcentagem_de_vendas_gerente = pd.read_sql_query(sql=query_porcentagem_de_vendas_gerente, con=engine)
query_vendedores_atingiram_meta_por_mes = pd.read_sql_query(sql=query_vendedores_atingiram_meta_por_mes, con=engine)
query_vendas_por_categoria = pd.read_sql_query(sql=query_vendas_por_categoria, con=engine)
porcentagem_vendas_por_categoria = pd.read_sql_query(sql=query_porcentagem_vendas_por_categoria, con=engine)
ticket_medio_por_mes_e_ano = pd.read_sql_query(sql=query_ticket_medio_por_mes_e_ano, con=engine)
top_5_clientes_faturamento_por_mes = pd.read_sql_query(sql=query_top_5_clientes_faturamento_por_mes, con=engine) 
top_5_vendedor_em_faturamento_por_mes = pd.read_sql_query(sql=query_top_5_vendedor_em_faturamento_por_mes, con=engine) 
peso_vendido_por_vendedor = pd.read_sql_query(sql=query_peso_vendido_por_vendedor, con=engine)

# Imprimindo os resultados das querys mais complexas
print("# FATURAMENTO POR ANO E TRIMESTRE #\n{}\n".format(faturamento_ano_e_trimestre))
print("# FATURAMENTO POR CATEGORIA #\n{}\n".format(faturamento_por_categoria))
print("# FATURAMENTO POR ESTADO #\n{}\n".format(faturamento_por_estado))
print("# FATURAMENTO POR GERENTE E VENDEDOR #\n{}\n".format(faturamento_por_gerente_e_vendedor))
print("# FATURAMENTO VS META #\n{}\n".format(faturamento_vs_meta))
print("# PORCENTAGEM DE VENDAS POR GERENTE #\n{}\n".format(porcentagem_de_vendas_gerente))
print("# VENDEDORES QUE ATINGIRAM META POR MÊS #\n{}\n".format(query_vendedores_atingiram_meta_por_mes))
print("# VENDAS POR CATEGORIA #\n{}\n".format(query_vendas_por_categoria))
print("# PORCENTAGEM VENDAS POR CATEGORIA #\n{}\n".format(porcentagem_vendas_por_categoria))
print("# TICKET MÉDIO POR MÊS E ANO #\n{}\n".format(ticket_medio_por_mes_e_ano))
print("# TOP 5 CLIENTES FATURAMENTO POR MÊS #\n{}\n".format(top_5_clientes_faturamento_por_mes))
print("# TOP 5 VENDEDOR EM FATURAMENTO POR MÊS #\n{}\n".format(top_5_vendedor_em_faturamento_por_mes))
print("# PESO VENDIDO POR VENDEDOR #\n{}\n".format(peso_vendido_por_vendedor))
