# Databricks notebook source
# MAGIC %md
# MAGIC # Workshop Hands-on no Databricks
# MAGIC
# MAGIC ## Objetivos do Exercício
# MAGIC
# MAGIC O objetivo desse laboratório é importar os dados que serão utilizados nos exercícios.
# MAGIC </br>
# MAGIC
# MAGIC ## Criação do database
# MAGIC
# MAGIC Primeiro, vamos criar um database (ou schema – esses nomes são usados como sinônimos). Esse funcionará como um conteiner para guardar os dados que iremos utilizar durante os exercícios.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

catalogo = 'workspace'
database = 'default'

# COMMAND ----------

spark.sql(f"use {catalogo}.{database}")
print (f"O catálogo que estarei utilizando nesse exercício é: {catalogo} e o database é: {database}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Importação dos Arquivos
# MAGIC
# MAGIC Agora, precisamos carregar os dados que usaremos nos próximos laboratórios.
# MAGIC
# MAGIC Esse conjunto consiste basicamente de quatro tabelas:
# MAGIC - **Avaliações:** conteúdo das avaliações
# MAGIC - **Clientes:** dados cadastrais e consumo dos clientes
# MAGIC - **Produtos:** dados de registro e descrições dos produto
# MAGIC - **FAQ:** perguntas e respostas frequentes em nosso website
# MAGIC
# MAGIC

# COMMAND ----------

def creating_tables(table_name, url):
    import pandas as pd

    df = pd.read_csv(url)
    
    print(f"creating `{table_name}` raw table")
    spark.createDataFrame(df).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)

# COMMAND ----------

# Creating the `avaliacoes` table from the specified CSV URL
creating_tables(f"{catalogo}.{database}.llm_avaliacoes", "https://raw.githubusercontent.com/Databricks-BR/lab_genai/refs/heads/main/dados/avaliacoes.csv")

# Creating the `clientes` table from the specified CSV URL
creating_tables(f"{catalogo}.{database}.llm_clientes", "https://raw.githubusercontent.com/Databricks-BR/lab_genai/refs/heads/main/dados/clientes.csv")

# Creating the `faq` table from the specified CSV URL
creating_tables(f"{catalogo}.{database}.llm_faq", "https://raw.githubusercontent.com/Databricks-BR/lab_genai/refs/heads/main/dados/faq.csv")

# Creating the `produtos` table from the specified CSV URL
creating_tables(f"{catalogo}.{database}.llm_produtos", "https://raw.githubusercontent.com/Databricks-BR/lab_genai/refs/heads/main/dados/produtos.csv")

# COMMAND ----------

# First, alter the column to be non-nullable
spark.sql(f"""
ALTER TABLE {catalogo}.{database}.llm_clientes
ALTER COLUMN id_cliente SET NOT NULL
""")

# Then, add the primary key constraint
spark.sql(f"""
ALTER TABLE {catalogo}.{database}.llm_clientes
ADD CONSTRAINT pk_cliente
PRIMARY KEY (id_cliente)
""")

# Finally, add the foreign key constraint
spark.sql(f"""
ALTER TABLE {catalogo}.{database}.llm_avaliacoes
ADD CONSTRAINT fk_cliente
FOREIGN KEY (id_cliente)
REFERENCES {catalogo}.{database}.llm_clientes(id_cliente)
""")