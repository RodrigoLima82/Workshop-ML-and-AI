# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Análise Exploratória e Limpeza dos dados
# MAGIC
# MAGIC Nesse notebook você irá aprender:
# MAGIC - Realizar uma análise exploratória sobre os dados
# MAGIC - Realizar a limpeza dos dados de uma tabela e salvar o resultado em outra

# COMMAND ----------

# MAGIC %md
# MAGIC ### Carregar dados da camada bronze

# COMMAND ----------

dbutils.widgets.removeAll()

dbutils.widgets.text("catalogo", "catalogo_treinamento", "Catalogo")
dbutils.widgets.text("database", "default", "Database")

catalogo = dbutils.widgets.get("catalogo")
database = dbutils.widgets.get("database")


# COMMAND ----------

spark.sql(f"use {catalogo}.{database}")
print (f"O catálogo que estarei utilizando nesse exercício é: {catalogo} e o database é: {database}")

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### Explore o conjunto de dados com o Data Profile
# MAGIC
# MAGIC Também podemos verificar a quantidade de nulos e outras informações através do profiling dos dados.O recurso **Data Profile** nos notebooks Databricks oferece insights e benefícios valiosos para análise e exploração de dados. Ao aproveitar o Data Profile, os usuários obtêm uma visão geral abrangente das **características, estatísticas e métricas de qualidade de dados** de seu conjunto de dados. Esse recurso permite que cientistas e analistas de dados entendam a distribuição dos dados, identifiquem valores ausentes, detectem valores discrepantes e explorem estatísticas descritivas com eficiência.
# MAGIC
# MAGIC Existem duas maneiras de visualizar o Data Profiler. A primeira opção é a IU.
# MAGIC
# MAGIC - Depois de usar a função `display` para mostrar um quadro de dados, clique no ícone **+** próximo à *Tabela* no cabeçalho.
# MAGIC - Clique em **Data Profile**.
# MAGIC
# MAGIC
# MAGIC
# MAGIC Essa funcionalidade também está disponível por meio da API dbutils em Python, Scala e R, usando o comando dbutils.data.summarize(df). Também podemos usar **`dbutils.data.summarize(df)`** para exibir a IU do perfil de dados.
# MAGIC
# MAGIC Observe que esses recursos criarão o perfil de todo o conjunto de dados no quadro de dados ou nos resultados da consulta SQL, não apenas na parte exibida na tabela.
# MAGIC
# MAGIC #### Crie gráficos para ajudar em sua exploração ####
# MAGIC
# MAGIC - Clique no ícone **+** próximo à *Data Profile 1* no cabeçalho.
# MAGIC - Clique em **Visualization**.
# MAGIC - Crie um gráfico de barras para sabermos quantos clientes temos por estado
# MAGIC - Crie um gráfico de pizza para sabermos quantidade por indice de satisfação
# MAGIC

# COMMAND ----------

# DBTITLE 1,Lendo os novos dados da tabela tb_cliente_bronze
df_clientes = spark.table(f"{catalogo}.{database}.tb_clientes_bronze")

# COMMAND ----------

display( df_clientes)

# COMMAND ----------

dbutils.data.summarize(df_clientes)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Estatísticas resumidas
# MAGIC
# MAGIC Três opções:
# MAGIC * **`Data Profile`**: opção anterior
# MAGIC * **`describe`**: contagem, média, desvio padrão, min, max
# MAGIC * **`summary`**: descrição + intervalo interquartil (IQR)

# COMMAND ----------

display(df_clientes.describe())

# COMMAND ----------

display(df_clientes.summary())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Noções básicas de manipulação de dados

# COMMAND ----------

# MAGIC %md
# MAGIC ## DataFrames Intros
# MAGIC Pensando em tabular: se você nunca trabalhou com "dataframes" ou dados tabulares, ajuda ter um modelo mental. Pense em **linhas** e **colunas**!
# MAGIC
# MAGIC ![dados tabulares](https://viacarreira.com/wp-content/uploads/2021/01/tabulacao-de-dados-tcc-1.png)
# MAGIC
# MAGIC Este é um arquivo Excel, mas muitos conjuntos de dados (relacionais e outros) podem ser considerados tabulares.
# MAGIC
# MAGIC Se você já ouviu falar do Spark antes, pode ter ouvido falar de RDDs, ou "conjuntos de dados distribuídos resilientes". Embora eles fossem importantes para as primeiras versões do Spark, quase todo o desenvolvimento agora acontece em Spark DataFrames, uma abstração de nível superior de RDDs. **Você deve concentrar seus esforços de aprendizado nas APIs do DataFrame**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Diferenças entre SQL e PySpark
# MAGIC SQL é uma sintaxe; PySpark é uma interface para o mecanismo Spark.
# MAGIC
# MAGIC Você prefere SQL? Ótimo! No Databricks, você pode concluir muitas das mesmas operações com `%sql SELECT * FROM...` como em uma ferramenta de consulta tradicional. No Pyspark, você também pode usar `spark.sql("SELECT * FROM...)"` para uma interface semelhante a SQL do Python.
# MAGIC
# MAGIC Curiosidade: o desempenho entre SQL, Pyspark e Scala Spark é quase idêntico na maioria das circunstâncias. Todos eles compilam para a mesma coisa!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Diferenças entre Pandas e Spark DataFrames
# MAGIC Muitos profissionais de dados começam a trabalhar com dados tabulares no Pandas, uma biblioteca Python popular. Vamos comparar com o PySpark:
# MAGIC
# MAGIC | | DataFrame do pandas | DataFrame do Spark |
# MAGIC | -------------- | --------------------------------- | ------------------------------------------------------------------- |
# MAGIC | Coluna | df\['col'\] | df\['col'\] |
# MAGIC | Mutabilidade | Mutável | Imutável |
# MAGIC | Adicionar uma coluna | df\['c'\] = df\['a'\] + df\['b'\] | df.withColumn('c', df\['a'\] + df\['b'\]) |
# MAGIC | Renomear colunas | df.columns = \['a','b'\] | df.select(df\['c1'\].alias('a'), df\['c2'\].alias('b')) |
# MAGIC | Contagem de valores | df\['col'\].value\_counts() | df.groupBy(df\['col'\]).count().orderBy('count', ascending = False) |

# COMMAND ----------

# MAGIC %md
# MAGIC A partir de agora, faremos algumas transformações nos dados da camada bronze, e deixaremos ele com mais qualidade para salvarmos na camada silver.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Conte o total de linhas do DataFrame 'df_clientes'

# COMMAND ----------

total_rows_df_clientes = df_clientes.count()
total_rows_df_clientes

# COMMAND ----------

# MAGIC %md
# MAGIC #### E se eu quisesse fazer isso com pandas?

# COMMAND ----------

df_clientes_pandas = df_clientes.pandas_api()
total_rows_df_clientes_pandas = len(df_clientes_pandas)
total_rows_df_clientes_pandas

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cerifique se existe valores nulos no campo CodigoCliente

# COMMAND ----------

from pyspark.sql.functions import col

null_codigo_cliente = df_clientes.filter(col("CodigoCliente").isNull())

display(null_codigo_cliente)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conte quantos nulos tem no campo Codigo Cliente

# COMMAND ----------

from pyspark.sql.functions import col, count, when

null_count_codigo_cliente = df_clientes.select(count(when(col("CodigoCliente").isNull(), "CodigoCliente")).alias("null_count_CodigoCliente"))

display(null_count_codigo_cliente)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Remover linhas onde o CodigoCliente é nulo

# COMMAND ----------

# DBTITLE 1,Preencha aqui!


# COMMAND ----------

df_clientes = df_clientes.filter(col("CodigoCliente").isNotNull())
display(df_clientes)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Agora vamos dar uma olhada na coluna `RendaMensal`

# COMMAND ----------

display(df_clientes.groupBy('RendaMensal').count().orderBy(col('RendaMensal').desc()).limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC Existem alguns valores estranhos acima de 10000. Vamos removê-los também.

# COMMAND ----------

max_value = 100000
df_clientes = df_clientes[df_clientes.RendaMensal<max_value]

display(df_clientes.groupBy('RendaMensal').count().orderBy(col('RendaMensal').desc()).limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vamos verificar o conteúdo da coluna `UF` também

# COMMAND ----------

from pyspark.sql.functions import length

display(df_clientes.groupBy(length(col('UF'))).count().limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC Há alguns valores maiores que 2 letras (padrão para `UF`). Vamos ver esses valores, talvez possamos manter e fazer alguma transformação.

# COMMAND ----------

display(df_clientes[length(df_clientes.UF) > 2].select('UF').distinct())

# COMMAND ----------

# MAGIC %md
# MAGIC De fato, podemos fazer algumas transformações nesses valores para usar o mesmo padrão `UF`

# COMMAND ----------

from pyspark.sql.functions import when

df_clientes = df_clientes.withColumn('UF', 
                           when(df_clientes.UF == 'Bahia', 'BA')
                           .when(df_clientes.UF == 'São Paulo', 'SP')
                           .when(df_clientes.UF == 'Rio de Janeiro', 'RJ')
                           .otherwise(df_clientes.UF))

# COMMAND ----------

display(df_clientes[length(df_clientes.UF) > 2].select('UF').distinct())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Obter o domínio do e-mail
# MAGIC O domínio do e-mail pode ser uma informação útil. Vamos dividir esses dados do e-mail completo.

# COMMAND ----------

from pyspark.sql.functions import split

# Create a new column mail_domain
df_clientes = df_clientes.withColumn('mail_domain', split(col('Email'), '@').getItem(1))

display(df_clientes.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mudança de tipo de colunas
# MAGIC Iremos mudar de INT para Long o campo CodigoCliente

# COMMAND ----------

from pyspark.sql.types import LongType

# Change the data type of the CodigoCliente field from INT to Long
df_clientes = df_clientes.withColumn("CodigoCliente", df_clientes["CodigoCliente"].cast(LongType()))


# COMMAND ----------

# MAGIC %md
# MAGIC #### Carregar os dados na camada silver

# COMMAND ----------

df_clientes.write.saveAsTable(f"{catalogo}.{database}.tb_clientes_silver")  

# COMMAND ----------

df_clientes_silver = spark.table("tb_clientes_silver")

# COMMAND ----------

display(df_clientes_silver)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Ler Dados de Transações para os próximos exercícios

# COMMAND ----------

volume_folder=f"/Volumes/{catalogo}/{database}/files"

transacoes_folder = f"{volume_folder}/transacoes"

# Crie a nova pasta dentro do volume
dbutils.fs.mkdirs(transacoes_folder)

# Verifique se a pasta foi criada
display(dbutils.fs.ls(volume_folder))

# COMMAND ----------

import requests 

def download_file(url, destination):
    local_filename = url.split('/')[-1]
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        print('saving '+destination+'/'+local_filename)
        with open(destination+'/'+local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                f.write(chunk)
    return local_filename

# COMMAND ----------

download_file("https://raw.githubusercontent.com/RodrigoLima82/Workshop_ML_LLM/refs/heads/main/dados_transacoes_cartao/transacoes.csv", volume_folder + "/transacoes")

# COMMAND ----------

df_transacoes = spark.read.csv(f"/Volumes/{catalogo}/{database}/files/transacoes/", header=True, inferSchema=True)

# COMMAND ----------

display(df_transacoes)

# COMMAND ----------

df_transacoes.write.mode("overwrite").saveAsTable("tb_transacoes_silver")

# COMMAND ----------

display(spark.table(f"{catalogo}.{database}.tb_transacoes_silver"))