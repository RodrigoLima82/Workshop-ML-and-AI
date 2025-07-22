-- Databricks notebook source
-- MAGIC %md <img src="https://github.com/Databricks-BR/lab_genai/blob/main/img/header.png?raw=true" width=100%>
-- MAGIC
-- MAGIC # Hands-On LAB 02 - Usando RAG
-- MAGIC
-- MAGIC Treinamento Hands-on na plataforma Databricks com foco nas funcionalidades de IA Generativa.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Objetivos do Exercício
-- MAGIC
-- MAGIC O objetivo desse laboratório é implementar o seguinte caso de uso:
-- MAGIC
-- MAGIC ### Personalizando o atendimento com RAG
-- MAGIC
-- MAGIC LLMs são ótimos para responder a perguntas. No entanto, isso sozinho não é suficiente para fornecer valor aos seus clientes.
-- MAGIC
-- MAGIC Para ser capaz de fornecer respostas mais complexas, informações adicionais e específicas para o usuário são necessárias, como seu ID de contrato, o último e-mail que enviaram para o seu suporte ou seu relatório de compras mais recentes.
-- MAGIC
-- MAGIC Agentes são projetados para superar este desafio. Eles são implantações de IA mais avançadas, compostas por múltiplas entidades (ferramentas) especializadas em diferentes ações (recuper informações ou interagir com sistemas externos).
-- MAGIC
-- MAGIC De forma geral, você constrói e apresenta um conjunto de funções personalizadas para a IA. A LLM pode então raciocinar sobre quais informações precisam ser reunidas e quais ferramentas utilizar para responder às intruções recebidas.
-- MAGIC
-- MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/llm-tools-functions/llm-tools-functions-flow.png?raw=true" width="100%">

-- COMMAND ----------

-- MAGIC %md ## Preparação
-- MAGIC
-- MAGIC Para executar os exercícios, precisamos conectar este notebook a um compute.
-- MAGIC
-- MAGIC Basta seguir os passos abaixo:
-- MAGIC 1. No canto superior direito, clique em **Connect**
-- MAGIC 2. Selecione o **SQL Warehouse** desejado

-- COMMAND ----------

-- MAGIC %md ## Exercício 02.01 - Usando Unity Catalog Tools
-- MAGIC
-- MAGIC O primeiro passo na construção do nosso agente será entender como utilizar **Unity Catalog Tools**.
-- MAGIC
-- MAGIC No laboratório anterior, criamos algumas funções, como a `llm_revisar_avaliacao`, que nos permitiam facilitar a invocação dos nossos modelos de IA Generativa a partir do SQL. No entanto, essas mesmas funções também podem ser utilizadas como ferramentas por nossas LLMs. Basta indicar quais funções o modelo pode utilizar!
-- MAGIC
-- MAGIC Poder utilizar um mesmo catálogo de ferramentas em toda a plataforma simplifica bastante a nossa vida ao promover a reutilização desses ativos. Isso pode economizar horas de redesenvolvimento, bem como padronizar esses conceitos.
-- MAGIC
-- MAGIC Vamos ver como utilizar ferramentas na prática!
-- MAGIC
-- MAGIC 1. No **menu principal** à esquerda, clique em **`Playground`**
-- MAGIC 2. Clique no **seletor de modelos** e selecione o modelo **`Claude Sonnet 4`** (caso já não esteja selecionado)
-- MAGIC 3. Clique em **Tools** e depois em **Add Tool** 
-- MAGIC 4. Em **Hosted Function**, digite `catalogo_treinamento.<seu_nome>.llm_revisar_avaliacao`
-- MAGIC 5. Adicione a instrução abaixo:
-- MAGIC     ```
-- MAGIC     Revise a avaliação abaixo:
-- MAGIC     Comprei um tablet e estou muito insatisfeito com a qualidade da bateria. Ela dura muito pouco tempo e demora muito para carregar.
-- MAGIC     ```
-- MAGIC 6. Clique no ícone **enviar**

-- COMMAND ----------

-- MAGIC %md ## Exercício 02.02 - Consultando dados do cliente
-- MAGIC
-- MAGIC Ferramentas podem ser utilizadas em diversos cenários, como por exemplo:
-- MAGIC
-- MAGIC - Consultar informações em bancos de dados
-- MAGIC - Calcular indicadores complexos
-- MAGIC - Gerar um texto baseado nas informações disponíveis
-- MAGIC - Interagir com APIs e sistemas externos
-- MAGIC
-- MAGIC Como já discutimos, isso vai ser muito importante para conseguirmos produzir respostas mais personalizadas e precisas no nosso agente. 
-- MAGIC
-- MAGIC No nosso caso, gostaríamos de:
-- MAGIC - Consultar os dados do cliente
-- MAGIC - Pesquisar perguntas e respostas em uma base de conhecimento
-- MAGIC - Fornecer recomendações personalizadas de produtos com base em suas descrições
-- MAGIC
-- MAGIC Vamos começar pela consulta dos dados do cliente!

-- COMMAND ----------

-- MAGIC %md ### A. Selecionar o database que criamos anteriormente

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.removeAll()
-- MAGIC
-- MAGIC dbutils.widgets.text("catalogo", "catalogo_treinamento", "Catalogo")
-- MAGIC dbutils.widgets.text("database", "default", "Database")
-- MAGIC
-- MAGIC catalogo = dbutils.widgets.get("catalogo")
-- MAGIC database = dbutils.widgets.get("database")
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"use {catalogo}.{database}")

-- COMMAND ----------

-- MAGIC %md ### B. Criar a função

-- COMMAND ----------

CREATE OR REPLACE FUNCTION CONSULTAR_CLIENTE(id_cliente BIGINT)
RETURNS TABLE (nome STRING, sobrenome STRING, num_pedidos INT)
COMMENT 'Use esta função para consultar os dados de um cliente'
RETURN 
  SELECT nome, sobrenome, num_pedidos 
    FROM llm_clientes c 
   WHERE c.id_cliente = consultar_cliente.id_cliente

-- COMMAND ----------

-- MAGIC %md ### C. Testar a função

-- COMMAND ----------

SELECT * FROM consultar_cliente(1)

-- COMMAND ----------

-- MAGIC %md ### D. Testar a função como ferramenta
-- MAGIC
-- MAGIC 1. No **menu principal** à esquerda, clique em **`Playground`**
-- MAGIC 2. Clique no **seletor de modelos** e selecione o modelo **`Claude Sonnet 4`** (caso já não esteja selecionado)
-- MAGIC 3. Clique em **Tools** e depois em **Add Tool** 
-- MAGIC 4. Em **Hosted Function**, digite `catalogo_treinamento.<seu_nome>.consultar_cliente` e `catalogo_treinamento.<seu_nome>.llm_revisar_avaliacao`
-- MAGIC 5. Adicione a instrução abaixo:<br>
-- MAGIC     `Gere uma resposta para o cliente 1 que está insatisfeito com a qualidade da tela do seu tablet. Não esqueça de customizar a mensagem com o nome do cliente.`
-- MAGIC 6. Clique no ícone **enviar**

-- COMMAND ----------

-- MAGIC %md ### E. Analisando os resultados
-- MAGIC
-- MAGIC Com o resultado do exercício anterior, siga os passos abaixo:
-- MAGIC
-- MAGIC 1. Na parte inferior da resposta, clique em **`View Trace`** 
-- MAGIC 2. Neste painel, navegue entre as diferentes ações executadas à esquerda
-- MAGIC
-- MAGIC Dessa forma, você poderá entender a linha de raciocínio do agente, ou seja, quais ações foram executas, com quais parâmetros e em que ordem. Além disso, quando houver algum erro de execução, também servirá de insumo para entendermos e corrigirmos eventuais problemas.

-- COMMAND ----------

-- MAGIC %md # Parabéns!
-- MAGIC
-- MAGIC Você concluiu o laboratório de **RAG**!
-- MAGIC
-- MAGIC Agora, você já sabe como utilizar o Foundation Models, Playground e Unity Catalog Tools para prototipar de forma rápida e simplificada agentes capazes de responder precisamente a perguntas complexas!