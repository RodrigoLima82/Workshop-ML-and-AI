-- Databricks notebook source
-- MAGIC %md <img src="https://github.com/Databricks-BR/lab_genai/blob/main/img/header.png?raw=true" width=100%>
-- MAGIC
-- MAGIC # Hands-On LAB 01 - Análise de sentimento, extração de entidades e geração de texto
-- MAGIC
-- MAGIC Treinamento Hands-on na plataforma Databricks com foco nas funcionalidades de IA Generativa.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Objetivos do Exercício
-- MAGIC
-- MAGIC O objetivo desse laboratório é implementar o seguinte caso de uso:
-- MAGIC
-- MAGIC ### Aumentando a satisfação do cliente com análise automática de avaliações
-- MAGIC
-- MAGIC Neste laboratório, construiremos um pipeline de dados que pega avaliações de clientes, na forma de texto livre, e as enriquece com informações extraídas ao fazer perguntas em linguagem natural aos modelos de IA Generativa disponíveis no Databricks. Também forneceremos recomendações para as próximas melhores ações à nossa equipe de atendimento ao cliente - ou seja, se um cliente requer acompanhamento e um rascunho de mensagem de resposta.
-- MAGIC
-- MAGIC Para cada avaliação, nós:
-- MAGIC
-- MAGIC - Identificamos o sentimento do cliente e extraímos os produtos mencionados
-- MAGIC - Geramos uma resposta personalizada para o cliente
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/sql-ai-functions/sql-ai-query-function-review.png" width="100%">

-- COMMAND ----------

-- MAGIC %md ## Preparação
-- MAGIC
-- MAGIC Para executar os exercícios, precisamos conectar este notebook a um compute.
-- MAGIC
-- MAGIC Basta seguir os passos abaixo:
-- MAGIC 1. No canto superior direito, clique em **Connect**
-- MAGIC 2. Selecione o **SQL Warehouse** desejado ou **SERVERLESS**

-- COMMAND ----------

-- MAGIC %md ## Exercício 01.01 - Acessando o conjunto de dados
-- MAGIC
-- MAGIC Agora, vamos acessar as avaliações de produto que carregamos no laboratório anterior.
-- MAGIC
-- MAGIC Neste laboratório iremos utilizar duas tabelas:
-- MAGIC - **Avaliações**: dados não-estruturados com o conteúdo das avaliações
-- MAGIC - **Clientes**: dados estruturados como o cadastro e consumo dos clientes
-- MAGIC
-- MAGIC Agora, vamos visualizar estes dados!

-- COMMAND ----------

-- MAGIC %md ### A. Selecionar o database que criamos anteriormente

-- COMMAND ----------

-- MAGIC %python
-- MAGIC catalogo = 'workspace'
-- MAGIC database = 'default'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"use {catalogo}.{database}")

-- COMMAND ----------

-- MAGIC %md ### B. Visualizar a tabela de avaliações

-- COMMAND ----------

SELECT * FROM llm_avaliacoes

-- COMMAND ----------

-- MAGIC %md ### C. Visualizar a tabela de clientes

-- COMMAND ----------


SELECT * FROM llm_clientes

-- COMMAND ----------

-- MAGIC %md ## Exercício 01.02 - Analisando sentimento e extraindo informações
-- MAGIC
-- MAGIC Nosso objetivo é permitir a análise rápida de grandes volumes de avaliações de forma rápida e eficiente. Para isso, precisamos extrair as seguintes informações:
-- MAGIC
-- MAGIC - Produtos mencionados
-- MAGIC - Sentimento do cliente
-- MAGIC - Caso seja negativo, qual o motivo da insatisfação
-- MAGIC
-- MAGIC Vamos ver como podemos aplicar IA Generativa para acelerar nosso trabalho.

-- COMMAND ----------

-- MAGIC %md-sandbox ### 1. Usando Foundation Models
-- MAGIC
-- MAGIC Primeiro, precisamos de um modelo capaz de interpretar o texto das avaliações e extrair as informações desejadas. Para isso, vamos utilizar **[Foundation Models](https://docs.databricks.com/en/machine-learning/foundation-models/index.html#pay-per-token-foundation-model-apis)**, que são grandes modelos de linguagem (LLMs) servidos pela Databricks e que podem ser consultados sob-demanda sem a necessidade de implantação ou gerenciamento desses recursos.
-- MAGIC
-- MAGIC Existem varios modelos disponíveis!
-- MAGIC
-- MAGIC Vamos vê-los em funcionamento!
-- MAGIC
-- MAGIC 1. No **menu principal** à esquerda, clique em **`Serving`**
-- MAGIC 2. No card do modelo **Gemma 3 12B**, clique em **`Use`**
-- MAGIC 3. Adicione a instrução abaixo:
-- MAGIC     ```
-- MAGIC     Classifique o sentimento da seguinte avaliação:
-- MAGIC     Comprei um tablet e estou muito insatisfeito com a qualidade da bateria. Ela dura muito pouco tempo e demora muito para carregar.
-- MAGIC     ```
-- MAGIC     <br>
-- MAGIC 4. Clique no ícone **enviar**
-- MAGIC
-- MAGIC Com isso, já conseguimos de forma rápida começar a prototipar nossos novos produtos de dados!

-- COMMAND ----------

-- MAGIC %md-sandbox ### 2. Comparando modelos no AI Playground
-- MAGIC
-- MAGIC Para decidir qual o melhor modelo e instrução para o nosso caso de uso, podemos utilizar o **[AI Playground](https://docs.databricks.com/en/large-language-models/ai-playground.html)**.
-- MAGIC
-- MAGIC Assim, podemos testar rapidamente diversas combinações de modelos e instruções através de uma interface intuitiva e escolher a melhor opção par utilizarmos no nosso projeto.
-- MAGIC
-- MAGIC Vamos fazer o seguinte teste:
-- MAGIC
-- MAGIC 1. Clique no ícone **`Add endpoint`**
-- MAGIC 2. Clique no **seletor de modelos** e selecione o modelo **`Llama 4 Maverick`**
-- MAGIC 3. Clique no ícone **`Add endpoint`**
-- MAGIC 4. Adicione a instrução abaixo:
-- MAGIC     ```
-- MAGIC     Classifique o sentimento da seguinte avaliação:
-- MAGIC     Comprei um tablet e estou muito insatisfeito com a qualidade da bateria. 
-- MAGIC     Ela dura muito pouco tempo e demora muito para carregar.
-- MAGIC     ```
-- MAGIC     <br>
-- MAGIC 7. Clique no ícone **enviar**
-- MAGIC
-- MAGIC Agora, podemos comparar as respostas, o tempo e o custo de cada um dos modelos para escolher aquele que melhor atende às necessidades do nosso projeto!

-- COMMAND ----------

-- MAGIC %md ### 3. Usando AI Functions
-- MAGIC
-- MAGIC Por fim, para que possamos escalar a utilização dos modelos de IA Generativa, podemos utilizar **[AI Functions](https://docs.databricks.com/en/large-language-models/ai-functions.html)**.
-- MAGIC
-- MAGIC Estas permitem executar modelos de IA Generativa sobre nossos bancos de dados corporativos diretamente em consultas SQL, uma linguagem amplamente utiliza por analistas de dados e de negócio. Com isso, também podemos criar novas tabelas com as informações extraídas para serem utilizadas em nossas análises.
-- MAGIC
-- MAGIC Existem funções nativas para executar tarefas pré-definidas ou enviar qualquer instrução desejada para ser executada. Seguem as descrições abaixo:
-- MAGIC
-- MAGIC | Gen AI SQL Function | Descrição |
-- MAGIC | -- | -- |
-- MAGIC | [ai_analyze_sentiment](https://docs.databricks.com/pt/sql/language-manual/functions/ai_analyze_sentiment.html) | Análise de Sentimento |
-- MAGIC | [ai_classify](https://docs.databricks.com/pt/sql/language-manual/functions/ai_classify.html) | Classifica o texto de acordo com as categorias definidas |
-- MAGIC | [ai_extract](https://docs.databricks.com/pt/sql/language-manual/functions/ai_extract.html) | Extrai as entidades desejadas |
-- MAGIC | [ai_fix_grammar](https://docs.databricks.com/pt/sql/language-manual/functions/ai_fix_grammar.html) | Corrige a gramática do texto fornecido |
-- MAGIC | [ai_gen](https://docs.databricks.com/pt/sql/language-manual/functions/ai_gen.html) | Gera um novo texto conforme a instrução | 
-- MAGIC | [ai_mask](https://docs.databricks.com/pt/sql/language-manual/functions/ai_mask.html) | Marcara dados sensíveis |
-- MAGIC | [ai_query](https://docs.databricks.com/pt/sql/language-manual/functions/ai_query.html) | Envia instruções para o modelo desejado |
-- MAGIC | [ai_similarity](https://docs.databricks.com/pt/sql/language-manual/functions/ai_similarity.html) | Calcula a similaridade entre duas expressões |
-- MAGIC | [ai_summarize](https://docs.databricks.com/pt/sql/language-manual/functions/ai_summarize.html) | Sumariza o texto fornecido |
-- MAGIC | [ai_translate](https://docs.databricks.com/pt/sql/language-manual/functions/ai_translate.html) | Traduz o texto fornecido |
-- MAGIC
-- MAGIC Para extrair as informações que precisamos, vamos utilizar algumas dessas funções abaixo!

-- COMMAND ----------

-- MAGIC %md #### A. Análise de sentimento

-- COMMAND ----------

SELECT *, ai_analyze_sentiment(avaliacao) AS sentimento 
FROM llm_avaliacoes LIMIT 10

-- COMMAND ----------

-- MAGIC %md #### B. Extração dos produtos mencionados

-- COMMAND ----------

SELECT *, ai_extract(avaliacao, ARRAY('produto')) AS produtos FROM llm_avaliacoes LIMIT 10

-- COMMAND ----------

-- MAGIC %md #### C. Extração do motivo da insatisfação
-- MAGIC
-- MAGIC *DICA: use a função AI_QUERY() para fornecer um prompt customizado*

-- COMMAND ----------

SELECT *, ai_query(
  'databricks-gemma-3-12b', 
  concat('Se o sentimento da avaliação for negativo, liste os motivos de insatisfação. Avaliação: ', avaliacao)) AS motivo_insatisfacao 
FROM llm_avaliacoes LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4. Analisando sentimento e extraindo entidades em escala
-- MAGIC
-- MAGIC Ter que especificar as instruções várias vezes acaba sendo trabalhoso, especialmente para Analistas de Dados que deveriam focar em analisar os resultados dessa extração.
-- MAGIC
-- MAGIC Para simplificar o acesso à essa inteligência, criaremos uma função SQL para encapsular esse processo e poder apenas informar em qual coluna do nosso conjunto de dados gostaríamos de aplicá-la.
-- MAGIC
-- MAGIC Aqui, vamos aproveitar para enviar uma única consulta ao nosso modelo para extrair todas as informações de uma única vez!
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/sql-ai-functions/sql-ai-query-function-review-wrapper.png" width="1200px">

-- COMMAND ----------

-- MAGIC %md #### A. Criando uma função para extrair todas as informações

-- COMMAND ----------

CREATE OR REPLACE FUNCTION REVISAR_AVALIACAO(avaliacao STRING)
RETURNS STRUCT<produto_nome: STRING, produto_categoria: STRING, sentimento: STRING, resposta: STRING, resposta_motivo: STRING>
RETURN FROM_JSON(REPLACE(
  AI_QUERY(
    'databricks-gemma-3-12b',
    CONCAT(
      'Um cliente fez uma avaliação. Nós respondemos todos que aparentem descontentes.
      Extraia as seguintes informações:
      - extraia o nome do produto
      - extraia a categoria de produto, por exemplo: tablet, notebook, smartphone
      - classifique o sentimento como ["POSITIVO","NEGATIVO","NEUTRO"]
      - retorne se o sentimento é NEGATIVO e precisa de responsta: S ou N
      - se o sentimento é NEGATIVO, explique quais os principais motivos
      - remover todos e sempre os caracteres ``` do resultado
      Retorne somente um JSON. Nenhum outro texto fora o JSON. Formato do JSON:
      {
        "produto_nome": <entidade nome>,
        "produto_categoria": <entidade categoria>,
        "sentimento": <entidade sentimento>,
        "resposta": <S ou N para resposta>,
        "motivo": <motivos de insatisfação>
      }
      Avaliação: ', avaliacao
    )
  ),'```json', ''),
  "STRUCT<produto_nome: STRING, produto_categoria: STRING, sentimento: STRING, resposta: STRING, motivo: STRING>"
)

-- COMMAND ----------

-- MAGIC %md #### B. Testando a análise das avaliações

-- COMMAND ----------

SELECT revisar_avaliacao('Comprei um tablet ASD e estou muito insatisfeito com a qualidade da bateria. Ela dura muito pouco tempo e demora muito para carregar.') AS resultado


-- COMMAND ----------

-- MAGIC %md #### C. Analisando todas as avaliações

-- COMMAND ----------

CREATE OR REPLACE TABLE llm_avaliacoes_revisadas AS
SELECT *, resultado.* FROM (
  SELECT *, revisar_avaliacao(avaliacao) as resultado FROM llm_avaliacoes LIMIT 10)  

-- COMMAND ----------

SELECT * FROM llm_avaliacoes_revisadas

-- COMMAND ----------

-- MAGIC %md Agora, todos os nossos usuários podem aproveitar nossa função que foi cuidadosamente preparada para analisar nossas avaliações de produtos.
-- MAGIC
-- MAGIC E podemos escalar esse processo facilmente aplicando essa função sobre todo o nosso conjunto de dados!

-- COMMAND ----------

-- MAGIC %md ### 5. Analisando os dados enriquecidos
-- MAGIC
-- MAGIC Com as informações extraídas nos laboratórios anteriores, nossos times de negócio podem aproveitar para analisar as avaliações de produtos facilmente – já que agora temos todos os dados estruturados dentro de uma simples tabela.
-- MAGIC
-- MAGIC A partir dessa base de dados enriquecida criada, podemos construir análises ad-hoc, dashboards e alertas diretamente do Databricks. E, para facilitar ainda mais a vida dos nossos analistas, podemos fazer isso usando somente linguagem natural!
-- MAGIC
-- MAGIC Aqui vamos explorar como utilizar a **Genie** para analisar nossas avaliações de produto.
-- MAGIC
-- MAGIC <br>
-- MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/sql-ai-functions/sql-ai-function-flow.png" width="1000">

-- COMMAND ----------

-- MAGIC %md ## Exercício 01.03 - Gerando uma sugestão de resposta
-- MAGIC
-- MAGIC Com todas as informações extraídas, podemos aproveitá-las para gerar sugestões de respostas personalizadas para acelerar o trabalho dos nossos times de atendimento.
-- MAGIC
-- MAGIC Outro ponto interessante é que, nesse processo, podemos aproveitar outras **informações estruturadas** que já tenhamos no nosso ambiente, como dados demográficos, psicográficos e o histórico de compras, para customizar ainda mais nossas respostas!
-- MAGIC
-- MAGIC Vamos ver como fazer isso!

-- COMMAND ----------

-- MAGIC %md ### A. Criando uma função para gerar um exemplo de resposta

-- COMMAND ----------

CREATE OR REPLACE FUNCTION GERAR_RESPOSTA(nome STRING, sobrenome STRING, num_pedidos INT, produto STRING, motivo STRING)
RETURNS TABLE(resposta STRING)
COMMENT 'Caso o cliente demonstre insatisfação com algum produto, use esta função para gerar uma resposta personalizada'
RETURN SELECT AI_QUERY(
    'databricks-gemma-3-12b',
    CONCAT(
        "Você é um assistente virtual de um e-commerce. Nosso cliente, ", gerar_resposta.nome, " ", gerar_resposta.sobrenome, " que comprou ", gerar_resposta.num_pedidos, " produtos este ano estava insatisfeito com o produto ", gerar_resposta.produto, 
        ", pois ", gerar_resposta.motivo, ". Forneça uma breve mensagem empática para o cliente incluindo a oferta de troca do produto, caso  esteja em conformidade com a nossa política de trocas. A troca pode ser feita diretamente por esse assistente. ",
        "Eu quero recuperar sua confiança e evitar que ele deixe de ser nosso cliente. ",
        "Escreva uma mensagem com poucas sentenças. ",
        "Não adicione nenhum texto além da mensagem. ",
        "Não adicione nenhuma assinatura."
    )
)

-- COMMAND ----------

-- MAGIC %md ### B. Gerando respostas automatizadas para todas as avaliações negativas

-- COMMAND ----------

CREATE OR REPLACE TABLE llm_respostas AS

  WITH avaliacoes_enriq AS (
    SELECT a.*, c.* EXCEPT (c.id_cliente) 
    FROM llm_avaliacoes_revisadas a 
    LEFT JOIN llm_clientes c 
    ON a.id_cliente = c.id_cliente 
    WHERE a.resposta = 'S' 
    LIMIT 10
  )

  SELECT 
    *, 
    (SELECT * FROM gerar_resposta(e.nome, e.sobrenome, e.num_pedidos, e.produto_nome, e.resposta_motivo)) AS rascunho 
  FROM avaliacoes_enriq e

-- COMMAND ----------

SELECT * FROM llm_respostas

-- COMMAND ----------

-- MAGIC %md # Parabéns!
-- MAGIC
-- MAGIC Você concluiu o laboratório de **Extração de informações e geração de texto**!
-- MAGIC
-- MAGIC Agora, você já sabe como utilizar a Foundation Models, Playground e AI Functions para analisar o sentimento e identificar entidades em avaliações de produtos de forma simples e escalável!