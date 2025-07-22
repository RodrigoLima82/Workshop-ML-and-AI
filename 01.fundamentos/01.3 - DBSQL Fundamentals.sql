-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC # Análise em SQL com o Databricks
-- MAGIC
-- MAGIC Nesse notebook você irá aprender:

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.removeAll()
-- MAGIC
-- MAGIC dbutils.widgets.text("catalogo", "catalogo_treinamento", "Catalogo")
-- MAGIC dbutils.widgets.text("database", "default", "Database")
-- MAGIC
-- MAGIC catalogo = dbutils.widgets.get("catalogo")
-- MAGIC database = dbutils.widgets.get("database")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"use {catalogo}.{database}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Meus primeiros comandos no DBSQL

-- COMMAND ----------

SELECT current_catalog(), current_schema();

-- COMMAND ----------

show tables

-- COMMAND ----------

show volumes

-- COMMAND ----------

select * from tb_clientes_silver limit 10

-- COMMAND ----------

select * from tb_transacoes_silver limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Constraints

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### NOT NULL

-- COMMAND ----------

ALTER TABLE tb_clientes_silver
ALTER COLUMN CodigoCliente SET NOT NULL

-- COMMAND ----------

ALTER TABLE tb_transacoes_silver
ALTER COLUMN id_transacao SET NOT NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Primary Key

-- COMMAND ----------


ALTER TABLE tb_clientes_silver
ADD CONSTRAINT CodigoCliente_pk PRIMARY KEY (CodigoCliente);

-- COMMAND ----------

ALTER TABLE tb_transacoes_silver
ADD CONSTRAINT id_transacao_pk PRIMARY KEY (id_transacao);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Foreign Key

-- COMMAND ----------

ALTER TABLE tb_transacoes_silver
ADD CONSTRAINT fk_id_cliente
FOREIGN KEY (id_cliente)
REFERENCES tb_clientes_silver(CodigoCliente);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Data Quality

-- COMMAND ----------

ALTER TABLE tb_clientes_silver ADD CONSTRAINT uf_check CHECK (len(UF) = 2);


-- COMMAND ----------

SELECT * FROM tb_clientes_silver LIMIT 1


-- COMMAND ----------

INSERT INTO tb_clientes_silver (CodigoCliente, PrimeiroNome, UF)
values
("11111", "Pedro", "São Paulo")

-- COMMAND ----------

INSERT INTO tb_clientes_silver (CodigoCliente, PrimeiroNome, UF)
values
("11111", "Pedro", "SP")

-- COMMAND ----------

select * from tb_clientes_silver where CodigoCliente = "11111"

-- COMMAND ----------

delete from tb_clientes_silver where CodigoCliente = "11111"

-- COMMAND ----------

ALTER TABLE tb_clientes_silver ADD CONSTRAINT cep_check CHECK (cep RLIKE '^[0-9]{5}-[0-9]{3}$');

-- COMMAND ----------

INSERT INTO tb_clientes_silver (CodigoCliente, PrimeiroNome, UF, CEP)
values
("222222", "Joana","RJ","09190360")

-- COMMAND ----------

INSERT INTO tb_clientes_silver (CodigoCliente, PrimeiroNome, UF, CEP)
values
("222222", "Joana", "RJ", "09190-360")

-- COMMAND ----------

select * from tb_clientes_silver where CodigoCliente = "222222"

-- COMMAND ----------

delete from tb_clientes_silver where CodigoCliente = "222222"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Tabelas Gold

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Agora vamos criar uma tabela com as TOP 3 categorias de lojas que os clientes mais compram

-- COMMAND ----------

CREATE OR REPLACE TABLE tb_top_transacoes_categoria_lojista_gold AS
WITH ranked_categories AS (
    SELECT 
        id_cliente,
        categoria_lojista,
        COUNT(*) AS transacao_count,
        ROW_NUMBER() OVER (
            PARTITION BY id_cliente 
            ORDER BY COUNT(*) DESC
        ) AS rank
    FROM 
        tb_transacoes_silver
    GROUP BY 
        id_cliente, 
        categoria_lojista
)
SELECT 
    id_cliente,
    MAX(CASE WHEN rank = 1 THEN categoria_lojista END) AS merchant_category_top1_transactions,
    MAX(CASE WHEN rank = 2 THEN categoria_lojista END) AS merchant_category_top2_transactions,
    MAX(CASE WHEN rank = 3 THEN categoria_lojista END) AS merchant_category_top3_transactions,
    MAX(CASE WHEN rank = 1 THEN transacao_count END) AS merchant_category_top1_transactions_count,
    MAX(CASE WHEN rank = 2 THEN transacao_count END) AS merchant_category_top2_transactions_count,
    MAX(CASE WHEN rank = 3 THEN transacao_count END) AS merchant_category_top3_transactions_count
FROM 
    ranked_categories
GROUP BY 
    id_cliente;

-- COMMAND ----------

select * from tb_top_transacoes_categoria_lojista_gold

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Agora crie uma tabela com as TOP 3 lojas que os clientes mais compram

-- COMMAND ----------

CREATE OR REPLACE TABLE tb_top_transacoes_nome_lojista_gold AS
WITH ranked_categories AS (
    SELECT 
        id_cliente,
        nome_do_lojista,
        COUNT(*) AS transacao_count,
        ROW_NUMBER() OVER (
            PARTITION BY id_cliente 
            ORDER BY COUNT(*) DESC
        ) AS rank
    FROM 
        tb_transacoes_silver
    GROUP BY 
        id_cliente, 
        nome_do_lojista
)
SELECT 
    id_cliente,
    MAX(CASE WHEN rank = 1 THEN nome_do_lojista END) AS merchant_top1_transactions,
    MAX(CASE WHEN rank = 2 THEN nome_do_lojista END) AS merchant_top2_transactions,
    MAX(CASE WHEN rank = 3 THEN nome_do_lojista END) AS merchant_top3_transactions,
    MAX(CASE WHEN rank = 1 THEN transacao_count END) AS merchant_top1_transactions_count,
    MAX(CASE WHEN rank = 2 THEN transacao_count END) AS merchant_top2_transactions_count,
    MAX(CASE WHEN rank = 3 THEN transacao_count END) AS merchant_top3_transactions_count
FROM 
    ranked_categories
GROUP BY 
    id_cliente;

-- COMMAND ----------

select * from tb_top_transacoes_nome_lojista_gold