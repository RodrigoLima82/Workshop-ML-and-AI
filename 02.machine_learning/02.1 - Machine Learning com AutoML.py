# Databricks notebook source
# MAGIC %md
# MAGIC # Instruções para Criar um Modelo de Auto ML de Classificação de Churn
# MAGIC
# MAGIC ## Passos:
# MAGIC
# MAGIC #### Navegue até a Seção de Machine Learning:
# MAGIC
# MAGIC 1. No menu lateral, clique em "Machine Learning" para acessar a seção dedicada ao aprendizado de máquina.
# MAGIC
# MAGIC #### Inicie um Novo Experimento de AutoML:
# MAGIC
# MAGIC 2. Dentro da seção de Machine Learning, procure pela opção `"Classification"` e clique nela.
# MAGIC 3. Clique no botão "Start AutoML Experiment" (Iniciar Experimento de AutoML).
# MAGIC
# MAGIC #### Selecione o Cluster:
# MAGIC
# MAGIC 4. Escolha o cluster que tenha um runtime de ML. 
# MAGIC
# MAGIC #### Selecione o Tipo de Modelo:
# MAGIC
# MAGIC 5. Escolha o tipo de problema que você deseja resolver: `classificação`
# MAGIC
# MAGIC #### Configure o Experimento:
# MAGIC
# MAGIC - **Dataset**: Selecione o conjunto de dados que você deseja usar: `tb_clientes_silver`
# MAGIC - **Coluna target**: Especifique a coluna do dataset que você deseja prever: Churn
# MAGIC
# MAGIC #### Configurações Avançadas:
# MAGIC
# MAGIC - Mude o `Timout` para 5 minutos.
# MAGIC - Mude o `Experiment Name` para `<seu_nome>_Churn`
# MAGIC - Mude o `Experiment directory` para `/Workspace/Shared/treinamento/<seu_nome>/databricks_automl/`
# MAGIC
# MAGIC #### Inicie o Treinamento:
# MAGIC
# MAGIC 6. Após configurar o experimento, clique em "Start" para iniciar o processo de AutoML.
# MAGIC 7. O Databricks AutoML irá automaticamente explorar diferentes algoritmos e configurações de hiperparâmetros para encontrar o melhor modelo para o seu conjunto de dados.
# MAGIC
# MAGIC #### Monitore o Progresso:
# MAGIC
# MAGIC 8. Durante o treinamento, você pode monitorar o progresso através do URL do experimento do MLflow que será exibido no console.
# MAGIC 9. Atualize a página do experimento do MLflow para ver os testes à medida que são concluídos.
# MAGIC
# MAGIC #### Analise os Resultados:
# MAGIC
# MAGIC 10. Após a conclusão do experimento, use os links no resumo dos resultados para navegar até o experimento do MLflow ou o notebook que gerou os melhores resultados.
# MAGIC 11. Você pode clonar qualquer notebook gerado a partir dos testes e reexecutá-lo anexando-o ao mesmo cluster para reproduzir os resultados.
# MAGIC
# MAGIC

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("catalogo", "catalogo_treinamento", "Catalogo")
dbutils.widgets.text("database", "default", "Database")

catalogo = dbutils.widgets.get("catalogo")
database = dbutils.widgets.get("database")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Registrando o Melhor Modelo pelo MLflow:

# COMMAND ----------

### Defina o nome do experimento
# Substitua pelo nome do seu experimento específico
usuario = "rodrigo_oliveira" # mudar para o seu nome
experiment_name = "Rodrigo_Oliveira_Churn" # colocar o nome do teu experimento
experiment_path = f"/Workspace/Shared/treinamento/{usuario}/databricks_automl/{experiment_name}"

# COMMAND ----------

import mlflow

### Defina o nome do modelo
model_name = f"{catalogo}.{database}.churn_model"
print(f"Finding best run from {experiment_name}_* and pushing new model version to {model_name}")

### Obtenha o ID do experimento
# Pesquise o experimento pelo nome e ordene pelo último tempo de atualização
experiment_id = mlflow.search_experiments(filter_string=f"name LIKE '%{experiment_name}%'", order_by=["last_update_time DESC"])[0].experiment_id
print(experiment_id)

# COMMAND ----------

# Vamos obter nossa melhor execução de ML
best_model = mlflow.search_runs(
  experiment_ids=experiment_id,
  order_by=["metrics.test_f1_score DESC"],
  max_results=1)
best_model

# COMMAND ----------

# MAGIC %md
# MAGIC Uma vez que temos nosso melhor modelo, podemos registrá-lo no Unity Catalog Model Registry usando seu ID de execução.

# COMMAND ----------

print(f"Registrando modelo em {model_name}")  # {model_name} é definido no script de configuração

# Obtenha o ID da execução do melhor modelo
run_id = best_model.iloc[0]['run_id']

# Registre o melhor modelo das execuções do experimento no registro de modelos do MLflow
model_details = mlflow.register_model(f"runs:/{run_id}/model", model_name)

# COMMAND ----------

# MAGIC %md
# MAGIC Neste ponto, o modelo ainda não possui nenhum alias ou descrição que indique seu ciclo de vida e meta-dados/informações. Vamos atualizar essas informações.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dê uma descrição ao modelo registrado
# MAGIC
# MAGIC Faremos isso para o modelo registrado como um todo.

# COMMAND ----------

from mlflow.tracking import MlflowClient

client = MlflowClient()

# COMMAND ----------

# A descrição principal do modelo, tipicamente feita uma vez.
client.update_registered_model(
  name=model_details.name,
  description="Este modelo prevê se um cliente irá churnar usando as features na tabela mlops_churn_training. Ele é usado para alimentar o Telco Churn Dashboard no DB SQL.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC E adicione mais detalhes sobre a nova versão que acabamos de registrar

# COMMAND ----------

# Forneça mais detalhes sobre esta versão específica do modelo
best_score = best_model['metrics.val_roc_auc'].values[0]
run_name = best_model['tags.mlflow.runName'].values[0]
version_desc = f"Esta versão do modelo tem uma métrica de validação Roc AUC de {round(best_score,4)*100}%. Siga o link para sua execução de treinamento para mais detalhes."

client.update_model_version(
  name=model_details.name,
  version=model_details.version,
  description=version_desc
)

# Também podemos marcar a versão do modelo com a pontuação Roc AUC para visibilidade
client.set_model_version_tag(
  name=model_details.name,
  version=model_details.version,
  key="roc_auc",
  value=f"{round(best_score,4)}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Defina a versão mais recente do modelo como o modelo Challenger
# MAGIC
# MAGIC Vamos definir esta nova versão registrada do modelo como o modelo __Challenger__. Modelos Challenger são modelos candidatos a substituir o modelo Champion, que é o modelo atualmente em uso.
# MAGIC
# MAGIC Usaremos o alias do modelo para indicar o estágio em que ele se encontra em seu ciclo de vida.

# COMMAND ----------

# Defina esta versão como o modelo Challenger, usando seu alias de modelo
client.set_registered_model_alias(
  name=model_name,
  alias="Challenger",
  version=model_details.version
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Testando a inferência do Modelo:
# MAGIC
# MAGIC

# COMMAND ----------

# %pip install --quiet mlflow==2.14.0
# dbutils.library.restartPython()

# COMMAND ----------

from mlflow.store.artifact.models_artifact_repo import ModelsArtifactRepository

# baixar o modelo do registro remoto
requirements_path = ModelsArtifactRepository(
    f"models:/{catalogo}.{database}.churn_model@Challenger"
).download_artifacts(artifact_path="requirements.txt")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inferência em lote no modelo Challenger
# MAGIC
# MAGIC Estamos prontos para executar a inferência no modelo Challenger. Vamos carregar o modelo como uma UDF do Spark e gerar previsões para nossos registros de clientes.
# MAGIC
# MAGIC Para simplificar, assumimos que as features foram extraídas para os novos registros de clientes e já estão armazenadas na tabela de features. Isso é tipicamente feito por pipelines separados de engenharia de features.

# COMMAND ----------

import mlflow

# Carregar as features dos clientes a serem pontuadas
inference_df = spark.read.table(f"{catalogo}.{database}.tb_clientes_silver").limit(10)

# Carregar o modelo Champion como uma UDF do Spark
champion_model = mlflow.pyfunc.spark_udf(spark, model_uri=f"models:/{catalogo}.{database}.churn_model@Challenger")

# Pontuação em lote
preds_df = inference_df.withColumn('predictions', champion_model(*champion_model.metadata.get_input_schema().input_names()))

display(preds_df)