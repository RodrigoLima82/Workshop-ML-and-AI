# Databricks notebook source
# MAGIC %md
# MAGIC ### Create Model Serving Endpoint
# MAGIC There are three different options for creating a model serving endpoint for your custom model: 
# MAGIC 1. **Use the Databricks Serving UI - this is the easiest way for getting started**
# MAGIC 2. REST API 
# MAGIC 3. MLflow Deployments SDK
# MAGIC
# MAGIC [Documentation for options 1-3]("https://docs.databricks.com/en/machine-learning/model-serving/create-manage-serving-endpoints.html#language-MLflow%C2%A0Deployments%C2%A0SDK")
# MAGIC
# MAGIC ##### This Notebook example shows how to create the model serving endpoint using the MLflow Deployments SDK.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Install MLflow Deployment Client
import mlflow.deployments

client = mlflow.deployments.get_deploy_client("databricks")


# COMMAND ----------

# MAGIC %md
# MAGIC MLflow Deployments provides an API for create, update and deletion tasks. The APIs for these tasks accept the same parameters as the REST API for serving endpoints. [See POST /api/2.0/serving-endpoints for endpoint configuration parameters]("https://docs.databricks.com/api/workspace/servingendpoints/create")

# COMMAND ----------

from mlflow.deployments import get_deploy_client

client = get_deploy_client("databricks")
endpoint = client.create_endpoint(
    name="iris_model_endpoint",
    config={
        "served_entities": [
            {
                "entity_name": "workspace.default.iris_custom_model",
                "entity_version": "1",
                "workload_size": "Small",
                "scale_to_zero_enabled": True
            }
        ],
        "traffic_config": {
            "routes": [
                {
                    "served_model_name": "iris_custom_model-1",
                    "traffic_percentage": 100
                }
            ]
        }
    }
)

# COMMAND ----------

# DBTITLE 1,Get info about your endpoint
from mlflow.deployments import get_deploy_client

# Get the deployment client for Databricks
client = get_deploy_client("databricks")

# Replace 'endpoint_name' with the actual name of your endpoint
endpoint_name = "iris_model_endpoint"

# Get the endpoint details using the correct method
endpoint_details = client.get_endpoint(endpoint_name)

# Display the endpoint details
print(endpoint_details)
