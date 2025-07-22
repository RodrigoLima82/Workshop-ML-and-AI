# Databricks notebook source
# DBTITLE 1,Set Token Env. Var
import os
os.environ['DATABRICKS_TOKEN'] = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
os.environ['MODEL_ENDPOINT'] = '<endpoint_url>'

# COMMAND ----------

# DBTITLE 1,Query Model Serving Endpoint


import requests
import numpy as np
import pandas as pd
import json


def create_tf_serving_json(data):
    return {'dataframe_records': data.to_dict(orient='records')}

def score_model(dataset):
    url = os.environ.get("MODEL_ENDPOINT")
    headers = {'Authorization': f'Bearer {os.environ.get("DATABRICKS_TOKEN")}', 'Content-Type': 'application/json'}
    data_json = json.dumps(create_tf_serving_json(dataset), allow_nan=True)
    response = requests.request(method='POST', headers=headers, url=url, data=data_json)
    if response.status_code != 200:
        raise Exception(f'Request failed with status {response.status_code}, {response.text}')

    return response.json()



# COMMAND ----------

# Create the original input format as a pandas DataFrame
iris_data = pd.DataFrame({
    "sepal_length": [5.1, 5.9, 6.3],
    "sepal_width": [3.5, 3.0, 3.3],
    "petal_length": [1.4, 4.2, 6.0],
    "petal_width": [0.2, 1.5, 2.5],
    "species": ["setosa", "versicolor", "virginica"]
})

# Call the score_model function and print the results
result = score_model(iris_data)
print(json.dumps(result, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Curl example
# MAGIC ~~~
# MAGIC curl -X POST -u token:XXXXXXXXXXXXXXXX https:/XXXXXXXX \
# MAGIC   -H "Content-Type: application/json" \
# MAGIC   -d '{
# MAGIC     "dataframe_split": {
# MAGIC       "columns": ["sepal_length", "sepal_width", "petal_length", "petal_width", "species"],
# MAGIC       "data": [
# MAGIC         [5.1, 3.5, 1.4, 0.2, "setosa"],
# MAGIC         [5.9, 3.0, 4.2, 1.5, "versicolor"],
# MAGIC         [6.3, 3.3, 6.0, 2.5, "virginica"]
# MAGIC       ]
# MAGIC     }
# MAGIC   }'
# MAGIC   ~~~~

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC #### For Custom Models, Model Serving supports scoring requests as Pandas DataFrame 
# MAGIC Requests should be sent by constructing a JSON-serialized Pandas DataFrame with one of the supported keys and a JSON object corresponding to the input format.
# MAGIC
# MAGIC * **(Recommended) dataframe_split format is a JSON-serialized Pandas DataFrame in the split orientation.**  
# MAGIC ~~~
# MAGIC {
# MAGIC   "dataframe_split": {
# MAGIC     "index": [0, 1],
# MAGIC     "columns": ["sepal length (cm)", "sepal width (cm)", "petal length (cm)", "petal width (cm)"],
# MAGIC     "data": [[5.1, 3.5, 1.4, 0.2], [4.9, 3.0, 1.4, 0.2]]
# MAGIC   }
# MAGIC }
# MAGIC ~~~
# MAGIC * **dataframe_records is JSON-serialized Pandas DataFrame in the records orientation.**
# MAGIC
# MAGIC Note: This format does not guarantee the preservation of column ordering, and the split format is preferred over the records format.
# MAGIC ~~~
# MAGIC {
# MAGIC   "dataframe_records": [
# MAGIC   {
# MAGIC     "sepal length (cm)": 5.1,
# MAGIC     "sepal width (cm)": 3.5,
# MAGIC     "petal length (cm)": 1.4,
# MAGIC     "petal width (cm)": 0.2
# MAGIC   },
# MAGIC   {
# MAGIC     "sepal length (cm)": 4.7,
# MAGIC     "sepal width (cm)": 3.2,
# MAGIC     "petal length (cm)": 1.3,
# MAGIC     "petal width (cm)": 0.2
# MAGIC   }
# MAGIC   ]
# MAGIC }
# MAGIC ~~~

# COMMAND ----------

# MAGIC %md
# MAGIC