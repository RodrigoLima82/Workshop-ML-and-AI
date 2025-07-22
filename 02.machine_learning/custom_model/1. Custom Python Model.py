# Databricks notebook source
# MAGIC %md
# MAGIC ### What is a Custom Model Wrapper Class? 
# MAGIC ######Using a custom model wrapper class in MLflow is beneficial when you have specific needs that go beyond the capabilities of typical machine learning models. Examples of Using Custom Model Wrapper Classes in MLflow: 
# MAGIC 1. Custom Preprocessing and Postprocessing
# MAGIC * Encapsulate data transformations that occur before and after model predictions.
# MAGIC * Useful for tasks like scaling inputs or adjusting outputs to fit specific needs.
# MAGIC 2. Ensemble Models
# MAGIC * Combine predictions from multiple models to create a stronger predictive capability.
# MAGIC * Can implement methods like averaging or voting to aggregate results.
# MAGIC 3. Models with External Dependencies
# MAGIC * Manage dependencies on external resources such as databases or web APIs.
# MAGIC * Ensure the model can interact with these resources to enhance its predictions.
# MAGIC 4. Complex Data Transformations
# MAGIC * Implement sophisticated data transformation logic that standard libraries can't handle. (e.g. Fourier transforms or other advanced mathematical operations.)
# MAGIC 5. **Non-Traditional Models**
# MAGIC * Use custom logic for models that are not typical machine learning models.
# MAGIC * Suitable for scenarios involving domain-specific computations or mathematical equations
# MAGIC
# MAGIC [MLflow Documentation]("https://mlflow.org/docs/latest/traditional-ml/creating-custom-pyfunc/index.html")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example: Non-Traditional Model
# MAGIC This example takes the iris dataset, and applies python functions based on a category (in this case, species). Setosa returns petal area, versicolor returns the sepal length/width ratio,and virginica returns the average size. The following wraps the model and defines the input and output schemas. From there, you can run and log the model using pyfunc.
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Python Function as pyfunc model
import mlflow
import mlflow.pyfunc
import pandas as pd
import json
from mlflow.models.signature import ModelSignature
from mlflow.types import Schema, ColSpec

# Set the MLflow registry URI
mlflow.set_registry_uri("databricks")

# load data
iris_url = 'https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data'
iris_columns = ["sepal_length", "sepal_width", "petal_length", "petal_width", "species"]
iris_df = pd.read_csv(iris_url, header=None, names=iris_columns)

# removing prefix from species names 
iris_df['species'] = iris_df['species'].str.replace('Iris-', '').str.strip().str.lower()

# Define specific functions for each species
def process_setosa(row):
    return {"species": row['species'], "label": "petal_area", "value": row['petal_length'] * row['petal_width']}

def process_versicolor(row):
    return {"species": row['species'], "label": "sepal_length_width_ratio", "value": row['sepal_length'] / row['sepal_width']}

def process_virginica(row):
    return {"species": row['species'], "label": "average_size", "value": (row['sepal_length'] + row['sepal_width'] + row['petal_length'] + row['petal_width']) / 4}


# Define ModelWrapper class
class ModelWrapper(mlflow.pyfunc.PythonModel):

    def load_context(self, context):
        self.iris_data = iris_df

    def predict(self, context, model_input):
        data = pd.DataFrame(model_input)
        data['species'] = data['species'].str.strip().str.lower()

        # species data processing 
        results = []
        for _, row in data.iterrows():
            if row['species'] == 'setosa':
                result = process_setosa(row)
            elif row['species'] == 'versicolor':
                result = process_versicolor(row)
            elif row['species'] == 'virginica':
                result = process_virginica(row)
            else:
                result = {"species": row['species'], "label": "unknown_species", "value": None}
            
            results.append(result)
        
        return results

# instance wrapped model 
wrapped_model = ModelWrapper()

# Define input and output schemas
input_schema = Schema([ColSpec("double", name) for name in iris_columns[:-1]] + [ColSpec("string", "species")])
output_schema = Schema([
    ColSpec("string", "species"),
    ColSpec("string", "label"),
    ColSpec("double", "value")
])
signature = ModelSignature(inputs=input_schema, outputs=output_schema)

# input example 
input_example = pd.DataFrame({
    "sepal_length": [5.1],
    "sepal_width": [3.5],
    "petal_length": [1.4],
    "petal_width": [0.2],
    "species": ["setosa"]
})

# Log model
with mlflow.start_run():
    mlflow.pyfunc.log_model("iris_processor_model", 
                            python_model=wrapped_model, 
                            input_example=input_example.to_json(orient="split",index=False), 
                            signature=signature)
    run_id = mlflow.active_run().info.run_id

# Load the model 
loaded_model = mlflow.pyfunc.load_model(f"runs:/{run_id}/iris_processor_model")

# test data 
df_test = pd.DataFrame({
    "sepal_length": [5.1, 5.9, 6.3],
    "sepal_width": [3.5, 3.0, 3.3],
    "petal_length": [1.4, 4.2, 6.0],
    "petal_width": [0.2, 1.5, 2.5],
    "species": ["setosa", "versicolor", "virginica"]
})

# predict
predictions = loaded_model.predict(df_test)
print(json.dumps(predictions, indent=2))

# COMMAND ----------

catalog = "workspace"
schema = "default"
model_name = "iris_custom_model"

mlflow.register_model((f"runs:/{run_id}/iris_processor_model"), f"{catalog}.{schema}.{model_name}")