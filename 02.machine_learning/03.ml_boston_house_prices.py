# Databricks notebook source
# MAGIC %md
# MAGIC # Conjunto de Dados de Habitação de Boston: 
# MAGIC
# MAGIC Usaremos o conjunto de dados de Habitação de Boston, um clássico para problemas de regressão. Ele contém informações sobre casas em Boston e várias características (como número de quartos, taxa de criminalidade, etc.) para prever os preços das casas.

# COMMAND ----------

# MAGIC %pip install --upgrade threadpoolctl

# COMMAND ----------

# DBTITLE 1,1. Import libraries
import pandas as pd
import numpy as np
from sklearn.datasets import fetch_openml

# COMMAND ----------

# DBTITLE 1,2. Load the Boston Housing dataset from OpenML (since sklearn removed it from the main datasets)
# fetch_openml is a convenient API for loading many public datasets.
boston = fetch_openml(name="boston", version=1, as_frame=True)
df = boston.frame  # This is a pandas DataFrame

# COMMAND ----------

# DBTITLE 1,3. Explore the dataset
df.head()

# COMMAND ----------

# DBTITLE 1,4. Let's see what columns (features) we have
print(df.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC The features include:
# MAGIC - CRIM: Crime rate by town
# MAGIC - ZN: Proportion of residential land zoned for lots over 25,000 sq.ft.
# MAGIC - INDUS: Proportion of non-retail business acres per town
# MAGIC - CHAS: Charles River dummy variable (= 1 if tract bounds river; 0 otherwise)
# MAGIC - NOX: Nitric oxides concentration (parts per 10 million)
# MAGIC - RM: Average number of rooms per dwelling
# MAGIC - AGE: Proportion of owner-occupied units built prior to 1940
# MAGIC - DIS: Weighted distances to five Boston employment centres
# MAGIC - RAD: Index of accessibility to radial highways
# MAGIC - TAX: Full-value property-tax rate per $10,000
# MAGIC - PTRATIO: Pupil-teacher ratio by town
# MAGIC - B: 1000(Bk - 0.63)^2 where Bk is the proportion of Black residents by town
# MAGIC - LSTAT: % lower status of the population
# MAGIC - MEDV: Median value of owner-occupied homes in $1000's (this is our target variable)

# COMMAND ----------

# DBTITLE 1,5. Define features (X) and target (y)
X = df.drop("MEDV", axis=1)  # All columns except 'MEDV'
y = df["MEDV"]               # 'MEDV' is the target

# COMMAND ----------

# DBTITLE 1,6. Split data into train and test sets
from sklearn.model_selection import train_test_split

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

# COMMAND ----------

# DBTITLE 1,7. Train a regression model (Linear Regression)
from sklearn.linear_model import LinearRegression

model = LinearRegression()
model.fit(X_train, y_train)

# COMMAND ----------

# DBTITLE 1,8. Evaluate the model
from sklearn.metrics import mean_squared_error, r2_score

# Ensure all columns in X_train and X_test are numeric
X_train = X_train.apply(pd.to_numeric, errors='coerce')
X_test = X_test.apply(pd.to_numeric, errors='coerce')

# Now you can safely call predict and evaluate
y_pred = model.predict(X_test)
mse = mean_squared_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)

print(f"Mean Squared Error: {mse:.2f}")
print(f"R^2 Score: {r2:.2f}")

# COMMAND ----------

# DBTITLE 1,9. Feature importance (coefficients)
# This helps us understand which features are most influential in predicting house prices
feature_importance = pd.Series(model.coef_, index=X.columns)
feature_importance.sort_values(ascending=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resumo
# MAGIC - Carregamos um conjunto de dados do mundo real do OpenML usando uma API.
# MAGIC - Exploramos e preparamos os dados, selecionamos as variáveis e o alvo.
# MAGIC - Treinamos e avaliamos um modelo de regressão.
# MAGIC - Examinamos quais variáveis foram mais importantes para prever os preços das casas.
# MAGIC
# MAGIC Experimente utilizar outros conjuntos de dados do OpenML ou Kaggle para praticar mais!