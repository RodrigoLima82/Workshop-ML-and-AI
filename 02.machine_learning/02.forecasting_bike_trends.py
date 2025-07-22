# Databricks notebook source
# MAGIC %md
# MAGIC # Exemplos de Time Series Forecasting com Prophet

# COMMAND ----------

# MAGIC %pip install --upgrade threadpoolctl

# COMMAND ----------

# MAGIC %pip install prophet
# MAGIC
# MAGIC import pandas as pd
# MAGIC from prophet import Prophet
# MAGIC import matplotlib.pyplot as plt
# MAGIC from sklearn.metrics import mean_absolute_error, mean_squared_error
# MAGIC from math import sqrt

# COMMAND ----------

# Load Example Time Series Data
# We'll use the bike sharing dataset available on Databricks
sparkDF = spark.read.csv("/databricks-datasets/bikeSharing/data-001/day.csv", header=True, inferSchema=True)
display(sparkDF)  # Optional: Visualize the data in Databricks

# Convert to pandas DataFrame and select relevant columns
df = sparkDF.select("dteday", "cnt").toPandas()
df.rename(columns={"dteday": "ds", "cnt": "y"}, inplace=True)

# COMMAND ----------

# Split Data into Train and Test Sets
# Use the last 30 days as the test set
train_df = df[:-30]
test_df = df[-30:]

# COMMAND ----------

# Train Prophet Model
prophet_model = Prophet()
prophet_model.fit(train_df)

# COMMAND ----------

# Forecast Future Values
future = prophet_model.make_future_dataframe(periods=30, freq='D')
forecast = prophet_model.predict(future)

# COMMAND ----------

# Evaluate Forecast Accuracy
# Merge actuals with forecast for the test period
forecast_test = forecast.set_index('ds').loc[test_df['ds']]
actuals = test_df['y'].values
predicted = forecast_test['yhat'].values

mae = mean_absolute_error(actuals, predicted)
mse = mean_squared_error(actuals, predicted)
rmse = sqrt(mse)

print(f"MAE: {mae:.2f}")
print(f"MSE: {mse:.2f}")
print(f"RMSE: {rmse:.2f}")

# COMMAND ----------

# Visualize Actual vs Forecasted Values
plt.figure(figsize=(12,6))
plt.plot(df['ds'], df['y'], label='Actual', color='blue')
plt.plot(forecast['ds'], forecast['yhat'], label='Forecast', color='orange')
plt.axvspan(test_df['ds'].iloc[0], test_df['ds'].iloc[-1], color='gray', alpha=0.2, label='Test Period')
plt.xlabel('Date')
plt.ylabel('Count')
plt.title('Actual vs Forecasted Values')
plt.legend()
plt.show()