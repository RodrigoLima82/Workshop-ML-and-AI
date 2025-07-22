# Databricks notebook source
# MAGIC %md
# MAGIC # Aprendizado de Máquina Básico com Pandas e Scikit-learn: Exemplo de Churn de Clientes
# MAGIC
# MAGIC ### Este notebook demonstra os fundamentos do aprendizado de máquina no Databricks usando **Pandas** e **Scikit-learn**, incluindo:
# MAGIC - Criação de um conjunto de dados de amostra
# MAGIC - Exploração e visualização de dados
# MAGIC - Preparação de dados (tratamento de características categóricas)
# MAGIC - Treinamento e avaliação de modelos

# COMMAND ----------

# DBTITLE 1,1. Imports
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix, roc_curve, auc

# COMMAND ----------

# DBTITLE 1,2. Create a sample pandas DataFrame
example_df = pd.DataFrame({
    'gender': ['Female', 'Male', 'Female', 'Male', 'Female'],
    'Partner': ['Yes', 'No', 'No', 'Yes', 'No'],
    'Dependents': ['No', 'No', 'Yes', 'No', 'Yes'],
    'tenure': [1, 34, 2, 45, 5],
    'MonthlyCharges': [29.85, 56.95, 53.85, 42.30, 70.70],
    'TotalCharges': [29.85, 1889.5, 108.15, 1840.75, 151.65], # Note: In real data, convert this to numeric, handle errors/NaNs
    'Churn': ['No', 'No', 'Yes', 'No', 'Yes']
})

# Convert TotalCharges to numeric (important for real datasets)
# example_df['TotalCharges'] = pd.to_numeric(example_df['TotalCharges'], errors='coerce')
# example_df['TotalCharges'].fillna(example_df['TotalCharges'].median(), inplace=True) # Example: fill missing with median

# COMMAND ----------

# DBTITLE 1,3. Exploratory Data Analysis (EDA) & Visualization
# Visualize distribution of numeric features
fig, axs = plt.subplots(1, 3, figsize=(18, 5))

sns.histplot(example_df['tenure'], bins=10, kde=True, ax=axs[0], color='skyblue')
axs[0].set_title('Tenure Distribution')

sns.histplot(example_df['MonthlyCharges'], bins=10, kde=True, ax=axs[1], color='lightgreen')
axs[1].set_title('Monthly Charges Distribution')

# Ensure TotalCharges is numeric before plotting
example_df['TotalCharges'] = pd.to_numeric(example_df['TotalCharges'], errors='coerce').fillna(0) 
sns.histplot(example_df['TotalCharges'], bins=10, kde=True, ax=axs[2], color='salmon')
axs[2].set_title('Total Charges Distribution')

plt.tight_layout()
plt.show()

# COMMAND ----------

# Visualize categorical features vs Churn
categorical_cols = ['gender', 'Partner', 'Dependents']
fig, axs = plt.subplots(1, len(categorical_cols), figsize=(18, 5))

for i, col in enumerate(categorical_cols):
    sns.countplot(x=col, hue='Churn', data=example_df, ax=axs[i], palette='viridis')
    axs[i].set_title(f'{col} vs Churn')
    axs[i].tick_params(axis='x', rotation=45) # Rotate labels if needed

plt.tight_layout()
plt.show()

# COMMAND ----------

# Churn distribution
plt.figure(figsize=(6, 4))
sns.countplot(x='Churn', data=example_df, palette='coolwarm')
plt.title('Churn Distribution')
plt.show()

# COMMAND ----------

# DBTITLE 1,4. Data Preparation for Modeling
# Create a copy for preprocessing
df_processed = example_df.copy()

# Identify categorical and numerical columns
categorical_features = ['gender', 'Partner', 'Dependents']
numerical_features = ['tenure', 'MonthlyCharges', 'TotalCharges']
target_col = 'Churn'

# Encode categorical features using one-hot encoding
df_processed = pd.get_dummies(df_processed, columns=categorical_features, drop_first=True)

# Encode the target variable ('Churn': Yes=1, No=0)
label_encoder = LabelEncoder()
df_processed[target_col] = label_encoder.fit_transform(df_processed[target_col])

# Display processed data head
df_processed.head()

# COMMAND ----------

# Define features (X) and target (y)
X = df_processed.drop(target_col, axis=1)
y = df_processed[target_col]

# Split data into training and testing sets (even for small data, it's good practice)
# Using stratify=y helps maintain the proportion of churn/no-churn in both sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42, stratify=y) 

# Scale numerical features (important for algorithms like Logistic Regression)
scaler = StandardScaler()
X_train[numerical_features] = scaler.fit_transform(X_train[numerical_features])
X_test[numerical_features] = scaler.transform(X_test[numerical_features]) # Use transform only on test set

# Display scaled data head (optional)
print(X_train.head())

# COMMAND ----------

# DBTITLE 1,5. Model Training (Logistic Regression)
# Initialize and train the Logistic Regression model
log_reg = LogisticRegression(random_state=42)
log_reg.fit(X_train, y_train)

# COMMAND ----------

# DBTITLE 1,6. Model Evaluation
# Make predictions on the test set
y_pred = log_reg.predict(X_test)
y_pred_proba = log_reg.predict_proba(X_test)[:, 1] # Probabilities for the positive class (Churn=1)

# Calculate metrics
accuracy = accuracy_score(y_test, y_pred)
conf_matrix = confusion_matrix(y_test, y_pred)
class_report = classification_report(y_test, y_pred)

print(f"Accuracy: {accuracy:.2f}")
print("\nConfusion Matrix:\n", conf_matrix)
print("\nClassification Report:\n", class_report)

# COMMAND ----------

# Visualize Confusion Matrix
plt.figure(figsize=(6, 4))
sns.heatmap(conf_matrix, annot=True, fmt='d', cmap='Blues', xticklabels=label_encoder.classes_, yticklabels=label_encoder.classes_)
plt.xlabel('Predicted Label')
plt.ylabel('True Label')
plt.title('Confusion Matrix')
plt.show()

# COMMAND ----------

# Visualize ROC Curve
fpr, tpr, thresholds = roc_curve(y_test, y_pred_proba)
roc_auc = auc(fpr, tpr)

plt.figure(figsize=(7, 5))
plt.plot(fpr, tpr, color='darkorange', lw=2, label=f'ROC curve (area = {roc_auc:.2f})')
plt.plot([0, 1], [0, 1], color='navy', lw=2, linestyle='--')
plt.xlim([0.0, 1.0])
plt.ylim([0.0, 1.05])
plt.xlabel('False Positive Rate')
plt.ylabel('True Positive Rate')
plt.title('Receiver Operating Characteristic (ROC) Curve')
plt.legend(loc="lower right")
plt.show()

# COMMAND ----------

# DBTITLE 1,7. Final Predictions DataFrame (Optional)
# Combine test features, actual labels, and predictions
results_df = X_test.copy() # Start with scaled test features
results_df['Actual_Churn'] = label_encoder.inverse_transform(y_test) # Get original labels ('Yes'/'No')
results_df['Predicted_Churn'] = label_encoder.inverse_transform(y_pred)
results_df['Predicted_Churn_Probability'] = y_pred_proba

# Display results
# Note: Numerical features are scaled in this view. You might want to join back to the original unscaled data if needed for reporting.
results_df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC # Resumo
# MAGIC - Criou um conjunto de dados de exemplo usando Pandas.
# MAGIC - Realizou EDA e visualização com Matplotlib e Seaborn.
# MAGIC - Preparou os dados usando Pandas (one-hot encoding) e Scikit-learn (normalização, divisão treino/teste).
# MAGIC - Treinou um modelo de Regressão Logística usando Scikit-learn.
# MAGIC - Avaliou o modelo utilizando métricas de classificação padrão e visualizações.
# MAGIC
# MAGIC Este notebook fornece um fluxo de trabalho completo usando bibliotecas padrão do Python, adequado para conjuntos de dados menores ou exploração inicial no Databricks. Para conjuntos de dados maiores, o PySpark seria mais apropriado.