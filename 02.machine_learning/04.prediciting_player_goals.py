# Databricks notebook source
# MAGIC %md
# MAGIC # Fantasy Premier League: Prevendo Gols de Jogadores
# MAGIC
# MAGIC Este notebook carrega dados abertos de jogadores da FPL, realiza engenharia de atributos, constrói um modelo para prever se um jogador marcará um gol em uma partida e salva os resultados como uma tabela no seu Unity Catalog.

# COMMAND ----------

# MAGIC %pip install --upgrade threadpoolctl

# COMMAND ----------

# DBTITLE 1,1. Import libraries
import pandas as pd
import numpy as np
import requests
import io
import zipfile

# COMMAND ----------

# DBTITLE 1,2. Download the Fantasy Premier League dataset (latest season) from GitHub
# We'll use the 2024-25 season as an example
url = "https://github.com/vaastav/Fantasy-Premier-League/archive/refs/heads/master.zip"
r = requests.get(url)
z = zipfile.ZipFile(io.BytesIO(r.content))

# Extract the merged_gw.csv for 2023-24 season
with z.open("Fantasy-Premier-League-master/data/2024-25/gws/merged_gw.csv") as f:
    df = pd.read_csv(f, error_bad_lines=False)

# COMMAND ----------

# DBTITLE 1,3. Inspect the data
df.head()

# COMMAND ----------

# DBTITLE 1,4. Feature engineering
# We'll predict if a player scores in a match (target: goals_scored > 0)
df['scored'] = (df['goals_scored'] > 0).astype(int)

# We'll use recent form as features:
# - minutes played last GW
# - total points last GW
# - assists last GW
# - clean sheets last GW
# We'll group by player and sort by gameweek to get lag features

df = df.sort_values(['element', 'round'])

# Create lag features (previous gameweek stats)
for col in ['minutes', 'total_points', 'assists', 'clean_sheets']:
    df[f'{col}_prev'] = df.groupby('element')[col].shift(1)

# Drop rows where lag features are missing (first appearance of each player)
df_model = df.dropna(subset=['minutes_prev', 'total_points_prev', 'assists_prev', 'clean_sheets_prev'])

# COMMAND ----------

# DBTITLE 1,5. Prepare features and target
features = ['minutes_prev', 'total_points_prev', 'assists_prev', 'clean_sheets_prev']
X = df_model[features]
y = df_model['scored']

# COMMAND ----------

# DBTITLE 1,6. Train/test split
from sklearn.model_selection import train_test_split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# COMMAND ----------

# DBTITLE 1,7. Train a classifier
from sklearn.ensemble import RandomForestClassifier

clf = RandomForestClassifier(n_estimators=50, random_state=42)
clf.fit(X_train, y_train)
y_pred = clf.predict(X_test)

# COMMAND ----------

# DBTITLE 1,8. Evaluate the model
from sklearn.metrics import accuracy_score, precision_score, recall_score

accuracy = accuracy_score(y_test, y_pred)
precision = precision_score(y_test, y_pred)
recall = recall_score(y_test, y_pred)

print(f"Accuracy: {accuracy:.2f}")
print(f"Precision: {precision:.2f}")
print(f"Recall: {recall:.2f}")

# COMMAND ----------

# Before train/test split, reset index
df_model_reset = df_model.reset_index(drop=True)
X = df_model_reset[features]
y = df_model_reset['scored']

# Train/test split (indices now aligned)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# After prediction, select matching rows from df_model_reset using y_test's index
df_model_test = df_model_reset.loc[y_test.index].copy()
df_model_test['predicted_prob'] = clf.predict_proba(X_test)[:, 1]

final_table = df_model_test[['name', 'team', 'opponent_team', 'round', 'minutes', 'total_points', 'assists', 'clean_sheets', 'goals_scored', 'scored', 'predicted_prob']]
final_table = final_table.rename(columns={
    'name': 'player_name',
    'team': 'team_name',
    'opponent_team': 'opponent_team_id',
    'round': 'gameweek'
})


# COMMAND ----------

# DBTITLE 1,10. Save to Unity Catalog
spark_df = spark.createDataFrame(final_table)
spark_df.write.mode('overwrite').saveAsTable('workspace.default.fpl_goal_predictions')

# COMMAND ----------

display(spark_df)