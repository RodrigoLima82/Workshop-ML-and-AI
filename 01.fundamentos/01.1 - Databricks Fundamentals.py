# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTMyIiBoZWlnaHQ9IjIyIiB2aWV3Qm94PSIwIDAgMTMyIDIyIiBmaWxsPSJub25lIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciPjxwYXRoIGQ9Ik0xOC4zMTc2IDkuMjc0NzlMOS42ODY2MyAxNC4xMzM5TDAuNDQ0NTExIDguOTQyMjNMMCA5LjE4MjQxVjEyLjk1MTVMOS42ODY2MyAxOC4zODMzTDE4LjMxNzYgMTMuNTQyN1YxNS41MzgxTDkuNjg2NjMgMjAuMzk3MkwwLjQ0NDUxMSAxNS4yMDU1TDAgMTUuNDQ1N1YxNi4wOTIzTDkuNjg2NjMgMjEuNTI0MkwxOS4zNTQ3IDE2LjA5MjNWMTIuMzIzM0wxOC45MTAyIDEyLjA4MzFMOS42ODY2MyAxNy4yNTYzTDEuMDM3MTkgMTIuNDE1N1YxMC40MjAzTDkuNjg2NjMgMTUuMjYwOUwxOS4zNTQ3IDkuODI5MDZWNi4xMTU0NUwxOC44NzMyIDUuODM4MzFMOS42ODY2MyAxMC45OTNMMS40ODE3IDYuNDExMDZMOS42ODY2MyAxLjgxMDYxTDE2LjQyODQgNS41OTgxM0wxNy4wMjExIDUuMjY1NTdWNC44MDM2N0w5LjY4NjYzIDAuNjgzNTk0TDAgNi4xMTU0NVY2LjcwNjY3TDkuNjg2NjMgMTIuMTM4NUwxOC4zMTc2IDcuMjc5NDJWOS4yNzQ3OVoiIGZpbGw9IiNFRTNEMkMiPjwvcGF0aD48cGF0aCBkPSJNMzcuNDQ5IDE4LjQ0MjdWMS44NTE1NkgzNC44OTMxVjguMDU5NEMzNC44OTMxIDguMTUxNzcgMzQuODM3NSA4LjIyNTY4IDM0Ljc0NDkgOC4yNjI2M0MzNC42NTIzIDguMjk5NTggMzQuNTU5NyA4LjI2MjYzIDM0LjUwNDEgOC4yMDcyQzMzLjYzMzYgNy4xOTEwNCAzMi4yODE2IDYuNjE4MjkgMzAuNzk5OSA2LjYxODI5QzI3LjYzMjcgNi42MTgyOSAyNS4xNTA5IDkuMjc4NzkgMjUuMTUwOSAxMi42NzgzQzI1LjE1MDkgMTQuMzQxMSAyNS43MjUgMTUuODc0NiAyNi43ODA4IDE3LjAwMTZDMjcuODM2NSAxOC4xMjg3IDI5LjI2MjYgMTguNzM4MyAzMC43OTk5IDE4LjczODNDMzIuMjYzMSAxOC43MzgzIDMzLjYxNTEgMTguMTI4NyAzNC41MDQxIDE3LjA3NTVDMzQuNTU5NyAxNy4wMDE2IDM0LjY3MDggMTYuOTgzMiAzNC43NDQ5IDE3LjAwMTZDMzQuODM3NSAxNy4wMzg2IDM0Ljg5MzEgMTcuMTEyNSAzNC44OTMxIDE3LjIwNDlWMTguNDQyN0gzNy40NDlaTTMxLjM1NTUgMTYuNDI4OUMyOS4zMTgyIDE2LjQyODkgMjcuNzI1MyAxNC43ODQ1IDI3LjcyNTMgMTIuNjc4M0MyNy43MjUzIDEwLjU3MjEgMjkuMzE4MiA4LjkyNzc1IDMxLjM1NTUgOC45Mjc3NUMzMy4zOTI4IDguOTI3NzUgMzQuOTg1NyAxMC41NzIxIDM0Ljk4NTcgMTIuNjc4M0MzNC45ODU3IDE0Ljc4NDUgMzMuMzkyOCAxNi40Mjg5IDMxLjM1NTUgMTYuNDI4OVoiIGZpbGw9ImJsYWNrIj48L3BhdGg+PHBhdGggZD0iTTUxLjExOCAxOC40NDM1VjYuODk2Mkg0OC41ODA2VjguMDYwMTdDNDguNTgwNiA4LjE1MjU0IDQ4LjUyNSA4LjIyNjQ1IDQ4LjQzMjQgOC4yNjM0QzQ4LjMzOTggOC4zMDAzNSA0OC4yNDcyIDguMjYzNCA0OC4xOTE2IDguMTg5NUM0Ny4zMzk3IDcuMTczMzMgNDYuMDA2MSA2LjYwMDU5IDQ0LjQ4NzQgNi42MDA1OUM0MS4zMjAyIDYuNjAwNTkgMzguODM4NCA5LjI2MTA5IDM4LjgzODQgMTIuNjYwNkMzOC44Mzg0IDE2LjA2MDEgNDEuMzIwMiAxOC43MjA2IDQ0LjQ4NzQgMTguNzIwNkM0NS45NTA2IDE4LjcyMDYgNDcuMzAyNiAxOC4xMTA5IDQ4LjE5MTYgMTcuMDM5NEM0OC4yNDcyIDE2Ljk2NTUgNDguMzU4MyAxNi45NDcgNDguNDMyNCAxNi45NjU1QzQ4LjUyNSAxNy4wMDI0IDQ4LjU4MDYgMTcuMDc2MyA0OC41ODA2IDE3LjE2ODdWMTguNDI1SDUxLjExOFYxOC40NDM1Wk00NS4wNjE1IDE2LjQyOTdDNDMuMDI0MiAxNi40Mjk3IDQxLjQzMTQgMTQuNzg1MyA0MS40MzE0IDEyLjY3OTFDNDEuNDMxNCAxMC41NzI5IDQzLjAyNDIgOC45Mjg1MiA0NS4wNjE1IDguOTI4NTJDNDcuMDk4OSA4LjkyODUyIDQ4LjY5MTcgMTAuNTcyOSA0OC42OTE3IDEyLjY3OTFDNDguNjkxNyAxNC43ODUzIDQ3LjA5ODkgMTYuNDI5NyA0NS4wNjE1IDE2LjQyOTdaIiBmaWxsPSJibGFjayI+PC9wYXRoPjxwYXRoIGQ9Ik03Mi44NDI2IDE4LjQ0MzVWNi44OTYySDcwLjMwNTJWOC4wNjAxN0M3MC4zMDUyIDguMTUyNTQgNzAuMjQ5NiA4LjIyNjQ1IDcwLjE1NyA4LjI2MzRDNzAuMDY0NCA4LjMwMDM1IDY5Ljk3MTggOC4yNjM0IDY5LjkxNjIgOC4xODk1QzY5LjA2NDMgNy4xNzMzMyA2Ny43MzA3IDYuNjAwNTkgNjYuMjEyIDYuNjAwNTlDNjMuMDI2MyA2LjYwMDU5IDYwLjU2MyA5LjI2MTA5IDYwLjU2MyAxMi42NzkxQzYwLjU2MyAxNi4wOTcxIDYzLjA0NDggMTguNzM5MSA2Ni4yMTIgMTguNzM5MUM2Ny42NzUyIDE4LjczOTEgNjkuMDI3MiAxOC4xMjk0IDY5LjkxNjIgMTcuMDU3OEM2OS45NzE4IDE2Ljk4MzkgNzAuMDgyOSAxNi45NjU1IDcwLjE1NyAxNi45ODM5QzcwLjI0OTYgMTcuMDIwOSA3MC4zMDUyIDE3LjA5NDggNzAuMzA1MiAxNy4xODcyVjE4LjQ0MzVINzIuODQyNlpNNjYuNzg2MSAxNi40Mjk3QzY0Ljc0ODggMTYuNDI5NyA2My4xNTYgMTQuNzg1MyA2My4xNTYgMTIuNjc5MUM2My4xNTYgMTAuNTcyOSA2NC43NDg4IDguOTI4NTIgNjYuNzg2MSA4LjkyODUyQzY4LjgyMzUgOC45Mjg1MiA3MC40MTYzIDEwLjU3MjkgNzAuNDE2MyAxMi42NzkxQzcwLjQxNjMgMTQuNzg1MyA2OC44MjM1IDE2LjQyOTcgNjYuNzg2MSAxNi40Mjk3WiIgZmlsbD0iYmxhY2siPjwvcGF0aD48cGF0aCBkPSJNNzcuNDkyMiAxNy4wNzU1Qzc3LjUxMDcgMTcuMDc1NSA3Ny41NDc4IDE3LjA1NzEgNzcuNTY2MyAxNy4wNTcxQzc3LjYyMTggMTcuMDU3MSA3Ny42OTU5IDE3LjA5NCA3Ny43MzMgMTcuMTMxQzc4LjYwMzUgMTguMTQ3MSA3OS45NTU1IDE4LjcxOTkgODEuNDM3MiAxOC43MTk5Qzg0LjYwNDQgMTguNzE5OSA4Ny4wODYyIDE2LjA1OTQgODcuMDg2MiAxMi42NTk4Qzg3LjA4NjIgMTAuOTk3IDg2LjUxMjEgOS40NjM1NSA4NS40NTY0IDguMzM2NTNDODQuNDAwNiA3LjIwOTUxIDgyLjk3NDUgNi41OTk4MiA4MS40MzcyIDYuNTk5ODJDNzkuOTc0MSA2LjU5OTgyIDc4LjYyMiA3LjIwOTUxIDc3LjczMyA4LjI2MjYzQzc3LjY3NzQgOC4zMzY1MyA3Ny41ODQ4IDguMzU1MDEgNzcuNDkyMiA4LjMzNjUzQzc3LjM5OTYgOC4yOTk1OCA3Ny4zNDQgOC4yMjU2OCA3Ny4zNDQgOC4xMzMzVjEuODUxNTZINzQuNzg4MVYxOC40NDI3SDc3LjM0NFYxNy4yNzg4Qzc3LjM0NCAxNy4xODY0IDc3LjM5OTYgMTcuMTEyNSA3Ny40OTIyIDE3LjA3NTVaTTc3LjIzMjkgMTIuNjc4M0M3Ny4yMzI5IDEwLjU3MjEgNzguODI1NyA4LjkyNzc1IDgwLjg2MzEgOC45Mjc3NUM4Mi45MDA0IDguOTI3NzUgODQuNDkzMiAxMC41NzIxIDg0LjQ5MzIgMTIuNjc4M0M4NC40OTMyIDE0Ljc4NDUgODIuOTAwNCAxNi40Mjg5IDgwLjg2MzEgMTYuNDI4OUM3OC44MjU3IDE2LjQyODkgNzcuMjMyOSAxNC43NjYxIDc3LjIzMjkgMTIuNjc4M1oiIGZpbGw9ImJsYWNrIj48L3BhdGg+PHBhdGggZD0iTTk0LjQ3NjYgOS4yNjMyOEM5NC43MTczIDkuMjYzMjggOTQuOTM5NiA5LjI4MTc1IDk1LjA4NzggOS4zMTg3VjYuNjk1MTVDOTQuOTk1MSA2LjY3NjY4IDk0LjgyODUgNi42NTgyIDk0LjY2MTggNi42NTgyQzkzLjMyODIgNi42NTgyIDkyLjEwNTggNy4zNDE4IDkxLjQ1NzYgOC40MzE4N0M5MS40MDIgOC41MjQyNSA5MS4zMDk0IDguNTYxMiA5MS4yMTY4IDguNTI0MjVDOTEuMTI0MiA4LjUwNTc3IDkxLjA1MDEgOC40MTMzOSA5MS4wNTAxIDguMzIxMDJWNi44OTgzOUg4OC41MTI3VjE4LjQ2NDJIOTEuMDY4NlYxMy4zNjQ5QzkxLjA2ODYgMTAuODMzNyA5Mi4zNjUxIDkuMjYzMjggOTQuNDc2NiA5LjI2MzI4WiIgZmlsbD0iYmxhY2siPjwvcGF0aD48cGF0aCBkPSJNOTkuMjkxNyA2Ljg5NzQ2SDk2LjY5ODdWMTguNDYzMkg5OS4yOTE3VjYuODk3NDZaIiBmaWxsPSJibGFjayI+PC9wYXRoPjxwYXRoIGQ9Ik05Ny45NTc2IDEuODcwMTJDOTcuMDg3MSAxLjg3MDEyIDk2LjM4MzMgMi41NzIxOSA5Ni4zODMzIDMuNDQwNTVDOTYuMzgzMyA0LjMwODkxIDk3LjA4NzEgNS4wMTA5OSA5Ny45NTc2IDUuMDEwOTlDOTguODI4MSA1LjAxMDk5IDk5LjUzMTkgNC4zMDg5MSA5OS41MzE5IDMuNDQwNTVDOTkuNTMxOSAyLjU3MjE5IDk4LjgyODEgMS44NzAxMiA5Ny45NTc2IDEuODcwMTJaIiBmaWxsPSJibGFjayI+PC9wYXRoPjxwYXRoIGQ9Ik0xMDYuODg2IDYuNjAwNTlDMTAzLjMzIDYuNjAwNTkgMTAwLjc1NSA5LjE1MDIzIDEwMC43NTUgMTIuNjc5MUMxMDAuNzU1IDE0LjM5NzMgMTAxLjM2NyAxNS45MzA4IDEwMi40NTkgMTcuMDM5NEMxMDMuNTcxIDE4LjE0NzkgMTA1LjEyNiAxOC43NTc2IDEwNi44NjcgMTguNzU3NkMxMDguMzEyIDE4Ljc1NzYgMTA5LjQyMyAxOC40ODA1IDExMS41MzUgMTYuOTI4NUwxMTAuMDcyIDE1LjM5NUMxMDkuMDM0IDE2LjA3ODYgMTA4LjA3MSAxNi40MTEyIDEwNy4xMjcgMTYuNDExMkMxMDQuOTc4IDE2LjQxMTIgMTAzLjM2NyAxNC44MDM4IDEwMy4zNjcgMTIuNjc5MUMxMDMuMzY3IDEwLjU1NDQgMTA0Ljk3OCA4Ljk0NyAxMDcuMTI3IDguOTQ3QzEwOC4xNDUgOC45NDcgMTA5LjA5IDkuMjc5NTYgMTEwLjAzNSA5Ljk2MzE2TDExMS42NjQgOC40Mjk2OEMxMDkuNzU3IDYuODAzODIgMTA4LjAzNCA2LjYwMDU5IDEwNi44ODYgNi42MDA1OVoiIGZpbGw9ImJsYWNrIj48L3BhdGg+PHBhdGggZD0iTTExNi4wMzUgMTMuMzYyQzExNi4wNzIgMTMuMzI1IDExNi4xMjggMTMuMzA2NiAxMTYuMTg0IDEzLjMwNjZIMTE2LjIwMkMxMTYuMjU4IDEzLjMwNjYgMTE2LjMxMyAxMy4zNDM1IDExNi4zNjkgMTMuMzgwNUwxMjAuNDYyIDE4LjQ0MjhIMTIzLjYxMUwxMTguMzE0IDEyLjA1MDJDMTE4LjIzOSAxMS45NTc4IDExOC4yMzkgMTEuODI4NSAxMTguMzMyIDExLjc1NDZMMTIzLjIwMyA2Ljg5NTUxSDEyMC4wNzNMMTE1Ljg2OSAxMS4xMDhDMTE1LjgxMyAxMS4xNjM0IDExNS43MjEgMTEuMTgxOSAxMTUuNjI4IDExLjE2MzRDMTE1LjU1NCAxMS4xMjY0IDExNS40OTggMTEuMDUyNSAxMTUuNDk4IDEwLjk2MDJWMS44NzAxMkgxMTIuOTI0VjE4LjQ2MTNIMTE1LjQ4VjEzLjk1MzJDMTE1LjQ4IDEzLjg5NzggMTE1LjQ5OCAxMy44MjM5IDExNS41NTQgMTMuNzg2OUwxMTYuMDM1IDEzLjM2MloiIGZpbGw9ImJsYWNrIj48L3BhdGg+PHBhdGggZD0iTTEyNy43NzYgMTguNzM5QzEyOS44NjkgMTguNzM5IDEzMS45OTkgMTcuNDY0MiAxMzEuOTk5IDE1LjA0MzlDMTMxLjk5OSAxMy40NTUgMTMwLjk5OSAxMi4zNjQ5IDEyOC45NjIgMTEuNjk5OEwxMjcuNTcyIDExLjIzNzlDMTI2LjYyOCAxMC45MjM4IDEyNi4xODMgMTAuNDgwNCAxMjYuMTgzIDkuODcwN0MxMjYuMTgzIDkuMTY4NjMgMTI2LjgxMyA4LjY4ODI2IDEyNy43MDIgOC42ODgyNkMxMjguNTU0IDguNjg4MjYgMTI5LjMxMyA5LjI0MjUzIDEyOS43OTUgMTAuMjAzM0wxMzEuODUxIDkuMDk0NzNDMTMxLjA5MiA3LjU0Mjc3IDEyOS41MTcgNi41ODIwMyAxMjcuNzAyIDYuNTgyMDNDMTI1LjQwNSA2LjU4MjAzIDEyMy43MzkgOC4wNjAwOSAxMjMuNzM5IDEwLjA3MzlDMTIzLjczOSAxMS42ODEzIDEyNC43MDIgMTIuNzUyOSAxMjYuNjgzIDEzLjM4MTFMMTI4LjExIDEzLjg0M0MxMjkuMTEgMTQuMTU3MSAxMjkuNTM2IDE0LjU2MzUgMTI5LjUzNiAxNS4yMTAyQzEyOS41MzYgMTYuMTg5NCAxMjguNjI4IDE2LjU0MDQgMTI3Ljg1IDE2LjU0MDRDMTI2LjgxMyAxNi41NDA0IDEyNS44ODcgMTUuODc1MyAxMjUuNDQzIDE0Ljc4NTJMMTIzLjM1IDE1Ljg5MzhDMTI0LjAzNSAxNy42NDkgMTI1LjcyIDE4LjczOSAxMjcuNzc2IDE4LjczOVoiIGZpbGw9ImJsYWNrIj48L3BhdGg+PHBhdGggZD0iTTU4LjIzMDQgMTguNjI4QzU5LjA0NTMgMTguNjI4IDU5Ljc2NzcgMTguNTU0MSA2MC4xNzUxIDE4LjQ5ODZWMTYuMjgxNUM1OS44NDE4IDE2LjMxODUgNTkuMjQ5MSAxNi4zNTU0IDU4Ljg5NzIgMTYuMzU1NEM1Ny44NiAxNi4zNTU0IDU3LjA2MzYgMTYuMTcwNyA1Ny4wNjM2IDEzLjkzNTFWOS4xODY4N0M1Ny4wNjM2IDkuMDU3NTQgNTcuMTU2MiA4Ljk2NTE3IDU3LjI4NTggOC45NjUxN0g1OS43ODYyVjYuODc3NDFINTcuMjg1OEM1Ny4xNTYyIDYuODc3NDEgNTcuMDYzNiA2Ljc4NTAzIDU3LjA2MzYgNi42NTU3VjMuMzMwMDhINTQuNTA3NlY2LjY3NDE4QzU0LjUwNzYgNi44MDM1MSA1NC40MTUgNi44OTU4OSA1NC4yODU0IDYuODk1ODlINTIuNTA3M1Y4Ljk4MzY0SDU0LjI4NTRDNTQuNDE1IDguOTgzNjQgNTQuNTA3NiA5LjA3NjAyIDU0LjUwNzYgOS4yMDUzNVYxNC41ODE4QzU0LjUwNzYgMTguNjI4IDU3LjIxMTcgMTguNjI4IDU4LjIzMDQgMTguNjI4WiIgZmlsbD0iYmxhY2siPjwvcGF0aD48L3N2Zz4=" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="fd2d84ac-6a17-44c2-bb92-18b0c7fef797"/>
# MAGIC
# MAGIC
# MAGIC
# MAGIC # Databricks Fundamentals
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 📌 Requisitos
# MAGIC
# MAGIC **Versão de tempo de execução do Databricks necessária:**
# MAGIC * Observe que para executar este notebook, você deve usar um dos seguintes Databricks
# MAGIC Runtime(s): **15.4.x-scala2.12**

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Trabalhando no Databricks
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Células do Notebook
# MAGIC Células são um pequeno segmento de código que fornece desenvolvimento simplificado e depuração fácil
# MAGIC Você pode clicar no botão redondo `(+)` entre células existentes para adicionar uma nova célula, arrastar células existentes, recolher/excluir/ocultar/etc. células conforme necessário.

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ### Magic Commands
# MAGIC Each cell can be a different supported language! This allows you to switch languages depending on the task at hand.

# COMMAND ----------

print("Essa é uma célula python.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "Essa é uma célula SQL." FROM (VALUES (1))

# COMMAND ----------

# MAGIC %md
# MAGIC Se você não usar um comando Magic, ele executará o idioma padrão do notebook, visível no topo da tela.
# MAGIC
# MAGIC Outros comandos magic importantes:
# MAGIC * `%fs`: comandos do sistema de arquivos
# MAGIC * `%sh`: comandos shell, por exemplo, `%sh ls`
# MAGIC * `%r`, `%scala`, `%sql`, `%python`: linguagens suportadas no notebook
# MAGIC * `%run`: para executar outro notebook

# COMMAND ----------

dbutils.widgets.removeAll()

dbutils.widgets.text("catalogo", "catalogo_treinamento", "Catalogo")
dbutils.widgets.text("database", "default", "Database")

catalogo = dbutils.widgets.get("catalogo")
database = dbutils.widgets.get("database")

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalogo}.{database}")

# COMMAND ----------

spark.sql(f"use {catalogo}.{database}")
print (f"O catálogo que estarei utilizando nesse exercício é: {catalogo} e o database é: {database}")

# COMMAND ----------

# MAGIC %md <i18n value="8ce92b68-6e6c-4fd0-8d3c-a57f27e5bdd9"/>
# MAGIC
# MAGIC
# MAGIC
# MAGIC ###Fazendo donwload da nossa base de clientes
# MAGIC Primeiro faremos o download de uma base de clientes fake, mas para isso, criaremos um Volume, que é um asset de dados do Unity Catalog, que você consegue salvar dados que não está em um formato tabular, podendo ser arquivos de imagem, vídeos, json, csv, e qualquer outro que você precisar salvar. E da mesma forma que você da permissões para grupos e usuários para uma tabela, você também pode com os volumes.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Criação do Volume
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalogo}.{database}.files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## DBUtils
# MAGIC Além das linguagens de programação, você pode usar um conjunto de funções "utilitárias" úteis para tarefas comuns. Esses utilitários são conhecidos como `dbutils` e podem ser acessados ​​em notebooks via `dbutils.`

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Criando uma pasta chamada landing dentro do volume criado
volume_folder=f"/Volumes/{catalogo}/{database}/files"

clientes_folder = f"{volume_folder}/clientes"

# Crie a nova pasta dentro do volume
dbutils.fs.mkdirs(clientes_folder)

# Verifique se a pasta foi criada
display(dbutils.fs.ls(volume_folder))

# COMMAND ----------

# MAGIC %md
# MAGIC Acesse seu catálogo e schema através do menu lateral `Catalog` para verificar também pela interface.

# COMMAND ----------

#Ver todos os módulos dbutils
dbutils.help()

# COMMAND ----------

# DBTITLE 1,Método para baixar arquivos
import requests 

def download_file(url, destination):
    local_filename = url.split('/')[-1]
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        print('saving '+destination+'/'+local_filename)
        with open(destination+'/'+local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                f.write(chunk)
    return local_filename

# COMMAND ----------

# DBTITLE 1,Realizando o download e verificando o volume se o arquivo foi baixado
url="https://github.com/RodrigoLima82/Workshop_ML_LLM/blob/main/dados_clientes/dados_clientes_raw.csv"

download_file(url, volume_folder + "/clientes")

# COMMAND ----------


display(dbutils.fs.ls(volume_folder + "/clientes"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carregando dados
# MAGIC No Pyspark, podemos carregar dados usando os métodos "Reader" do Spark DataFrame. Para o restante do notebook, usaremos um dos Datasets do Databricks que vem carregado com cada workspace.
# MAGIC
# MAGIC [pyspark.sql.DataFrameReader](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameReader)
# MAGIC
# MAGIC Métodos comuns do reader:
# MAGIC * `csv`, `text`, `delta`
# MAGIC * Exemplo: `spark.read.format('json').load('<path>')`
# MAGIC
# MAGIC Para lermos os dados, vamos começar a testar o **Databricks Assistant**, que é o assistente da Databricks inteligênte que te ajuda no desenvolvimento de seus códigos tanto em notebooks quanto em queries sql, e também te ajuda a corrigir erros. Vamos ver se ele acerta o código para carregar os dados.
# MAGIC
# MAGIC Clique no botão do assistant na célula ou clique em *ctrl + i* e cole o seguinte texto: `Como eu posso ler os dados baixados na celula anterior`

# COMMAND ----------

# DBTITLE 1,Preencha aqui:


# COMMAND ----------

# DBTITLE 1,Resposta
# Resposta
# Caminho do arquivo baixado
file_path = volume_folder + "/clientes"

# Leitura do arquivo CSV em um DataFrame Spark
df_clientes = spark.read.csv(file_path, header=True, inferSchema=True)

# Exibir o DataFrame
display(df_clientes)

# COMMAND ----------

# MAGIC %md <i18n value="8ce92b68-6e6c-4fd0-8d3c-a57f27e5bdd9"/>
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Criando uma tabela Delta
# MAGIC
# MAGIC #### Por que Delta Lake?<br><br>
# MAGIC
# MAGIC <div style="img align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://user-images.githubusercontent.com/20408077/87175470-4d8e1580-c29e-11ea-8f33-0ee14348a2c1.png" width="500"/>
# MAGIC </div>
# MAGIC
# MAGIC À primeira vista, Delta Lake é uma camada de armazenamento de código aberto que traz **confiabilidade e desempenho** para data lakes. Delta Lake fornece transações ACID, manipulação escalonável de metadados e unifica streaming e processamento de dados em lote.
# MAGIC
# MAGIC Delta Lake é executado em seu data lake existente e é totalmente compatível com APIs Apache Spark. <a href="https://docs.databricks.com/delta/delta-intro.html" target="_blank">Para mais informações acessse esse link.</a>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salvar os dados CSV em uma tabela Delta
# MAGIC Agora vamos testar o **Auto Complete** comece digitando `df_clientes.wri` e veja a sugestão que ele te dá, caso concorde com sua sugestão, clique em tab para completar com a sugestão apresentada. Deixe o nome da tabela como `tb_clientes_bronze`

# COMMAND ----------

# DBTITLE 1,Preencha aqui:


# COMMAND ----------

# DBTITLE 1,Resposta
# resposta
df_clientes.write.mode("overwrite").saveAsTable("tb_clientes_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC Observe o uso de `mode("overwrite")`. Isso "sobrescreverá" a tabela existente naquele local. Modos de gravação suportados:
# MAGIC
# MAGIC * `append`: Acrescenta o conteúdo deste DataFrame aos dados existentes.
# MAGIC
# MAGIC * `overwrite`: Sobrescreve os dados existentes.
# MAGIC
# MAGIC * `error` ou `errorifexists`: Lança uma exceção se os dados já existirem.
# MAGIC
# MAGIC * `ignore`: Ignora silenciosamente esta operação se os dados já existirem.