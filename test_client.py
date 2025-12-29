from app.databricks_client import DatabricksClient

c = DatabricksClient()
print(c.classify("what is ibuprofen used for"))
print(c.retrieve("what is ibuprofen used for"))