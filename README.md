# scheming

Scheming is a pyspark schema helper. Load a saved JSON schema file for a DataFrame if one exists, otherwise infer schema and save the resulting schema. Also adds a way to see metadata in the schema.

## Usage

spark-submit example.py