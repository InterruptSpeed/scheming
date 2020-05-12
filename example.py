from pyspark.sql import SparkSession

import scheming

spark = (
    SparkSession.builder.appName("scheming example")
    .getOrCreate()
)

data_name = "example"
schema_filename = scheming.get_filename(data_name)
csv_filename = f"{data_name}.csv"

schema, inferSchema = scheming.load(schema_filename)

df = spark.read.csv(csv_filename, header=True, schema=schema, inferSchema=inferSchema)
if inferSchema:
    scheming.save(schema_filename, df.schema)
    print(f"WARN: inferSchema=True on {csv_filename}")
    
df.show()
df.printSchema()

print(scheming.metadata(df.schema))