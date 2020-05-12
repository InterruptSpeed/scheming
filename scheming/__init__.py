import json
from pyspark.sql.types import StructType
import safer
import os


def get_filename(name, path="./"):
    return os.path.join(path, f"{name}.schema.json")


def load(schema_filename):
    if not os.path.exists(schema_filename):
        return None, True

    with open(schema_filename, "r") as f:
        schema_json = "".join(f.readlines())

    return StructType.fromJson(json.loads(schema_json)), False


def save(schema_filename, schema, path="./"):
    with safer.open(schema_filename, "w") as f:
        f.write(json.dumps(schema.jsonValue(), indent=4))


def metadata(schema):
    str_list = ["root"]
    str_list.append("\n".join([f" |-- {x.name}: {x.metadata}" for x in schema.fields]))

    return "\n".join(str_list)
