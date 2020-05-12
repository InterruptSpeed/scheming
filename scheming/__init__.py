# -*- coding: utf-8 -*-

"""
scheming
~~~~~~~~~~~~
This module implements scheming.
:copyright: (c) 2020 by Steven Docherty.
:license: Apache2, see LICENSE for more details.
"""

import json
from pyspark.sql.types import StructType
import safer
import os


def get_filename(name: str, path: str = "./") -> str:
    return os.path.join(path, f"{name}.schema.json")


def load(schema_filename: str) -> (StructType, bool):
    if not os.path.exists(schema_filename):
        return None, True

    with open(schema_filename, "r") as f:
        schema_json = "".join(f.readlines())

    return StructType.fromJson(json.loads(schema_json)), False


def save(schema_filename: str, schema: StructType, path: str = "./"):
    with safer.open(schema_filename, "w") as f:
        f.write(json.dumps(schema.jsonValue(), indent=4))


def metadata(schema: StructType) -> str:
    r"""Returns the metadata of the schema in a format like printSchema.
    :param schema: :class:`StructType` schema to be evaluated.
    :rtype: str
    """
    str_list = ["root"]
    str_list.append("\n".join([f" |-- {x.name}: {x.metadata}" for x in schema.fields]))

    return "\n".join(str_list)
