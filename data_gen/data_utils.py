from pyspark.sql.types import StructType, StringType

'''
dict 结构的 event/user schema 转成 spark 的
'''
def convert_eu_schema_to_spark_schema(schema):
    spark_schema = StructType()
    for name, value in schema.items():
        if name == "identities" or name == "properties":
            inner_schema = StructType()
            for inner_name, inner_data_type in value.items():
                inner_schema.add(inner_name, inner_data_type.get_spark_schema_type())
            spark_schema.add(name, inner_schema)
        else:
            spark_schema.add(name, value.get_spark_schema_type())
    return spark_schema


def convert_eu_schema_to_spark_csv_schema(schema):
    spark_schema = StructType()
    for name, value in schema.items():
        if name == "identities" or name == "properties":
            spark_schema.add(name, StringType())
        else:
            spark_schema.add(name, value.get_spark_schema_type())
    return spark_schema