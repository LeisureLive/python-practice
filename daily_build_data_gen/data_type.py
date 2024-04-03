from enum import Enum
from pyspark.sql.types import StringType, IntegerType, FloatType, BooleanType, ArrayType, StructType, LongType


class DataType(Enum):
    STRING = 1,
    NUMBER = 2,
    BOOL = 3,
    DATETIME = 4,
    LIST = 5,
    NUMBER_WITH_DOUBLE = 6

    def get_spark_schema_type(self):
        data_type = self
        if data_type == DataType.STRING:
            return StringType()
        if data_type == DataType.NUMBER:
            return LongType()
        if data_type == DataType.NUMBER_WITH_DOUBLE:
            return FloatType()
        if data_type == DataType.BOOL:
            return BooleanType()
        if data_type == DataType.DATETIME:
            return StringType()
        if data_type == DataType.LIST:
            return ArrayType(StringType())
        raise Exception("data_type invalid. type=" + str(data_type))
