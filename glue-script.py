try:
    import os
    import sys
    import uuid

    import pyspark
    from pyspark import SparkConf, SparkContext
    from awsglue.job import Job
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, asc, desc
    from awsglue.utils import getResolvedOptions
    from awsglue.dynamicframe import DynamicFrame
    from awsglue.context import GlueContext
    from pyspark.sql.functions import *

    from faker import Faker
    from delta.tables import *

    print("All modules are loaded .....")

except Exception as e:
    print("Some modules are missing {} ".format(e))





base_s3_path = "s3a://glue-learn-begineers"
table_name = "delta_table"

final_base_path = "{base_s3_path}/deltalake/{table_name}".format(
    base_s3_path=base_s3_path, table_name=table_name
)

global faker
faker = Faker()


class DataGenerator(object):

    @staticmethod
    def get_data():
        return [
            (
                x,
                faker.name(),
                faker.random_element(elements=('IT', 'HR', 'Sales', 'Marketing')),
                faker.random_element(elements=('CA', 'NY', 'TX', 'FL', 'IL', 'RJ')),
                faker.random_int(min=10000, max=150000),
                faker.random_int(min=18, max=60),
                faker.random_int(min=0, max=100000),
                faker.unix_time()
            ) for x in range(10)
        ]


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    return spark


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
spark = create_spark_session()
sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)




# ====================================================
"""Create Spark Data Frame """
# ====================================================
data = DataGenerator.get_data()
columns = ["emp_id", "employee_name", "department", "state", "salary", "age", "bonus", "ts"]
df_write = spark.createDataFrame(data=data, schema=columns)
df_write.write.format("delta").mode("overwrite").save(final_base_path)


# ====================================================
"""READ FROM DELTA LAKE  """
# ====================================================
df_read = spark.read.format("delta").load(final_base_path)
print("READ", df_read.show())

# ====================================================
"""UPDATE DELTA"""
# ====================================================


delta_df = DeltaTable.forPath(spark, final_base_path)

print("READ OK... ")
try:
    delta_df.update("emp_id = '3'", { "employee_name": "'THIS WAS UPDATE ON DELTA LAKE'" } )   # predicate using SQL formatted string
except Exception as e:
    print("ERROR ** ",e)

try:
    delta_df.update(col("emp_id") == "4", { "employee_name": lit("THIS WAS UPDATE ON DELTA LAKE EMP 4") } )   # predicate using Spark SQL functions
except Exception as e:
    print("ERROR ** ",e)

# ====================================================
""" DELETE DELTA"""
# ====================================================
try:
    delta_df.delete("emp_id = '7'")
except Exception as e:
    print("ERROR ** ",e)


delta_df.generate("symlink_format_manifest")


job.commit()
