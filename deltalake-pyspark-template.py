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

    from delta.tables import *
    from faker import Faker
    from delta.tables import DeltaTable

    print("All modules are loaded .....")

except Exception as e:
    print("Some modules are missing {} ".format(e))

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


class DeltaLakeHelper(object):
    def __init__(self, delta_lake_path):
        self.spark = self.__create_spark_session()
        self.delta_lake_path = delta_lake_path
        self.delta_df = None

    def generate_manifest_files(self):
        self.delta_df.generate("symlink_format_manifest")
        return True

    def __generate_delta_df(self):
        try:
            if self.delta_df is None:
                self.delta_df = DeltaTable.forPath(self.spark, self.delta_lake_path)
        except Exception as e:
            pass

    def compact_table(self, num_of_files=4):
        df_read = self.spark.read.format("delta") \
            .load(self.delta_lake_path) \
            .repartition(num_of_files) \
            .write.option("dataChange", "false") \
            .format("delta") \
            .mode("overwrite") \
            .save(self.delta_lake_path)
        return True

    def delete_older_files_versions(self):
        self.__generate_delta_df()
        try:
            print("start vacuum ")
            self.delta_df.vacuum(0)
        except Exception as e:
            print("****ERROR***** ",e)

    def insert_records_delta_lake(self, spark_df, method='overwrite', maxRecordsPerFile='10000'):
        """
        Read More https://docs.databricks.com/delta/tune-file-size.html
        Given Spark DF insert into Delta Lakes
        :param spark_df:
        :return: Bool
        """
        spark_df.write.format("delta") \
            .mode(method) \
            .option("maxRecordsPerFile", maxRecordsPerFile) \
            .save(self.delta_lake_path)

        return True

    def append_records_delta_lake(self, spark_df, maxRecordsPerFile="10000"):
        """
        Given Spark DF insert into Delta Lakes
        :param spark_df:
        :return: Bool
        """
        spark_df.write.format("delta") \
            .mode('append') \
            .option("maxRecordsPerFile", maxRecordsPerFile) \
            .save(self.delta_lake_path)
        return True

    def update_records_delta_lake(self, condition="", value_to_set={}):
        """
        Set the value on delta lake
        :param condition:Str IE "emp_id = '3'",
        :param value_to_set: Dict IE  {"employee_name": "'THIS WAS UPDATE ON DELTA LAKE'"}
        :return:
        """
        self.__generate_delta_df()
        self.delta_df.update(condition, value_to_set)
        return True

    def upsert_records_delta_lake(self, old_data_key, new_data_key , new_spark_df):
        """
        Set the value on delta lake
        :param condition:Str IE "emp_id = '3'",
        :param value_to_set: Dict IE  {"employee_name": "'THIS WAS UPDATE ON DELTA LAKE'"}
        :return:
        """

        self.__generate_delta_df()
        dfUpdates = new_spark_df

        self.delta_df.alias('oldData') \
            .merge(dfUpdates.alias('newData'), f'oldData.{old_data_key} = newData.{new_data_key}') \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()

        return True

    def delete_records_delta_lake(self, condition=""):
        """
        Set the value on delta lake
        :param condition:Str IE "emp_id = '3'",
        :return:Bool
        """
        self.__generate_delta_df()
        self.delta_df.delete(condition)
        return True

    def read_delta_lake(self):
        """
        Reads from Delta lakes
        :return: Spark DF
        """
        df_read = self.spark.read.format("delta").load(self.delta_lake_path)
        return df_read

    def __create_spark_session(self):
        self.spark = SparkSession \
            .builder \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
        return self.spark


def run():
    helper = DeltaLakeHelper(delta_lake_path="s3a://glue-learn-begineers/deltalake/delta_table")

    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    spark = helper.spark
    sc = spark.sparkContext
    glueContext = GlueContext(sc)
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    # ====================================================
    """Create Spark Data Frame """
    # ====================================================
    # data = DataGenerator.get_data()
    # columns = ["emp_id", "employee_name", "department", "state", "salary", "age", "bonus", "ts"]
    # df_write = spark.createDataFrame(data=data, schema=columns)
    # helper.insert_records_delta_lake(spark_df=df_write)

    # ====================================================
    """READ FROM DELTA LAKE  """
    # ====================================================
    # df_read = helper.read_delta_lake()
    # print("READ", df_read.show())

    # ====================================================
    """UPDATE DELTA"""
    # ====================================================
    # helper.update_records_delta_lake(condition="emp_id = '3'",value_to_set={"employee_name": "'THIS WAS UPDATE ON DELTA LAKE'"})

    # ====================================================
    """ DELETE DELTA"""
    # ====================================================
    # helper.delete_records_delta_lake(condition="emp_id = '4'")


    impleDataUpd = [
        (3, "this is update on delta lake ", "Sales", "RJ", 81000, 30, 23000, 827307999),
        (11, "This should be append ", "Engineering", "RJ", 79000, 53, 15000, 1627694678),
    ]
    columns = ["emp_id", "employee_name", "department", "state", "salary", "age", "bonus", "ts"]
    usr_up_df = spark.createDataFrame(data=impleDataUpd, schema=columns)
    helper.upsert_records_delta_lake(old_data_key='emp_id',
                                     new_data_key='emp_id',
                                     new_spark_df=usr_up_df)


    # ====================================================
    """ Compaction DELTA"""
    # ====================================================
    helper.compact_table(num_of_files=2)
    helper.delete_older_files_versions()

    helper.generate_manifest_files()

    job.commit()


run()
