from pyspark.sql import SparkSession
from pyspark.sql.functions import  col, lower, concat, concat_ws, when, expr
from pyspark.sql.types import StructType, IntegerType, StringType, StructField
from pyspark.sql.functions import input_file_name

spark = SparkSession.builder.appName("Functions and Expression.com").getOrCreate()

emp_schema = StructType([
    StructField('empno', IntegerType(), True),
    StructField('first_name', StringType(), True),
    StructField('job', StringType(), True),
    StructField('mgr', IntegerType(), True),
    StructField('hiredate', StringType(), True),
    StructField('sal', IntegerType(), True),
    StructField('comm', IntegerType(), True),
    StructField('deptno', IntegerType(), True),
    StructField('grade', IntegerType(), True),
])

emp_df = spark.read.format("csv").option("header", "true").schema(emp_schema) \
    .load(r"D:/Shyam/GCP/GCP1/Spark/EMPY.csv")

#emp_df.printSchema()
#emp_df.show()
emp_df.select(emp_df.empno, emp_df.first_name, emp_df.job).show()
emp_df.select(col("empno"), col("first_name"), col("job"), col("sal"), col("deptno")).show()

#emp_df = emp_df.withColumn("depn", col("deptno"))
#emp_df = emp_df.withColumn("YearComm", expr("comm * 12"))
#emp_df.show()
#emp_df.filter(emp_df.deptno == 10).show()
#emp_df.where(emp_df.deptno == 20).show()

#emp_df.filter(emp_df.deptno.isin(10, 20)).show()
#emp_df.filter((emp_df.deptno == 10) & (emp_df.sal > 500)).show()
#emp_df.filter(~emp_df.deptno.isin(10, 20)).show()
#emp_df.filter(emp_df.first_name.startswith("S")).show()
#emp_df.filter(emp_df.first_name.endswith("S")).show()
#emp_df.filter(emp_df.first_name.like("%S%")).show()
#emp_df.filter(emp_df.first_name.contains("S")).show()

#emp_df.withColumn("Grade", when(col("sal") < 2000, "low") \
#                  .when(col("sal").between(2000, 4000), "medium") \
#                 .otherwise("High")).show()
#emp_df.distinct().show()

##-------------------------------------------------------------------------------------------------------
# permissive, dropmalformed, failfast
# permissive

# emp_df = spark.read.format("csv").option("header", "true").option("mode", "permissive") \
#     .load("C:/Users/test_emp.txt")
# emp_df.printSchema()
# emp_df.show()

# dropmalformed

# emp_df = spark.read.format("csv").option("header", "true").option("mode", "dropmalformed") \
#     .load("C:/Users/test_emp.txt")
# emp_df.printSchema()
# emp_df.show()

#emp_df = spark.read.format("csv").option("header", "true").option("mode", "failfast") \
#    .load("C:/Users/GCP/test_emp.txt")
#emp_df.printSchema()
#emp_df.show()
##------------------------------------------------------------------------------------------------

spark2 = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

data = [(None, None), ("null", ""), ("NULL", ""), ("null", None), ("NULL", None), ("", ""), (" ", " "), (1, "lak"),
        (2, "viy"), ("", "sa")]

schema = StructType([
    StructField('cno', StringType(), True),
    StructField('cname', StringType(), True)])

df = spark2.createDataFrame(data=data, schema=schema)

df.show()
print("all the columns in the form of list")
print(df.columns) #[cno,cname]

# for c in [cno,cname]:
# case when cno is "null" or cno is "NULL" or cno == "" or cno == " "  then None

# df1 = df.select([when(col(c) == "null", None).otherwise(col(c)).alias(c) for c in df.columns])
df1 = df.select([when(col(c).isin("null", "", "NULL"," "), None).otherwise(col(c)).alias(c) for c in df.columns])
df1.show()
df1.dropna(how='all').show()
