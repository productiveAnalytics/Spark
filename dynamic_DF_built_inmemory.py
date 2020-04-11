#!/usr/bin/env python

###
### Author : LChawathe
###

import findspark
findspark.init()    # findspark.find() to print out SPARK_HOME

from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType
from pyspark.sql import Row
from pyspark.sql.functions import col, avg, sum

app_name='PySpark-dynamic-Dataframe-with-SQL-query'
master_host = 'local'

spark = SparkSession \
    .builder \
    .appName(app_name) \
    .config("spark.some.config.option", "some-value") \
    .master(master_host) \
    .getOrCreate()

# Spark Context
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# Spark configuration
## spark.conf.set("spark.dynamicAllocation.enabled", "true")
## spark.conf.set("spark.executor.cores", 4)
## spark.conf.set("spark.executor.instances", 4)
## spark.conf.set("spark.dynamicAllocation.minExecutors", "1")
## spark.conf.set("spark.dynamicAllocation.maxExecutors", "6")

###
# Departments
###
dept_schema = StructType(
    [
        StructField('Dept_ID', IntegerType(), nullable=False),
        StructField('Dept_Name', StringType(), nullable=False),
        StructField('Location', StringType(), nullable=False)
    ]
)

dept_data = [(100, 'Computer Engineering', 'Seattle, WA'),
             (120, 'Economics', 'Austin, TX'),
             (150, 'Electrical & Electronics', 'Madison, WI'),
             (180, 'Medicine', 'Madison, WI'),
             (200, 'Finance', 'SLC, UT'),
             (250, 'Airospace', 'Seattle, WA'),
             (300, 'Medicine', 'Seattle, WA'),
             (303, 'Medicine', 'Boston, MA')
            ]
dept_rdd = sc.parallelize(dept_data)

dept_df = spark.createDataFrame(dept_rdd, schema=dept_schema)
dept_df.createOrReplaceTempView('Departments')  # table: Departments

dept_df.show()

###
# Employees
###
Employee = Row('dept_id', 'firstName', 'lastName', 'email', 'salary')

employee1 = Employee(100, 'michael', 'armbrust', 'michael.armbrust@berkeley.edu', 100000)
employee2 = Employee(100, 'xiangrui', 'meng', 'xiangrui_meing@stanford.edu', 120000)
employee3 = Employee(150, 'matei', None, 'no-reply@uw.edu', 140000)
employee4 = Employee(100, None, 'wendell', 'no-reply@berkeley.edu', 160000)
employee5 = Employee(200, 'michael', 'jackson', 'michael_jackson@vanderbilt.edu', 80000)
employee6 = Employee(250, 'larry', 'chaw', 'larry.chaw@disney.com', 200000)
employee7 = Employee(300, 'John', 'H', 'john.hopkins@johnhopkinds.com', 200000)
employee8 = Employee(180, 'achi', 'gochi', 'achi.gochi@uw.edu', 150000)
employee9 = Employee(303, 'Rx', 'Rx', 'rx@harward.edu', 200000)

employee_data = [employee1, employee2, employee3, employee4, employee5, employee6, employee7, employee8, employee9]
employee_df = spark.createDataFrame(employee_data)
employee_df.filter(col('firstName') == None)    # Filter null first-name
employee_df.createOrReplaceTempView('Employees')    # table: Employees

employee_df.show()

###
# Spark SQL join
###
dept_empl_df = spark.sql("""SELECT d.Dept_Name, d.Location, e.firstName, e.lastName, e.salary
                            FROM Departments d, Employees e
                            WHERE d.Dept_ID == e.dept_id""")
dept_empl_df.write.csv(path="./dept-empl-joined-CSV", mode='overwrite')
dept_empl_df.show()

# avg salary by dept
avg_salary_by_dept_df = dept_empl_df.groupBy(col('Dept_Name')) \
                                    .agg(avg('salary').alias('Avg_Salary_by_Dept'))     # Inline alias for aggregrate column

avg_salary_by_dept_df.show()
dept_empl_df.write.parquet(path="./avg-salary-by-dept-parquet", mode='overwrite')

# avg salry by location
avg_salary_by_location_df = dept_empl_df.groupBy(col('Location')) \
                                        .agg({'salary': 'avg'}) \
                                        .withColumnRenamed('AVG(salary)', 'Avg_Salary_by_Location')    # Rename column for DF

avg_salary_by_location_df.show()
dept_empl_df.write.parquet(path="./avg-salary-by-location-parquet", mode='overwrite')