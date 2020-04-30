#!/usr/bin/env python

###
### Author : LChawathe
###

import findspark
findspark.init()    # findspark.find() to print out SPARK_HOME

from itertools import chain

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType
from pyspark.sql import Row
from pyspark.sql.functions import lit, col, avg, sum, broadcast, when, create_map

app_name='PySpark-dynamic-Dataframe-with-SQL-query'
master_host = 'local'

# Spark configuration
sparkCfg = SparkConf() \
    .set("spark.driver.host", "localhost") \
    .set("spark.executor.cores", 4) \
    .set("spark.executor.instances", 4) \
    .set("spark.executor.memory", "4g") \
    .set("spark.driver.memory", "8g") \
    .set("spark.dynamicAllocation.enabled", "true") \
    .set("spark.dynamicAllocation.minExecutors", "1") \
    .set("spark.dynamicAllocation.maxExecutors", "6")

# Spark session
spark = SparkSession \
    .builder \
    .appName(app_name) \
    .config(conf = sparkCfg) \
    .master(master_host) \
    .getOrCreate()

# Spark Context
sc = spark.sparkContext
sc.setLogLevel("ERROR")

print("=== Checking Spark config ===")
print(sc.getConf().getAll())
print("=============================")

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
             (150, 'AI & ML', 'Madison, WI'),
             (180, 'Medicine', 'Madison, WI'),
             (200, 'Finance', 'SLC, UT'),
             (222, 'Import/Export', 'Nashville, TN'),
             (233, 'Economics', 'Tampa, FL'),
             (250, 'Airospace', 'Seattle, WA'),
             (300, 'Medicine', 'Seattle, WA'),
             (303, 'Medicine', 'Boston, MA'),
             (333, 'Import/Export', 'Boulder, CO'),
             (350, 'Finance', 'Research Triangle Park, Raleigh-Durham-Cary, NC'),
             (369, 'MBA', 'Research Triangle Park, Raleigh-Durham-Cary, NC'),
             (400, 'MBA', 'NYC')
            ]
dept_rdd = sc.parallelize(dept_data)

# Option 1 : Create dataframe from RDD
dept_df = spark.createDataFrame(dept_rdd, schema=dept_schema)
dept_df.createOrReplaceTempView('Departments')  # table: Departments

dept_df.show(truncate=False)

###
# Employees
###
Employee = Row('dept_id', 'firstName', 'lastName', 'email', 'salary')

employee1 = Employee(100, 'michael', 'armbrust', 'michael.armbrust@berkeley.edu', 100000)
employee2 = Employee(100, 'xiangrui', 'meng', 'xiangrui_meing@stanford.edu', 120000)
employee3 = Employee(150, 'matei', None, 'no-reply@uw.edu', 140000)
employee4 = Employee(150, 'Machine', 'Learning', 'ml@cmu.edu', 125000)
employee5 = Employee(100, None, 'wendell', 'no-reply@berkeley.edu', 160000)
employee6 = Employee(200, 'michael', 'jackson', 'michael_jackson@vanderbilt.edu', 80000)
employee7 = Employee(250, 'larry', 'chaw', 'larry.chaw@disney.com', 200000)
employee8 = Employee(300, 'John', 'H', 'john.hopkins@johnhopkinds.com', 200000)
employee9 = Employee(180, 'achi', 'gochi', 'achi.gochi@uw.edu', 150000)
employee10 = Employee(303, 'Rx', 'Rx', 'rx@harward.edu', 200000)
employee11 = Employee(222, 'Fresno', 'Quincy', 'quincy@vanderbilt.edu', 160000)
employee12 = Employee(369, 'Rx', 'Rx', 'sae@duke.edu', 180000)
employee13 = Employee(400, 'Caillou', 'Rosy', 'caillou.rosy@columbia.edu', 160000)

employee_data_rows = [employee1, employee2, employee3, employee4, employee5, employee6, employee7, employee8, employee9, employee10, employee11, employee12, employee13]

# Option 2 : Create dataframe from List of Row
all_employees_df = spark.createDataFrame(employee_data_rows)
all_employees_count = all_employees_df.count()
print('All employees : ', all_employees_count)
all_employees_df.show(truncate=False)

# Clone dataframe
employee_df = all_employees_df.select("*")

# filter out records with nulls values in columns. Prefer Pythonic way...
### Pythonic ways to filter out null value
employee_df = employee_df.filter(employee_df.firstName.isNotNull()) # WORKING: Filter null first-name
# employee_df = employee_df.filter(col('lastName').isNotNull())     # WORKING: Filter null last-name
### ***Non-Pythonic*** ways to filter out null values
# employee_df = employee_df.filter(col('lastName') != None)         # NOT-WWORKING!!! Do not use :  != None
# employee_df = employee_df.filter(col('lastName') != 'null')       # WORKING: Filte null last-name
employee_df = employee_df.filter(col('lastName') != '')             # WORKING: Filte null last-name

employee_df.createOrReplaceTempView('Employees')    # table: Employees
valid_employees_count = employee_df.count()
print('Valid (non-null named) employees : ', valid_employees_count)
employee_df.show(truncate=False)

# Ensure null-value records have been filtered out 
assert (valid_employees_count < all_employees_count)

# broadcast simple map
city_to_region_map = { 'Seattle, WA' : 'North West', 
                       'Austin, TX' : 'South',
                       'SLC, UT' : 'West',
                       'Boulder, CO' : 'West',
                       'Nashville, TN' : 'South East',
                       'Tampa, FL' : 'South East',
                       'Madison, WI' : 'Mid West',
                       'Boston, MA' : 'North East',
                       'NYC' : 'North East'

                       # RTP, NC is not mapped intentionally
                       # 'Research Triangle Park, Raleigh-Durham-Cary, NC' : '---'
                    }
bc_city_to_region = sc.broadcast(city_to_region_map)

# Refer: https://stackoverflow.com/questions/50321549/map-values-in-a-dataframe-from-a-dictionary-using-pyspark
bc_c_2_r_dict = bc_city_to_region.value
mapping_expr = create_map ( [lit(x) for x in chain(*bc_c_2_r_dict.items())] )    # <--- This is absolutely wonderful!!!

# JOIN using broadcasted dataframe
joined_with_broadast_df = all_employees_df \
            .join( broadcast(dept_df), all_employees_df.dept_id == dept_df.Dept_ID , how = 'right') \
            .drop(all_employees_df.dept_id) \
            .withColumn('Region', mapping_expr[dept_df.Location] ) \
            .fillna( {'Region' : 'Unknown Region'} )    # if Region has null, due to unmapped city-to-region, default it
            #.withColumn ('Region', when( dept_df.Location.isNotNull() , lit('Known Region') ).otherwise( lit('Unknown Region') ) )
            # lambda city: bc_c_2_3_map[dept_df.Location] if dept_df.Location in bc_c_2_3_map else 'Unknown Region'

joined_with_broadast_df.printSchema()
joined_with_broadast_df.show()

print('Average salary by Region : ')
avg_salary_by_region_for_all_employes_df = joined_with_broadast_df.groupBy(col('Region')) \
                                                                    .agg({'salary': 'avg'}) \
                                                                    .withColumnRenamed('AVG(salary)', 'Avg_Salary_by_Region')
avg_salary_by_region_for_all_employes_df.show(truncate=False)

###
# Spark SQL join
###
dept_empl_df = spark.sql("""SELECT d.Dept_Name, d.Location, e.firstName, e.lastName, e.salary
                            FROM Departments d, Employees e
                            WHERE d.Dept_ID == e.dept_id""")
dept_empl_df.show()
### Save as single CSV file
dept_empl_df.coalesce(1) \
            .write.csv(path="./dept-empl-joined-csv", mode='overwrite', header=True)

# avg salary by dept
avg_salary_by_dept_df = dept_empl_df.groupBy(col('Dept_Name')) \
                                    .agg(avg('salary').alias('Avg_Salary_by_Dept'))     # Inline alias for aggregrate column
print('Average salary by Dept : ')
avg_salary_by_dept_df.show()
dept_empl_df.write.parquet(path="./avg-salary-by-dept-parquet", mode='overwrite')

# avg salry by location
avg_salary_by_location_df = dept_empl_df.groupBy(col('Location')) \
                                        .agg({'salary': 'avg'}) \
                                        .withColumnRenamed('AVG(salary)', 'Avg_Salary_by_Location')    # Rename column for DF
print('Average salary by City : ')
avg_salary_by_location_df.show()
dept_empl_df.write.parquet(path="./avg-salary-by-location-parquet", mode='overwrite')