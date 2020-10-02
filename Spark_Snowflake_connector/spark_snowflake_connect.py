 
def materialize_sf(tablename, viewname):
    ''' Reads from a specific Snowflake DB (see spark.read for particulars) and returns a dataframe and view for use elsewhere
    :param tablename - the table in SF
    :param viewname - the view that is created
    :return sf_df - a Spark dataframe paired to the specified view. 
    '''
    sqlquery = "select * from LNR_POC.{};".format(tablename)
    
    # Using Spark-Snowflake connector to create Spark dataframe from Snowflake table 
    sf_df = spark.read \
        .option("sfWarehouse", "ETL_WH_2XLARGE") \
        .option("sfDatabase","LINEAR_POC_DB") \
        .option("sfSchema", "LNR_POC") \
        .option("sfRole", "LNR_POC_ROLE") \
        .snowflake("Linear-Data-Validation", "ETL_WH_2XLARGE",sqlquery)
    
    # Temporary view with SparkSQL
    sf_df.createOrReplaceTempView(viewname)
    return sf_df

# Load sample table as dataframe
advert_df = materialize_sf('ADVRT', 'A')
