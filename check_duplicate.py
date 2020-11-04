
from pyspark.sql import SQLContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import * 
import datetime
import os
import sys
import boto3
import botocore
import logging
import argparse


def check_duplicate(start_date, todate, totime, log_prefix):
    """
    Checks for duplicates in landed date. Given a start date, it will loop from that date till today.

    :param start_date: The date to start checking duplicates for. Default is today
    :param todate: Today's date
    :param totime: Current time - used for output bucket creation
    :param log_prefix: Log prefix from previous call for logging
    """
    log_prefix=log_prefix+':check_duplicate'
    logging.info("{}: start_date:{} todate:{} totime:{}".format(log_prefix,start_date,todate,totime))
    inputdf = spark.read.csv("s3a://lineardp-repository-dev/dp-linear-landing-nifi/awscli/input/alltables.csv",header=True,sep="|");

    results3bucket = "s3a://lineardp-regressiontest-output-dev/"
    resultpath = "duplicate_checks/"
    bucket="lineardp-landing-advisor-dev"
    s3bucket="s3a://lineardp-landing-advisor-dev/"

    schema = StructType([
        StructField('LandingDate',StringType(), False),
        StructField('Tablename', StringType(), False),
        StructField('TotalCount', IntegerType(), False),
        StructField('DupeCount', IntegerType(), False)
    ])

    while start_date < todate:
        outputdf = spark.createDataFrame([], schema)
        check=0
    
        for f in inputdf.collect(): 
            stabname = f.schema+"."+f.tabname
        

            sdate=start_date+"/04"
            s3Path=f.schema+"/"+f.tabname+"/"+sdate+"/"
            s3object ="{}{}{}".format(s3bucket,s3Path,"*.parquet")
        
            #print(s3Path)
        
            s3 = boto3.resource('s3')
            my_bucket = s3.Bucket(bucket)
            counter=0
        
        
            for object_summary in my_bucket.objects.filter(Prefix=s3Path):
                #print(object_summary.key)
                counter=counter+1
                if counter==1:
                   df = spark.read.parquet(s3object)
                   df.createOrReplaceTempView("countFile")
        
                   dupes = spark.sql("SELECT SRC_KEY_VAL,SRC_CDC_OPER_NM,count(*) FROM countFile group by SRC_KEY_VAL,SRC_CDC_OPER_NM having count(*) > 1")
                   logging.info("{}: {}|{}".format(log_prefix,stabname,df.count(),dupes.count()))
                   if dupes.count() > 0:
                      check = 1
                   newRow = spark.createDataFrame([(start_date,stabname,df.count(),dupes.count())])
                   outputdf = outputdf.union(newRow)
                   break
    
            if counter == 0:
                logging.info("{}: {}|{}|{}".format(log_prefix,stabname,-1,0));
                newRow = spark.createDataFrame([(start_date,stabname,-1,0)])
                outputdf = outputdf.union(newRow)



        resultfullpath=results3bucket+resultpath+start_date     
    
        if check == 1:
           logging.info("{}: DUPLICATES FOUND for Date: {} in folder:{}".format(log_prefix,start_date,resultfullpath))
        else:
           logging.info("{}: NO DUPLICATES for Date: {} in folder:{}".format(log_prefix,start_date,resultfullpath))
    
        #logging.info("{}: Writing results to output folder: {}".format(log_prefix,resultfullpath))
        outputdf.coalesce(1).write.save(resultfullpath, format='csv',mode='overwrite',sep =",",header=True)
    
        ndate=datetime.datetime.strptime(start_date,"%Y/%m/%d") + datetime.timedelta(days=1)
        start_date=datetime.datetime.strftime(ndate,'%Y/%m/%d')


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)
    log_prefix="lineardp_duplicate_check"
    logging.info("{}: START OPERATIONAL TIME IS: {}".format(log_prefix, datetime.datetime.now()))

    now = datetime.datetime.now()
    todate= now.strftime("%Y/%m/%d")
    totime= now.strftime("%Y/%m/%d/%Y%m%d%H%M%S")
    yestdate= now + datetime.timedelta(days=-1)
    yestdate= datetime.datetime.strftime(yestdate,"%Y/%m/%d")
    logging.info("{}: Today is: {}".format(log_prefix, todate))

    spark = SparkSession.builder.appName("test").getOrCreate() 

    

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--start_date', default=yestdate, type=str,
        help="Execution date to check for duplicates")
    args = parser.parse_args()
    
    # Not needed as execution date from Airflow is one day less when scheduled. this minus one day is done, as data in S3 is available for previous day partition only
    #start_date = datetime.datetime.strptime(args.start_date,"%Y/%m/%d") + datetime.timedelta(days=-1)
    #start_date=datetime.datetime.strftime(start_date,'%Y/%m/%d')

    check_duplicate(start_date, todate, totime, log_prefix)

    logging.info("{}: END OPERATIONAL TIME IS: {}".format(log_prefix, datetime.datetime.now()))
