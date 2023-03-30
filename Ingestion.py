from pyspark.sql import SparkSession
import json
import argparse
from pyspark.sql import SQLContext
from pyspark import SparkContext
#from pymongo import MongoClient


parser = argparse.ArgumentParser()


# Add the command line arguments

parser.add_argument("--s_path", type=str, required=True, help="source path")
parser.add_argument("--s_type", type=str, required=True)
parser.add_argument("--t_path", type=str, required=True, help="target path")
parser.add_argument("--t_type", type=str, required=True)
parser.add_argument("--s_table_name", type=str,required= False,default="")
parser.add_argument("--s_db_name", type=str,required= False,default="")
parser.add_argument("--s_username", type=str,required= False,default="")
parser.add_argument("--s_password", type=str,required= False,default="")
parser.add_argument("--t_table_name", type=str,required= False,default="")
parser.add_argument("--t_db_name", type=str,required= False,default="")
parser.add_argument("--t_username", type=str,required= False,default="")
parser.add_argument("--t_password", type=str,required= False,default="")


args = parser.parse_args()

# Load the metadata file

with open("config.json", "r") as f:
    metadata = json.load(f)


# Update the metadata with the command line arguments

metadata["source"]["file_path"] = args.s_path
metadata["target"]["file_path"] = args.t_path
metadata["source"]["type"] = args.s_type
metadata["target"]["type"] = args.t_type
metadata["source"]["table_name"] = args.s_table_name
metadata["source"]["db_name"] = args.s_db_name
metadata["source"]["username"] = args.s_username
metadata["source"]["password"] = args.s_password
metadata["target"]["table_name"] = args.t_table_name
metadata["target"]["db_name"] = args.t_db_name
metadata["target"]["username"] = args.t_username
metadata["target"]["password"] = args.t_password

# Save the modified metadata back to the file

with open("config.json", "w") as f:
    json.dump(metadata, f)


with open("config.json", "r") as f:
    metadata = json.load(f)


def read_write_data(source_path,target_path,source_type,target_type,source_options={},target_options={}):
    if source_type=="rdbms" or target_type=="rdbms":
        return rdbms_read_write_data(source_path,target_path,source_type,target_type,source_options,target_options)
    else:
        return local_read_write_data(source_path,target_path,source_type,target_type,source_options={},target_options={})


def local_read_write_data(source_path,target_path,source_type,target_type,source_options={},target_options={}):
    spark = SparkSession.builder.appName("ReadWriteData").getOrCreate()
    df = spark.read.format(source_type).options(**source_options).load(source_path)
    df.coalesce(1).write.format(target_type).mode("overwrite").options(**target_options).save(target_path)
    if validate(target_path,target_type,df.count(),spark):
        print("NO DATA LOSS")
    else:
        print("DATA LOST WHILE WRITING")
    return df

def rdbms_read_write_data(source_path,target_path,source_type,target_type,source_options={},target_options={}):
    spark = SparkSession.builder.appName("ReadWriteData").getOrCreate()

    if source_type=="rdbms":
        url = source_path
        table = source_options["table_name"]
        properties = { "user": source_options["username"], "password": source_options["password"], "driver": "org.postgresql.Driver" }
        df = spark.read.jdbc(url=url, table=table, properties=properties)
        read_count = df.count()
        df.coalesce(1).write.format(target_type).mode("overwrite").options(**target_options).save(target_path)
        if validate(target_path,target_type,read_count,spark,target_options):
            print("NO DATA LOSS")
        else:
            print("DATA LOST")
    else:
        df = spark.read.format(source_type).options(**source_options).load(source_path)
        read_count = df.count()
        url = target_path
        table = target_options["table_name"]
        properties = { "user": target_options["username"], "password": target_options["password"], "driver": "org.postgresql.Driver" }
        df.write.jdbc(url=url, table=table,mode="overwrite", properties=properties)
        if validate(target_path,target_type,read_count,spark,target_options,properties):
            print("NO DATA LOSS")
        else:
            print("DATA LOST")

    return df

def validate(target_path,target_type,read_count,spark,target_options={},properties={}):
    if target_type=="rdbms":
        df = spark.read.jdbc(url=target_path, table=target_options["table_name"], properties=properties)
    else:
        df = spark.read.format(target_type).load(target_path)
    df.show()
    if read_count==df.count():
        return True
    else:
        return False
    
    
    

source_options = {
    "db_name":metadata["source"]["db_name"],
    "table_name":metadata["source"]["table_name"],
    "username":metadata["source"]["username"],
    "password":metadata["source"]["password"]
}

target_options = {
    "db_name":metadata["target"]["db_name"],
    "table_name": metadata["target"]["table_name"],
    "username": metadata["target"]["username"],
    "password": metadata["target"]["password"]
}

df = read_write_data(metadata["source"]["file_path"],metadata["target"]["file_path"],metadata["source"]["type"],metadata["target"]["type"],source_options,target_options)

