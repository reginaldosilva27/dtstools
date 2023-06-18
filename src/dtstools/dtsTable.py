from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import sum, col, count, round, concat, lit
from datetime import datetime

spark = SparkSession.builder.master("local").appName("dtstools").getOrCreate()

def get_dbutils(spark):
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
        except ImportError:
            import IPython
            dbutils = IPython.get_ipython().user_ns["dbutils"]
        return dbutils

dbutils = get_dbutils(spark)

# Function to get storage size
def getDirContent(ls_path):
    path_list = dbutils.fs.ls(ls_path)
    for dir_path in dbutils.fs.ls(ls_path):
        if (
            dir_path.isDir()
            and ls_path != dir_path.path
            and "_delta_log" not in dir_path.path
        ):
            path_list += getDirContent(dir_path.path)
    return path_list

# Function to return Actual table size vs Storage size
def tableSize(database, tableName):
    try:
        ddlSchema = StructType(
            [
                StructField("path", StringType()),
                StructField("name", StringType()),
                StructField("size", IntegerType()),
                StructField("modificationTime", StringType()),
            ]
        )

        df = spark.sql(f"describe detail {database}.{tableName}")

        df_detail = df.select(
            "name",
            "location",
            "createdAt",
            "lastModified",
            col("numFiles").alias("actualTableFiles"),
            (col("sizeInBytes") / 1024 / 1024).alias("actualTableSizeMB"),
            (col("sizeInBytes") / 1024 / 1024 / 1024).alias("actualTableSizeGB"),
        )

        dfPath = spark.createDataFrame(
            getDirContent(df_detail.collect()[0]["location"]), ddlSchema
        )

        dfSumStorage = (
            dfPath.filter("size > 0")
            .select(col("size"), col("path"))
            .agg(
                sum(col("size") / 1024 / 1024).alias("StorageSizeMB"),
                sum(col("size") / 1024 / 1024 / 1024).alias("StorageSizeGB"),
                count(col("path")).alias("StorageFiles"),
            )
        )

        df_return = df_detail.crossJoin(dfSumStorage).select(
            "name",
            "actualTableFiles",
            "StorageFiles",
            round("actualTableSizeMB", 2).alias("actualTableSizeMB"),
            round("StorageSizeMB", 2).alias("StorageSizeMB"),
            round("actualTableSizeGB", 2).alias("actualTableSizeGB"),
            round("StorageSizeGB", 2).alias("StorageSizeGB"),
            concat(concat(lit("Storage is bigger "),round(col("StorageSizeGB") / col("actualTableSizeGB"),0)),lit("x (times)")).alias("Status"),
            "location",
            "createdAt",
            "lastModified",
        )
        return df_return.withColumn('dateLog',lit(f'{datetime.today()}'))
    except Exception as e:
        print(f"###### Error to load tableName {tableName} - {e}######")

# Function to apply Optimize and Vacuum
def tableMaintenance (schemaName='none', tableName='none', zorderColumns='none', vacuumRetention=168, vacuum=True, optimize=True, debug=True):
    if debug:
        print("Debug enbaled!")
        if optimize:            
            if zorderColumns != "none":
                print(f">>> Optimizing table {schemaName}.{tableName} ZORDER with columns: {zorderColumns} <<< >>> {str(datetime.now())}")
                print(f"CMD: OPTIMIZE {schemaName}.{tableName} ZORDER BY ({zorderColumns})")
            else:
                print(f">>> Optimizing table {schemaName}.{tableName} without ZORDER <<< >>> {str(datetime.now())}")
                print(f"CMD: OPTIMIZE {schemaName}.{tableName}")
            print(f">>> Table {schemaName}.{tableName} optimized! <<< >>> {str(datetime.now())}")
        else:
            print(f"### OPTIMIZE not run! ###")
        
        if vacuum:
            print(f">>> Setting {vacuumRetention} hours for delta lake version cleanup... <<< >>> {str(datetime.now())}")
            print(f"CMD: VACUUM {schemaName}.{tableName} RETAIN {vacuumRetention} Hours")
            print(f">>> Successful cleaning {schemaName}.{tableName} <<< >>> {str(datetime.now())}")
        else:
            print(f"### VACUUM not run! ###")
    else:
        if optimize:
            if zorderColumns != "none":
                print(f">>> Optimizing table {schemaName}.{tableName} ZORDER with columns: {zorderColumns} <<< >>> {str(datetime.now())}")
                print(f"CMD: OPTIMIZE {schemaName}.{tableName} ZORDER BY ({zorderColumns})")
                spark.sql(f"OPTIMIZE {schemaName}.{tableName} ZORDER BY ({zorderColumns})")
            else:
                print(f">>> Optimizing table {schemaName}.{tableName} without ZORDER <<< >>> {str(datetime.now())}")
                print(f"CMD: OPTIMIZE {schemaName}.{tableName}")
                spark.sql(f"OPTIMIZE {schemaName}.{tableName}")
            print(f">>> Table {schemaName}.{tableName} optimized! <<< >>> {str(datetime.now())}")
        else:
            print(f"### OPTIMIZE not run! ###")
        
        if vacuum:
            print(f">>> Setting {vacuumRetention} hours for delta lake version cleanup... <<< >>> {str(datetime.now())}")
            spark.sql("set spark.databricks.delta.retentionDurationCheck.enabled = false")
            print(f"CMD: VACUUM {schemaName}.{tableName} RETAIN {vacuumRetention} Hours")
            spark.sql(f"VACUUM {schemaName}.{tableName} RETAIN {vacuumRetention} Hours")
            spark.sql("set spark.databricks.delta.retentionDurationCheck.enabled = true")
            print(f">>> Successful cleaning {schemaName}.{tableName} <<< >>> {str(datetime.now())}")
        else:
            print(f"### VACUUM not run! ###")

# Function to get help about
def Help():
    print("v0.0.2")
    print("""____  ______ __ ______  ___    ___  __    __  """)
    print('|| \\\\ | || |(( \| || | // \\\\  // \\\\ ||   (( \ ')
    print("""||  ))  ||   \\\\   ||  ((   ))((   ))||    \\\\  """)
    print("""||_//   ||  \_))  ||   \\\\_//  \\\\_// ||__|\_)) """)
    print("")
    print("------------------------------------------------------")
    print("Function tableSize(database, tableName)")
    print("------------------------------------------------------")
    print("This function return a Dataframe with result")
    print("Find out the true size of your table")
    print("")
    print(">> Sample call:")
    print("dtsTable.tableSize('dbname','tbName').display()")
    print("")
    print(">> Save the result in a Delta Table for monitoring and baseline")
    print("dtsTable.tableSize(databaseName,tableName) \ ")
    print(" .write.format('delta') \ ")
    print(" .mode('append') \ ")
    print(" .saveAsTable('db_demo.tableSize',path='abfss://container@storage.dfs.core.windows.net/bronze/tableSize')")
    print("")
    print(">> Get the size of all tables in your database")
    print("for tb in spark.sql(f'show tables from db_demo').collect():")
    print("try:")
    print("    print('>> Collecting data... Table:',tb.tableName)")
    print("    dtsTable.tableSize(tb.database,tb.tableName) \ ")
    print("    .write.format('delta') \ ")
    print("    .mode('append') \ ")
    print("    .saveAsTable('db_demo.tableSize',path='abfss://container@storage.dfs.core.windows.net/bronze/tableSize') ")
    print("except Exception as e:")
    print("    print (f'###### Error to load tableName {tb.tableName} - {e}######') ') ")
    print("")
    print("------------------------------------------------------")
    print("Function tableMaintenance()")
    print("------------------------------------------------------")
    print("This function apply Optimize and Vacuum")
    print("")
    print(">> Sample call:")
    print("dtsTable.tableMaintenance(schemaName='silver', tableName='tableName', zorderColumns='none', vacuumRetention=168, vacuum=True, optimize=True, debug=False)")
    print("")
    print("Reference: https://github.com/reginaldosilva27/dtstools")

