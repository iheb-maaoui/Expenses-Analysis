from pyspark.sql.functions import *
from utils import create_date_dim

boursorama_df = spark.read.option("inferSchema","true").option("header","true").option("sep",",").csv("gs://project-banque/boursorama_cleaned.csv")
boursorama_df.createOrReplaceTempView("boursorama_statistics")
boursorama_table = spark.sql("select * from boursorama_statistics")

sg_df = spark.read.option("inferSchema","true").option("header","true").option("sep",",").csv("gs://project-banque/sg_cleaned.csv")
sg_df.createOrReplaceTempView("sg_statistics")
sg_table = spark.sql("select * from sg_statistics")

sg_table = sg_table.withColumn("Label",upper(col("Label")))
sg_table = sg_table.withColumn("Entity",upper(col("Entity")))

boursorama_table = boursorama_table.withColumn("Label",upper(col("Label")))
boursorama_table = boursorama_table.withColumn("Entity",upper(col("Entity")))

sg_table = sg_table.withColumn("Banque",lit("Société_Générale"))
sg_table = sg_table.withColumn("BNID",lit("BN1"))

boursorama_table = boursorama_table.withColumnRenamed('Date','date')

boursorama_table = boursorama_table.select('_c0','date','solde','Label','Entity','transactionAmount','TransID','Banque','BNID')

ready_dataset = sg_table.union(boursorama_table)

ready_dataset = ready_dataset.orderBy("Date")

ready_dataset = ready_dataset.drop('_c0')

ready_dataset.toPandas().to_csv("gs://project-banque/ready_dataset.csv")


Date = ready_dataset.select("Date").distinct()
Date_dim_table = create_date_dim(Date)

from pyspark.sql.window import Window

modifyID = udf(lambda s : "Day"+ s)

Date_dim_table = Date_dim_table.withColumn("DayID",modifyID(row_number() \
            .over(Window.orderBy(monotonically_increasing_id())).cast("string").alias("DayID")))

Banque_dim_table = ready_dataset.select("Banque","BNID").distinct()

transaction_dim_table = ready_dataset.select("Label","Entity").distinct()

from pyspark.sql.window import Window

modifyID = udf(lambda s : "Tans"+ s)

transaction_dim_table = transaction_dim_table.withColumn("TransID",modifyID(row_number() \
            .over(Window.orderBy(monotonically_increasing_id())).cast("string").alias("TransID")))

dates_list = Date_dim_table.select("Date").rdd.flatMap(lambda x: x).collect()
dates_dict = {dates_list[i]:f"Day{i+1}" for i in range(len(dates_list))}
assign_id = udf(lambda x : dates_dict[x])

ready_dataset = ready_dataset.withColumn('DayID',assign_id(col("Date")))

ready_dataset = ready_dataset.withColumn("util",udf(lambda x,y : x + y )(col("Label"),col("Entity")))

transaction_dim_table = transaction_dim_table.withColumn("util",udf(lambda x,y : x + y )(col("Label"),col("Entity")))

trans_list = transaction_dim_table.select("util").rdd.flatMap(lambda x: x).collect()
trans_dict = {trans_list[i]:f"Trans{i+1}" for i in range(len(trans_list))}
assign_id = udf(lambda x : trans_dict[x])

ready_dataset = ready_dataset.withColumn('TransID',assign_id(col("util")))

transaction_dim_table = transaction_dim_table.drop('util')

ready_dataset = ready_dataset.drop('util')

fact_table = ready_dataset.select("DayID","TransID","BNID","transactionAmount")

ready_dataset.toPandas().to_csv("gs://project-banque/ready_dataset_final.csv")
transaction_dim_table.toPandas().to_csv("gs://project-banque/transaction_dim_table.csv")
Banque_dim_table.toPandas().to_csv("gs://project-banque/Banque_dim_table.csv")
Date_dim_table.toPandas().to_csv("gs://project-banque/Date_dim_table.csv")
fact_table.toPandas().to_csv("gs://project-banque/fact_table.csv")

