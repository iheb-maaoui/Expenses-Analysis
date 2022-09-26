from pyspark.sql.functions import *
from utils import categorize_expenses,entity_label


sg_df = spark.read.option("inferSchema","true").option("header","true").option("sep",",").csv("gs://project-banque/sg.csv")
sg_df.createOrReplaceTempView("sg_statistics")
sg_table = spark.sql("select * from sg_statistics")

expenses = sg_table.filter((col("Nature").like("CARTE %") | col("Nature").like("%FRAIS%VIR%")) & (~col("Nature").like("CARTE%RETRAIT%")) & (~col("Nature").like("%MAISEL%")))
expenses = expenses.withColumn("Nature",upper(col("Nature")))

expenses_dict = {}
expenses_aux = expenses
for i in entity_label.keys():
    expenses_aux,expenses_categorical = categorize_expenses(expenses_aux,i,entity_label[i])
    expenses_dict[i] = expenses_categorical
expenses_aux = expenses_aux.withColumn("Label",lit("AUTRE"))
expenses_aux = expenses_aux.drop('Entity')
expenses_aux = expenses_aux.select("*").withColumn("Entity",lit("AUTRE"))
expenses_dict["AUTRE"] = expenses_aux

from pyspark.sql.types import StructType,StructField, StringType, IntegerType,DoubleType
schema = StructType([ \
    StructField("_c0",IntegerType(),True), \
    StructField("Date",StringType(),True), \
    StructField("Valeur",StringType(),True), \
    StructField("Nature", StringType(), True), \
    StructField("Debit", DoubleType(), True), \
    StructField("Credit", DoubleType(), True), \
    StructField("solde", DoubleType(), True), \
    StructField("Label", StringType(), True), \
    StructField("Entity", StringType(), True)
  ])

emptyRDD = spark.sparkContext.emptyRDD()
expenses = spark.createDataFrame(data = emptyRDD,schema = schema)

for i in expenses_dict.keys():
    expenses = expenses.union(expenses_dict[i])

expenses = expenses.withColumn('transactionAmount',when(col("Debit").isNull(),col("Credit")).otherwise(-col("Debit")))

expenses = expenses.withColumn("date",to_date(col("Date"),"dd/MM/yyyy"))
expenses = expenses.orderBy("date")

expenses = expenses.drop("_c0","Valeur","Nature","Credit","Debit")

expenses = expenses.withColumn("Label",upper(col("Label")))
expenses = expenses.withColumn("Entity",upper(col("Entity")))

modifyID = udf(lambda s : "TR"+ s)

from pyspark.sql.window import Window
expenses = expenses.select("*").withColumn("TransID",modifyID(row_number() \
            .over(Window.orderBy(monotonically_increasing_id())).cast("string").alias("TransID")))

expenses.toPandas().to_csv("gs://project-banque/sg_cleaned.csv")








