from pyspark.sql.functions import *
from utils import categorize_expenses,entity_label


boursorama_df = spark.read.option("inferSchema","true").option("header","true").option("sep",";").csv("gs://project-banque/boursorama.csv")
boursorama_df.createOrReplaceTempView("boursorama_statistics")
boursorama_table = spark.sql("select * from boursorama_statistics")

boursorama_table = boursorama_table.filter(~(col("category").like("%Virement%") | col("label").like("%VIR%")))

boursorama_table = boursorama_table.withColumn('solde',col('accountbalance'))

boursorama_table = boursorama_table.withColumn('transaction_amount',col('amount'))

boursorama_table = boursorama_table.withColumn("Date",to_date(col("dateOp")))

boursorama_table = boursorama_table.withColumn("Nature",upper(col("label")))

boursorama_table = boursorama_table.select("Date","Nature","solde","transaction_amount")

expenses = boursorama_table.filter((col("Nature").like("%CARTE%")))

extract_entity = lambda entity : "_".join(entity.split()[-4:])
extract_entity = udf(extract_entity)

expenses = expenses.withColumn("Entity",extract_entity(expenses["Nature"]))

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
    StructField("Date",StringType(),True), \
    StructField("Nature", StringType(), True), \
    StructField("solde", DoubleType(), True), \
    StructField("transactionAmount", StringType(), True) , \
    StructField("Label", StringType(), True), \
    StructField("Entity", DoubleType(), True), \
  ])

emptyRDD = spark.sparkContext.emptyRDD()
expenses = spark.createDataFrame(data = emptyRDD,schema = schema)

for i in expenses_dict.keys():
    expenses = expenses.union(expenses_dict[i])

expenses = expenses.orderBy("date")
expenses = expenses.drop('Nature')

modifyID = udf(lambda s : "TR"+ s)

from pyspark.sql.window import Window
expenses = expenses.select("*").withColumn("TransID",modifyID(row_number() \
            .over(Window.orderBy(monotonically_increasing_id())).cast("string").alias("TransID")))


expenses.toPandas().to_csv("gs://project-banque/boursorama_cleaned.csv")
