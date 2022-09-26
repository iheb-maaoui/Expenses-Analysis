def categorize_expenses(expenses,label,entity):
    expenses1 = expenses.withColumn("Entity",col("Nature"))
    expenses2 = expenses1.filter(col("Entity").like(f"%{label}%"))
    expenses0 = expenses1.filter(~col("Entity").like(f"%{label}%"))
    expenses2 = expenses2.withColumn("Entity",lit(f"spent_money_{entity}"))
    expenses2 = expenses2.withColumn("Label",lit(f"{label}"))
    return expenses0,expenses2


entity_label = {"ORAIN":"CAFE","REINE":"BOULAGERIE","BOULANGERIE":"BOULAGERIE","HALFETI" : "KEBAB","MALIK":"KEBAB","TACOS":"OTACOS",\
               "AIR_FRANCE":"VOYAGE_VOL","EASYJET":"VOYAGE_VOL","TRAVELGENIO":"VOYAGE_HOTEL","LA_MAISON_DU":"CAFE",\
               "netflix":"NETFLIX","NETFLIX":"NETFLIX","CIRILLO":"VOYAGE_HOTEL","SAGA_SUPERMARCH":"SUPER_MARCHE",\
               "RESTAU" : "FOOD","CARREFOUR":"SUPER_MARCHE","BOUCHERIE":"BOUCHERIE","FRANPRIX":"SUPER_MARCHE", \
               "MONOPRIX":"SUPER_MARCHE","H MARKET":"SUPER_MARCHE","PRIMARK":"VETEMENTS",\
                "ADIDAS":"VETEMENTS","KIABI":"VETEMENTS","ZALANDO":"VETEMENTS","BEST SECRET":"VETEMENTS", \
                "TERRASSES":"CAFE","ACTION":"SUPER_MARCHE","DZ POWER":"PIZZA","UDEMY":"ONLINE_COURSES"}

def create_date_dim(date_column):
    dim_table = date_column.select(col("Date"),year(col("Date")) \
                                     .alias("Year"),month(col("Date")) \
                                     .alias("Month"),dayofmonth(col("Date")) \
                                     .alias("DayOfMonth"),dayofweek(col("Date")) \
                                     .alias("DayOfWeek"),dayofyear(col("Date")) \
                                     .alias("DayOfYear"),weekofyear(col("Date")) \
                                     .alias("WeekOfYear"))
    return dim_table