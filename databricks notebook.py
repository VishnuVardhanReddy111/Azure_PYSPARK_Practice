# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM `man_cata`.`martschemavishnu`.`big_mart_sales`;

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Reading JSON

# COMMAND ----------

df_json = spark.read.format('json').option('inferschema',True)\
                    .option('header',True)\
                    .option('multiline',False)\
                    .load('/Volumes/man_cata/martschema/vishnuvolume/drivers.json')

# COMMAND ----------

df_json.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Reading

# COMMAND ----------

dbutils.fs.ls('/Volumes/man_cata/martschema/vishnuvolume/BigMart Sales.csv')

# COMMAND ----------

df = spark.read.format('csv').option('inferschema',True).option('header',True).load('/Volumes/man_cata/martschema/vishnuvolume/BigMart Sales.csv')

# COMMAND ----------

df.show()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Definition

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## DDL SCHEMA

# COMMAND ----------

myddl_schema = """
Item_Identifier string,
Item_Weight string,
Item_Fat_Content string,
Item_Visibility double,
Item_Type string,
Item_MRP double,
Outlet_Identifier string,
Outlet_Establishment_Year integer,
Outlet_Size string
"""

# COMMAND ----------

df = spark.read.format('csv').schema(myddl_schema).option('header',True).load('/Volumes/man_cata/martschema/vishnuvolume/BigMart Sales.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## StructType() schema

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

my_struct_schema = StructType([
  StructField('Item_Identifier', StringType(),True),
  StructField('Item_Weight', StringType(),True),
  StructField('Item_Fat_Content', StringType(),True),
  StructField('Item_Visibility', DoubleType(),True),
  StructField('Item_Type', StringType(),True),
  StructField('Item_MRP',StringType(),True),
  StructField('Outlet_Identifier', StringType(),True),
  StructField('Outlet_Establishment_Year', StringType(),True),
  StructField('Outlet_Size', StringType(),True)
])

# COMMAND ----------

df = spark.read.format('csv').schema(my_struct_schema).option('header',True).load('/Volumes/man_cata/martschema/vishnuvolume/BigMart Sales.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## SELECT

# COMMAND ----------

df.select(col('Item_Identifier'),col('Item_Weight'),col('Item_Fat_Content')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # ALIAS
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

df.select(col('Item_Identifier').alias('Item_ID')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # FILTER
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 1

# COMMAND ----------

df.display()

# COMMAND ----------

df.filter(col('Item_Fat_Content')=='Regular').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 2

# COMMAND ----------

df.filter((col('Item_Weight')<5) & (col('Item_Type')=='Soft Drinks')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 3

# COMMAND ----------

df.filter((col('Outlet_Size').isNull()) & (col('Outlet_Location_Type').isin('Tier 1', 'Tier 2'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # With Columns

# COMMAND ----------

df.withColumnRenamed('Item_Weight','Item_Wt').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 1

# COMMAND ----------

df = df.withColumn('flag',lit('new'))

# COMMAND ----------

df.display()

# COMMAND ----------

df.withColumn('multiply',col('Item_Weight')*col('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 2

# COMMAND ----------

df.withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),"Regular","Reg"))\
  .withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),"Low Fat","LF"))\
  .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Type casting

# COMMAND ----------

df = df.withColumn('Item_Weight',col('Item_Weight').cast(StringType()))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # Sort

# COMMAND ----------

df.sort(col('Item_Weight').desc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 2

# COMMAND ----------

df.sort(col('Item_Visibility').asc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 3

# COMMAND ----------

df.sort(['Item_Weight','Item_Visibility'], ascending = [0,0]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 4

# COMMAND ----------

df.sort(['Item_Weight','Item_Visibility'],ascending=[0,1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #LIMIT

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Drop

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario1

# COMMAND ----------

df.drop(col('Item_Visibility')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 2
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

df.drop('Item_MRP','Item_Type').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop_Duplicates

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario - 2

# COMMAND ----------

df.drop_duplicates(subset = ['Item_Type']).display()

# COMMAND ----------

df.distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Preparing dataframes

# COMMAND ----------

data1 = [('Vishnu','1'),('Ravi','2'),('Suresh','3')]
schema1 = 'name string, id string'
df1 = spark.createDataFrame(data1, schema1)
data2 = [('4','Harika'),('5','Sai'),('6','Ramesh')]
schema2 = 'id string, name string'
df2 = spark.createDataFrame(data2, schema2)

# COMMAND ----------

df1.display()

# COMMAND ----------

df2.display()

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

df1.unionAll(df2).display()

# COMMAND ----------

df1.unionByName(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Union By Name

# COMMAND ----------

df1.unionByName(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # String Funxtions

# COMMAND ----------

# MAGIC %md
# MAGIC ## initcap()

# COMMAND ----------

df.select(initcap('Item_Type')).display()

# COMMAND ----------

df.select(lower('Item_Type')).display()

# COMMAND ----------

df.select(upper('Item_Type')).display()

# COMMAND ----------

df.select(upper('Item_Type').alias('Upper_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Date Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ## CurrentDate

# COMMAND ----------

df = df.withColumn('curr_date',current_date())
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Date_Add()

# COMMAND ----------

df = df.withColumn('week_after',date_add('curr_date',7))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Date_Sub()

# COMMAND ----------

df.withColumn('week_before',date_sub('curr_date',7)).display()

# COMMAND ----------

df = df.withColumn('week_before',date_add('curr_date',-7))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #DateDiff

# COMMAND ----------

df = df.withColumn('datediff',datediff('curr_date','week_after'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Date_Format

# COMMAND ----------

df.withColumn('Date_Format',date_format('curr_date','dd-MM-yyyy')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Handling NULLS

# COMMAND ----------

# MAGIC %md
# MAGIC ##Dropping Nulls

# COMMAND ----------

df.dropna('all').display()

# COMMAND ----------

df.dropna('any').display()

# COMMAND ----------

df.dropna(subset=['Outlet_Size']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Filling Nulls

# COMMAND ----------

df.fillna('Not Available').display()

# COMMAND ----------

df.fillna('Not Available',subset=['Outlet_Size']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #SPLIT and INDEXING

# COMMAND ----------

# MAGIC %md
# MAGIC ##SPLIT

# COMMAND ----------

df.withColumn('Outlet_Type',split(col('Outlet_Type'),' ')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## INDEXING

# COMMAND ----------

df.withColumn('Outlet_Type',split('Outlet_Type',' ')[1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #EXPLODE

# COMMAND ----------

df_explode = df.withColumn('Outlet_Type',split('Outlet_Type',' ')[1]).display()
df_explode.display()

# COMMAND ----------

df_explode.withColumn('Outlet_Type',explode('Outlet_Type')).display()

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumn('Outlet_Type',split('Outlet_Type',' '))

# COMMAND ----------

df.display()

# COMMAND ----------

df_exp = df.withColumn('Outlet_Type', explode('Outlet_Type'))
df_exp.display()


# COMMAND ----------

df.withColumn('Type1_Flag',array_contains('Outlet_Type','Type1')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Group By

# COMMAND ----------

# MAGIC %md
# MAGIC ##Scenario 1

# COMMAND ----------

df.display()

# COMMAND ----------

df.groupBy("Item_Type").agg(sum('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Scenario 2

# COMMAND ----------

df.groupBy('Item_Type').agg(avg("Item_MRP")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Scenario 3

# COMMAND ----------

df.groupBy("Item_Type",'Outlet_Size').agg(sum("Item_MRP").alias('Total_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 4

# COMMAND ----------

df.groupBy('Item_Type','Outlet_Type').agg(avg('Item_MRP'),sum('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Collect List

# COMMAND ----------

data = [('user1', 'book1'),('user1', 'book2'),('user2', 'book1'),('user2', 'book2'),('user3', 'book1')]
schema = 'user string, book string'
df_book = spark.createDataFrame(data,schema)
df_book.display()

# COMMAND ----------

df_book.groupBy('user').agg(collect_list('book')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #PIVOT

# COMMAND ----------

df.groupBy('Item_Type').pivot('Outlet_Size').agg(avg('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #When-Otherwise

# COMMAND ----------

# MAGIC %md
# MAGIC ##Scenario-1

# COMMAND ----------

df = df.withColumn('veg_flag', when(col('Item_Type') == 'Meat', 'Non-Veg').otherwise('Veg'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Scenario-2

# COMMAND ----------

df.withColumn('veg_expensive', when(((col('veg_flag') == 'Veg') & (col('Item_MRP') < 100)), 'Veg_Inexpensive')\
                              .when((col('veg_flag') == 'Veg') & (col('Item_MRP') > 100), 'Veg_Expensive')\
                              .otherwise('Non_Veg')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Joins

# COMMAND ----------

dataj1 = [('1','Vishnu','d01'),('2','Ravi','d02'),('3','Vikram','d03'),('4','Veena','d03'),('5','Rajesh','d05'),('6','Priya','d06')]
schemaj1 = 'id string, name string, dept_id string'
dfj1 = spark.createDataFrame(dataj1,schemaj1)
dataj2 = [('d01','HR'),('d02','Sales'),('d03','Marketing'),('d04','Accounts'),('d05','Finance')]
schemaj2 = 'dept_id string, dept_name string'
dfj2 = spark.createDataFrame(dataj2,schemaj2)

# COMMAND ----------

dfj1.display()

# COMMAND ----------

dfj2.display()

# COMMAND ----------

dfj1.join(dfj2,dfj1['dept_id'] == dfj2['dept_id'],'inner').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## LEFT JOIN

# COMMAND ----------

dfj1.join(dfj2,dfj1['dept_id'] == dfj2['dept_id'],'left').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Right JOIN

# COMMAND ----------

dfj1.join(dfj2,dfj1['dept_id'] == dfj2['dept_id'],'right').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##ANTI JOIN

# COMMAND ----------

dfj1.join(dfj2,dfj1['dept_id'] == dfj2['dept_id'],'anti').display()

# COMMAND ----------

dfj2.join(dfj1, dfj2['dept_id'] == dfj1['dept_id'], 'left_anti').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #WINDOW FUNCTIONS

# COMMAND ----------

# MAGIC %md
# MAGIC ## ROW_Number

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

df.display()

# COMMAND ----------

df.withColumn('rowCol',row_number().over(Window.orderBy('Item_Identifier'))).display()

# COMMAND ----------

df.withColumn('Rank', rank().over(Window.orderBy(col('Item_Identifier').desc()))).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ##Dense_Rank

# COMMAND ----------

df.withColumn('Dense_Rank',dense_rank().over(Window.orderBy(col('Item_Identifier').desc()))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Cumulative Sum

# COMMAND ----------

df.withColumn('cum_sum',sum('Item_MRP').over(Window.orderBy('Item_Type'))).display()

# COMMAND ----------

df.withColumn('cum_sum',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.currentRow))).display()

# COMMAND ----------

df.withColumn('total_sum',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #User Defined Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step1

# COMMAND ----------

def my_function(x):
  return x*x

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 2

# COMMAND ----------

my_udf = udf(my_function)

# COMMAND ----------

df.withColumn('mynewColumn',my_udf('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Data Writing

# COMMAND ----------

# MAGIC %md
# MAGIC ##CSV

# COMMAND ----------

df.write.format('csv')\
        .save('/Volumes/man_cata/martschema/vishnuvolume/data.csv')

# COMMAND ----------

dbutils.fs.ls('/Volumes/man_cata/martschema/vishnuvolume/')

# COMMAND ----------

# MAGIC %md
# MAGIC ##APPEND

# COMMAND ----------

df.write.format('csv').mode('append').save('/Volumes/man_cata/martschema/vishnuvolume/data.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ##OVERWRITE

# COMMAND ----------

df.write.format('csv').mode('overwrite').option('path','/Volumes/man_cata/martschema/vishnuvolume/data.csv').save()

# COMMAND ----------

# MAGIC %md
# MAGIC ##ERROR

# COMMAND ----------

df.write.format('csv').mode('error').option('path','/Volumes/man_cata/martschema/vishnuvolume/data.csv').save()

# COMMAND ----------

# MAGIC %md
# MAGIC ##IGNORE

# COMMAND ----------

df.write.format('csv').mode('ignore').option('path','/Volumes/man_cata/martschema/vishnuvolume/data.csv').save()

# COMMAND ----------

# MAGIC %md
# MAGIC #PARQUET FILE FORMAT

# COMMAND ----------

df.write.format('parquet').mode('overwrite').option('path','/Volumes/man_cata/martschema/vishnuvolume/data.csv').save()

# COMMAND ----------


