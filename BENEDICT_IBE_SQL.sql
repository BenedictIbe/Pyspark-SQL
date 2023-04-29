-- Databricks notebook source
-- MAGIC %python
-- MAGIC pip install seaborn

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Number 1 answer

-- COMMAND ----------

-- DBTITLE 1,Importing the required libraries
-- MAGIC %python
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC import pandas as pd
-- MAGIC import seaborn as sns
-- MAGIC import numpy as np

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("/FileStore/tables/")

-- COMMAND ----------

-- DBTITLE 1,Defining the file you want to analyse
-- MAGIC %python
-- MAGIC file =  'clinicaltrial_2021'

-- COMMAND ----------

-- DBTITLE 1,Defining the pharm file you want to analyse
-- MAGIC %python
-- MAGIC file2 = 'pharma'

-- COMMAND ----------

-- DBTITLE 1,Defining databricks base path - to make the codes shorter
-- MAGIC %python
-- MAGIC mainpath = '/FileStore/tables/'

-- COMMAND ----------

-- DBTITLE 1,Clean up the DBFS of any previous pharma csv
-- MAGIC %python
-- MAGIC dbutils.fs.rm(mainpath + file2 + '.csv', True)

-- COMMAND ----------

-- DBTITLE 1,Clean up the DBFS of any previous clinical trial csv
-- MAGIC %python
-- MAGIC dbutils.fs.rm(mainpath + file + '.csv', True)

-- COMMAND ----------

-- DBTITLE 1,Since it is a zipped file, copy it into a temp file to unzip it there
-- MAGIC %python
-- MAGIC dbutils.fs.cp(mainpath + file + '.zip', 'file:/tmp/')

-- COMMAND ----------

-- DBTITLE 1,Since it is a zipped file, copy the pharma into a temp file to unzip it there
-- MAGIC %python
-- MAGIC dbutils.fs.cp(mainpath + file2 + '.zip', 'file:/tmp/')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import os
-- MAGIC os.environ['file'] = file

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import os
-- MAGIC os.environ['file2'] = file2

-- COMMAND ----------

-- DBTITLE 1,Unzip the file(s) in the temp folder
-- MAGIC %sh
-- MAGIC unzip -d /tmp/ /tmp/$file

-- COMMAND ----------

-- DBTITLE 1,Unzip the pharma in the temp folder
-- MAGIC %sh
-- MAGIC unzip -d /tmp/ /tmp/$file2

-- COMMAND ----------

-- DBTITLE 1,Move the unzipped csv from the temporary folder into the newly created folder
-- MAGIC %python
-- MAGIC dbutils.fs.mv('file:/tmp/' + file + '.csv', mainpath, True)

-- COMMAND ----------

-- DBTITLE 1,Move the unzipped pharma csv from the temporary folder into the newly created folder
-- MAGIC %python
-- MAGIC dbutils.fs.mv('file:/tmp/' + file2 + '.csv', mainpath, True)

-- COMMAND ----------

-- DBTITLE 1,View the contents of the csv
-- MAGIC %python
-- MAGIC dbutils.fs.head(mainpath + file + '.csv')

-- COMMAND ----------

-- DBTITLE 1,Create the dataframe using the spark read options
-- MAGIC %python
-- MAGIC clinicSQL = spark.read.options(delimiter ="|").csv(mainpath + file + '.csv', header = True)
-- MAGIC clinicSQL.show(5, truncate = False)

-- COMMAND ----------

-- DBTITLE 1,Import the date and date format functions
-- MAGIC %python
-- MAGIC from pyspark.sql.functions import to_date, date_format

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC from pyspark.sql.functions import split, col

-- COMMAND ----------

-- DBTITLE 1,Convert the completion column to an actual date format
-- MAGIC %python
-- MAGIC clinicSQL2 = clinicSQL.withColumn('Completion', to_date('Completion', 'MMM yyyy'))
-- MAGIC clinicSQL2.show()

-- COMMAND ----------

-- DBTITLE 1,Create the first temporary view
-- MAGIC %python
-- MAGIC clinicSQL2.createOrReplaceTempView('clinicaltrial')

-- COMMAND ----------

-- DBTITLE 1,View the contents of the temporary view
select * from clinicaltrial


-- COMMAND ----------

-- DBTITLE 1,Count the distinct studies
select count(distinct(Id)) from clinicaltrial

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Number 2 answer

-- COMMAND ----------

-- DBTITLE 1,Group by the type of studies and order by the frequency in descending order
select Type, count(Id) as frequency from clinicaltrial
group by Type
order by count(Id) desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Number 3 answer

-- COMMAND ----------

-- DBTITLE 1,Getting the aggregate count for each disease after splitting  and exploding the conditions column
SELECT expl_conditions, COUNT(*) as frequency
FROM 
(select Id, explode(split_conditions)  as expl_conditions
from
(select Id, split(Conditions, ',') as split_conditions 
from clinicaltrial
where Conditions  != 'Null'))
GROUP BY expl_conditions
ORDER BY COUNT(*) DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Number 4 answer

-- COMMAND ----------

-- DBTITLE 1,Create the pharma dataframe
-- MAGIC %python
-- MAGIC pharmSQL = spark.read.options(delimiter =",").csv(mainpath + file2 + '.csv', header = True)
-- MAGIC pharmSQL.show(5, truncate = False)

-- COMMAND ----------

-- DBTITLE 1,Importing the required library
-- MAGIC %python
-- MAGIC from pyspark.sql.types import *

-- COMMAND ----------

-- DBTITLE 1,Creating the pharma temporary view
-- MAGIC %python
-- MAGIC pharmSQL.createOrReplaceTempView(file2)

-- COMMAND ----------

select * from pharma

-- COMMAND ----------

-- DBTITLE 1,Using the left outer join function to extract the non pharmaceutical companies
SELECT Sponsor, COUNT(*) as frequency
FROM 
(select c.Sponsor, p.Parent_Company
from clinicaltrial c
left outer join 
(select Parent_Company,Penalty_Amount, Penalty_Amount_Adjusted_For_Eliminating_Multiple_Counting as Penalty_adjusted,
Penalty_Year, Penalty_Date, Offense_Group, Primary_Offense, Secondary_Offense
from pharma) p
on c.Sponsor = p.Parent_Company
where p.Parent_Company is Null)
GROUP BY Sponsor
ORDER BY COUNT(*) DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Number 5 answer

-- COMMAND ----------

-- DBTITLE 1,Using the Case function to assign a value to each month so that they can be sorted by the assigned value
select month_name, frequency from
(SELECT 
CASE 
    WHEN DATE_FORMAT(Completion, 'MMM') = 'Jan' THEN 1
    WHEN DATE_FORMAT(Completion, 'MMM') = 'Feb' THEN 2 
    WHEN DATE_FORMAT(Completion, 'MMM') = 'Mar' THEN 3 
    WHEN DATE_FORMAT(Completion, 'MMM') = 'Apr' THEN 4 
    WHEN DATE_FORMAT(Completion, 'MMM') = 'May' THEN 5
    WHEN DATE_FORMAT(Completion, 'MMM') = 'Jun' THEN 6 
    WHEN DATE_FORMAT(Completion, 'MMM') = 'Jul' THEN 7 
    WHEN DATE_FORMAT(Completion, 'MMM') = 'Aug' THEN 8 
    WHEN DATE_FORMAT(Completion, 'MMM') = 'Sep' THEN 9 
    WHEN DATE_FORMAT(Completion, 'MMM') = 'Oct' THEN 10 
    WHEN DATE_FORMAT(Completion, 'MMM') = 'Nov' THEN 11 
    WHEN DATE_FORMAT(Completion, 'MMM') = 'Dec' THEN 12 
  END as month_num, 
  DATE_FORMAT(Completion, 'MMM') as month_name, COUNT(*) as frequency
FROM (select Status, Completion from clinicaltrial
where Status = 'Completed' and Completion like('%2021%'))
GROUP BY month_name
ORDER BY  month_num ASC)


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Further analysis

-- COMMAND ----------

-- DBTITLE 1,Problem statement
-- MAGIC %md
-- MAGIC Suppose we want to get the pharmaceutical companies who have spent the most on penalties

-- COMMAND ----------

select Parent_Company, SUM(PenaltyAmount) from
(select Parent_Company, CAST(REPLACE(PenaltyAmount,',', '') AS INT) as PenaltyAmount, 
CAST(REPLACE(PenaltyAdjust,',', '') AS INT) as PenaltyAdjust, Penalty_Year, Offense_Group, 
Primary_Offense, Secondary_Offense 
from 
(SELECT Parent_Company, REPLACE(Penalty_Amount,'$', '') AS PenaltyAmount,
REPLACE(Penalty_Amount_Adjusted_For_Eliminating_Multiple_Counting,'$', '') AS PenaltyAdjust,
Penalty_Year, Offense_Group, Primary_Offense, Secondary_Offense
FROM pharma)
where PenaltyAmount is not Null)
group by Parent_Company
order by SUM(PenaltyAmount)  desc
limit 10

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pandas as pd

-- COMMAND ----------

-- DBTITLE 1,Converting the result table code to pandas dataframe
-- MAGIC %python
-- MAGIC df = spark.sql("""
-- MAGIC     select Parent_Company, SUM(PenaltyAmount) from
-- MAGIC (select Parent_Company, CAST(REPLACE(PenaltyAmount,',', '') AS INT) as PenaltyAmount, 
-- MAGIC CAST(REPLACE(PenaltyAdjust,',', '') AS INT) as PenaltyAdjust, Penalty_Year, Offense_Group, 
-- MAGIC Primary_Offense, Secondary_Offense 
-- MAGIC from 
-- MAGIC (SELECT Parent_Company, REPLACE(Penalty_Amount,'$', '') AS PenaltyAmount,
-- MAGIC REPLACE(Penalty_Amount_Adjusted_For_Eliminating_Multiple_Counting,'$', '') AS PenaltyAdjust,
-- MAGIC Penalty_Year, Offense_Group, Primary_Offense, Secondary_Offense
-- MAGIC FROM pharma)
-- MAGIC where PenaltyAmount is not Null)
-- MAGIC group by Parent_Company
-- MAGIC order by SUM(PenaltyAmount)  desc
-- MAGIC limit 10
-- MAGIC """)
-- MAGIC df1 = df.toPandas()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df1

-- COMMAND ----------

-- DBTITLE 1,BarPlot of the pharmaceutical companies 
-- MAGIC %python
-- MAGIC df1.plot(kind="bar", x="Parent_Company", y="sum(PenaltyAmount)")
-- MAGIC plt.show()

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC rm -r  /FileStore/tables/clinicaltrialFiles
-- MAGIC rm    /FileStore/tables/clinicaltrialFiles

-- COMMAND ----------

-- DBTITLE 1,Clean the Databricks folder to be ready for another rerun
-- MAGIC %python
-- MAGIC dbutils.fs.rm(mainpath + file + '.csv', True)

-- COMMAND ----------

-- DBTITLE 1,Clean the Databricks folder to be ready for another rerun
-- MAGIC %python
-- MAGIC dbutils.fs.rm(mainpath + file2 + '.csv', True)

-- COMMAND ----------


