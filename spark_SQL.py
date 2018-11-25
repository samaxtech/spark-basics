from pyspark.sql import SQLContext

## 2. Register the DataFrame as a Table ##
sqlCtx = SQLContext(sc)
df = sqlCtx.read.json("data/census_2010.json")

df.registerTempTable('census2010')
tables = sqlCtx.tableNames()
print(tables)

## 3. Querying ##

q = "SELECT age FROM census2010"
sqlCtx.sql(q).show()

## 4. Filtering ##

query = 'SELECT males, females FROM census2010 WHERE age > 5 AND age < 15'
sqlCtx.sql(query).show()

## 5. Mixing Functionality ##

q = "SELECT males, females FROM census2010"
sqlCtx.sql(q).describe().show()

## 6. Multiple tables ##
sqlCtx = SQLContext(sc)

df1 = sqlCtx.read.json("data/census_2010.json")
df1.registerTempTable('census2010')

df2 = sqlCtx.read.json("data/census_1980.json")
df2.registerTempTable('census1980')

df3 = sqlCtx.read.json("data/census_1990.json")
df3.registerTempTable('census1990')

df4 = sqlCtx.read.json("data/census_2000.json")
df4.registerTempTable('census2000')

tables = sqlCtx.tableNames()
print(tables)

## 7. Joins ##

q = '''
    SELECT c2.total, c1.total 
    FROM census2000 c1 
    INNER JOIN census2010 c2 
    ON c1.age = c2.age
'''

sqlCtx.sql(q).show()

## 8. SQL Functions ##

q = '''
    SELECT SUM(census2010.total), SUM(census2000.total), SUM(census1990.total) 
    FROM census1990
    INNER JOIN census2000 ON census1990.age = census2000.age
    INNER JOIN census2010 ON census1990.age = census2010.age
'''

sqlCtx.sql(q).show()
    