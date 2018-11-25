# Import SQLContext
from pyspark.sql import SQLContext

## 1. The Spark DataFrame: An Introduction ##

with open('data/census_2010.json') as f:
    for i in range(0,4):
        print(f.readline())

## 2. Reading in Data ##
# Pass in the SparkContext object `sc`
sqlCtx = SQLContext(sc)

# Read JSON data into a DataFrame object `df`
df = sqlCtx.read.json("census_2010.json")

# Print the type
print(df)

## 3. Schema ##

sqlCtx = SQLContext(sc)
df = sqlCtx.read.json("census_2010.json")
df.printSchema()

## 4. Pandas vs Spark DataFrames ##

df.show()

## 5. Row Objects ##

first_five = df.head(5)

for x in first_five:
    print(x.age)

## 6. Selecting Columns ##

df[['age', 'males', 'females']].show()

## 7. Filtering Rows ##

five_plus = df[df['age'] > 5]
five_plus.show()

## 8. Using Column Comparisons as Filters ##

df[df['females'] < df['males']].show()