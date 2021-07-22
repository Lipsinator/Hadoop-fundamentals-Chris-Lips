import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").appName("Linear Regression Model").config("spark.executor.memory", "1gb").getOrCreate()

sc = spark.sparkContext

# Load the data from spark into a dataframe
df = spark.read.format("CSV").option("header", "true").load(".\\titanic.csv")
df = df.withColumn("Survived", df["Survived"].cast(IntegerType())) \
    .withColumn("Pclass", df["Pclass"].cast(IntegerType())) \
    .withColumn("Name", df["Name"].cast(StringType())) \
    .withColumn("Sex", df["Sex"].cast(StringType())) \
    .withColumn("Age", df["Age"].cast(IntegerType())) \
    .withColumn("Siblings/Spouses Aboard", df["Siblings/Spouses Aboard"].cast(IntegerType())) \
    .withColumn("Parents/Children Aboard", df["Parents/Children Aboard"].cast(IntegerType())) \
    .withColumn("Fare", df["Fare"].cast(FloatType()))

# Assignment A:
# Select the fields Survived, passenger class and Sex
df_assignmentA = df.select("Survived", "Pclass", "Sex")
# Group the Sex and passengerclass by survived, this should result in an overview of average survival.
df_assignmentA = df_assignmentA.groupBy("Sex", "Pclass").avg("Survived")
# Print results
df_assignmentA.show()

# Assignment B:
# Select the age passenger class and survived.
df_assignmentB = df.select("Age", "Pclass", "Survived").toPandas()
# Get the results of children under or equal to age 10 and passenger class 3.
df_assignmentB = df_assignmentB[(df_assignmentB['Age'] <= 10) & (df_assignmentB['Pclass'] == 3)][['Age', 'Survived', 'Pclass']]
# Get the count of children that survived.
survived_y = df_assignmentB[df_assignmentB['Survived'] == 1].count()[0]
# Calculate the probability of the survivablity of children on the thirld class. This is done by putting the children numbers next to the overall numbers
# and calculating the percentages.
prob = (survived_y / df_assignmentB.count()[0] * 100).item()
print("probability of survival: " + str(prob))

# Assignment C
# Select the Passenger class and the fares.
dfC = df.select("Pclass", "Fare")
# Group the passengers by class and calculate the average fare.
dfC = dfC.groupBy("Pclass").avg("Fare")
dfC.show()
