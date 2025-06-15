## 1.Python libraries used to execute Spark2 and Cassandra sessions. 


```python
# 1. Import required libraries to execute Spark2 and Cassandra sessions
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


spark = SparkSession.builder \
    .appName("UserDataToCassandra") \
    .config("spark.cassandra.connection.host", "localhost") \
    .getOrCreate()

```

## 2. Function to parse the u.user file into HDFS


```python
# This step is based on the u.user file being uploaded to the HDFS path.hdfs:///user/root/ml-100k/u.user
## 
def parse_user_file_hdfs(path="hdfs:///user/root/ml-100k/u.user"):
    """
    Read the u.user file from HDFS, split by |, and convert it to RDD.
    """
    rdd = spark.sparkContext.textFile(path) \
        .map(lambda line: line.split("|")) \
        .map(lambda fields: (int(fields[0]), int(fields[1]), fields[2], fields[3], fields[4]))
    return rdd

```

## 3. Function to load, read, and create RDD objects


```python

def create_user_rdd():
    """
    Load user data and return RDD.
    """
    return parse_user_file_hdfs()

```

## 4. Convert RDD back to DataFrame using schema


```python

def rdd_to_dataframe(user_rdd):
    """
    User RDD converted to DataFrame.
    """
    schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("age", IntegerType(), True),
        StructField("gender", StringType(), True),
        StructField("occupation", StringType(), True),
        StructField("zip_code", StringType(), True)
    ])
    user_df = spark.createDataFrame(user_rdd, schema=schema)
    return user_df


```

## 5. Function to write the DataFrame into Cassandra keyspace

Tip: First create the required tables in Cassandra, then write the dataframe into PySpark.


```python
## CQL table creation
CREATE KEYSPACE IF NOT EXISTS movielens 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};

CREATE TABLE IF NOT EXISTS movielens.users (
    user_id int PRIMARY KEY,
    age int,
    gender text,
    occupation text,
    zip_code text
);

```


```python
# Function 
def write_to_cassandra(df, keyspace="movielens", table="users"):
    """
    Write the user DataFrame to the Cassandra table.
    """
    df.write \
      .format("org.apache.spark.sql.cassandra") \
      .mode("append") \
      .options(table=table, keyspace=keyspace) \
      .save()

```

## 6. Function to read the table from Cassandra back into a new DataFrame



```python

def read_from_cassandra(keyspace="movielens", table="users"):
    """
    Read the table from Cassandra and return a DataFrame.
    """
    df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=table, keyspace=keyspace) \
        .load()
    return df

```

## Conduct query analysis


```python
## i) Calculate the average rating for each movie
from pyspark.sql.functions import avg

avg_rating_df = ratings_df.groupBy("movie_id").agg(avg("rating").alias("avg_rating"))
avg_rating_df.show(10)
```


```python
+--------+------------------+
|movie_id|        avg_rating|
+--------+------------------+
|     496| 4.121212121212121|
|     471|3.6108597285067874|
|     463| 3.859154929577465|
|     148|          3.203125|
|    1342|               2.5|
|     833| 3.204081632653061|
|    1088| 2.230769230769231|
|    1591|3.1666666666666665|
|    1238|             3.125|
|    1580|               1.0|
+--------+------------------+
```


```python
##  ii) Identify the top ten movies with the highest average ratings
# First calculate the average ratings, then combine the movie names to select the top ten.
top_10_movies = avg_rating_df.join(movies_df, on="movie_id") \
                              .select("movie_id", "title", "avg_rating") \
                              .orderBy("avg_rating", ascending=False) \
                              .limit(10)
top_10_movies.show(truncate=False)

```


```python
+--------+-------------------------------------------------+----------+
|movie_id|title                                            |avg_rating|
+--------+-------------------------------------------------+----------+
|1599    |Someone Else's America (1995)                    |5.0       |
|1293    |Star Kid (1997)                                  |5.0       |
|1653    |Entertaining Angels: The Dorothy Day Story (1996)|5.0       |
|1201    |Marlene Dietrich: Shadow and Light (1996)        |5.0       |
|1189    |Prefontaine (1997)                               |5.0       |
|1467    |Saint of Fort Washington, The (1993)             |5.0       |
|1122    |They Made Me a Criminal (1939)                   |5.0       |
|1500    |Santa with Muscles (1996)                        |5.0       |
|1536    |Aiqing wansui (1994)                             |5.0       |
|814     |Great Day in Harlem, A (1994)                    |5.0       |
+--------+-------------------------------------------------+----------+
```


```python
##iii) Find users who have rated at least 50 movies and identify their favourite movie genres
from pyspark.sql.functions import count, desc

# Step 1: Count the number of ratings for each user
active_users = ratings_df.groupBy("user_id") \
                         .agg(count("movie_id").alias("rating_count")) \
                         .filter("rating_count >= 50")

# Step 2: Expand the movie and genre into a long format.
from pyspark.sql.functions import lit

genre_columns = ["unknown", "Action", "Adventure", "Animation", "Children's", "Comedy", "Crime",
                 "Documentary", "Drama", "Fantasy", "Film-Noir", "Horror", "Musical",
                 "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western"]

movie_genre_pairs = []
for genre in genre_columns:
    movie_genre_pairs.append(
        movies_df.select("movie_id")
        .where(movies_df[genre] == 1)
        .withColumn("genre", lit(genre))
    )
movie_genre_df = movie_genre_pairs[0]
for df in movie_genre_pairs[1:]:
    movie_genre_df = movie_genre_df.union(df)

# Step 3: Connect the rating data and the user table
user_genre_df = ratings_df.join(active_users, "user_id") \
                          .join(movie_genre_df, "movie_id")

# Step 4: Count each user's favorite movie genre (based on the genre with the most ratings)
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

genre_count = user_genre_df.groupBy("user_id", "genre") \
                           .agg(count("rating").alias("genre_rating_count"))

window_spec = Window.partitionBy("user_id").orderBy(desc("genre_rating_count"))

favourite_genres = genre_count.withColumn("rank", row_number().over(window_spec)) \
                              .filter("rank = 1")

favourite_genres.show(10)

```


```python
+-------+------+------------------+----+
|user_id| genre|genre_rating_count|rank|
+-------+------+------------------+----+
|    148| Drama|                25|   1|
|    463| Drama|                59|   1|
|    496| Drama|                52|   1|
|    833| Drama|                97|   1|
|    243| Drama|                59|   1|
|    392| Drama|                57|   1|
|    540| Drama|                28|   1|
|    897|Action|                57|   1|
|     85| Drama|               152|   1|
|    251|Action|                31|   1|
+-------+------+------------------+----+
only showing top 10 rows

```


```python
##  iv) Find all the users who are less than 20 years old
users_df_from_cassandra.filter("age < 20") \
     .orderBy("age") \
     .limit(10) \
     .show()

```


```python
+-------+---+------+----------+--------+
|user_id|age|gender|occupation|zip_code|
+-------+---+------+----------+--------+
|     30|  7|     M|   student|   55436|
|    471| 10|     M|   student|   77459|
|    289| 11|     M|      none|   94619|
|    609| 13|     F|   student|   55106|
|    880| 13|     M|   student|   83702|
|    674| 13|     F|   student|   55337|
|    142| 13|     M|     other|   48118|
|    628| 13|     M|      none|   94306|
|    206| 14|     F|   student|   53115|
|    887| 14|     F|   student|   27249|
+-------+---+------+----------+--------+
```


```python
##  v) Find all the users whose occupation is “scientist” and whose age is between 30 and 40 years old

users_df_from_cassandra.filter("occupation = 'scientist' AND age BETWEEN 30 AND 40") \
     .orderBy("age") \
     .limit(10) \
     .show()

```


```python
+-------+---+------+----------+--------+
|user_id|age|gender|occupation|zip_code|
+-------+---+------+----------+--------+
|    730| 31|     F| scientist|   32114|
|    538| 31|     M| scientist|   21010|
|    554| 32|     M| scientist|   62901|
|    543| 33|     M| scientist|   95123|
|    183| 33|     M| scientist|   27708|
|    272| 33|     M| scientist|   53706|
|    874| 36|     M| scientist|   37076|
|    337| 37|     M| scientist|   10522|
|     40| 38|     M| scientist|   27514|
|    430| 38|     M| scientist|   98199|
+-------+---+------+----------+--------+

```
