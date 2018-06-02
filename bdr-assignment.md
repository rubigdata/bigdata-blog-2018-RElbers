## Hello World


Convert all ones in the "ecom_action" column to a 2
Convert the 3's and 5's to 4's
```scala
clicks_val = clicks_val.withColumn("ecom_action", when(col("ecom_action") === 1, 2).otherwise(col("ecom_action")))
                       .withColumn("ecom_action", when(col("ecom_action") === 3, 4).otherwise(col("ecom_action")))
                       .withColumn("ecom_action", when(col("ecom_action") === 5, 4).otherwise(col("ecom_action")))
```

Try to come up with a data-driven reasoning for choosing the ratio between clicks and applications
```scala
// Scale the weights relative to finishing an application.
// By the way I find it weird that finishing an application is 5 times higher then starting an application.
// See below after executing: clicks_val.groupBy("ecom_action").count().show()
/*
+-----------+-----+
|ecom_action|count|
+-----------+-----+
|          6|20186|
|          4| 4246|
|          2|32104|
+-----------+-----+
*/
```


Perform ALS on the clicks_train dataset
Check whether to use implicit or explicit matrix factorization
Look into the possible hyperparameters of the ALS function
```scala
val als = new ALS()
  .setMaxIter(5)
  .setRegParam(0.01)
  .setImplicitPrefs(true)
  .setUserCol("candidate_number")
  .setItemCol("vacancy_number")
  .setRatingCol("ecom_action")
val model = als.fit(clicks_train)
```





Perform ALS on the function_name (item col) and candidate_number (user col)
ALS requires its inputs to be integers. Since the function_name is a string, you first need to convert this to an integer index
There can be multiple rows for a single candidate/function_name pair. Make sure to aggregate the ratings (e.g. by summing the "ecom_action" column)

```scala
val indexer = new StringIndexer()
  .setInputCol("function_name")
  .setOutputCol("function_index")
  .fit(clicks_train)

clicks_train = indexer.transform(clicks_train)
clicks_train = clicks_train.groupBy("candidate_number","function_index").agg(sum("ecom_action") as "ecom_action")

val als = new ALS()
  .setMaxIter(5)
  .setRegParam(0.01)
  .setImplicitPrefs(true)
  .setUserCol("candidate_number")
  .setItemCol("function_index")
  .setRatingCol("ecom_action")
```









