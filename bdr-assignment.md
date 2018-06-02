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
