## HELLO SPARK

You might think that garbage collection has something to do with memory management, but that's wrong. Garbage collection is what happens when you let a database lose in the wild. 
In order to properly analyse the dataset we first need to remove the garbage.									
													
First of all the floating point numbers are in written with a comma as decimal point seperator, but we need a dot to properly parse it as a float.
So lets write a conversion function and register it as a user-defined function.
Any value that can not be parsed becomes None. We introduce this piece of garbage so we can easily remove it later.

```scala
def convToFloat(s: String): Option[Float] = {
  try {
    Some(nf.parse(s).floatValue)
  } catch {
    case e: Exception => None
  }
}
val tfloat = udf((f: String) => convToFloat(f).getOrElse(0f))		
```

After conversion the None from before is now a 0.0.
We can simply filter those out, since we can't use them for data analysis anyway.
Finally we collect the data into an Addr class and cache it.

```scala
val bagdata = spark.read.format("csv").option("header", true).load("/data/bigdata/BAG_ADRES.csv").cache()
case class Addr(street:String, quarter:String, x:Float, y:Float)
val addrs = bagdata.select($"STRAAT" as "street",
                            tfloat($"X_COORD").cast(FloatType) as "x",
                            tfloat($"Y_COORD").cast(FloatType) as "y",
                            $"WIJK_OMS" as "quarter")
                   .as[Addr]
                   .filter({a: Addr => a.x != 0.0 &&  a.y != 0.0})
                   .cache()
```

Finally we create a sql view from the addrs RDD and use sql to query it.

```scala
addrs.createOrReplaceTempView("addresses")
```scala

As we all know, art is no stranger to garbage. So we will need to clean that up as well.

```scala
val kunst = spark.read
    .format("csv")
    .option("header", "true") // Use first line of all files as header
    .option("inferSchema", "true") // Automatically infer data types
    .load("/data/bigdata/kunstopstraat-kunstwerk.csv").cache()
```
		
First lets remove all the collumns except those we need.
We will only use the latitude, longitude and bouwjaar.
Again we filter out the values that could not be parsed and bouwjaren that are in the future.

```scala
val kunstjaar = kunst.select(tfloat($"bouwjaar") as "jaar",
                             tfloat($"longitude") as "x",
                             tfloat($"latitude") as "y")
                     .as[Kunstwerk]
                     .filter({a: Kunstwerk => a.x != 0.0 &&  a.y != 0.0 && a.jaar <= 2018})
										 .cache()
```































