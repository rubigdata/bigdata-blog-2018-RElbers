## HELLO SPARK

You might think that garbage collection has something to do with memory management, but that's wrong. Garbage collection is what happens when you let a database loose in the wild. 
In order to properly analyse the dataset we first need to remove the garbage.		
													
First of all the floating point numbers are in written with a comma as decimal point seperator, but we need a dot to properly parse it as a float.
So lets write a conversion function and register it as a user-defined function.
Any value that can not be parsed becomes None. We introduce this piece of garbage so we can easily remove it later.
It turns out that the Netherlands are special enough to have their own coordinate system, so we will need to convert those to longitude-latitude. Instead of the sql library we will use a standard map. See the CT class containing the transformXY function at the end of this blog.

```scala
def convToFloat(s: String): Option[Float] = {
  try {
    Some(nf.parse(s).floatValue)
  } catch {
    case e: Exception => None
  }
}
val tofloat = udf((f: String) => convToFloat(f).getOrElse(0f))		
```

After conversion the None from before is now a 0.0, as are any nulls or strings in the original dataset.
We can simply filter those zeroes out, since we can't use them for data analysis anyway.
Finally we collect the data into an Addr class and cache it.

```scala

case class Addr(street:String, quarter:String, x:Float, y:Float)

val bagdata = spark.read.format("csv").option("header", true).load("/data/bigdata/BAG_ADRES.csv").cache()
val addrs = bagdata.select($"STRAAT" as "street",
                            tfloat($"X_COORD").cast(FloatType) as "x",
                            tfloat($"Y_COORD").cast(FloatType) as "y",
                            $"WIJK_OMS" as "quarter")
                  .as[Addr]
                  .filter({a: Addr => 
                           a.x != 0.0 &&  
                           a.y != 0.0
                           })
                  .map((a:Addr) => new Addr(a.street, 
                                    a.quarter, 
                                    CT.transformXY(a.x, a.y)._1, 
                                    CT.transformXY(a.x, a.y)._2))
                  .cache()
```


As we all know, art is no stranger to garbage. So we will need to clean that up as well.
We will remove all the collumns except those we need.
This time we will just replace the comma's with dots for the coordinates.
Null values will be filtered out as well

```scala
case class Kunstwerk(naam:String, jaar:Float, x:Float, y:Float)

val kunst = spark.read
    .format("csv")
    .option("header", "true") // Use first line of all files as header
    .option("inferSchema", "true") // Automatically infer data types
    .load("/data/bigdata/kunstopstraat-kunstwerk.csv").cache()
    
val kunstjaar = kunst.select("naam", "longitude", "latitude", "bouwjaar")
                    .filter($"longitude".isNotNull)
                    .filter($"latitude".isNotNull)
                    .filter($"bouwjaar".isNotNull)
                    .withColumn("latitude", translate(ks.col("latitude"),",",".").cast("float"))
                    .withColumn("longitude", translate(ks.col("longitude"),",",".").cast("float"))
                    .withColumn("bouwjaar",col("bouwjaar").cast("int"))
                    .select("naam","latitude","longitude","bouwjaar")
                    .as[Kunstwerk]
                    .cache()
```

Now we can create tables and qeury them using sql like ~~dirty database engineers~~ proper data scientists.

```scala
addrs.createOrReplaceTempView("addresses")
kunstjaar.createOrReplaceTempView("artworks")
```scala

We can now group together all the artworks by name and quarter that are near a location.
```scala
val kosquarter = spark.sql(
  "select distinct naam, quarter, min(jaar) as jaar " + 
  "from artworks l, addresses r " +
  "where abs(l.x - r.x) < 10.0 and " +
  "      abs(l.y - r.y) < 10.0 " +
  "group by naam, quarter "
).cache()
```


## Can you produce the list of quarters that is missing because no artwork has been situated in the quarter?
Because where WHERE ... NOT IN does not seem to not work properly, we will need to use a left outer join to pair addresses with artworks.
```scala
// This does not work:
// spark.sql("SELECT DISTINCT quarter FROM addresses WHERE quarter NOT IN (SELECT DISTINCT k.quarter FROM kosquarter k)").show()

// Quarters that are NULL on addresses side have no partner on kosquarter side.
spark.sql("SELECT DISTINCT a.quarter " +
          "FROM addresses a LEFT OUTER JOIN kosquarter k " +
          "ON a.quarter = k.quarter "+
          "WHERE k.quarter IS NULL ").show()
```

					
## Can you produce a longer list of quarters and their oldest artworks?
We can increase the coordinate margins to 20.0 to get more quarters
```scala
val ksq = spark.sql(
  "select distinct naam, quarter, min(bouwjaar) as jaar " +
  "from kosxy , addresses " +
  "where abs(XY._1 - x) < 20.0 and abs(XY._2 - y) < 20.0 " +
  "group by naam, quarter "
).cache()

ksq.createOrReplaceTempView("ksq")
spark.sql("select distinct quarter, min(jaar) as jaar from ksq group by quarter order by jaar").show(false)
```

## What are the years associated to artworks not yet matched up with the addresses database? What does this mean for our initial research question?
It seems that the artworks that are not matched up with the addresses database are built around 1973.
```scala
// The original query to get art by quarter.
val tmp = spark.sql(
  "select distinct naam, quarter, min(bouwjaar) as jaar " +
  "from kosxy , addresses " +
  "where abs(XY._1 - x) < 10.0 and abs(XY._2 - y) < 10.0 " +
  "group by naam, quarter "
).cache()
tmp.createOrReplaceTempView("tmp")

// Similar to the query before, checking what artworks are not in quarters.
val sql = spark.sql("SELECT bouwjaar " +
                  "FROM kos k LEFT OUTER JOIN tmp z " +
                  "ON k.naam == z.naam " +
                  "WHERE z.naam IS NULL ")
		  
sql.describe().show()
+-------+-----------------+
|summary|         bouwjaar|
+-------+-----------------+
|  count|              189|
|   mean|1973.132275132275|
| stddev|41.76744718395329|
|    min|             1645|
|    max|             2017|
+-------+-----------------+
```


## Bonus content
```scala
object CT extends Serializable {
  
  import org.cts.CRSFactory;
  import org.cts.crs.GeodeticCRS;
  import org.cts.registry.EPSGRegistry;
  import org.cts.op.CoordinateOperationFactory;
  import org.cts.op.CoordinateOperation;

  // global variables to keep state for transformations
  @transient private var xy2latlonOp : CoordinateOperation = null;
  @transient private var latlon2xyOp : CoordinateOperation = null;

  // Create the coordinate transformation functions to convert from RD New to WGS:84 and vice versa
  def initTransforms : (CoordinateOperation, CoordinateOperation) = {
    // Create a new CRSFactory, a necessary element to create a CRS without defining one by one all its components
    val cRSFactory = new CRSFactory();

    // Add the appropriate registry to the CRSFactory's registry manager. Here the EPSG registry is used.
    val registryManager = cRSFactory.getRegistryManager();
    registryManager.addRegistry(new EPSGRegistry());

    // CTS will read the EPSG registry seeking the 4326 code, when it finds it,
    // it will create a CoordinateReferenceSystem using the parameters found in the registry.
    val crs1 : GeodeticCRS = (cRSFactory.getCRS("EPSG:28992")).asInstanceOf[GeodeticCRS];
    val crs2 : GeodeticCRS = (cRSFactory.getCRS("EPSG:4326") ).asInstanceOf[GeodeticCRS];
    
    // Transformation (x,y) -> (lon,lat)
    val xy2latlonOps = CoordinateOperationFactory.createCoordinateOperations(crs1,crs2);
    val xy2latlon = xy2latlonOps.get(0);
    
    val latlon2xyOps = CoordinateOperationFactory.createCoordinateOperations(crs2,crs1);
    val latlon2xy = latlon2xyOps.get(0);
    
    (xy2latlon, latlon2xy)
  }

  // Encapsulate private transient variable (for serializability of the object)
  def getXYOp : CoordinateOperation = {
    if (xy2latlonOp == null){
      val ts = initTransforms
      xy2latlonOp = ts._1
      latlon2xyOp = ts._2
    }
    xy2latlonOp
  }

  // Encapsulate private transient variable (for serializability of the object)
  def getLatLonOp : CoordinateOperation = {
    if (latlon2xyOp == null){
      val ts = initTransforms
      xy2latlonOp = ts._1
      latlon2xyOp = ts._2
    }
    latlon2xyOp
  }
  
  // Use the library's transformation function to convert the coordinates
  def transformXY(x:Float, y:Float) : (Float, Float) = {   
    // Note: easily confused, (lat,lon) <-> (y,x)
    val lonlat = this.getXYOp.transform(Array(x.toDouble, y.toDouble));
    return ( lonlat(1).toFloat, lonlat(0).toFloat)
  }
  
  // Use the library's transformation function to convert the coordinates
  def transformLatLon(lat:Float, lon:Float) : (Float, Float) = {
    // Note: easily confused, (lat,lon) <-> (y,x)
    val xy = this.getLatLonOp.transform(Array(lon.toDouble, lat.toDouble));
    return ( xy(0).toFloat, xy(1).toFloat)
  }
}

```





















