## ????

Once again we would like you to write a blogpost on how you worked on the notebook and what the end result was. Make sure you talk about at least

    What you found easy or difficult about the assignment/the Spark Streaming API
    The questions found in the notebook
    The code you wrote to do some analysis task at the end of the notebook and a brief explanation

Your blog post should posted on your main blog, as before. There is no seperate assignment repository for this assignment.


## Q: What factors should you consider when setting the window size?

> The operations are processed in batches. The window size determines how many items are in a batch. The choice of window size depends on how much memory you are able to use and how fast you want to have results. A smaller batch will have less latency.


```scala
def parseTransaction (x: String) = {
   def words = x.split("\\s+")
  (words(0),words(1),words(5).toInt)
}
```


## How many rune items were sold?

```scala
def runes = transactions.filter(_._1 =="rune").map(x=>1).reduce((a,b)=> a+b)
runes
```

> 42


## How many of each item type was sold?
    
```scala
def numSoldPerType = transactions.map((x) => (x._2, x._3)).reduceByKey((a,b) => (a+b)).cache()
numSoldPerType.toDF().show()
```
> +----------+--------+ <br />
> |        _1|      _2| <br />
> +----------+--------+ <br />
> | platebody| 4642198| <br />
> |   halberd| 4962105| <br />
> |    gloves| 8608193| <br />
> | longsword| 3454993| <br />
> |      mace| 8807999| <br />
> |     boots| 5191413| <br />
> | battleaxe| 5051397| <br />
> |   pickaxe| 5962298| <br />
> |      helm| 3672317| <br />
> | platelegs| 5402413| <br />
> |plateskirt| 6528786| <br />
> |  scimitar| 3405492| <br />
> | warhammer|11954607| <br />
> |    dagger| 5872939| <br />
> |    shield| 5203866| <br />
> |       axe| 1203987| <br />
> |     claws| 7132943| <br />
> | chainbody| 7379304| <br />
> |     sword| 3262778| <br />
> |     spear| 6910623| <br />
> +----------+--------+ <br />


## How much gold was spent buying swords?
    
```scala
def goldSpentOnSwords = transactions.filter(_._2 =="sword").map(_._3).reduce((a,b)=>(a+b))
goldSpentOnSwords
```
> 3262778


## For a 10 second window, give the top 3 materials by total amount of gold spent.

It's quite annoying I have to restart the kernel every time I change the computation graph :(
First we map to get the material and gold. Then we can reduce by key and window to sum the gold spent per material for a 10 second window
We can't just sort the stream, but we need to sort every batch independently. To do this we can call transform to use the underlying rdd. After sorting we take the top 3. Take returns an array and not an rdd and transform needs a function RDD -> RDD. So we parallelize the array. Finally we print the top 3.


```scala
import org.apache.spark.streaming._
val sc2 = new StreamingContext(sparkContext, Seconds(5))
val textStream2 = sc2.socketTextStream("146.185.183.168", 9999)
```

```scala
def parseTransaction (x: String) = {
   def words = x.split("\\s+")
  (words(0),words(1),words(5).toInt)
}

def pairs = textStream2.map(parseTransaction)
                   .map((x) => (x._1, x._3))
def goldPerMaterial = pairs.reduceByKeyAndWindow((a:Int,b:Int) => (a+b), Seconds(10), Seconds(5))
def top3Materials = goldPerMaterial.transform({ rdd =>  
    def sorted = rdd.sortBy(_._2, ascending=false)
    sc.parallelize(sorted.take(3))
})

top3Materials.print()
```

```scala
sc2.start()
sc2.awaitTerminationOrTimeout(20000)
```

>   -------------------------------------------
>   Time: 1527108215000 ms
>   -------------------------------------------
>   (rune,10958377)
>   (dragon,9739721)
>   (bronze,9035991)
 
>   -------------------------------------------
>   Time: 1527108220000 ms
>   -------------------------------------------
>   (dragon,29694332)
>   (steel,27679571)
>   (rune,25050273)
 
>   -------------------------------------------
>   Time: 1527108225000 ms
>   -------------------------------------------
>   (dragon,40609320)
>   (steel,39246118)
>   (iron,38885733)
 
>   -------------------------------------------
>   Time: 1527108230000 ms
>   -------------------------------------------
>   (rune,39469261)
>   (dragon,39331531)
>   (mithril,39312646)
 
>   res4: Boolean = false
