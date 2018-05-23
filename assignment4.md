## ????

Once again we would like you to write a blogpost on how you worked on the notebook and what the end result was. Make sure you talk about at least

    What you found easy or difficult about the assignment/the Spark Streaming API
    The questions found in the notebook
    The code you wrote to do some analysis task at the end of the notebook and a brief explanation

Your blog post should posted on your main blog, as before. There is no seperate assignment repository for this assignment.


Q: What factors should you consider when setting the window size?

> The operations are processed in batches. The window size determines how many items are in a batch.


```scala
def parseTransaction (x: String) = {
   def words = x.split("\\s+")
  (words(0),words(1),words(2).toInt)
}
```



```scala
def runes = transactions.filter(_._1 =="rune").map(x=>1).reduce((a,b)=> a+b)
runes
```
> 42

```scala
def numSoldPerType = transactions.map((x) => (x._2, x._3)).reduceByKey((a,b) => (a+b)).cache()
numSoldPerType.toDF().show()
```
> +----------+--------+
> |        _1|      _2|
> +----------+--------+
> | platebody| 4642198|
> |   halberd| 4962105|
> |    gloves| 8608193|
> | longsword| 3454993|
> |      mace| 8807999|
> |     boots| 5191413|
> | battleaxe| 5051397|
> |   pickaxe| 5962298|
> |      helm| 3672317|
> | platelegs| 5402413|
> |plateskirt| 6528786|
> |  scimitar| 3405492|
> | warhammer|11954607|
> |    dagger| 5872939|
> |    shield| 5203866|
> |       axe| 1203987|
> |     claws| 7132943|
> | chainbody| 7379304|
> |     sword| 3262778|
> |     spear| 6910623|
> +----------+--------+

```scala
def goldSpentOnSwords = transactions.filter(_._2 =="sword").map(_._3).reduce((a,b)=>(a+b))
goldSpentOnSwords
```
> 3262778
