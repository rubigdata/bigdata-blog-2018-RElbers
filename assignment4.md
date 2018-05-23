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


    How many rune items were sold?

```scala
def runes = transactions.filter(_._1 =="rune").map(x=>1).reduce((a,b)=> a+b)
runes
```

> 42


    How many of each item type was sold?
    
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


    How much gold was spent buying swords?
    
```scala
def goldSpentOnSwords = transactions.filter(_._2 =="sword").map(_._3).reduce((a,b)=>(a+b))
goldSpentOnSwords
```
> 3262778
