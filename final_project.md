# Final Project

The goal of my project is to find the country with the laziest web developers. And in my eyes the laziest of web developers use bootstrap. The metric I will use is therefore the ratio between number of websites that use bootstrap for a country and total number of websites for that country.

There are three steps to this process:
1. Find the number of websites that use bootstrap per country.
2. Find the total number of websites per country.
3. Calculate the ratio (bootstrap / total) per country.

The .scala code is [here](https://raw.githubusercontent.com/rubigdata/bigdata-blog-2018-RElbers/master/final_project/scala/src/main/scala/EntryPoint.scala)

## Helper functions
First we need to parse the WARC file. At this point we can already filter on top level domains (TLD) that belong to countries. Lets define a function that will extract the html content from a WarcRecord. 

```scala
  def extractHtmlContent(rdd: RDD[(LongWritable, WarcRecord)]): RDD[Option[(String, String)]] = {
    rdd
      .filter { case (_, record) => Try(record.getHttpHeader.contentType.contains("text/html")) getOrElse false }
      .map { case (_, record) =>
        val uri = record.header.warcTargetUriStr
        if (isCountry(uri)) {
          Some(uri, IOUtils.toString(record.getPayloadContent))
        } else {
          None
        }
      }
  }
```

We will need to check the header of the html payload for links. In order to do this we first split the payload when we encounter a body tag. That way when we search for links, we don't need to consider the whole payload. Instead of returning an Option[String], we return an empty string if the tag is not found. 

```scala
  def extractHeader(html: String): String = {
    val head = html.split("<body", 2)
    if (head.isEmpty)
      return ""
    head(0)
  }
```

Now that we have the head of the html, we can search for links to stylesheets. The following function  retuns a list of all stylesheets in a string. Of these stylesheets we are only interested in those that link to bootstrap.

```scala
  def stylesheets(string: String): Seq[String] = {
    val regex = "<link rel=('stylesheet'|\"stylesheet\").*\\/>".r
    val matches = regex.findAllIn(string)

    val links = new ListBuffer[String]()
    while (matches.hasNext) {
      links += matches.next()
    }

    links
  }
  
  def isBootstrapLink(stylesheet: String): Boolean = stylesheet.contains("bootstrap.min.css") || stylesheet.contains("bootstrap.css")
```

A single domain may have many resources related to it. If we count naïvely we could overestimate the number of websites. Assume the following websites are using bootstrap:
* mywebsite.nl/my/resource/1
* mywebsite.nl/my/resource/2
* mywebsite.nl/my/resource/3
* mywebsite.nl/my/resource/4
* mywebsite.nl/my/resource/5

If we simply count the number of uri's we may conclude that there are 5 websites that are using bootstrap, while actually there is only 1, namely mywebsite.nl. We will need to define some functions to map the URI's to fully qualified domain names and top level domains.

```scala
  def fullyQualifiedDomain(uri: String): String = {
    val fqdnPart = uri.split("/", 4)

    if (fqdnPart.isEmpty) {
      ""
    } else {
      fqdnPart(2)
    }
  }
  
  def topLevelDomain(fqdn: String): String = fqdn.split("\\.", 0).last
```





  
## 1. Find the number of websites that use bootstrap per country. 

We can now use the functions defined above to find the number of websites that use bootstrap per country. We will collect the results into a map : (tld) -> (count).

```scala
  val warcContent = extractHtmlContent(warcInput)
  val bootstrapPerTLDCount = warcContent
      // Only keep the domains that are countries
      .filter(x => x.isDefined).map(x => x.get)
      // Split the content so we end up with only the <head> element
      .map(x => (x._1, extractHeader(x._2)))
      // From the <head> element, collect all stylesheets and filter so we only keep stylesheets linking to bootstrap
      .map(x => (x._1, stylesheets(x._2).filter(isBootstrapLink))) 
      // Filter so sites without bootstrap are removed
      .filter(x => x._2.nonEmpty)
      // Only keep the URI part
      .map(x=>x._1)
      // Group fully qualified domain names together
      .groupBy(x => fullyQualifiedDomain(x))
      // We only want the keys, every fqdn will count for 1
      .map(x=>x._1)
      // Group by top level domain
      .groupBy(x => topLevelDomain(x))
      // Count per tld
      .map(x => (x._1, x._2.size))
      // The number ot tld's is manageable so we can safely collect as map
      .collectAsMap()
```

  
## 2. Find the total number of websites per country.

Getting the total number of websites per country is pretty trivial now. We still need to consider the grouping of fully qualified domain names. And again we will collect the results into a map : (tld) -> (count).

```scala
    val TLDCount = warcContent
      // Only keep the domains that are countries
      .filter(x => x.isDefined).map(x => x.get)
      // Only keep the URI part
      .map(x=>x._1)
      // Group fully qualified domain names together
      .groupBy(x => fullyQualifiedDomain(x))
      // We only want the keys, every fqdn will count for 1
      .map(x=>x._1)
      // Group by top level domain
      .groupBy(x => topLevelDomain(x))
      // Count per tld
      .map(x => (x._1, x._2.size))
      // The number ot tld's is manageable so we can safely collect as map
      .collectAsMap()
```




## 3. Calculate the ratio (bootstrap / total) per country.

Now that we have the number of sites using bootstrap and the total, we can calculate the ratio. We will first define a function for calculating this ratio. 

```scala
  def getRatio(bootstrapPerTLDCount: collection.Map[String, Int], tldTotal: (String, Int)): Double = {
    val nBootstrap = bootstrapPerTLDCount.getOrElse(tldTotal._1, 0)
    nBootstrap / tldTotal._2.toDouble
  }
```

Next we will need to map the TLD's to proper country names. I have created a python script that parses https://simple.wikipedia.org/wiki/Country_code_top-level_domain and prints the map we need. I will not explain the code here, but the script is [here](https://raw.githubusercontent.com/rubigdata/bigdata-blog-2018-RElbers/master/final_project/tld_to_country.py). The result is the map below:
```scala
  var tldToCountry: Map[String, String] = Map[String, String]()
  tldToCountry += "ac" -> "Ascension Island"
  tldToCountry += "ad" -> "Andorra"
  ...
  tldToCountry += "ελ" -> "Greece"
  tldToCountry += "한국" -> "South Korea"
```

Finally we need to consider the fact that TLD's with too few websites will give an inaccurate result. Therefore we will only check those countries with a count higher then a certain threshold. I choose 100 for that threshold, but it can easily be changed.

```scala
    val minCount = 100
    val bootRatio = TLDCount
      // Only consider countries with more then <minCount> domains
      .filter(x => x._2 >= minCount)
      // Get the ratio of bootstrap users
      .map((x) => (x._1, getRatio(bootstrapPerTLDCount, x)))
      // TLD --> countryname
      .map((x) => (tldToCountry.getOrElse(x._1, ""), x._2))
      // Sort by ratio
      .toList.sortBy(x => x._2)
```

## 4. Results
Below is the table containing with the results. You can see that China has the fewest bootstrap users relatively, while India has the most. The Netherlands is somewhere in the middle. 


| Country | Bootstrap ratio | 
| --- | --- |
| China | 0.008928571428571428 |
| Japan | 0.012180267965895249 |
| Hungary | 0.017543859649122806 |
| Belgium | 0.018518518518518517 |
| Sweden | 0.019867549668874173 |
| Czech Republic | 0.020833333333333332 |
| Germany | 0.021889400921658985 |
| Ukraine | 0.021929824561403508 |
| Spain | 0.023255813953488372 |
| Russia | 0.025439127801332527 |
| Iran | 0.028846153846153848 |
| Netherlands | 0.029914529914529916 |
| Switzerland | 0.032520325203252036 |
| United Kingdom | 0.033566433566433566 |
| France | 0.036231884057971016 |
| Austria | 0.0392156862745098 |
| Canada | 0.04132231404958678 |
| Australia | 0.042704626334519574 |
| Italy | 0.05420054200542006 |
| Poland | 0.059880239520958084 |
| European Union | 0.06451612903225806 |
| Romania | 0.06666666666666667 |
| Brazil | 0.07958477508650519 |
| India | 0.0847457627118644 |

And here is a fancy map:

<script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>
<script type="text/javascript">
google.charts.load('current', {
'packages': ['geochart'],
// Note: you will need to get a mapsApiKey for your project.
// See: https://developers.google.com/chart/interactive/docs/basic_load_libs#load-settings
'mapsApiKey': 'AIzaSyD-9tSrke72PouQMnMX-a7eZSW0jkFMBWY'
});
google.charts.setOnLoadCallback(drawRegionsMap);

function drawRegionsMap() {
var data = google.visualization.arrayToDataTable([
['Country', 'Ratio'],
['Poland', 0.059880239520958084],
['Japan', 0.012180267965895249],
['Spain', 0.023255813953488372],
['Russia', 0.025439127801332527],
['China', 0.008928571428571428],
['Romania', 0.06666666666666667],
['Belgium', 0.018518518518518517],
['Iran', 0.028846153846153848],
['Canada', 0.04132231404958678],
['Ukraine', 0.021929824561403508],
['United Kingdom', 0.033566433566433566],
['Czech Republic', 0.020833333333333332],
['India', 0.0847457627118644],
['Sweden', 0.019867549668874173],
['Brazil', 0.07958477508650519],
['Australia', 0.042704626334519574],
['Austria', 0.0392156862745098],
['Italy', 0.05420054200542006],
['France', 0.036231884057971016],
['Netherlands', 0.029914529914529916],
['Switzerland', 0.032520325203252036],
['European Union', 0.06451612903225806],
['Germany', 0.021889400921658985],
['Hungary', 0.017543859649122806]
]);

var options = {};

var chart = new google.visualization.GeoChart(document.getElementById('regions_div'));

chart.draw(data, options);
}

document.getElementById("main_content").style.maxWidth = "1280px"
document.getElementById("main_content").style.padding = "8px"
</script>

<div id="regions_div" style="width: 1280px; height: 720px;"></div>




