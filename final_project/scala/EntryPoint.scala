import java.io.{File, PrintWriter}

import org.apache.commons.io.IOUtils
import org.apache.hadoop.io.LongWritable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.jwat.warc.WarcRecord
import warcutils.WarcInputFormat

import scala.collection.mutable.ListBuffer
import scala.util.Try

object EntryPoint {
  def main(args: Array[String]): Unit = {
    val master = "local"

    val outfile = "results.txt"
//    val infile = "/data/public/common-crawl/crawl-data/CC-MAIN-2016-07/segments/*/warc/*.warc.gz"
     val infile = "rsc/CC-MAIN-20170622114718-20170622134718-00000.warc"
    // val infile = "rsc/CC-MAIN-20170622114718-20170622134718-00000_000001.warc"
    // val infile = "rsc/CC-MAIN-20170622114718-20170622134718-00000_000003.warc"

    val spark = SparkSession.builder.
      master(master)
      .appName("rubd08_final_project")
      .config("spark.app.id", "rubd08")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.dynamicAllocation.enabled", "true")
      .config("spark.shuffle.service.enabled", "true")
      .config("spark.dynamicAllocation.minExecutors", "50")
      .config("spark.dynamicAllocation.maxExecutors", "300")
      .getOrCreate()

    try {
      val result = start(infile, spark.sparkContext)
      val resultString = result.map(x => s""""${x._1}\", \"${x._2}"""").mkString("\n")

      // Print results
      println("\n\n\n" + resultString + "\n\n\n")

      // Save results to file
      val pw = new PrintWriter(new File(outfile))
      pw.write(resultString)
      pw.close()

    } finally {
      spark.stop()
    }
  }

  private def start(infile: String, sc: SparkContext): Seq[(String, Double)] = {
    val warcInput = sc.newAPIHadoopFile[LongWritable, WarcRecord, WarcInputFormat](infile)

    // 1. Find the number of websites that use bootstrap per country.
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
      .map(x => x._1)
      // Group fully qualified domain names together
      .groupBy(x => fullyQualifiedDomain(x))
      // We only want the keys, every fqdn will count for 1
      .map(x => x._1)
      // Group by top level domain
      .groupBy(x => topLevelDomain(x))
      // Count per tld
      .map(x => (x._1, x._2.size))
      // The number ot tld's is manageable so we can safely collect as map
      .collectAsMap()

    //  2. Find the total number of websites per country.
    val TLDCount = warcContent
      // Only keep the domains that are countries
      .filter(x => x.isDefined).map(x => x.get)
      // Only keep the URI part
      .map(x => x._1)
      // Group fully qualified domain names together
      .groupBy(x => fullyQualifiedDomain(x))
      // We only want the keys, every fqdn will count for 1
      .map(x => x._1)
      // Group by top level domain
      .groupBy(x => topLevelDomain(x))
      // Count per tld
      .map(x => (x._1, x._2.size))
      // The number ot tld's is manageable so we can safely collect as map
      .collectAsMap()

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

    //    // Markdown table
    //    println("| Country | Bootstrap ratio | ")
    //    println("| --- | --- | ")
    //    println(bootRatio.map(x => s"| ${x._1} | ${x._2} |").mkString("\n"))
    //
    //    // JS-like array for visualization
    //    println(bootRatio.map(x => s"['${x._1}', ${x._2}]").mkString(",\n"))

    bootRatio
  }


  def isCountry(uri: String): Boolean = tldToCountry.contains(topLevelDomain(fullyQualifiedDomain(uri)))


  /**
    * @return the top level domain of a fqdn
    */
  def topLevelDomain(fqdn: String): String = fqdn.split("\\.", 0).last

  /**
    * @return the fully qualified domain of an uri
    */
  def fullyQualifiedDomain(uri: String): String = {
    val fqdnPart = uri.split("/", 4)

    if (fqdnPart.isEmpty) {
      ""
    } else {
      fqdnPart(2)
    }
  }

  /**
    * @return (# bootstrap / total))
    */
  def getRatio(bootstrapPerTLDCount: collection.Map[String, Int], tldTotal: (String, Int)): Double = {
    val nBootstrap = bootstrapPerTLDCount.getOrElse(tldTotal._1, 0)
    nBootstrap / tldTotal._2.toDouble
  }

  /**
    * @return checks if a stylesheet is bootstrap
    */
  def isBootstrapLink(stylesheet: String): Boolean = stylesheet.contains("bootstrap.min.css") || stylesheet.contains("bootstrap.css")

  /**
    * @return returns a list of all stylesheets in a string
    */
  def stylesheets(string: String): Seq[String] = {
    val regex = "<link rel=('stylesheet'|\"stylesheet\").*\\/>".r
    val matches = regex.findAllIn(string)

    val links = new ListBuffer[String]()
    while (matches.hasNext) {
      links += matches.next()
    }

    links
  }

  /**
    * @return Head of html
    */
  private def extractHeader(html: String): String = {
    val head = html.split("<body", 2)
    if (head.isEmpty)
      return ""
    head(0)
  }

  /**
    * @return (URI, PayloadContent)
    */
  private def extractHtmlContent(rdd: RDD[(LongWritable, WarcRecord)]): RDD[Option[(String, String)]] = {
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

  var tldToCountry: Map[String, String] = Map[String, String]()
  tldToCountry += "ac" -> "Ascension Island"
  tldToCountry += "ad" -> "Andorra"
  tldToCountry += "ae" -> "United Arab Emirates"
  tldToCountry += "af" -> "Afghanistan"
  tldToCountry += "ag" -> "Antigua and Barbuda"
  tldToCountry += "ai" -> "Anguilla"
  tldToCountry += "al" -> "Albania"
  tldToCountry += "am" -> "Armenia"
  tldToCountry += "an" -> "Netherlands Antilles"
  tldToCountry += "ao" -> "Angola"
  tldToCountry += "aq" -> "Antarctica"
  tldToCountry += "ar" -> "Argentina"
  tldToCountry += "as" -> "American Samoa"
  tldToCountry += "at" -> "Austria"
  tldToCountry += "au" -> "Australia"
  tldToCountry += "aw" -> "Aruba"
  tldToCountry += "ax" -> "Åland Islands"
  tldToCountry += "az" -> "Azerbaijan"
  tldToCountry += "ba" -> "Bosnia and Herzegovina"
  tldToCountry += "bb" -> "Barbados"
  tldToCountry += "bd" -> "Bangladesh"
  tldToCountry += "be" -> "Belgium"
  tldToCountry += "bf" -> "Burkina Faso"
  tldToCountry += "bg" -> "Bulgaria"
  tldToCountry += "bh" -> "Bahrain"
  tldToCountry += "bi" -> "Burundi"
  tldToCountry += "bj" -> "Benin"
  tldToCountry += "bl" -> "Saint Barthélemy"
  tldToCountry += "bm" -> "Bermuda"
  tldToCountry += "bn" -> "Brunei"
  tldToCountry += "bo" -> "Bolivia"
  tldToCountry += "br" -> "Brazil"
  tldToCountry += "bs" -> "Bahamas"
  tldToCountry += "bt" -> "Bhutan"
  tldToCountry += "bv" -> "Bouvet Island"
  tldToCountry += "bw" -> "Botswana"
  tldToCountry += "by" -> "Belarus"
  tldToCountry += "bz" -> "Belize"
  tldToCountry += "cat" -> "Catalonia"
  tldToCountry += "ca" -> "Canada"
  tldToCountry += "cc" -> "Cocos (Keeling) Islands"
  tldToCountry += "cd" -> "Democratic Republic of the Congo"
  tldToCountry += "cf" -> "Central African Republic"
  tldToCountry += "cg" -> "Republic of the Congo"
  tldToCountry += "ch" -> "Switzerland"
  tldToCountry += "ci" -> "Côte d\\'Ivoire"
  tldToCountry += "ck" -> "Cook Islands"
  tldToCountry += "cl" -> "Chile"
  tldToCountry += "cm" -> "Cameroon"
  tldToCountry += "cn" -> "China"
  tldToCountry += "co" -> "Colombia"
  tldToCountry += "cr" -> "Costa Rica"
  tldToCountry += "cu" -> "Cuba"
  tldToCountry += "cv" -> "Cape Verde"
  tldToCountry += "cx" -> "Christmas Island"
  tldToCountry += "cy" -> "Cyprus"
  tldToCountry += "cz" -> "Czech Republic"
  tldToCountry += "de" -> "Germany"
  tldToCountry += "dj" -> "Djibouti"
  tldToCountry += "dk" -> "Denmark"
  tldToCountry += "dm" -> "Dominica"
  tldToCountry += "do" -> "Dominican Republic"
  tldToCountry += "dz" -> "Algeria"
  tldToCountry += "ec" -> "Ecuador"
  tldToCountry += "ee" -> "Estonia"
  tldToCountry += "eg" -> "Egypt"
  tldToCountry += "eh" -> "Western Sahara"
  tldToCountry += "er" -> "Eritrea"
  tldToCountry += "es" -> "Spain"
  tldToCountry += "et" -> "Ethiopia"
  tldToCountry += "eu" -> "European Union"
  tldToCountry += "fi" -> "Finland"
  tldToCountry += "fj" -> "Fiji"
  tldToCountry += "fk" -> "Falkland Islands"
  tldToCountry += "fm" -> "Federated States of Micronesia"
  tldToCountry += "fo" -> "Faroe Islands"
  tldToCountry += "fr" -> "France"
  tldToCountry += "ga" -> "Gabon"
  tldToCountry += "gb" -> "United Kingdom"
  tldToCountry += "gd" -> "Grenada"
  tldToCountry += "ge" -> "Georgia"
  tldToCountry += "gf" -> "French Guiana"
  tldToCountry += "gg" -> "Guernsey"
  tldToCountry += "gh" -> "Ghana"
  tldToCountry += "gi" -> "Gibraltar"
  tldToCountry += "gl" -> "Greenland"
  tldToCountry += "gm" -> "Gambia"
  tldToCountry += "gn" -> "Guinea"
  tldToCountry += "gp" -> "Guadeloupe"
  tldToCountry += "gq" -> "Equatorial Guinea"
  tldToCountry += "gr" -> "Greece"
  tldToCountry += "gs" -> "South Georgia and the South Sandwich Islands"
  tldToCountry += "gt" -> "Guatemala"
  tldToCountry += "gu" -> "Guam"
  tldToCountry += "gw" -> "Guinea-Bissau"
  tldToCountry += "gy" -> "Guyana"
  tldToCountry += "hk" -> "Hong Kong"
  tldToCountry += "hm" -> "Heard Island and McDonald Islands"
  tldToCountry += "hn" -> "Honduras"
  tldToCountry += "hr" -> "Croatia"
  tldToCountry += "ht" -> "Haiti"
  tldToCountry += "hu" -> "Hungary"
  tldToCountry += "id" -> "Indonesia"
  tldToCountry += "ie" -> "Ireland"
  tldToCountry += "il" -> "Israel"
  tldToCountry += "im" -> "Isle of Man"
  tldToCountry += "in" -> "India"
  tldToCountry += "io" -> "British Indian Ocean Territory"
  tldToCountry += "iq" -> "Iraq"
  tldToCountry += "ir" -> "Iran"
  tldToCountry += "is" -> "Iceland"
  tldToCountry += "it" -> "Italy"
  tldToCountry += "je" -> "Jersey"
  tldToCountry += "jm" -> "Jamaica"
  tldToCountry += "jo" -> "Jordan"
  tldToCountry += "jp" -> "Japan"
  tldToCountry += "ke" -> "Kenya"
  tldToCountry += "kg" -> "Kyrgyzstan"
  tldToCountry += "kh" -> "Cambodia"
  tldToCountry += "ki" -> "Kiribati"
  tldToCountry += "km" -> "Comoros"
  tldToCountry += "kn" -> "Saint Kitts and Nevis"
  tldToCountry += "kp" -> "North Korea"
  tldToCountry += "kr" -> "South Korea"
  tldToCountry += "kw" -> "Kuwait"
  tldToCountry += "ky" -> "Cayman Islands"
  tldToCountry += "kz" -> "Kazakhstan"
  tldToCountry += "la" -> "Laos"
  tldToCountry += "lb" -> "Lebanon"
  tldToCountry += "lc" -> "Saint Lucia"
  tldToCountry += "li" -> "Liechtenstein"
  tldToCountry += "lk" -> "Sri Lanka"
  tldToCountry += "lr" -> "Liberia"
  tldToCountry += "ls" -> "Lesotho"
  tldToCountry += "lt" -> "Lithuania"
  tldToCountry += "lu" -> "Luxembourg"
  tldToCountry += "lv" -> "Latvia"
  tldToCountry += "ly" -> "Libya"
  tldToCountry += "ma" -> "Morocco"
  tldToCountry += "mc" -> "Monaco"
  tldToCountry += "md" -> "Moldova"
  tldToCountry += "me" -> "Montenegro"
  tldToCountry += "mg" -> "Madagascar"
  tldToCountry += "mh" -> "Marshall Islands"
  tldToCountry += "mk" -> "Republic of Macedonia"
  tldToCountry += "ml" -> "Mali"
  tldToCountry += "mm" -> "Myanmar"
  tldToCountry += "mn" -> "Mongolia"
  tldToCountry += "mo" -> "Macau"
  tldToCountry += "mp" -> "Northern Mariana Islands"
  tldToCountry += "mq" -> "Martinique"
  tldToCountry += "mr" -> "Mauritania"
  tldToCountry += "ms" -> "Montserrat"
  tldToCountry += "mt" -> "Malta"
  tldToCountry += "mu" -> "Mauritius"
  tldToCountry += "mv" -> "Maldives"
  tldToCountry += "mw" -> "Malawi"
  tldToCountry += "mx" -> "Mexico"
  tldToCountry += "my" -> "Malaysia"
  tldToCountry += "mz" -> "Mozambique"
  tldToCountry += "na" -> "Namibia"
  tldToCountry += "nc" -> "New Caledonia"
  tldToCountry += "ne" -> "Niger"
  tldToCountry += "nf" -> "Norfolk Island"
  tldToCountry += "ng" -> "Nigeria"
  tldToCountry += "ni" -> "Nicaragua"
  tldToCountry += "nl" -> "Netherlands"
  tldToCountry += "no" -> "Norway"
  tldToCountry += "np" -> "Nepal"
  tldToCountry += "nr" -> "Nauru"
  tldToCountry += "nu" -> "Niue"
  tldToCountry += "nz" -> "New Zealand"
  tldToCountry += "om" -> "Oman"
  tldToCountry += "pa" -> "Panama"
  tldToCountry += "pe" -> "Peru"
  tldToCountry += "pf" -> "French Polynesia"
  tldToCountry += "pg" -> "Papua New Guinea"
  tldToCountry += "ph" -> "Philippines"
  tldToCountry += "pk" -> "Pakistan"
  tldToCountry += "pl" -> "Poland"
  tldToCountry += "pm" -> "Saint Pierre and Miquelon"
  tldToCountry += "pn" -> "Pitcairn Islands"
  tldToCountry += "pr" -> "Puerto Rico"
  tldToCountry += "ps" -> "Palestine"
  tldToCountry += "pt" -> "Portugal"
  tldToCountry += "pw" -> "Palau"
  tldToCountry += "py" -> "Paraguay"
  tldToCountry += "qa" -> "Qatar"
  tldToCountry += "re" -> "Réunion"
  tldToCountry += "ro" -> "Romania"
  tldToCountry += "rs" -> "Serbia"
  tldToCountry += "ru" -> "Russia"
  tldToCountry += "rw" -> "Rwanda"
  tldToCountry += "sa" -> "Saudi Arabia"
  tldToCountry += "sb" -> "Solomon Islands"
  tldToCountry += "sc" -> "Seychelles"
  tldToCountry += "sd" -> "Sudan"
  tldToCountry += "se" -> "Sweden"
  tldToCountry += "sg" -> "Singapore"
  tldToCountry += "sh" -> "Saint Helena"
  tldToCountry += "si" -> "Slovenia"
  tldToCountry += "sj" -> "Svalbard and Jan Mayen"
  tldToCountry += "sk" -> "Slovakia"
  tldToCountry += "sl" -> "Sierra Leone"
  tldToCountry += "sm" -> "San Marino"
  tldToCountry += "sn" -> "Senegal"
  tldToCountry += "so" -> "Somalia"
  tldToCountry += "sr" -> "Suriname"
  tldToCountry += "st" -> "São Tomé and Príncipe"
  tldToCountry += "su" -> "Soviet Union"
  tldToCountry += "sv" -> "El Salvador"
  tldToCountry += "sy" -> "Syria"
  tldToCountry += "sz" -> "Swaziland"
  tldToCountry += "tc" -> "Turks and Caicos Islands"
  tldToCountry += "td" -> "Chad"
  tldToCountry += "tf" -> "French Southern Territories"
  tldToCountry += "tg" -> "Togo"
  tldToCountry += "th" -> "Thailand"
  tldToCountry += "tj" -> "Tajikistan"
  tldToCountry += "tk" -> "Tokelau"
  tldToCountry += "tl" -> "East Timor"
  tldToCountry += "tm" -> "Turkmenistan"
  tldToCountry += "tn" -> "Tunisia"
  tldToCountry += "to" -> "Tonga"
  tldToCountry += "tp" -> "East Timor"
  tldToCountry += "tr" -> "Turkey"
  tldToCountry += "tt" -> "Trinidad and Tobago"
  tldToCountry += "tv" -> "Tuvalu"
  tldToCountry += "tw" -> "Taiwan"
  tldToCountry += "tz" -> "Tanzania"
  tldToCountry += "ua" -> "Ukraine"
  tldToCountry += "ug" -> "Uganda"
  tldToCountry += "uk" -> "United Kingdom"
  tldToCountry += "um" -> "US Minor Outlying Islands"
  tldToCountry += "us" -> "United States"
  tldToCountry += "uy" -> "Uruguay"
  tldToCountry += "uz" -> "Uzbekistan"
  tldToCountry += "va" -> "Vatican City"
  tldToCountry += "vc" -> "Saint Vincent and the Grenadines"
  tldToCountry += "ve" -> "Venezuela"
  tldToCountry += "vg" -> "British Virgin Islands"
  tldToCountry += "vi" -> "United States Virgin Islands"
  tldToCountry += "vn" -> "Vietnam"
  tldToCountry += "vu" -> "Vanuatu"
  tldToCountry += "wf" -> "Wallis and Futuna"
  tldToCountry += "ws" -> "Samoa"
  tldToCountry += "ye" -> "Yemen"
  tldToCountry += "yt" -> "Mayotte"
  tldToCountry += "yu" -> "Yugoslavia"
  tldToCountry += "za" -> "South Africa"
  tldToCountry += "zm" -> "Zambia"
  tldToCountry += "zw" -> "Zimbabwe"
  tldToCountry += "бг" -> "Bulgaria"
  tldToCountry += "бел" -> "Belarus"
  tldToCountry += "ею" -> "European Union"
  tldToCountry += "қаз" -> "Kazakhstan"
  tldToCountry += "мон" -> "Mongolia"
  tldToCountry += "мкд" -> "Macedonia"
  tldToCountry += "рф" -> "Russia"
  tldToCountry += "срб" -> "Serbia"
  tldToCountry += "укр" -> "Ukraine"
  tldToCountry += "الجزائر" -> "Algeria"
  tldToCountry += "مصر" -> "Egypt"
  tldToCountry += "ایران" -> "Iran"
  tldToCountry += "الاردن" -> "Jordan"
  tldToCountry += "فلسطين" -> "Palestine"
  tldToCountry += "پاکستان" -> "Pakistan"
  tldToCountry += "قطر" -> "Qatar"
  tldToCountry += "السعودية" -> "Saudi Arabia"
  tldToCountry += "سوريا" -> "Syria"
  tldToCountry += "تونس" -> "Tunisia"
  tldToCountry += "امارات" -> "UAE"
  tldToCountry += "عمان" -> "Oman"
  tldToCountry += "مليسيا" -> "Malaysia"
  tldToCountry += "المغرب" -> "Morocco"
  tldToCountry += "سودان" -> "Sudan"
  tldToCountry += "اليمن" -> "Yemen"
  tldToCountry += "বাংলা" -> "Bangladesh"
  tldToCountry += "ভাৰত" -> "India"
  tldToCountry += "ভারত" -> "India"
  tldToCountry += "ලංකා" -> "Sri Lanka"
  tldToCountry += "ไทย" -> "Thailand"
  tldToCountry += "中国" -> "China"
  tldToCountry += "香港" -> "Hong Kong"
  tldToCountry += "澳門" -> "Macau"
  tldToCountry += "新加坡" -> "Singapore"
  tldToCountry += "台灣" -> "Taiwan"
  tldToCountry += "հայ" -> "Armenia"
  tldToCountry += "გე" -> "Georgia"
  tldToCountry += "ελ" -> "Greece"
  tldToCountry += "한국" -> "South Korea"
}
