import re
from bs4 import BeautifulSoup
import urllib.request

url = r"https://simple.wikipedia.org/wiki/Country_code_top-level_domain"

urlData = urllib.request.urlopen(url)
content = urlData.read()

soup = BeautifulSoup(content, 'html.parser')
div = soup.findAll("div", {"class": r"mw-parser-output"}).pop()

lists = div.findAll("ul", {"class": ""})
listItems = []
for ul in lists:
    listItems.extend(ul.findAll("li", {"class": ""}))


def extractCountry(listItem):
    links = listItem.findAll("a")
    if len(links) < 2:
        return None
    while len(links) > 2:
        links.pop()

    country = links.pop().text
    tld = links.pop().text
    if country == "People's Republic of China":
        country = "China"

    tld = tld.replace("'", "\\\\'").replace(".", "")
    country = country.replace("'", "\\\\'")

    return tld, country

listItems = list(map(extractCountry, listItems))
listItems = list(filter(lambda x: x is not None, listItems))

print("var tldToCountry: Map[String, String] = Map[String, String]()")
for tld, country in listItems:
    print(f"tldToCountry += \"{tld}\" -> \"{country}\"")
