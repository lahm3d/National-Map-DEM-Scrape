# National Map DEM Scrape
Small script that allows users to web scrape National Map webppage and download all intersecting DEM tiles. Currently works on 3m DEM tiles only. 

Author: Labeeb Ahmed

## Requirements
    Python 3.7
    Third party libraries:
      requests
      geopandas
      beautifulsoup
      shapely

## How-to-use

Clone or download the repo., install the libraries, and then execute the tool

    python scrape_dems.py --path c:/test_project --aoi c:/test_project/aoi.shp --year 2005 --scrape --download

* `path`: Project directory
* `aoi`: Full path for aoi shapefile
* `year`: the year from `begdate` variable is used. The variable is present in metadata. Tiles that have year that are equal to or greater than are included, and all other dates are excluded. Here is an example [xml file](https://thor-f5.er.usgs.gov/ngtoc/metadata/waf/elevation/1-9_arc-second/img/ned19_n40x75_w078x25_pa_northwest_2006_meta.xml). The listed dates are:

    <begdate>20060327</begdate>
    <enddate>20060429</enddate>
* `scrape`: Default value is False. Enables scraping files.
* `download`: Default value is False. Enables downloading DEM tiles.
