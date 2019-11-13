import argparse
from pathlib import Path
from timeit import default_timer as timer
import multiprocessing as mp
import os
import re
import shutil
import xml.etree.ElementTree as ET
import zipfile

import requests
import geopandas as gpd
import pandas as pd
from bs4 import BeautifulSoup
from shapely.geometry import box, Polygon


def get_aoi(aoi_shp, NAD83):
    """ define AOI, dissolve and reproject """

    gdf = gpd.read_file(str(aoi_shp))
    gdf['unique'] = 1
    gdf = gdf.dissolve(by='unique')
    gdf_prj = gdf.to_crs(NAD83)

    return gdf_prj


def get_data_from_xml_mp(args):
    st = timer()
    aoi_gdf, file, NAD83, csvs_dir, shp_dir, YEAR = args
    #copy
    aoi_gdf = aoi_gdf.copy()

    xml_request = requests.get(file)
    xml_as_string = xml_request.text
    root = ET.fromstring(xml_as_string)

    # get necessary data
    beg_dt = root[0][2][0][0][0].text
    end_dt = root[0][2][0][0][0].text
    beg_yr, end_yr = beg_dt[:4], end_dt[:4]
    download_url = root[0][8][0].text
    # extract file name only
    tmp_split = download_url.split('/')
    parts = len(tmp_split) - 1
    filename = tmp_split[parts].split('.')[0]

    # bounding box - order west, south, east, north
    west = float(root[0][4][0][0].text)
    south = float(root[0][4][0][3].text)
    east = float(root[0][4][0][1].text)
    north = float(root[0][4][0][2].text)

    # create bounding box geometry from rectangular poly
    bbox = box(west, south, east, north)
    poly = [Polygon(bbox.exterior.coords)] # create x,y coords
    gdf = gpd.GeoDataFrame({'geometry': poly})
    gdf.crs = NAD83

    # check if year is missing
    try:
        int(beg_yr)
    except ValueError:
        yr_from_string = re.findall('([0-9]{4})', filename)
        beg_yr, end_yr = yr_from_string[0], yr_from_string[0]

    if int(beg_yr) >= YEAR:
        # perform intersection
        intersections = gpd.sjoin(aoi_gdf, gdf, how="inner", op='intersects')
        if not intersections.empty:
            # add metadata to geopackage files
            gdf['beg_yr'] = beg_yr
            gdf['end_yr'] = end_yr
            gdf['url'] = download_url
            gdf['filename'] = filename

            # # write out file
            shp_filename = shp_dir / f'{filename}.shp'
            gdf.to_file(str(shp_filename))

            df = pd.DataFrame({
                'filename': [filename],
                'url': [download_url],
                'beg_yr': [beg_yr],
                'end_yr': [end_yr]
                })
            df.to_csv(str(csvs_dir / f'{filename}.csv'))
            print(filename, timer()-st)
        else:
            print('not inside aoi: ', filename, timer()-st)


def get_DEM_coverage(nmap_3m_url, xml_urls_csv, aoi_gdf, NAD83, csvs_dir, shp_dir, YEAR):
    """
    create list of all xml urls in a csv, so you're not webscraping every single time &

    """

    if not xml_urls_csv.is_file():
        # access url and create BS object
        xml_index = requests.get(nmap_3m_url)
        xml_soup = BeautifulSoup(xml_index.text, 'html.parser')

        # from BS object pull all xml urls
        xml_list = xml_soup.find_all('a') # a href

        cleaned_xml_list = []
        for i in xml_list:
            string = i.contents[0]
            if string.endswith('.xml'):
                xml_url = nmap_3m_url + string # complete url
                cleaned_xml_list.append(xml_url)

        # save the cleaned xml list
        df_xml = pd.DataFrame({'xml_urls_csv': cleaned_xml_list})
        df_xml.to_csv(str(xml_urls_csv))

    # read in XML_URLS_CSV
    df_xml = pd.read_csv(str(xml_urls_csv))
    cleaned_xml_list = list(df_xml.xml_urls_csv)

    print('starting mp')
    pool = mp.Pool(processes=60)
    pool.map(get_data_from_xml_mp, [(aoi_gdf, file, NAD83, csvs_dir, shp_dir, YEAR) for file in cleaned_xml_list])
    pool.close()


def download_data(args):
    """
    Read in csv file, extract download url and unzip
    """
    csv, zip_dir, dem_dir = args

    csv_df = pd.read_csv(str(csv))
    dem_tile = csv_df.to_dict('records')[0]

    filename, url = dem_tile['filename'], dem_tile['url']

    zip_file = zip_dir / f'{filename}.zip'
    dem_unzip = dem_dir / filename

    if zip_file.is_file() and dem_unzip.is_dir():
        return

    # download the file
    try:
        with requests.get(url, stream=True) as r:
            with open(zip_file, 'wb') as f:
                shutil.copyfileobj(r.raw, f)
        print('Download Successful: ', filename)
    except:
        print(f'Failed: {filename}. Unable to retrieve the file!')

    # unzip the file
    try:
        z = zipfile.ZipFile(zip_file)
        z.extractall(dem_unzip)
        print('Extract Successful: ', filename)
    except:
        print('Failed to extract: ', filename)


def main():
    
    parser = argparse.ArgumentParser(
        prog='National Map DEM Scraping Script',
        description='Scrapes all DEM 3m tiles in National Map by an AOI and year'
        )
    # user arg defined project path
    parser.add_argument('--path', '-p', help='Full pathname for project directory', required=True)
    parser.add_argument('--aoi', '-a', help='Full path AOI shapefile', required=True)
    parser.add_argument('--year', '-y', help='Provide year threshold. Should be YYYY format and it uses >= for filtering', required=True)
    parser.add_argument('--scrape', '-s', help='True for scraping new xml files; otherwise False', nargs='?', const=0, type=int)
    parser.add_argument('--download', '-d', help='True to download identified files; otherwise False', nargs='?', const=0, type=int)
   
    args = parser.parse_args()

    # args 
    work_dir = Path(args.path)
    aoi_shp = Path(args.aoi)
    YEAR = args.year
    SKIP_SCRAPING = args.scrape
    SKIP_DOWNLOADS = args.download
    print(work_dir, aoi_shp, YEAR, SKIP_SCRAPING, SKIP_DOWNLOADS)

    nmap_3m_url = 'https://thor-f5.er.usgs.gov/ngtoc/metadata/waf/elevation/1-9_arc-second/img/'
    NAD83 = {'init': 'epsg:4269'} #'+proj=longlat +ellps=GRS80 +datum=NAD83 +no_defs'
    temp_dir = work_dir / 'tmp'
    shp_dir = temp_dir / 'shps'
    csvs_dir = temp_dir / 'csvs'
    zip_dir = temp_dir / 'zips'
    dem_dir = temp_dir / 'dems'
    xml_urls_csv = temp_dir / 'xml_list.csv'

    # work_dir = Path("D:/nmap_test")
    # aoi_shp = work_dir / "test_area.shp"
    # SKIP_SCRAPING = False
    # SKIP_DOWNLOADS = True

    # create directories
    list_of_dirs = [work_dir, temp_dir, shp_dir, csvs_dir, zip_dir, dem_dir]
    for dir in list_of_dirs:
        if not dir.exists():
            os.makedirs(dir)

    # prep the aoi
    aoi_gdf = get_aoi(aoi_shp, NAD83)

    if SKIP_SCRAPING:
        # creates extent of DEM tiles from XML
        get_DEM_coverage(nmap_3m_url, xml_urls_csv, aoi_gdf, NAD83, csvs_dir, shp_dir, YEAR)

    if SKIP_DOWNLOADS:
        # download all the DEM tiles
        print('starting mp')
        list_of_csvs = list(csvs_dir.rglob('*.csv'))
        pool = mp.Pool(processes=10)
        pool.map(download_data, [(csv, zip_dir, dem_dir) for csv in list_of_csvs])
        pool.close()


if __name__ == "__main__":
    main()
