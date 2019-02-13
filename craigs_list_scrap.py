from pymongo import MongoClient
import time
import random

#Import beautiful soup
import requests
import re
from bs4 import BeautifulSoup
import argparse


# Allows passing in of arguuments at time of calling (for the use of cronjobs to launch the scraping).
parser = argparse.ArgumentParser(description = "Description scraper")
parser.add_argument("-City", help = "City to scrape (as shown in url)", required = False, default = "austin")
parser.add_argument("-Record", help = "What record to start scraping at.", required = False, default = 0)



# Connect to client as a global to save passing it
client = MongoClient()
database = client['craigslist']   # Database name
mongo_connect = database['apts'] # Collection name



def scrape(city, record_start):
    '''
    Call to scrape craigslist Austin Apts listings

    Will scrape the most recent 1000 postings

    Craigslist seems to block ips after a specific number of requests in a given time
    Got around this by setting up EC2 to shut down and start back up every several hours changing my IP restarting the scraping with a cronjob

    '''

    for page_num in range(record_start,880,120):


        webpage = requests.get('https://{}.craigslist.org/search/apa?s={}'.format(city,page_num))
        
        # Keep a log of my requests and time they occure (to see if getting blocked)
        with open('scrape_records.log', 'a+') as log:
            log.write(time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()))
            log.write('City: {}, Page number: {} Status: {}\n'.format(city,page_num,webpage.status_code))

        soup = BeautifulSoup(webpage.text, 'html.parser')

        # Any errors while getting fields I want to record to see what caused it
        for tag in soup.find_all('a',class_='result-title hdrlnk'):
            try:
                fields = get_data(tag,header)
            except Exception as e:
                with open('scrape_error_log.log', 'a+') as log:
                    log.write(time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()))
                    log.write("type error: " + str(e))
                continue

            if fields == {}:
                continue

            fields['city'] = city
            store_in_db(fields)
            time.sleep(random.randint(5,30))

    pass



def get_data(tag,header):

    fields = {}

    fields['link'] = tag['href']
    fields['u_id'] = tag['data-id']
    fields['title'] = tag.text

    #Does this record exist already? dont store if so
    #Using the find and limit(1) instead of findOne for speed issues. Look at below link for more information
    #https://blog.serverdensity.com/checking-if-a-document-exists-mongodb-slow-findone-vs-find/

    record = mongo_connect.find({'u_id' : fields['u_id'], 'link' : fields['link']}).limit(1)
    if record.count() != 0:
        return {}

    # get page with rest of info we want

    sub_page = requests.get(fields['link'],headers=header)
    sub_soup = BeautifulSoup(sub_page.text, 'html.parser')

    # These fields may not exist and if missing does not matter right now but dont want code to break

    fields['price'] = sub_soup.find_all('span',class_='price')[0].text
    fields['housing'] = sub_soup.find_all('span',class_='housing')[0].text
    fields['description'] = sub_soup.find_all('section',id='postingbody')[0].text
    fields['map_extra'] = sub_soup.find_all('div',class_='mapAndAttrs')[0].decode()

    return fields


def store_in_db(fields):
    '''
    Store the data in DB
    This is its own function so I can easly change where I am storing (say switch to SQL)
    '''
    mongo_connect.insert_one(fields)

    pass


if __name__ =="__main__":
    argument = parser.parse_args()

    scrape(argument.City, argument.Record)
