###  testing git for the third time

# import packages
import pandas as pd
import requests
from bs4 import BeautifulSoup

# declare variables
wiki_url = "https://en.wikipedia.org/wiki/List_of_professional_sports_leagues_by_revenue"

def extract_data():

    page = requests.get(wiki_url)
    response_code = page.status_code
    #check request is successful, if not exit
    print("Connected" if response_code == 200 else "Error: " + page.status_code)
    if response_code != 200:
        return
    
    soup = BeautifulSoup(page.content, 'html.parser')
    table = soup.find('table',{'class':'wikitable sortable'})
    # fix headers
    list_headers = ["League", "Sport", "Country(ies)", "Season", "Level on Pyramid", "Teams", "Matches // Games // Events", "Revenue (M Euro)", "Revenue p/Team", "Revenue p/Match (thousands)", "Ref."]
    rows = []
    # find rows associated with the table and pass to a list
    data_rows = table.find_all('tr')
    for row in data_rows:
        value = row.find_all('td')
        beautified_value = [ele.text.strip() for ele in value]
        if len(beautified_value) == 0:
            continue
        rows.append(beautified_value)

def clean_data():
    

extract_data()
