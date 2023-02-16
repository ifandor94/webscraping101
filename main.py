###  

# import packages
import pandas as pd
import requests
import sys
import os
from bs4 import BeautifulSoup
from datetime import datetime

# declare variables
wiki_url = "https://en.wikipedia.org/wiki/List_of_professional_sports_leagues_by_revenue"

def extract_data():

    page = requests.get(wiki_url)
    response_code = page.status_code
    #check request is successful, if not exit
    print("Connected" if response_code == 200 else "Error: " + page.status_code)
    if response_code != 200:
        status = "Error: connection failed"
        return status
    
    soup = BeautifulSoup(page.content, 'html.parser')
    table = soup.find('table',{'class':'wikitable sortable'})
    # fix headers
    list_headers = ["League", "Sport", "Country(ies)", "Season", "Level on Pyramid", "Teams", "Matches / Games / Events", "Revenue (M Euro)", "Revenue p/Team", "Revenue p/Match (thousands)", "Ref."]
    rows = []
    # find rows associated with the table and pass to a list
    data_rows = table.find_all('tr')
    for row in data_rows:
        value = row.find_all('td')
        beautified_value = [ele.text.strip() for ele in value]
        if len(beautified_value) == 0:
            continue
        rows.append(beautified_value)
    # pass output to dataframe
    df = pd.DataFrame(columns=list_headers, data=rows)
    status = 'Success'

    return df, status, rows

def transform_data(df, rows):
    # cleanse data as needed
    #drop the reference column
    df.drop(columns='Ref.',inplace=True)
    #drop the references within columns (indicated by square brackets)
    df.replace(to_replace='\[.*?\]', value="", regex=True, inplace=True)
    #split countries
    df['Country(ies)'] = [x[2].split('\xa0') for x in rows]
    #convert revenue to floats, teams and level to integer
    df[['Matches / Games / Events', 'Revenue (M Euro)', 'Revenue p/Team', 'Revenue p/Match (thousands)']] = df[['Matches / Games / Events', 'Revenue (M Euro)', 'Revenue p/Team', 'Revenue p/Match (thousands)']].replace(to_replace=',', value="", regex=True)
    df = df.astype(
        {'League':'string'
        , 'Sport':'string'
        , 'Matches / Games / Events':'float'
        , 'Revenue (M Euro)':'float'
        , 'Revenue p/Team':'float'
        , 'Revenue p/Match (thousands)':'float'}
        )
    # add a run date column
    df['run_datetime'] = datetime.now().strftime('%Y-%h-%d-%H:%M-%S')
    return df
    
def load_data(df):
    targetfile = "sports_leagues_by_revenue_data.csv"
    df.to_csv(targetfile, mode='a', header=not os.path.exists(targetfile))

def log(message):
    timestamp_format = '%Y-%h-%d-%H:%M-%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("logfile.txt", "a") as f:
        f.write(timestamp + ',' + message + '\n')

log("ETL Job Started")

log("Extract phase started")
extracted_data, status, rows = extract_data()
log("Extract phase ended: " + status)

if status != 'Success':
    log("ETL Job Terminated: Unsuccessful")
    sys.exit()

log("Transform phase started")
transformed_data = transform_data(extracted_data, rows)
log("Transform phase complete")

log("Load phase started")
load_data(transformed_data)
log("Load phase complete")

log("ETL Job Ended")

# next stage is to try and write data to a file on one drive instead and to schedule the python script
