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
    # pass output to dataframe
    df = pd.DataFrame(columns=list_headers, data=rows)

    return df

def transform_data(df):
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
    


