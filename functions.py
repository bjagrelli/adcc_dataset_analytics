import requests
import os
from bs4 import BeautifulSoup
import pandas as pd
import duckdb
import numpy as np


# HTML FUNCIONS
def get_page_html(url):
    data = requests.get(url)

    return data.text


def main_page_scraper(html):
    soup = BeautifulSoup(html, "html.parser")

    return soup


def get_main_page_columns(soup):
    link = soup.find('thead')

    columns = link.find_all('th')

    result = [column.text for column in columns]
    result.append('Id')

    return result


def get_main_page_fighters_info(soup):
    rows = soup.find_all('tr')
    result = []
    for row in rows:
        columns = row.find_all('td')
        if len(columns) > 0:
            values = [column.text.strip() for column in columns]
            href = columns[0].find('a')['href']
            values.append(href[4:])
            result.append(values)

    return result


def get_fighters_page_link(info_list, url):
    result_list = []
    for info in info_list:
        result_list.append(url.format(info[4]))

    return result_list


def fighters_page_scraper_html(scraper_dict, fighters_links):
    import re
    fighters_list = []
    for link in fighters_links:
        id = link.replace('https://www.bjjheroes.com/?p=', '')

        try:
            raw_html = scraper_dict[link]['Raw Html']

            soup = BeautifulSoup(raw_html, "html.parser")   

            # GATHER FIGHTER DATA
            try:
                fighter_name = soup.find('strong', text=re.compile("Full Name")).parent.getText().split(": ",1)[1]
            except:
                fighter_name = False
            try:
                fighter_weight_class = soup.find('strong', text=re.compile("Weight Division")).parent.getText().split(": ",1)[1]
            except:
                fighter_weight_class = False  
            try:
                fighter_team = soup.find('strong', text=re.compile("Team/Association")).parent.getText().split(": ",1)[1]
            except:
                fighter_team = False
            try:
                fight_record = str(soup.find("table", {"class": "table table-striped sort_table"}))
            except:
                fight_record = False

            fighters_list.append((id, fighter_name, fighter_weight_class, fighter_team, fight_record))
        except:
            pass

    return fighters_list


# OS FUNCTIONS
def create_tables_path(path):
    # Check if the path exists
    if os.path.exists(path) and os.path.isdir(path):
        pass
    else:
        # Create the path
        os.mkdir(path)


def check_folder(files):
    # Check if each file in the list exists in the subdirectory
    if type(files) == list:
        for file_path in files:
            if not os.path.exists(file_path):
                return False
    if type(files) == str:
        if not os.path.exists(file_path):
                return False
    return True


# DATAFRAME FUNCTIONS
def create_fighters_df(fighters_list, columns):
    df = pd.DataFrame(data=fighters_list, columns=columns)
    return df


def create_csv_from_table(df, filename):
    df.to_csv(filename, sep=';', index=False)


def df_from_csv(filename):
    df = pd.read_csv(filename, sep=';')
    return df


def extract_table_data(record):
    soup = BeautifulSoup(record, 'html.parser')
    table = soup.find('table')
    rows = table.find_all('tr')
    data = []
    for row in rows:
        cells = row.find_all('td')
        row_data = [cell.text.strip() for cell in cells]
        
        # Extract Opponent Id from the link
        opponent_id = None
        link = row.find('a', href=True)
        if link:
            opponent_id = int(link['href'].split('=')[-1])
        
        # Append the Opponent Id to the row data
        row_data.append(opponent_id)
        data.append(row_data)
    return data


def fix_duplicate_name(name):
    fixed_name = ""
    for i, char in enumerate(name):
        if i > 0 and char.isupper() and name[i-1].islower():
            fixed_name += " " + char
        else:
            fixed_name += char  
    dedup_name = fixed_name.strip().split()
    return ' '.join(dedup_name[:2]) 

  
def append_fighting_record_to_df(df, new_columns):
    # Drop null rows from the Record column
    df = df.replace('None', np.nan).dropna(subset=['record'])
    # Create a new DataFrame by repeating the id, Fighter Name, Weight Class, and Team columns
    new_df = df[['id', 'fighter_name', 'weight_class', 'team']].copy()

    # Apply the extraction function to create the 'Record' column
    new_df['record'] = df['record'].apply(extract_table_data)

    # Create an empty list to store the rows for the new DataFrame
    new_rows = []

    # Iterate over each row in the DataFrame
    for index, row in new_df.iterrows():
        id = row['id']
        fighter_name = row['fighter_name']
        weight_class = row['weight_class']
        team = row['team']
        record_data = row['record']
        
        # Iterate over each row extracted from the HTML table
        for record_row in record_data:
            if len(record_row) > 1:
                # Create a new row with the id, Fighter Name, Weight Class, and Team values
                new_row = [id, fighter_name, weight_class, team] + record_row
                new_rows.append(new_row)

    # Create the new DataFrame
    new_columns = ['id', 'fighter_name', 'weight_class', 'team'] + new_columns
    new_df = pd.DataFrame(new_rows, columns=new_columns)

    # Drop invalid rows
    new_df = new_df.dropna(subset=['match_id'])

    # Apply lambda function to fix duplicate names
    new_df['opponent'] = new_df['opponent'].apply(fix_duplicate_name)

    return new_df


# DB FUNCTIONS
def create_table(schema, database, filename):
    # Connect to DuckDB in-memory database
    con = duckdb.connect(database=database, read_only=False)
    
    # Create a table from the CSV file
    con.sql(f"CREATE OR REPLACE TABLE {schema}.{filename[7:-4]} AS (SELECT * FROM read_csv_auto('{filename}'))")
    
    # Close the connection
    con.close()