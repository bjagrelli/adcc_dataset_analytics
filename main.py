from functions import *
from queries import * 
from datetime import datetime
from WebScraper import WebScraper

# Variables

# URLs
main_page_url = 'https://www.bjjheroes.com/a-z-bjj-fighters-list'
fighter_page_url = 'https://www.bjjheroes.com/?p={}'

# 
columns = ['id', 'fighter_name', 'weight_class', 'team', 'record']
new_columns = ['match_id', 'opponent', 'result', 'method', 'competition', 'weight', 'stage', 'year', 'opponent_id']
database = 'adcc.db'
path = 'tables'
files = ['tables/main_list.csv', 'tables/fighters_list.csv']



def main():

    create_tables_path('tables')

    folder = check_folder(files)

    print('>>> Beginning of process\n')
    
    if not folder:
        main_page_html = get_page_html(main_page_url)

        soup = main_page_scraper(main_page_html)

        main_columns = get_main_page_columns(soup)
        main_list = get_main_page_fighters_info(soup)
        
        print('>>> Getting page links for each athlete\n')
        
        fighters_links = get_fighters_page_link(main_list, fighter_page_url)

        print('>>> Starting download of raw data')

        t1 = datetime.now()
        scraper = WebScraper(urls = fighters_links)

        print(f'>>> Download completed')
        print()
        print(f'>>> Data scraping started')

        fighters_list = fighters_page_scraper_html(scraper.master_dict, fighters_links)

        t2 = datetime.now()
        scraping_time = t2 - t1
        print(f'>>> Total time: {scraping_time.seconds} seconds')

        # Create dataframes from main page and save them as csv files
        main_df = create_fighters_df(main_list, main_columns)
        create_csv_from_table(main_df, files[0])

        # Create dataframes from fighters page and save them as csv files
        records_df = create_fighters_df(fighters_list, columns)
        new_df = append_fighting_record_to_df(records_df, new_columns)
        create_csv_from_table(new_df, files[1])
    else:
        print('Arquivos já existem na pasta!')
        pass
    
    print()

    # CREATE RAW TABLES
    schema = 'raw'
    con = duckdb.connect(database=database, read_only=False)
    try:
        con.execute(f'CREATE SCHEMA {schema};')
    except:
        pass
    finally:
        for file in files:
            schema = 'raw'
            create_table(schema, database, file)
            print(f'>>> Criando tabela {schema}.{file[7:-4]}.')
        
        con.close()


    print()

    # CREATE TRUSTED TABLES
    schema = 'trusted'
    con = duckdb.connect(database=database, read_only=False)
    try:
        con.execute(f'CREATE SCHEMA {schema};')
    except:
        pass
    finally:
        con.sql(create_trusted_main_list)
        print(f'>>> Criando tabela trusted.main_list.')

        con.sql(create_trusted_fighters_list)
        print(f'>>> Criando tabela trusted.fighters_list.')

        con.close()


    print()

    # CREATE REFINED TABLES
    schema = 'refined'
    con = duckdb.connect(database=database, read_only=False)
    try:
        con.execute(f'CREATE SCHEMA {schema};')
    except:
        pass
    finally:
        con.sql(create_refined_fight_record)
        print(f'>>> Criando tabela refined.fight_record.')

        con.close()

    con = duckdb.connect(database=database, read_only=False)
    fight_record_df = con.execute("SELECT * FROM refined.fight_record").fetchall()
    refined_columns = [column[0] for column in con.execute("""SELECT column_name 
                                                              FROM information_schema.columns 
                                                              WHERE table_schema = 'refined' 
                                                              AND table_name = 'fight_record' 
                                                              ORDER BY ordinal_position""").fetchall()]
    
    fight_record_df = pd.DataFrame(fight_record_df, columns=refined_columns)
    create_csv_from_table(fight_record_df, 'adcc_historical_data.csv')

    print(">>> Criado na pasta o arquivo adcc_historical_data.csv")


if __name__ == '__main__':
    x = main()
    print(x)