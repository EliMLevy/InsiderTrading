import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from IPython.display import clear_output
from tqdm import tqdm
import datetime as dt

from s3handler import (get_client, put_file)

from dotenv import load_dotenv
import os

load_dotenv()
base_dir = os.getenv("BASE_DIR")

#Looks up Edgar CIK Number
def symbol_to_cik(symbols):
    ticker_cik = pd.read_csv(base_dir + 'ticker_and_edgar_cik.csv', delimiter=',')
    df = pd.DataFrame(ticker_cik)
    df.set_index('Ticker', inplace=True)
    new_symbols = [i.lower() for i in symbols]
    cik = [df.loc[i, 'CIK'] for i in new_symbols]
    return cik
#Looks up Symbol from CIK Number:
def cik_to_symbol(ciks):
    ticker_cik = pd.read_csv(base_dir + 'ticker_and_edgar_cik.csv', delimiter=',')
    df = pd.DataFrame(ticker_cik)
    df.set_index('CIK', inplace=True)
    df = df[~df.index.duplicated(keep='first')]
    tickers = [df.loc[i, 'Ticker'] for i in ciks]
    new_tickers = [i.upper() for i in tickers]
    return new_tickers
#Turns URL into Soup object
def to_soup(url):
    url_response = requests.get(url)
    webpage = url_response.content
    soup = BeautifulSoup(webpage, 'html.parser')
    return soup

def scrape_filings_for(ticker, end):
    lst = [ticker]
    # print(lst)
    cik = symbol_to_cik(lst)
    page = 0
    # https://www.sec.gov/cgi-bin/own-disp?action=getissuer&CIK=1046179&type=&dateb=&owner=include&start=0
    beg_url = 'https://www.sec.gov/cgi-bin/own-disp?action=getissuer&CIK='+str(cik[0])+'&type=&dateb=&owner=include&start='+str(page*80)
    urls = [beg_url]
    df_data = []
    for url in urls:
        soup = to_soup(url)
        transaction_report = soup.find('table', {'id':'transaction-report'})

        try:
            t_chil = [i for i in transaction_report.children]
            t_cont = [i for i in t_chil if i != '\n']
            # raise Exception
        except:
            return pd.DataFrame()
            # print(soup)
            # print(beg_url)
            # raise Exception

        headers = [ i for i in t_cont[0].get_text().split('\n') if i != '']
        data_rough = [i for lst in t_cont[1:] for i in lst.get_text().split('\n') if i != '' ]
        data = [data_rough[i:i+12] for i in range(0,len(data_rough), 12)]
        if len(data) > 0:
            last_line = data[-1]
        for i in data:
            if (end > i[1]):
                break
            else:
                if (i != last_line):
                    df_data.append(i)
                else:
                    df_data.append(i)
                    page += 1
                    urls.append('https://www.sec.gov/cgi-bin/own-disp?action=getissuer&CIK='+str(cik[0])+'&type=&dateb=&owner=include&start='+str(page*80))
    df = pd.DataFrame(df_data,columns = headers)                
    
    return df
    
    

def summarize_filings(ticker, df):
    try:
        df['Purch'] = pd.to_numeric(df['Acquistion or Disposition'].apply(lambda x: 1 if x == 'A' else 0)
                        *df['Number of Securities Transacted'])
        df['Sale'] = pd.to_numeric(df['Acquistion or Disposition'].apply(lambda x: 1 if x == 'D' else 0)
                        *df['Number of Securities Transacted'])
    except:
        print("Error parsing transaction details into numbers for " + str(ticker))
        return None
    
    purchases = df[(df["Transaction Type"] == "P-Purchase")]
    if len(purchases) > 0:
        purchase_summary_df = pd.DataFrame({
            "Symbol": ticker,
            "# Purchases": len(purchases),
            "Total bought": int(purchases['Purch'].sum(skipna=True)),
            "Avg per Transaction": round(int(purchases['Purch'].sum(skipna=True)) / len(purchases), 2)
        }, index = [0])
        purchase_summary_df.to_csv(base_dir + "data/insiderPurchases/" + str(dt.date.today()) + ".csv", header=False, mode='a', index=False)
    
    purch = df['Acquistion or Disposition'] == 'A'
    sale = df['Acquistion or Disposition'] == 'D'
    num_purch = len(df[purch])
    num_sale = len(df[sale])
    total_purch = int(df['Purch'].sum(skipna=True))
    total_sale = int(df['Sale'].sum(skipna=True))

    if num_purch > 0:
        avg_purch = int(total_purch/num_purch)
    else:
        avg_purch = 0
    if num_sale > 0:
        avg_sale = int(total_sale/num_sale)
        ratio = round(num_purch/num_sale, 2)
    else:
        avg_sale = 0
        ratio = float('inf')
    
    new_df = pd.DataFrame({'Symbol': ticker,
                            'Purchases': num_purch,
                            'Sales': num_sale,
                            'Buy/Sell Ratio': ratio,
                            'Total Bought': f'{total_purch}',
                            'Total Sold': f'{total_sale}',
                            'Avg Shares Bought': f'{avg_purch}',
                            'Avg Shares Sold': f'{avg_sale}'},
                            index = [0])

    
    if total_sale > 0 or total_purch > 0:
        return new_df
    else:
        return None


#Pulls the Insider Trading Statistics
def insider_trading(start_from=0):
    ticker_csv = pd.read_csv(base_dir + 'ticker_and_edgar_cik.csv', delimiter=',')
    symbols = [i.upper() for i in ticker_csv.Ticker]
    
    end = str(dt.date.today() - dt.timedelta(days=30))
    if start_from == 0:
        empty_output = pd.DataFrame(columns=["Symbol","Purchases","Sales","Buy/Sell Ratio","Total Bought","Total Sold","Avg Shares Bought","Avg Shares Sold"])
        empty_output.to_csv(base_dir + "data/insiderTransactions/" + str(dt.date.today()) + ".csv", index=False)
        
        empty_purchases = pd.DataFrame(columns=["Symbol", "# Purchases", "Total bought", "Avg per Transaction"])
        empty_purchases.to_csv(base_dir + "data/insiderPurchases/" + str(dt.date.today()) + ".csv", index=False)
    for i in tqdm(range(len(symbols))[start_from:]):
        symbol_data = scrape_filings_for(symbols[i], end)
        summary_df = summarize_filings(symbols[i], symbol_data)
        if summary_df is not None:
            summary_df.to_csv(base_dir + "data/insiderTransactions/" + str(dt.date.today()) + ".csv", mode="a",header=False, index=False)


    
    # s3client = get_client()
    # put_file(s3client, "mysecfilings", base_dir + "data/insiderTransactions/" + str(dt.date.today()) + ".csv", "data/insiderTransactions/" + str(dt.date.today()) + ".csv")
    # put_file(s3client, "mysecfilings", base_dir + "data/insiderTransactions/" + str(dt.date.today()) + ".csv", "data/insiderTransactions/" + str(dt.date.today() + dt.timedelta(days=1)) + ".csv")
    
    # put_file(s3client, "mysecfilings", base_dir + "data/insiderPurchases/"    + str(dt.date.today()) + ".csv", "data/insiderPurchases/"    + str(dt.date.today()) + ".csv")
    # put_file(s3client, "mysecfilings", base_dir + "data/insiderPurchases/"    + str(dt.date.today()) + ".csv", "data/insiderPurchases/"    + str(dt.date.today() + dt.timedelta(days=1)) + ".csv")
