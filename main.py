from InsiderTrading import insider_trading
from s3handler import (get_client, read_obj_to_df, put_file)
import datetime as dt
from dotenv import load_dotenv
import os
import pandas as pd

def main():
    # print("hello world")
    # data = insider_trading()
    client = get_client()


    load_dotenv()
    base_dir = os.getenv("BASE_DIR")
    purchases = pd.read_csv(base_dir + "data/insiderPurchases/" + str(dt.date.today()) + ".csv")
    # purchases = read_obj_to_df(client, "mysecfilings", "data/insiderPurchases/2022-03-03.csv")
    purchases.to_json(base_dir + "data/transactions.json", orient="records")
    put_file(client, "mysecfilings", base_dir + "data/transactions.json", "data/insiderPurchases/"+ str(dt.date.today()) +".json")
    put_file(client, "mysecfilings", base_dir + "data/transactions.json", "data/insiderPurchases/"+ str(dt.date.today() + dt.timedelta(days=1)) +".json")

    transactions = pd.read_csv(base_dir + "data/insiderTransactions/" + str(dt.date.today()) + ".csv")
    # transactions = read_obj_to_df(client, "mysecfilings", "data/insiderTransactions/2022-03-03.csv")
    transactions.to_json(base_dir + "data/transactions.json", orient="records")
    put_file(client, "mysecfilings", base_dir + "data/transactions.json", "data/insiderTransactions/"+ str(dt.date.today()) +".json")
    put_file(client, "mysecfilings", base_dir + "data/transactions.json", "data/insiderTransactions/"+ str(dt.date.today() + dt.timedelta(days=1)) +".json")


    # data = 

if __name__ == "__main__":
    main()