import boto3
import pandas
from dotenv import load_dotenv
import os



def get_client():
    # Creating the low level functional client
    load_dotenv()
    client = boto3.client(
        's3',
        aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name = 'us-east-1'
    )

    return client

def get_buckets_list(client):
    # Fetch the list of existing buckets
    clientResponse = client.list_buckets()
    return clientResponse



# To read a csv from bucket

def read_obj_to_df(client, bucket, key):
    # Create the S3 object
    obj = client.get_object(
        Bucket=bucket,
        Key=key
    )
        
    # Read data from the S3 object
    data = pandas.read_csv(obj['Body'])
        
    return data



def put_file(client, bucket, path, key):
    # To put an object
    data = open(path,"rb")
    # data = data.read()
    obj = client.put_object(
        Bucket=bucket,
        Body=data,
        Key=key
    )

def put_string(client, bucket, data, key):
    # To put an object
    byte_data = str.encode(data)
    # data = data.read()
    obj = client.put_object(
        Bucket=bucket,
        Body=byte_data,
        Key=key
    )


def main():
    client = get_client()
    data = read_obj_to_df(client, "mysecfilings", "output/65_daysAgo/AllParsedForm4s.csv")
    data.to_csv("fetched.csv")
    print(data.head(5))
    # put_obj(client, "mysecfilings", "./output/60_daysAgo/AllParsedForm4s.csv", "testDoc.csv")


if __name__ == "__main__":
    main()
