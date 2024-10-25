import pandas as pd
import os
import io
from azure.storage.blob import BlobServiceClient, BlobClient
from dotenv import load_dotenv

#Extracting data
ziko_df = pd.read_csv("ziko_logistics_data.csv")

#Filling the missing parameters of the data
ziko_df.fillna({
    'Unit_Price':ziko_df['Unit_Price'].mean(),
    'Total_Cost':ziko_df['Total_Cost'].mean(),
    'Discount_Rate':0.0,
    'Return_Reason':'Unknown'

}, inplace= True)

# Correcting the date func
ziko_df['Date'] =pd.to_datetime(ziko_df['Date'])

# Creating the Schemas/ Customer
customer = ziko_df[['Customer_ID','Customer_Name','Customer_Phone', 'Customer_Email', 'Customer_Address']].copy().drop_duplicates().reset_index(drop=True)
customer.head()

#Product schema
product = ziko_df[['Product_ID','Product_List_Title','Quantity','Unit_Price', 'Discount_Rate',]].copy().drop_duplicates().reset_index(drop=True)

#Transaction Schema
transaction =ziko_df.merge(customer, on=['Customer_ID','Customer_Name','Customer_Phone', 'Customer_Email', 'Customer_Address'], how='left') \
                    .merge(product, on=['Product_ID','Product_List_Title','Quantity','Unit_Price', 'Discount_Rate'], how ='left')\
                    [['Transaction_ID', 'Date','Total_Cost','Sales_Channel','Order_Priority',
                            'Warehouse_Code', 'Ship_Mode', 'Delivery_Status','Customer_Satisfaction', 'Item_Returned',
                            'Return_Reason','Payment_Type', 'Taxable', 'Region', 'Country']]

#Temporal storage of the data
customer.to_csv(r'dataset\customer.csv', index= False)
product.to_csv(r'dataset\product.csv', index= False)
transaction.to_csv(r'dataset\transaction.csv', index= False)

print('Data stored successfully')

#Loading Data
#Azure blob connection
load_dotenv()

connect_str = os.getenv("CONNECT_STR")
blob_service_client = BlobServiceClient.from_connection_string(connect_str)

container_name = os.getenv('CONTAINER_NAME')
container_client = blob_service_client.get_container_client(container_name)


#Create a function that loads data in Azure storage as parquet file
def upload_df_to_blob_as_parquet(df, container_client, blob_name):
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(buffer, blob_type ="BlockBlob", overwrite = True)
    print(f'{blob_name} uploaded successfully')


upload_df_to_blob_as_parquet(customer, container_client, 'rawdata/customer.parquet')
upload_df_to_blob_as_parquet(product, container_client, 'rawdata/product.parquet')
upload_df_to_blob_as_parquet(transaction, container_client, 'rawdata/transaction.parquet')