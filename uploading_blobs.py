import time
import pyodbc
import pandas as pd
import os, uuid, sys
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

def upload_data():
    
    #Set path to where you are storing the data as a Environment variable like C:\\Users\\User\\Desktop\\PROJECT\\Sensor_data\\
    #Then GET the path
    directory = os.environ.get('DATA_PATH') 
    

    #Create Client
    #Obtain connection string from Azure Portal and set as environnment variable the GET it
    CONN = os.environ.get('CONN_STRING')
    service = BlobServiceClient.from_connection_string(conn_str=CONN)

    for filename in os.listdir(directory):
        if filename.startswith("standing"): activity = 'standing'
        elif filename.startswith("walking"): activity  = 'walking'
        elif filename.startswith("sitting") : activity  = 'sitting'
        else: activity  = 'laying'
        
        if filename.endswith("accelerometer.csv"):
            
            #Upload Accelorometer Data
            acc_data_path = directory + filename
            blob_name = 'acc/'+filename
            blob = BlobClient.from_connection_string(conn_str=CONN, container_name = activity, blob_name = blob_name)
            
            data_path = r'{}'.format(acc_data_path)
            with open(data_path, "rb") as data:
                    blob.upload_blob(data)
        else:
    
            #Upload Gyroscope Data
            gyr_data_path = directory + filename
            blob_name = 'gyr/'+filename
            blob = BlobClient.from_connection_string(conn_str=CONN, container_name = activity, blob_name = blob_name)
            
            data_path = r'{}'.format(gyr_data_path)
            with open(data_path, "rb") as data:
                    blob.upload_blob(data)
        os.remove(acc_data_path)
        os.remove(gyr_data_path)
    print("Sucessfully uploaded!")
    
    



    
    
def run():
    
    proceed = True

    prompt = """
    \n Uploading data to Blob Storage...
    """

    while proceed:
    

        try:
                upload_data()
                proceed = False
        except Exception as message:
                print("Upload to storage failed terribly due to\n", message)
                choice = input('Repeat or exit\n1. Try again\n2. Exit\n')
                if choice != '1':
                    print("Exiting...")
                    proceed = False


run()   
