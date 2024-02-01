import boto3
import io
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import json


# ==================== yandex config ====================
with open('passwords.json', 'r') as j:
    pswrd = json.loads(j.read())
    
ya_service_name = 's3'
ya_url = 'https://storage.yandexcloud.net'
ya_key_id = pswrd['key_id']
ya_key = pswrd['key']
container_name = 'spardata'

session = boto3.session.Session()


# ==================== functions ==================== 

def put_dfto_cloud(data, container_name, key_name):
    '''
        Uploading data to the cloud
            data -- pd.DataFrame, data to be saved
            container_name -- str, buket name
            key_name -- str, file way and name 
    '''
    s3 = session.client(
        service_name=ya_service_name,
        endpoint_url=ya_url,
        aws_access_key_id = ya_key_id,
        aws_secret_access_key = ya_key
    )
    with io.StringIO() as csv_buffer:
        data.to_csv(csv_buffer, index=False)
        response = s3.put_object(
            Bucket=container_name,
            Key=key_name,
            Body=csv_buffer.getvalue()
        )
        

def reader_csv(container_name, key_name, is_header=True):
    '''
        Get data from storage for a single operation:
          container_name -- str, container name in storage
          key_name -- str, way to data in storage
          is_header -- bool, if TRUE header in data exist, FALSE header=None
        Return: DataFrame
    '''
    s3 = session.client(
        service_name=ya_service_name,
        endpoint_url=ya_url,
        aws_access_key_id = ya_key_id,
        aws_secret_access_key = ya_key
    )
    response = s3.get_object(Bucket=container_name, 
                             Key=key_name)
    if is_header:
        df = pd.read_csv(response.get("Body"))
    else:
        df = pd.read_csv(response.get("Body"), header=None)
    return df


def list_objects_in_container(container_name, prefix):
    '''
        Get all object names in bucket with need prefix:
            container_name -- str, bucket name
            prefix -- str, prefix name
        Return: List
    '''
    s3 = session.client(
        service_name=ya_service_name,
        endpoint_url=ya_url,
        aws_access_key_id = ya_key_id,
        aws_secret_access_key = ya_key
    )
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=container_name, Prefix=prefix)
    files = []
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                files += [obj['Key']]
    return files


def read_FilesbyPrfx_concat(container_name, prefix, is_header=True, concurr=True):
    '''
        Reading all files from the container/buket by a specific prefix
            container_name -- str, container name in storage
            prefix -- str, prefix -- str, prefix name
            is_header -- bool, if TRUE header in data exist, FALSE header=None 
            concurr -- bool, TRUE - use conThreadPoolExecutor
        Return: pd.DataFrame
    '''
    # get all need ibject ways by prefix
    obj_list = list_objects_in_container(container_name, prefix)
    # construct reader
    s3_task = lambda x: reader_csv(container_name=container_name, key_name=x, is_header=is_header)
    # if use concurrent reading 
    data = []
    if concurr:
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(s3_task, f) for f in obj_list]
            for s3_thread in as_completed(futures):
                data += [s3_thread.result()]
    # is use standart reading 
    else:
        data = [s3_task(f) for f in obj_list]

    data = pd.concat(data)
    return data