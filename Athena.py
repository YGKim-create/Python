#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import sys
import io
sys.path.append("/tf/notebooks/libs/")
#sys.path.append('/tf/notebooks/libs/AWS')
import requests
import AWS.Botosession
import time

import pandas as pd
#슬랙세팅
from SendSlack import *
SendSlackSetDefaults("#s3_log", "s3-logger")


# In[ ]:


AWS_REGION_NAME = 'ap-northeast-2'
AWS_ATHENA_RESULT_BUCKETNAME = 'aws-athena-query-results-auto-ap-northeast-2'

def Query(dbname, query):
    global AWS_REGION_NAME
    global AWS_ATHENA_RESULT_BUCKETNAME
    
    session = AWS.Botosession.session()
    credentials = session.get_credentials()
    # Credentials are refreshable, so accessing your access key / secret key
    # separately can lead to a race condition. Use this to get an actual matched
    # set.
    credentials = credentials.get_frozen_credentials()
    access_key = credentials.access_key
    secret_key = credentials.secret_key
    athena = session.client('athena')
    bucketname = AWS_ATHENA_RESULT_BUCKETNAME
    athenaquery_str = query
    SendSlack("[S3 Download] begin query to athena : %s"%athenaquery_str)
    queryStart = athena.start_query_execution(
        QueryString = athenaquery_str,
        QueryExecutionContext = {
            'Database': dbname
        },
        ResultConfiguration={
            'OutputLocation':'s3://%s/'%bucketname
        }
    )
    query_id = queryStart['QueryExecutionId']
    while True:
        qs = athena.get_query_execution(QueryExecutionId=query_id)
        status = qs['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(1)  # 200ms
    SendSlack("finish athena query")
    SendSlack("get result from s3 with %s.csv"%query_id)
    s3r = session.resource('s3')
    file = s3r.Object(bucketname, "%s.csv"%query_id)
    f = file.get()['Body'].read()
    SendSlack("finish get result ... making dataframe")
    df = pd.read_csv(io.BytesIO(f))
    SendSlack("[S3 Download] finish make dataframe with rows = %d"%len(df))
    return df


# In[ ]:


if __name__ == "__main__":
    display(Query('applodata', 'select * from "applodata";'))


# In[ ]:





# In[ ]:




