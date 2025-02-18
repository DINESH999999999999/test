# -*- coding: utf-8 -*-
"""
Created on Wed Dec 25 15:10:59 2024

@author: DINESH_MALLIKARJUNAN
"""
import os
import subprocess
import datetime
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import as_completed
import threading
import snowflake.connector
import teradatasql
import pandas as pd
from tpt_utils import tpt_script_generator,tptexport
from td_utils import getcolumninfo,tdquery,tdcount
from aws_utils import s3upload
from sf_utils import create_table,create_stage,copycommand,getcdcdates,mergecommand,auditupdate,sfcount
from logger import batch_create,log_update
import snowflake.snowpark as snowpark
from snowflake.snowpark import Session
from threading import Lock
import multiprocessing
import shutil
import json

thread_local_data = threading.local()
lock = multiprocessing.Lock()
#lock = threading.Lock()
#lock = Lock()

print("kasava") 


def datamigration(task):
    time.sleep(1)
    job=task
    rc_sum=0
    tddbname=job[0]
    tdtablename=job[1]
    s3_path=job[14]
    sfdbname=job[2]
    sfschname=job[3]
    sftablename=job[4]
    batch_id=job[15]
    job_id=job[16]

    returncode,tdcnt=tdcount(tddbname,tdtablename)
    rc_sum=rc_sum+returncode

    print("RC_SUM",rc_sum)

    log_update('tdcount',[returncode,tdcnt],batch_id,job_id)


    if returncode != 0:
        return sftablename

    returncode,errormsg,tptfilename,tptcontent,exportfilename,export_start_time=tpt_script_generator(job)
    rc_sum=rc_sum+returncode
    print("RC_SUM",rc_sum)
    log_update('tpt_script_generator',[returncode,errormsg,tptfilename,tptcontent],batch_id,job_id)
    
    if returncode != 0:
        return sftablename    


    print("MADHUSUDHANA")
    print(returncode,errormsg)
    print("KASAVA")
    
    returncode,tpt_cmd,stdout=tptexport(tptfilename,exportfilename,job)
    if returncode == 4:
        returncode = 0
    rc_sum=rc_sum+returncode
    print("RC_SUM",rc_sum)
    log_update('tptexport',[returncode,tpt_cmd,stdout],batch_id,job_id)

    if returncode != 0:
        return sftablename

    print("MADHAVA")
    uploadfilename=exportfilename.replace('.csv','')
    print(f"S3 LOAD STARTED FOR : {s3_path},{uploadfilename}")
    print("GOVINDA")
    
    returncode,s3_cmd,s3_log=s3upload(s3_path,uploadfilename)
    rc_sum=rc_sum+returncode
    print("RC_SUM",rc_sum)
    log_update('s3upload',[returncode,s3_cmd,s3_log],batch_id,job_id)
    
    if returncode != 0:
        return sftablename
    
    print(f"S3 UPLOAD COMPLETED FOR :{s3_path},{uploadfilename}")
    print("SRINIVASA")
    
    returncode,result=create_table(sfdbname,sfschname,sftablename,uploadfilename)
    rc_sum=rc_sum+returncode
    print("RC_SUM",rc_sum)
    print("MADHAVA","MADHAVA",result)

    log_update('create_table',[returncode,result],batch_id,job_id)
    
    if returncode != 0:
        return sftablename    

    print("TABLE CREATION COMPLETED",sfdbname,sfschname,sftablename,uploadfilename)
    
    print("KODANDAPANI")

    print("CREAT STAGE STARTED FOR:",sftablename)

    returncode,log,stagename=create_stage(sfdbname,sfschname,s3_path)
    rc_sum=rc_sum+returncode
    print("RC_SUM",rc_sum)
    log_update('create_stage',[returncode,log,stagename],batch_id,job_id)

    if returncode != 0:
        return sftablename

    print("CREAT STAGE COMPLETED FOR:",sftablename)

    print("VARADHA")
    print("COPY COMMAND STARTED FOR :",sftablename)
    
    returncode,copystmnt,result=copycommand(stagename,job,uploadfilename)
    rc_sum=rc_sum+returncode
    print("RC_SUM",rc_sum)
    print(returncode,copystmnt,result)


    log_update('copycommand',[returncode,copystmnt,result],batch_id,job_id)
    
    if returncode != 0:
        return sftablename

    print("COPY COMMAND COMPLETED FOR :",sftablename)
    print("RANGA")
    
    print("SRIMATHA")
    print("MERGE STATEMENT STARTED FOR :",sftablename)
    returncode,merstmnt,result=mergecommand(job)
    print("RC_SUM",rc_sum)
    print("MERGE STATEMENT COMPLETED FOR :",sftablename)
    rc_sum=rc_sum+returncode
    log_update('mergecommand',[returncode,merstmnt,result],batch_id,job_id)
    
    if returncode != 0:
        return sftablename

    print("Srirama")
    print("AUDIT UPDATE STARTED FOR :",sftablename)
    returncode,auditstmnt,result=auditupdate(job,export_start_time)
    rc_sum=rc_sum+returncode
    print("RC_SUM",rc_sum)
    print("AUDIT UPDATE STARTED FOR :",sftablename)

    log_update('auditupdate',[returncode,auditstmnt,result],batch_id,job_id)

    if returncode != 0:
        return sftablename

    returncode,sfcnt=sfcount(sfdbname,sfschname,sftablename)
    rc_sum=rc_sum+returncode
    print("RC_SUM",rc_sum)
    log_update('sfcount',[returncode,sfcnt],batch_id,job_id)

    if returncode != 0:
        return sftablename

    returncode_final=rc_sum
    
    log_update('final_status',[returncode_final],batch_id,job_id)

    return sftablename

if __name__ == "__main__":
    print("SriRama")
    start=time.time()

    with open('/media/ssd/python/credentials.json','r+') as config_file:
        cred=json.load(config_file)

    sf_host = cred['sf_host']
    sf_user = cred['sf_user']
    sf_password = cred['sf_password']
    sf_warehouse = cred['sf_warehouse']
    sf_database = cred['sf_database']
    sf_schema = cred['sf_schema']

    sfcon = snowflake.connector.connect(
        account=sf_host ,
        user=sf_user, 
        password=sf_password,
        database=sf_database,
        schema=sf_schema,
        warehouse=sf_warehouse)
    
    spcon = {
    "account": sf_host,
    "user": sf_user,
    "password": sf_password,
    "warehouse": sf_warehouse,
    "database": sf_database,
    "schema": sf_schema
    }

    #query="""SELECT * FROM DATAMIGRATION.DEMO_USER.CONFIG_TABLE;"""
    query="""SELECT TD_DATABASE_NAME,TD_TABLE_NAME,SF_DATABASE_NAME,SF_SCHEMA_NAME,SF_TABLE_NAME,WAREHOUSE_NAME,SCD_TYPE,LOAD_TYPE,CDC_COLUMNS,PRIMARY_KEY,DELIMITER,FILTER_CONDITION,
            TRIM,ENCRYPTION_COLUMNS,S3_PATH,(SELECT COALESCE((SELECT MAX(BATCH_ID) FROM DATAMIGRATION.DEMO_USER.LOG_TABLE)+1,10000)) AS BATCH_ID,JOB_ID FROM DATAMIGRATION.DEMO_USER.CONFIG_TABLE"""
    
    spsession=Session.builder.configs(spcon).create()
    batch=spsession.sql(query)
    #print("Sridhara")
    
     

    #print(list(test.collect()))
    config=batch.collect()
    configtable=list(config)

    try:
        batch_create()
    except Exception as e:
        print(e)
        print("NOT ABLE TO CREATE LOG")
        exit()


    '''
    config=pd.read_sql(query, sfcon)
    configtable=config.values.tolist()
    '''
    
    #tptlogdir=r"/media/ssd/tptlog"
        
    #tpt_jobs=[]
    
    '''
    with ThreadPoolExecutor() as executor:
        for job in configtable:
            sts=executor.submit(tpt_script_generator,job)
            #print(sts.result(),"Return Code")
        #print("s")
    '''
    
    with ProcessPoolExecutor() as executor:
        status_code_tpt_scr_gen = {executor.submit(datamigration, task): task for task in configtable}
        for return_code in as_completed(status_code_tpt_scr_gen):
            print(return_code.result(),"Return Code")
    

    '''
    for i in configtable:
        res=datamigration(i)
        print(res)

    '''

    #status=tpt_script_generator(configtable)
    #print("Krishna")
    #print(status)
    
    '''
    for tptscrptnm in tpt_jobs:
        print(tptscrptnm)
        #cmd=f"tbuild -f {tptscrptnm} -C"
        cmd = ["tbuild", "-f", tptscrptnm, "-C"]
        #t=subprocess.run(cmd,shell=True,stdout=subprocess.PIPE)
        print("Damodhara")
        print(cmd)
        t=subprocess.run(cmd, capture_output=True, text=True)
        
        print(t.returncode)
        
        print("SriRanga")
        print(t.stdout)
        #print(t.stdout)
    '''
    #EXPORT FILES FROM TERADATA
    
    '''
    with ProcessPoolExecutor() as executor:
        print("Kanna")
        status_code_tpt={executor.submit(tptexport,tptscptnm_filename[0],tptscptnm_filename[1],tptscptnm_filename[2]): tptscptnm_filename for tptscptnm_filename in tpt_jobs}
    '''
    
    '''
    for tptscrptnm in tpt_jobs:
        print("Export started for :" ,tptscrptnm)
        tptexport(tptscrptnm)
        print("Export Completed for :",tptscrptnm)
    '''

    print("Extraction completed")
    #print(tpt_jobs)
    end=time.time()
    print(end-start)
