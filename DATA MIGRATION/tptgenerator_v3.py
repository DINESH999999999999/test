# -*- coding: utf-8 -*-
"""
Created on Wed Dec 25 15:10:59 2024

@author: manga
"""
import os
import subprocess
import datetime
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import as_completed
import snowflake.connector
import teradatasql
import pandas as pd
from getcolumns import getcolumninfo,tdquery
from awsupload import s3upload
from ddlcmd import create_table
from sfutils import copycommand,getcdcdates,mergecommand,auditupdate

import snowflake.snowpark as snowpark
from snowflake.snowpark import Session


print("kasava")



def tpt_script_generator(job):
    #for job in configtable:
    print("Govinda")
    #print(job)
    tddbname=job[0]
    tdtablename=job[1]
    scdtype=job[6]
    loadtype=job[7]
    cdccol=job[8]
    delimiter=job[10]
    filterconditon=job[11]
    trim=job[12]
    encrpt=job[13]
    export_start_time=tdquery("SELECT CAST(CURRENT_TIMESTAMP AS VARCHAR(26))")[0][0]
    print(export_start_time)
    curr_datetime = str(export_start_time)[:16]
    curr_datetime=curr_datetime.replace(" ","_")
    curr_datetime=curr_datetime.replace(":","")
    curr_datetime=curr_datetime.replace("-","")
    #loadtype='I'
    #cdc='LOAD_DTTM,UPDATE_DTTM,START_DTTM,END_DTTM'
    #scdtype=2

    tptexpdir=r'/media/ssd/exportfiles'
    tptjobname=tddbname+"_"+tdtablename+"_TPT_JOB"
    exportfilename=tddbname+"_"+tdtablename+"_TPT_"+curr_datetime+".csv"
    schemaname=f"TPT_SCH_{tdtablename}"
    selsmnt=""
    
    print(type(encrpt))
    print(tddbname,tdtablename,scdtype,loadtype,cdccol,delimiter,filterconditon,trim,encrpt)
    #print(tpt_jobs)
    
    collist=getcolumninfo(tddbname,tdtablename)
    
    
    for col in collist:
        selsmnt=selsmnt+col[3]+","    
    
    selsmnt="SELECT "+selsmnt
    selsmnt=selsmnt[:-1]
    
    print(selsmnt)
    print(type(collist))
    
    condition=""
    
    if loadtype=='N':
        condition="(1=1)"
    
    if loadtype=='F':
        filterconditon=filterconditon.replace("'","''")
        condition="("+filterconditon+")"
    
    if loadtype=='I':

        cdcdates=getcdcdates(tddbname,tdtablename)
        if len(cdcdates)==0:
            cdcdates=['1900-01-01 00:00:00.000','1900-01-01 00:00:00.000']
            print("Govinda")
        else:
            cdcdates=cdcdates[0]
            print("KRISHNA",cdcdates,type(cdcdates),cdcdates[0][1])
        if scdtype==0:
            #auditcondition=getcolumninfo
            #condition=f"({cdc}>'2024-12-25 23:08:45')"
            print("Thyagaraja",cdccol,type(cdccol))
            
            if str(cdccol)!='None' and len(cdccol)>1:
                condition="("
                clms=cdccol.split(",")
                
                for clmnitem in range(0,len(clms)):
                    condition=condition+f"""(({clms[clmnitem]} >= CAST(''{cdcdates[0]}'' AS TIMESTAMP)) and ({clms[clmnitem]} < CAST(''{export_start_time}'' AS TIMESTAMP))) OR """
                condition=condition[:-4]+")"

            else:
                condition='(1=1)'

        if scdtype==1:
            #auditcondition=getcolumninfo
            #condition=f"({cdc}>'2024-12-25 23:08:45')"
            print("Thyagaraja",cdccol,type(cdccol))
            
            if str(cdccol)!='None' and len(cdccol)>1:
                condition="("
                clms=cdccol.split(",")
                
                for clmnitem in range(0,len(clms)):
                    condition=condition+f"""(({clms[clmnitem]} >= CAST(''{cdcdates[0]}'' AS TIMESTAMP)) and ({clms[clmnitem]} < CAST(''{export_start_time}'' AS TIMESTAMP))) OR """
                condition=condition[:-4]+")"

            else:
                condition='(1=1)'
        
        if scdtype==2:
            #auditcondition=getcolumninfo
            #condition=f"({cdc}>'2024-12-25 23:08:45')"
            print("Thyagaraja",cdccol,type(cdccol))
            
            if str(cdccol)!='None' and len(cdccol)>1:
                condition="("
                clms=cdccol.split(",")
                
                for clmnitem in range(0,len(clms)):
                    condition=condition+f"""(({clms[clmnitem]} >= CAST(''{cdcdates[0]}'' AS TIMESTAMP)) and ({clms[clmnitem]} < CAST(''{export_start_time}'' AS TIMESTAMP))) OR """
                condition=condition[:-4]+")"

            else:
                condition='(1=1)'
    
    print(selsmnt)
    print(condition)
    
    
    tpt_extract_query=selsmnt + f" FROM {tddbname}.{tdtablename} WHERE "+condition+";"
    
    print(tptjobname)
    print(exportfilename)        
    print(tpt_extract_query)
    
    tptfilename=fr"/media/ssd/tptscripts/{tptjobname}.tpt"
    
    print(tptfilename)
    #tpt_jobs.append([tptfilename,exportfilename,job])
    
    with open(tptfilename, "w") as w:
    ###USING CHARACTER SET UTF8
        sql = f"""
        DEFINE JOB {tptjobname}
        DESCRIPTION 'EXPORT TERADATA_SRC'
        (
        /*****************************/ """
        w.write(sql + " \n")
        sql = f"""      DEFINE SCHEMA  {schemaname}  ("""
        w.write(sql + " \n")
        commastr=""
        for col in collist:
            if col[4] in ["DATE", "INTDATE"]:
                #colstr = "      "+commastr + col[3] + " " + "VARDATE(10) FORMATIN('YYYY-MM-DD') FORMATOUT('YYYY-MM-DD')"+ " \n"
                colstr = "      "+commastr + col[3] + " " + "VARCHAR(10)"+ " \n"
            else:
                colstr = "      "+commastr + col[3] + " " + col[4] + " \n"
            commastr=","
            w.write(colstr)
        sql = f"""      );
        /*****************************/ 
        /*****************************/ 
        DEFINE OPERATOR FILE_WRITER_OPERATOR
        DESCRIPTION 'TPT DATA CONNECTOR OPERATOR'
        TYPE DATACONNECTOR CONSUMER
        SCHEMA {schemaname}
        ATTRIBUTES                  
        (                    
        VARCHAR PrivateLogName =  '{tptjobname}_log',
        VARCHAR DIRECTORYPath = '{tptexpdir}',
        VARCHAR FileName = '{exportfilename}',
        VARCHAR IndicatorMode     = 'N',    
        VARCHAR OpenMode          = 'Write',  
        VARCHAR Format            = 'Delimited', 
        VARCHAR TextDelimiter = '{delimiter}' ,
        VARCHAR FileSizeMax = '52428800',
        /*VARCHAR QuotedData = 'Optional'    */      
        VARCHAR QuotedData = 'Yes'          
        );  
        """
            
        w.write(sql + " \n")
        sql = f"""      /*****************************/ 
        DEFINE OPERATOR EXPORT_OPERATOR
        DESCRIPTION 'TPT EXPORT OPERATOR'
        TYPE EXPORT
        SCHEMA {schemaname}
        ATTRIBUTES
        (
        VARCHAR PrivateLogName    =  '{tptjobname}_log',
        INTEGER MaxSessions = 16,
        INTEGER MinSessions = 1,
        VARCHAR TdpId = 'dev-o8ws24dp4xzqbj8b.env.clearscape.teradata.com',
        VARCHAR UserName = 'demo_user',
        VARCHAR UserPassword = 'Govindagovinda@9',
        VARCHAR SelectStmt = '{tpt_extract_query}'                   
        );             
        /*****************************/ 
        
        /*****************************/
        
        APPLY TO OPERATOR (FILE_WRITER_OPERATOR[2])
        SELECT * FROM OPERATOR (EXPORT_OPERATOR[2]);
            ); 
        /*****************************/
"""
        w.write(sql + " \n")
    print(export_start_time)
    return [tptfilename,exportfilename,export_start_time]
        

def tptexport(tptscrptnm,uploadfilename,jobdetails):

    sfdatabasename=jobdetails[2]
    sfschemaname=jobdetails[3]
    sftablename=jobdetails[4]
    delimiter=jobdetails[10]
    
    print("SeethaRama")
    print(tptscrptnm)
    print(uploadfilename)
    print(jobdetails)
    #cmd=f"tbuild -f {tptscrptnm} -C"
    print(datetime.datetime.now())
    cmd = ["tbuild", "-f", tptscrptnm, "-C"]
    #t=subprocess.run(cmd,shell=True,stdout=subprocess.PIPE)
    print("Damodhara")
    print(cmd)
    t=subprocess.run(cmd, capture_output=True, text=True)
    
    print(t.returncode)
    
    print("SriRanga")
    
    print(t.stdout)
    
    
    #print(t.stdout)    


def datamigration(job):

    s3_path=job[14]
    sfdbname=job[2]
    sfschname=job[3]
    sftablename=job[4]

    tptfilename,exportfilename,export_start_time=tpt_script_generator(job)
    
    print("KASAVA")
    
    tptexport(tptfilename,exportfilename,job)

    print("MADHAVA")
    uploadfilename=exportfilename.replace('.csv','')
    print(f"S3 LOAD STARTED FOR : {s3_path},{uploadfilename}")
    print("GOVINDA")
    
    s3upload(s3_path,uploadfilename)

    print(f"S3 UPLOAD COMPLETED FOR :{s3_path},{uploadfilename}")
    print("SRINIVASA")
    
    create_table(sfdbname,sfschname,sftablename,uploadfilename)
    
    print("TABLE CREATION COMPLETED",sfdbname,sfschname,sftablename,uploadfilename)
    print("VARADHA")
    print("COPY COMMAND STARTED FOR :",sftablename)
    
    copycommand(job,uploadfilename)

    print("COPY COMMAND COMPLETED FOR :",sftablename)
    print("RANGA")

    print("SRIMATHA")
    print("MERGE STATEMENT STARTED FOR :",sftablename)
    mergecommand(job)
    print("MERGE STATEMENT COMPLETED FOR :",sftablename)

    print("Srirama")
    print("AUDIT UPDATE STARTED FOR :",sftablename)
    auditupdate(job,export_start_time)
    print("AUDIT UPDATE STARTED FOR :",sftablename)
    return sftablename

if __name__ == "__main__":
    print("SriRama")
    start=time.time()
    sfcon = snowflake.connector.connect(
        account='qk97382.ap-southeast-1' ,
        user='DINESHM', 
        password='Govindagovinda@9',
        database='DATAMIGRATION',
        warehouse='COMPUTE_WH')
    
    spcon = {
    "account": "qk97382.ap-southeast-1",
    "user": "DINESHM",
    "password": "Govindagovinda@9",
    "warehouse": "COMPUTE_WH",
    "database": "DATAMIGRATION"
    }

    query="""SELECT * FROM DATAMIGRATION.DEMO_USER.CONFIG_TABLE;"""
    
    spsession=Session.builder.configs(spcon).create()
    test=spsession.sql(query)
    #print("Sridhara")
    
    #print(list(test.collect()))
    config=test.collect()
    configtable=list(config)

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
    
    with ThreadPoolExecutor() as executor:
        status_code_tpt_scr_gen = {executor.submit(datamigration, job): job for job in configtable}
        for return_code in as_completed(status_code_tpt_scr_gen):
            print(return_code.result(),"Return Code")
            
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
