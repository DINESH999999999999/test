import snowflake.connector
import json
from td_utils import tdquery
from datetime import datetime, timedelta

def sfquery(query):
    with open('/media/ssd/python/credentials.json','r+') as config_file:
        cred=json.load(config_file)

    sf_host = cred['sf_host']
    sf_user = cred['sf_user']
    sf_password = cred['sf_password']
    sf_warehouse = cred['sf_warehouse']
    sf_database = cred['sf_database']
    sf_schema = cred['sf_schema']
    print("RAMA")            

    sfcon = snowflake.connector.connect(
        account=sf_host ,
        user=sf_user, 
        password=sf_password,
        database=sf_database,
        schema=sf_schema,
        warehouse=sf_warehouse)
    
    query=query
     
    print(query)

    with sfcon.cursor() as curr:
        curr.execute(query)
        result=curr.fetchall()
    
    print(result,type(result))
    return result


def create_table(sfdatabasename,sfschemaname,sftablename,uploadfoldername):
    print("JANAKIRAMA",sfdatabasename,sfschemaname,sftablename,uploadfoldername)
    query=f"""DELETE FROM {sfdatabasename}.{sfschemaname}_WRK.{sftablename};"""
    try:
        result=str(sfquery(query))
        returncode=0
        
    except Exception as e:
        returncode=1
        result=str(e)
    
    return [returncode,result]

 
def create_stage(sfdatabasename,sfschemaname,s3_path):
    print("MAHALAKSHMI")

    query=f"""SELECT STAGE_NAME FROM  {sfdatabasename}.INFORMATION_SCHEMA.STAGES WHERE stage_schema = '{sfschemaname}' AND STAGE_NAME = 'S3_{sfdatabasename}_{sfschemaname}';"""
    result=sfquery(query)
    if len(result)==1:
        print("Stage present")
        print(result)
        result=str(result[0][0]) + ' ALREADY EXISTS'
        stagename=f'{sfdatabasename}.{sfschemaname}.S3_{sfdatabasename}_{sfschemaname}'
        returncode=0
    else:
        print(result)
        print("Stage Not present")
        stagename=f'S3_{sfdatabasename}_{sfschemaname}'
        query1=f"""CREATE OR REPLACE STAGE {sfdatabasename}.{sfschemaname}.{stagename}
                URL='{s3_path}'
                STORAGE_INTEGRATION = S3_BUCKET;"""
        print(query1)
        try:
            result=sfquery(query1)
            print(result)
            returncode=0
            stagename=f'{sfdatabasename}.{sfschemaname}.S3_{sfdatabasename}_{sfschemaname}'
        except Exception as e:
            returncode=1
            result=str(e)
            stagename=f'{sfdatabasename}.{sfschemaname}.S3_{sfdatabasename}_{sfschemaname}'
    

    return [returncode,result,stagename]
    '''
        return [returncode,f"""{sfdatabasename}.{sfschemaname}.{stagename}""",result]
    except Exception as e:
        returncode=1
        stagename=f'S3_{sfdatabasename}_{sfschemaname}'
        result=str(e)
        return [returncode,f"""{sfdatabasename}.{sfschemaname}.{stagename}""",result]
'''

def copycommand(stagename,jobdetails,uploadfilename):
    try:

        print("NARAYANA")
        print(jobdetails,uploadfilename)

        tddbname=jobdetails[0]
        tdtablename=jobdetails[1]
        sfdatabasename=jobdetails[2]
        sfschemaname=jobdetails[3]
        sftablename=jobdetails[4]
        delimiter=jobdetails[10]
        s3_path=jobdetails[14]
        uploadfoldername=uploadfilename.replace('.csv','')
        sfwrktable=sfdatabasename+'.'+sfschemaname+'_WRK'+'.'+sftablename

        '''
        uploadfilename='DEMO_USER_IOP_TPT_20250119_0818.csv'
        sfdatabasename='DATAMIGRATION'
        sfschemaname='DEMO_USER'
        sftablename='IOP'
        delimiter=','
        uploadfoldername=uploadfilename.replace('.csv','')
        sfwrktable=sfdatabasename+'.'+sfschemaname+'_WRK'+'.'+sftablename
        '''

        print(tddbname,tdtablename,s3_path,uploadfilename,sfdatabasename,sfschemaname,sftablename,delimiter,uploadfilename,uploadfoldername,sfwrktable)

        print("THIRUVIKRAMA")

        #stagename=create_stage(sfdatabasename,sfschemaname,s3_path)

        copystmnt=fr"""COPY INTO {sfwrktable} FROM @{stagename}/{uploadfoldername}/ FILE_FORMAT = ( TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY = '"', FIELD_DELIMITER = '{delimiter}' )"""

    
        result=str(sfquery(copystmnt))
        returncode=0
        
    except Exception as e:
        returncode=1
        result=str(e)
    
    return [returncode,copystmnt,result]

#copycommand(1,1)

#sfquery('test') 
#sfdatabasename='DATAMIGRATION'
#sfschemaname='DEMO_USER'

#qw=f"""SELECT STAGE_NAME FROM  {sfdatabasename}.INFORMATION_SCHEMA.STAGES WHERE stage_schema = '{sfschemaname}' AND STAGE_NAME = 'S3_{sfdatabasename}_{sfschemaname}';"""
#sd=sfquery(qw)

#a,b,c=create_stage('DATAMIGRATION','DEMO_USER','s3://tdsfbucket/TDEXPORT/DATAMIGRATION/DEMO_USER/')

#print(a,b,c)
'''
def mergecommand(job,export_start_time):
    print("RAGAVA")
    print(job,export_start_time)
''' 


def mergecommand(job):
    print("RAGAVA")
    print(job)
    try:
        sfdatabasename=job[2]
        sfschemaname=job[3]
        sftablename=job[4]
        filter=job[11]
        scd_type=job[6]
        load_type=job[7]
        primarykey=list(job[9].split(","))
        print(primarykey)

        if load_type=='FULL':
            print(load_type)
            
            delstatement=f"DELETE FROM {sfdatabasename}.{sfschemaname}.{sftablename};"
            insstatement=f"INSERT INTO {sfdatabasename}.{sfschemaname}.{sftablename} SELECT * FROM {sfdatabasename}.{sfschemaname}_WRK.{sftablename};"
            print(delstatement)
            print(insstatement)
            try:
                delreturn=sfquery(delstatement)
                insreturn=sfquery(insstatement)
                runstmnt = delstatement +"   \n" + insstatement
                returnstmnt=f"Number Of Records Deleted : {str(delreturn[0][0])} \n Number Of Records Inserted : {str(insreturn[0][0])}"
                returncode=0
            except Exception as e:
                returncode=1
                runstmnt= delstatement +"   \n" + insstatement
                returnstmnt=str(e)


        elif load_type=='FILTER':
            
            delstatement=f"DELETE FROM {sfdatabasename}.{sfschemaname}.{sftablename} WHERE {filter};"
            insstatement=f"INSERT INTO {sfdatabasename}.{sfschemaname}.{sftablename} SELECT * FROM {sfdatabasename}.{sfschemaname}_WRK.{sftablename};"
            print(delstatement)
            print(insstatement)
            try:
                delreturn=sfquery(delstatement)
                insreturn=sfquery(insstatement)
                runstmnt= delstatement +"   \n" + insstatement
                returnstmnt=f"Number Of Records Deleted : {str(delreturn[0][0])} \n Number Of Records Inserted : {str(insreturn[0][0])}"
                returncode=0
            except Exception as e:
                returncode=1
                runstmnt= delstatement +"   \n" + insstatement
                returnstmnt=str(e)

        elif load_type=='INCREMENTAL':
            
            if scd_type==0:
                insstatement=f"INSERT INTO {sfdatabasename}.{sfschemaname}.{sftablename} SELECT * FROM {sfdatabasename}.{sfschemaname}_WRK.{sftablename};"
                try:
                    insreturn=sfquery(insstatement)
                    runstmnt=insstatement
                    returnstmnt=f"Number Of Records Inserted : {str(insreturn[0][0])}"
                    returncode=0
                except Exception as e:
                    returncode=1
                    runstmnt= insstatement
                    returnstmnt=str(e)

            if scd_type==1:
                pkcondition=""
                updstatement=""
                insstatement="("
                valstatement="("

                for i in primarykey:
                    t=f"TARGET.{i}=SOURCE.{i} AND "
                    pkcondition=pkcondition+t
                pkcondition=pkcondition[:-4]

                colliststatement=F"""SELECT COLUMN_NAME
                    FROM {sfdatabasename}.INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = '{sftablename}' and TABLE_SCHEMA='{sfschemaname}' AND TABLE_CATALOG='{sfdatabasename}'
                    ORDER BY ORDINAL_POSITION;"""    
                

                colreturn=sfquery(colliststatement)
                collist=[]
                
                for i in colreturn:
                    collist.append(i[0])
                
                print("Narayana",collist)

                for i in collist:
                    t=f"TARGET.{i}=SOURCE.{i}, "
                    r=f"{i}, "
                    e=f"SOURCE.{i}, "
                    updstatement=updstatement+t
                    insstatement=insstatement+r
                    valstatement=valstatement+e
                updstatement=updstatement[:-2]
                insstatement=insstatement[:-2]+')'
                valstatement=valstatement[:-2]+')'

                merstatement=f"""MERGE INTO {sfdatabasename}.{sfschemaname}.{sftablename} AS TARGET USING {sfdatabasename}.{sfschemaname}_WRK.{sftablename} AS SOURCE ON 
                    {pkcondition}
                    WHEN MATCHED THEN UPDATE SET 
                    {updstatement}
                    WHEN NOT MATCHED THEN INSERT 
                    {insstatement}
                    VALUES
                    {valstatement} ;
                    """
                print(merstatement)
                try:
                    merreturn=sfquery(merstatement)
                    runstmnt=merstatement
                    returnstmnt=f"""Number Of Records Inserted : {str(merreturn[0][0])} \n Number of Records Updated : {str(merreturn[0][1])}"""
                    returncode=0
                except Exception as e:
                    returncode=1
                    runstmnt=merstatement
                    returnstmnt=str(e)



            if scd_type==2:
                pkcondition=""
                updstatement=""
                insstatement="("
                valstatement="("

                for i in primarykey:
                    t=f"TARGET.{i}=SOURCE.{i} AND "
                    pkcondition=pkcondition+t
                pkcondition=pkcondition[:-4]

                colliststatement=F"""SELECT COLUMN_NAME
                    FROM {sfdatabasename}.INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = '{sftablename}' and TABLE_SCHEMA='{sfschemaname}' AND TABLE_CATALOG='{sfdatabasename}'
                    ORDER BY ORDINAL_POSITION;"""    
                
                colreturn=sfquery(colliststatement)
                collist=[]
                
                for i in colreturn:
                    collist.append(i[0])
                
                print("Narayana",collist)

                for i in collist:
                    t=f"TARGET.{i}=SOURCE.{i}, "
                    r=f"{i}, "
                    e=f"SOURCE.{i}, "
                    updstatement=updstatement+t
                    insstatement=insstatement+r
                    valstatement=valstatement+e
                updstatement=updstatement[:-2]
                insstatement=insstatement[:-2]+')'
                valstatement=valstatement[:-2]+')'

                merstatement=f"""MERGE INTO {sfdatabasename}.{sfschemaname}.{sftablename} AS TARGET USING {sfdatabasename}.{sfschemaname}_WRK.{sftablename} AS SOURCE ON 
                    {pkcondition}
                    WHEN MATCHED THEN UPDATE SET 
                    {updstatement}
                    WHEN NOT MATCHED THEN INSERT 
                    {insstatement}
                    VALUES
                    {valstatement} ;
                    """
                print(merstatement)
                try:
                    merreturn=sfquery(merstatement)
                    runstmnt=merstatement
                    returnstmnt=f"""Number Of Records Inserted : {str(merreturn[0][0])} \n Number of Records Updated : {str(merreturn[0][1])}"""
                    returncode=0
                except Exception as e:
                    returncode=1
                    runstmnt=merstatement
                    returnstmnt=str(e)
        return [returncode,runstmnt,returnstmnt]
    except Exception as e:
        returncode=1
        result=str(e)
        runstmnt=""
        print("AACHUTHA")

        return [returncode,runstmnt,returnstmnt]

def getcdcdates(tddbname,tdtablename):
    query2=f"""SELECT CAST(EXTRACTSTARTDTTM AS VARCHAR) AS EXTRACTSTARTDTTM,CAST(EXTRACTENDDTTM AS VARCHAR) AS EXTRACTENDDTTM FROM DATAMIGRATION.DEMO_USER.AUDIT_TABLE WHERE TD_DATABASE_NAME='{tddbname}' and TD_TABLE_NAME='{tdtablename}'"""
    result=sfquery(query2)
    return result

'''
sd=getcdcdates('DEMO_USER','IOP')
print(sd)
'''

def auditupdate(job,export_start_time):

    print(job,export_start_time)
    tddbname=job[0]
    tdtablename=job[1]

    audit_query=f"""UPDATE DATAMIGRATION.DEMO_USER.AUDIT_TABLE SET PREV_EXTRACTSTARTDTTM=EXTRACTSTARTDTTM  , PREV_EXTRACTENDDTTM='{export_start_time}' , EXTRACTSTARTDTTM='{export_start_time}' ,EXTRACTENDDTTM=NULL WHERE TD_DATABASE_NAME='{tddbname}' AND TD_TABLE_NAME='{tdtablename}';"""
    try:
        result=sfquery(audit_query)
        auditstmnt=audit_query
        returnstmnt=f"Number Of Records Updated : {str(result[0][0])}"
        returncode=0
    except Exception as e:
        returncode=1
        auditstmnt=audit_query
        returnstmnt=str(e)
    
    print(audit_query)
    return [returncode,auditstmnt,returnstmnt]

'''
export_start_time='2025-02-06 09:08:04.110000'
tdtablename='IOP'
tddbname='DEMO_USER'
audit_query=f"""UPDATE DATAMIGRATION.DEMO_USER.AUDIT_TABLE SET PREV_EXTRACTSTARTDTTM=EXTRACTSTARTDTTM  , PREV_EXTRACTENDDTTM='{export_start_time}' , EXTRACTSTARTDTTM='{export_start_time}' ,EXTRACTENDDTTM=NULL WHERE TD_DATABASE_NAME='{tddbname}' AND TD_TABLE_NAME='{tdtablename}';"""
print(audit_query)
'''


def sfcount(sfdbname,sfschname,sftablename):
    #query2=f"SELECT CAST(CURRENT_TIMESTAMP AS VARCHAR(26));"
    #job_end_time=tdquery(query2)[0][0]
    #job_end_time=str(datetime.now() - timedelta(hours=5))
    try:
        query1=f"SELECT COUNT(*) FROM {sfdbname}.{sfschname}.{sftablename};"
        sfcnt=sfquery(query1)[0][0]
        returncode=0
    except Exception as e:
        returncode=1
        sfcnt=str(e)
        

    return [returncode,sfcnt]
