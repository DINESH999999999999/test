import snowflake.connector

def sfquery(query):
    print("RAMA")            

    sfcon = snowflake.connector.connect(
    account='qk97382.ap-southeast-1' ,
    user='DINESHM', 
    password='Govindagovinda@9',
    database='DATAMIGRATION',
    warehouse='COMPUTE_WH')
    
    query=query
    
    print(query)

    with sfcon.cursor() as curr:
        curr.execute(query)
        result=curr.fetchall()
    
    print(result,type(result))
    return result


def create_stage(sfdatabasename,sfschemaname,s3_path):
    print("MAHALAKSHMI")
    query=f"""SELECT STAGE_NAME FROM  {sfdatabasename}.INFORMATION_SCHEMA.STAGES WHERE stage_schema = '{sfschemaname}' AND STAGE_NAME = 'S3_{sfdatabasename}_{sfschemaname}';"""
    result=sfquery(query)
    if len(result)>0:
        print("Stage present")
        print(result)
        return f"""{sfdatabasename}.{sfschemaname}.S3_{sfdatabasename}_{sfschemaname}"""
    else:
        print(result)
        print("Stage Not present")
        stagename=f'S3_{sfdatabasename}_{sfschemaname}'
        query1=f"""CREATE OR REPLACE STAGE {sfdatabasename}.{sfschemaname}.{stagename}
                URL='{s3_path}'
                STORAGE_INTEGRATION = S3_BUCKET;"""
        print(query1)
        result1=sfquery(query1)
        print(result1)
        return f"""{sfdatabasename}.{sfschemaname}.{stagename}"""
    

def copycommand(jobdetails,uploadfilename):
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

    stagename=create_stage(sfdatabasename,sfschemaname,s3_path)

    copystmnt=fr"""COPY INTO {sfwrktable} FROM @{stagename}/{uploadfoldername}/ FILE_FORMAT = ( TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY = '"', FIELD_DELIMITER = '{delimiter}' )"""
    


    print(copystmnt)
    sfquery(copystmnt)

#copycommand(1,1)

#sfquery('test') 
#sfdatabasename='DATAMIGRATION'
#sfschemaname='DEMO_USER'

#qw=f"""SELECT STAGE_NAME FROM  {sfdatabasename}.INFORMATION_SCHEMA.STAGES WHERE stage_schema = '{sfschemaname}' AND STAGE_NAME = 'S3_{sfdatabasename}_{sfschemaname}';"""
#sd=sfquery(qw)

#print(create_stage('DATAMIGRATION','DEMO_USER','s3://tdsfbucket/TDEXPORT/DATAMIGRATION/DEMO_USER/'))

'''
def mergecommand(job,export_start_time):
    print("RAGAVA")
    print(job,export_start_time)
''' 


def mergecommand(job):
    print("RAGAVA")
    print(job)

    sfdatabasename=job[2]
    sfschemaname=job[3]
    sftablename=job[4]
    filter=job[11]
    scd_type=job[6]
    load_type=job[7]
    primarykey=list(job[9].split(","))
    print(primarykey)

    if load_type=='N':
        print(load_type)
        
        delstatement=f"DELETE FROM {sfdatabasename}.{sfschemaname}.{sftablename};"
        insstatement=f"INSERT INTO {sfdatabasename}.{sfschemaname}.{sftablename} SELECT * FROM {sfdatabasename}.{sfschemaname}_WRK.{sftablename};"
        print(delstatement)
        print(insstatement)
        delreturn=sfquery(delstatement)
        insreturn=sfquery(insstatement)

    elif load_type=='F':
         
        delstatement=f"DELETE FROM {sfdatabasename}.{sfschemaname}.{sftablename} WHERE {filter};"
        insstatement=f"INSERT INTO {sfdatabasename}.{sfschemaname}.{sftablename} SELECT * FROM {sfdatabasename}.{sfschemaname}_WRK.{sftablename};"
        print(delstatement)
        print(insstatement)
        delreturn=sfquery(delstatement)
        insreturn=sfquery(insstatement)

    elif load_type=='I':
        
        if scd_type==0:
            insstatement=f"INSERT INTO {sfdatabasename}.{sfschemaname}.{sftablename} SELECT * FROM {sfdatabasename}.{sfschemaname}_WRK.{sftablename};"
            insreturn=sfquery(insstatement)
        
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
            merreturn=sfquery(merstatement)

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
            merreturn=sfquery(merstatement)


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
    sfquery(audit_query)
    print(audit_query)


'''
export_start_time='2025-02-06 09:08:04.110000'
tdtablename='IOP'
tddbname='DEMO_USER'
audit_query=f"""UPDATE DATAMIGRATION.DEMO_USER.AUDIT_TABLE SET PREV_EXTRACTSTARTDTTM=EXTRACTSTARTDTTM  , PREV_EXTRACTENDDTTM='{export_start_time}' , EXTRACTSTARTDTTM='{export_start_time}' ,EXTRACTENDDTTM=NULL WHERE TD_DATABASE_NAME='{tddbname}' AND TD_TABLE_NAME='{tdtablename}';"""
print(audit_query)
'''