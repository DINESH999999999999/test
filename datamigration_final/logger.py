from sf_utils import sfquery
from datetime import datetime, timedelta
 
def batch_create():
    print("NARAYANA")
    query="""INSERT INTO DATAMIGRATION.DEMO_USER.LOG_TABLE (BATCH_ID,JOB_ID, TD_DATABASE_NAME, TD_TABLE_NAME, SF_DATABASE_NAME, SF_SCHEMA_NAME, SF_TABLE_NAME, LOAD_TYPE)
            SELECT COALESCE((SELECT MAX(BATCH_ID) FROM DATAMIGRATION.DEMO_USER.LOG_TABLE)+1,10000),JOB_ID,TD_DATABASE_NAME, TD_TABLE_NAME, SF_DATABASE_NAME, SF_SCHEMA_NAME, SF_TABLE_NAME, LOAD_TYPE FROM DATAMIGRATION.DEMO_USER.CONFIG_TABLE;"""
    sfquery(query)

def log_update(step,stepvalues,batch_id,job_id):
    
    if step == 'tpt_script_generator':
        print('SRIMAHA LAKSHMI')
        print(step,stepvalues,batch_id,job_id)
        print('SRI LAKSHMI NARAYANA')
        content=""
        log=""
        status=""
        if stepvalues[0]==0:
            content=stepvalues[3]
            content=content.replace("'","''")
            log="TPT SCRIPT GENERATED SUCCESSFULLY"
            status="SUCCESS"

 
            updquery=f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE 
                    SET TPT_SCRIPT_NAME = '{stepvalues[2]}' ,TPT_SCRIPT_CONTENT = '{content}', TPT_SCRIPT_LOG = '{log}', TPT_SCRIPT_GENERATION_STATUS = '{status}'
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}
                  """
        else:
            content=stepvalues[3]
            log=stepvalues[1]
            status="FAILED"
            p_status='FAILED IN PREVIOUS STEP'
            job_end_time=str(datetime.now() - timedelta(hours=5))
            updquery=f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE 
                    SET TPT_SCRIPT_NAME = '{stepvalues[2]}' ,TPT_SCRIPT_CONTENT = '{content}', TPT_SCRIPT_LOG = '{log}', TPT_SCRIPT_GENERATION_STATUS = '{status}',
                        TPT_EXPORT_STATUS = '{p_status}',
                        S3_UPLOAD_STATUS = '{p_status}',
                        CREATE_TABLE_STATUS = '{p_status}',
                        COPY_COMMAND_STATUS = '{p_status}',
                        CREATE_STAGE_STATUS = '{p_status}',
                        MERGE_STATEMENT_STATUS = '{p_status}',
                        AUDIT_STATEMENT_STATUS = '{p_status}',
                        JOB_END_TIME = '{job_end_time}',
                        JOB_DURATION = TIMESTAMPDIFF( SECOND , JOB_START_TIME, '{job_end_time}' ),
                        FINAL_STATUS = '{status}' 
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""


        sfquery(updquery)
    
    if step == 'tptexport':
        if stepvalues[0]==0:
            status="SUCCESS"
            log=stepvalues[2].replace("'","''")
        
        
            updquery=f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE 
                    SET TPT_EXPORT_CMD = '{stepvalues[1]}' ,TPT_EXPORT_LOG = '{log}', TPT_EXPORT_STATUS = '{status}'
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""
        
        else:
            status="FAILED"
            log=stepvalues[2].replace("'","''")
            p_status='FAILED IN PREVIOUS STEP'
            job_end_time=str(datetime.now() - timedelta(hours=5))
            
            updquery=f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE 
                    SET TPT_EXPORT_CMD = '{stepvalues[1]}' ,TPT_EXPORT_LOG = '{log}', TPT_EXPORT_STATUS = '{status}',
                        S3_UPLOAD_STATUS = '{p_status}',
                        CREATE_TABLE_STATUS = '{p_status}',
                        COPY_COMMAND_STATUS = '{p_status}',
                        CREATE_STAGE_STATUS = '{p_status}',
                        MERGE_STATEMENT_STATUS = '{p_status}',
                        AUDIT_STATEMENT_STATUS = '{p_status}',
                        JOB_END_TIME = '{job_end_time}',
                        JOB_DURATION = TIMESTAMPDIFF( SECOND , JOB_START_TIME, '{job_end_time}' ),
                        FINAL_STATUS = '{status}' 
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""

        

        print("THYAGARAJA", updquery)
        
        sfquery(updquery)
        print(stepvalues)
    

    if step == 's3upload':
        if stepvalues[0]==0:
            status="SUCCESS"
            cmd=stepvalues[1].replace("'","''")
            log=stepvalues[2].replace("'","''")
        
        
            updquery=f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE 
                    SET S3_UPLOAD_CMD = '{cmd}' ,S3_UPLOAD_LOG = '{log}', S3_UPLOAD_STATUS = '{status}'
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""
        
        else:
            status="FAILED"
            cmd=stepvalues[1].replace("'","''")
            log=stepvalues[2].replace("'","''")
            p_status='FAILED IN PREVIOUS STEP'
            job_end_time=str(datetime.now() - timedelta(hours=5))
            
            updquery=f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE 
                    SET S3_UPLOAD_CMD = '{cmd}' ,S3_UPLOAD_LOG = '{log}', S3_UPLOAD_STATUS = '{status}',
                    CREATE_TABLE_STATUS = '{p_status}',
                        COPY_COMMAND_STATUS = '{p_status}',
                        CREATE_STAGE_STATUS = '{p_status}',
                        MERGE_STATEMENT_STATUS = '{p_status}',
                        AUDIT_STATEMENT_STATUS = '{p_status}',
                        JOB_END_TIME = '{job_end_time}',
                        JOB_DURATION = TIMESTAMPDIFF( SECOND , JOB_START_TIME, '{job_end_time}' ),
                        FINAL_STATUS = '{status}' 
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""




        print("THYAGARAJA", updquery)
        
        sfquery(updquery)
    
    if step == 'create_table':
        if stepvalues[0]==0:
            status="SUCCESS"
            log="WORK TABLE CREATED SUCCESSFULLY"

        
            updquery=f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE 
                    SET CREATE_TABLE_LOG = '{log}' ,CREATE_TABLE_STATUS = '{status}'
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""
        
        else:
            status='FAILED'
            log=stepvalues[1].replace("'","''")
            p_status='FAILED IN PREVIOUS STEP'
            job_end_time=str(datetime.now() - timedelta(hours=5))

            updquery=f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE 
                    SET CREATE_TABLE_LOG = '{log}' ,CREATE_TABLE_STATUS = '{status}',
                    COPY_COMMAND_STATUS = '{p_status}',
                        CREATE_STAGE_STATUS = '{p_status}',
                        MERGE_STATEMENT_STATUS = '{p_status}',
                        AUDIT_STATEMENT_STATUS = '{p_status}',
                        JOB_END_TIME = '{job_end_time}',
                        JOB_DURATION = TIMESTAMPDIFF( SECOND , JOB_START_TIME, '{job_end_time}' ),
                        FINAL_STATUS = '{status}' 
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""

        
        print("THYAGARAJA", updquery)

        sfquery(updquery)
    
    
    
    if step == 'create_stage':
        if stepvalues[0]==0:
            status="SUCCESS"
            stagename=str(stepvalues[1]).replace("'","''")
            log=str(stepvalues[2]).replace("'","''")
        
        
            updquery=f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE 
                    SET CREATE_STAGE_NAME = '{stagename}', CREATE_STAGE_LOG = '{log}' ,CREATE_STAGE_STATUS = '{status}'
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""
        
        else:
            status='FAILED'
            stagename=str(stepvalues[1]).replace("'","''")
            log=str(stepvalues[2]).replace("'","''")

            p_status='FAILED IN PREVIOUS STEP'
            job_end_time=str(datetime.now() - timedelta(hours=5))

            updquery=f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE 
                    SET CREATE_STAGE_NAME = '{stagename}', CREATE_STAGE_LOG = '{log}' ,CREATE_STAGE_STATUS = '{status}',
                    COPY_COMMAND_STATUS = '{p_status}',
                        MERGE_STATEMENT_STATUS = '{p_status}',
                        AUDIT_STATEMENT_STATUS = '{p_status}',
                        JOB_END_TIME = '{job_end_time}',
                        JOB_DURATION = TIMESTAMPDIFF( SECOND , JOB_START_TIME, '{job_end_time}' ),
                        FINAL_STATUS = '{status}' 
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""


        print("THYAGARAJA", updquery)

        sfquery(updquery)


    if step == 'copycommand':
        if stepvalues[0]==0:
            status="SUCCESS"
            copystmnt=stepvalues[1].replace("'","''")
            log=stepvalues[2].replace("'","''")
        
        
            updquery=f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE 
                    SET COPY_COMMAND = '{copystmnt}', COPY_COMMAND_LOG = '{log}' ,COPY_COMMAND_STATUS = '{status}'
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""
        
        else:
            status='FAILED'
            copystmnt=stepvalues[1].replace("'","''")
            log=stepvalues[2].replace("'","''")

            p_status='FAILED IN PREVIOUS STEP'
            job_end_time=str(datetime.now() - timedelta(hours=5))

            updquery=f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE 
                    SET COPY_COMMAND = '{copystmnt}', COPY_COMMAND_LOG = '{log}' ,COPY_COMMAND_STATUS = '{status}',
                        MERGE_STATEMENT_STATUS = '{p_status}',
                        AUDIT_STATEMENT_STATUS = '{p_status}',
                        JOB_END_TIME = '{job_end_time}',
                        JOB_DURATION = TIMESTAMPDIFF( SECOND , JOB_START_TIME, '{job_end_time}' ),
                        FINAL_STATUS = '{status}' 
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""



        print("THYAGARAJA", updquery)

        sfquery(updquery)


    
    if step == 'mergecommand':
        if stepvalues[0]==0:
            status="SUCCESS"
            merstmnt=str(stepvalues[1]).replace("'","''")
            log=str(stepvalues[2]).replace("'","''")
        
        
            updquery=f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE 
                    SET MERGE_STATEMENT = '{merstmnt}', MERGE_STATEMENT_LOG = '{log}' ,MERGE_STATEMENT_STATUS = '{status}'
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""
        
        else:
            status='FAILED'
            merstmnt=str(stepvalues[1]).replace("'","''")
            log=str(stepvalues[2]).replace("'","''")

            p_status='FAILED IN PREVIOUS STEP'
            job_end_time=str(datetime.now() - timedelta(hours=5))

            updquery=f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE 
                    SET MERGE_STATEMENT = '{merstmnt}', MERGE_STATEMENT_LOG = '{log}' ,MERGE_STATEMENT_STATUS = '{status}',
                    AUDIT_STATEMENT_STATUS = '{p_status}',
                        JOB_END_TIME = '{job_end_time}',
                        JOB_DURATION = TIMESTAMPDIFF( SECOND , JOB_START_TIME, '{job_end_time}' ),
                        FINAL_STATUS = '{status}' 
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""

        


        print("THYAGARAJA", updquery)

        sfquery(updquery)
    
    if step == 'auditupdate':
        if stepvalues[0]==0:
            status="SUCCESS"
            auditstmnt=str(stepvalues[1]).replace("'","''")
            log=str(stepvalues[2]).replace("'","''")
        
        
            updquery=f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE 
                    SET AUDIT_STATEMENT = '{auditstmnt}', AUDIT_STATEMENT_LOG = '{log}' ,AUDIT_STATEMENT_STATUS = '{status}'
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""
        
        else:
            status='FAILED'
            auditstmnt=str(stepvalues[1]).replace("'","''")
            log=str(stepvalues[2]).replace("'","''")

            p_status='FAILED IN PREVIOUS STEP'
            job_end_time=str(datetime.now() - timedelta(hours=5))
            
            updquery=f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE 
                    SET AUDIT_STATEMENT = '{auditstmnt}', AUDIT_STATEMENT_LOG = '{log}' ,AUDIT_STATEMENT_STATUS = '{status}',
                    JOB_END_TIME = '{job_end_time}',
                        JOB_DURATION = TIMESTAMPDIFF( SECOND , JOB_START_TIME, '{job_end_time}' ),
                        FINAL_STATUS = '{status}' 
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""

        

        print("THYAGARAJA", updquery)
        sfquery(updquery)
    
    if step == 'tdcount':
        if stepvalues[0]==0:
            status="SUCCESS"
            tdcnt=str(stepvalues[1]).replace("'","''")
            job_start_time=str(datetime.now() - timedelta(hours=5))

        
            updquery=f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE 
                    SET TD_TABLE_COUNT = '{tdcnt}', JOB_START_TIME = '{job_start_time}' 
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""
            
        else:
            f_status='FAILED'
            p_status='FAILED IN PREVIOUS STEP'
            tdcnt=str(stepvalues[1]).replace("'","''")
            job_start_time=str(datetime.now() - timedelta(hours=5))
            job_end_time=str(datetime.now() - timedelta(hours=5))

            updquery=f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE 
                    SET TD_TABLE_COUNT = '{tdcnt}', JOB_START_TIME = '{job_start_time}' ,
                        TPT_SCRIPT_GENERATION_STATUS = '{p_status}',
                        TPT_EXPORT_STATUS = '{p_status}',
                        S3_UPLOAD_STATUS = '{p_status}',
                        CREATE_TABLE_STATUS = '{p_status}',
                        COPY_COMMAND_STATUS = '{p_status}',
                        CREATE_STAGE_STATUS = '{p_status}',
                        MERGE_STATEMENT_STATUS = '{p_status}',
                        AUDIT_STATEMENT_STATUS = '{p_status}',
                        JOB_END_TIME = '{job_end_time}',
                        JOB_DURATION = TIMESTAMPDIFF( SECOND , '{job_start_time}' , '{job_end_time}' ),
                        FINAL_STATUS = '{f_status}' 
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""


        
        print("THYAGARAJA", updquery)
        sfquery(updquery)

    if step == 'sfcount':
        if stepvalues[0]==0:
            status="SUCCESS"
            sfcnt=str(stepvalues[1]).replace("'","''")
            job_end_time=str(datetime.now() - timedelta(hours=5))
        
        
            updquery=f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE 
                    SET SF_TABLE_COUNT = '{sfcnt}', JOB_END_TIME = '{job_end_time}' 
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""
        

        else:
            status='FAILED'
            sfcnt=str(stepvalues[1]).replace("'","''")
            job_end_time=str(datetime.now() - timedelta(hours=5))

            updquery=f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE 
                    SET SF_TABLE_COUNT = '{sfcnt}', JOB_END_TIME = '{job_end_time}' , JOB_DURATION = TIMESTAMPDIFF( SECOND , JOB_START_TIME, '{job_end_time}' ),
                        FINAL_STATUS = '{status}' 
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""

        print("THYAGARAJA", updquery)
        sfquery(updquery)


    if step == 'final_status':
        print(stepvalues[0])
        if stepvalues[0]==0:
            final_status='SUCCESS'
        else:
            final_status='FAILED'

        updquery=f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE 
                    SET FINAL_STATUS = '{final_status}' , JOB_DURATION = TIMESTAMPDIFF( SECOND , JOB_START_TIME, JOB_END_TIME )
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""
        print("THYAGARAJA", updquery)
        sfquery(updquery)