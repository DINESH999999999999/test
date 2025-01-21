import snowflake.connector
def sfquery(query):
    print("RAMA")           

    sfcon = snowflake.connector.connect(
    account='uk04596.central-india.azure' ,
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


def copycommand(jobdetails,uploadfilename):
    print("NARAYANA")
    print(jobdetails,uploadfilename)

    
    sfdatabasename=jobdetails[2]
    sfschemaname=jobdetails[3]
    sftablename=jobdetails[4]
    delimiter=jobdetails[10]
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

    print(uploadfilename,sfdatabasename,sfschemaname,sftablename,delimiter,uploadfilename,uploadfoldername,sfwrktable)

    print("SRIVIKRAMA")

    
    copystmnt=fr"""COPY INTO {sfwrktable} FROM @DATAMIGRATION.DEMO_USER.S3_STAGE/{uploadfoldername}/ FILE_FORMAT = ( TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY = '"', FIELD_DELIMITER = '{delimiter}' )"""
    


    print(copystmnt)
    sfquery(copystmnt)

#copycommand(1,1)

#sfquery('test')