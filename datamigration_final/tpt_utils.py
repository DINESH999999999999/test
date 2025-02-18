import subprocess
import datetime
from td_utils import getcolumninfo,tdquery
from sf_utils import getcdcdates
import json


def tpt_script_generator(job):
    #for job in configtable:
    try:
        with open('/media/ssd/python/credentials.json','r+') as config_file:
            cred=json.load(config_file)

        td_host = cred['td_host']
        td_user = cred['td_user']
        td_password = cred['td_password']
        tpt_script_path = cred['tpt_script_path']
        tpt_export_path = cred['tpt_export_path']
        tpt_instance_count = cred['tpt_instance_count']


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

        #tptexpdir=r'/media/ssd/exportfiles'
        
        tptexpdir=tpt_export_path
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
         
        if loadtype=='FULL':
            condition="(1=1)"
        
        if loadtype=='FILTER':
            filterconditon=filterconditon.replace("'","''")
            condition="("+filterconditon+")"
        
        if loadtype=='INCREMENTAL':

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
        
        #tptfilename=fr"/media/ssd/tptscripts/{tptjobname}.tpt"
        
        tptfilename=fr"{tpt_script_path}/{tptjobname}.tpt"

        print(tptfilename)
        #tpt_jobs.append([tptfilename,exportfilename,job])
        
        with open(tptfilename, "w") as w:
        ###USING CHARACTER SET UTF8
            sql = f"""
            USING CHARACTER SET UTF8 
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
                    #colstr = "      "+commastr + col[3] + " " + "VARCHAR(10)"+ " \n"
                    colstr = "      "+commastr + col[3] + " " + "ANSIDATE"+ " \n"
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
            VARCHAR TdpId = '{td_host}',
            VARCHAR UserName = '{td_user}',
            VARCHAR UserPassword = '{td_password}',
            VARCHAR SelectStmt = '{tpt_extract_query}'                   
            );             
            /*****************************/ 
            
            /*****************************/
            
            APPLY TO OPERATOR (FILE_WRITER_OPERATOR[{tpt_instance_count}])
            SELECT * FROM OPERATOR (EXPORT_OPERATOR[{tpt_instance_count}]);
                ); 
            /*****************************/
            """
            w.write(sql + " \n")
        

        with open(tptfilename, 'r') as file:
            tptcontent = file.read()
        print("RANGANATHA")
        #print(tptcontent)
        print(tptfilename)
        print(type(tptcontent))
        
        print(export_start_time)
        returncode=0
        errormsg=" "


    except Exception as e:
        print("SRINIVASA")
        returncode=1
        errormsg=e
        tptfilename=""
        tptcontent=""
        exportfilename=""
        export_start_time=""

    print("KRISHNA")
    #print(returncode,errormsg,tptfilename,tptcontent,exportfilename,export_start_time)
    print('KRISHNA')
    return [returncode,errormsg,tptfilename,tptcontent,exportfilename,export_start_time]
        

def tptexport(tptscrptnm,uploadfilename,jobdetails):

    sfdatabasename=jobdetails[2]
    sfschemaname=jobdetails[3]
    sftablename=jobdetails[4]
    delimiter=jobdetails[10]
    
    print("SeethaRama")
    print(tptscrptnm)
    print(uploadfilename)
    print(jobdetails)
    jobchkpoint=uploadfilename.replace(".csv","")

    #cmd=f"tbuild -f {tptscrptnm} -C"
    print(datetime.datetime.now())
    '''
    checkpoint_path = '/opt/teradata/client/20.00/tbuild/checkpoint/'    
    rmcmd = f"""rm '{checkpoint_path}*'"""
    try:
        rmchk = subprocess.run(rmcmd, capture_output=True, text=True)
    except Exception as re:
        print("No Checkpoint File present")
    
    '''
    cmd = ["tbuild", "-f", tptscrptnm, "-j", jobchkpoint, "-e", "UTF-8", "-C"]
    tpt_cmd = ' '.join(cmd)
    #t=subprocess.run(cmd,shell=True,stdout=subprocess.PIPE)
    print("Damodhara")
    print(cmd)
    t=subprocess.run(cmd, capture_output=True, text=True)
    
    print(t.returncode)
    
    print("SriRanga")
    
    print(t.stdout)
    
    return [t.returncode,tpt_cmd,t.stdout]