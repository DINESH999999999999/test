import subprocess
import os
import glob
import json
print("Padmanabha")


#s3upload('s3://tdsfbucket/TDEXPORT/DATAMIGRATION/DEMO_USER/','DEMO_USER_QWE_TPT_20250208_2235')

def s3upload(s3_path,filename):

    with open('/media/ssd/python/credentials.json','r+') as config_file:
        cred=json.load(config_file)
    
    tpt_export_path = cred['tpt_export_path']

    cmd=f"""aws s3 cp '{tpt_export_path}/' '{s3_path}{filename}/' '--recursive' '--exclude' '*' '--include' '*{filename}*'"""
    print("krisha")
    print(cmd)
    t=subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print(t.returncode)
    #print(t.stdout)
    if t.returncode==0:
        log=t.stdout
        print(t.stderr)
        
        print("madhava")

        uploaded_files_txt=""
        uploaded_files=[]
        uploaded_log=log.split('\n')
        for i in uploaded_log:
            #print("DHAMODAHARA")

            if 'upload:' in i:
                uploaded_files_txt=uploaded_files_txt+'\n'+i
                uploaded_files.append(i)
        header=f"Number of files uploaded: {len(uploaded_files)}"
  
        print("KRISHNA")
        s3_log=f"{header} \n{uploaded_files_txt}"

        print(s3_log)
        return [t.returncode,cmd,s3_log]

    else:
        print("")
        print(t.stderr)
        print(t.stdout)
        return [t.returncode,cmd,t.stderr]

#s3upload('s3://tdsfbucket/TDEXPORT/DATAMIGRATION/DEMO_USER/','DEMO_USER_QWE_TPT_20250208_2235')


