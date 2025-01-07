import subprocess
import os
import glob

print("Padmanabha")


def s3upload(filename):
    cmd=['aws', 's3', 'cp', '/media/ssd/exportfiles', f's3://tdsfbucket/TDEXPORT/{filename}/', '--recursive', '--exclude', '*', '--include', f'*{filename}*']
    print("krisha")
    print(cmd)
    t=subprocess.run(cmd, capture_output=True, text=True)
    print(t.returncode)
    #print(t.stdout)
    print(t.stderr)
    print("madhava")


