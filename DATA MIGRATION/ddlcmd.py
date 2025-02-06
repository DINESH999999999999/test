from sfutils import sfquery

def create_table(sfdatabasename,sfschemaname,sftablename,uploadfoldername):
    print("JANAKIRAMA",sfdatabasename,sfschemaname,sftablename,uploadfoldername)
    query=f"""DELETE FROM {sfdatabasename}.{sfschemaname}_WRK.{sftablename};"""
    sfquery(query)