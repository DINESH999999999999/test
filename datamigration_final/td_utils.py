# -*- coding: utf-8 -*-
"""
Created on Wed Dec 25 16:48:38 2024

@author: manga
"""

import teradatasql
import pandas as pd
import json
print("madhava")

def getcolumninfo(databasename,tablename):
    with open('/media/ssd/python/credentials.json','r+') as config_file:
        cred=json.load(config_file)

    td_host = cred['td_host']
    td_user = cred['td_user']
    td_password = cred['td_password']

    tdcon=teradatasql.connect(
        user=td_user,
        password=td_password,
        host=td_host)
    
    print("Venkateshwara")
    cur=tdcon.cursor()
    sql="""    
    SELECT Coalesce(Trim(DATABASENAME),''), Coalesce(Trim(TABLENAME),''), 
    TRIM(ROW_NUMBER() OVER(PARTITION BY   TABLENAME ,  DATABASENAME ORDER BY COLUMNID )), 
    Coalesce(Trim(COLUMNNAME),''), Trim(Coalesce(COLUMN_DATATYPE,''))
    ,Coalesce(Trim(ColumnLength),''),Coalesce(Trim(ColumnFormat),'') FROM
    (
    select c.tablename ,  c.DATABASENAME , c.COLUMNNAME, c.ColumnLength , c.ColumnFormat ,  CASE c.ColumnType
        WHEN 'BF' THEN 'BYTE('            || TRIM(ColumnLength (FORMAT '-(9)9')) || ')'
        WHEN 'BV' THEN 'VARBYTE('         || TRIM(ColumnLength (FORMAT 'Z(9)9')) || ')'
        WHEN 'CF' THEN 'CHAR('            || TRIM( CASE WHEN ColumnLength * 2 > 64000 THEN 64000 ELSE ColumnLength * 2 END (FORMAT 'Z(9)9')) || ')'
        WHEN 'CV' THEN 'VARCHAR('         || TRIM(64000 (FORMAT 'Z(9)9')) || ')'
        WHEN 'D ' THEN 'DECIMAL('         || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ','
                                          || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')'
        WHEN 'DA' THEN 'INTDATE' /* DATE WAS HERE*/
        WHEN 'F ' THEN 'FLOAT'
        WHEN 'I1' THEN 'BYTEINT'
        WHEN 'I2' THEN 'SMALLINT'
        WHEN 'I8' THEN 'BIGINT'
        WHEN 'I ' THEN 'INTEGER'
        WHEN 'AT' THEN 'TIME('            || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')'
        WHEN 'TS' THEN 'TIMESTAMP('       || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')'
        WHEN 'TZ' THEN 'TIME('            || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')' || ' WITH TIME ZONE'
        WHEN 'SZ' THEN 'TIMESTAMP('       || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')' || ' WITH TIME ZONE'
        WHEN 'YR' THEN 'INTERVAL YEAR('   || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'
        WHEN 'YM' THEN 'INTERVAL YEAR('   || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'      || ' TO MONTH'
        WHEN 'MO' THEN 'INTERVAL MONTH('  || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'
        WHEN 'DY' THEN 'INTERVAL DAY('    || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'
        WHEN 'DH' THEN 'INTERVAL DAY('    || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'      || ' TO HOUR'
        WHEN 'DM' THEN 'INTERVAL DAY('    || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'      || ' TO MINUTE'
        WHEN 'DS' THEN 'INTERVAL DAY('    || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'      || ' TO SECOND('
                                          || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')'
        WHEN 'HR' THEN 'INTERVAL HOUR('   || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'
        WHEN 'HM' THEN 'INTERVAL HOUR('   || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'      || ' TO MINUTE'
        WHEN 'HS' THEN 'INTERVAL HOUR('   || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'      || ' TO SECOND('
                                          || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')'
        WHEN 'MI' THEN 'INTERVAL MINUTE(' || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'
        WHEN 'MS' THEN 'INTERVAL MINUTE(' || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'      || ' TO SECOND('
                                          || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')'
        WHEN 'SC' THEN 'INTERVAL SECOND(' || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ','
                                          || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')'
        WHEN 'BO' THEN 'BLOB('            || TRIM(ColumnLength (FORMAT 'Z(9)9')) || ')'
        WHEN 'CO' THEN 'CLOB('            || TRIM(ColumnLength (FORMAT 'Z(9)9')) || ')'
    
        WHEN 'PD' THEN 'PERIOD(DATE)'
        WHEN 'PM' THEN 'PERIOD(TIMESTAMP('|| TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')' || ' WITH TIME ZONE)'
        WHEN 'PS' THEN 'PERIOD(TIMESTAMP('|| TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || '))'
        WHEN 'PT' THEN 'PERIOD(TIME('     || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || '))'
        WHEN 'PZ' THEN 'PERIOD(TIME('     || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')' || ' WITH TIME ZONE)'
        WHEN 'UT' THEN COALESCE(ColumnUDTName,  '<Unknown> ' || ColumnType)
    
        WHEN '++' THEN 'TD_ANYTYPE'
        WHEN 'N'  THEN 'NUMBER('          || CASE WHEN DecimalTotalDigits = -128 THEN '*' ELSE TRIM(DecimalTotalDigits (FORMAT '-(9)9')) END
                                          || CASE WHEN DecimalFractionalDigits IN (0, -128) THEN '' ELSE ',' || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) END
                                          || ')'
        WHEN 'A1' THEN COALESCE('SYSUDTLIB.' || ColumnUDTName,  '<Unknown> ' || ColumnType)
        WHEN 'AN' THEN COALESCE('SYSUDTLIB.' || ColumnUDTName,  '<Unknown> ' || ColumnType)
    
        WHEN 'JN' THEN 'JSON('            || TRIM(ColumnLength (FORMAT 'Z(9)9')) || ')'
        WHEN 'VA' THEN 'TD_VALIST'
        WHEN 'XM' THEN 'XML'
    
        ELSE '<Unknown> ' || ColumnType
      END  COLUMN_DATATYPE ,  INDEXTYPE,
      CASE INDEXTYPE
    WHEN  'P'     then 'Nonpartitioned primary index'
    WHEN  'Q'     then 'Partitioned primary index'
    WHEN  'S'     then 'Secondary index'
    WHEN  'J'     then 'n index'
    WHEN  'N'    Then 'Hash index'
    WHEN  'K'     then 'Primary key'
    WHEN  'U'     then 'Unique constraint'
    WHEN  'V'     then 'Value-ordered secondary index'
    WHEN  'H'     then 'Hash-ordered ALL covering secondary index'
    WHEN  'O'     then 'Valued-ordered ALL covering secondary index'
    WHEN  'I'      then 'dering column of a composite secondary index'
    WHEN  'G'     then 'Geospatial non-unique secondary index.'
    when 'M'	  then 'Multi column statistics'
    when 'D'	     then 'Derived column partition statistics'
    when '1'    	then 'field1 column of a join or hash index'
    when '2'	    then ' field2 column of a join or hash index'
    END INDEX_TYPE_NAME  ,
    ColumnPosition ,IndexNumber ,
    PartitioningColumn
    , CASE
            WHEN ColumnType IN ('CV', 'CF', 'CO')
            THEN CASE CharType
                    WHEN 1 THEN ' CHARACTER SET LATIN'
                    WHEN 2 THEN ' CHARACTER SET UNICODE'
                    WHEN 3 THEN ' CHARACTER SET KANJISJIS'
                    WHEN 4 THEN ' CHARACTER SET GRAPHIC'
                    WHEN 5 THEN ' CHARACTER SET KANJI1'
                    ELSE ''
                 END
             ELSE ''
          END STRING_TYPE ,COLUMNID
    
    from DBC.columnsV  c
    left join    DBC.IndicesV  i   on   c.tablename=i.tablename AND c.DATABASENAME=i.DATABASENAME  and c.COLUMNNAME=i.COLUMNNAME
    where upper(C.tablename)=upper('{}')
    AND upper(C.DATABASENAME)=upper('{}')
     ) a;
        """.format(tablename, databasename)
    cur.execute(f"{sql}")
    result=cur.fetchall()
    #print(sql)
    return result
    #return [databasename,tablename]

def tdquery(query):
    with open('/media/ssd/python/credentials.json','r+') as config_file:
        cred=json.load(config_file)

    td_host = cred['td_host']
    td_user = cred['td_user']
    td_password = cred['td_password']

    tdcon=teradatasql.connect(
        user=td_user,
        password=td_password,
        host=td_host)
    print("Venkateshwara")
    cur=tdcon.cursor()
    sd=cur.execute(query)
    result=sd.fetchall()
    print(result)
    return result

def tdcount(tddbname,tdtablename):
    #query2=f"SELECT CAST(CURRENT_TIMESTAMP AS VARCHAR(26));"
    #export_start_time=tdquery(query2)[0][0]
    try:
        query1=f"SELECT COUNT(*) FROM {tddbname}.{tdtablename};"
        tdcnt=tdquery(query1)[0][0]
        returncode=0
    except Exception as e:
        returncode=1
        tdcnt=str(e)
        

    #return [returncode,tdcnt,export_start_time]
    return [returncode,tdcnt]
