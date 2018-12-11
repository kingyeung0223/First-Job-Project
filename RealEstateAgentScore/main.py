''''Purpose of this program is to find out which agent is the first one who get the form3 / form5 for each cenunit from Aplus System Database
    Then, the obtained list of cenunits and agents would be export to another database for score calculation
'''

import pyodbc_sql_function as sql
import pandas as pd
import datetime
from sqlalchemy import create_engine
from sqlalchemy.types import SMALLINT, NVARCHAR  
from typing import Dict, List, Union, Tuple, NoReturn, Optional


def main():
    start_time = datetime.datetime.now()
    con_dict_source1 = {'driver': 'SQL Server Native Client 11.0',
                          'server': 'source1_server',
                          'db': 'source1_db',
                          'user': 'user',
                          'pw': 'pw'
                          }


    con_dict_source2 = {'driver': 'SQL Server Native Client 11.0',
                         'server': 'source2_server',
                         'db': 'source2_db',
                         'user': 'user',
                         'pw': 'pw'
                         }

    con_dict_dest = {'driver': 'SQL Server Native Client 11.0',
                        'server': 'destination_server',
                        'db': 'destination_db',
                        'user': 'user',
                        'pw': 'pw'
                        }

    schema_def = {'cuntcode': NVARCHAR(40),
                  'StatusCategory': SMALLINT(),
                  'StatusCategoryName': NVARCHAR(40),
                  'FirstForm3AgentNo': NVARCHAR(32),
                  'FirstForm5AgentNo': NVARCHAR(32),
                  'OnlyTrustForm3AgentNo': NVARCHAR(32),
                  'OnlyTrustForm5AgentNo': NVARCHAR(32),
                  'KeyAgentNo': NVARCHAR(32),
        }


    # start getting data from source1_server
    print("Getting centascore data from source1")
    sql_string = "execute dbo.ExportCentaScoreData"
    key_form_data = sql.GetDataFromSqlToDF(con_dict_source1, sql_string, win_auth=False)
    
    # start getting currently posted real-estate's code from source2_server
    print("Getting cuntcode from source2")
    sql_string = "SELECT P.cuntcode FROM oto.Post P GROUP BY P.cuntcode"
    cuntcode = sql.GetDataFromSqlToDF(con_dict_source2, sql_string, win_auth=False)
    
    # join data from the 2 sources on cuntcode
    df = key_form_data.join(cuntcode.set_index('cuntcode'), how='inner', on='cuntcode', lsuffix='_left', rsuffix='_right')
    print("Finish Joining")
    print(df)
    
    # start insertion to db
    ctpost_engine_dev = create_engine("mssql+pyodbc://user:pw@destination_server/destionation_db?driver=SQL+Server+Native+Client+11.0")
    sql.DelTable(con_dict_dest, 'DataForScore', 'centanet', False, False)
    sql.InsertDFtoDB(table='DataForScore', schema='centanet', dataframe=df, dtype=schema_def, con=None, engine=ctpost_engine_dev)

    print("Start Time: ", start_time)
    print("End Time: ", datetime.datetime.now())

main()