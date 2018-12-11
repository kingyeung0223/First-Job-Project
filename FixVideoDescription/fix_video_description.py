''' Purpose of this Program is to retrieve updated information from an excel file.
    Then perform update action in the database based on the updated info.'''

import pyodbc_sql_function as sql
import pandas as pd
from excel_file_handler import Excel_Csv_Handler
from typing import Union, List, Dict, Tuple, NoReturn


def ConvertExcelToDF(excel_file_path: str, sheet_name: str) -> pd.DataFrame:
    '''Import data from excel to a pandas dataframe'''
    with pd.ExcelFile(excel_file_path) as excel:
        df = pd.read_excel(excel, sheet_name)
        print(df)
        return df


def UpdateVideoByDF(con_dict: Dict[str, str], df: str, sql_statement: str) -> NoReturn:
    '''Iterate every row in the DataFrame and execute update statement'''
    for row in df.values:
        advertisment_num = row[2]
        video_id = row[7]
        update_statement = sql_statement.format(adv_num=advertisment_num, vdo_id=video_id)

        print("advertisement no: ", advertisment_num)
        print("video_id", video_id)
        print("\n")

        print("Update Statement:\n", update_statement)

        sql.ExecNonQuerySQL(con_dict, update_statement, return_con=False)


def main():
    # Setting Global Variable
    con_dict_dest = {'driver': 'SQL Server Native Client 11.0',
                       'server': '59.152.202.130',
                       'db': 'CPN',
                       'user': 'webaccess',
                       'pw': 'webaccess'}

    #sql_statement = """
     #               update dbo.video
      #              set [video_description] = v.[video_description] + char(13) + char(13) + N'拍攝日期: ' + replace(CONVERT(VARCHAR(10), v.ShootDate, 111), '/', '-') + char(13) + N'廣告日期: ' + replace(CONVERT(VARCHAR(10), m.video_date, 111), '/', '-'),
	   #                 [video_title] = [video_title] + ' (' + N'物業編號: ' + '{adv_num}' + ')'
        #            from dbo.video v
         #           join dbo.menu m on v.menu_ID = m.menu_ID
          #          where v.type = 0 and v.code <> '' and v.video_ID = '{vdo_id}'
    #"""

    sql_statement = "select top 100 * from dbo.video"

    file_path = './video.xlsx'
    sheet_name = '工作表1'


    df = ConvertExcelToDF(file_path, sheet_name)
    UpdateVideoByDF(con_dict_dest, df, sql_statement)

main()