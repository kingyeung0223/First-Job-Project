import pyodbc_sql_function as sql
from send_email_function import write_email, send_email
from typing import Tuple, List, Dict, Union
import pandas as pd
import datetime
from prettytable import PrettyTable
from sqlalchemy import create_engine
from sqlalchemy.types import NVARCHAR, CHAR

'''The purpose of this program is to form a hierarchy based on geographical location of different real-estates.
The structure of the hierarchy would be as follow: unit --> building --> estate --> hma --> district --> territory

For this purpose, this program would ensure each subsidary in the hierarchy belongs to only a single superior level.
If data from data source violate this rule, a notification email would be sent to the database adminstrator.
'''

#------------------global variable used in defining function---------------------
# sql statement that used to check if data from buildingHma table is valid or not
sql_dist_terr = """SELECT bHMA.District FROM dbk.BuildingHMA bHMA group by District having max(bHMA.Terr) <> min(bHMA.Terr)"""
sql_hma_terr = """SELECT bHMA.HMA, bHMA.District FROM dbk.BuildingHMA bHMA group by HMA, District having max(bHMA.Terr) <> min(bHMA.Terr)"""
sql_hma_dist = """SELECT bHMA.HMA FROM dbk.BuildingHMA bHMA where HMA NOT LIKE '其他%' group by HMA having max(bHMA.District) <> min(bHMA.District)"""
sql_est_terr = """SELECT bHMA.centaest FROM dbk.BuildingHMA bHMA group by centaest having max(bHMA.Terr) <> min(bHMA.Terr)"""
sql_est_dist = """SELECT bHMA.centaest FROM dbk.BuildingHMA bHMA group by centaest having max(bHMA.district) <> min(bHMA.district)"""
sql_est_hma = """SELECT bHMA.centaest FROM dbk.BuildingHMA bHMA group by centaest having max(bHMA.HMA) <> min(bHMA.HMA)"""
sql_bldg_est = """SELECT centabldg FROM dbk.BuildingHMA group by centabldg having max(centaest) <> min(centaest)"""

# a check list that maps the name of different testing conditions to different testing sql statements
checklist_sql_statement_map = {
    'dist_terr': sql_dist_terr,
    'hma_terr': sql_hma_terr,
    'hma_dist': sql_hma_dist,
    'est_terr': sql_est_terr,
    'est_dist': sql_est_dist,
    'est_hma': sql_est_hma,
    'bldg_est': sql_bldg_est
}

# a check list that maps different testing conditions to different error messages
# The error message would be used to construct the email notification
checklist_error_msg_map = {
    'dist_terr': "The following DISTRICT are pointing to multiple TERRITORY:",
    'hma_terr': "The following HMA are pointing to multiple TERRITORY:",
    'hma_dist': "The following HMA are pointing to multiple DISTRICTS:",
    'est_terr': "The following ESTATE are pointing to multiple TERRITORY:",
    'est_dist': "The following ESTATE are pointing to multiple DISTRICTS:",
    'est_hma': "The following ESTATE are pointing to multiple HMA:",
    'bldg_est': "The following BUILDING are pointing to multiple ESTATE:"
}


#--------------------------function definition-----------------------------------------------------------
def ValidateDataByTest(con: Dict[str, str], check_item: str) -> Tuple[bool, str]:
    '''
    Return a bool to indicate if data in BuildingHma pass a single sql-based test
    If not pass, a pretty table in str format would be returned to list all those troublesome cases.
    '''
    sql_string = checklist_sql_statement_map[check_item]
    data, col_name = sql.ExecQuery(con, sql_string, False, False)

    if len(data) == 0:
        # This means a subsidary only belongs to a single superior level
        print("Test Case: {} PASSED".format(check_item))
        return True, None
    else:
        # This means a subsidary belongs to more than one superior units
        print("Test Case: {} FAILED".format(check_item))
        table = PrettyTable(col_name)
        for row in data:
            table.add_row(row)
        return False, table.get_html_string()


def ValidateData(con_dict: Dict[str,str], check_list: Union[Tuple[str],List[str]], win_auth: bool=False) -> Tuple[bool, str]:
    '''
    Return a boolean to indicate whether all tests have been passed
    Return a string which would be the content of the notification email
    '''
    start_time = datetime.datetime.now()
    email_body = """<p>The job for updating HmaHierarchy has been suspended at {}</p>""".format(start_time)
    IsPassAll = True
    for item in check_list:
        is_pass, error_case = ValidateDataByTest(con=con_dict, check_item=item)
        if not is_pass:
            email_body = email_body + '<br/>' + checklist_error_msg_map[item] + '<br/>' + error_case + '<br/>'
            IsPassAll = False
    print("IsPassAll: {}".format(IsPassAll))
    print("Email Body:\n {}".format(email_body))
    return IsPassAll, email_body


def GetBuildingHma(con_dict: Dict[str, str], win_auth: bool=False) -> pd.DataFrame:
    '''Return the Hma data as a pandas DataFrame'''
    # create a temp table with index to speed up the retreival of buildinghma data
    sql_temp_table = """
                        CREATE TABLE #temp_building_hma(
	                        [centaest] [char](10),
	                        [centabldg] [char](10),
	                        [HMACode] [nvarchar](255),
	                        [HMA] [nvarchar](255),
	                        [Street] [nvarchar](250),
	                        [District] [nvarchar](255),
	                        [Terr] [nvarchar](255)
                        );

                        insert into #temp_building_hma
                        select * from dbk.BuildingHMA;

                        create index IX_temp_est_bldg_HMACode_District_Terr on #temp_building_hma (centabldg, centaest, HMACode, District, Terr);
                    """
    con = sql.ExecNonQuerySQL(con_dict, sql_temp_table, True)

    # query to ensure that each sub-unit would only point to 1 super-unit
    sql_string = """
                    with max_cenest as
                    (
	                    select t.centabldg, max(t.centaest) as centaest
	                    from #temp_building_hma t
	                    group by t.centabldg
                    ),
                    hma_list as
                    (
	                    select distinct t.HMACode, t.HMA
	                    from #temp_building_hma t
                    ),
                    max_hma as 
                    (
	                    select t.centaest, max(t.HMACode) as HMACode
	                    from #temp_building_hma t
	                    group by t.centaest
                    ),
                    max_district as
                    (
	                    select t.HMACode, max(t.District) as District
	                    from #temp_building_hma t
	                    group by t.HMACode
                    ),
                    max_terr as
                    (
	                    select t.District, max(t.Terr) as Terr
	                    from #temp_building_hma t
	                    group by t.District
                    )
                    select e.centaest, hma.centabldg, h.HMACode, l.HMA, hma.Street, d.District, t.Terr
                    from #temp_building_hma hma
	                    join max_cenest e on hma.centabldg = e.centabldg
	                    join max_hma h on e.centaest = h.centaest
	                    join max_district d on d.HMACode = h.HMACode
	                    join max_terr t on t.District = d.District
	                    join hma_list l on l.HMACode = h.HMACode
                """
    df = sql.GetDataFromSqlToDF(con, sql_string, False)

    # No need to drop the created temp table because it would be dropped automatically when the connection is closed
    return df


def main():
    start_time = datetime.datetime.now()

    # the list of key that mapped different test cases and error message
    # check global variable for more details
    check_list = ('dist_terr', 'hma_terr', 'hma_dist', 'est_terr', 'est_dist', 'est_hma', 'bldg_est')

    # connect dict used by pyodbc
    con_dict_source = {'driver': 'SQL Server Native Client 11.0',
                       'server': 'source_server',
                       'db': 'source_db',
                       'user': 'user',
                       'pw': 'pw'
                       }

    con_dict_agentext_test = {'driver': 'SQL Server Native Client 11.0',
                              'server': 'source_server',
                              'db': 'source_db',
                              'user': 'user',
                              'pw': 'pw'
                              }

    # connect url used by sqlalchemy engine
    con_url = "mssql+pyodbc://{user}:{pw}@{server}/{db}?driver=SQL+Server+Native+Client+11.0"
    con_url_test = con_url.format(user=con_dict_agentext_test['user'], pw=con_dict_agentext_test['pw'],
                             server=con_dict_agentext_test['server'], db=con_dict_agentext_test['db'])

    # create engine for destination db
    dest_engine_test = create_engine(con_url_test)

    # parameter for email
    subject = "Suspension in Updating HMAHierarchy at {}".format(start_time)
    sender = "server@testing.com"
    to = ["receiver@testing.com"]

    # start validating data in building hma
    pass_all_test, email_body = ValidateData(con_dict_source, check_list, False)

    # send email problem still exists in the data
    if not pass_all_test:
        send_email(write_email(subject, email_body, sender, to, None, None))

    # extract hmahierarchy for insertion
    df_for_insert = GetBuildingHma(con_dict_source, win_auth=False)
    print(df_for_insert)

    table_def = {'centaest': CHAR(10),
                 'centabldg': CHAR(10),
                 'HMACode': NVARCHAR(255),
                 'HMA': NVARCHAR(255),
                 'Street': NVARCHAR(250),
                 'District': NVARCHAR(255),
                 'Terr': NVARCHAR(255)
                 }
 
    sql_create_view = """
                            create view dbo.HmaHierarchy with schemabinding
                            as
	                            SELECT
	                            cu.cuntcode,
	                            LTRIM(RTRIM(cu.y_axis)) AS Floor,
	                            LTRIM(RTRIM(cu.x_axis)) AS Flat,
	                            cb.cblgcode AS cblgcode,
	                            LTRIM(RTRIM(cb.c_property)) AS BuildingChName,
	                            LTRIM(RTRIM(cb.e_property)) AS BuildingEnName,
	                            ce.cestcode AS cestcode,
	                            LTRIM(RTRIM(LTRIM(ce.c_estate))  + RTRIM(LTRIM(cb.c_phase))) AS EstateChName,
	                            LTRIM(RTRIM(LTRIM(ce.e_estate)) + ' ' + RTRIM(LTRIM(cb.e_phase))) AS EstateEnName,
	                            CASE
		                            WHEN bHMA.Street = '' THEN N'未劃分' + '(' + bHMA.District + ')'
		                            WHEN bHMA.Street = N'未劃分' THEN N'未劃分' + '(' + bHMA.District + ')'
		                            WHEN bHMA.HMA = N'其他' THEN bHMA.HMA + '(' + bHMA.District + ')'
		                            ELSE bHMA.HMA
	                            END AS Street,
	                            CASE
		                            -- to distinguish "其他" HMA in different districts
		                            WHEN bHMA.HMA = '其他' THEN bHMA.HMA + '(' + bHMA.District + ')'
		                            ELSE bHMA.HMA
	                            END AS HMA,
	                            bHMA.District AS District,
	                            bHMA.Terr AS Territory
	                            FROM dbo.cenunit cu
		                            JOIN dbo.cenbldg cb ON cb.centabldg = cu.centabldg
		                            JOIN dbo.cenest ce ON ce.centaest = cb.centaest
		                            JOIN import.BuildingHMA bHMA ON bHMA.centabldg = cb.centabldg and bHMA.centaest = ce.centaest
                    """

    sql_cindex = "create unique clustered index UCIX_v_HmaHierarchy on dbo.HmaHierarchy (cuntcode)"
    sql_index = "create index IX_v_HmaHierarchy_Terr_District_HMA_Street_cest_cblg_cunt on dbo.HmaHierarchy (Territory, District, HMA, Street, cestcode, cblgcode, cuntcode) include (EstateChName, BuildingChName, Floor, Flat)"

    print("Start Inserting into Development Environment")
    con = sql.DropView(sql.DelTable(con_dict_agentext_test, 'BuildingHMA', 'import', True), 'HmaHierarchy', 'dbo', False)
    sql.InsertDFtoDB('BuildingHMA', 'import', df_for_insert, table_def, None, dest_engine_test)

    con = sql.ExecNonQuerySQL(con_dict_agentext_test, sql_create_view, True)
    sql.ExecNonQuerySQL(sql.ExecNonQuerySQL(con, sql_cindex, True), sql_index, False)


    print("Start Time: ", start_time)
    print("End Time: ", datetime.datetime.now())

main()
