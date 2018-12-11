from typing import Union, Dict, List, NoReturn, Tuple, Optional
import pyodbc
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def StartConnection(driver: str, server: str, db: str, user: str=None, pw: str=None, win_auth: bool=False) -> pyodbc.Connection:
    """
    A function that returns a pyodbc connection object
    """
    if win_auth:
        connect_str = "DRIVER={{{driver}}};SERVER={server};DATABASE={db};Trusted_Connection=yes;"
        connect_str = connect_str.format(driver=driver,
                                         server=server,
                                         db=db)
    else:
        connect_str = "DRIVER={{{driver}}};SERVER={server};DATABASE={db};UID={user};PWD={pw}"
        connect_str = connect_str.format(driver=driver,
                                         server=server,
                                         db=db,
                                         user=user,
                                         pw=pw)

    connection = pyodbc.connect(connect_str)
    print("Finish Connecting to {ip} @ {db}\n".format(ip=server, db=db))
    return connection


def ExecQuery(con: Union[Dict[str, str], pyodbc.Connection], sql_string: str, nextset: bool=False, win_auth: bool=False):
    """
    Given a connection dict or connection and sql string
    Excute query sql statement, return the fetched data set and a list of column name
    """
    if type(con) == dict:
        con = StartConnection(**con, win_auth=win_auth)

    cursor = con.cursor()
    print("Executing SQL statement: {}".format(sql_string))
    cursor.execute(sql_string)

    data = ""
    col_name_list = []
    if not nextset:
        data = cursor.fetchall()
        col_name_list = list(col[0] for col in cursor.description)
    else:
        while cursor.nextset():
            try:
                data = cursor.fetchall()
                col_name_list = list(col[0] for col in cursor.description)
                break
            except pyodbc.ProgrammingError:
                continue
    cursor.close()
    del cursor
    con.close()

    print("Finish Execution")
    print("{} records are found.".format(len(data)))
    msg = "Fields of data: \n"
    for field in enumerate(col_name_list):
        msg = msg + str(field[0]) + ": " + str(field[1]) + "\n"
    print(msg, "\n")
    return data, col_name_list


def FetchQueryResultToDF(data, col_name: List[str]) -> pd.DataFrame:
    """
    A function that can convert query result from pyodbc into pandas dataframe.
    A pandas dataframe and a list of column name would be returned
    """
    result = []
    for row in data:
        to_be_append = []
        for col in row:
            to_be_append.append(col)
        result.append(to_be_append)
    df = pd.DataFrame(result, columns=col_name)
    print(df)
    return df


def GetDataFromSqlToDF(con: Union[Dict[str, str], pyodbc.Connection], sql_string: str, win_auth: bool=False) -> pd.DataFrame:
    """
    A function that retrieve data from sql server db to a pandas dataframe.
    Default function in pandas would be used for simply query.
    For complex query, mannual fetching would be used.
    A pandas dataframe and a list of column name would be returned.
    """
    if type(con) == dict:
        con = StartConnection(con['driver'], con['server'], con['db'], con['user'], con['pw'], win_auth)
    try:
        df = pd.read_sql_query(sql_string, con)
        col_name_list = list(df.columns.get_values())
        
        print("Finish Execution")
        print("{} records are found.".format(len(df)))
        msg = "Fields of data: \n"
        for field in enumerate(col_name_list):
            msg = msg + str(field[0]) + ": " + str(field[1]) + "\n"
        print(msg, "\n")

        con.close()
        return df

    except pyodbc.ProgrammingError:
        data, col = ExecQuery(con, sql_string, True)
        df = FetchQueryResultToDF(data, col)
        con.close()
        return df


def DelTable(con_obj: Union[Dict[str, str], pyodbc.Connection], table: str, schema: str, return_con: bool=False, win_auth: bool=False) -> Optional[pyodbc.Connection]:
    """
    Given a connection object, table name, schema: either a dictionary or a connection,
    this function delete all records in the table.

    An opened connection would be returned and transaction would not be committed if specified.
    Else transaction would be committed and connection would be closed.
    """
    if type(con_obj) == dict:
        con_obj = StartConnection(**con_obj, win_auth=win_auth)
        
    cursor = con_obj.cursor()
    sql_string = "delete [{schema}].[{table}]".format(schema=schema, table=table)

    try:
        cursor.execute(sql_string)
        con_obj.commit()
    except pyodbc.ProgrammingError as e:
        if str(e)[172:176] == '3701':
            print("Table {schema}.{table} is not found.".format(schema=schema, table=table))
            pass
        else:
            raise

    if return_con:
        return con_obj
    else:
        con_obj.close()


def DropView(con_obj: Union[Dict[str, str], pyodbc.Connection], view: str, schema: str, return_con: bool=False, win_auth: bool=False) -> Optional[pyodbc.Connection]:
    """
    Given a connection object, view, schema: either a dictionary or a connection,
    this function delete all records in the table.

    An opened connection would be returned and transaction would not be committed if specified.
    Else transaction would be committed and connection would be closed.
    """
    if type(con_obj) == dict:
        con_obj = StartConnection(**con_obj, win_auth=win_auth)
        
    cursor = con_obj.cursor()
    sql_string = "drop view [{schema}].[{view}]".format(schema=schema, view=view)

    try:
        cursor.execute(sql_string)
        con_obj.commit()
    except pyodbc.ProgrammingError as e:
        if str(e)[172:176] == '3701':
            print("Table {schema}.{view} is not found.".format(schema=schema, table=view))
            pass
        else:
            raise

    if return_con:
        return con_obj
    else:
        con_obj.close()


def ExecNonQuerySQL(con_obj: Union[Dict[str, str], pyodbc.Connection], sql_string: str, return_con: bool=False) -> Optional[pyodbc.Connection]:
    """
    A function that used to execute sql statement that would not return any result-set.
    If specified, an opened connection would be return for reuse,
    else transaction would be commit and the connection would be closed
    """
    if type(con_obj) == dict:
        con_obj = StartConnection(**con_obj, win_auth=False)
   
    cursor = con_obj.cursor()
    cursor.execute(sql_string)
    con_obj.commit()
    
    if return_con:
        return con_obj
    else:
        con_obj.close()


def InsertDFtoDB(table: str, schema: str, dataframe: pd.DataFrame, dtype=None, con=None, engine: Engine=None):
    """
    A function that would append record to an existing table in sql server.
    An sql-alchemy engine or a connection dict could be provided for connection
    """
    if engine is None:
        if con is None:
            raise TypeError('Either con or engine must be provided')
        else:
            con_url = "mssql+pyodbc://{user}:{pw}@{server}/{db}?driver=SQL+Server+Native+Client+11.0"
            con_url = con_url.format(user=con_dict_agentext_pro['user'], pw=con_dict_agentext_pro['pw'],
                             server=con_dict_agentext_pro['server'], db=con_dict_agentext_pro['db'])
            engine = create_engine(con_url)
    
    print("Start Insertion")
    dataframe.to_sql(name=table, con=engine, if_exists='append', index=False, schema=schema, dtype=dtype)
    print("Finish Insertion")
