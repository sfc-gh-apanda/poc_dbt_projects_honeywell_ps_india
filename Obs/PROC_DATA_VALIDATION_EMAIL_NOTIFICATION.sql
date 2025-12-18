CREATE OR REPLACE PROCEDURE EDW.CORP_DATA_VALIDATE.PROC_DATA_VALIDATION_EMAIL_NOTIFICATION("MODULE_NAME" VARCHAR(100))
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python','pandas')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

import snowflake.snowpark as snowpark
import pandas as pd

def send_email(
    session: snowpark.Session,
    email_integration: str,
    distribution_list: str,
    distribution_list_default: str,
    subject_line: str,
    email_body: str
) -> bool:
    try:
        success: bool = session.call(
            "SYSTEM$SEND_EMAIL",
            email_integration,
            distribution_list,
            subject_line,
            email_body,
            "text/html"
        )
    except:
        success: bool = session.call(
            "SYSTEM$SEND_EMAIL",
            email_integration,
            distribution_list_default,
            subject_line,
            email_body,
            "text/html"
        )
    return success

def main(session: snowpark.Session, module_name: str) -> str:
    try:
        report_date = str(session.sql("SELECT DATE(CONVERT_TIMEZONE(''EST'', CURRENT_TIMESTAMP)) AS REPORT_DATE").to_pandas().iloc[0]["REPORT_DATE"])
        account_name = session.sql("SELECT CURRENT_ACCOUNT_NAME() AS ACCOUNT_NAME").to_pandas().iloc[0]["ACCOUNT_NAME"]
        database_name = session.sql("SELECT CURRENT_DATABASE() AS DATABASE_NAME").to_pandas().iloc[0]["DATABASE_NAME"]
        email_integration = "EMAIL_NOTIFICATION_UTILITY"
        config_table = "CORP_DATA_VALIDATE.TV_XREF_EMAIL_NOTIFICATION_CONFIG"
        results_table = "CORP_DATA_VALIDATE.DATA_VALIDATION_RESULTS_SUMMARY"
        results_table_view = "CORP_DATA_VALIDATE.TV_DATA_VALIDATION_RESULTS_SUMMARY"
        
        
        if account_name == "HONEYWELLFORGE":
            env_name = "Non-EC Prod"
        elif account_name == "HONEYWELLFORGE_NONPROD" and database_name == "EDW":
            env_name = "Non-EC QA"
        elif account_name == "HONEYWELLFORGE_NONPROD" and database_name == "DEV":
            env_name = "Non-EC Dev"
        else:
            env_name = "Unspecified Environment"
        
        config_df: pd.DataFrame = session.table(config_table).filter(snowpark.functions.col("MODULE_NAME") == module_name).to_pandas()
        if config_df.empty:
            raise Exception("Module name: " + module_name + " does not exist in the config table [" + config_table + "].")
        
        view_name = config_df.iloc[0]["VIEW_NAME"]
        distribution_list = config_df.iloc[0]["DISTRIBUTION_LIST"]
        distribution_list_default = config_df.iloc[0]["DISTRIBUTION_LIST_DEFAULT"]
        success_message = config_df.iloc[0]["SUCCESS_MESSAGE"]
        failure_message = config_df.iloc[0]["FAILURE_MESSAGE"]
        stop_on_error = config_df.iloc[0]["STOP_ON_ERROR"]
        
        sql_prev_validation_id = "SELECT MAX(VALIDATION_ID) AS MAX_VALIDATION_ID FROM " + results_table_view + " WHERE MODULE_NAME = ''" + module_name + "'' AND VALIDATION_DATE = ''" + report_date + "''"
        sql_results = "SELECT * FROM " + view_name
        sql_query_id = "SELECT LAST_QUERY_ID() AS QUERY_ID"
        
        prev_validation_id = int(session.sql(sql_prev_validation_id).to_pandas().fillna(0).iloc[0]["MAX_VALIDATION_ID"])
        results_df: pd.DataFrame = session.sql(sql_results).to_pandas()
        query_id = str(session.sql(sql_query_id).to_pandas().iloc[0]["QUERY_ID"])
        
        column_list = list(results_df.columns.values)
        json_format = ""
        for column in column_list:
            json_format += " ''" + column + "''," + column + ","
        json_format = json_format[1:-1]
        
        sql_insert_results = "INSERT INTO " + results_table + " ( MODULE_NAME, VALIDATION_DATE, VALIDATION_ID, QUERY_ID, QUERY_RESULT, LOAD_TS, UPDATE_TS ) SELECT ''" + module_name + "'' AS MODULE_NAME, ''" + report_date + "'' AS VALIDATION_DATE, " + str(prev_validation_id+1) + " AS VALIDATION_ID, ''" + query_id + "'' AS QUERY_ID, OBJECT_CONSTRUCT(" + json_format + ") AS QUERY_RESULT, CURRENT_TIMESTAMP AS LOAD_TS, CURRENT_TIMESTAMP AS UPDATE_TS FROM " + view_name
        insert_results = str(session.sql(sql_insert_results).collect())
        
    except Exception as err:
        if stop_on_error or config_df.empty:
            raise err
        else:
            return err
    
    try:
        column_list.remove("HIGHLIGHT_FLAG")
        highlight_flag = max(results_df["HIGHLIGHT_FLAG"])
        results_df["BGCOLOR"] = results_df.apply(lambda row: "yellow" if row["HIGHLIGHT_FLAG"] else "white", axis=1)
    except:
        highlight_flag = False
        results_df["BGCOLOR"] = "white"
    
    if results_df.empty and view_name == "CORP_DATA_VALIDATE.VW_FIN_ORDER_METRICS_ZERO_BACKLOG":
        message = success_message
        subject_line = "[" + env_name + "] " +" Audit – Success : " + config_df.iloc[0]["SUBJECT_LINE"] 
    elif results_df.empty and view_name == "CORP_DATA_VALIDATE.VW_DIM_ORG_SBG_GBE_VALIDATION":
        message = success_message
        subject_line = "[" + env_name + "] " +" Audit – Success : " + config_df.iloc[0]["SUBJECT_LINE"] 
    elif results_df.empty:
        message = "<b>Error:</b> No data was fetched from the data validation script [" + view_name + "]. Please check the script."
    elif highlight_flag:
        message = failure_message
        subject_line = "[" + env_name + "] " +" Audit – Fail : " + config_df.iloc[0]["SUBJECT_LINE"] 
    else:
        message = success_message
        subject_line = "[" + env_name + "] " +" Audit – Success : " + config_df.iloc[0]["SUBJECT_LINE"] 
    
    
    email_body: str = f''''''
        <html>
            <body>
                <p>Hi All,</p>
                <p>'''''' + message + ''''''</p>
                <p></p>
                <table border="1">
                    <tr>
    ''''''
    
    for column in column_list:
        email_body += f''''''
                        <th align="left">'''''' + column + ''''''</th>
        ''''''
    
    email_body += f''''''
                    </tr>
    '''''';
    
    for index, row in results_df.iterrows():
        email_body += f''''''
                    <tr bgcolor='''''' + row["BGCOLOR"] + ''''''>
        ''''''
        for column in column_list:
            email_body += f''''''
                        <td>'''''' + str(row[column]) + ''''''</td>
            ''''''
        email_body += f''''''
                    </tr>
        ''''''
    
    email_body += f''''''
                </table>
                <p></p>
                <p>Thanks,<br>EDW Team</p>
            </body>
    </html>
    ''''''
    
    if stop_on_error and (results_df.empty or highlight_flag):
        success = send_email(session, email_integration, distribution_list, distribution_list_default, subject_line, email_body)
        raise Exception(message)
    
    success = send_email(session, email_integration, distribution_list, distribution_list_default, subject_line, email_body)
    
    return "Report Published Successfully. " + insert_results if success else "There is a problem while sending the email. Please check if the DISTRIBUTION_LIST is valid."
    
';