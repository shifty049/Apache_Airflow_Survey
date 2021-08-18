import sys
sys.path.append('/home/shifty/data_etl/sub_functions')
import pytz
from datetime import datetime
from GCP_GoogleSheet.GoogleSheetHandler import GoogleSheetHandler
from Slack.SlackHandler import SlackHandler
from airflow import AirflowException

#############################################################################################################################
# send_starting_slack_message (Activated)

def send_starting_slack_message(project_id, slack_channel = 'log-test', slack_proxy = 'CORP', **context):
    '''
    purpose             : used for notifying of the start of an assigned etl job via Slack message
    param project_id    : project id recorded at GooghleSheets
    param slack_channel : assigned slack channel for sending message with default = log-test
    param slack_proxy   : proxy used for slack api with default = CORP
    
    '''
    tw = pytz.timezone('Asia/Taipei') 
    
    starting_timestamp = datetime.now()
    
    slk = SlackHandler( slack_proxy = slack_proxy,
                        slack_channel = 'log-test')
    
    # save starting timestamp for calculating overall processing time
    context['ti'].xcom_push(key = 'starting_time', value = starting_timestamp)
    
    attachments = [
                      {
                          "fallback": "Started AutoMailing ETL job: {}".format(project_id),
                          "color": "#2eb886",
                          "author_name": "Shifty Hsu",
                          "title": "Succeeded to start process: {}".format(project_id),
                          "text" : starting_timestamp.astimezone(tw).strftime('%Y-%m-%d %H:%M:%S %Z%z'),
                                                                    
                      }
                  ]
    
    is_process_suecceed = slk.PostMessage(slack_channel,
                                          '',
                                            username = None,
                                            stopword = None,
                                            attachments = attachments)
    if not is_process_suecceed:
        raise AirflowException

#############################################################################################################################
# send_ending_slack_message (Activated)

def send_ending_slack_message(project_id, slack_channel = 'log-test', slack_proxy = 'CORP', **context):
    '''
    purpose             : used for notifying of the end of an assigned etl job via Slack message
    param project_id    : project id recorded at GooghleSheets
    param slack_channel : assigned slack channel for sending message with default = log-test
    param slack_proxy   : proxy used for slack api with default = CORP
    '''
    tw = pytz.timezone('Asia/Taipei') 
    
    ending_timestamp = datetime.now()
    
    slk = SlackHandler( slack_proxy = slack_proxy,
                        slack_channel = 'log-test')
    
    time_duration = ending_timestamp - context['ti'].xcom_pull(key = 'starting_time', task_ids = 'send_slack_starting_message')
    
    attachments = [
                      {
                          "fallback": "Finished AutoMailing ETL job: {}".format(project_id),
                          "color": "#2eb886",
                          "author_name": "Shifty Hsu",
                          "title": "Succeeded to finish process: {} using time: {}".format(project_id, time_duration),
                          "text" : ending_timestamp.astimezone(tw).strftime('%Y-%m-%d %H:%M:%S %Z%z'),
                                                                    
                      }
                  ]
    
    is_process_suecceed = slk.PostMessage(slack_channel,
                                            '',
                                            username = None,
                                            stopword = None,
                                            attachments = attachments)
    if not is_process_suecceed:
        raise AirflowException

#############################################################################################################################
# refresh_tableau_view_page (Activated)

def refresh_tableau_view_page(slack_proxy = 'CORP', tableau_proxy = None, **context):
    '''
    purpose             : used for refreshing assigned view page on Tableau Server
    param slack_proxy   : proxy used for slack api with default = CORP
    param tableau_proxy : proxy used for selenium with default = None
    '''
    
    is_process_succeed = False
    
    return_stdout = context['ti'].xcom_pull(key = 'return_value', task_ids = 'acquire_automailing_info_via_docker')
    
    data_dict = eval(return_stdout[0].split('\n')[-1])
    
    from Refresh_View.ViewRefresher import ViewRefresher
    
    view_refresh = ViewRefresher(geckodriver_path = '/home/hduser/geckodriver', 
                                 tableau_proxy = None, 
                                 slack_proxy = slack_proxy, 
                                 slack_channel = 'log-test')
                        
    # if succeed to login, then refresh view page for downloaded as png image
    if view_refresh.Login():
        is_process_succeed = view_refresh.Refresh('{}/#/views/{}'.format(view_refresh.key_dict['tableau']['url'],
                                                                         data_dict['View URL']))

    # close gecko driver
    view_refresh.driver.quit() 
    
    context['ti'].xcom_push(key = 'automailing_information', value = data_dict)
    
    if not is_process_succeed:
        raise AirflowException

#############################################################################################################################
# refresh_tableau_view_page (Activated)

def retrieve_snapshot_from_tableau(project_id, slack_proxy = 'CORP', s3_proxy = 'AWS', **context):
    '''
    purpose             : used for retrieving assigned view page's snapshot image from Tableau Server
    param project_id    : project id recorded at GooghleSheets
    param slack_proxy   : proxy used for slack api with default = CORP
    param tableau_proxy : proxy used for s3 with default = AWS
    '''
    
    # file name of downloaded snapshot image from Tableau Server
    file_name = '{}_snapshot'.format(project_id)
    
    # file type of downloaded snapshot image from Tableau Server
    file_type = 'png'
    
    # download_directory of snapshot image
    download_directory = '/home/airflow/airflow_download/automailing'
    
    is_process_succeed = False
    
    from Tableau.TableauHandler import TableauHandler
    
    tableau = TableauHandler(s3_proxy = s3_proxy, slack_proxy = slack_proxy, slack_channel = 'log-test')
    
    data_dict = context['ti'].xcom_pull(key = 'automailing_information', task_ids = 'refresh_tableau_view_page')

    # if succeeded to login then execute download process to download view as png image to assigned location
    if tableau.Login():              
        is_process_succeed = tableau.DownloadByViewId(data_dict['View ID'], download_directory, file_name, file_type)
    
    if not is_process_succeed:
        raise AirflowException
    
    data_dict["{{dashboard_image}}"] = '{}/{}.{}'.format(download_directory, file_name, file_type)
    
    context['ti'].xcom_push(key = 'automailing_information', value = data_dict)

#############################################################################################################################
# constitute_mail_body (Activated)

def constitute_mail_body(mail_body_path, ti):   
    '''
    param project_id : project id recorded at GooghleSheets
    purpose          : used for constituting mail body
    '''
    tw = pytz.timezone('Asia/Taipei')
    
    with open(mail_body_path, 'r') as f:
        
        html_body = f.read() 
        
    data_dict = ti.xcom_pull(key = 'automailing_information', task_ids = 'retrieve_snapshot_from_tableau_server')
    
    embeded_image_dict = {"{{Source_Analytics}}": "/home/shifty/data_etl/Airflow_ETL/AutoMailing/embeded_images/Source_Analytics.png", 
                          "{{Source}}": "/home/shifty/data_etl/Airflow_ETL/AutoMailing/embeded_images/Source.png"}
    
    # screanshot of tableau dashboard
    embeded_image_dict["{{dashboard_image}}"] = data_dict["{{dashboard_image}}"]   

    data_dict['embeded_image_lst'] = list(embeded_image_dict.values())
    # current timestamp
    current_timestamp = datetime.now()

    # email subject
    data_dict['title'] = '{} ({})'.format(data_dict['Email Title'], current_timestamp.date())

    # replace content of html body
    html_body = html_body.replace("{{delivery_time}}", current_timestamp.\
                                  astimezone(pytz.timezone('Asia/Taipei')).strftime('%Y-%m-%d %H:%M:%S %Z%z'))
    
    html_body = html_body.replace("{{report_title}}", data_dict['Email Title'])
    
    html_body = html_body.replace("{{dashboard_path}}", data_dict['SourceAnalytics Path'])

    for replace_string, embeded_image_path in embeded_image_dict.items():
        html_body = html_body.replace(replace_string, embeded_image_path)

    data_dict['body'] = html_body

    ti.xcom_push(key = 'final_automailing_information', value = data_dict)

#############################################################################################################################
# send_exchange_mail (Activated)

def send_mail_via_exchangelib(slack_proxy = 'CORP', **context):
    '''   
    purpose             : send email to assigned exchange mailbox
    param slack_proxy   : proxy used for slack api with default = CORP
    '''
    is_process_succeed = False
    
    data_dict = context['ti'].xcom_pull(key = 'final_automailing_information', task_ids = 'constitute_mail_body')
    
    from Exchange.ExchangeMailHandler import ExchangeMailHandler
    
    exchange = ExchangeMailHandler(slack_proxy = slack_proxy)

    if exchange.is_connect_succeed:
        is_process_succeed = exchange.SendMail(recipient_lst = data_dict['Recipient List'], 
                                subject = data_dict['title'], 
                                body = data_dict['body'], 
                                cc_recipient_lst = data_dict['CC List'],
                                is_html_body = True, 
                                embeded_image_lst = data_dict['embeded_image_lst'])
    
    if not is_process_succeed:
        raise AirflowException

#############################################################################################################################
# acquire_automailing_info (Inactivated)

# def acquire_automailing_info(project_id, ti):
#     '''
#     param project_id : dag_id of Airflow which equals to corresponsive AutoMailing project_id recorded at GooghleSheets
#     purpose      : used for acquiring necessary information from GoogleSheets
#     '''
#     is_process_succeed = False
#     data_dict = {}
#     gs_email = GoogleSheetHandler(gspread_key_name = 'email_scheduling', 
#                                   gs_proxy = 'CORP', 
#                                   gs_timeout = 10, 
#                                   slack_proxy = 'CORP', 
#                                   slack_channel = 'log-test')
        
#     if gs_email.is_connect_succeed:
#         result_dict = gs_email.ReadAsDataFrame()

#         # clear proxy setting in os environ
#         gs_email.ClearProxy()

#         if result_dict['is_read_succeed']:

#             task_df = result_dict['read_df']

#             # retrieve corresponsive data from GoogleSheets
#             dashboard_path, rc_string, cc_string, view_id, view_url, is_activate, if_dependency_exist, sync_state, original_title = \
#             task_df.loc[task_df['Project ID'] == dag_id, ['SourceAnalytics Path', 'Recipient List', 'CC List', 'View ID', 'View URL', 'Activate', 'If Dependency Exist', 'Sync State', 'Email Title']].values.tolist()[0]

#             # avoid triggering inactive jobs
#             if is_activate == 'TRUE':

#                 # No dependency ETL task or has dependency ETL task and it was executed successfully 
#                 if if_dependency_exist == 'FALSE' or (if_dependency_exist == 'TRUE' and sync_state == 'TRUE'):
                    
#                     data_dict['original_title'],data_dict['dashboard_path'] = original_title, dashboard_path
#                     # retrieve recipient list
#                     data_dict['recipient_list'] = rc_string.replace(' ','').split(',') if rc_string not in ['', '#N/A'] else []

#                     # retrieve carbon copy recipient list
#                     data_dict['cc_list'] = cc_string.replace(' ','').split(',') if cc_string not in ['', '#N/A'] else []
#                     ti.xcom_push(key = 'initial_automailing_information', value = data_dict)
                    
#                     is_process_succeed = True
    
#     if not is_process_succeed:
#         raise AirflowException