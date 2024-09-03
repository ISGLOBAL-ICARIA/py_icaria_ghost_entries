import pandas as pd
import params
import tokens
import gspread
from datetime import datetime
from gspread_dataframe import set_with_dataframe
import redcap
import requests

def define_ghost_records():
    ghost_entries = pd.DataFrame(columns=['project','record_id','study_number'])
    g = 0
    for token in tokens.REDCAP_PROJECTS_ICARIA.keys():
        print(token)
        project = redcap.Project(tokens.URL,tokens.REDCAP_PROJECTS_ICARIA[token])
        df = project.export_records(format_type='df',forms=params.all_forms_redcap,fields=['record_id','study_number'])
        record_ids = list(df.reset_index()['record_id'].unique())
        #print(df[df.index.get_level_values('redcap_event_name')=='epipenta1_v0_recru_arm_1']['study_number'])
        c=4
        if token in params.prefix_three:
            c = 3
        prefix_records = [str(i)[0:c] for i in list(record_ids)]
        count = 0

        
        for prefix in prefix_records:
            if prefix != params.dict_prefixes[token]:
                if len(df[(df.index.get_level_values('redcap_event_name') == 'epipenta1_v0_recru_arm_1')&
                         (df.index.get_level_values('record_id') == record_ids[count])]['study_number'].values) == 0:
                    study_number = ''
                else:
                    study_number = df[(df.index.get_level_values('redcap_event_name') == 'epipenta1_v0_recru_arm_1')&
                                      (df.index.get_level_values('record_id') == record_ids[count])]['study_number'].values[0]
                ghost_entries.loc[g] = token,record_ids[count],study_number
                g+= 1
                print("\t",record_ids[count],study_number)
            count += 1

    ghost_entries.loc[g] = str(datetime.today()),'',''
    ghost_entries = ghost_entries.drop_duplicates()
#    file_to_drive(tokens.ghost_drive_worksheet_name,ghost_entries,
#                  tokens.ghost_drive_filename,tokens.ghost_drive_folder,
#                  index_included=False, deleting=True)
    print("\n\nScript Completed on "+ str(datetime.today()))


def file_to_drive(worksheet,df,drive_file_name,folder_id,index_included=True,deleting=False):
    gc = gspread.oauth(tokens.path_credentials)
    sh = gc.open(title=drive_file_name,folder_id=folder_id)
    if deleting:
        actual_worksheet = sh.worksheet(worksheet)
        actual_worksheet.clear()
    set_with_dataframe(sh.worksheet(worksheet), df,include_index=index_included)


def empty_uncompleted_instruments():
    for token in tokens.REDCAP_PROJECTS_ICARIA.keys():
        print(token)
        project = redcap.Project(tokens.URL,tokens.REDCAP_PROJECTS_ICARIA[token])

        df = project.export_records(format_type='df',fields=params.complete_interv_fields)

        df_iv = project.export_records(format_type='df',fields=params.interv_fields)

        df = df.fillna('')
        for k,el in df.T.items():
            print(k,el)
            if k[1] in params.events:
                for field in params.fields_per_event[k[1]]:
                    if 'complete' in field and t == 0:
                        print(el[field])
                    if 'interviewer' in field:
                        t = 1
                        if el[field] == '':
                            t = 0
            break
        break

from redcap import Project
def export_logging():
    data = {
        'token': 'D1642BA340EF1EE461BEFDF73E742043',
        'content': 'log',
        'logtype': 'record_edit',
        'user': '',
        'record': '',
        'beginTime': '2022-12-01 09:19',
        'endTime': '2022-12-10 09:19',
        'format': 'csv',
        'returnFormat': 'csv'
    }

    beg_time = datetime.strptime('2022-12-01 00:00:00', '%Y-%m-%d %H:%M:%S').date()
    beg_time = ''
    count = 0
    for token in tokens.REDCAP_PROJECTS_ICARIA.keys():
        print(token)
        project = redcap.Project(tokens.URL,tokens.REDCAP_PROJECTS_ICARIA[token])

        df = project.export_logging(format_type='df',log_type='record_edit',begin_time=beg_time,end_time='')
        print(df)
        if count > 0:
            final_df = pd.concat([final_df,df])
        elif count == 0:
            final_df = df
        count += 1
        if count == 2:
            break
    print(final_df)

    final_df.to_csv(tokens.path_loggings)
