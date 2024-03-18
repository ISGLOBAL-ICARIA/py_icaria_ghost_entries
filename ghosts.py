import pandas as pd
import params
import tokens
import gspread
from datetime import datetime
from gspread_dataframe import set_with_dataframe
import redcap

def define_ghost_records():
    ghost_entries = pd.DataFrame(columns=['project','record_id','study_number'])
    g = 0
    for token in tokens.REDCAP_PROJECTS_ICARIA.keys():
        print(token)
        project = redcap.Project(tokens.URL,tokens.REDCAP_PROJECTS_ICARIA[token])

        df = project.export_records(format='df',fields=['study_number'])

        record_ids = list(df.reset_index()['record_id'])
        c=4
        if token in params.prefix_three:
            c = 3
        prefix_records = [str(i)[0:c] for i in list(record_ids)]
        count = 0
        for prefix in prefix_records:
            if prefix != params.dict_prefixes[token]:
                ghost_entries.loc[g] = token,record_ids[count],df['study_number'][record_ids[count]].values[0]
                g+= 1
                print("\t",record_ids[count],df['study_number'][record_ids[count]].values[0])
            count += 1

    ghost_entries.loc[g] = str(datetime.today()),'',''
    ghost_entries = ghost_entries.drop_duplicates()
    file_to_drive(tokens.ghost_drive_worksheet_name,ghost_entries,
                  tokens.ghost_drive_filename,tokens.ghost_drive_folder,
                  index_included=False, deleting=True)
    print("\n\nScript Completed on "+ str(datetime.today()))


def file_to_drive(worksheet,df,drive_file_name,folder_id,index_included=True,deleting=False):
    gc = gspread.oauth(tokens.path_credentials)
    sh = gc.open(title=drive_file_name,folder_id=folder_id)
    if deleting:
        actual_worksheet = sh.worksheet(worksheet)
        actual_worksheet.clear()
    set_with_dataframe(sh.worksheet(worksheet), df,include_index=index_included)

