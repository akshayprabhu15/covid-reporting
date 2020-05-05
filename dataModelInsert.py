import numpy as np
import pymssql
import sys
#import pyodbc
import pandas as pd
import datetime

class dataModelInsert:

    def datamartcreate(incoming_df,Modelkey):
    #try:
        conn = pymssql.connect(server='#######', user='#####', password='####', database='####')
        #conn = pyodbc.connect('DRIVER={SQL Server};SERVER=aadatamart.database.windows.net;DATABASE=aadatamart;UID=datamartadmin@aadatamart;PWD=AA$admin123;')
        cur = conn.cursor()

        df_data=incoming_df

        df_data['Model_Key'] = df_data['Model_Key'].astype('int64')
        df_data['Date'] = df_data['Date'].astype('datetime64[ns]')
        df_data['Actuals'] = df_data['Actuals'].astype('float64')
        df_data['Predicted'] = df_data['Predicted'].astype('float64')
        df_data['Predicted_Lower'] = df_data['Predicted_Lower'].astype('float64')
        df_data['Predicted_Upper'] = df_data['Predicted_Upper'].astype('float64')

        querystring = "select * from modelresults where Model_Key="+str(Modelkey)+"and Is_current = 'Y';"
        df_main_tbl_data = pd.read_sql(querystring, conn)

        df_main_tbl_data['Actuals'] = df_main_tbl_data['Actuals'].astype('float64')
        df_main_tbl_data['Predicted'] = df_main_tbl_data['Predicted'].astype('float64')
        df_main_tbl_data['Predicted_Lower'] = df_main_tbl_data['Predicted_Lower'].astype('float64')
        df_main_tbl_data['Predicted_Upper'] = df_main_tbl_data['Predicted_Upper'].astype('float64')

        df_all = df_data.merge(df_main_tbl_data.drop_duplicates(), on=['Model_Key','Date'],
                           how='outer', indicator=True)

        df_data_both = df_all.loc[df_all['_merge'] == 'both']

        df_data_both['match'] = np.where(((df_data_both['Actuals_x'] == df_data_both['Actuals_y']) & (df_data_both['Predicted_x'] == df_data_both['Predicted_y']) & (df_data_both['Predicted_Lower_x'] == df_data_both['Predicted_Lower_y']) & (df_data_both['Predicted_Upper_x'] == df_data_both['Predicted_Upper_y'])), 'True', 'False')
        df_data_both = df_data_both.loc[df_data_both['match'] == 'False']

        print("Number records to update : ", len(df_data_both))

        if len(df_data_both) > 0:
            df_data_both['IS_current'] = 'Y'
            df_data_both['last_updated'] = datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S")

            # Insert DataFrame recrds one by one.
            df_data_both['last_updated'] = df_data_both['last_updated'].astype('object')

            for i,row in df_data_both.iterrows():
                sql_update = " UPDATE modelresults SET Is_Current = 'N', last_updated = '"  + datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S") + "' WHERE Model_Key='" + str(row['Model_Key']) + "' and Date='" + str(row['Date']) +"'";
                cur.execute(sql_update)
                str1 = "'" + str(row['Model_Key']) + "','" + str(row['Date']) + "'," + str(row['Actuals_x']) + "," + str(row['Predicted_x']) + "," + str(row['Predicted_Lower_x']) + "," + str(row['Predicted_Upper_x'])  + ",'" + row['IS_current']  + "','" + row['last_updated'] + "'"
                sql = "INSERT INTO modelresults (Model_key,Date,Actuals,Predicted,Predicted_Lower,Predicted_Upper,Is_Current,last_updated) VALUES (" + str1 + ")"
                #print(sql)
                cur.execute(sql)
            print("No records updated : ", len(df_data_both))
        else:
            print("No records to update : ", len(df_data_both))

        df_data_left = df_all.loc[df_all['_merge'] == 'left_only']

        print("Number of records to insert : ", len(df_data_left))

        if len(df_data_left):
            for i, row in df_data_left.iterrows():
                str1 = "'" + str(row['Model_Key'])+ "','" + str(row['Date']) + "'," + str(row['Actuals_x']) + "," + str(row['Predicted_x']) + "," + str(row['Predicted_Lower_x']) + "," + str(row['Predicted_Upper_x']) + ",'Y','" + datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S") + "'"
                sql = "INSERT INTO modelresults (Model_Key,Date,Actuals,Predicted,Predicted_Lower,Predicted_Upper,Is_Current,last_updated) VALUES (" + str1 + ")"
                cur.execute(sql)
            print("No of records inserted : ", len(df_data_left))

        df_data_right = df_all.loc[df_all['_merge'] == 'right_only']
        print("Number of records to Delete : ", len(df_data_right))

        if len(df_data_right):
            for i,row in df_data_right.iterrows():
                sql_delete = " UPDATE modelresults SET IS_current = 'D', last_updated = '"  + datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S") + "' WHERE Model_Key='" + row['Model_Key'] + "' and Date='" + str(row['Date']) +"'";
                cur.execute(sql_delete)
            print("Number of records Deleted : ", len(df_data_right))

        print("Processing Finished")

        conn.commit()
        conn.close()

        #except:
        #    print("Exception Occurred !!!", str(sys.exc_info()))

if __name__ == "__main__":
    dataModelInsert()