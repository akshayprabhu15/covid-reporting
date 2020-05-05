import dataModelInsert as db
import pandas as pd
import pyodbc


df_data = pd.read_excel('/Users/sprabhu/Documents/data/CAS_Monthly_BI.xlsx').fillna(0)
#print(df_data)

Model=df_data.Model_Key.unique()

for i in Model:
    print(i)
    df_data[df_data['Model_Key'] == i]
    db.dataModelInsert.datamartcreate(df_data[df_data['Model_Key'] == i],i)

