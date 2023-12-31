import mysql.connector
from mysql.connector import Error
import numpy as np
import pandas as pd
import chardet as cd

try:
    #Establish connection
    connection = mysql.connector.connect(host = 'localhost', user = 'root', password = '123456', database = 'world')
    if connection.is_connected():
        cursor = connection.cursor()
        cursor.execute('select * from city')
        #fetch all record for the executed query
        record = cursor.fetchall()

        column_names = [desc[0] for desc in cursor.description]
        # Creating source dataframe
        df_source = pd.DataFrame(record,columns = column_names)
        # converting NaN to 'Empty'
        for col in df_source.columns:
            df_source[col].replace({'':'Empty',np.nan:'Empty'},inplace = True)
        # Detecting the encoding type
        with open('city_target.csv','rb') as file:
            encoding_type =  cd.detect(file.read())['encoding']
        df_target = pd.read_csv('city_target.csv',delimiter = ',',encoding=encoding_type)
        # converting NaN to 'Empty'
        for col in df_target.columns:
            df_target[col].replace({'':'Empty',np.nan:'Empty','None':'Empty'},inplace = True)
        # Compare data
        Comparing_data = {columns:(df_source[columns] == df_target[columns]) for columns in df_source.columns}
        comparison_result = pd.DataFrame(Comparing_data)
        # comparison_result = pd.merge(df_source, df_target, how='outer', indicator=True)#.loc[lambda x: x['_merge'] != 'both']
        # Compared data in Excel sheet
        with pd.ExcelWriter('Comparison.xlsx') as Writer:
            df_source.to_excel(Writer, sheet_name = 'Source Data', index = False)
            df_target.to_excel(Writer, sheet_name = 'Target Data', index = False)
            comparison_result.to_excel(Writer, sheet_name = 'Comparison' ,index = False)

        print("Comparison completed. Results saved to",'D:\VS Code programs\Data validator\Comparison.xlsx')
except Error as e:
    print('while connecting to sql:',e)

finally:
    # Close the connection
    if connection.is_connected():
        cursor.close()
        connection.close()
        print('MySQL connection is closed')
