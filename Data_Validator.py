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
       
        record = cursor.fetchall()

        column_names = [desc[0] for desc in cursor.description]
        # Creating source dataframe
        df_source = pd.DataFrame(record,columns = column_names)
        # converting NaN to 'Empty'
        for col in df_source.columns:
            df_source[col].replace({'':'Empty',np.nan:'Empty','None':'Empty'},inplace = True)
        # Detecting the encoding type
        with open('city_target.csv','rb') as file:
            encoding_type =  cd.detect(file.read())
        df_target = pd.read_csv('city_target.csv',delimiter = ',',encoding=encoding_type['encoding'])
        # converting NaN to 'Empty'
        for col in df_target.columns:
            df_target[col].replace({'':'Empty',np.nan:'Empty','None':'Empty'},inplace = True)
        # Compare data
        comparison_result = pd.merge(df_source, df_target, how='outer', indicator=True)#.loc[lambda x: x['_merge'] != 'both']
        # Write the comparison result to an Excel file
        comparison_result.to_excel('comparision.xlsx',index = False)

        print("Comparison completed. Results saved to",'D:\VS Code programs\Data validator')
except Error as e:
    print('while connecting to sql:',e)

finally:
    # Close the connection
    if connection.is_connected():
        cursor.close()
        connection.close()
        print('MySQL connection is closed')
