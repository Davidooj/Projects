import pandas as pd
import sqlalchemy
import pyodbc
import requests

# Extract the data
download_url = 'https://raw.githubusercontent.com/Davidooj/Projects/main/AimLab/GridshotUltimate_%20Data.csv'
output_filename = (r'C:/Users/david/AppData/Local/Packages/CanonicalGroupLimited.Ubuntu_79rhkp1fndgsc/LocalState/rootfs'
                   r'/home/davidmartinez/Gridshot_Ultimate_Data.csv')
response = requests.get(download_url)

if response.status_code == 200:
    with open(output_filename, 'wb') as f:
        f.write(response.content)
    print(f"CSV file downloaded to '{output_filename}'")
else:
    print("Failed to download CSV file")

# Transform the data
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

df = pd.read_csv(r'C:/Users/david/AppData/Local/Packages/CanonicalGroupLimited.Ubuntu_79rhkp1fndgsc/LocalState/rootfs'
                 r'/home/davidmartinez/Gridshot_Ultimate_Data.csv')
df2 = df.drop(["accB0", "accB1", "accB2", "accB3", "accB4", "accB5", "accB6", "accB7", 'rtB0', 'rtB1', 'rtB2',
               'rtB3', 'rtB4', 'rtB5', 'rtB6', 'rtB7', "version", "map", "mode", "weaponName"], axis=1)
df3 = df2.rename(columns={"targetsTotal": "Total Shots",
                          "shotsTotal": "Hits Per Sec",
                          "killsPerSec": "Total Hits",
                          "accTotal": "Accuracy Rate",
                          "createDate": "Task",
                          "rtTotal": "Reaction Time(ms)",
                          "taskName": "Task Name"})
df4 = df3.drop(["Task Name", "killTotal"], axis=1)
df4.iloc[95:, [1, 2]] = df4.iloc[95:, [2, 1]]

output_filename = (r'C:/Users/david/AppData/Local/Packages/CanonicalGroupLimited.Ubuntu_79rhkp1fndgsc/LocalState/rootfs'
                   r'/home/davidmartinez/Transformed_Data.csv')
df4.to_csv(output_filename, index=False)

# Upload the data into the database
server_name = 'DESKTOP-2QMIIKF\\SQLEXPRESS'
database_name = 'Testingplace'
username = 'DESKTOP-2QMIIKF\\david'
password = ''

# Create a pyodbc connection
conn_str = (
    f'DRIVER={{ODBC Driver 17 for SQL Server}};'
    f'SERVER={server_name};'
    f'DATABASE={database_name};'
    f'Trusted_Connection=yes;'
)

connection = pyodbc.connect(conn_str)

# Create SQLAlchemy engine using the pyodbc connection
engine = sqlalchemy.create_engine(f'mssql://', creator=lambda: connection)

df4.to_sql('test_table', engine, if_exists='append', index=False)
