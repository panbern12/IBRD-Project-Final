# IBRD-Project-Final
An email is sent from 'Bernard Kagimu' with Subject 'IBRD STATEMENT OF LOANS'
The process detects it automatically and Downloads the Email attachment on it 'ibrd-statement-of-loans-latest-available-snapshot.csv'
This is then Stored in folder './data'
The date of receipt is received and appended on to the file name and stored in the same folder './data' as 'ibrd-statement-of-loans-latest-available-snapshot.csv_20200818'
The csv file is then broken down into tables
Tables are created in the Mysql Database 
Data is pushed from csv to these Database tables.
Dashboards are created off this Database and are stored in the folder './data/excel_dashboard' as 'final_dashboard1.xlsx'
This dashboard is then sent using gmail to management i.e 'to_addr=['panadolkb@gmail.com', 'bkagimu12@gmail.com'],
            subject=u'IBRD Loans Dashboard'

