#%%
#Import packages
import pandas as pd
import datetime as dt 
import email
import imaplib
import os
import sys

#%%
class ETL():

    def __init__(self):
        self.connection = imaplib.IMAP4_SSL('imap.gmail.com')
        self.userName = 'bkagimu12@gmail.com'
        self.password = 'bkpython'
        self.date = dt.datetime.today().strftime("""%d-%b-%Y""")


    def DownloadingEmailAttachment(self, From, Subject):

        """ Fuction for downloading Email Attachment 
        from email Account
        
        Returns Filename
        """
        
        self.connection.login(self.userName, self.password)
        self.connection.select('Inbox') # select which folder to search from in the mailbox
        response, data= self.connection.search(None,f'(FROM "{From}" SUBJECT "{Subject}" SENTON "{self.date}")') #Specify the search  criteria

        for msgId in data[0].split():
            typ,messageParts = self.connection.fetch(msgId, '(RFC822)')
            emailBody = messageParts[0][1]
            emailBody = emailBody.decode('utf-8')
            mail = email.message_from_string(emailBody)

            for part in mail.walk():
                fileName = part.get_filename()

                if bool(fileName):
                    filePath = os.path.join('./data', fileName)
                    if not os.path.isfile(filePath) :
                        print (fileName)
                        fp = open(filePath, 'wb')
                        fp.write(part.get_payload(decode=True))
                        fp.close()

                    else:
                            print("no attachment")

            return fileName


    def DataProcessing(self, filename):

        df = pd.read_csv(filename)

        df.to_csv(filename+f"_{dt.datetime.today().strftime('%Y%m%d')}.csv")

        #####split the csv into tables###################################
        #############country table############# 
        country=df[['Country Code', 'Country','Region']]

        #working with columns
        country.columns=['Country_Code', 'Country','Region']

        # sorting by Country code 
        country.sort_values("Country_Code", inplace=True) 
        
        # dropping duplicate values 
        country.drop_duplicates(subset ="Country_Code",keep='first',inplace=True)

        #############guarantor table############# 
        guarantor= df[['Guarantor Country Code', 'Guarantor']]

        #guarantor columns
        guarantor.columns=['Guarantor_Country_Code', 'Guarantor']

        # sorting by Guarantor_Country_Code
        guarantor.sort_values("Guarantor_Country_Code", inplace=True) 
        
        # dropping duplicate values 
        guarantor.drop_duplicates(subset ="Guarantor_Country_Code",keep='first',inplace=True) 
        
        #############project table############# 
        project=df[['Project ID','Project Name']]

        #project columns
        project.columns=['Project_ID','Project_Name']

        # sorting by Project_ID
        project.sort_values("Project_ID", inplace=True) 
        
        # dropping duplicate values 
        project.drop_duplicates(subset ="Project_ID",keep='first',inplace=True) 

        #############loan table############# 
        loan=df[['Loan Number','Loan Type','Project ID','Country Code','Guarantor Country Code'
                ,'Borrower','Loan Status', 'Interest Rate', 'Currency of Commitment',
                    'Original Principal Amount', 'Cancelled Amount','Undisbursed Amount'
                    , 'Disbursed Amount', 'Repaid to IBRD','Due to IBRD', 'Exchange Adjustment'
                    , '''Borrower's Obligation''','Sold 3rd Party', 'Repaid 3rd Party'
                    , 'Due 3rd Party', 'Loans Held','First Repayment Date', 'Last Repayment Date'
                    , 'Agreement Signing Date','Board Approval Date', 'Effective Date (Most Recent)'
                    ,'Closed Date (Most Recent)', 'Last Disbursement Date','End of Period']]

        loan.columns=['Loan_Number','Loan_Type','Project_ID','Country_Code','Guarantor_Country_Code'
                ,'Borrower','Loan_Status', 'Interest_Rate', 'Currency_of_Commitment',
                    'Original_Principal_Amount', 'Cancelled_Amount','Undisbursed_Amount'
                    , 'Disbursed_Amount', 'Repaid_to_IBRD','Due_to_IBRD', 'Exchange_Adjustment'
                    , '''Borrowers_Obligation''','Sold_3rd_Party', 'Repaid_3rd_Party'
                    , 'Due_3rd_Party', 'Loans_Held','First_Repayment_Date', 'Last_Repayment_Date'
                    , 'Agreement_Signing_Date','Board_Approval_Date', 'Effective_Date'
                    ,'Closed_Date', 'Last_Disbursement_Date','End_of_Period']

        # sorting by Loan_Number
        loan.sort_values("Loan_Number", inplace=True) 
        
        # dropping duplicate values 
        loan.drop_duplicates(subset ="Loan_Number",keep='first',inplace=True) 

        #inserting the processed or received date

        loan.insert(0,'processed_date',dt.datetime.today())



        return country, guarantor, project, loan




#%%

############create My sql database with Star Schema ######
import mysql.connector

mydb = mysql.connector.connect(
  host='127.0.0.1',
  user="root",
  password=""
)

mycursor = mydb.cursor()

#mycursor.execute("CREATE DATABASE IBRD")

mycursor.execute("USE IBRD")

mycursor.execute("""CREATE TABLE IF NOT EXISTS loan(
processed_date DATETIME NOT NULL,
Loan_Number varchar(50) NOT NULL, 
Loan_Type varchar(50) ,
Project_ID varchar(50),
Country_Code varchar(5),
Guarantor_Country_Code varchar(5),
Borrower varchar(50),
Loan_Status varchar(50),
Interest_Rate DOUBLE(40,2),
Currency_of_Commitment varchar(50),
Original_Principal_Amount DOUBLE(40,2),
Cancelled_Amount DOUBLE(40,2),
Undisbursed_Amount DOUBLE(40,2),
Disbursed_Amount DOUBLE(40,2),
Repaid_to_IBRD DOUBLE(40,2),
Due_to_IBRD DOUBLE(40,2),
Exchange_Adjustment DOUBLE(40,2),
Borrowers_Obligation DOUBLE(40,2),
Sold_3rd_Party DOUBLE(40,2),
Repaid_3rd_Party DOUBLE(40,2),
Due_3rd_Party DOUBLE(40,2),
Loans_Held DOUBLE(40,2),
First_Repayment_Date DATETIME ,
Last_Repayment_Date DATETIME ,
Agreement_Signing_Date DATETIME ,
Board_Approval_Date DATETIME ,
Effective_Date DATETIME ,
Closed_Date DATETIME ,
Last_Disbursement_Date DATETIME ,
End_of_Period DATETIME ,
CONSTRAINT loan_key PRIMARY KEY (processed_date,Loan_Number),
FOREIGN KEY(Country_Code) REFERENCES         
country(Country_Code)
ON UPDATE CASCADE ON DELETE RESTRICT,  
FOREIGN KEY(Guarantor_Country_Code) REFERENCES         
guarantor(Guarantor_Country_Code)
ON UPDATE CASCADE ON DELETE RESTRICT,  
FOREIGN KEY(Project_ID) REFERENCES         
project(Project_ID )
ON UPDATE CASCADE ON DELETE RESTRICT)""")

mycursor.execute("""CREATE TABLE IF NOT EXISTS country(
Country_Code varchar(50) NOT NULL UNIQUE PRIMARY KEY, 
Country varchar(50) ,
Region varchar(50)
)""")


mycursor.execute("""CREATE TABLE IF NOT EXISTS guarantor(
Guarantor_Country_Code varchar(50) NOT NULL UNIQUE PRIMARY KEY, 
Guarantor varchar(50) ,
FOREIGN KEY(Guarantor_Country_Code) REFERENCES         
country(Country_Code )
ON UPDATE CASCADE ON DELETE RESTRICT
)""")


mycursor.execute("""CREATE TABLE IF NOT EXISTS project(
Project_ID varchar(50) NOT NULL UNIQUE PRIMARY KEY, 
Project_Name varchar(50)
)""")
    
mycursor.execute("select count(Loan_Number) from loan")

####Load data from the csv into the database schema tables
from sqlalchemy import create_engine

# create sqlalchemy engine
engine = create_engine("mysql+mysqlconnector://{user}:{pw}@localhost/{db}"
                       .format(user="root",
                               pw="",
                               db="IBRD"))

# Insert whole DataFrame into MySQL
country.to_sql('country', con = engine, if_exists = 'append', chunksize = 1000,index=False)

project.to_sql('project', con = engine, if_exists = 'append', chunksize = 1000,index=False)

guarantor.to_sql('guarantor', con = engine, if_exists = 'append', chunksize = 1000,index=False)

loan.to_sql('loan', con = engine, if_exists = 'append', chunksize = 1000,index=False)
 
#########################################WORKING ON THE DASHBOARD###############################

# ##### Data Accuracy Dashboard - Getting counts and missing values from the provided csv######
values=int(len(df['Loan Number']))

stats= pd.DataFrame()

stats['count']=df.count(0)

stats['missing_values']=[(values-x) for x in stats['count']]

stats.reset_index(inplace=True)

#getting summary statistics
stat=df.describe()

#transpose the data
stat=stat.T

#drop count column and reset index
stat.drop(['count'],inplace=True,axis=1)

stat.reset_index(inplace=True)

stats_final=pd.merge(stats,stat,how='left',on='index')
     
###########################DASHBOARD DATA AGREGATIONS###################
#Pick data from database
#######Total, Average, Minimum, Maximum #########
data=pd.read_sql("select processed_date,Original_Principal_Amount,Cancelled_Amount,Undisbursed_Amount,Disbursed_Amount,Repaid_to_IBRD,Due_to_IBRD,Borrowers_Obligation,Sold_3rd_Party,Repaid_3rd_Party,Due_3rd_Party,Loans_Held from loan",engine)

#introducing the month column
data['processed_month']=data['processed_date'].dt.to_period('M').astype('str')

#drop processed date
data.drop(['processed_date'],inplace=True,axis=1)

#Make data agrregations
total=data.groupby(['processed_month']).sum()
total['category']='Total'
total.reset_index(inplace=True)

mean=data.groupby(['processed_month']).mean()
mean['category']='Average'
mean.reset_index(inplace=True)

min_=data.groupby(['processed_month']).min()
min_['category']='Min'
min_.reset_index(inplace=True)

median=data.groupby(['processed_month']).median()
median['category']='Median'
median.reset_index(inplace=True)

max_=data.groupby(['processed_month']).max()
max_['category']='Max'
max_.reset_index(inplace=True)

#appending the various dataframes
final=pd.concat([total,mean],ignore_index=True)
final=pd.concat([final,min_],ignore_index=True)
final=pd.concat([final,median],ignore_index=True)
final=pd.concat([final,max_],ignore_index=True)

##set category as index and stack dataframe
final.set_index('category',inplace=True)

final=pd.DataFrame(final.stack())

final.reset_index(inplace=True)

#creating the column names
names=final.iloc[0].to_list()

names[0:0]=['Category']
names[1:1]=['Field']

names.pop(2)
names.pop(2)

final.columns=names

final=final.loc[~final['Field'].isin(['processed_month'])]

####################################################
### KPI --- Number of Projects########

dash1=pd.read_sql("select count(distinct Project_ID) No_of_projects from Project",engine)

####################
### KPI --- Loan Status Summary######

dash2=pd.read_sql("""select Loan_Status, count(distinct Loan_Number) No_of_loans from loan
                  group by Loan_Status Order by count(Loan_Number) Desc""", engine)

###############
### KPI --- Top 10 Countries with Loans ######
dash3=pd.read_sql("""select country, sum(Disbursed_Amount) loans_held from
                  (select distinct country, Disbursed_Amount from country c
                  inner join loan l on c.country_code = l.country_code)s 
                   group by country order by sum(Disbursed_Amount) desc"""
                  , engine)

##############
### KPI --- Percentage Repayment ########
dash4=pd.read_sql("""select Sum(Repaid_to_IBRD+Repaid_3rd_Party)/Sum(Disbursed_Amount) Repaid_portion 
                  from loan""",engine)
                  
##############
### KPI --- Total Number of Loans Given out ####
dash5=pd.read_sql("""select COUNT(distinct loan_Number) loans from loan""",engine)

#############
#### KPI --- Total Number of Approved Loans ###

dash6=pd.read_sql("""select count(distinct Loan_Number) Approved_loans from loan where Loan_Status = 'Approved'
                      """, engine)
#############
#### KPI --- % of Approved Loans of Total Loans ###             
dash10['Percent_Approved_Loans'] = dash6['Approved_loans']/dash5['loans']

#############
#### KPI --- Total Number of Repaid Loans ###

dash7=pd.read_sql("""select count(distinct Loan_Number) Repaid_loans from loan where Loan_Status like 'Repaid%'
                      """, engine)
                      
#############
#### KPI --- Total Number of Cancelled Loans ###

dash8=pd.read_sql("""select count(distinct Loan_Number) Cancelled_loans from loan where Loan_Status like 'cancel%'
                      """, engine)


#############
#### KPI --- Total Borrowers Obligation ###

dash9=pd.read_sql("""select Sum(Borrowers_Obligation) Borrowers_Obligation from loan """, engine)

from openpyxl import Workbook
wb = Workbook()

ws = wb.create_sheet("Loan_KPI_Dashboard") 

ws1 = wb.create_sheet("Data_Aggregations") 

ws2 = wb.create_sheet("Data_Quality_Statistics") 

std=wb['Sheet']
wb.remove(std)

##save data frame to sheet

#### save data Dashboard Data Aggregation to sheet 2
from openpyxl.utils.dataframe import dataframe_to_rows
#sheet 2        
rows = dataframe_to_rows(final)

for r_idx, row in enumerate(rows, 1):
    for c_idx, value in enumerate(row, 1):
         ws1.cell(row=r_idx, column=c_idx, value=value)
         
# save data from Data Quality statistics to  Sheet 3#####         
rows = dataframe_to_rows(stats_final)

for r_idx, row in enumerate(rows, 1):
    for c_idx, value in enumerate(row, 1):
         ws2.cell(row=r_idx, column=c_idx, value=value)

wb.save(r'C:\Users\kagimub\Desktop\IBRD Project\final_dashboard1.xlsx')



from openpyxl import load_workbook 
wb = load_workbook(r'C:\Users\kagimub\Desktop\IBRD Project\final_dashboard1.xlsx')

ws =wb['Loan_KPI_Dashboard']
ws2 = wb['Data_Aggregations']
ws3 = wb['Data_Quality_Statistics']
        
###############FORMATING DATA AGGREGATIONS AND STATISTICS SHEET ##########
#drop colum n rows
ws3.delete_rows(2)
ws3.delete_cols(1)

ws2.delete_rows(2)
ws2.delete_cols(1)

ws3['A1']='Category'

from openpyxl.styles import Border,Side,Font 
thin = Side(border_style="thin", color="000000")

###########data aggregations table#######
ws2.insert_rows(1,1)
ws2.merge_cells('A1:C1') 
ws2.cell(row=1, column=1).value = 'DATA AGGREGATION'
ws2.cell(row=1, column=1).font  = Font(b=True, color="000000")

max_row = ws2.max_row +1
max_col = ws2.max_column +1
for row in range(3,max_row):
    for col in range(2,max_col):
        ws2.cell(row,col).style='Comma [0]'
        
for row in range(2,max_row):
    for col in range(1,max_col):
        ws2.cell(row,col).border = Border(top=thin, left=thin, right=thin, bottom=thin)

###########statistics table#######
ws3.insert_rows(1,1)
ws3.merge_cells('A1:J1') 
ws3.cell(row=1, column=1).value = 'STATISTICS AND DATA QUALITY'
ws3.cell(row=1, column=1).font  = Font(b=True, color="000000")

max_row = ws3.max_row +1
max_col = ws3.max_column +1
for row in range(3,max_row):
    for col in range(2,max_col):
        ws3.cell(row,col).style='Comma [0]'
        
for row in range(2,max_row):
    for col in range(1,max_col):
        ws3.cell(row,col).border = Border(top=thin, left=thin, right=thin, bottom=thin)


##########DASHBOARD SHEET###################
####Total Loans#####
ws['B2']='Total loans'

ws['B3']=dash5.loc[0].at['loans']

for row in range(2,4):
    for col in range(2,3):
        ws.cell(row,col).border = Border(top=thin, left=thin, right=thin, bottom=thin)

####Total Projects #####
ws['D2']='Total Projects'

ws['D3']=dash1.loc[0].at['No_of_projects']

for row in range(2,4):
    for col in range(4,5):
        ws.cell(row,col).border = Border(top=thin, left=thin, right=thin, bottom=thin)

####Repaid Percentage ####
ws['F2']='Repaid_portion '

ws['F3']=dash4.loc[0].at['Repaid_portion']

ws.cell(3,6).style='Percent'

for row in range(2,4):
    for col in range(6,7):
        ws.cell(row,col).border = Border(top=thin, left=thin, right=thin, bottom=thin)



#######HERE ######
ws['B5']='Approved loans'

ws['B6']=dash6.loc[0].at['Approved_loans']

for row in range(5,7):
    for col in range(2,3):
        ws.cell(row,col).border = Border(top=thin, left=thin, right=thin, bottom=thin)

ws['D5']='Approved Loan Portion (%)'

ws['D6']=dash10.loc[0].at['Percent_Approved_Loans']
ws.cell(6,4).style='Percent'


for row in range(5,7):
    for col in range(4,5):
        ws.cell(row,col).border = Border(top=thin, left=thin, right=thin, bottom=thin)

ws['F5']='Overall Borrowers Obligation '

ws['F6']=dash9.loc[0].at['Borrowers_Obligation']
ws.cell(6,6).style.format('{3:,}')



for row in range(5,7):
    for col in range(6,7):
        ws.cell(row,col).border = Border(top=thin, left=thin, right=thin, bottom=thin)


wb.save(r'C:\Users\kagimub\Desktop\IBRD Project\final_dashboard1.xlsx')

# %%
