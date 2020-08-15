#%%
#Import packages
import pandas as pd
from envelopes import Envelope, GMailSMTP
import datetime as dt 
import xlsxwriter
import email
import imaplib
from openpyxl import Workbook
import os
import sys
from openpyxl import load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import Border,Side,Font 
import mysql.connector
from sqlalchemy import create_engine

#%%
class ETL():

    def __init__(self):
        self.connection = imaplib.IMAP4_SSL('imap.gmail.com')
        self.userName = 'bkagimu12@gmail.com'
        self.password = 'bkpython'
        self.date = dt.datetime.today().strftime("""%d-%b-%Y""")

        self.mydb = mysql.connector.connect(
                    host='127.0.0.1',
                    user="root",
                    password=""
                    )

        # create sqlalchemy engine
        self.engine = create_engine("mysql+mysqlconnector://{user}:{pw}@localhost/{db}"
                       .format(user="root",
                               pw="",
                               db="IBRD"))

    def DownloadingEmailAttachment(self, From, Subject):

        """ Fuction for downloading Email Attachment 
        from email Account
        
        Returns Filename
        """
        
        global fileName

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

            return filePath


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


        guarantor.dropna(subset=['Guarantor'], inplace=True)

        project.dropna(subset=['Project_ID'], inplace=True)

        country.dropna(subset=['Country_Code'], inplace=True)




        # Check if data is already in DB
        countryIDs = pd.read_sql('select distinct Country_Code from country', self.engine)
        projectIDs = pd.read_sql('select distinct Project_ID from project', self.engine)
        guarantorIDs = pd.read_sql('select distinct Guarantor_Country_Code from guarantor', self.engine)
        loanDate  = pd.read_sql('select  processed_date from loan', self.engine)

        country = country[~country['Country_Code'].isin(countryIDs.Country_Code)]
        project = project[~project['Project_ID'].isin(projectIDs.Project_ID)]
        loan = loan[loan['Loan_Number'].isin(loanDate.processed_date)]
        guarantor = guarantor[~guarantor['Guarantor_Country_Code'].isin(guarantorIDs.Guarantor_Country_Code)]

        return country, guarantor, project, loan



    def CreatingMySQLDB(self): 
        """ Creating a STAR Schema architecture in MySQL Based Database"""

        mycursor = self.mydb.cursor()

        # create MYSQL DataBase
        

        
        mycursor.execute("CREATE DATABASE IF NOT EXISTS IBRD")

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
    
#mycursor.execute("select count(Loan_Number) from loan")

    def LoadingCSVToDB(self, country, guarantor, project, loan):

        """ Loadin Data into Database """
        # Insert whole DataFrame into MySQL

        loan.to_sql('loan', con = self.engine, chunksize= 500, if_exists = 'append',index=False)

        country.to_sql('country', con = self.engine, chunksize= 500,if_exists = 'append',index=False)

        project.to_sql('project', con = self.engine,chunksize= 500, if_exists = 'append',index=False)

        guarantor.to_sql('guarantor', con = self.engine, chunksize= 500,if_exists = 'append',index=False)

        


 
    def Dashboard(self, filename):

        """ """


        df = pd.read_csv(filename)

        dashboardpath = "./data/excel_dashboard/final_dashboard1.xlsx"
            ####WORKING ON THE DASHBOARD############

            ##### Data Accuracy Dashboard - Getting counts and missing values from the provided csv######
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
        data=pd.read_sql("select processed_date,Original_Principal_Amount,Cancelled_Amount,Undisbursed_Amount,Disbursed_Amount,Repaid_to_IBRD,Due_to_IBRD,Borrowers_Obligation,Sold_3rd_Party,Repaid_3rd_Party,Due_3rd_Party,Loans_Held from loan",self.engine)

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

        dash1=pd.read_sql("select count(distinct Project_ID) No_of_projects from Project",self.engine)

        ####################
        ### KPI --- Loan Status Summary######

        dash2=pd.read_sql("""select Loan_Status, count(distinct Loan_Number) No_of_loans from loan
                        group by Loan_Status Order by count(Loan_Number) Desc""", self.engine)

        ###############
        ### KPI --- Top 10 Countries with Loans ######
        dash3=pd.read_sql("""select country, sum(Disbursed_Amount) loans_held from
                        (select distinct country, Disbursed_Amount from country c
                        inner join loan l on c.country_code = l.country_code)s 
                        group by country order by sum(Disbursed_Amount) desc"""
                        , self.engine)

        ##############
        ### KPI --- Percentage Repayment ########
        dash4=pd.read_sql("""select Sum(Repaid_to_IBRD+Repaid_3rd_Party)/Sum(Disbursed_Amount) Repaid_portion 
                        from loan""",self.engine)
                        
        ##############
        ### KPI --- Total Number of Loans Given out ####
        dash5=pd.read_sql("""select COUNT(distinct loan_Number) loans from loan""",self.engine)

        #############
        #### KPI --- Total Number of Approved Loans ###

        dash6=pd.read_sql("""select count(distinct Loan_Number) Approved_loans from loan where Loan_Status = 'Approved'
                            """, self.engine)
        #############
        #### KPI --- % of Approved Loans of Total Loans ###    
        dash10 = pd.DataFrame()         
        dash10['Percent_Approved_Loans'] = dash6['Approved_loans']/dash5['loans']

        #############
        #### KPI --- Total Number of Repaid Loans ###

        dash7=pd.read_sql("""select count(distinct Loan_Number) Repaid_loans from loan where Loan_Status like 'Repaid%'
                            """, self.engine)
                            
        #############
        #### KPI --- Total Number of Cancelled Loans ###

        dash8=pd.read_sql("""select count(distinct Loan_Number) Cancelled_loans from loan where Loan_Status like 'cancel%'
                            """, self.engine)


        #############
        #### KPI --- Total Borrowers Obligation ###

        dash9=pd.read_sql("""select Sum(Borrowers_Obligation) Borrowers_Obligation from loan """, self.engine)


        wb = Workbook()

        ws = wb.create_sheet("Loan_KPI_Dashboard") 

        ws1 = wb.create_sheet("Data_Aggregations") 

        ws2 = wb.create_sheet("Data_Quality_Statistics") 

        std=wb['Sheet']
        wb.remove(std)

        ##save data frame to sheet

        #### save data Dashboard Data Aggregation to sheet 2

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

        wb.save(dashboardpath)



        wb = load_workbook(dashboardpath)

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


        wb.save(dashboardpath)



        # wbk = xlsxwriter.Workbook(dashboardpath)
        # ws4 = wbk.get_worksheet_by_name('Loan_KPI_Dashboard')

        # ws4.write('D1', 'Loan_KPI_Dashboard') 
        # ws4.hide_gridlines(2)

        # # Create a Pandas Excel writer using XlsxWriter as the engine.
        # writer = pd.ExcelWriter(dashboardpath, engine='xlsxwriter')

        # chart1 = wbk.add_chart({'type': 'column'})

        # dash2.to_excel(writer, sheet_name='Loan_KPI_Dashboard',
        #         startrow=9, startcol=2, index=False)

        # chart1.add_series({ 'Loan_status': '=Sheet1!$B$10:$B$21','values':     '=Sheet1!$C$10:$C$21',  })

        # # Add a chart title and some axis labels.
        # chart1.set_title ({'Number of loans per loan status'})
        # chart1.set_x_axis({'Loan_Status'})
        # chart1.set_y_axis({'No_of_loans'})

        # # Insert the chart into the worksheet (with an offset).
        # ws4.insert_chart('D10', chart1)#, {'x_offset': 25, 'y_offset': 10})

        # # Apply a conditional format to the cell range.
        # ws4.conditional_format('c10:c21', {'type': '3_color_scale'})
        # ws4.conditional_format( 'B9:C21' , { 'type' : 'no_blanks' , 'format' : 'border_format'})

        # wbk.set_size(1200, 800)
        # wbk.save(dashboardpath)
        # wbk.close()


        return dashboardpath


    def SendExcelDashboard(self, Dashbardpath):


        envelope = Envelope(
            from_addr=(u'bkagimu12@gmail.com', u'Bernard Kagimu'),
            to_addr=['raynermukiza@gmail.com', 'pandolkb@gmail.com'],
            subject=u'Dashboard',
            text_body=u"I'm a helicopter!"
        )
        envelope.add_attachment(Dashbardpath)

        # Send the envelope using an ad-hoc connection...
        envelope.send('smtp.googlemail.com',tls=True)

        


if __name__ == "__main__":
    etl = ETL()

    DownloadedFilePath = etl.DownloadingEmailAttachment('Bernard Kagimu', 'HELLO TEST MAIL')

    countryDF, guarantorDF, projectDF, loanDF = etl.DataProcessing(DownloadedFilePath)

    etl.LoadingCSVToDB(countryDF, guarantorDF, projectDF, loanDF)

    etl.Dashboard(DownloadedFilePath)

    
#%%