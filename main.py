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
from xlsxwriter.workbook import Workbook

os.chdir('C:/Users/kagimub/Desktop/IBRD Project')

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

        wbk = Workbook(dashboardpath)
        ####WORKING ON THE DASHBOARD############
                
        ### KPI --- Loan Status Summary######
        dash1=pd.read_sql("select count(distinct Project_ID) No_of_projects from Project",self.engine)

        dash2=pd.read_sql("""select Loan_Status, count(distinct Loan_Number) No_of_loans from loan
                        group by Loan_Status Order by count(Loan_Number) Desc""", self.engine)
                        
        dash3=pd.read_sql("""select country, sum(Disbursed_Amount) loans_held from
                        (select distinct country, Disbursed_Amount from country c
                        inner join loan l on c.country_code = l.country_code)s 
                        group by country order by sum(Disbursed_Amount) desc limit 10"""
                        , self.engine)

        dash4=pd.read_sql("""select Sum(Repaid_to_IBRD+Repaid_3rd_Party)/Sum(Disbursed_Amount) Repaid_portion 
                        from loan""",self.engine)
                        
        dash5=pd.read_sql("""select COUNT(distinct loan_Number) loans from loan""",self.engine)

        dash6=pd.read_sql("""select count(distinct Loan_Number) Approved_loans from loan where Loan_Status = 'Approved'
                            """, self.engine)
        dash10 = pd.DataFrame()         
        dash10['Percent_Approved_Loans'] = dash6['Approved_loans']/dash5['loans']

        dash7=pd.read_sql("""select count(distinct Loan_Number) Repaid_loans from loan where Loan_Status like 'Repaid%'
                            """, self.engine)
        dash8=pd.read_sql("""select count(distinct Loan_Number) Cancelled_loans from loan where Loan_Status like 'cancel%'
                            """, self.engine)
                            
        dash9=pd.read_sql("select sum(Original_Principal_Amount) Total_Principal,sum(Cancelled_Amount) Total_Cancelled,sum(Undisbursed_Amount) Total_Undisbursed,sum(Disbursed_Amount) Total_Disbursed,sum(Repaid_to_IBRD) Total_Repaid_IBRD,sum(Due_to_IBRD) Total_Due_to_IBRD,sum(Borrowers_Obligation) Total_Borrowers_Obligation,Sum(Sold_3rd_Party) Total_Sold_3rd_Party,sum(Repaid_3rd_Party) Total_Repaid_3rd_Party,sum(Due_3rd_Party) Total_Due_3rd_Party,Sum(Loans_Held) Total_Loans_Held from loan",self.engine)


        dash11=pd.read_sql("select avg(Original_Principal_Amount) avg_Principal,avg(Cancelled_Amount) avg_Cancelled,avg(Undisbursed_Amount) avg_Undisbursed,avg(Disbursed_Amount) avg_Disbursed,avg(Repaid_to_IBRD) avg_Repaid_IBRD,avg(Due_to_IBRD) avg_Due_to_IBRD,avg(Borrowers_Obligation) avg_Borrowers_Obligation,avg(Sold_3rd_Party) avg_Sold_3rd_Party,avg(Repaid_3rd_Party) avg_Repaid_3rd_Party,avg(Due_3rd_Party) avg_Due_3rd_Party,avg(Loans_Held) avg_Loans_Held from loan",self.engine)


        ############loan status vs loans chart#####################

       
        #ws4 = wbk.get_worksheet_by_name('Loan_KPI_Dashboard')

        ws4=wbk.add_worksheet('Loan_KPI_Dashboard')
        ws3=wbk.add_worksheet('Data_Qlty_Stat_Dashboard')
        ws2=wbk.add_worksheet('Data_Aggregation_Dashboard')

        # Format cell borders via a configurable RxC box 
        def draw_frame_border(workbook, worksheet, first_row, first_col, rows_count, cols_count,thickness=1):

            if cols_count == 1 and rows_count == 1:
                # whole cell
                worksheet.conditional_format(first_row, first_col,
                                            first_row, first_col,
                                            {'type': 'formula', 'criteria': 'True',
                                            'format': workbook.add_format({'top': thickness, 'bottom':thickness,
                                                                            'left': thickness,'right':thickness})})    
            elif rows_count == 1:
                # left cap
                worksheet.conditional_format(first_row, first_col,
                                        first_row, first_col,
                                        {'type': 'formula', 'criteria': 'True',
                                        'format': workbook.add_format({'top': thickness, 'left': thickness,'bottom':thickness})})
                # top and bottom sides
                worksheet.conditional_format(first_row, first_col + 1,
                                        first_row, first_col + cols_count - 2,
                                        {'type': 'formula', 'criteria': 'True', 'format': workbook.add_format({'top': thickness,'bottom':thickness})})

                # right cap
                worksheet.conditional_format(first_row, first_col+ cols_count - 1,
                                        first_row, first_col+ cols_count - 1,
                                        {'type': 'formula', 'criteria': 'True',
                                        'format': workbook.add_format({'top': thickness, 'right': thickness,'bottom':thickness})})

            elif cols_count == 1:
                # top cap
                worksheet.conditional_format(first_row, first_col,
                                        first_row, first_col,
                                        {'type': 'formula', 'criteria': 'True',
                                        'format': workbook.add_format({'top': thickness, 'left': thickness,'right':thickness})})

                # left and right sides
                worksheet.conditional_format(first_row + 1,              first_col,
                                        first_row + rows_count - 2, first_col,
                                        {'type': 'formula', 'criteria': 'True', 'format': workbook.add_format({'left': thickness,'right':thickness})})

                # bottom cap
                worksheet.conditional_format(first_row + rows_count - 1, first_col,
                                        first_row + rows_count - 1, first_col,
                                        {'type': 'formula', 'criteria': 'True',
                                        'format': workbook.add_format({'bottom': thickness, 'left': thickness,'right':thickness})})

            else:
                # top left corner
                worksheet.conditional_format(first_row, first_col,
                                        first_row, first_col,
                                        {'type': 'formula', 'criteria': 'True',
                                        'format': workbook.add_format({'top': thickness, 'left': thickness})})

                # top right corner
                worksheet.conditional_format(first_row, first_col + cols_count - 1,
                                        first_row, first_col + cols_count - 1,
                                        {'type': 'formula', 'criteria': 'True',
                                        'format': workbook.add_format({'top': thickness, 'right': thickness})})

                # bottom left corner
                worksheet.conditional_format(first_row + rows_count - 1, first_col,
                                        first_row + rows_count - 1, first_col,
                                        {'type': 'formula', 'criteria': 'True',
                                        'format': workbook.add_format({'bottom': thickness, 'left': thickness})})

                # bottom right corner
                worksheet.conditional_format(first_row + rows_count - 1, first_col + cols_count - 1,
                                        first_row + rows_count - 1, first_col + cols_count - 1,
                                        {'type': 'formula', 'criteria': 'True',
                                        'format': workbook.add_format({'bottom': thickness, 'right': thickness})})

                # top
                worksheet.conditional_format(first_row, first_col + 1,
                                            first_row, first_col + cols_count - 2,
                                            {'type': 'formula', 'criteria': 'True', 'format': workbook.add_format({'top': thickness})})

                # left
                worksheet.conditional_format(first_row + 1,              first_col,
                                            first_row + rows_count - 2, first_col,
                                            {'type': 'formula', 'criteria': 'True', 'format': workbook.add_format({'left': thickness})})

                # bottom
                worksheet.conditional_format(first_row + rows_count - 1, first_col + 1,
                                            first_row + rows_count - 1, first_col + cols_count - 2,
                                            {'type': 'formula', 'criteria': 'True', 'format': workbook.add_format({'bottom': thickness})})

                # right
                worksheet.conditional_format(first_row + 1,              first_col + cols_count - 1,
                                            first_row + rows_count - 2, first_col + cols_count - 1,
                                            {'type': 'formula', 'criteria': 'True', 'format': workbook.add_format({'right': thickness})})


        # here we create bold format object .
        bold = wbk.add_format({ 'bold' : 1 })
        heading_format=wbk.add_format({ 'bold' : 1 })
        heading_format.set_font_size(25)
        number_format = wbk.add_format({'bold' : 1,'num_format': '#,##0'})
        percentage_format = wbk.add_format({'bold' : 1,'num_format': '0.0%'})
        merge_format = wbk.add_format({
            'bold': 1,
            'border': 1,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': 'yellow'})


        # Merge 3 cells.
        ws4.merge_range('E1:H1', 'Merged Range', merge_format)


        #draw_frame_border(wbk, ws4, 2, 1, 2, 1,2)
        ws4.write_row( 'B3' , ['Total Projects'], bold)
        ws4.write_row( 'B4' , dash1.iloc[:,0], number_format)
        for row in range(2,4):
                draw_frame_border(wbk, ws4, row, 1, 1, 1,2)


        #draw_frame_border(wbk, ws4, 2, 1, 2, 1,2)
        ws4.write_row( 'D3' , ['Total Loans'], bold)
        ws4.write_row( 'D4' , dash5.iloc[:,0], number_format)
        for row in range(2,4):
                draw_frame_border(wbk, ws4, row, 3, 1, 1,2)

        ws4.write_row( 'F3' , ['Repaid Percentage'], bold)
        ws4.write_row( 'F4' , dash4.iloc[:,0], percentage_format)
        for row in range(2,4):
                draw_frame_border(wbk, ws4, row, 5, 1, 1,2)

        ws4.write_row( 'H3' , ['Approved Percentage'], bold)
        ws4.write_row( 'H4' , dash10.iloc[:,0], percentage_format)
        for row in range(2,4):
                draw_frame_border(wbk, ws4, row, 7, 1, 1,2)

        ws4.write_row( 'J3' , ['Repaid Loans'], bold)
        ws4.write_row( 'J4' , dash7.iloc[:,0], number_format)
        for row in range(2,4):
                draw_frame_border(wbk, ws4, row, 9, 1, 1,2)

        ws4.write_row( 'L3' , ['Cancelled Loans'], bold)
        ws4.write_row( 'L4' , dash8.iloc[:,0], number_format)
        for row in range(2,4):
                draw_frame_border(wbk, ws4, row, 11, 1, 1,2)

        ####
        ws4.write_row( 'D6' , ['Total Principal'], bold)
        ws4.write_row( 'D7' , dash9.iloc[:,0], number_format)
        for row in range(5,7):
                draw_frame_border(wbk, ws4, row, 3, 1, 1,2)

        ws4.write_row( 'H6' , ['Total Cancelled'], bold)
        ws4.write_row( 'H7' , dash9.iloc[:,1], number_format)
        for row in range(5,7):
                draw_frame_border(wbk, ws4, row, 7, 1, 1,2)

        ws4.write_row( 'J6' , ['Total UnDisbursed'], bold)
        ws4.write_row( 'J7' , dash9.iloc[:,2], number_format)
        for row in range(5,7):
                draw_frame_border(wbk, ws4, row, 9, 1, 1,2)

        ws4.write_row( 'F6' , ['Total Disbursed'], bold)
        ws4.write_row( 'F7' , dash9.iloc[:,3], number_format)
        for row in range(5,7):
                draw_frame_border(wbk, ws4, row, 5, 1, 1,2)

        ws4.write_row( 'F9' , ['Repaid to IBRD'], bold)
        ws4.write_row( 'F10' , dash9.iloc[:,4], number_format)
        for row in range(8,10):
                draw_frame_border(wbk, ws4, row, 5, 1, 1,2)

        ws4.write_row( 'D9' , ['Due to IBRD'], bold)
        ws4.write_row( 'D10' , dash9.iloc[:,5], number_format)
        for row in range(8,10):
                draw_frame_border(wbk, ws4, row, 3, 1, 1,2)

        ws4.write_row( 'B9' , ['Borrowers Obligation'], bold)
        ws4.write_row( 'B10' , dash9.iloc[:,6], number_format)
        for row in range(8,10):
                draw_frame_border(wbk, ws4, row, 1, 1, 1,2)

        ws4.write_row( 'J9' , ['Sold 3rd_Party'], bold)
        ws4.write_row( 'J10' , dash9.iloc[:,7], number_format)
        for row in range(8,10):
                draw_frame_border(wbk, ws4, row, 9, 1, 1,2)

        ws4.write_row( 'H9' , ['Repaid 3rd_Party'], bold)
        ws4.write_row( 'H10' , dash9.iloc[:,8], number_format)
        for row in range(8,10):
                draw_frame_border(wbk, ws4, row, 7, 1, 1,2)

        ws4.write_row( 'L9' , ['Due 3rd_Party'], bold)
        ws4.write_row( 'L10' , dash9.iloc[:,9], number_format)
        for row in range(8,10):
                draw_frame_border(wbk, ws4, row, 11, 1, 1,2)
                
        ws4.write_row( 'B6' , ['Total Loans Held'], bold)
        ws4.write_row( 'B7' , dash9.iloc[:,10], number_format)
        for row in range(5,7):
                draw_frame_border(wbk, ws4, row, 1, 1, 1,2)


        ####
        ws4.write_row( 'D13' , ['AVG Principal'], bold)
        ws4.write_row( 'D14' , dash11.iloc[:,0], number_format)
        for row in range(12,14):
                draw_frame_border(wbk, ws4, row, 3, 1, 1,2)

        ws4.write_row( 'H13' , ['AVG Cancelled'], bold)
        ws4.write_row( 'H14' , dash11.iloc[:,1], number_format)
        for row in range(12,14):
                draw_frame_border(wbk, ws4, row, 7, 1, 1,2)

        ws4.write_row( 'J13' , ['AVG UnDisbursed'], bold)
        ws4.write_row( 'J14' , dash11.iloc[:,2], number_format)
        for row in range(12,14):
                draw_frame_border(wbk, ws4, row, 9, 1, 1,2)

        ws4.write_row( 'F13' , ['AVG Disbursed'], bold)
        ws4.write_row( 'F14' , dash11.iloc[:,3], number_format)
        for row in range(12,14):
                draw_frame_border(wbk, ws4, row, 5, 1, 1,2)

        ws4.write_row( 'F17' , ['AVG Repaid to IBRD'], bold)
        ws4.write_row( 'F18' , dash11.iloc[:,4], number_format)
        for row in range(16,18):
                draw_frame_border(wbk, ws4, row, 5, 1, 1,2)

        ws4.write_row( 'D17' , ['AVG Due to IBRD'], bold)
        ws4.write_row( 'D18' , dash11.iloc[:,5], number_format)
        for row in range(16,18):
                draw_frame_border(wbk, ws4, row, 3, 1, 1,2)

        ws4.write_row( 'B17' , ['AVG Borrowers Obligation'], bold)
        ws4.write_row( 'B18' , dash11.iloc[:,6], number_format)
        for row in range(16,18):
                draw_frame_border(wbk, ws4, row, 1, 1, 1,2)
                
        ws4.write_row( 'J17' , ['AVG Sold 3rd_Party'], bold)
        ws4.write_row( 'J18' , dash11.iloc[:,7], number_format)
        for row in range(16,18):
                draw_frame_border(wbk, ws4, row, 9, 1, 1,2)

        ws4.write_row( 'H17' , ['AVG Repaid 3rd_Party'], bold)
        ws4.write_row( 'H18' , dash11.iloc[:,8], number_format)
        for row in range(16,18):
                draw_frame_border(wbk, ws4, row, 7, 1, 1,2)

        ws4.write_row( 'L17' , ['AVG Due 3rd_Party'], bold)
        ws4.write_row( 'L18' , dash11.iloc[:,9], number_format)
        for row in range(16,18):
                draw_frame_border(wbk, ws4, row, 11, 1, 1,2)

        ws4.write_row( 'B13' , ['AVG Loans Held'], bold)
        ws4.write_row( 'B14' , dash11.iloc[:,10], number_format)
        for row in range(12,14):
                draw_frame_border(wbk, ws4, row, 1, 1, 1,2)


        #making the heading
        ws4.write_row( 'E1' , ['IBRD LOAN KPI Dashboard'], heading_format)

        #ws4.write(0,4, 'Loan_KPI_Dashboard')
        #Removing gridlines
        ws4.hide_gridlines(2)

        # create a data list .
        headings = [ 'Loan Status' , 'number']

        ####
        heading2 = [ 'Country' , ' Loan Held']

        # Write a row of data starting from 'A1'
        # with bold format .
        ws4.write_row( 'B38' , heading2, bold)

        # Write a column of data starting from
        # 'A2', 'B2', 'C2' respectively .
        ws4.write_column( 'B39' , dash3.iloc[:,0], number_format)
        ws4.write_column( 'C39' , dash3.iloc[:,1], number_format)

        for col in range(1,3): 
            for row in range(37,48):
                draw_frame_border(wbk, ws4, row, col, 1, 1,2)

        #Adding a chart
        chart2 = wbk.add_chart({'type': 'column'})

        #'name' : '= Loan_KPI_Dashboard !$A$3' ,
        chart2.add_series({
            'name' : '= Loan_KPI_Dashboard !$A$3' ,
            'categories': '=Loan_KPI_Dashboard!$B$39:$B$49',
            'values':     '=Loan_KPI_Dashboard!$C$39:$C$49'
        })

        # Insert the chart into the worksheet (with an offset).
        ws4.insert_chart('E38', chart2)#, {'x_offset': 25, 'y_offset': 10})

        # Add a chart title and some axis labels.
        chart2.set_title({'name' :'Loans Held per Country'})
        chart2.set_x_axis({'name' :'Country'})
        chart2.set_y_axis({'name' :'Total Loan Held'})

        ####

        # Write a row of data starting from 'A1'
        # with bold format .
        ws4.write_row( 'B21' , headings, bold)

        # Write a column of data starting from
        # 'A2', 'B2', 'C2' respectively .
        ws4.write_column( 'B22' , dash2.iloc[:,0], number_format)
        ws4.write_column( 'C22' , dash2.iloc[:,1], number_format)

        for col in range(1,3): 
            for row in range(20,32):
                draw_frame_border(wbk, ws4, row, col, 1, 1,2)
                
        #Adding a chart
        chart1 = wbk.add_chart({'type': 'column'})

        #'name' : '= Loan_KPI_Dashboard !$A$3' ,
        chart1.add_series({
            'name' : '= Loan_KPI_Dashboard !$A$3' ,
            'categories': '=Loan_KPI_Dashboard!$B$22:$B$32',
            'values':     '=Loan_KPI_Dashboard!$C$22:$C$32'
        })


        # Insert the chart into the worksheet (with an offset).
        ws4.insert_chart('E21', chart1)#, {'x_offset': 25, 'y_offset': 10})

        # Add a chart title and some axis labels.
        chart1.set_title({'name' :'Number of loans per loan status'})
        chart1.set_x_axis({'name' :'Loan_Status'})
        chart1.set_y_axis({'name' :'No_of_loans'})


        # Apply a conditional format to the cell range.
        ws4.conditional_format('C22:C32', {'type': '3_color_scale'})
        ws4.conditional_format('C39:C49', {'type': '3_color_scale'})
        #ws4.conditional_format('A3:B14' , { 'type' : 'no_blanks' , 'format' : 'border_format'})
                    

        ###
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

        stats_final.fillna(0,inplace=True)
            
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

        ###         

        #making the heading
        ws2.write_row( 'E1' , ['IBRD DATA AGGREGATION Dashboard'], heading_format)
        ws3.write_row( 'E1' , ['IBRD DATA QUALITY STATISTICS Dashboard'], heading_format)

        #ws4.write(0,4, 'Loan_KPI_Dashboard')
        #Removing gridlines
        ws2.hide_gridlines(2)

        # create a data list .
        heading3 = final.columns

        # Write a row of data starting from 'A1'
        # with bold format .
        ws2.write_row( 1,0, heading3, bold)

        # Write a column of data starting from
        # 'A2', 'B2', 'C2' respectively .
        for col in range(0,len(heading3)):
            ws2.write_column( 2,col , final.iloc[:,col], number_format)

        for col in range(0,len(heading3)): 
            for row in range(1,57):
                draw_frame_border(wbk, ws2, row, col, 1, 1,2)
                
                
                
        ########
        heading4 = stats_final.columns

        # Write a row of data starting from 'A1'
        # with bold format .
        ws3.write_row( 1,0, heading4, bold)

        # Write a column of data starting from
        # 'A2', 'B2', 'C2' respectively .
        for col in range(0,len(heading4)):
            ws3.write_column( 2,col , stats_final.iloc[:,col],number_format)

        for col in range(0,len(heading4)): 
            for row in range(1,38):
                draw_frame_border(wbk, ws3, row, col, 1, 1,2)

        wbk.close()

      

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

        

#%%
if __name__ == "__main__":
    etl = ETL()

    DownloadedFilePath = etl.DownloadingEmailAttachment('Bernard Kagimu', 'HELLO TEST MAIL')

    countryDF, guarantorDF, projectDF, loanDF = etl.DataProcessing(DownloadedFilePath)

    etl.LoadingCSVToDB(countryDF, guarantorDF, projectDF, loanDF)
    etl.Dashboard(DownloadedFilePath)
    # etl.SendExcelDashboard(etl.Dashboard(DownloadedFilePath))

    
#%%