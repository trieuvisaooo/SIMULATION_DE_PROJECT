/***
* ODAP-CQ2021
* Group 06
* CREDIT_CARD_TRANSACTIONS Database
***/

USE MASTER
GO
--------------------------------------------------------------------------------------------------
IF DB_ID('CREDIT_CARD_TRANSACTIONS') IS NOT NULL DROP DATABASE CREDIT_CARD_TRANSACTIONS;

CREATE DATABASE CREDIT_CARD_TRANSACTIONS;
GO

USE CREDIT_CARD_TRANSACTIONS;
GO

CREATE TABLE TRANSACTIONS (
    TransactionID INT IDENTITY(1,1) PRIMARY KEY,  
    UserID INT NOT NULL,                         
    Card INT NOT NULL,                           
    Year INT NOT NULL,                
    Month INT NOT NULL,       
    Day INT NOT NULL, 
	Date DATE NOT NULL, 
    Time TIME NOT NULL,               
    AmountUSD FLOAT NOT NULL,  
	AmountVND FLOAT NOT NULL, 
    UseChip NVARCHAR(50) NOT NULL,               
    MerchantName NVARCHAR(255) NOT NULL,         
    MerchantCity NVARCHAR(255) NOT NULL,         
    MerchantState NVARCHAR(50) NOT NULL,         
    ZipCode FLOAT,                               
    MCC INT,                                     
    Errors NVARCHAR(255),                        
    IsFraud NVARCHAR(50) NOT NULL,                               
);

CREATE INDEX IDX_TransactionDate_UserID ON Transactions (Date, UserID);
CREATE INDEX IDX_IsFraud ON Transactions (IsFraud);