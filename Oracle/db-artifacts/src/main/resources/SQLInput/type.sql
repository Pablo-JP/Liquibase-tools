--------------------------------------------------------
--  File created - Monday-January-27-2025   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Type TYP_REC_BTC_BALANCE
--------------------------------------------------------

  CREATE OR REPLACE  TYPE SC_CREDIT.TYP_REC_BTC_BALANCE AS OBJECT
(
         --thit is the second version
   FI_LOAN_BALANCE_ID           NUMBER(15,0),
   FI_ADMIN_CENTER_ID           NUMBER(8,0),
   FI_LOAN_ID                   NUMBER(15,0),
   FI_LOAN_OPERATION_ID         NUMBER(15,0),
   FI_BALANCE_SEQ               NUMBER(5,0),
   FN_PRINCIPAL_BALANCE         NUMBER(12, 2),
   FN_FINANCE_CHARGE_BALANCE    NUMBER(12, 2),
   FN_ADDITIONAL_CHARGE_BALANCE NUMBER(12, 2)
);

/

  GRANT EXECUTE ON SC_CREDIT.TYP_REC_BTC_BALANCE TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.TYP_REC_BTC_BALANCE TO USRBTCCREDIT1;
--------------------------------------------------------
--  DDL for Type TYP_REC_BTC_DETAIL
--------------------------------------------------------

  CREATE OR REPLACE  TYPE SC_CREDIT.TYP_REC_BTC_DETAIL AS OBJECT
(
   FI_ADMIN_CENTER_ID       NUMBER(8,0),
   FI_LOAN_ID               NUMBER(15,0),
   FI_LOAN_OPERATION_ID     NUMBER(15,0),
   FI_LOAN_CONCEPT_ID       NUMBER(5,0),
   FN_ITEM_AMOUNT           NUMBER(12,2)
);

/

  GRANT EXECUTE ON SC_CREDIT.TYP_REC_BTC_DETAIL TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.TYP_REC_BTC_DETAIL TO USRBTCCREDIT1;
--------------------------------------------------------
--  DDL for Type TYP_REC_BTC_ERROR
--------------------------------------------------------

  CREATE OR REPLACE  TYPE SC_CREDIT.TYP_REC_BTC_ERROR AS OBJECT
   (FC_ADMIN_CENTER_ID   NUMBER(8,0)
   ,FI_LOAN_ID           NUMBER(15,0)
   ,FC_PROCESS           VARCHAR2(80)
   ,FI_SQL_CODE          NUMBER(5)
   ,FC_SQL_ERRM          VARCHAR2(1000)
   ,FC_BACKTRACE         VARCHAR2(600)
   ,FD_ERROR             VARCHAR2(150)
   ,FI_TRANSACTION           NUMBER(33,0)
   ,FC_ADITIONAL             VARCHAR2(500 BYTE));

/

  GRANT EXECUTE ON SC_CREDIT.TYP_REC_BTC_ERROR TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.TYP_REC_BTC_ERROR TO USRBTCCREDIT1;
--------------------------------------------------------
--  DDL for Type TYP_REC_BTC_INTEREST
--------------------------------------------------------

  CREATE OR REPLACE  TYPE SC_CREDIT.TYP_REC_BTC_INTEREST AS OBJECT
   (FI_LOAN_ID                      NUMBER(15,0)
   ,FI_ADMIN_CENTER_ID              NUMBER(8,0)
   ,FI_PAYMENT_NUMBER_ID            NUMBER(3,0)
   ,FI_DAYS_ACUM_BY_TERM            NUMBER(10,0)
   ,FN_DAILY_INTEREST               NUMBER(15,5)
   ,FN_ACCRUED_INTEREST_BALANCE     NUMBER(15,5)
   ,FN_ACCRUED_INTEREST_LOAN        NUMBER(15,5)
   ,FN_PAYMENT_INTEREST             NUMBER(15,5)
   ,FC_CONDITION_INTEREST           VARCHAR2(50 BYTE)
   ,FD_OPERATION_DATE               VARCHAR2(150)
   ,FD_APPLICATION_DATE             VARCHAR2(150)
   ,FI_BAN_OPERATION                NUMBER(2,0)
   ,FI_TRANSACTION                  NUMBER(15,0)
   ,FI_CURRENT_BALANCE_SEQ          NUMBER(5,0)
   );

/
--------------------------------------------------------
--  DDL for Type TYP_REC_BTC_LOAN
--------------------------------------------------------

  CREATE OR REPLACE  TYPE SC_CREDIT.TYP_REC_BTC_LOAN AS OBJECT
   (FI_ADMIN_CENTER_ID                NUMBER(8,0)
   ,FI_LOAN_ID                        NUMBER(15,0)
   ,FN_PRINCIPAL_BALANCE              NUMBER(12,2)
   ,FN_FINANCE_CHARGE_BALANCE         NUMBER(12,2)
   ,FN_ADDITIONAL_CHARGE_BALANCE      NUMBER(12,2)
   ,FI_ADDITIONAL_STATUS              NUMBER(5,0)
   ,FI_CURRENT_BALANCE_SEQ            NUMBER(5,0)
   ,FI_LOAN_STATUS_ID                 NUMBER(5,0)
   ,FC_LOAN_STATUS_DATE               VARCHAR(50)
   ,FI_TRANSACTION                    NUMBER(15));

/

  GRANT EXECUTE ON SC_CREDIT.TYP_REC_BTC_LOAN TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.TYP_REC_BTC_LOAN TO USRBTCCREDIT1;
--------------------------------------------------------
--  DDL for Type TYP_REC_BTC_OPERATION
--------------------------------------------------------

  CREATE OR REPLACE  TYPE SC_CREDIT.TYP_REC_BTC_OPERATION AS OBJECT
(
   FI_LOAN_OPERATION_ID     NUMBER(15),
   FI_COUNTRY_ID            NUMBER(3),
   FI_COMPANY_ID            NUMBER(3),
   FI_BUSINESS_UNIT_ID      NUMBER(3),
   FI_LOAN_ID               NUMBER(15),
   FI_ADMIN_CENTER_ID       NUMBER(8,0),
   FI_OPERATION_TYPE_ID     NUMBER(5),
   FN_OPERATION_AMOUNT      NUMBER(12, 2),
   FC_APPLICATION_DATE      VARCHAR2(50),
   FC_OPERATION_DATE        VARCHAR2(50),
   FI_STATUS                NUMBER(3),
   FC_END_USER              VARCHAR2(10),
   FC_UUID_TRACKING         VARCHAR2(36)
);

/

  GRANT EXECUTE ON SC_CREDIT.TYP_REC_BTC_OPERATION TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.TYP_REC_BTC_OPERATION TO USRBTCCREDIT1;
--------------------------------------------------------
--  DDL for Type TYP_REC_BTC_STATUS
--------------------------------------------------------

  CREATE OR REPLACE  TYPE SC_CREDIT.TYP_REC_BTC_STATUS AS OBJECT
 (FI_LOAN_ID                  NUMBER(15,0)
 ,FI_ADMIN_CENTER_ID          NUMBER(8,0)
 ,FI_LOAN_OPERATION_ID      NUMBER(15,0)
 ,FI_LOAN_STATUS_ID             NUMBER(5,0)
 ,FI_LOAN_STATUS_OLD_ID     NUMBER(5,0)
 ,FI_TRIGGER_ID             NUMBER(3,0)
 ,FD_LOAN_STATUS_DATE         VARCHAR(50)
 );

/

  GRANT EXECUTE ON SC_CREDIT.TYP_REC_BTC_STATUS TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.TYP_REC_BTC_STATUS TO USRBTCCREDIT1;
--------------------------------------------------------
--  DDL for Type TYP_REC_BTC_STATUS_DETAIL
--------------------------------------------------------

  CREATE OR REPLACE  TYPE SC_CREDIT.TYP_REC_BTC_STATUS_DETAIL AS OBJECT
   (FI_LOAN_ID                 NUMBER(15,0)
   ,FI_ADMIN_CENTER_ID         NUMBER(8,0)
   ,FI_LOAN_STATUS_ID            NUMBER(5,0)
   ,FI_ACTION_DETAIL_ID        NUMBER(3,0)
   ,FI_COUNTER_DAY                 NUMBER(5,0)
   ,FD_INITIAL_DATE                VARCHAR(50)
   ,FI_PAYMENT_NUMBER_ID             NUMBER(8,0)
   ,FD_FINAL_DATE                    VARCHAR(50)
   ,FI_ON_OFF                    NUMBER(3,0)
   ,FI_OPTION                  NUMBER(3,0)
   );

/

  GRANT EXECUTE ON SC_CREDIT.TYP_REC_BTC_STATUS_DETAIL TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.TYP_REC_BTC_STATUS_DETAIL TO USRBTCCREDIT1;
--------------------------------------------------------
--  DDL for Type TYP_REC_BTC_STATUS_PWO
--------------------------------------------------------

  CREATE OR REPLACE  TYPE SC_CREDIT.TYP_REC_BTC_STATUS_PWO AS OBJECT
 (  FI_LOAN_ID                   NUMBER (15)                    ,
        FI_ADMIN_CENTER_ID       NUMBER (8)                     ,
        FN_PAY_OFF_AMOUNT        NUMBER (12,2)                  ,
        FN_PWO_EXT_PAYMENT       NUMBER (12,2)                  ,
        FN_AMOUNT_PAID           NUMBER (12,2)                  ,
        FN_PWO_MIN_PAYMENT       NUMBER (12,2)                  ,
        FI_ADD_EXTENSION         NUMBER (3)                     ,
    FD_PWO_DATE       VARCHAR(50)
 );

/

  GRANT EXECUTE ON SC_CREDIT.TYP_REC_BTC_STATUS_PWO TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.TYP_REC_BTC_STATUS_PWO TO USRBTCCREDIT1;
--------------------------------------------------------
--  DDL for Type TYP_REC_LOAN_OP_TENDER
--------------------------------------------------------

  CREATE OR REPLACE  TYPE SC_CREDIT.TYP_REC_LOAN_OP_TENDER AS OBJECT
( FI_TENDER_TYPE_ID   NUMBER (4,0),
  FN_OPERATION_AMOUNT NUMBER(12,2)
);

/

  GRANT EXECUTE ON SC_CREDIT.TYP_REC_LOAN_OP_TENDER TO USRNCPCREDIT1;
--------------------------------------------------------
--  DDL for Type TYP_TAB_BTC_BALANCE
--------------------------------------------------------

  CREATE OR REPLACE
         --this is the version 2
         TYPE SC_CREDIT.TYP_TAB_BTC_BALANCE AS TABLE OF SC_CREDIT.TYP_REC_BTC_BALANCE;


/

  GRANT EXECUTE ON SC_CREDIT.TYP_TAB_BTC_BALANCE TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.TYP_TAB_BTC_BALANCE TO USRBTCCREDIT1;
--------------------------------------------------------
--  DDL for Type TYP_TAB_BTC_DETAIL
--------------------------------------------------------

  CREATE OR REPLACE  TYPE SC_CREDIT.TYP_TAB_BTC_DETAIL AS TABLE OF SC_CREDIT.TYP_REC_BTC_DETAIL;

/

  GRANT EXECUTE ON SC_CREDIT.TYP_TAB_BTC_DETAIL TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.TYP_TAB_BTC_DETAIL TO USRBTCCREDIT1;
--------------------------------------------------------
--  DDL for Type TYP_TAB_BTC_ERROR
--------------------------------------------------------

  CREATE OR REPLACE  TYPE SC_CREDIT.TYP_TAB_BTC_ERROR AS TABLE OF SC_CREDIT.TYP_REC_BTC_ERROR;

/

  GRANT EXECUTE ON SC_CREDIT.TYP_TAB_BTC_ERROR TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.TYP_TAB_BTC_ERROR TO USRBTCCREDIT1;
--------------------------------------------------------
--  DDL for Type TYP_TAB_BTC_INTEREST
--------------------------------------------------------

  CREATE OR REPLACE  TYPE SC_CREDIT.TYP_TAB_BTC_INTEREST AS TABLE OF SC_CREDIT.TYP_REC_BTC_INTEREST;

/
--------------------------------------------------------
--  DDL for Type TYP_TAB_BTC_LOAN
--------------------------------------------------------

  CREATE OR REPLACE  TYPE SC_CREDIT.TYP_TAB_BTC_LOAN AS TABLE OF SC_CREDIT.TYP_REC_BTC_LOAN;

/

  GRANT EXECUTE ON SC_CREDIT.TYP_TAB_BTC_LOAN TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.TYP_TAB_BTC_LOAN TO USRBTCCREDIT1;
--------------------------------------------------------
--  DDL for Type TYP_TAB_BTC_OPERATION
--------------------------------------------------------

  CREATE OR REPLACE  TYPE SC_CREDIT.TYP_TAB_BTC_OPERATION AS TABLE OF SC_CREDIT.TYP_REC_BTC_OPERATION;

/

  GRANT EXECUTE ON SC_CREDIT.TYP_TAB_BTC_OPERATION TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.TYP_TAB_BTC_OPERATION TO USRBTCCREDIT1;
--------------------------------------------------------
--  DDL for Type TYP_TAB_BTC_STATUS
--------------------------------------------------------

  CREATE OR REPLACE  TYPE SC_CREDIT.TYP_TAB_BTC_STATUS AS TABLE OF SC_CREDIT.TYP_REC_BTC_STATUS;

/

  GRANT EXECUTE ON SC_CREDIT.TYP_TAB_BTC_STATUS TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.TYP_TAB_BTC_STATUS TO USRBTCCREDIT1;
--------------------------------------------------------
--  DDL for Type TYP_TAB_BTC_STATUS_DETAIL
--------------------------------------------------------

  CREATE OR REPLACE  TYPE SC_CREDIT.TYP_TAB_BTC_STATUS_DETAIL AS TABLE OF SC_CREDIT.TYP_REC_BTC_STATUS_DETAIL;

/

  GRANT EXECUTE ON SC_CREDIT.TYP_TAB_BTC_STATUS_DETAIL TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.TYP_TAB_BTC_STATUS_DETAIL TO USRBTCCREDIT1;
--------------------------------------------------------
--  DDL for Type TYP_TAB_BTC_STATUS_PWO
--------------------------------------------------------

  CREATE OR REPLACE  TYPE SC_CREDIT.TYP_TAB_BTC_STATUS_PWO AS TABLE OF SC_CREDIT.TYP_REC_BTC_STATUS_PWO;

/

  GRANT EXECUTE ON SC_CREDIT.TYP_TAB_BTC_STATUS_PWO TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.TYP_TAB_BTC_STATUS_PWO TO USRBTCCREDIT1;
--------------------------------------------------------
--  DDL for Type TYP_TAB_LOAN_OP_TENDER
--------------------------------------------------------

  CREATE OR REPLACE  TYPE SC_CREDIT.TYP_TAB_LOAN_OP_TENDER AS TABLE OF SC_CREDIT.TYP_REC_LOAN_OP_TENDER;

/

  GRANT EXECUTE ON SC_CREDIT.TYP_TAB_LOAN_OP_TENDER TO USRNCPCREDIT1;
