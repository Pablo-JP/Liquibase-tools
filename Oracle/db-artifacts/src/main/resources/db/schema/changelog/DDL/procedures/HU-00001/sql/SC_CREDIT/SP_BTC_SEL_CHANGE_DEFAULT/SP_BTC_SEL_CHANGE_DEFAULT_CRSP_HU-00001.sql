CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_BTC_SEL_CHANGE_DEFAULT 
    (PA_FIRST_CENTER_ID        IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE
   ,PA_END_CENTER_ID          IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE
   ,PA_STATUS_CODE            OUT NUMBER
   ,PA_STATUS_MSG             OUT VARCHAR2
   ,PA_CUR_SELECT             OUT SC_CREDIT.PA_TYPES.TYP_CURSOR)

   IS
      ----------------------------------------------------------------------
          -- PROJECT: LOAN LIFE CYCLE
      -- CREATOR: Eduardo Cervantes Hernandez
      -- CREATED DATE:   18/12/2024
      -- DESCRIPTION: Select CHANGE DEFAULT
      -- APPLICATION:  Process Batch of Purpose
      --MODIFIED DATE: 30/12/2024
      -----------------------------------------------------------------------

      CSL_0                   CONSTANT SIMPLE_INTEGER := 0;
      CSL_1                   CONSTANT SIMPLE_INTEGER := 1;
      CSL_2                   CONSTANT SIMPLE_INTEGER := 2;
      CSL_3                   CONSTANT SIMPLE_INTEGER := 3;
      CSL_COMA                CONSTANT VARCHAR2(3) := ', ';
      CSL_MSG_SUCCESS         CONSTANT VARCHAR2(7) := 'SUCCESS';
      CSL_FIRST               CONSTANT VARCHAR2(7) := 'First: ';
      CSL_END                 CONSTANT VARCHAR2(5) := 'End: ';
      CSL_DATE                CONSTANT VARCHAR2(22) := 'MM/DD/YYYY hh24:mi:ss';
      CSL_ARROW               CONSTANT VARCHAR2(5) := ' -> ';

   BEGIN

      PA_STATUS_CODE := CSL_0;
      PA_STATUS_MSG  := CSL_MSG_SUCCESS;
      PA_CUR_SELECT  := NULL;

      OPEN PA_CUR_SELECT FOR
          SELECT FI_LOAN_ID
               ,FI_ADMIN_CENTER_ID
               ,FC_CUSTOMER_ID
               ,FI_COUNTRY_ID
               ,FI_COMPANY_ID
               ,FI_BUSINESS_UNIT_ID
               ,FN_PRINCIPAL_BALANCE
               ,FN_FINANCE_CHARGE_BALANCE
               ,FN_ADDITIONAL_CHARGE_BALANCE
               ,FI_PRODUCT_ID
               ,FI_RULE_ID
               ,FI_LOAN_STATUS_ID
               ,FI_CURRENT_BALANCE_SEQ
               ,FC_PLATFORM_ID
               ,FC_SUB_PLATFORM_ID
               ,FI_REGISTRATION_NUMBER
               ,FI_COUNTER_DAY
               ,FI_PAYMENT_NUMBER_ID
               ,FI_ACTION_DETAIL_ID
               ,FD_INITIAL_DATE
               ,BALANCE_DET_JSON
               ,INTEREST_JSON
           FROM(
         SELECT LO.FI_LOAN_ID
               ,LO.FI_ADMIN_CENTER_ID
               ,LO.FC_CUSTOMER_ID
               ,LO.FI_COUNTRY_ID
               ,LO.FI_COMPANY_ID
               ,LO.FI_BUSINESS_UNIT_ID
               ,LO.FN_PRINCIPAL_BALANCE
               ,LO.FN_FINANCE_CHARGE_BALANCE
               ,LO.FN_ADDITIONAL_CHARGE_BALANCE
               ,LO.FI_PRODUCT_ID
               ,LO.FI_RULE_ID
               ,LO.FI_LOAN_STATUS_ID
               ,LO.FI_CURRENT_BALANCE_SEQ
               ,LO.FC_PLATFORM_ID
               ,LO.FC_SUB_PLATFORM_ID
               ,SD.FI_REGISTRATION_NUMBER
               ,SD.FI_COUNTER_DAY
               ,SD.FI_PAYMENT_NUMBER_ID
               ,SD.FI_ACTION_DETAIL_ID
               ,TO_CHAR(SD.FD_INITIAL_DATE,CSL_DATE) AS FD_INITIAL_DATE
               ,SC_CREDIT.FN_SEL_LOAN_BALANCE_DET_JSON
                  (LO.FI_LOAN_ID
                  ,LO.FI_ADMIN_CENTER_ID
                  ,LO.FI_CURRENT_BALANCE_SEQ
                  ,LO.FC_UUID_TRACKING) AS BALANCE_DET_JSON
               ,SC_CREDIT.FN_SEL_LOAN_INTEREST_JSON
                  (LO.FI_LOAN_ID
                  ,LO.FI_ADMIN_CENTER_ID
                  ,LO.FC_UUID_TRACKING) AS INTEREST_JSON
               ,ROW_NUMBER() OVER (PARTITION BY LO.FI_LOAN_ID ORDER BY SD.FI_PAYMENT_NUMBER_ID) TAB_LOAN
           FROM SC_CREDIT.TA_LOAN LO
     INNER JOIN SC_CREDIT.TA_LOAN_STATUS_DETAIL SD
             ON SD.FI_LOAN_ID = LO.FI_LOAN_ID
            AND SD.FI_ADMIN_CENTER_ID = LO.FI_ADMIN_CENTER_ID
            AND FI_REGISTRATION_NUMBER  >  CSL_0
            AND SD.FI_ON_OFF = CSL_1
            AND SD.FI_ACTION_DETAIL_ID = CSL_3
          WHERE LO.FI_LOAN_STATUS_ID = CSL_3
            AND LO.FI_ADMIN_CENTER_ID BETWEEN PA_FIRST_CENTER_ID AND PA_END_CENTER_ID
       )
        WHERE TAB_LOAN=1;

   EXCEPTION
      WHEN OTHERS THEN
         ROLLBACK;
         PA_STATUS_CODE := SQLCODE;
         PA_STATUS_MSG := SQLERRM || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
         SC_CREDIT.SP_BATCH_ERROR_LOG(UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_1)
                                     ,SQLCODE
                                     ,SQLERRM
                                     ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                     ,CSL_0
                                     ,CSL_FIRST || PA_FIRST_CENTER_ID || CSL_COMA
                                     || CSL_END ||PA_END_CENTER_ID);
END SP_BTC_SEL_CHANGE_DEFAULT;

/

GRANT EXECUTE ON SC_CREDIT.SP_BTC_SEL_CHANGE_DEFAULT TO USRNCPCREDIT1
/
GRANT EXECUTE ON SC_CREDIT.SP_BTC_SEL_CHANGE_DEFAULT TO USRBTCCREDIT1
/
