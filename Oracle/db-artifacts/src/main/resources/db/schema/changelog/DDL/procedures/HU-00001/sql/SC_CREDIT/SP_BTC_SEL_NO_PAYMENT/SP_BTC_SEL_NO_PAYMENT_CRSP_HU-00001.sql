CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_BTC_SEL_NO_PAYMENT 
   (PA_FIRST_CENTER_ID        IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE
   ,PA_END_CENTER_ID          IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE
   ,PA_TODAY                  IN VARCHAR2
   ,PA_STATUS_CODE            OUT NUMBER
   ,PA_STATUS_MSG             OUT VARCHAR2
   ,PA_CUR_SELECT             OUT SC_CREDIT.PA_TYPES.TYP_CURSOR)
 IS
      /* **************************************************************
      * CREATOR: Eduardo Cervantes Hernandez
      * CREATED DATE:   07/11/2024
      * DESCRIPTION: Select loan delinquent
      * APPLICATION:  Process Batch of Purpose
      * MODIFICATION DATE: 23/01/2025
	  * [NCPADC-4631-1V2]
      ************************************************************** */

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
         WITH TABLOAN AS(
            SELECT LO.FI_LOAN_ID                        AS FI_LOAN_ID
                  ,LO.FI_ADMIN_CENTER_ID                AS FI_ADMIN_CENTER_ID
                  ,LO.FI_COUNTRY_ID                     AS FI_COUNTRY_ID
                  ,LO.FI_COMPANY_ID                     AS FI_COMPANY_ID
                  ,LO.FI_BUSINESS_UNIT_ID               AS FI_BUSINESS_UNIT_ID
                  ,LO.FI_CURRENT_BALANCE_SEQ            AS FI_CURRENT_BALANCE_SEQ
                  ,LO.FI_LOAN_STATUS_ID                 AS FI_LOAN_STATUS_ID
                  ,LO.FI_PRODUCT_ID                     AS FI_PRODUCT_ID
                  ,LO.FI_RULE_ID                        AS FI_RULE_ID
                  ,LO.FC_UUID_TRACKING                  AS FC_UUID_TRACKING
                  ,LO.FN_PRINCIPAL_BALANCE              AS FN_PRINCIPAL_BALANCE
                  ,LO.FN_FINANCE_CHARGE_BALANCE         AS FN_FINANCE_CHARGE_BALANCE
                  ,LO.FN_ADDITIONAL_CHARGE_BALANCE      AS FN_ADDITIONAL_CHARGE_BALANCE
                  ,LO.FC_PLATFORM_ID                    AS FC_PLATFORM_ID
                  ,LO.FC_SUB_PLATFORM_ID                AS FC_SUB_PLATFORM_ID
                  ,LO.FC_CUSTOMER_ID                    AS FC_CUSTOMER_ID
                  ,PS.FI_PAYMENT_NUMBER_ID              AS FI_PAYMENT_NUMBER_ID
                  ,TRUNC(TO_DATE(PA_TODAY, CSL_DATE) - PS.FD_DUE_DATE) AS FI_DAYS_DELINQUENT
                  ,ROW_NUMBER() OVER (PARTITION BY LO.FI_LOAN_ID ORDER BY PS.FI_PAYMENT_NUMBER_ID ASC,PS.FI_PAYMENT_NUMBER_ID ASC) AS ORDEN
              FROM SC_CREDIT.TA_LOAN LO
        INNER JOIN SC_CREDIT.TA_PAYMENT_SCHEDULE PS
                ON LO.FI_LOAN_ID = PS.FI_LOAN_ID
               AND LO.FI_ADMIN_CENTER_ID = PS.FI_ADMIN_CENTER_ID
               AND PS.FI_PMT_SCHEDULE_STATUS_ID = CSL_1
               AND PS.FD_DUE_DATE <= TO_DATE(PA_TODAY, CSL_DATE)
             WHERE LO.FI_LOAN_STATUS_ID = CSL_2
               AND LO.FI_ADMIN_CENTER_ID BETWEEN PA_FIRST_CENTER_ID AND PA_END_CENTER_ID
               AND PS.FI_PAYMENT_SCHEDULE_ID  >= CSL_1
               AND PS.FI_LOAN_ID >= CSL_1
         )
         SELECT FI_LOAN_ID                                  AS FI_LOAN_ID
               ,FI_ADMIN_CENTER_ID                          AS FI_ADMIN_CENTER_ID
               ,FI_COUNTRY_ID                               AS FI_COUNTRY_ID
               ,FI_COMPANY_ID                               AS FI_COMPANY_ID
               ,FI_BUSINESS_UNIT_ID                         AS FI_BUSINESS_UNIT_ID
               ,FI_CURRENT_BALANCE_SEQ                      AS FI_CURRENT_BALANCE_SEQ
               ,FI_LOAN_STATUS_ID                           AS FI_LOAN_STATUS_ID
               ,NVL(FI_PRODUCT_ID, CSL_0)                   AS FI_PRODUCT_ID
               ,FI_RULE_ID                                  AS FI_RULE_ID
               ,FI_PAYMENT_NUMBER_ID                        AS FI_PAYMENT_NUMBER_ID
               ,FI_DAYS_DELINQUENT                          AS FI_DAYS_DELINQUENT
               ,FN_PRINCIPAL_BALANCE                        AS FN_PRINCIPAL_BALANCE
               ,FN_FINANCE_CHARGE_BALANCE                   AS FN_FINANCE_CHARGE_BALANCE
               ,FN_ADDITIONAL_CHARGE_BALANCE                AS FN_ADDITIONAL_CHARGE_BALANCE
               ,SC_CREDIT.FN_SEL_LOAN_BALANCE_DET_JSON
                  (LO.FI_LOAN_ID
                  ,LO.FI_ADMIN_CENTER_ID
                  ,LO.FI_CURRENT_BALANCE_SEQ
                  ,FC_UUID_TRACKING) AS BALANCE_DET_JSON
               ,FC_PLATFORM_ID                              AS FC_PLATFORM_ID
               ,FC_SUB_PLATFORM_ID                          AS FC_SUB_PLATFORM_ID
               ,FC_CUSTOMER_ID                              AS FC_CUSTOMER_ID
           FROM TABLOAN LO
          WHERE LO.FI_DAYS_DELINQUENT >= CSL_0
            AND LO.ORDEN = CSL_1;


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
END SP_BTC_SEL_NO_PAYMENT;

/

GRANT EXECUTE ON SC_CREDIT.SP_BTC_SEL_NO_PAYMENT TO USRNCPCREDIT1
/
GRANT EXECUTE ON SC_CREDIT.SP_BTC_SEL_NO_PAYMENT TO USRBTCCREDIT1
/
