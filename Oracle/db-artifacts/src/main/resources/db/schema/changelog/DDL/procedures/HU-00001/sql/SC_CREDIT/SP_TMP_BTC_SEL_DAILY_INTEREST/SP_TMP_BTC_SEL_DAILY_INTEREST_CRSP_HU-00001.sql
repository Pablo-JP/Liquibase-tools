CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_TMP_BTC_SEL_DAILY_INTEREST 
  (
   PA_FIRST_CENTER_ID     IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE
   ,PA_END_CENTER_ID       IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE
   ,PA_TODAY               IN VARCHAR2
   ,PA_PROCESS             IN NUMBER
   ,PA_TRACK               IN NUMBER
   ,PA_STATUS_CODE         OUT NUMBER
   ,PA_STATUS_MSG          OUT VARCHAR2
   ,PA_CUR_SELECT          OUT SC_CREDIT.PA_TYPES.TYP_CURSOR
   )
   IS
   /* **************************************************************
   * PROYECT: LOAN LIFE CYCLE
   * DESCRIPTION: INTEREST CALCULATION PROCEDURE (TEST VICTOR)
   * CREATED DATE: 02/12/2024
   * CREATOR:IVAN LOPEZ
   ************************************************************** */

     --CONSTANT
   CSL_ARROW        CONSTANT VARCHAR2(2)   := '->';
   CSL_DATE         CONSTANT VARCHAR2(10)   := 'MM/DD/YYYY';
   CSL_0            CONSTANT SIMPLE_INTEGER := 0;
   CSL_1            CONSTANT SIMPLE_INTEGER := 1;
   CSL_2            CONSTANT SIMPLE_INTEGER := 2;
   CSL_3            CONSTANT SIMPLE_INTEGER := 3;
   CSL_MSG_SUCCESS  CONSTANT VARCHAR2(7)   := 'SUCCESS';
   CSL_SP          CONSTANT SIMPLE_INTEGER := 1;

   BEGIN

    PA_STATUS_CODE:=CSL_0;
    PA_STATUS_MSG:=CSL_MSG_SUCCESS;
    PA_CUR_SELECT:=NULL;

          --CONSULT LOAN,INTEREST,SCHEDULE
      OPEN PA_CUR_SELECT FOR
       WITH TABPED AS
           (
SELECT L.FI_LOAN_ID                                             AS FI_LOAN_ID
                  ,L.FI_ADMIN_CENTER_ID                         AS FI_ADMIN_CENTER_ID
                  ,LO.FI_COUNTRY_ID                                                                           AS FI_COUNTRY_ID
                  ,LO.FI_COMPANY_ID                                                                           AS FI_COMPANY_ID
                  ,LO.FI_BUSINESS_UNIT_ID                                                     AS FI_BUSINESS_UNIT_ID
                  ,LO.FC_CUSTOMER_ID                                                                  AS FC_CUSTOMER_ID
                  ,LO.FN_FINANCE_CHARGE_AMOUNT                  AS FN_FINANCE_CHARGE_AMOUNT
                  ,LO.FN_PAID_INTEREST_AMOUNT                   AS FN_PAID_INTEREST_AMOUNT
                  ,LO.FN_PRINCIPAL_BALANCE                      AS FN_PRINCIPAL_BALANCE
                  ,LO.FN_FINANCE_CHARGE_BALANCE                 AS FN_FINANCE_CHARGE_BALANCE
                  ,LO.FN_ADDITIONAL_CHARGE_BALANCE              AS FN_ADDITIONAL_CHARGE_BALANCE
                  ,TO_CHAR(LO.FD_ORIGINATION_DATE, CSL_DATE)    AS FD_ORIGINATION_DATE
                  ,TO_CHAR(LO.FD_FIRST_PAYMENT, CSL_DATE)       AS FD_FIRST_PAYMENT
                  ,LO.FI_CURRENT_BALANCE_SEQ                    AS FI_CURRENT_BALANCE_SEQ
                  ,LO.FN_INTEREST_RATE                          AS FN_INTEREST_RATE
                  ,LO.FI_NUMBER_OF_PAYMENTS                     AS FI_NUMBER_OF_PAYMENTS
                  ,LO.FI_TERM_TYPE                              AS FI_TERM_TYPE
                  ,LO.FI_LOAN_STATUS_ID                                                             AS FI_LOAN_STATUS_ID
                  ,LO.FI_RULE_ID                                AS FI_RULE_ID
                  ,TO_CHAR(LO.FD_LOAN_EFFECTIVE_DATE, CSL_DATE) AS FD_LOAN_EFFECTIVE_DATE
                  ,LI.FI_PAYMENT_NUMBER_ID                      AS FI_PAYMENT_NUMBER_ID
                  ,LI.FI_DAYS_ACUM_BY_TERM                      AS FI_DAYS_ACUM_BY_TERM
                  ,LI.FN_DAILY_INTEREST                         AS FN_DAILY_INTEREST
                  ,LI.FN_ACCRUED_INTEREST_BALANCE               AS FN_ACCRUED_INTEREST_BALANCE
                  ,LI.FN_ACCRUED_INTEREST_LOAN                                          AS FN_ACCRUED_INTEREST_LOAN
                  ,LI.FN_PAYMENT_INTEREST                                                           AS FN_PAYMENT_INTEREST
                  ,LI.FC_CONDITION_INTEREST                     AS FC_CONDITION_INTEREST
                  ,TO_CHAR(LI.FD_APPLICATION_DATE, CSL_DATE)    AS FD_APPLICATION_DATE
                   ,ROW_NUMBER() OVER (PARTITION BY LO.FI_LOAN_ID ORDER BY LI.FI_PAYMENT_NUMBER_ID DESC,LI.FI_DAYS_ACUM_BY_TERM DESC) AS ORDEN
              FROM SC_CREDIT.TA_TMP_LOAN_PROCESS L
              INNER JOIN SC_CREDIT.TA_LOAN LO
                ON L.FI_LOAN_ID = LO.FI_LOAN_ID
               AND L.FI_ADMIN_CENTER_ID = LO.FI_ADMIN_CENTER_ID
               LEFT JOIN SC_CREDIT.TA_LOAN_INTEREST LI
                ON LI.FI_LOAN_ID = L.FI_LOAN_ID
               AND LI.FI_ADMIN_CENTER_ID = L.FI_ADMIN_CENTER_ID
               WHERE L.FI_PROCESS = PA_PROCESS
               AND L.FI_TRACK = PA_TRACK
               AND LO.FI_LOAN_STATUS_ID IN (CSL_2, CSL_3))
            SELECT A.FI_LOAN_ID                                 AS FI_LOAN_ID
                  ,A.FI_ADMIN_CENTER_ID                         AS FI_ADMIN_CENTER_ID
                  ,A.FI_COUNTRY_ID                                                                            AS FI_COUNTRY_ID
                  ,A.FI_COMPANY_ID                                                                            AS FI_COMPANY_ID
                  ,A.FI_BUSINESS_UNIT_ID                                                            AS FI_BUSINESS_UNIT_ID
                  ,A.FC_CUSTOMER_ID                                                                     AS FC_CUSTOMER_ID
                  ,A.FN_FINANCE_CHARGE_AMOUNT                   AS FN_FINANCE_CHARGE_AMOUNT
                  ,A.FN_PAID_INTEREST_AMOUNT                    AS FN_PAID_INTEREST_AMOUNT
                  ,A.FN_PRINCIPAL_BALANCE                       AS FN_PRINCIPAL_BALANCE
                  ,A.FN_FINANCE_CHARGE_BALANCE                  AS FN_FINANCE_CHARGE_BALANCE
                  ,A.FN_ADDITIONAL_CHARGE_BALANCE               AS FN_ADDITIONAL_CHARGE_BALANCE
                  ,A.FD_ORIGINATION_DATE                        AS FD_ORIGINATION_DATE
                  ,A.FD_FIRST_PAYMENT                           AS FD_FIRST_PAYMENT
                  ,A.FI_CURRENT_BALANCE_SEQ                     AS FI_CURRENT_BALANCE_SEQ
                  ,A.FN_INTEREST_RATE                           AS FN_INTEREST_RATE
                  ,A.FI_NUMBER_OF_PAYMENTS                      AS FI_NUMBER_OF_PAYMENTS
                  ,A.FI_TERM_TYPE                               AS FI_TERM_TYPE
                  ,A.FI_LOAN_STATUS_ID                                                              AS FI_LOAN_STATUS_ID
                  ,A.FI_RULE_ID                                 AS FI_RULE_ID
                  ,A.FD_LOAN_EFFECTIVE_DATE                     AS FD_LOAN_EFFECTIVE_DATE
                  ,A.FI_PAYMENT_NUMBER_ID                       AS FI_PAYMENT_NUMBER_ID
                  ,A.FI_DAYS_ACUM_BY_TERM                       AS FI_DAYS_ACUM_BY_TERM
                  ,A.FN_DAILY_INTEREST                          AS FN_DAILY_INTEREST
                  ,A.FN_ACCRUED_INTEREST_BALANCE                AS FN_ACCRUED_INTEREST_BALANCE
                  ,A.FN_ACCRUED_INTEREST_LOAN                                             AS FN_ACCRUED_INTEREST_LOAN
                  ,A.FN_PAYMENT_INTEREST                                                            AS FN_PAYMENT_INTEREST
                  ,A.FC_CONDITION_INTEREST                      AS FC_CONDITION_INTEREST
                  ,A.FD_APPLICATION_DATE                        AS FD_APPLICATION_DATE
                  ,(SELECT TO_CHAR(PSA.FD_DUE_DATE, CSL_DATE)   AS FD_DUE_DATE_ANT
                      FROM SC_CREDIT.TA_PAYMENT_SCHEDULE PSA
                     WHERE PSA.FI_LOAN_ID = A.FI_LOAN_ID
                       AND PSA.FI_ADMIN_CENTER_ID = A.FI_ADMIN_CENTER_ID
                       AND PSA.FD_DUE_DATE < TO_DATE(PA_TODAY, CSL_DATE)
                       ORDER BY PSA.FI_PAYMENT_NUMBER_ID DESC
                       FETCH FIRST 1 ROW ONLY)                      AS FD_DUE_DATE_BEFORE
                  ,(SELECT PSA.FI_PAYMENT_NUMBER_ID             AS FI_PAYMENT
                      FROM SC_CREDIT.TA_PAYMENT_SCHEDULE PSA
                     WHERE PSA.FI_LOAN_ID = A.FI_LOAN_ID
                       AND PSA.FI_ADMIN_CENTER_ID = A.FI_ADMIN_CENTER_ID
                       AND PSA.FD_DUE_DATE >= TO_DATE(PA_TODAY, CSL_DATE)
                       AND ROWNUM = CSL_1)                      AS FI_PAYMENT_ID
                  ,(SELECT TO_CHAR(PSA.FD_DUE_DATE,CSL_DATE)   AS FD_DUE_DATE_NEXT
                      FROM SC_CREDIT.TA_PAYMENT_SCHEDULE PSA
                     WHERE PSA.FI_LOAN_ID = A.FI_LOAN_ID
                       AND PSA.FI_ADMIN_CENTER_ID = A.FI_ADMIN_CENTER_ID
                       AND PSA.FD_DUE_DATE >= TO_DATE(PA_TODAY, CSL_DATE)
                       AND ROWNUM = CSL_1)                      AS FD_DUE_DATE_AFTER
                  ,SC_CREDIT.FN_SEL_LOAN_BALANCE_DET_JSON
                     (A.FI_LOAN_ID
                     ,A.FI_ADMIN_CENTER_ID
                     ,A.FI_CURRENT_BALANCE_SEQ
                     ,NULL) AS BALANCE_DET_JSON
              FROM TABPED A
             WHERE A.ORDEN =CSL_1;


      EXCEPTION
      WHEN OTHERS THEN
         PA_STATUS_CODE := SQLCODE;
         PA_STATUS_MSG := SQLERRM || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;

         SC_CREDIT.SP_BATCH_ERROR_LOG(UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP)
                                     ,SQLCODE
                                     ,SQLERRM
                                     ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                     ,CSL_0
                                     ,PA_FIRST_CENTER_ID ||','|| PA_END_CENTER_ID ||',' ||PA_TODAY
                                     );
END SP_TMP_BTC_SEL_DAILY_INTEREST;

/

GRANT EXECUTE ON SC_CREDIT.SP_TMP_BTC_SEL_DAILY_INTEREST TO USRNCPCREDIT1
/
GRANT EXECUTE ON SC_CREDIT.SP_TMP_BTC_SEL_DAILY_INTEREST TO USRBTCCREDIT1
/
GRANT EXECUTE ON SC_CREDIT.SP_TMP_BTC_SEL_DAILY_INTEREST TO USRCREDIT02
/
