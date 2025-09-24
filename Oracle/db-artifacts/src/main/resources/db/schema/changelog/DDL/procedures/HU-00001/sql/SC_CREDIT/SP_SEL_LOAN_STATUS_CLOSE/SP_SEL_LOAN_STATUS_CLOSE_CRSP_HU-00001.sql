CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_SEL_LOAN_STATUS_CLOSE 
   (PA_LOAN_ID                IN      SC_CREDIT.TA_LOAN_STATUS.FI_LOAN_ID%TYPE
   ,PA_LOAN_ADMIN_CENTER_ID   IN      SC_CREDIT.TA_LOAN_STATUS.FI_ADMIN_CENTER_ID%TYPE
   ,PA_CUR_RESULTS            OUT     SC_CREDIT.PA_TYPES.TYP_CURSOR
   ,PA_STATUS_CODE            OUT     NUMBER
   ,PA_STATUS_MSG             OUT     VARCHAR2)
IS
/************************************************************************************************************************************************************************************************************************************************
PROJECT:            PURPOSE_LIFE_LOAN_CYCLE
DESCRIPTION:        THIS STORE PROCEDURE EXECUTES A CONSULT ON TA_LOAN TABLE AND RETURNS A CURSOR WITH THE INFORMATION OF THE LOAN
PRECONDITIONS:      IT MUST RECEIVE A LOAN_ID AND A LOAN_ADMIN_CENTER_ID
CREATOR:            AIXA SARMIENTO
CREATED DATE:       02/01/2025
MODIFICATION DATE:
USER MODIFICATION:
*************************************************************************************************************************************************************************************************************************************************/
   CSL_ARROW        CONSTANT VARCHAR2(5)    := '-->';                                   -- AESTHETICS SIGN
   CSL_0            CONSTANT SIMPLE_INTEGER := 0;
   CSL_1            CONSTANT SIMPLE_INTEGER := 1;
   CSL_SUCCESS_MSG  CONSTANT VARCHAR2(10)   := 'SUCCESS';                               -- MESSAGE SUCCESS
   CSL_PKG          CONSTANT SIMPLE_INTEGER := 1;                                               -- CONSTANT WITH VALUE EQUAL TO 1, FOR THE SP_BATCH_ERROR_LOG
   CSL_DATE_FORMAT  CONSTANT VARCHAR2(25)   := 'MM/DD/YYYY hh24:mi:ss'; -- CONSTANT FOR DATE CONVERSION WITH HOURS
   CSL_SPACE        CONSTANT VARCHAR2(3)    := ' ';
   VL_DATA_OUT      VARCHAR2(30)            :=  PA_LOAN_ID || CSL_SPACE || PA_LOAN_ADMIN_CENTER_ID;

BEGIN

   OPEN PA_CUR_RESULTS FOR
      SELECT
             A.FI_LOAN_ID,
                         A.FI_ADMIN_CENTER_ID,
                         A.FI_PRODUCT_ID,
             A.FI_RULE_ID,
             A.FC_CUSTOMER_ID,
             A.FI_COUNTRY_ID,
             A.FI_BUSINESS_UNIT_ID,
             A.FI_COMPANY_ID,
             A.FI_LOAN_STATUS_ID,
             TO_CHAR(A.FD_LOAN_STATUS_DATE,CSL_DATE_FORMAT) AS FD_LOAN_STATUS_DATE,
             A.FN_PRINCIPAL_BALANCE,
             A.FN_FINANCE_CHARGE_BALANCE,
             A.FN_ADDITIONAL_CHARGE_BALANCE
        FROM SC_CREDIT.TA_LOAN A
       WHERE A.FI_LOAN_ID = PA_LOAN_ID
         AND A.FI_ADMIN_CENTER_ID = PA_LOAN_ADMIN_CENTER_ID;

   PA_STATUS_CODE   := CSL_0;
   PA_STATUS_MSG    := CSL_SUCCESS_MSG;

EXCEPTION
    WHEN OTHERS THEN
       PA_STATUS_CODE := SQLCODE;
       PA_STATUS_MSG  := SQLERRM || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;

       SC_CREDIT.SP_ERROR_LOG('SP_SEL_LOAN_STATUS_CLOSE', SQLCODE, SQLERRM,
         DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, PA_LOAN_ID, VL_DATA_OUT);

END SP_SEL_LOAN_STATUS_CLOSE;

/

GRANT EXECUTE ON SC_CREDIT.SP_SEL_LOAN_STATUS_CLOSE TO USRNCPCREDIT1
/
