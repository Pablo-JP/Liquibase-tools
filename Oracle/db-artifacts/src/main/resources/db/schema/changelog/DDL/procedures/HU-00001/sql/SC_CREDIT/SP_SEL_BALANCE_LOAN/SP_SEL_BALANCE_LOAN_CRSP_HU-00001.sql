CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_SEL_BALANCE_LOAN (
 PA_LOANS IN CLOB
, PA_OUTPUT_BALANCE_LOAN OUT CLOB
, PA_STATUS_CODE OUT NUMBER
, PA_STATUS_MSG OUT VARCHAR2)
    IS
/*************************************************************
  * PROJECT    :  NCP-OUTSTANDING BALANCE
  * DESCRIPTION:  PROCEDURE FOR QUERYNG LOAN BALANCE
  * CREATOR:      CARLOS EDUARDO_MARTINEZ CANTERO
  * CREATED DATE: 2024-11-14
  * MODIFICATION: 2025-01-06 CARLOS EDUARDO_MARTINEZ CANTERO
  * v1.1 ADD BALANCE DETAILS
  * V1.2 REMOVE INNER JOIN AND ADD NEW PRIMARY KEY (FI_LOAN_ID) BALANCE_DETAIL,OPERATION_DETAIL
  * V1.3 ADMIN_CENTER IS ADDED TO DATA OUTPUT.
*************************************************************/

    CSG_SUCCESS_CODE CONSTANT      SIMPLE_INTEGER := 0;
    CSG_SUCCESS_MSG CONSTANT       VARCHAR2(10)   := 'SUCCESS';
    CSG_ARROW CONSTANT             VARCHAR2(5)    := ' -> ';
    CSL_ISSUE_BAL_SEQ_MSG CONSTANT VARCHAR2(50)   := 'ISSUE IN SP_SEL_BALANCE_LOAN: ';
    CSG_X CONSTANT                 VARCHAR2(3)    := 'X';
BEGIN

    WITH TBL_LOANS AS (SELECT FI_LOAN_ID, FI_ADMIN_CENTER_ID
                       FROM JSON_TABLE(PA_LOANS, '$[*]'
                                       COLUMNS (
                                           FI_LOAN_ID NUMBER PATH '$.FI_LOAN_ID',
                                           FI_ADMIN_CENTER_ID NUMBER PATH '$.FI_ADMIN_CENTER_ID'
                                           )
                            ))


    SELECT JSON_ARRAYAGG(
                   JSON_OBJECT(
                           'FI_LOAN_ID' VALUE LOAN.FI_LOAN_ID,
                           'FI_ADMIN_CENTER_ID' VALUE LOAN.FI_ADMIN_CENTER_ID,
                           'FN_PRINCIPAL_BALANCE' VALUE LOAN.FN_PRINCIPAL_BALANCE,
                           'FN_FINANCE_CHARGE_BALANCE' VALUE LOAN.FN_FINANCE_CHARGE_BALANCE,
                           'FN_ADDITIONAL_CHARGE_BALANCE' VALUE LOAN.FN_ADDITIONAL_CHARGE_BALANCE,
                           'FN_PAID_INTEREST_AMOUNT' VALUE LOAN.FN_PAID_INTEREST_AMOUNT,
                           'DETAILS' VALUE (SELECT JSON_ARRAYAGG(
                                                           JSON_OBJECT(
                                                                   'FI_LOAN_CONCEPT_ID' VALUE
                                                                   BALANCE_DETAIL.FI_LOAN_CONCEPT_ID,
                                                                   'FN_ITEM_AMOUNT' VALUE BALANCE_DETAIL.FN_ITEM_AMOUNT
                                                           )
                                                   )
                                            FROM SC_CREDIT.TA_LOAN_BALANCE_DETAIL BALANCE_DETAIL
                                                    WHERE BALANCE_DETAIL.FI_LOAN_ID = LOAN.FI_LOAN_ID
                                              AND BALANCE_DETAIL.FI_ADMIN_CENTER_ID = LOAN.FI_ADMIN_CENTER_ID
                                              AND BALANCE_DETAIL.FI_LOAN_BALANCE_ID = (SELECT MAX(MAX_DETAIL.FI_LOAN_BALANCE_ID)
                                                                              FROM SC_CREDIT.TA_LOAN_BALANCE MAX_DETAIL
                                                                              WHERE MAX_DETAIL.FI_LOAN_ID = LOAN.FI_LOAN_ID
                                                                                AND MAX_DETAIL.FI_ADMIN_CENTER_ID = LOAN.FI_ADMIN_CENTER_ID))
                   )
           )
    INTO PA_OUTPUT_BALANCE_LOAN
    FROM TBL_LOANS LOAN_LIST
             INNER JOIN SC_CREDIT.TA_LOAN LOAN
                        ON LOAN_LIST.FI_LOAN_ID = LOAN.FI_LOAN_ID
                            AND LOAN_LIST.FI_ADMIN_CENTER_ID = LOAN.FI_ADMIN_CENTER_ID;

    PA_STATUS_CODE := CSG_SUCCESS_CODE;
    PA_STATUS_MSG := CSG_SUCCESS_MSG;


EXCEPTION
    WHEN OTHERS THEN
        PA_STATUS_CODE := SQLCODE;
        PA_STATUS_MSG := SQLERRM || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;

        SC_CREDIT.SP_ERROR_LOG(CSL_ISSUE_BAL_SEQ_MSG, SQLCODE, SQLERRM,
                               DBMS_UTILITY.FORMAT_ERROR_BACKTRACE,
                               CSG_X, CSG_ARROW);

END SP_SEL_BALANCE_LOAN;

/

GRANT EXECUTE ON SC_CREDIT.SP_SEL_BALANCE_LOAN TO USRNCPCREDIT1
/
GRANT EXECUTE ON SC_CREDIT.SP_SEL_BALANCE_LOAN TO USRPURPOSEWS
/
GRANT EXECUTE ON SC_CREDIT.SP_SEL_BALANCE_LOAN TO USRCREDIT02
/
