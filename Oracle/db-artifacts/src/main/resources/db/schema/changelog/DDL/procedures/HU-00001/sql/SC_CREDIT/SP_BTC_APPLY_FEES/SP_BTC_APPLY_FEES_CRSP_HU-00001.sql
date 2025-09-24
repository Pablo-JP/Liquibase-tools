CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_BTC_APPLY_FEES 
    (PTAB_STATUS_DETAIL          IN SC_CREDIT.TYP_TAB_BTC_STATUS_DETAIL
    ,PTAB_LOANS                  IN SC_CREDIT.TYP_TAB_BTC_LOAN
    ,PTAB_OPERATIONS             IN SC_CREDIT.TYP_TAB_BTC_OPERATION
    ,PTAB_OPERATIONS_DETAIL      IN SC_CREDIT.TYP_TAB_BTC_DETAIL
    ,PTAB_BALANCES               IN SC_CREDIT.TYP_TAB_BTC_BALANCE
    ,PTAB_BALANCES_DETAIL        IN SC_CREDIT.TYP_TAB_BTC_DETAIL
    ,PA_USER                     IN VARCHAR2
    ,PA_DEVICE                   IN VARCHAR2
    ,PA_GPS_LATITUDE             IN VARCHAR2
    ,PA_GPS_LONGITUDE            IN VARCHAR2
    ,PA_STATUS_CODE              OUT NUMBER
    ,PA_STATUS_MSG               OUT VARCHAR2
    ,PA_RECORDS_READ             OUT NUMBER
    ,PA_RECORDS_SUCCESS          OUT NUMBER
    ,PA_RECORDS_ERROR            OUT NUMBER
    ,PTAB_ERROR_RECORDS          OUT SC_CREDIT.TYP_TAB_BTC_ERROR)
IS
   /* **************************************************************
   * DESCRIPTION: PROCESS TO INSERT IN TABLE LOAN_STATUS_DETAIL,LOAN,STATUS,LOAN_OPERATION
   * LOAN_BALANCE
   * CREATED DATE: 21/11/2024
   * CREATOR: CRISTHIAN MORALES AND ITZEL TRINIDAD RAMOS
   ************************************************************** */
    CSL_0                      CONSTANT SIMPLE_INTEGER := 0;
    CSL_1                      CONSTANT SIMPLE_INTEGER := 1;
    CSL_NUMERROR               CONSTANT SIMPLE_INTEGER := -20012;
    CSL_MSG_SUCCESS            CONSTANT VARCHAR2(20)   := 'SUCCESS';
    CSL_TYPE_NULL              CONSTANT VARCHAR2(50) := 'TYPE IS NULL';
    CSL_SUCCESS_ERROR          CONSTANT VARCHAR2(50) := 'SUCCESS, WITH ERRORS RECORDS';
    CSL_PKG                    CONSTANT SIMPLE_INTEGER := 1;
    CSL_ARROW                  CONSTANT VARCHAR2(10)    := '->';
    CSL_SPACE                  CONSTANT VARCHAR2(40) := ' ';

    VL_I                       NUMBER(10,0) := 0;
    VL_TAB_ERRORS              SC_CREDIT.TYP_TAB_BTC_ERROR;
    VL_STATUS_CODE             NUMBER(10,0) := 0;
    VL_STATUS_MSG              VARCHAR2(1000);
    VL_PROCESS_DESC            VARCHAR2(150);
    VL_TAB_LOANS               SC_CREDIT.TYP_TAB_BTC_LOAN;
BEGIN
    PA_STATUS_CODE := CSL_0;
    PA_STATUS_MSG  := CSL_MSG_SUCCESS;
    PA_RECORDS_SUCCESS := 0;
    PA_RECORDS_ERROR := 0;
    PA_RECORDS_READ := 0;
    VL_TAB_ERRORS := SC_CREDIT.TYP_TAB_BTC_ERROR();

    IF PTAB_STATUS_DETAIL IS NULL OR PTAB_LOANS IS NULL OR
       PTAB_OPERATIONS IS NULL OR PTAB_OPERATIONS_DETAIL IS NULL OR
       PTAB_BALANCES IS NULL OR PTAB_BALANCES_DETAIL IS NULL THEN
       RAISE_APPLICATION_ERROR(CSL_NUMERROR, CSL_TYPE_NULL);
    END IF;

    VL_I := PTAB_STATUS_DETAIL.FIRST;
    PA_RECORDS_READ := PTAB_STATUS_DETAIL.COUNT;
    VL_TAB_LOANS := SC_CREDIT.TYP_TAB_BTC_LOAN();

    WHILE (VL_I IS NOT NULL) LOOP
    BEGIN

       VL_PROCESS_DESC := 'SP_BTC_UPD_LOAN_STATUS_DETAIL';
       SC_CREDIT.SP_BTC_UPD_LOAN_STATUS_DETAIL
                (PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID
                ,PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID
                ,PTAB_STATUS_DETAIL(VL_I).FI_PAYMENT_NUMBER_ID
                ,NULL
                ,CSL_0
                ,NULL
                ,CSL_1
                ,CSL_0
                ,VL_STATUS_CODE
                ,VL_STATUS_MSG);

       IF(VL_STATUS_CODE != CSL_0)THEN
         RAISE_APPLICATION_ERROR( CSL_NUMERROR, VL_PROCESS_DESC || CSL_SPACE || VL_STATUS_MSG);
       END IF;

       VL_PROCESS_DESC := 'SP_BTC_INS_LOAN_STATUS_DETAIL';
       SC_CREDIT.SP_BTC_INS_LOAN_STATUS_DETAIL
                (PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID
                ,PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID
                ,PTAB_STATUS_DETAIL(VL_I).FI_LOAN_STATUS_ID
                ,PTAB_STATUS_DETAIL(VL_I).FI_ACTION_DETAIL_ID
                ,PTAB_STATUS_DETAIL(VL_I).FI_COUNTER_DAY
                ,PTAB_STATUS_DETAIL(VL_I).FD_INITIAL_DATE
                ,PTAB_STATUS_DETAIL(VL_I).FI_PAYMENT_NUMBER_ID
                ,PTAB_STATUS_DETAIL(VL_I).FD_FINAL_DATE
                ,PTAB_STATUS_DETAIL(VL_I).FI_ON_OFF
                ,CSL_0
                ,VL_STATUS_CODE
                ,VL_STATUS_MSG);

          IF(VL_STATUS_CODE != CSL_0)THEN
          RAISE_APPLICATION_ERROR( CSL_NUMERROR, VL_PROCESS_DESC || CSL_SPACE || VL_STATUS_MSG);
       END IF;

       SELECT SC_CREDIT.TYP_REC_BTC_LOAN(
                   LO.FI_ADMIN_CENTER_ID
                  ,LO.FI_LOAN_ID
                  ,LO.FN_PRINCIPAL_BALANCE
                  ,LO.FN_FINANCE_CHARGE_BALANCE
                  ,LO.FN_ADDITIONAL_CHARGE_BALANCE
                  ,LO.FI_ADDITIONAL_STATUS
                  ,LO.FI_CURRENT_BALANCE_SEQ
                  ,LO.FI_LOAN_STATUS_ID
                  ,LO.FC_LOAN_STATUS_DATE
                  ,LO.FI_TRANSACTION)             AS REC_BTC_LOAN
       BULK COLLECT INTO VL_TAB_LOANS
            FROM TABLE (PTAB_LOANS) LO
       WHERE LO.FI_ADMIN_CENTER_ID = PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID
            AND LO.FI_LOAN_ID = PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID;


       VL_PROCESS_DESC := 'SP_BTC_GEN_OPERATION_BALANCE';
       SC_CREDIT.SP_BTC_GEN_OPERATION_BALANCE
                  (VL_TAB_LOANS
                  ,PTAB_OPERATIONS
                  ,PTAB_OPERATIONS_DETAIL
                  ,PTAB_BALANCES
                  ,PTAB_BALANCES_DETAIL
                  ,PA_USER
                  ,PA_DEVICE
                  ,PA_GPS_LATITUDE
                  ,PA_GPS_LONGITUDE
                  ,CSL_0
                  ,VL_STATUS_CODE
                  ,VL_STATUS_MSG);

       IF(VL_STATUS_CODE != CSL_0)THEN
         RAISE_APPLICATION_ERROR( CSL_NUMERROR, VL_PROCESS_DESC || CSL_SPACE || VL_STATUS_MSG);
       END IF;

      PA_RECORDS_SUCCESS := PA_RECORDS_SUCCESS + CSL_1;
         COMMIT;
      EXCEPTION
      WHEN OTHERS THEN
           ROLLBACK;
           SC_CREDIT.SP_BATCH_ERROR_LOG(UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_PKG)
                                             ,SQLCODE
                                             ,SQLERRM
                                             ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                             ,CSL_0
                                             ,PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID || ',' || PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID);
           PA_RECORDS_ERROR :=PA_RECORDS_ERROR + CSL_1;

           VL_TAB_ERRORS.EXTEND;
           VL_TAB_ERRORS(VL_TAB_ERRORS.LAST) :=
           SC_CREDIT.TYP_REC_BTC_ERROR(PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID
                                            ,PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID
                                            ,UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_PKG)
                                            ,SQLCODE
                                            ,SQLERRM
                                            ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                            ,SYSDATE
                                            ,CSL_0
                                            ,NULL);
         END;
         VL_I := PTAB_STATUS_DETAIL.NEXT(VL_I);

       END LOOP;
       PTAB_ERROR_RECORDS := VL_TAB_ERRORS;
       IF(PA_RECORDS_ERROR > CSL_0)THEN
         PA_STATUS_CODE := CSL_1;
         PA_STATUS_MSG := CSL_SUCCESS_ERROR;
       END IF;
       COMMIT;

       EXCEPTION
       WHEN OTHERS THEN
         PA_STATUS_CODE := SQLCODE;
         PA_STATUS_MSG := SQLERRM || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;

         SC_CREDIT.SP_BATCH_ERROR_LOG(UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_PKG)
                                     ,SQLCODE
                                     ,SQLERRM
                                     ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                     ,CSL_0
                                     ,NULL);

    END SP_BTC_APPLY_FEES;

/

GRANT EXECUTE ON SC_CREDIT.SP_BTC_APPLY_FEES TO USRNCPCREDIT1
/
GRANT EXECUTE ON SC_CREDIT.SP_BTC_APPLY_FEES TO USRBTCCREDIT1
/
