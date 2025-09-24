CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_BTC_APPLY_ACTIONS 
 (PTAB_STATUS_DETAIL             IN SC_CREDIT.TYP_TAB_BTC_STATUS_DETAIL
    ,PA_STATUS_CODE              OUT NUMBER
    ,PA_STATUS_MSG               OUT VARCHAR2
    ,PA_RECORDS_READ             OUT NUMBER
    ,PA_RECORDS_SUCCESS          OUT NUMBER
    ,PA_RECORDS_ERROR            OUT NUMBER
    ,PTAB_ERROR_RECORDS          OUT SC_CREDIT.TYP_TAB_BTC_ERROR)
AS
    /* **************************************************************
    * PROJECT: LOAN-LIFE-CYCLE
    * DESCRIPTION: PROCESS TO GENERATE ACTIONS
    * CREATED DATE: 14/11/2024
    * CREATOR: ITZEL TRINIDAD RAMOS/CRISTHIAN MORALES
    * MODIFICATION DATE: 05/12/2024
    ************************************************************** */
    CSL_0            CONSTANT SIMPLE_INTEGER := 0;
    CSL_1            CONSTANT SIMPLE_INTEGER := 1;
    CSL_2            CONSTANT SIMPLE_INTEGER := 2;
    CSL_4            CONSTANT SIMPLE_INTEGER := 4;
    CSL_NUMERROR     CONSTANT SIMPLE_INTEGER := -20012;
    CSL_STATUS_DETAIL CONSTANT VARCHAR2(30) := 'SP_BTC_INS_LOAN_STATUS_DETAIL';
    CSL_UPD_STATUS_DETAIL CONSTANT VARCHAR2(30) := 'SP_BTC_UPD_LOAN_STATUS_DETAIL';
    CSL_UPD_LOAN     CONSTANT VARCHAR2(15) := 'SP_BTC_UPD_LOAN';
    CSL_MSG_SUCCESS  CONSTANT VARCHAR2(7)   := 'SUCCESS';
    CSG_SUCCESS_ERROR CONSTANT VARCHAR2(30) := 'SUCCESS, WITH ERRORS RECORDS';
    CSL_TYPE_NULL    CONSTANT VARCHAR2(23) := 'TYPE STATUS DETAIL NULL';
    CSL_PKG          CONSTANT SIMPLE_INTEGER := 1;
    CSL_ARROW        CONSTANT VARCHAR2(2)    := '->';
    CSL_SPACE        CONSTANT VARCHAR2(1) := ' ';
    CSL_COMMA        CONSTANT VARCHAR2(3) := ' , ';

    VL_I                          NUMBER(10,0) := 0;
    VL_TAB_ERRORS                 SC_CREDIT.TYP_TAB_BTC_ERROR;
    VL_STATUS_CODE                NUMBER(10,0) := 0;
    VL_STATUS_MSG                 VARCHAR2(1000);
    VL_PROCESS_DESC               VARCHAR2(150);
BEGIN
    PA_STATUS_CODE := CSL_0;
    PA_STATUS_MSG  := CSL_MSG_SUCCESS;
    PA_RECORDS_SUCCESS := CSL_0;
    PA_RECORDS_ERROR := CSL_0;
    PA_RECORDS_READ := CSL_0;
    VL_TAB_ERRORS := SC_CREDIT.TYP_TAB_BTC_ERROR();

    IF PTAB_STATUS_DETAIL IS NULL THEN
     RAISE_APPLICATION_ERROR( CSL_NUMERROR, CSL_TYPE_NULL);
    END IF;

    VL_I := PTAB_STATUS_DETAIL.FIRST;
    PA_RECORDS_READ := PTAB_STATUS_DETAIL.COUNT;

    WHILE (VL_I IS NOT NULL) LOOP
    BEGIN
        IF PTAB_STATUS_DETAIL(VL_I).FI_OPTION = CSL_1 THEN
           IF PTAB_STATUS_DETAIL(VL_I).FI_ON_OFF = CSL_1 THEN
                --UPDATE COUNT DAYS BY INSTALLMENT
                VL_PROCESS_DESC := CSL_UPD_STATUS_DETAIL;
                SC_CREDIT.SP_BTC_UPD_LOAN_STATUS_DETAIL
                   (PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID
                   ,PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID
                   ,PTAB_STATUS_DETAIL(VL_I).FI_PAYMENT_NUMBER_ID
                   ,PTAB_STATUS_DETAIL(VL_I).FI_COUNTER_DAY
                   ,CSL_1
                   ,NULL
                   ,CSL_1
                   ,CSL_0
                   ,VL_STATUS_CODE
                   ,VL_STATUS_MSG);

                IF(VL_STATUS_CODE != CSL_0)THEN
                   RAISE_APPLICATION_ERROR( CSL_NUMERROR, VL_PROCESS_DESC || CSL_SPACE || VL_STATUS_MSG);
                END IF;
            END IF;
        ELSE
            IF PTAB_STATUS_DETAIL(VL_I).FI_OPTION = CSL_2 THEN
                --OFF PREVIOUS ACTIONS BY INSTALLMENT
                VL_PROCESS_DESC := CSL_UPD_STATUS_DETAIL;
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
            ELSE
                --OFF PREVIOUS ACTIONS BY LOAN
                VL_PROCESS_DESC := CSL_UPD_STATUS_DETAIL;
                SC_CREDIT.SP_BTC_UPD_LOAN_STATUS_DETAIL
                   (PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID
                   ,PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID
                   ,PTAB_STATUS_DETAIL(VL_I).FI_PAYMENT_NUMBER_ID
                   ,NULL
                   ,CSL_0
                   ,NULL
                   ,CSL_0
                   ,CSL_0
                   ,VL_STATUS_CODE
                   ,VL_STATUS_MSG);

                IF(VL_STATUS_CODE != CSL_0)THEN
                   RAISE_APPLICATION_ERROR( CSL_NUMERROR, VL_PROCESS_DESC || CSL_SPACE || VL_STATUS_MSG);
                END IF;

                --UPDATE STATUS DEFAULT
                VL_PROCESS_DESC := CSL_UPD_LOAN;
                SC_CREDIT.SP_BTC_UPD_LOAN
                (PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID
                ,PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID
                ,CSL_4
                ,CSL_0
                ,VL_STATUS_CODE
                ,VL_STATUS_MSG
                );

                IF(VL_STATUS_CODE != CSL_0)THEN
                   RAISE_APPLICATION_ERROR( CSL_NUMERROR, VL_PROCESS_DESC || CSL_SPACE || VL_STATUS_MSG);
                END IF;
            END IF;

            --INSERT ACTION(Cure Period Non-compliance)
            VL_PROCESS_DESC := CSL_STATUS_DETAIL;
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
        END IF;

        PA_RECORDS_SUCCESS := PA_RECORDS_SUCCESS + CSL_1;
     --Exception block
     EXCEPTION
        WHEN OTHERS THEN
           ROLLBACK;
             SC_CREDIT.SP_BATCH_ERROR_LOG(UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_PKG)
                                         ,SQLCODE
                                         ,SQLERRM
                                         ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                         ,CSL_1
                                         ,PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID || CSL_COMMA || PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID);

             PA_RECORDS_ERROR :=PA_RECORDS_ERROR + CSL_1;

             --Adding to a collection, loans with error, detail
             VL_TAB_ERRORS.EXTEND;
             VL_TAB_ERRORS(VL_TAB_ERRORS.LAST) :=
             SC_CREDIT.TYP_REC_BTC_ERROR(PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID
                                        ,PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID
                                        ,UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_PKG)
                                        ,SQLCODE
                                        ,SQLERRM
                                        ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                        ,SYSDATE
                                        ,CSL_1
                                        ,NULL);
     END;

     VL_I := PTAB_STATUS_DETAIL.NEXT(VL_I);
     COMMIT;
    END LOOP;

    PTAB_ERROR_RECORDS := VL_TAB_ERRORS;
    IF(PA_RECORDS_ERROR > CSL_0)THEN
        PA_STATUS_CODE := CSL_1;
        PA_STATUS_MSG := CSG_SUCCESS_ERROR;
    END IF;
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
END SP_BTC_APPLY_ACTIONS;

/

GRANT EXECUTE ON SC_CREDIT.SP_BTC_APPLY_ACTIONS TO USRNCPCREDIT1
/
GRANT EXECUTE ON SC_CREDIT.SP_BTC_APPLY_ACTIONS TO USRBTCCREDIT1
/
