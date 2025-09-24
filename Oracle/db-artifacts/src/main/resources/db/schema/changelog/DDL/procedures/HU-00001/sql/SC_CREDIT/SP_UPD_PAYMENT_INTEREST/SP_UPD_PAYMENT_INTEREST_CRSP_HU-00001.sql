CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_UPD_PAYMENT_INTEREST (
    PA_FI_LOAN_ID                   IN NUMBER
   ,PA_FI_ADMIN_CENTER_ID           IN NUMBER
   ,PA_PAYMENT_INTEREST             IN NUMBER
   ,PA_COMMIT                       IN NUMBER
   ,PA_STATUS_CODE                  OUT NUMBER
   ,PA_STATUS_MSG                   OUT VARCHAR2
   )
   IS
   /* **************************************************************
   * DESCRIPTION: UPDATE IN TABLE INTEREST IN INTEREST
   * PRECONDITIONS:
   * CREATED DATE: 04/11/2024
   * CREATOR: IVAN LOPEZ
   ************************************************************** */

   CSL_0                 CONSTANT SIMPLE_INTEGER := 0;
   CSL_1                 CONSTANT SIMPLE_INTEGER := 1;
   CSL_SP                CONSTANT SIMPLE_INTEGER := 1;
   CSL_2002              CONSTANT SIMPLE_INTEGER := -20002;
   CSL_2003              CONSTANT SIMPLE_INTEGER := -20003;
   CSL_MSG_SUCCESS       CONSTANT VARCHAR2(20)   := 'SUCCESS';
   CSL_NOT_AFFECT_DATA         CONSTANT VARCHAR2(20)   := 'DOES NOT AFFECT DAT';
    CSL_NOT_DATA         CONSTANT VARCHAR2(20)   := 'NO DATA FOUND';

    CLS_CONDITION     CONSTANT VARCHAR2(20)   := 'PAYMENT';


     VL_PAYMENT_NUMBER                  NUMBER(3);
     VL_DAYS_ACUM_BY_TERM               NUMBER(14,4);
     VL_ORDER                           NUMBER(2,0);
     VL_PAYMENT                         NUMBER(14,4);

   BEGIN

      SELECT
            A.FI_PAYMENT_NUMBER_ID,
            A.FI_DAYS_ACUM_BY_TERM,
            A.FN_PAYMENT_INTEREST
          ,ROW_NUMBER() OVER (PARTITION BY A.FI_LOAN_ID ORDER BY A.FI_PAYMENT_NUMBER_ID DESC,A.FI_DAYS_ACUM_BY_TERM DESC) AS ORDEN
       INTO VL_PAYMENT_NUMBER,VL_DAYS_ACUM_BY_TERM,VL_PAYMENT  ,VL_ORDER
       FROM sc_credit.ta_loan_interest A
        WHERE A.FI_LOAN_ID = PA_FI_LOAN_ID
      AND A.FI_ADMIN_CENTER_ID = PA_FI_ADMIN_CENTER_ID
      FETCH NEXT 1 ROW ONLY;


     UPDATE sc_credit.ta_loan_interest SET FN_PAYMENT_INTEREST = VL_PAYMENT + PA_PAYMENT_INTEREST ,
                                           FD_MODIFICATION_DATE = SYSDATE,
                                           FC_CONDITION_INTEREST = CLS_CONDITION
     WHERE FI_LOAN_ID = PA_FI_LOAN_ID
     AND FI_ADMIN_CENTER_ID = PA_FI_ADMIN_CENTER_ID
     AND FI_PAYMENT_NUMBER_ID = VL_PAYMENT_NUMBER
     AND FI_DAYS_ACUM_BY_TERM = VL_DAYS_ACUM_BY_TERM;

     IF PA_COMMIT = CSL_1 THEN
         COMMIT;
     END IF;

       PA_STATUS_CODE:= CSL_0;
       PA_STATUS_MSG := CSL_MSG_SUCCESS;


      EXCEPTION
        WHEN NO_DATA_FOUND THEN
         PA_STATUS_CODE:= CSL_2003;
         PA_STATUS_MSG := CSL_NOT_DATA;
        WHEN OTHERS THEN
        ROLLBACK;
        PA_STATUS_CODE := SQLCODE;
        PA_STATUS_MSG := SQLERRM  || ' -> ' ||  DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
        SC_CREDIT.SP_ERROR_LOG(UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP)
                                    ,SQLCODE
                                    ,SQLERRM
                                    ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                    ,PA_FI_LOAN_ID || PA_FI_ADMIN_CENTER_ID
                                    ,NULL);
END SP_UPD_PAYMENT_INTEREST;

/

GRANT EXECUTE ON SC_CREDIT.SP_UPD_PAYMENT_INTEREST TO USRNCPCREDIT1
/
