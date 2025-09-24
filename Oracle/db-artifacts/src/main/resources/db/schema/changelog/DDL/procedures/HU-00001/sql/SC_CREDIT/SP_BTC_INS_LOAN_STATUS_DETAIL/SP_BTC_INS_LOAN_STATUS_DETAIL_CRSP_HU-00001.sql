CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_BTC_INS_LOAN_STATUS_DETAIL 
   (
   PA_LOAN_ID               IN NUMBER
  ,PA_ADMIN_CENTER_ID       IN NUMBER
  ,PA_STATUS_ID             IN NUMBER
  ,PA_ACTION_ID             IN NUMBER
  ,PA_COUNTER_DAY           IN NUMBER
  ,PA_INITIAL_DATE          IN VARCHAR2
  ,PA_PAYMENT_NUMBER_ID     IN NUMBER
  ,PA_FINAL_DATE            IN VARCHAR2
  ,PA_ON_OFF                IN NUMBER
  ,PA_COMMIT                IN NUMBER
  ,PA_STATUS_CODE           OUT NUMBER
  ,PA_STATUS_MSG            OUT VARCHAR2
   )
   IS
    /* **************************************************************
   * DESCRIPTION: PROCESS TO INSERT IN TABLE LOAN_STATUS_DETAIL
   * CREATED DATE: 11/11/2024
   * CREATOR: CRISTHIAN MORALES
   ************************************************************** */

   CSL_0                        CONSTANT SIMPLE_INTEGER := 0;
   CSL_1                        CONSTANT SIMPLE_INTEGER := 1;
   CSL_DATE_FORMAT              CONSTANT VARCHAR2(40)   := 'MM/DD/YYYY hh24:mi:ss';
   CSL_SP                       CONSTANT SIMPLE_INTEGER := 1;
   CSL_2002                     CONSTANT SIMPLE_INTEGER := -20002;
   CSL_MSG_SUCCESS              CONSTANT VARCHAR2(20)   := ' SUCCESS';
   CSL_TA_LOAN_STATUS_DETAIL    CONSTANT VARCHAR2(20)   := ' LOAN_STATUS_DETAIL';
   CSL_NOT_INSERT               CONSTANT VARCHAR2(20)   := ' NOT_INSERT ';
   CSL_ADMIN_CENTER_ID          CONSTANT VARCHAR2(20)   := ' ADMIN_CENTER_ID';
   CSL_ARROW                    CONSTANT VARCHAR2(20)   := '->';

   BEGIN

      PA_STATUS_CODE:= CSL_0;
      PA_STATUS_MSG := CSL_MSG_SUCCESS;

      INSERT INTO SC_CREDIT.TA_LOAN_STATUS_DETAIL
                 (FI_LOAN_ID
                 ,FI_ADMIN_CENTER_ID
                 ,FI_REGISTRATION_NUMBER
                 ,FI_LOAN_STATUS_ID
                 ,FI_ACTION_DETAIL_ID
                 ,FI_COUNTER_DAY
                 ,FD_INITIAL_DATE
                 ,FI_PAYMENT_NUMBER_ID
                 ,FD_FINAL_DATE
                 ,FI_ON_OFF
                 ,FC_USER
                 ,FD_CREATED_DATE
                 ,FD_MODIFICATION_DATE)
          VALUES (PA_LOAN_ID
                 ,PA_ADMIN_CENTER_ID
                 ,SC_CREDIT.SE_LOAN_STATUS_DETAIL.NEXTVAL
                 ,PA_STATUS_ID
                 ,PA_ACTION_ID
                 ,PA_COUNTER_DAY
                 ,TO_DATE(PA_INITIAL_DATE,CSL_DATE_FORMAT)
                 ,PA_PAYMENT_NUMBER_ID
                 ,TO_DATE(PA_FINAL_DATE,CSL_DATE_FORMAT)
                 ,PA_ON_OFF
                 ,USER
                 ,SYSDATE
                 ,SYSDATE
                 );

      IF SQL%ROWCOUNT = CSL_0 THEN
      RAISE_APPLICATION_ERROR(CSL_2002, CSL_TA_LOAN_STATUS_DETAIL || CSL_NOT_INSERT);
      END IF;

      IF PA_COMMIT = CSL_1 THEN
         COMMIT;
      END IF;

      --EXCEPTION HANDLING
      EXCEPTION
      WHEN OTHERS THEN
      ROLLBACK;
      PA_STATUS_CODE := SQLCODE;
      PA_STATUS_MSG := SQLERRM || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;

      SC_CREDIT.SP_BATCH_ERROR_LOG(UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP)
                                  ,SQLCODE
                                  ,SQLERRM
                                  ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                  ,CSL_0
                                  ,NULL
                                  ||CSL_ADMIN_CENTER_ID ||PA_ADMIN_CENTER_ID);

   END SP_BTC_INS_LOAN_STATUS_DETAIL;

/

GRANT EXECUTE ON SC_CREDIT.SP_BTC_INS_LOAN_STATUS_DETAIL TO USRNCPCREDIT1
/
GRANT EXECUTE ON SC_CREDIT.SP_BTC_INS_LOAN_STATUS_DETAIL TO USRBTCCREDIT1
/
