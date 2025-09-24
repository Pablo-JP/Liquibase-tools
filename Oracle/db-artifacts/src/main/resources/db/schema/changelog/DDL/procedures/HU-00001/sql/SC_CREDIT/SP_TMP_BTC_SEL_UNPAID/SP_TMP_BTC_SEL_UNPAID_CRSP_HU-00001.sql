CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_TMP_BTC_SEL_UNPAID 
   (PA_FIRST_CENTER_ID        IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE
   ,PA_END_CENTER_ID          IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE
   ,PA_TODAY                  IN VARCHAR2
    ,PA_PROCESS             IN NUMBER
   ,PA_TRACK               IN NUMBER
   ,PA_STATUS_CODE            OUT NUMBER
   ,PA_STATUS_MSG             OUT VARCHAR2
   ,PA_CUR_SELECT             OUT SC_CREDIT.PA_TYPES.TYP_CURSOR)

   IS
      ----------------------------------------------------------------------
          -- PROJECT: LOAN LIFE CYCLE
      -- CREATOR: IVAN LOPEZ
      -- CREATED DATE:   02/01/2025
      -- DESCRIPTION: Select loan unpaid TEMP
      -- APPLICATION:  Process Batch of Purpose
      --PROSES 4
      ----------------------------------------------------------------------

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
                  ,LO.FI_LOAN_STATUS_ID                 AS FI_LOAN_STATUS_ID
                  ,PS.FI_PAYMENT_NUMBER_ID              AS FI_PAYMENT_NUMBER_ID
                  ,TO_CHAR(PS.FD_DUE_DATE,CSL_DATE)     AS FD_DUE_DATE
                  ,(SELECT COUNT(0) AS FLAG_NO_PAYMENT
              FROM SC_CREDIT.TA_LOAN_STATUS_DETAIL SD
             WHERE SD.FI_LOAN_ID = PS.FI_LOAN_ID
               AND SD.FI_ADMIN_CENTER_ID = PS.FI_ADMIN_CENTER_ID
               AND SD.FI_PAYMENT_NUMBER_ID = PS.FI_PAYMENT_NUMBER_ID
               AND SD.FI_ON_OFF = CSL_1
               AND SD.FI_ACTION_DETAIL_ID = CSL_3) AS FLAG_NO_PAYMENT
              FROM   SC_CREDIT.TA_TMP_LOAN_PROCESS L
        INNER JOIN      SC_CREDIT.TA_LOAN LO
         ON L.FI_LOAN_ID = LO.FI_LOAN_ID
         AND L.FI_ADMIN_CENTER_ID = LO.FI_ADMIN_CENTER_ID
        INNER JOIN SC_CREDIT.TA_PAYMENT_SCHEDULE PS
                ON L.FI_LOAN_ID = PS.FI_LOAN_ID
               AND L.FI_ADMIN_CENTER_ID = PS.FI_ADMIN_CENTER_ID
               AND PS.FI_PMT_SCHEDULE_STATUS_ID = CSL_1
               AND PS.FD_DUE_DATE <= TO_DATE(PA_TODAY, CSL_DATE)
             WHERE LO.FI_LOAN_STATUS_ID = CSL_3
             AND   L.FI_PROCESS = PA_PROCESS
               AND L.FI_TRACK = PA_TRACK
         )
         SELECT LO.FI_LOAN_ID                        AS FI_LOAN_ID
               ,LO.FI_ADMIN_CENTER_ID                AS FI_ADMIN_CENTER_ID
               ,LO.FI_LOAN_STATUS_ID                 AS FI_LOAN_STATUS_ID
               ,LO.FI_PAYMENT_NUMBER_ID              AS FI_PAYMENT_NUMBER_ID
               ,LO.FD_DUE_DATE                       AS FD_DUE_DATE
           FROM TABLOAN LO
          WHERE LO.FLAG_NO_PAYMENT = CSL_0;

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
END SP_TMP_BTC_SEL_UNPAID;

/

GRANT EXECUTE ON SC_CREDIT.SP_TMP_BTC_SEL_UNPAID TO USRNCPCREDIT1
/
GRANT EXECUTE ON SC_CREDIT.SP_TMP_BTC_SEL_UNPAID TO USRBTCCREDIT1
/
GRANT EXECUTE ON SC_CREDIT.SP_TMP_BTC_SEL_UNPAID TO USRCREDIT02
/
