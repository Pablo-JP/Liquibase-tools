CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_UPD_LOAN_STATUS 
     (PA_JSON_INPUT        IN VARCHAR2,
      PA_JSON_OUPUT       OUT VARCHAR2,
      PA_STATUS_CODE      OUT NUMBER,
      PA_STATUS_MESSAGE   OUT VARCHAR2)
IS
/**********************************************************************************************************************************************
PROJECT:            PURPOSE_LIFE_LOAN_CYCLE
DESCRIPTION:        THIS STORE PROCEDURE DOES AN UPDATE OF THE FIELD FI_LOAN_STATUS_ID AND FD_LOAN_STATUS_DATE ON THE SC_CREDIT.TA_LOAN TABLE
PRECONDITIONS:      IT MUST RECEIVE A JSON OBJECT
CREATED DATE:       09/10/2024
CREATOR:            CESAR EDUARDO MEDINA MACIEL
MODIFICATION DATE:  19/12/2024
USER MODIFICATION : AIXA SARMIENTO
***********************************************************************************************************************************************/
      CSL_0                             CONSTANT SIMPLE_INTEGER := 0;
      CSL_DATE_FORMAT               CONSTANT VARCHAR2(25)   := 'MM/DD/YYYY hh24:mi:ss';
      CSL_ARROW                     CONSTANT VARCHAR2(5)    := '-->';
      CSL_MSG_SUCCESS                   CONSTANT VARCHAR2(10)   := 'SUCCESS';
      EXC_ERROR_DETAIL              EXCEPTION;
          VL_CUR_RESULTS                SC_CREDIT.PA_TYPES.TYP_CURSOR;
          VL_STATUS_CODE                NUMBER(5);
      VL_STATUS_MESSAGE             VARCHAR2(200);
      VL_DATE                       DATE;

TYPE REC_LO_STAT IS RECORD (
      FI_LOAN_ID                                        SC_CREDIT.TA_LOAN_STATUS.FI_LOAN_ID%TYPE,
          FI_ADMIN_CENTER_ID            SC_CREDIT.TA_LOAN_STATUS.FI_ADMIN_CENTER_ID%TYPE,
      FI_LOAN_OPERATION_ID          SC_CREDIT.TA_LOAN_OPERATION.FI_LOAN_OPERATION_ID%TYPE,
      FI_LOAN_STATUS_ID             SC_CREDIT.TA_LOAN_STATUS.FI_LOAN_STATUS_ID%TYPE,
      FI_LOAN_STATUS_OLD_ID         SC_CREDIT.TA_LOAN_STATUS.FI_LOAN_STATUS_OLD_ID%TYPE,
      FI_TRIGGER_ID                 SC_CREDIT.TA_LOAN_STATUS.FI_TRIGGER_ID%TYPE,
      FD_LOAN_STATUS_DATE           VARCHAR2(25),
      FC_UUID_TRACKING              SC_CREDIT.TA_LOAN_OPERATION.FC_UUID_TRACKING%TYPE
      );
          VL_LO_STAT                    REC_LO_STAT;

BEGIN

   VL_DATE := SYSDATE;

   SELECT
          FI_LOAN_ID,
              FI_ADMIN_CENTER_ID,
              FI_LOAN_OPERATION_ID,
              NVL(FI_LOAN_STATUS_ID,0),
          NVL(FI_LOAN_STATUS_OLD_ID,0),
              FI_TRIGGER_ID,
          FD_LOAN_STATUS_DATE,
          FC_UUID_TRACKING
     INTO
          VL_LO_STAT
     FROM JSON_TABLE(
          PA_JSON_INPUT
   , '$'
      COLUMNS(
          FI_LOAN_ID                      NUMBER(15)    PATH   '$.loan.id',
          FI_ADMIN_CENTER_ID          NUMBER(8)     PATH   '$.loan.adminCenterId',
          FI_LOAN_OPERATION_ID        NUMBER(15)    PATH   '$.loan.operationId',
          FI_LOAN_STATUS_ID           NUMBER(3)     PATH   '$.loan.status.newId',
          FI_LOAN_STATUS_OLD_ID       VARCHAR2      PATH   '$.loan.status.oldId',
          FI_TRIGGER_ID               NUMBER(3)     PATH   '$.loan.triggerId',
          FD_LOAN_STATUS_DATE         VARCHAR2(25)  PATH   '$.loan.status.detail.endDate',
          FC_UUID_TRACKING            VARCHAR2(36)  PATH   '$.control.uuidTracking'
          ));

   INSERT
     INTO SC_CREDIT.TA_LOAN_STATUS
          (
              FI_LOAN_ID,
              FI_ADMIN_CENTER_ID,
              FI_LOAN_OPERATION_ID,
              FI_LOAN_STATUS_ID,
          FI_LOAN_STATUS_OLD_ID,
              FI_TRIGGER_ID,
              FD_LOAN_STATUS_DATE,
              FC_USER,
              FD_CREATED_DATE,
              FD_MODIFICATION_DATE
                  )
   VALUES (
              VL_LO_STAT.FI_LOAN_ID,
                  VL_LO_STAT.FI_ADMIN_CENTER_ID,
                  VL_LO_STAT.FI_LOAN_OPERATION_ID,
                  VL_LO_STAT.FI_LOAN_STATUS_ID,
                  VL_LO_STAT.FI_LOAN_STATUS_OLD_ID,
                  VL_LO_STAT.FI_TRIGGER_ID,
                  TO_DATE(VL_LO_STAT.FD_LOAN_STATUS_DATE,CSL_DATE_FORMAT),
                  USER,
                  VL_DATE,
                  VL_DATE);

   UPDATE SC_CREDIT.TA_LOAN
      SET
          FI_LOAN_STATUS_ID    = VL_LO_STAT.FI_LOAN_STATUS_ID,
          FD_LOAN_STATUS_DATE  = TO_DATE(VL_LO_STAT.FD_LOAN_STATUS_DATE,CSL_DATE_FORMAT),
          FD_MODIFICATION_DATE = VL_DATE
    WHERE
          FI_LOAN_ID           = VL_LO_STAT.FI_LOAN_ID
      AND FI_ADMIN_CENTER_ID   = VL_LO_STAT.FI_ADMIN_CENTER_ID;

        SC_CREDIT.SP_UPD_LOAN_STATUS_DETAIL(VL_LO_STAT.FI_LOAN_ID,
                                            VL_LO_STAT.FI_ADMIN_CENTER_ID,
                                                                                PA_JSON_INPUT,
                                        PA_JSON_OUPUT,
                                                                                VL_STATUS_CODE,
                                                                                VL_STATUS_MESSAGE);

        IF VL_STATUS_CODE <> CSL_0 THEN
                RAISE EXC_ERROR_DETAIL;
        END IF;

   PA_STATUS_CODE    := CSL_0;                -- SUCCESS CODE
   PA_STATUS_MESSAGE := CSL_MSG_SUCCESS;          -- SUCCESS MESSAGE

   PA_JSON_OUPUT         := JSON_OBJECT('PA_STATUS_CODE'    VALUE PA_STATUS_CODE,
                                    'PA_STATUS_MESSAGE' VALUE PA_STATUS_MESSAGE
                                                                        );
COMMIT;

EXCEPTION
   WHEN EXC_ERROR_DETAIL THEN
         ROLLBACK;

         PA_STATUS_CODE    := 102;                                          -- NO DATA FOUND CODE
         PA_STATUS_MESSAGE := 'No update SP_UPD_LOAN_STATUS_DETAIL';        -- NO DATA FOUND MESSAGE
         PA_JSON_OUPUT := PA_JSON_OUPUT;

         SC_CREDIT.SP_ERROR_LOG ('SP_UPD_LOAN_STATUS', SQLCODE, SQLERRM,
         DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, VL_LO_STAT.FC_UUID_TRACKING,PA_JSON_INPUT );

   WHEN NO_DATA_FOUND THEN
      ROLLBACK;

         PA_STATUS_CODE    := 101;                     -- NO UPDATED CODE
         PA_STATUS_MESSAGE := 'No data updated';       -- NO LOAN MESSAGE
         PA_JSON_OUPUT     := JSON_OBJECT ('PA_STATUS_CODE'   VALUE SQLCODE,
                                                                               'PA_STATUS_MESSAGE' VALUE SQLERRM  || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                                                               FORMAT JSON);

                 SC_CREDIT.SP_ERROR_LOG ('SP_UPD_LOAN_STATUS', SQLCODE, SQLERRM,
         DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, VL_LO_STAT.FC_UUID_TRACKING,PA_JSON_INPUT );

   WHEN OTHERS THEN
      ROLLBACK;

         PA_STATUS_CODE    := 100;                               -- NO DATA FOUND CODE
         PA_STATUS_MESSAGE := 'No update/insertion done';        -- NO DATA FOUND MESSAGE
         PA_JSON_OUPUT     := JSON_OBJECT ('PA_STATUS_CODE'   VALUE SQLCODE,
                                                                              'PA_STATUS_MESSAGE' VALUE SQLERRM  || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                                                              FORMAT JSON);

         SC_CREDIT.SP_ERROR_LOG ('SP_UPD_LOAN_STATUS', SQLCODE, SQLERRM,
         DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, VL_LO_STAT.FC_UUID_TRACKING,PA_JSON_INPUT );

END SP_UPD_LOAN_STATUS;

/

GRANT EXECUTE ON SC_CREDIT.SP_UPD_LOAN_STATUS TO USRNCPCREDIT1
/
GRANT EXECUTE ON SC_CREDIT.SP_UPD_LOAN_STATUS TO USRBTCCREDIT1
/
