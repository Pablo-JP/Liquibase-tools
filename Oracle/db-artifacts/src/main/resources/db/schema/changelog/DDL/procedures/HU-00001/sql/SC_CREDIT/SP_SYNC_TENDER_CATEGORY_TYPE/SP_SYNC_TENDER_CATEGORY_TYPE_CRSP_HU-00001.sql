CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_SYNC_TENDER_CATEGORY_TYPE (
   PA_SYNC_JSON          CLOB,
   PA_UPDATED_ROWS   OUT NUMBER,
   PA_STATUS_CODE    OUT NUMBER,
   PA_STATUS_MSG     OUT VARCHAR2
   )
IS
/* **************************************************************
* PROJECT: NCP
* DESCRIPTION: CATALOG SYNCHONIZATION TC_TENDER_CATEGORY_TYPE
* CREATED DATE: 2025/01/09
* CREATOR: CESAR CORTES
* MODIFICATION DATE: 2025/01/09
************************************************************** */
BEGIN
   PA_STATUS_CODE := 0;
   PA_STATUS_MSG  := 'OK';

   MERGE INTO SC_CREDIT.TC_TENDER_CATEGORY_TYPE A
      USING (
         SELECT
                *
           FROM JSON_TABLE ( PA_SYNC_JSON, '$.tenderCategoryType[*]'
                            COLUMNS (
                                     ID NUMBER PATH '$.id',
                                     DESCRIPTION VARCHAR2 ( 50 ) PATH '$.description',
                                     STATUS NUMBER PATH '$.status',
                                     USER_NAME VARCHAR2 ( 50 ) PATH '$.user',
                                     CREATED_DATE TIMESTAMP PATH '$.createdDate',
                                     MODIFICATION_DATE TIMESTAMP PATH '$.modificationDate'
                                    )
                           )
            ) B ON ( A.FI_TENDER_CATEGORY_TYPE_ID = B.ID )
      WHEN MATCHED THEN
         UPDATE SET A.FC_TENDER_CATEGORY_TYPE_DESC = B.DESCRIPTION,
           A.FI_STATUS = B.STATUS,
           A.FC_USER = B.USER_NAME,
           A.FD_MODIFICATION_DATE = CAST(B.MODIFICATION_DATE AS DATE)
      WHEN NOT MATCHED THEN
         INSERT (
                  FI_TENDER_CATEGORY_TYPE_ID,
                  FC_TENDER_CATEGORY_TYPE_DESC,
                  FI_STATUS,
                  FC_USER,
                  FD_CREATED_DATE,
                  FD_MODIFICATION_DATE )
         VALUES ( B.ID,
                  B.DESCRIPTION,
                  B.STATUS,
                  B.USER_NAME,
                  B.CREATED_DATE,
                  B.MODIFICATION_DATE );

   PA_UPDATED_ROWS := SQL%ROWCOUNT;

   COMMIT;

   EXCEPTION
   WHEN OTHERS THEN
      ROLLBACK;
         PA_STATUS_CODE := SQLCODE;
         PA_STATUS_MSG  := SQLERRM;
         SC_CREDIT.SP_ERROR_LOG('SP_SYNC_TENDER_CATEGORY_TYPE', SQLCODE, SQLERRM, DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, NULL,'');

END SP_SYNC_TENDER_CATEGORY_TYPE;

/

GRANT EXECUTE ON SC_CREDIT.SP_SYNC_TENDER_CATEGORY_TYPE TO USRPURPOSEWS
/
GRANT EXECUTE ON SC_CREDIT.SP_SYNC_TENDER_CATEGORY_TYPE TO USRNCPCREDIT1
/
