-- Arreglar el SP a la nueva estructura
CREATE OR REPLACE PROCEDURE SC_CREDIT.SP_SYNC_COUNTRY (
    PA_SYNC_JSON     CLOB,
    PA_UPDATED_ROWS  OUT NUMBER,
    PA_STATUS_CODE   OUT NUMBER,
    PA_STATUS_MSG    OUT VARCHAR2
) IS
  CSL_0 CONSTANT SIMPLE_INTEGER := 0;
BEGIN
  PA_STATUS_CODE  := CSL_0;
  PA_STATUS_MSG   := 'OK';
  PA_UPDATED_ROWS := CSL_0;

  MERGE INTO SC_CREDIT.TC_COUNTRY A
  USING (
    SELECT *
    FROM JSON_TABLE ( PA_SYNC_JSON, '$.country[*]'
      COLUMNS (
        ID                NUMBER           PATH '$.id',
        name              VARCHAR2(50)     PATH '$.name',
        code              VARCHAR2(50)     PATH '$.code',
        STATUS            NUMBER           PATH '$.status',
        USER_NAME         VARCHAR2(50)     PATH '$.user',
        CREATED_DATE      TIMESTAMP        PATH '$.createdDate',
        MODIFICATION_DATE TIMESTAMP        PATH '$.modificationDate'
      )
    )
  ) B
  ON (A.FI_COUNTRY_ID = B.ID)
  WHEN MATCHED THEN UPDATE SET
      A.FC_COUNTRY_NAME       = B.name,
      A.FC_COUNTRY_CODE_ISO2       = B.code,            -- <== nuevo nombre
      A.FI_STATUS             = B.STATUS,
      A.FC_USER               = B.USER_NAME,
      A.FD_MODIFICATION_DATE  = CAST(B.MODIFICATION_DATE AS DATE)
  WHEN NOT MATCHED THEN
    INSERT (
      FI_COUNTRY_ID,
      FC_COUNTRY_NAME,
      FC_COUNTRY_CODE_ISO2,                             -- <== nuevo nombre
      FI_STATUS,
      FC_USER,
      FD_CREATED_DATE,
      FD_MODIFICATION_DATE
    )
    VALUES (
      B.ID,
      B.name,
      B.code,
      B.STATUS,
      B.USER_NAME,
      B.CREATED_DATE,
      B.MODIFICATION_DATE
    );

  PA_UPDATED_ROWS := SQL%ROWCOUNT;
  COMMIT;

EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    PA_STATUS_CODE := SQLCODE;
    PA_STATUS_MSG  := SQLERRM;
    SC_CREDIT.SP_ERROR_LOG('SP_SYNC_COUNTRY', SQLCODE, SQLERRM,
                           DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, NULL,'');
END SP_SYNC_COUNTRY;
/