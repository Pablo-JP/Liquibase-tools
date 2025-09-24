CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_SYNC_PAYMENT_TYPE (PA_SYNC_JSON   IN CLOB,
                                                            PA_UPDATED_ROWS out NUMBER,
                                                            PA_STATUS_CODE OUT NUMBER,
                                                            PA_STATUS_MSG  OUT VARCHAR2) IS
/* **************************************************************
* PROJECT: NCP
* DESCRIPTION: CATALOG SYNCHONIZATION TC_PAYMENT_TYPE
* CREATED DATE: 2025/01/09
* CREATOR: CESAR CORTES
* MODIFICATION DATE: 2025/01/09
************************************************************** */
  BEGIN
    PA_STATUS_CODE := 0;
    PA_STATUS_MSG  := 'OK';

    MERGE INTO SC_CREDIT.TC_PAYMENT_TYPE A
        USING (SELECT *
                 FROM JSON_TABLE ( PA_SYNC_JSON, '$.paymentType[*]'
                                 COLUMNS (FI_PAYMENT_TYPE_ID    NUMBER(5)       PATH '$.id',
                                          FC_PAYMENT_TYPE_DESC  VARCHAR2(50)    PATH '$.description',
                                          FI_STATUS             NUMBER(2)       PATH '$.status',
                                          FC_USER               VARCHAR2(30)    PATH '$.user',
                                          FD_CREATED_DATE       TIMESTAMP       PATH '$.createdDate',
                                          FD_MODIFICATION_DATE  TIMESTAMP       PATH '$.modificationDate')
                                 )
               ) B
            ON ( A.FI_PAYMENT_TYPE_ID = B.FI_PAYMENT_TYPE_ID )
        WHEN MATCHED THEN UPDATE
            SET A.FC_PAYMENT_TYPE_DESC = B.FC_PAYMENT_TYPE_DESC,
                A.FI_STATUS = B.FI_STATUS,
                A.FC_USER = B.FC_USER,
                A.FD_MODIFICATION_DATE = CAST(B.FD_MODIFICATION_DATE AS DATE)
        WHEN NOT MATCHED THEN
            INSERT (FI_PAYMENT_TYPE_ID,
                    FC_PAYMENT_TYPE_DESC,
                    FI_STATUS,
                    FC_USER,
                    FD_CREATED_DATE,
                    FD_MODIFICATION_DATE )
            VALUES (B.FI_PAYMENT_TYPE_ID,
                    B.FC_PAYMENT_TYPE_DESC,
                    B.FI_STATUS,
                    B.FC_USER,
                    CAST(B.FD_CREATED_DATE AS DATE),
                    CAST(B.FD_MODIFICATION_DATE AS DATE) );

        PA_UPDATED_ROWS := SQL%ROWCOUNT;

  COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      PA_STATUS_CODE := SQLCODE;
      PA_STATUS_MSG  := SQLERRM;
      SC_CREDIT.SP_ERROR_LOG('SP_SYNC_PAYMENT_TYPE', SQLCODE, SQLERRM, DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, NULL,'');

END SP_SYNC_PAYMENT_TYPE;

/

GRANT EXECUTE ON SC_CREDIT.SP_SYNC_PAYMENT_TYPE TO USRPURPOSEWS
/
GRANT EXECUTE ON SC_CREDIT.SP_SYNC_PAYMENT_TYPE TO USRNCPCREDIT1
/
