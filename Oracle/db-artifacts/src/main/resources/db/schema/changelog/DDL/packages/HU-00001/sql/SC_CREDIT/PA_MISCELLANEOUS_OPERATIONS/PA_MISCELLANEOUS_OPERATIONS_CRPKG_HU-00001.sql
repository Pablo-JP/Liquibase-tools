CREATE OR REPLACE  PACKAGE SC_CREDIT.PA_MISCELLANEOUS_OPERATIONS AS
/*****************************************************************
  *PROJECT:      NCP
  *DESCRIPTION:  PACKAGE FOR CANCELLATION OF LOAN.
  *CREATOR:      JESUS ALBERTO SUAREZ XELO.
  *CREATED DATE: DEC-13-2024
  *MODIFICATED:  DEC-27-2024
*****************************************************************/

    CSG_SUCCES      		CONSTANT VARCHAR2(15) := 'SUCCESS';
    CSG_SEPARATOR   		CONSTANT VARCHAR2(10) := '->';
    CSG_X           		CONSTANT VARCHAR2(2)  := 'X';
    CSG_FORMATDATE  		CONSTANT VARCHAR2(50) := 'YYYY-MM-DDTHH24:MI:SSTZH:TZM';
    CSG_ZERO        		CONSTANT SIMPLE_INTEGER := 0;
    CSG_ONE         		CONSTANT SIMPLE_INTEGER := 1;
    CSG_TWO         		CONSTANT SIMPLE_INTEGER := 2;
	CSG_LOANIDJSON			CONSTANT VARCHAR2(100) := '$.loanId';
	CSG_ADMINCENTERID		CONSTANT VARCHAR2(100) := '$.adminCenterId';
	CSG_APPLICATIONDATE		CONSTANT VARCHAR2(100) := '$.applicationDate';
	CSG_TRANSACTIONNUMBER	CONSTANT VARCHAR2(100) := '$.transactionNumber';
	CSG_IPADDRESS			CONSTANT VARCHAR2(100) := '$.ipAddress';
	CSG_DEVICEJSON			CONSTANT VARCHAR2(100) := '$.device';
	CSG_USERJSON			CONSTANT VARCHAR2(100) := '$.user';
	CSG_MISCELLANOPERID    	CONSTANT VARCHAR2(100) := '$.miscellaneousOperationId';

    --PROCEDURE FOR INSERT BANKRUPTCY OF LOAN IN TABLE TA_LOAN_BANKRUPTCY
    PROCEDURE SP_INS_MISCELLAN_OPERATIONS(PA_JSON             IN CLOB,
                                          PA_STATUS_CODE     OUT NUMBER,
                                          PA_STATUS_MSG      OUT VARCHAR2);

END PA_MISCELLANEOUS_OPERATIONS;

/




CREATE OR REPLACE  PACKAGE BODY SC_CREDIT.PA_MISCELLANEOUS_OPERATIONS AS

    PROCEDURE SP_INS_MISCELLAN_OPERATIONS(PA_JSON             IN CLOB,
                                          PA_STATUS_CODE     OUT NUMBER,
                                          PA_STATUS_MSG      OUT VARCHAR2) AS

    VL_STORE                VARCHAR2(30) := 'SP_INS_MISCELLAN_OPERATIONS';
    VL_LOANID               NUMBER(30);
    VL_ADMINCENID           NUMBER(30);
    VL_IP                   VARCHAR2(39);
    VL_DEVICE               VARCHAR2(50);
    VL_USER                 VARCHAR2(30);
    VL_TRANSACTION          NUMBER(33);
    VL_APPLDATE             DATE;
    VL_APPLDATECHAR         VARCHAR2(50);
    VL_MISCELLAN_OPERAT_ID  NUMBER(33);
    VL_TENDER_SEQ           NUMBER(33);
    VL_LOAN_OPERATION_ID    NUMBER(33);

    BEGIN
        PA_STATUS_CODE := CSG_ZERO;
        PA_STATUS_MSG  := CSG_SUCCES;

        VL_ADMINCENID           := JSON_VALUE(PA_JSON, CSG_ADMINCENTERID);
        VL_LOANID               := JSON_VALUE(PA_JSON, CSG_LOANIDJSON);
        VL_MISCELLAN_OPERAT_ID  := JSON_VALUE(PA_JSON, CSG_MISCELLANOPERID);
        VL_APPLDATECHAR         := JSON_VALUE(PA_JSON, CSG_APPLICATIONDATE);
        VL_TRANSACTION          := JSON_VALUE(PA_JSON, CSG_TRANSACTIONNUMBER);
        VL_IP                   := JSON_VALUE(PA_JSON, CSG_IPADDRESS);
        VL_DEVICE               := JSON_VALUE(PA_JSON, CSG_DEVICEJSON);
        VL_USER                 := JSON_VALUE(PA_JSON, CSG_USERJSON);
        VL_APPLDATE             := CAST(TO_TIMESTAMP_TZ(VL_APPLDATECHAR, CSG_FORMATDATE) AS DATE);

        FOR CUR_CURSOR IN (SELECT LOAN_OPERATIONID
                            FROM JSON_TABLE(PA_JSON,'$[*]'
                                            COLUMNS  (NESTED PATH '$.loanOperations[*]'
                                                     COLUMNS (LOAN_OPERATIONID NUMBER   path '$.id')
                                                     )
                                           )
                           )
        LOOP
            INSERT INTO SC_CREDIT.TA_LOAN_MISCELLAN_OPERATIONS(FI_LOAN_OPERATION_ID,
                                                               FI_ADMIN_CENTER_ID,
                                                               FI_LOAN_ID,
                                                               FI_MISCELLAN_OPERATIONS_ID,
                                                               FD_APPLICATION_DATE,
                                                               FI_TRANSACTION,
                                                               FC_IP_ADDRESS,
                                                               FC_DEVICE,
                                                               FI_STATUS,
                                                               FC_USER,
                                                               FD_CREATED_DATE,
                                                               FD_MODIFICATION_DATE)
                                                        VALUES(CUR_CURSOR.LOAN_OPERATIONID,
                                                               VL_ADMINCENID,
                                                               VL_LOANID,
                                                               VL_MISCELLAN_OPERAT_ID,
                                                               VL_APPLDATE,
                                                               VL_TRANSACTION,
                                                               VL_IP,
                                                               VL_DEVICE,
                                                               CSG_ONE,
                                                               VL_USER,
                                                               SYSDATE,
                                                               SYSDATE);

        END LOOP;

            IF VL_MISCELLAN_OPERAT_ID = CSG_TWO THEN
                SELECT LOAN_OPERATIONID
                  INTO VL_LOAN_OPERATION_ID
                  FROM JSON_TABLE(PA_JSON,'$[*]'
                                  COLUMNS (NESTED PATH '$.loanOperations[*]'
                                          COLUMNS (LOAN_OPERATIONID NUMBER   path '$.id')
                                           )
                                  );

                FOR CUR_CURSOR IN (SELECT tenderId,
                                          tenderAmount
                                     FROM JSON_TABLE(PA_JSON,'$[*]'
                                                     COLUMNS (NESTED PATH '$.loanOperations.tenderTypes[*]'
                                                             COLUMNS (tenderId     NUMBER PATH '$.id',
                                                                      tenderAmount NUMBER PATH '$.amount')
                                                              )
                                                     )
                                   )
                    LOOP
                        SELECT COUNT(FI_LOAN_OPERATION_ID) + CSG_ONE
                          INTO VL_TENDER_SEQ
                          FROM SC_CREDIT.TA_LOAN_OPERATION_TENDER
                         WHERE FI_LOAN_ID = VL_LOANID
                           AND FI_LOAN_OPERATION_ID = VL_LOAN_OPERATION_ID
                           AND FI_ADMIN_CENTER_ID = VL_ADMINCENID;

                        INSERT INTO SC_CREDIT.TA_LOAN_OPERATION_TENDER( FI_LOAN_ID,
                                                                       FI_LOAN_OPERATION_ID,
                                                                       FI_ADMIN_CENTER_ID,
                                                                       FI_TENDER_TYPE_ID,
                                                                       FI_OPERATION_TENDER_SEQ,
                                                                       FN_OPERATION_AMOUNT,
                                                                       FI_STATUS,
                                                                       FC_USER,
                                                                       FD_CREATED_DATE,
                                                                       FD_MODIFICATION_DATE)
                                                                VALUES(VL_LOANID,
                                                                       VL_LOAN_OPERATION_ID,
                                                                       VL_ADMINCENID,
                                                                       CUR_CURSOR.tenderId,
                                                                       VL_TENDER_SEQ,
                                                                       CUR_CURSOR.tenderAmount,
                                                                       CSG_ONE,
                                                                       VL_USER,
                                                                       SYSDATE,
                                                                       SYSDATE);
                    END LOOP;

            END IF;


    COMMIT;

    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            PA_STATUS_CODE := SQLCODE;
            PA_STATUS_MSG := SQLERRM
                             || CSG_SEPARATOR
                             || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
            SC_CREDIT.SP_ERROR_LOG(VL_STORE, SQLCODE, SQLERRM, DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X,
                                  CSG_X);
        WHEN OTHERS THEN
            PA_STATUS_CODE := SQLCODE;
            PA_STATUS_MSG := SQLERRM
                             || CSG_SEPARATOR
                             || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
            SC_CREDIT.SP_ERROR_LOG(VL_STORE, SQLCODE, SQLERRM, DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X,
                                  CSG_X);
    END SP_INS_MISCELLAN_OPERATIONS;

END PA_MISCELLANEOUS_OPERATIONS;

/


GRANT EXECUTE ON SC_CREDIT.PA_MISCELLANEOUS_OPERATIONS TO USRNCPCREDIT1
/
GRANT EXECUTE ON SC_CREDIT.PA_MISCELLANEOUS_OPERATIONS TO USRNCPCREDIT1
/
