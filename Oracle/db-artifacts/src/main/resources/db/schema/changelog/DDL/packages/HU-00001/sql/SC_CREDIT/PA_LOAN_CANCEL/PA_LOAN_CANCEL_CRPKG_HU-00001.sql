CREATE OR REPLACE  PACKAGE SC_CREDIT.PA_LOAN_CANCEL AS

/*****************************************************************
  *PROJECT:      NCP
  *DESCRIPTION:  PACKAGE FOR CANCELLATION OF LOAN.
  *CREATOR:      JESUS ALBERTO SUAREZ XELO.
  *CREATED DATE: NOV-20-2024
  *MODIFICATED:  JAN-14-2025
*****************************************************************/

    CSG_ZERO 		CONSTANT SIMPLE_INTEGER := 0;
    CSG_ONE 		CONSTANT SIMPLE_INTEGER := 1;
    CSG_TWO			CONSTANT SIMPLE_INTEGER := 2;
    TYPE TYSPLIT_TBL IS TABLE OF VARCHAR2 (32767);
    CSG_SUCCES      CONSTANT VARCHAR2(15) := 'SUCCESS';
    CSG_SEPARATOR   CONSTANT VARCHAR2(10) := '->';
    CSG_X           CONSTANT VARCHAR2(2)  := 'X';
    CSG_COMMA       CONSTANT VARCHAR2(2)  := ',';

   --PROCEDURE FOR INSERT CANCELLATION OF LOAN IN TABLE TA_LOAN_CANCELLATION
    PROCEDURE SP_INS_CANCEL(PA_JSON            IN CLOB,
                            PA_OPERDATE        OUT VARCHAR2,
                            PA_STATUS_CODE     OUT NUMBER,
                            PA_STATUS_MSG      OUT VARCHAR2);

    --PROCEDURE FOR CURSOR WITH INFORMATION OF THE CUSTOMER ABOUT LOAN
    PROCEDURE SP_SEL_LOAN_RESCIND (
                        PA_LOAN_ID              IN SC_CREDIT.TA_LOAN.FI_LOAN_ID%TYPE,
                        PA_ADMIN_CENTER_ID      IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE,
                        PA_CUSTOMER_ID          IN SC_CREDIT.TA_LOAN.FC_CUSTOMER_ID%TYPE,
                        PA_OPERATION_TYPE       IN VARCHAR2,
                        PA_LOAN_CONCEPT         IN VARCHAR2,
                        PA_CUR_LOAN_OPERATION  OUT SC_CREDIT.PA_TYPES.TYP_CURSOR,
                        PA_CUR_LOAN            OUT SC_CREDIT.PA_TYPES.TYP_CURSOR,
                        PA_CUR_BAL_DET_CONCEPT OUT SC_CREDIT.PA_TYPES.TYP_CURSOR,
                        PA_STATUS_CODE         OUT NUMBER,
                        PA_STATUS_MSG          OUT VARCHAR2);

	--PROCEDURE FOR SELECTS IN TABLE TA_LOAN
    PROCEDURE SP_SEL_LOAN(
                       PA_LOAN_ID               IN SC_CREDIT.TA_LOAN.FI_LOAN_ID%TYPE,
                       PA_ADMIN_CENTER_ID       IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE,
                       PA_LOAN_OPERATION_ID     IN SC_CREDIT.TA_LOAN_OPERATION.FI_LOAN_OPERATION_ID%TYPE,
                       PA_LOAN_CONCEPT          IN VARCHAR2,
                       PA_CUR_LOAN              OUT SC_CREDIT.PA_TYPES.TYP_CURSOR,
                       PA_CUR_OPERATIONS        OUT SC_CREDIT.PA_TYPES.TYP_CURSOR,
                       PA_CUR_OP_DET_CONCEPT    OUT SC_CREDIT.PA_TYPES.TYP_CURSOR,
                       PA_CUR_BAL_DET_CONCEPT   OUT SC_CREDIT.PA_TYPES.TYP_CURSOR,
                       PA_CUR_OPERATION_TENDER  OUT SC_CREDIT.PA_TYPES.TYP_CURSOR,
                       PA_STATUS_CODE           OUT NUMBER,
                       PA_STATUS_MSG            OUT VARCHAR2);

	--FUNCION FOR SELECT IN ARRAY
    FUNCTION FN_SPLITST (PA_LIST IN VARCHAR2,
                        PA_DEL  IN VARCHAR2 := ' ') RETURN SC_CREDIT.PA_LOAN_CANCEL.TYSPLIT_TBL
        PIPELINED;

END PA_LOAN_CANCEL;

/




CREATE OR REPLACE  PACKAGE BODY SC_CREDIT.PA_LOAN_CANCEL AS

    PROCEDURE SP_INS_CANCEL(PA_JSON            IN CLOB,
                            PA_OPERDATE        OUT VARCHAR2,
                            PA_STATUS_CODE     OUT NUMBER,
                            PA_STATUS_MSG      OUT VARCHAR2) AS

    VL_STORE                VARCHAR2(30) := 'SP_INS_CANCEL';
    VL_LOANID               NUMBER(30)   := 0;
    VL_ADMINCENID           NUMBER(30)   := 0;
    VL_CANCELTYPE           NUMBER(30)   := 0;
    VL_CANCELCHARACT        VARCHAR2(50);
    VL_UUDITRACK            VARCHAR2(36);
    VL_IP                   VARCHAR2(39);
    VL_DEVICE               VARCHAR2(50);
    VL_USER                 VARCHAR2(30);
    VL_TENDERTYPE_ID        NUMBER(30)   := 0;
    VL_OPERATION_AMOUNT     NUMBER(30)   := 0;
    VL_REASON_CANCEL        VARCHAR2(255);
    VL_LOAN_OPERATION_ID    NUMBER(30)   := 0;
    VL_FORMATDATE           VARCHAR2(30) := 'YYYY-MM-DDTHH24:MI:SSTZH:TZM';
    VL_CANCELDATE           DATE;
    VL_TRANSACTION          NUMBER(33);
    VL_FORMATYYYY           VARCHAR2(30) :='YYYY-MM-DD HH24:MI:SS';
    VL_LOANCONCEPT          VARCHAR2(1000);
    VL_TENDER_SEQ           NUMBER(33);
	VL_LOANIDJSON			VARCHAR2(100) := '$.loanId';
	VL_ADMINCENTERID		VARCHAR2(100) := '$.adminCenterId';
	VL_CANCELATIONTYPEID	VARCHAR2(100) := '$.cancelationTypeId';
	VL_CANCELATIONDATE		VARCHAR2(100) := '$.cancelationDate';
	VL_REASONCANCELLATION	VARCHAR2(100) := '$.reasonCancellation';
	VL_UUIDTRACKING			VARCHAR2(100) := '$.uuidTracking';
	VL_IPADDRESS			VARCHAR2(100) := '$.ipAddress';
	VL_DEVICEJSON			VARCHAR2(100) := '$.device';
	VL_TENDERTYPEID			VARCHAR2(100) := '$.tenderTypeId';
	VL_OPERATIONAMOUNT		VARCHAR2(100) := '$.operationAmount';
	VL_USERJSON			    VARCHAR2(100) := '$.user';
	VL_LOANOPERATIONID		VARCHAR2(100) := '$.loanOperationId';
	VL_TRANSACTIONNUMBER	VARCHAR2(100) := '$.transactionNumber';

    BEGIN
        PA_STATUS_CODE := CSG_ZERO;
        PA_STATUS_MSG  := CSG_SUCCES;

        VL_LOANID               := JSON_VALUE(PA_JSON, VL_LOANIDJSON);
        VL_ADMINCENID           := JSON_VALUE(PA_JSON, VL_ADMINCENTERID);
        VL_CANCELTYPE           := JSON_VALUE(PA_JSON, VL_CANCELATIONTYPEID);
        VL_CANCELCHARACT        := JSON_VALUE(PA_JSON, VL_CANCELATIONDATE);
        VL_REASON_CANCEL        := JSON_VALUE(PA_JSON, VL_REASONCANCELLATION);
        VL_UUDITRACK            := JSON_VALUE(PA_JSON, VL_UUIDTRACKING);
        VL_IP                   := JSON_VALUE(PA_JSON, VL_IPADDRESS);
        VL_DEVICE               := JSON_VALUE(PA_JSON, VL_DEVICEJSON);
        VL_TENDERTYPE_ID        := JSON_VALUE(PA_JSON, VL_TENDERTYPEID);
        VL_OPERATION_AMOUNT     := JSON_VALUE(PA_JSON, VL_OPERATIONAMOUNT);
        VL_USER                 := JSON_VALUE(PA_JSON, VL_USERJSON);
        VL_LOAN_OPERATION_ID    := JSON_VALUE(PA_JSON, VL_LOANOPERATIONID);
        VL_CANCELDATE           := CAST(TO_TIMESTAMP_TZ(VL_CANCELCHARACT, VL_FORMATDATE) AS DATE);
        VL_TRANSACTION          := JSON_VALUE(PA_JSON, VL_TRANSACTIONNUMBER);

        SELECT LISTAGG(DISTINCT FI_BALANCE_CATEGORY_ID, CSG_COMMA) WITHIN GROUP(ORDER BY FI_BALANCE_CATEGORY_ID)
          INTO VL_LOANCONCEPT
          FROM SC_CREDIT.TC_LOAN_CONCEPT;

        SELECT DISTINCT
               TO_CHAR(FD_OPERATION_DATE, VL_FORMATYYYY)
          INTO PA_OPERDATE
          FROM SC_CREDIT.TA_LOAN_OPERATION
         WHERE FI_LOAN_ID = VL_LOANID
           AND FI_LOAN_OPERATION_ID = VL_LOAN_OPERATION_ID
           AND FI_ADMIN_CENTER_ID = VL_ADMINCENID;

        INSERT INTO SC_CREDIT.TA_LOAN_CANCELLATION(FI_LOAN_ID,
                                                   FI_ADMIN_CENTER_ID,
                                                   FI_CANCELLATION_TYPE_ID,
                                                   FD_CANCELLATION,
                                                   FC_REASON,
                                                   FI_TRANSACTION,
                                                   FI_STATUS,
                                                   FC_UUID_TRACKING,
                                                   FC_IP_ADDRESS,
                                                   FC_DEVICE,
                                                   FC_USER,
                                                   FD_CREATED_DATE,
                                                   FD_MODIFICATION_DATE)
                                            VALUES(VL_LOANID,
                                                   VL_ADMINCENID,
                                                   VL_CANCELTYPE,
                                                   VL_CANCELDATE,
                                                   VL_REASON_CANCEL,
                                                   VL_TRANSACTION,
                                                   CSG_ONE,
                                                   VL_UUDITRACK,
                                                   VL_IP,
                                                   VL_DEVICE,
                                                   VL_USER,
                                                   SYSDATE,
                                                   SYSDATE);

        IF VL_CANCELTYPE = CSG_TWO THEN
            FOR CUR_CURSOR IN (SELECT tenderId,
                                      tenderAmount
                                 FROM JSON_TABLE(PA_JSON,'$.tenderType[*]'
                                        COLUMNS  (tenderId NUMBER PATH '$.id',
                                                 tenderAmount NUMBER PATH '$.amount')
                                                 ))
                LOOP
                    SELECT COUNT(FI_LOAN_OPERATION_ID) + CSG_ONE
                      INTO VL_TENDER_SEQ
                      FROM SC_CREDIT.TA_LOAN_OPERATION_TENDER
                     WHERE FI_LOAN_ID = VL_LOANID
                       AND FI_LOAN_OPERATION_ID = VL_LOAN_OPERATION_ID
                       AND FI_ADMIN_CENTER_ID = VL_ADMINCENID;

                    INSERT INTO SC_CREDIT.TA_LOAN_OPERATION_TENDER(FI_LOAN_ID,
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

        COMMIT;

        END IF;

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
    END SP_INS_CANCEL;

    PROCEDURE SP_SEL_LOAN_RESCIND (
        PA_LOAN_ID              IN SC_CREDIT.TA_LOAN.FI_LOAN_ID%TYPE,
        PA_ADMIN_CENTER_ID      IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE,
        PA_CUSTOMER_ID          IN SC_CREDIT.TA_LOAN.FC_CUSTOMER_ID%TYPE,
        PA_OPERATION_TYPE       IN VARCHAR2,
        PA_LOAN_CONCEPT         IN VARCHAR2,
        PA_CUR_LOAN_OPERATION  OUT SC_CREDIT.PA_TYPES.TYP_CURSOR ,
        PA_CUR_LOAN            OUT SC_CREDIT.PA_TYPES.TYP_CURSOR,
        PA_CUR_BAL_DET_CONCEPT OUT SC_CREDIT.PA_TYPES.TYP_CURSOR,
        PA_STATUS_CODE         OUT NUMBER,
        PA_STATUS_MSG          OUT VARCHAR2
    ) AS

        VL_COMA                 CONSTANT VARCHAR2(10) := ',';
        VL_STORE                VARCHAR2(30) := 'SP_SEL_LOAN_RESCIND';
        VL_CUR_OPERATIONS       SC_CREDIT.PA_TYPES.TYP_CURSOR;
        VL_CUR_OP_DET_CONCEPT   SC_CREDIT.PA_TYPES.TYP_CURSOR;
        VL_CUR_OPERATION_TENDER SC_CREDIT.PA_TYPES.TYP_CURSOR;
        VL_COUNT SIMPLE_INTEGER:= CSG_ZERO;

    BEGIN
        PA_STATUS_CODE := CSG_ZERO;
        PA_STATUS_MSG := CSG_SUCCES;

            SELECT COUNT(FI_LOAN_ID)
              INTO VL_COUNT
              FROM SC_CREDIT.TA_LOAN
             WHERE FI_LOAN_ID = PA_LOAN_ID
               AND FC_CUSTOMER_ID = NVL(PA_CUSTOMER_ID, FC_CUSTOMER_ID)
               AND FI_ADMIN_CENTER_ID = PA_ADMIN_CENTER_ID;

        IF VL_COUNT =CSG_ZERO THEN

        RAISE  NO_DATA_FOUND;
        ELSE

        OPEN PA_CUR_LOAN_OPERATION FOR
            SELECT DISTINCT
                   B.FI_OPERATION_TYPE_ID,
                   B.FC_OPERATION_TYPE_DESC
              FROM SC_CREDIT.TA_LOAN_OPERATION A
             INNER JOIN SC_CREDIT.TC_OPERATION_TYPE B
                ON A.FI_OPERATION_TYPE_ID = B.FI_OPERATION_TYPE_ID
             INNER JOIN SC_CREDIT.TA_LOAN           C
                ON A.FI_LOAN_ID = C.FI_LOAN_ID
               AND A.FI_ADMIN_CENTER_ID = C.FI_ADMIN_CENTER_ID
             WHERE C.FI_LOAN_ID = NVL(PA_LOAN_ID, C.FI_LOAN_ID)
               AND C.FC_CUSTOMER_ID = NVL(PA_CUSTOMER_ID, C.FC_CUSTOMER_ID)
               AND C.FI_ADMIN_CENTER_ID = PA_ADMIN_CENTER_ID
               AND A.FI_OPERATION_TYPE_ID IN (SELECT TO_NUMBER(COLUMN_VALUE)
                                                FROM TABLE ( SC_CREDIT.PA_LOAN_CANCEL.FN_SPLITST(PA_OPERATION_TYPE, VL_COMA) ))
             ORDER BY B.FI_OPERATION_TYPE_ID ASC;

            SC_CREDIT.PA_LOAN_CANCEL.SP_SEL_LOAN(PA_LOAN_ID,
                                                 PA_ADMIN_CENTER_ID,
                                                 NULL,
                                                 PA_LOAN_CONCEPT,
                                                 PA_CUR_LOAN,
                                                 VL_CUR_OPERATIONS,
                                                 VL_CUR_OP_DET_CONCEPT,
                                                 PA_CUR_BAL_DET_CONCEPT,
                                                 VL_CUR_OPERATION_TENDER,
                                                 PA_STATUS_CODE,
                                                 PA_STATUS_MSG);
    END IF;

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
    END SP_SEL_LOAN_RESCIND;

    PROCEDURE SP_SEL_LOAN (
                       PA_LOAN_ID               IN SC_CREDIT.TA_LOAN.FI_LOAN_ID%TYPE,
                       PA_ADMIN_CENTER_ID       IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE,
                       PA_LOAN_OPERATION_ID     IN SC_CREDIT.TA_LOAN_OPERATION.FI_LOAN_OPERATION_ID%TYPE,
                       PA_LOAN_CONCEPT          IN VARCHAR2,
                       PA_CUR_LOAN              OUT SC_CREDIT.PA_TYPES.TYP_CURSOR,
                       PA_CUR_OPERATIONS        OUT SC_CREDIT.PA_TYPES.TYP_CURSOR,
                       PA_CUR_OP_DET_CONCEPT    OUT SC_CREDIT.PA_TYPES.TYP_CURSOR,
                       PA_CUR_BAL_DET_CONCEPT   OUT SC_CREDIT.PA_TYPES.TYP_CURSOR,
                       PA_CUR_OPERATION_TENDER  OUT SC_CREDIT.PA_TYPES.TYP_CURSOR,
                       PA_STATUS_CODE           OUT NUMBER,
                       PA_STATUS_MSG            OUT VARCHAR2) AS

    VL_STORE        VARCHAR2(30)    := 'SP_SEL_LOAN';

    BEGIN
        PA_STATUS_CODE := CSG_ZERO;
        PA_STATUS_MSG  := CSG_SUCCES;

        OPEN PA_CUR_LOAN FOR
            SELECT DISTINCT
                   L.FI_LOAN_ID,
                   L.FI_ADMIN_CENTER_ID,
                   L.FI_COUNTRY_ID,
                   L.FI_COMPANY_ID,
                   L.FI_BUSINESS_UNIT_ID,
                   L.FC_CUSTOMER_ID,
                   L.FI_PRODUCT_ID,
                   CP.FC_PRODUCT_NAME,
                   L.FN_PRINCIPAL_AMOUNT,
                   L.FN_FINANCE_CHARGE_AMOUNT,
                   L.FN_PRINCIPAL_BALANCE,
                   L.FN_FINANCE_CHARGE_BALANCE,
                   L.FN_ADDITIONAL_CHARGE_BALANCE,
                   L.FD_ORIGINATION_DATE,
                   L.FD_DUE_DATE,
                   L.FN_APR,
                   L.FI_CURRENT_BALANCE_SEQ,
                   L.FN_INTEREST_RATE,
                   L.FI_TERM_TYPE,
                   TT.FC_TERM_TYPE_DESC,
                   L.FI_LOAN_STATUS_ID,
                   LS.FC_LOAN_STATUS_DESC,
                   L.FI_RULE_ID,
                   L.FD_LOAN_EFFECTIVE_DATE
              FROM SC_CREDIT.TA_LOAN L
             INNER JOIN SC_CREDIT.TC_LOAN_STATUS LS
                ON L.FI_LOAN_STATUS_ID = LS.FI_LOAN_STATUS_ID
             INNER JOIN SC_CREDIT.TC_PRODUCT CP
                ON CP.FI_PRODUCT_ID = L.FI_PRODUCT_ID
              INNER JOIN SC_CREDIT.TC_TERM_TYPE  TT
                ON L.FI_TERM_TYPE = TT.FI_TERM_TYPE_ID
             WHERE L.FI_ADMIN_CENTER_ID = PA_ADMIN_CENTER_ID
               AND L.FI_LOAN_ID = PA_LOAN_ID;

        OPEN PA_CUR_OPERATIONS FOR
            SELECT DISTINCT
                   A.FI_LOAN_ID,
                   B.FC_CUSTOMER_ID,
                   B.FI_ADMIN_CENTER_ID AS FI_ADMIN_CENTER_ID_LOAN,
                   A.FI_LOAN_OPERATION_ID,
                   A.FN_OPERATION_AMOUNT,
                   A.FI_OPERATION_TYPE_ID,
                   C.FC_OPERATION_TYPE_DESC,
                   LB.FN_PRINCIPAL_BALANCE,
                   LB.FN_FINANCE_CHARGE_BALANCE,
                   LB.FN_ADDITIONAL_CHARGE_BALANCE,
                   A.FI_COUNTRY_ID,
                   A.FI_COMPANY_ID,
                   A.FI_BUSINESS_UNIT_ID,
                   A.FI_ADMIN_CENTER_ID
              FROM SC_CREDIT.TA_LOAN B
             INNER JOIN SC_CREDIT.TA_LOAN_OPERATION A
                ON A.FI_LOAN_ID = B.FI_LOAN_ID
               AND B.FI_ADMIN_CENTER_ID = A.FI_ADMIN_CENTER_ID
             INNER JOIN SC_CREDIT.TC_OPERATION_TYPE C
                ON A.FI_OPERATION_TYPE_ID = C.FI_OPERATION_TYPE_ID
             INNER JOIN SC_CREDIT.TA_LOAN_BALANCE LB
                ON A.FI_LOAN_ID = LB.FI_LOAN_ID
               AND A.FI_LOAN_OPERATION_ID =LB.FI_LOAN_OPERATION_ID
               AND A.FI_ADMIN_CENTER_ID = LB.FI_ADMIN_CENTER_ID
             WHERE A.FI_LOAN_ID= PA_LOAN_ID
               AND A.FI_LOAN_OPERATION_ID = PA_LOAN_OPERATION_ID
               AND A.FI_ADMIN_CENTER_ID = PA_ADMIN_CENTER_ID
             ORDER BY A.FI_OPERATION_TYPE_ID ASC;

        OPEN PA_CUR_OP_DET_CONCEPT FOR
            SELECT C.FI_LOAN_CONCEPT_ID,
                   LC.FC_LOAN_CONCEPT_DESC,
                   C.FN_ITEM_AMOUNT,
                   LC.FI_BALANCE_CATEGORY_ID
              FROM SC_CREDIT.TA_LOAN_OPERATION A
             INNER JOIN SC_CREDIT.TA_LOAN_OPERATION_DETAIL C
                ON A.FI_LOAN_ID = C.FI_LOAN_ID
                AND A.FI_LOAN_OPERATION_ID = C.FI_LOAN_OPERATION_ID
               AND A.FI_ADMIN_CENTER_ID= C.FI_ADMIN_CENTER_ID
             INNER JOIN SC_CREDIT.TC_LOAN_CONCEPT LC
                ON C.FI_LOAN_CONCEPT_ID = LC.FI_LOAN_CONCEPT_ID
             WHERE A.FI_LOAN_ID= PA_LOAN_ID
               AND A.FI_LOAN_OPERATION_ID = PA_LOAN_OPERATION_ID
               AND A.FI_ADMIN_CENTER_ID = PA_ADMIN_CENTER_ID
               AND LC.FI_BALANCE_CATEGORY_ID IN (SELECT TO_NUMBER(COLUMN_VALUE)
                                                   FROM TABLE (SC_CREDIT.PA_LOAN_CANCEL.FN_SPLITST(PA_LOAN_CONCEPT, CSG_COMMA)))
             ORDER BY C.FI_LOAN_CONCEPT_ID ASC;

        OPEN PA_CUR_BAL_DET_CONCEPT FOR
            SELECT DISTINCT
                   L.FI_LOAN_ID,
                   LBD.FI_LOAN_CONCEPT_ID,
                   LC.FC_LOAN_CONCEPT_DESC,
                   LBD.FN_ITEM_AMOUNT,
                   LC.FI_BALANCE_CATEGORY_ID
              FROM SC_CREDIT.TA_LOAN L
             INNER JOIN SC_CREDIT.TA_LOAN_OPERATION TLO
                ON L.FI_LOAN_ID = TLO.FI_LOAN_ID
               AND L.FI_ADMIN_CENTER_ID = TLO.FI_ADMIN_CENTER_ID
             INNER JOIN SC_CREDIT.TA_LOAN_BALANCE LB
                ON LB.FI_ADMIN_CENTER_ID    = TLO.FI_ADMIN_CENTER_ID
               AND LB.FI_LOAN_OPERATION_ID  = TLO.FI_LOAN_OPERATION_ID
               AND LB.FI_LOAN_ID            = TLO.FI_LOAN_ID
               AND L.FI_CURRENT_BALANCE_SEQ = LB.FI_BALANCE_SEQ
             INNER JOIN SC_CREDIT.TA_LOAN_BALANCE_DETAIL LBD
                ON LB.FI_LOAN_ID            = LBD.FI_LOAN_ID
               AND LB.FI_LOAN_BALANCE_ID    = LBD.FI_LOAN_BALANCE_ID
               AND LB.FI_ADMIN_CENTER_ID    = LBD.FI_ADMIN_CENTER_ID
             INNER JOIN SC_CREDIT.TC_LOAN_CONCEPT LC
                ON LBD.FI_LOAN_CONCEPT_ID = LC.FI_LOAN_CONCEPT_ID
               AND LC.FI_BALANCE_CATEGORY_ID IN (SELECT TO_NUMBER(COLUMN_VALUE)
              FROM TABLE (SC_CREDIT.PA_LOAN_CANCEL.FN_SPLITST(PA_LOAN_CONCEPT,CSG_COMMA)))
             WHERE L.FI_LOAN_ID = PA_LOAN_ID
               AND L.FI_ADMIN_CENTER_ID = PA_ADMIN_CENTER_ID
             ORDER BY LBD.FI_LOAN_CONCEPT_ID ASC;

        OPEN PA_CUR_OPERATION_TENDER FOR
            SELECT LPT.FI_LOAN_OPERATION_ID,
                   LPT.FI_TENDER_TYPE_ID,
                   TT.FC_TENDER_TYPE_DESC,
                   LPT.FN_OPERATION_AMOUNT,
                   LPT.FI_OPERATION_TENDER_SEQ
              FROM SC_CREDIT.TA_LOAN_OPERATION OP
             INNER JOIN SC_CREDIT.TA_LOAN_OPERATION_TENDER LPT
                ON OP.FI_LOAN_ID = LPT.FI_LOAN_ID
               AND OP.FI_LOAN_OPERATION_ID = LPT.FI_LOAN_OPERATION_ID
               AND OP.FI_ADMIN_CENTER_ID = LPT.FI_ADMIN_CENTER_ID
             INNER JOIN SC_CREDIT.TC_TENDER_TYPE TT
                ON LPT.FI_TENDER_TYPE_ID = TT.FI_TENDER_TYPE_ID
             WHERE OP.FI_LOAN_ID = PA_LOAN_ID
               AND OP.FI_LOAN_OPERATION_ID = PA_LOAN_OPERATION_ID
               AND OP.FI_ADMIN_CENTER_ID = PA_ADMIN_CENTER_ID
             ORDER BY OP.FI_LOAN_OPERATION_ID ASC;

    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            ROLLBACK;
            PA_STATUS_CODE := SQLCODE;
            PA_STATUS_MSG := SQLERRM || CSG_SEPARATOR || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
            SC_CREDIT.SP_ERROR_LOG(VL_STORE, PA_STATUS_CODE, PA_STATUS_MSG,
                                    DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, CSG_X);
        WHEN OTHERS THEN
            ROLLBACK;
            PA_STATUS_CODE := SQLCODE;
            PA_STATUS_MSG := SQLERRM || CSG_SEPARATOR || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
            SC_CREDIT.SP_ERROR_LOG(VL_STORE, PA_STATUS_CODE, PA_STATUS_MSG,
                                    DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, CSG_X);
    END SP_SEL_LOAN;


    FUNCTION FN_SPLITST (PA_LIST IN VARCHAR2,
                        PA_DEL  IN VARCHAR2 := ' ') RETURN SC_CREDIT.PA_LOAN_CANCEL.TYSPLIT_TBL
        PIPELINED
    IS

        VL_IDX       SIMPLE_INTEGER := 0;
        VL_LIST      VARCHAR2(32767) := PA_LIST;

    BEGIN
        LOOP
            VL_IDX := INSTR(VL_LIST, PA_DEL);
            IF VL_IDX > CSG_ZERO THEN
                PIPE ROW ( SUBSTR(VL_LIST, CSG_ONE, VL_IDX - CSG_ONE) );
                VL_LIST := SUBSTR(VL_LIST, VL_IDX + LENGTH(PA_DEL));
            ELSE
                PIPE ROW ( VL_LIST );
                EXIT;
            END IF;

        END LOOP;

        RETURN;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN;
        WHEN OTHERS THEN
            RETURN;
    END FN_SPLITST;

END PA_LOAN_CANCEL;

/


GRANT EXECUTE ON SC_CREDIT.PA_LOAN_CANCEL TO USRNCPCREDIT1
/
GRANT EXECUTE ON SC_CREDIT.PA_LOAN_CANCEL TO USRNCPCREDIT1
/
