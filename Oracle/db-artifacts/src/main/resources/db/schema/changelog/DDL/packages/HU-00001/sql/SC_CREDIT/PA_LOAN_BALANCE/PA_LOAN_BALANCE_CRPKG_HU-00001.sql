CREATE OR REPLACE  PACKAGE SC_CREDIT.PA_LOAN_BALANCE AS

/*****************************************************************
  *PROJECT:      NCP
  *DESCRIPTION:  PACKAGE FOR SELECT ABOUT LOANS.
  *CREATOR:      LAURA ELENA SALAZAR AGUIRRE
  *CREATED DATE: NOV-20-2024
  *MODIFICATED:  JAN-16-2024
*****************************************************************/

    CSG_ZERO        CONSTANT SIMPLE_INTEGER := 0;
    CSG_ONE         CONSTANT SIMPLE_INTEGER := 1;
    CSG_COMMA       CONSTANT VARCHAR2(10) := ',';
    CSG_SEPARATOR   CONSTANT VARCHAR2(10) := '->';
    CSG_X           CONSTANT VARCHAR2(2) := 'X';
    CSG_ZEROCHAR    CONSTANT VARCHAR2(2) := '0';
    CSG_SUCCESS     CONSTANT VARCHAR2(15) := 'SUCCESS';

    TYPE TYSPLIT_TBL IS TABLE OF VARCHAR2 (32767);

    -- STORE PROCEDURE SEARCH BY CUSTOMER
    PROCEDURE SP_EXE_LOAN_INFO (PA_LOAN_ID                  IN SC_CREDIT.TA_LOAN.FI_LOAN_ID%TYPE,
                                PA_CUSTOMER_ID              IN SC_CREDIT.TA_LOAN.FC_CUSTOMER_ID%TYPE,
                                PA_ADMIN_CENTER_ID          IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE,
                                PA_LOAN_STATUS              IN VARCHAR2,
                                PA_INITIAL_RECORD           IN NUMBER,
                                PA_FINAL_RECORD             IN NUMBER,
                                PA_LOAN_CONCEPT             IN VARCHAR2,
                                PA_TOTAL_RECORDS            OUT NUMBER,
                                PA_CUR_LOAN                 OUT SC_CREDIT.PA_TYPES.TYP_CURSOR,
                                PA_CUR_BAL_DET_CONCEPT      OUT SC_CREDIT.PA_TYPES.TYP_CURSOR,
                                PA_STATUS_CODE              OUT NUMBER,
                                PA_STATUS_MSG               OUT VARCHAR2);

    -- STORE PROCEDURE SEARCH BY LOAN
    PROCEDURE SP_EXE_LOAN_BALANCE_INFO (PA_LOAN_ID                 IN SC_CREDIT.TA_LOAN.FI_LOAN_ID%TYPE,
                                       PA_CUSTOMER_ID              IN SC_CREDIT.TA_LOAN.FC_CUSTOMER_ID%TYPE,
                                       PA_ADMIN_CENTER_ID          IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE,
                                       PA_LOAN_CONCEPT             IN VARCHAR2,
                                       PA_PAYMENT_STATUS           IN VARCHAR2,
                                       PA_CUR_LOAN                 OUT SC_CREDIT.PA_TYPES.TYP_CURSOR,
                                       PA_CUR_PAY_SCHEDULE         OUT SC_CREDIT.PA_TYPES.TYP_CURSOR,
                                       PA_CUR_BAL_DET_CONCEPT      OUT SC_CREDIT.PA_TYPES.TYP_CURSOR,
                                       PA_STATUS_CODE              OUT NUMBER,
                                       PA_STATUS_MSG               OUT VARCHAR2);

-- PROCEDURE FOR LOAN TRANSACTION HISTORY
    PROCEDURE SP_SEL_OPERAT_HIST (PA_LOAN_ID              IN SC_CREDIT.TA_LOAN.FI_LOAN_ID%TYPE,
                                  PA_ADMIN_CENTER_ID      IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE,
                                  PA_INITIAL_RECORD       IN NUMBER,
                                  PA_FINAL_RECORD         IN NUMBER,
                                  PA_TOTAL_RECORDS        OUT NUMBER,
                                  PA_CUR_OPERATIONS       OUT SC_CREDIT.PA_TYPES.TYP_CURSOR,
                                  PA_CUR_OPERATION_TENDER OUT SC_CREDIT.PA_TYPES.TYP_CURSOR,
                                  PA_STATUS_CODE          OUT NUMBER,
                                  PA_STATUS_MSG           OUT VARCHAR2);

    -- PROCEDURE FOR DETAIL LOAN TRANSACTION HISTORY
    PROCEDURE SP_SEL_OPERAT_DET_HIST (PA_LOAN_ID               IN SC_CREDIT.TA_LOAN.FI_LOAN_ID%TYPE,
                                       PA_ADMIN_CENTER_ID       IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE,
                                       PA_LOAN_OPERATION_ID     IN SC_CREDIT.TA_LOAN_OPERATION.FI_LOAN_OPERATION_ID%TYPE,
                                       PA_LOAN_CONCEPT          IN VARCHAR2,
                                       PA_CUR_OPERATIONS        OUT SC_CREDIT.PA_TYPES.TYP_CURSOR,
                                       PA_CUR_OP_DET_CONCEPT    OUT SC_CREDIT.PA_TYPES.TYP_CURSOR,
                                       PA_CUR_BAL_DET_CONCEPT   OUT SC_CREDIT.PA_TYPES.TYP_CURSOR,
                                       PA_CUR_OPERATION_TENDER OUT SC_CREDIT.PA_TYPES.TYP_CURSOR,
                                       PA_STATUS_CODE           OUT NUMBER,
                                       PA_STATUS_MSG            OUT VARCHAR2);

    -- PROCEDURE FOR DETAIL LOAN TRANSACTION HISTORY
    PROCEDURE SP_SEL_PWO_AMOUNT_DET (PA_LOAN_ID               IN SC_CREDIT.TA_LOAN.FI_LOAN_ID%TYPE,
                                     PA_ADMIN_CENTER_ID       IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE,
                                     PA_CUR_PWO_AMOUNT_DET    OUT SC_CREDIT.PA_TYPES.TYP_CURSOR,
                                     PA_STATUS_CODE           OUT NUMBER,
                                     PA_STATUS_MSG            OUT VARCHAR2);

--FUNCION FOR SELECT IN ARRAY
    FUNCTION FN_SPLITST (PA_LIST IN VARCHAR2,
                        PA_DEL  IN VARCHAR2 := ' ') RETURN SC_CREDIT.PA_LOAN_BALANCE.TYSPLIT_TBL
    PIPELINED;

END PA_LOAN_BALANCE;

/




CREATE OR REPLACE  PACKAGE BODY SC_CREDIT.PA_LOAN_BALANCE AS

     PROCEDURE SP_EXE_LOAN_INFO (PA_LOAN_ID                  IN SC_CREDIT.TA_LOAN.FI_LOAN_ID%TYPE,
                                PA_CUSTOMER_ID              IN SC_CREDIT.TA_LOAN.FC_CUSTOMER_ID%TYPE,
                                PA_ADMIN_CENTER_ID          IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE,
                                PA_LOAN_STATUS              IN VARCHAR2,
                                PA_INITIAL_RECORD           IN NUMBER,
                                PA_FINAL_RECORD             IN NUMBER,
                                PA_LOAN_CONCEPT             IN VARCHAR2,
                                PA_TOTAL_RECORDS            OUT NUMBER,
                                PA_CUR_LOAN                 OUT SC_CREDIT.PA_TYPES.TYP_CURSOR,
                                PA_CUR_BAL_DET_CONCEPT      OUT SC_CREDIT.PA_TYPES.TYP_CURSOR,
                                PA_STATUS_CODE              OUT NUMBER,
                                PA_STATUS_MSG               OUT VARCHAR2
    ) AS

        VL_STORE        VARCHAR2(30) := 'SP_EXE_LOAN_INFO';
        VL_LOANSTATUS   VARCHAR2(3000);
        VL_LISTAGG      VARCHAR2(3000);
        VL_LOAN_LISTAGG VARCHAR2(4000);
    BEGIN
        PA_STATUS_CODE := CSG_ZERO;
        PA_STATUS_MSG := CSG_SUCCESS;

        SELECT LISTAGG(FI_LOAN_STATUS_ID, CSG_COMMA) WITHIN GROUP(ORDER BY FI_LOAN_STATUS_ID)
          INTO  VL_LOANSTATUS
          FROM SC_CREDIT.TC_LOAN_STATUS;

        SELECT (CASE
                WHEN TO_CHAR(PA_LOAN_STATUS) = CSG_ZEROCHAR OR TO_CHAR(PA_LOAN_STATUS) IS NULL THEN
                    VL_LOANSTATUS
                ELSE
                    PA_LOAN_STATUS
                END)
          INTO VL_LISTAGG
          FROM DUAL;

        SELECT DISTINCT
               COUNT(L.FI_LOAN_ID)
          INTO PA_TOTAL_RECORDS
          FROM SC_CREDIT.TA_LOAN      L
          INNER JOIN SC_CREDIT.TC_LOAN_STATUS LS
                ON L.FI_LOAN_STATUS_ID = LS.FI_LOAN_STATUS_ID
             INNER JOIN SC_CREDIT.TC_PRODUCT CP
                ON CP.FI_PRODUCT_ID = L.FI_PRODUCT_ID
          INNER JOIN SC_CREDIT.TC_TERM_TYPE TT
            ON L.FI_TERM_TYPE = TT.FI_TERM_TYPE_ID
         WHERE L.FC_CUSTOMER_ID = NVL(PA_CUSTOMER_ID, L.FC_CUSTOMER_ID)
           AND L.FI_LOAN_ID = NVL(PA_LOAN_ID, L.FI_LOAN_ID)
           AND L.FI_ADMIN_CENTER_ID = NVL(PA_ADMIN_CENTER_ID, L.FI_ADMIN_CENTER_ID)
           AND L.FI_LOAN_STATUS_ID IN (SELECT TO_NUMBER(COLUMN_VALUE)
                                         FROM TABLE ( SC_CREDIT.PA_LOAN_BALANCE.FN_SPLITST(VL_LISTAGG, CSG_COMMA) ))
           AND L.FI_LOAN_ID BETWEEN CSG_ONE AND (SELECT MAX(FI_LOAN_ID)
                                                  FROM SC_CREDIT.TA_LOAN);

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
             WHERE L.FC_CUSTOMER_ID = NVL(PA_CUSTOMER_ID, L.FC_CUSTOMER_ID)
               AND L.FI_LOAN_ID = NVL(PA_LOAN_ID, L.FI_LOAN_ID)
               AND L.FI_ADMIN_CENTER_ID = NVL(PA_ADMIN_CENTER_ID, L.FI_ADMIN_CENTER_ID)
               AND L.FI_LOAN_STATUS_ID IN (SELECT TO_NUMBER(COLUMN_VALUE)
                                             FROM TABLE ( SC_CREDIT.PA_LOAN_BALANCE.FN_SPLITST(VL_LISTAGG, CSG_COMMA) ))
               AND L.FI_LOAN_ID BETWEEN CSG_ONE AND (SELECT MAX(FI_LOAN_ID)
                                                 FROM SC_CREDIT.TA_LOAN)
             ORDER BY L.FI_LOAN_ID ASC
            OFFSET PA_INITIAL_RECORD ROWS FETCH NEXT PA_FINAL_RECORD ROWS ONLY;

            SELECT DISTINCT LISTAGG(L.FI_LOAN_ID, ',') WITHIN GROUP(ORDER BY L.FI_LOAN_ID)
              INTO VL_LOAN_LISTAGG
              FROM SC_CREDIT.TA_LOAN L
             INNER JOIN SC_CREDIT.TC_LOAN_STATUS LS
                ON L.FI_LOAN_STATUS_ID = LS.FI_LOAN_STATUS_ID
             INNER JOIN SC_CREDIT.TC_PRODUCT CP
                ON CP.FI_PRODUCT_ID = L.FI_PRODUCT_ID
              INNER JOIN SC_CREDIT.TC_TERM_TYPE  TT
                ON L.FI_TERM_TYPE = TT.FI_TERM_TYPE_ID
             WHERE L.FC_CUSTOMER_ID = NVL(PA_CUSTOMER_ID, L.FC_CUSTOMER_ID)
               AND L.FI_LOAN_ID = NVL(PA_LOAN_ID, L.FI_LOAN_ID)
               AND L.FI_ADMIN_CENTER_ID = NVL(PA_ADMIN_CENTER_ID, L.FI_ADMIN_CENTER_ID)
               AND L.FI_LOAN_STATUS_ID IN (SELECT TO_NUMBER(COLUMN_VALUE)
                                             FROM TABLE ( SC_CREDIT.PA_LOAN_BALANCE.FN_SPLITST(VL_LISTAGG, CSG_COMMA) ))
               AND L.FI_LOAN_ID BETWEEN 1 AND (SELECT MAX(FI_LOAN_ID)
                                                 FROM SC_CREDIT.TA_LOAN)
            OFFSET PA_INITIAL_RECORD ROWS FETCH NEXT PA_FINAL_RECORD ROWS ONLY;

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
                 ON LB.FI_LOAN_ID = LBD.FI_LOAN_ID
                AND LB.FI_LOAN_BALANCE_ID    = LBD.FI_LOAN_BALANCE_ID
                AND LB.FI_ADMIN_CENTER_ID    = LBD.FI_ADMIN_CENTER_ID
              INNER JOIN SC_CREDIT.TC_LOAN_CONCEPT LC
                 ON LBD.FI_LOAN_CONCEPT_ID = LC.FI_LOAN_CONCEPT_ID
                AND LC.FI_BALANCE_CATEGORY_ID IN (SELECT TO_NUMBER(COLUMN_VALUE)
                                                    FROM TABLE (SC_CREDIT.PA_LOAN_BALANCE.FN_SPLITST(PA_LOAN_CONCEPT,CSG_COMMA)))
              WHERE L.FI_LOAN_ID IN (SELECT TO_NUMBER(COLUMN_VALUE)
                                       FROM TABLE ( SC_CREDIT.PA_LOAN_BALANCE.FN_SPLITST(VL_LOAN_LISTAGG, CSG_COMMA) ))
              ORDER BY LBD.FI_LOAN_CONCEPT_ID ASC;


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

    END SP_EXE_LOAN_INFO;


       PROCEDURE SP_EXE_LOAN_BALANCE_INFO (PA_LOAN_ID                  IN SC_CREDIT.TA_LOAN.FI_LOAN_ID%TYPE,
                                            PA_CUSTOMER_ID              IN SC_CREDIT.TA_LOAN.FC_CUSTOMER_ID%TYPE,
                                            PA_ADMIN_CENTER_ID          IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE,
                                            PA_LOAN_CONCEPT             IN VARCHAR2,
                                            PA_PAYMENT_STATUS           IN VARCHAR2,
                                            PA_CUR_LOAN                 OUT SC_CREDIT.PA_TYPES.TYP_CURSOR,
                                            PA_CUR_PAY_SCHEDULE          OUT SC_CREDIT.PA_TYPES.TYP_CURSOR,
                                            PA_CUR_BAL_DET_CONCEPT      OUT SC_CREDIT.PA_TYPES.TYP_CURSOR,
                                            PA_STATUS_CODE              OUT NUMBER,
                                            PA_STATUS_MSG               OUT VARCHAR2
    ) AS

        VL_STORE     VARCHAR2(30) := 'SP_EXE_LOAN_BALANCE_INFO';
        VL_LOANSTATUS   VARCHAR2(3000);
        VL_LISTAGG   VARCHAR2(3000);
        VL_TOTAL_RECORDS NUMBER(36);
        VL_STATUS_CODE   SIMPLE_INTEGER:=0;
        VL_STATUS_MSG    VARCHAR2(3000);
        VL_PAYMENT_STATUS  VARCHAR2(3000);
        VL_CUR_BAL_DET_CONCEPT SC_CREDIT.PA_TYPES.TYP_CURSOR;

    BEGIN
        PA_STATUS_CODE := CSG_ZERO;
        PA_STATUS_MSG := CSG_SUCCESS;

        -- SEND CURSOR PA_CUR_LOAN OF SP_EXE_LOAN_INFO
        SC_CREDIT.PA_LOAN_BALANCE.SP_EXE_LOAN_INFO (PA_LOAN_ID,
                                                    PA_CUSTOMER_ID,
                                                    PA_ADMIN_CENTER_ID,
                                                    VL_LOANSTATUS,
                                                    CSG_ZERO,
                                                    CSG_ONE,
                                                    PA_LOAN_CONCEPT,
                                                    VL_TOTAL_RECORDS,
                                                    PA_CUR_LOAN,
                                                    VL_CUR_BAL_DET_CONCEPT,
                                                    VL_STATUS_CODE,
                                                    VL_STATUS_MSG);

            SELECT LISTAGG(FI_PMT_SCHEDULE_STATUS_ID, CSG_COMMA) WITHIN GROUP(ORDER BY FI_PMT_SCHEDULE_STATUS_ID)
                INTO  VL_PAYMENT_STATUS
                FROM SC_CREDIT.TC_PAYMENT_SCHEDULE_STATUS;

                 SELECT (CASE
                         WHEN TO_CHAR(PA_PAYMENT_STATUS) = CSG_ZEROCHAR OR TO_CHAR(PA_PAYMENT_STATUS) IS NULL THEN
                            VL_PAYMENT_STATUS
                        ELSE
                        PA_PAYMENT_STATUS
                    END)
                    INTO VL_LISTAGG
                FROM DUAL;

             OPEN PA_CUR_PAY_SCHEDULE FOR
                SELECT FI_PAYMENT_NUMBER_ID,
                    FN_PAYMENT_AMOUNT,
                    FN_PAYMENT_BALANCE,
                    FD_DUE_DATE,
                    FI_PMT_SCHEDULE_STATUS_ID
                FROM SC_CREDIT.TA_PAYMENT_SCHEDULE
                WHERE FI_PMT_SCHEDULE_STATUS_ID IN (SELECT TO_NUMBER(COLUMN_VALUE)
                                             FROM TABLE ( SC_CREDIT.PA_LOAN_BALANCE.FN_SPLITST(VL_LISTAGG, CSG_COMMA) ))
                    AND FI_LOAN_ID= PA_LOAN_ID
                    AND FI_ADMIN_CENTER_ID = PA_ADMIN_CENTER_ID
                    AND FI_STATUS = CSG_ONE
                    ORDER BY FI_PAYMENT_SCHEDULE_ID ASC,
                             FI_PAYMENT_NUMBER_ID ASC;

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
                    ON LB.FI_LOAN_ID = LBD.FI_LOAN_ID
                        AND LB.FI_LOAN_BALANCE_ID    = LBD.FI_LOAN_BALANCE_ID
                        AND LB.FI_ADMIN_CENTER_ID    = LBD.FI_ADMIN_CENTER_ID
                INNER JOIN SC_CREDIT.TC_LOAN_CONCEPT LC
                    ON LBD.FI_LOAN_CONCEPT_ID = LC.FI_LOAN_CONCEPT_ID
                        AND LC.FI_BALANCE_CATEGORY_ID IN (SELECT TO_NUMBER(COLUMN_VALUE)
                    FROM TABLE (SC_CREDIT.PA_LOAN_BALANCE.FN_SPLITST(PA_LOAN_CONCEPT,CSG_COMMA)))
                WHERE L.FI_LOAN_ID = PA_LOAN_ID
                    AND L.FI_ADMIN_CENTER_ID = PA_ADMIN_CENTER_ID
                    ORDER BY LBD.FI_LOAN_CONCEPT_ID ASC;

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

    END SP_EXE_LOAN_BALANCE_INFO;

    PROCEDURE SP_SEL_OPERAT_HIST (PA_LOAN_ID              IN SC_CREDIT.TA_LOAN.FI_LOAN_ID%TYPE,
                                  PA_ADMIN_CENTER_ID      IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE,
                                  PA_INITIAL_RECORD       IN NUMBER,
                                  PA_FINAL_RECORD         IN NUMBER,
                                  PA_TOTAL_RECORDS        OUT NUMBER,
                                  PA_CUR_OPERATIONS       OUT SC_CREDIT.PA_TYPES.TYP_CURSOR,
                                  PA_CUR_OPERATION_TENDER OUT SC_CREDIT.PA_TYPES.TYP_CURSOR,
                                  PA_STATUS_CODE          OUT NUMBER,
                                  PA_STATUS_MSG           OUT VARCHAR2) AS

    VL_STORE        VARCHAR2(30)    := 'SP_SEL_OPERAT_HIST';

    BEGIN
        PA_STATUS_CODE := CSG_ZERO;
        PA_STATUS_MSG := CSG_SUCCESS;

            OPEN PA_CUR_OPERATIONS FOR
                SELECT DISTINCT
                TL.FC_CUSTOMER_ID,
                OP.FI_LOAN_ID,
                TL.FI_ADMIN_CENTER_ID,
                OP.FI_LOAN_OPERATION_ID,
                OP.FN_OPERATION_AMOUNT,
                OP.FD_APPLICATION_DATE,
                OP.FC_PLATFORM_ID,
                OP.FI_OPERATION_TYPE_ID,
                OPT.FC_OPERATION_TYPE_DESC,
                LB.FN_PRINCIPAL_BALANCE,
                LB.FN_FINANCE_CHARGE_BALANCE,
                LB.FN_ADDITIONAL_CHARGE_BALANCE
            FROM SC_CREDIT.TA_LOAN TL
                INNER JOIN SC_CREDIT.TA_LOAN_BALANCE LB
                    ON TL.FI_LOAN_ID = LB.FI_LOAN_ID
                        AND TL.FI_ADMIN_CENTER_ID = LB.FI_ADMIN_CENTER_ID
                INNER JOIN SC_CREDIT.TA_LOAN_OPERATION OP
                    ON OP.FI_LOAN_ID = LB.FI_LOAN_ID
                        AND OP.FI_ADMIN_CENTER_ID = LB.FI_ADMIN_CENTER_ID
                        AND OP.FI_LOAN_OPERATION_ID = LB.FI_LOAN_OPERATION_ID
                INNER JOIN SC_CREDIT.TC_OPERATION_TYPE OPT
                    ON OP.FI_OPERATION_TYPE_ID = OPT.FI_OPERATION_TYPE_ID
            WHERE TL.FI_LOAN_ID = PA_LOAN_ID
            AND TL.FI_ADMIN_CENTER_ID = PA_ADMIN_CENTER_ID
                ORDER BY OP.FI_LOAN_OPERATION_ID ASC
                OFFSET PA_INITIAL_RECORD ROWS FETCH NEXT PA_FINAL_RECORD ROWS ONLY;

            SELECT DISTINCT
               COUNT(TL.FI_LOAN_ID)
          INTO PA_TOTAL_RECORDS
          FROM SC_CREDIT.TA_LOAN TL
                INNER JOIN SC_CREDIT.TA_LOAN_BALANCE LB
                    ON TL.FI_LOAN_ID = LB.FI_LOAN_ID
                        AND TL.FI_ADMIN_CENTER_ID = LB.FI_ADMIN_CENTER_ID
                INNER JOIN SC_CREDIT.TA_LOAN_OPERATION OP
                    ON OP.FI_LOAN_ID = LB.FI_LOAN_ID
                        AND OP.FI_ADMIN_CENTER_ID = LB.FI_ADMIN_CENTER_ID
                        AND OP.FI_LOAN_OPERATION_ID = LB.FI_LOAN_OPERATION_ID
                INNER JOIN SC_CREDIT.TC_OPERATION_TYPE OPT
                    ON OP.FI_OPERATION_TYPE_ID = OPT.FI_OPERATION_TYPE_ID
            WHERE TL.FI_LOAN_ID = PA_LOAN_ID
            AND TL.FI_ADMIN_CENTER_ID = PA_ADMIN_CENTER_ID;

            OPEN PA_CUR_OPERATION_TENDER FOR
             SELECT
                LPT.FI_LOAN_OPERATION_ID,
                LPT.FI_TENDER_TYPE_ID,
                TT.FC_TENDER_TYPE_DESC,
                LPT.FN_OPERATION_AMOUNT
            FROM SC_CREDIT.TA_LOAN_OPERATION OP
            INNER JOIN SC_CREDIT.TA_LOAN_OPERATION_TENDER LPT
                ON OP.FI_LOAN_ID = LPT.FI_LOAN_ID
                AND OP.FI_LOAN_OPERATION_ID = LPT.FI_LOAN_OPERATION_ID
                AND OP.FI_ADMIN_CENTER_ID = LPT.FI_ADMIN_CENTER_ID
            INNER JOIN SC_CREDIT.TC_TENDER_TYPE TT
                ON LPT.FI_TENDER_TYPE_ID = TT.FI_TENDER_TYPE_ID
            WHERE OP.FI_LOAN_ID = PA_LOAN_ID
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
    END SP_SEL_OPERAT_HIST;

    PROCEDURE SP_SEL_OPERAT_DET_HIST (PA_LOAN_ID               IN SC_CREDIT.TA_LOAN.FI_LOAN_ID%TYPE,
                                       PA_ADMIN_CENTER_ID       IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE,
                                       PA_LOAN_OPERATION_ID     IN SC_CREDIT.TA_LOAN_OPERATION.FI_LOAN_OPERATION_ID%TYPE,
                                       PA_LOAN_CONCEPT          IN VARCHAR2,
                                       PA_CUR_OPERATIONS        OUT SC_CREDIT.PA_TYPES.TYP_CURSOR,
                                       PA_CUR_OP_DET_CONCEPT    OUT SC_CREDIT.PA_TYPES.TYP_CURSOR,
                                       PA_CUR_BAL_DET_CONCEPT   OUT SC_CREDIT.PA_TYPES.TYP_CURSOR,
                                       PA_CUR_OPERATION_TENDER OUT SC_CREDIT.PA_TYPES.TYP_CURSOR,
                                       PA_STATUS_CODE           OUT NUMBER,
                                       PA_STATUS_MSG            OUT VARCHAR2) AS

    VL_STORE        VARCHAR2(30)    := 'SP_SEL_OPERAT_DET_HIST';

    BEGIN
        PA_STATUS_CODE := CSG_ZERO;
        PA_STATUS_MSG := CSG_SUCCESS;

               OPEN PA_CUR_OPERATIONS FOR
                SELECT  DISTINCT
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
                    A.FI_ADMIN_CENTER_ID,
                    A.FI_TRANSACTION,
                    A.FC_UUID_TRACKING
                FROM SC_CREDIT.TA_LOAN B
                        INNER JOIN SC_CREDIT.TA_LOAN_OPERATION A
                            ON B.FI_LOAN_ID = A.FI_LOAN_ID
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
                  SELECT
                        A.FI_LOAN_ID,
                        C.FI_LOAN_CONCEPT_ID,
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
                            FROM TABLE (SC_CREDIT.PA_LOAN_BALANCE.FN_SPLITST(PA_LOAN_CONCEPT, CSG_COMMA)))
                    ORDER BY C.FI_LOAN_CONCEPT_ID ASC;

                    OPEN PA_CUR_BAL_DET_CONCEPT FOR
                          SELECT DISTINCT
                            LB.FI_LOAN_ID,
                            LBD.FI_LOAN_CONCEPT_ID,
                            LC.FC_LOAN_CONCEPT_DESC,
                            LBD.FN_ITEM_AMOUNT,
                            LC.FI_BALANCE_CATEGORY_ID
                        FROM SC_CREDIT.TA_LOAN_OPERATION TLO
                        INNER JOIN SC_CREDIT.TA_LOAN_BALANCE LB
                           ON LB.FI_ADMIN_CENTER_ID    = TLO.FI_ADMIN_CENTER_ID
                          AND LB.FI_LOAN_OPERATION_ID  = TLO.FI_LOAN_OPERATION_ID
                          AND LB.FI_LOAN_ID            = TLO.FI_LOAN_ID
                        INNER JOIN SC_CREDIT.TA_LOAN_BALANCE_DETAIL LBD
                           ON LB.FI_LOAN_ID = LBD.FI_LOAN_ID
                          AND LB.FI_LOAN_BALANCE_ID    = LBD.FI_LOAN_BALANCE_ID
                          AND LB.FI_ADMIN_CENTER_ID    = LBD.FI_ADMIN_CENTER_ID
                        INNER JOIN SC_CREDIT.TC_LOAN_CONCEPT LC
                           ON LBD.FI_LOAN_CONCEPT_ID = LC.FI_LOAN_CONCEPT_ID
                        WHERE TLO.FI_LOAN_ID = PA_LOAN_ID
                        AND TLO.FI_LOAN_OPERATION_ID = PA_LOAN_OPERATION_ID
                        AND TLO.FI_ADMIN_CENTER_ID = PA_ADMIN_CENTER_ID
                        AND LC.FI_BALANCE_CATEGORY_ID IN (SELECT TO_NUMBER(COLUMN_VALUE)
                            FROM TABLE (SC_CREDIT.PA_LOAN_BALANCE.FN_SPLITST(PA_LOAN_CONCEPT,CSG_COMMA)))
                        ORDER BY LBD.FI_LOAN_CONCEPT_ID ASC;

                        OPEN PA_CUR_OPERATION_TENDER FOR
                            SELECT LPT.FI_LOAN_OPERATION_ID,
                                LPT.FI_TENDER_TYPE_ID,
                                TT.FC_TENDER_TYPE_DESC,
                                LPT.FN_OPERATION_AMOUNT
                            FROM SC_CREDIT.TA_LOAN_OPERATION OP
                            INNER JOIN SC_CREDIT.TA_LOAN_OPERATION_TENDER LPT
                                ON OP.FI_LOAN_ID = LPT.FI_LOAN_ID
                                AND OP.FI_LOAN_OPERATION_ID = LPT.FI_LOAN_OPERATION_ID
                                AND OP.FI_ADMIN_CENTER_ID = LPT.FI_ADMIN_CENTER_ID
                            INNER JOIN SC_CREDIT.TC_TENDER_TYPE TT
                                ON LPT.FI_TENDER_TYPE_ID = TT.FI_TENDER_TYPE_ID
                            WHERE OP.FI_LOAN_ID = PA_LOAN_ID
                                AND OP.FI_ADMIN_CENTER_ID = PA_ADMIN_CENTER_ID
                                AND OP.FI_LOAN_OPERATION_ID = PA_LOAN_OPERATION_ID
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
    END SP_SEL_OPERAT_DET_HIST;

    PROCEDURE SP_SEL_PWO_AMOUNT_DET (PA_LOAN_ID               IN SC_CREDIT.TA_LOAN.FI_LOAN_ID%TYPE,
                                     PA_ADMIN_CENTER_ID       IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE,
                                     PA_CUR_PWO_AMOUNT_DET    OUT SC_CREDIT.PA_TYPES.TYP_CURSOR,
                                     PA_STATUS_CODE           OUT NUMBER,
                                     PA_STATUS_MSG            OUT VARCHAR2) AS

    VL_STORE        VARCHAR2(30)    := 'SP_SEL_PWO_AMOUNT_DET';

    BEGIN
        PA_STATUS_CODE := CSG_ZERO;
        PA_STATUS_MSG := CSG_SUCCESS;

            OPEN PA_CUR_PWO_AMOUNT_DET FOR
                SELECT FI_LOAN_ID,
                       FI_ADMIN_CENTER_ID,
                       FN_PAY_OFF_AMOUNT,
                       FN_PWO_EXT_PAYMENT,
                       FN_AMOUNT_PAID,
                       FN_PWO_MIN_PAYMENT,
                       FI_ADD_EXTENSION,
                       FD_PWO_DATE
                  FROM SC_CREDIT.TA_PWO_AMOUNT_DETAIL
                 WHERE FI_LOAN_ID = PA_LOAN_ID
                   AND FI_ADMIN_CENTER_ID = PA_ADMIN_CENTER_ID;

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
    END SP_SEL_PWO_AMOUNT_DET;

    FUNCTION FN_SPLITST (PA_LIST IN VARCHAR2,
                        PA_DEL  IN VARCHAR2 := ' ') RETURN SC_CREDIT.PA_LOAN_BALANCE.TYSPLIT_TBL
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

END PA_LOAN_BALANCE;

/


GRANT EXECUTE ON SC_CREDIT.PA_LOAN_BALANCE TO USRNCPCREDIT1
/
GRANT EXECUTE ON SC_CREDIT.PA_LOAN_BALANCE TO USRNCPCREDIT1
/
