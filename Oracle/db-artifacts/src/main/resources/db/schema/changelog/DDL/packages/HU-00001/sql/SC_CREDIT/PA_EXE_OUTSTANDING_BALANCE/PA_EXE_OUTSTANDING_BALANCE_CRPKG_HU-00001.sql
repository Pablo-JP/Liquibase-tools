CREATE OR REPLACE  PACKAGE SC_CREDIT.PA_EXE_OUTSTANDING_BALANCE 
/*************************************************************
 * PROJECT    :  NCP-OUTSTANDING BALANCE
 * DESCRIPTION:  PACKAGE FOR ALL MODULES THAT REQUIRE OUTSTANDING BALANCE.
 * CREATOR:      LUIS FELIPE ROJAS GONZALEZ / CARLOS EDUARDO MARTINEZ CANTERO
 * CREATED DATE: 2024-11-04
 * MODIFICATION: 2025-01-08 CARLOS EDUARDO MARTINEZ CANTERO
 *   V1.1: ADD STORED SC_CREDIT.SP_UPD_PAYMENT_INTEREST
 *   v1.2: ADD NEW LOGIC IN BALANCE DETAILS
 *   v1.3: ADD NEW PRIMARY KEY (FI_LOAN_ID) BALANCE_DETAIL,OPERATION_DETAIL AND LOAN_OPERATION_TENDER.
 *   v1.4: ADD NEW ISO FORMAT DATE.
 *   v1.5: ADD NEW VALIDATION FOR STORED SC_CREDIT.SP_UPD_PAYMENT_INTEREST
*************************************************************/
AS
    -- GLOBAL CONSTANTS
    CSG_0 CONSTANT SIMPLE_INTEGER := 0;
    CSG_X CONSTANT VARCHAR2(3) := 'X';
    CSG_ARROW CONSTANT VARCHAR2(5) := ' -> ';
    CSG_COLON CONSTANT VARCHAR2(5) := ' : ';
    CSG_CURRENT_DATE CONSTANT DATE := SYSDATE;
    CSG_CURRENT_USER CONSTANT VARCHAR2(30) := USER;
    CSG_SUCCESS_CODE CONSTANT SIMPLE_INTEGER := 0;
    CSG_SUCCESS_MSG CONSTANT VARCHAR2(10) := 'SUCCESS';

    PROCEDURE SP_EXE_LOAN_OPERATION(
        PA_DATA_LOAN_OPERATION IN CLOB
    , PA_JS_BALANCE OUT CLOB
    , PA_STATUS_CODE OUT NUMBER
    , PA_STATUS_MSG OUT VARCHAR2);

END PA_EXE_OUTSTANDING_BALANCE;

/




CREATE OR REPLACE  PACKAGE BODY SC_CREDIT.PA_EXE_OUTSTANDING_BALANCE 
/*************************************************************
 * PROJECT    :  NCP-OUTSTANDING BALANCE
 * DESCRIPTION:  PACKAGE FOR ALL MODULES THAT REQUIRE OUTSTANDING BALANCE.
 * CREATOR:      LUIS FELIPE ROJAS GONZALEZ / CARLOS EDUARDO MARTINEZ CANTERO
 * CREATED DATE: 2024-11-04
 * MODIFICATION: 2025-01-08 CARLOS EDUARDO MARTINEZ CANTERO
 *   V1.1: ADD STORED SC_CREDIT.SP_UPD_PAYMENT_INTEREST
 *   v1.2: ADD NEW LOGIC IN BALANCE DETAILS
 *   v1.3: ADD NEW PRIMARY KEY (FI_LOAN_ID) BALANCE_DETAIL,OPERATION_DETAIL AND LOAN_OPERATION_TENDER
 *   v1.4: ADD NEW ISO FORMAT DATE
 *   v1.5: ADD NEW VALIDATION FOR STORED SC_CREDIT.SP_UPD_PAYMENT_INTEREST
*************************************************************/
AS

    PROCEDURE SP_UPD_LOAN_BALANCE(
        PA_LOAN_ID IN SC_CREDIT.TA_LOAN.FI_LOAN_ID%TYPE
    , PA_ADMIN_CENTER_ID IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE
    , PA_PRINCIPAL_BALANCE IN SC_CREDIT.TA_LOAN.FN_PRINCIPAL_BALANCE%TYPE
    , PA_FINANCE_CHARGE_BALANCE IN SC_CREDIT.TA_LOAN.FN_FINANCE_CHARGE_BALANCE%TYPE
    , PA_ADDITIONAL_CHARGE_BALANCE IN SC_CREDIT.TA_LOAN.FN_ADDITIONAL_CHARGE_BALANCE%TYPE
    , PA_FN_PAID_INTEREST_AMOUNT IN SC_CREDIT.TA_LOAN.FN_PAID_INTEREST_AMOUNT%TYPE
    , PA_BAL_SEQ IN SC_CREDIT.TA_LOAN.FI_CURRENT_BALANCE_SEQ%TYPE
    , PA_STATUS_CODE OUT NUMBER
    , PA_STATUS_MSG OUT VARCHAR2);


    PROCEDURE SP_INS_LOAN_OPERATIONS(
        PA_LOAN_OPERATION_ID IN SC_CREDIT.TA_LOAN_OPERATION.FI_LOAN_OPERATION_ID%TYPE
    , PA_ADMIN_CENTER_ID IN SC_CREDIT.TA_LOAN_OPERATION.FI_ADMIN_CENTER_ID%TYPE
    , PA_LOAN_ID IN SC_CREDIT.TA_LOAN_OPERATION.FI_LOAN_ID%TYPE
    , PA_COUNTRY_ID IN SC_CREDIT.TA_LOAN_OPERATION.FI_COUNTRY_ID%TYPE
    , PA_COMPANY_ID IN SC_CREDIT.TA_LOAN_OPERATION.FI_COMPANY_ID%TYPE
    , PA_BUSINESS_UNIT_ID IN SC_CREDIT.TA_LOAN_OPERATION.FI_BUSINESS_UNIT_ID%TYPE
    , PA_OPERATION_TYPE_ID IN SC_CREDIT.TA_LOAN_OPERATION.FI_OPERATION_TYPE_ID%TYPE
    , PA_PLATFORM_ID IN SC_CREDIT.TA_LOAN_OPERATION.FC_PLATFORM_ID%TYPE
    , PA_SUB_PLATFORM_ID IN SC_CREDIT.TA_LOAN_OPERATION.FC_SUB_PLATFORM_ID%TYPE
    , PA_OPERATION_AMOUNT IN SC_CREDIT.TA_LOAN_OPERATION.FN_OPERATION_AMOUNT%TYPE
    , PA_APPLICATION_DATE IN VARCHAR2
    , PA_OPERATION_DATE IN VARCHAR2
    , PA_STATUS IN SC_CREDIT.TA_LOAN_OPERATION.FI_STATUS%TYPE
    , PA_END_USER IN SC_CREDIT.TA_LOAN_OPERATION.FC_END_USER%TYPE
    , PA_UUID_TRACKING IN SC_CREDIT.TA_LOAN_OPERATION.FC_UUID_TRACKING%TYPE
    , PA_GPS_LATITUDE IN SC_CREDIT.TA_LOAN_OPERATION.FC_GPS_LATITUDE%TYPE
    , PA_GPS_LONGITUDE IN SC_CREDIT.TA_LOAN_OPERATION.FC_GPS_LONGITUDE%TYPE
    , PA_IP_ADDRESS IN SC_CREDIT.TA_LOAN_OPERATION.FC_IP_ADDRESS%TYPE
    , PA_DEVICE IN SC_CREDIT.TA_LOAN_OPERATION.FC_DEVICE%TYPE
    , PA_FI_TRANSACTION IN SC_CREDIT.TA_LOAN_OPERATION.FI_TRANSACTION%TYPE
    , PA_STATUS_CODE OUT NUMBER
    , PA_STATUS_MSG OUT VARCHAR2
    );


    PROCEDURE SP_INS_LOAN_BALANCE(
        PA_LOAN_BALANCE_ID IN SC_CREDIT.TA_LOAN_BALANCE.FI_LOAN_BALANCE_ID%TYPE
    , PA_FI_ADMIN_CENTER_ID IN SC_CREDIT.TA_LOAN_BALANCE.FI_ADMIN_CENTER_ID%TYPE
    , PA_LOAN_ID IN SC_CREDIT.TA_LOAN_BALANCE.FI_LOAN_ID%TYPE
    , PA_LOAN_OPERATION_ID IN SC_CREDIT.TA_LOAN_BALANCE.FI_LOAN_OPERATION_ID%TYPE
    , PA_BALANCE_SEQ IN SC_CREDIT.TA_LOAN_BALANCE.FI_BALANCE_SEQ%TYPE
    , PA_PRINCIPAL_BALANCE IN SC_CREDIT.TA_LOAN_BALANCE.FN_PRINCIPAL_BALANCE%TYPE
    , PA_FINANCE_CHARGE_BALANCE IN SC_CREDIT.TA_LOAN_BALANCE.FN_FINANCE_CHARGE_BALANCE%TYPE
    , PA_ADDITIONAL_CHARGE_BALANCE IN SC_CREDIT.TA_LOAN_BALANCE.FN_ADDITIONAL_CHARGE_BALANCE%TYPE
    , PA_STATUS_CODE OUT NUMBER
    , PA_STATUS_MSG OUT VARCHAR2);


    PROCEDURE SP_INS_LOAN_OPERATION_DETAIL(
        PA_LOAN_ID IN SC_CREDIT.TA_LOAN_OPERATION_DETAIL.FI_LOAN_ID%TYPE
    , PA_FI_ADMIN_CENTER_ID IN SC_CREDIT.TA_LOAN_OPERATION_DETAIL.FI_ADMIN_CENTER_ID%TYPE
    , PA_LOAN_OPERATION_ID IN SC_CREDIT.TA_LOAN_OPERATION_DETAIL.FI_LOAN_OPERATION_ID%TYPE
    , PA_LOAN_CONCEPT_ID IN SC_CREDIT.TA_LOAN_OPERATION_DETAIL.FI_LOAN_CONCEPT_ID%TYPE
    , PA_ITEM_AMOUNT IN SC_CREDIT.TA_LOAN_OPERATION_DETAIL.FN_ITEM_AMOUNT%TYPE
    , PA_STATUS_CODE OUT NUMBER
    , PA_STATUS_MSG OUT VARCHAR2
    );


    PROCEDURE SP_INS_LOAN_BALANCE_DETAIL(
        PA_LOAN_ID IN SC_CREDIT.TA_LOAN_BALANCE_DETAIL.FI_LOAN_ID%TYPE
    , PA_FI_ADMIN_CENTER_ID IN SC_CREDIT.TA_LOAN_BALANCE_DETAIL.FI_ADMIN_CENTER_ID%TYPE
    , PA_LOAN_BALANCE_ID IN SC_CREDIT.TA_LOAN_BALANCE_DETAIL.FI_LOAN_BALANCE_ID%TYPE
    , PA_LOAN_CONCEPT_ID IN SC_CREDIT.TA_LOAN_BALANCE_DETAIL.FI_LOAN_CONCEPT_ID%TYPE
    , PA_ITEM_AMOUNT IN SC_CREDIT.TA_LOAN_BALANCE_DETAIL.FN_ITEM_AMOUNT%TYPE
    , PA_STATUS_CODE OUT NUMBER
    , PA_STATUS_MSG OUT VARCHAR2
    );

    PROCEDURE SP_CONS_BAL_SEQ(
        PA_LOAN_ID IN SC_CREDIT.TA_LOAN.FI_LOAN_ID%TYPE
    , PA_ADMIN_CENTER IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE
    , PA_BAL_SEQ OUT SC_CREDIT.TA_LOAN_BALANCE.FI_BALANCE_SEQ%TYPE
    , PA_STATUS_CODE OUT NUMBER
    , PA_STATUS_MSG OUT VARCHAR2
    );


    PROCEDURE SP_SEL_LOAN_DATA(
        PA_LOAN_ID IN SC_CREDIT.TA_LOAN.FI_LOAN_ID%TYPE
    , PA_ADMIN_CENTER IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE
    , PA_FI_COUNTRY_ID OUT SC_CREDIT.TA_LOAN.FI_COUNTRY_ID%TYPE
    , PA_FI_COMPANY_ID OUT SC_CREDIT.TA_LOAN.FI_COMPANY_ID%TYPE
    , PA_FI_BUSINESS_UNIT_ID OUT SC_CREDIT.TA_LOAN.FI_BUSINESS_UNIT_ID%TYPE
    , PA_FC_CUSTOMER_ID OUT SC_CREDIT.TA_LOAN.FC_CUSTOMER_ID%TYPE
    , PA_FI_ORIGINATION_CENTER_ID OUT SC_CREDIT.TA_LOAN.FI_ORIGINATION_CENTER_ID%TYPE
    , PA_FI_PRODUCT_ID OUT SC_CREDIT.TA_LOAN.FI_PRODUCT_ID%TYPE
    , PA_FI_LOAN_STATUS_ID OUT SC_CREDIT.TA_LOAN.FI_LOAN_STATUS_ID%TYPE
    , PA_FI_ADDITIONAL_STATUS OUT SC_CREDIT.TA_LOAN.FI_ADDITIONAL_STATUS%TYPE
    , PA_FD_LOAN_STATUS_DATE OUT SC_CREDIT.TA_LOAN.FD_LOAN_STATUS_DATE%TYPE
    , PA_FC_USER OUT SC_CREDIT.TA_LOAN.FC_USER%TYPE
    , PA_STATUS_CODE OUT NUMBER
    , PA_STATUS_MSG OUT VARCHAR2
    );


    PROCEDURE SP_EXE_LOAN_OPERATION(
        PA_DATA_LOAN_OPERATION IN CLOB
    , PA_JS_BALANCE OUT CLOB
    , PA_STATUS_CODE OUT NUMBER
    , PA_STATUS_MSG OUT VARCHAR2)
    AS

        CSL_ISSUE_LOAN_ID_CODE CONSTANT              SIMPLE_INTEGER   := -20040;
        CSL_ISSUE_LOAN_ID_MSG CONSTANT               VARCHAR2(100)    := 'FAILED TO RETRIEVE INFORMATION OF LOAN_ID IN SP_SEL_LOAN_DATA.';
        CSL_ISSUE_OPERATION_CODE CONSTANT            SIMPLE_INTEGER   := -20050;
        CSL_ISSUE_OPERATION_MSG CONSTANT             VARCHAR2(50)     := 'ISSUE IN SP_INS_LOAN_OPERATION: ';
        CSL_ISSUE_OPERATION_DETAIL_CODE CONSTANT     SIMPLE_INTEGER   := -20060;
        CSL_ISSUE_OPERATION_DETAIL_MSG CONSTANT      VARCHAR2(50)     := 'ISSUE IN SP_INS_LOAN_OPERATION_DETAIL: ';
        CSL_ISSUE_BALANCE_CODE CONSTANT              SIMPLE_INTEGER   := -20070;
        CSL_ISSUE_BALANCE_MSG CONSTANT               VARCHAR2(50)     := 'ISSUE IN SP_INS_LOAN_BALANCE: ';
        CSL_ISSUE_BALANCE_DETAIL_CODE CONSTANT       SIMPLE_INTEGER   := -20080;
        CSL_ISSUE_BALANCE_DETAIL_MSG CONSTANT        VARCHAR2(50)     := 'ISSUE IN SP_INS_LOAN_BALANCE_DETAIL: ';
        CSL_ISSUE_BAL_SEQ_CODE CONSTANT              SIMPLE_INTEGER   := -20090;
        CSL_ISSUE_BAL_SEQ_MSG CONSTANT               VARCHAR2(50)     := 'ISSUE IN SP_CONS_BAL_SEQ: ';
        CSL_ISSUE_LOAN_CODE CONSTANT                 SIMPLE_INTEGER   := -20100;
        CSL_ISSUE_LOAN_MSG CONSTANT                  VARCHAR2(50)     := 'ISSUE IN SP_UPD_LOAN_BALANCE: ';
        CSL_ISSUE_UPD_PAYMENT_INTEREST_MSG CONSTANT  VARCHAR2(50)     := 'ISSUE IN SP_UPD_PAYMENT_INTEREST UPDATE: ';
        CSL_ISSUE_UPD_PAYMENT_INTEREST_CODE CONSTANT SIMPLE_INTEGER   := -20110;
        CSL_UUID_TRACKING CONSTANT                   VARCHAR2(100)    := JSON_VALUE(PA_DATA_LOAN_OPERATION, '$.FC_UUID_TRACKING');
        CSL_GPS_LATITUDE CONSTANT                    DOUBLE PRECISION := JSON_VALUE(PA_DATA_LOAN_OPERATION, '$.FC_GPS_LATITUDE');
        CSL_GPS_LONGITUDE CONSTANT                   DOUBLE PRECISION := JSON_VALUE(PA_DATA_LOAN_OPERATION, '$.FC_GPS_LONGITUDE');
        CSL_IP_ADDRESS CONSTANT                      VARCHAR2(50)     := JSON_VALUE(PA_DATA_LOAN_OPERATION, '$.FC_IP_ADDRESS');
        CSL_DEVICE CONSTANT                          VARCHAR2(100)    := JSON_VALUE(PA_DATA_LOAN_OPERATION, '$.FC_DEVICE');
        CSL_END_USER CONSTANT                        VARCHAR2(100)    := JSON_VALUE(PA_DATA_LOAN_OPERATION, '$.FC_END_USER');
        CSL_FI_STATUS CONSTANT                       SIMPLE_INTEGER   := 1;
        VL_FI_COUNTRY_ID                             SC_CREDIT.TA_LOAN.FI_COUNTRY_ID%TYPE;
        VL_FI_COMPANY_ID                             SC_CREDIT.TA_LOAN.FI_COMPANY_ID%TYPE;
        VL_FI_BUSINESS_UNIT_ID                       SC_CREDIT.TA_LOAN.FI_BUSINESS_UNIT_ID%TYPE;
        VL_FI_ADMIN_CENTER_ID                        SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE;
        VL_FC_CUSTOMER_ID                            SC_CREDIT.TA_LOAN.FC_CUSTOMER_ID %TYPE;
        VL_FI_ORIGINATION_CENTER_ID                  SC_CREDIT.TA_LOAN.FI_ORIGINATION_CENTER_ID%TYPE;
        VL_FI_PRODUCT_ID                             SC_CREDIT.TA_LOAN.FI_PRODUCT_ID %TYPE;
        VL_FI_LOAN_STATUS_ID                         SC_CREDIT.TA_LOAN.FI_LOAN_STATUS_ID %TYPE;
        VL_FI_ADDITIONAL_STATUS                      SC_CREDIT.TA_LOAN.FI_ADDITIONAL_STATUS %TYPE;
        VL_FD_LOAN_STATUS_DATE                       SC_CREDIT.TA_LOAN.FD_LOAN_STATUS_DATE %TYPE;
        VL_FC_USER                                   SC_CREDIT.TA_LOAN.FC_USER%TYPE;
        VL_LOAN_OPERATION_ID                         SC_CREDIT.TA_LOAN_OPERATION.FI_LOAN_OPERATION_ID%TYPE;
        VL_LOAN_BALANCE_ID                           SC_CREDIT.TA_LOAN_BALANCE.FI_LOAN_BALANCE_ID%TYPE;
        VL_COUNT_OPE                                 NUMBER           := 0;
        VL_COUNT_LOAN                                NUMBER           := 0;
        VL_BAL_SEQ                                   SC_CREDIT.TA_LOAN_BALANCE.FI_BALANCE_SEQ%TYPE;
        VL_LOAN_ERR                                  NUMBER           := 0;
        VL_JSON                                      CLOB             := '['; -- ARRAY JSON
        VL_OPERATION                                 CLOB             := '['; -- ARRAY JSON OPERATION
        VL_ERROR                                     CLOB ; -- ARRAY JSON ERROR
        VL_BALANCE                                   CLOB             := '['; -- ARRAY JSON BALANCE
        VL_LOAN                                      CLOB ;
        VL_OPERATIONS_DETAILS                        CLOB             := '[';
        VL_BALANCE_DETAILS                           CLOB             := '[';
        VL_DETAILS_OPERATIONS_REC                    CLOB ;
        VL_DETAILS_BALANCES_REC                      CLOB ;
        --CSL_NO_FIRST
        VL_NO_FIRST                                  BOOLEAN          := FALSE;
        VL_OPERATION_JSON                            CLOB;
        VL_LOAN_ID                                   NUMBER;
        VL_SECOND_OPERATION                          BOOLEAN ;
        VL_COMMIT                                    NUMBER;

    BEGIN


        PA_STATUS_CODE := 0;

        FOR JSON_REC IN (
            SELECT FI_LOAN_ID
                 , FN_PRINCIPAL_BALANCE_LOAN
                 , FN_FINANCE_CHARGE_BALANCE_LOAN
                 , FN_ADDITIONAL_CHARGE_BALANCE_LOAN
                 , FN_PAID_INTEREST_AMOUNT
                 , FI_ADMIN_CENTER_ID

            FROM JSON_TABLE(
                    PA_DATA_LOAN_OPERATION,
                    '$.TA_LOAN[*]' COLUMNS (
                        FI_LOAN_ID NUMBER PATH '$.FI_LOAN_ID',
                        FN_PRINCIPAL_BALANCE_LOAN NUMBER PATH '$.FN_PRINCIPAL_BALANCE',
                        FN_FINANCE_CHARGE_BALANCE_LOAN NUMBER PATH '$.FN_FINANCE_CHARGE_BALANCE',
                        FN_ADDITIONAL_CHARGE_BALANCE_LOAN NUMBER PATH '$.FN_ADDITIONAL_CHARGE_BALANCE',
                        FN_PAID_INTEREST_AMOUNT NUMBER PATH '$.FN_PAID_INTEREST_AMOUNT',
                        FI_ADMIN_CENTER_ID NUMBER PATH '$.FI_ADMIN_CENTER_ID'
                        )))
            LOOP

                VL_OPERATION_JSON := JSON_QUERY(PA_DATA_LOAN_OPERATION,
                                                '$.TA_LOAN[' || VL_COUNT_LOAN ||
                                                '].TA_LOAN_OPERATION');

                VL_LOAN := NULL;
                IF VL_NO_FIRST = TRUE THEN
                    VL_JSON := VL_JSON || ',';

                END IF;


                IF PA_STATUS_CODE = CSG_0 THEN

                    SP_SEL_LOAN_DATA
                    (JSON_REC.FI_LOAN_ID
                        , JSON_REC.FI_ADMIN_CENTER_ID
                        , VL_FI_COUNTRY_ID
                        , VL_FI_COMPANY_ID
                        , VL_FI_BUSINESS_UNIT_ID
                        , VL_FC_CUSTOMER_ID
                        , VL_FI_ORIGINATION_CENTER_ID
                        , VL_FI_PRODUCT_ID
                        , VL_FI_LOAN_STATUS_ID
                        , VL_FI_ADDITIONAL_STATUS
                        , VL_FD_LOAN_STATUS_DATE
                        , VL_FC_USER
                        , PA_STATUS_CODE
                        , PA_STATUS_MSG
                    );

                    IF PA_STATUS_CODE <> 0 THEN
                        ROLLBACK;
                        VL_LOAN_ERR := JSON_REC.FI_LOAN_ID;

                        PA_STATUS_MSG :=
                                CSL_ISSUE_LOAN_ID_MSG || CSL_ISSUE_LOAN_ID_CODE || CSG_ARROW || PA_STATUS_CODE ||
                                CSG_COLON ||
                                PA_STATUS_MSG || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;

                        VL_ERROR := VL_ERROR ||
                                    JSON_OBJECT('FI_LOAN_ID' VALUE JSON_REC.FI_LOAN_ID, 'CODE' VALUE PA_STATUS_CODE,
                                                'DETAILS'
                                                VALUE PA_STATUS_MSG);
                    END IF;

                END IF;


                BEGIN
                    FOR JSON_OP IN (
                        SELECT FI_OPERATION_TYPE_ID
                             , FC_PLATFORM_ID
                             , FC_SUB_PLATFORM_ID
                             , FN_OPERATION_AMOUNT
                             , FD_APPLICATION_DATE
                             , FD_OPERATION_DATE
                             , FN_PRINCIPAL_BALANCE
                             , FN_FINANCE_CHARGE_BALANCE
                             , FN_ADDITIONAL_CHARGE_BALANCE
                             , FI_TRANSACTION
                             , FI_OPERATION_SIGN
                        FROM JSON_TABLE(
                                VL_OPERATION_JSON,
                                '$[*]' COLUMNS (
                                    FI_OPERATION_TYPE_ID NUMBER PATH '$.FI_OPERATION_TYPE_ID',
                                    FC_PLATFORM_ID VARCHAR2 PATH '$.FC_PLATFORM_ID',
                                    FC_SUB_PLATFORM_ID VARCHAR2 PATH '$.FC_SUB_PLATFORM_ID',
                                    FN_OPERATION_AMOUNT NUMBER PATH '$.FN_OPERATION_AMOUNT',
                                    FD_APPLICATION_DATE VARCHAR2 PATH '$.FD_APPLICATION_DATE',
                                    FD_OPERATION_DATE VARCHAR2 PATH '$.FD_OPERATION_DATE',
                                    FN_PRINCIPAL_BALANCE NUMBER PATH '$.FN_PRINCIPAL_BALANCE',
                                    FN_FINANCE_CHARGE_BALANCE NUMBER PATH '$.FN_FINANCE_CHARGE_BALANCE',
                                    FN_ADDITIONAL_CHARGE_BALANCE NUMBER PATH '$.FN_ADDITIONAL_CHARGE_BALANCE',
                                    FI_TRANSACTION NUMBER PATH '$.FI_TRANSACTION',
                                    FI_OPERATION_SIGN NUMBER PATH '$.FI_OPERATION_SIGN'
                                    )))
                        LOOP
                            VL_DETAILS_OPERATIONS_REC := JSON_QUERY(PA_DATA_LOAN_OPERATION,
                                                                    '$.TA_LOAN[' || VL_COUNT_LOAN ||
                                                                    '].TA_LOAN_OPERATION[' ||
                                                                    VL_COUNT_OPE || '].TA_LOAN_OPERATION_DETAIL');
                            VL_DETAILS_BALANCES_REC := JSON_QUERY(PA_DATA_LOAN_OPERATION,
                                                                  '$.TA_LOAN[' || VL_COUNT_LOAN ||
                                                                  '].TA_LOAN_OPERATION[' ||
                                                                  VL_COUNT_OPE || '].TA_LOAN_BALANCE_DETAIL');
                            VL_FI_ADMIN_CENTER_ID := JSON_REC.FI_ADMIN_CENTER_ID;

                            IF VL_COUNT_OPE > 0 THEN
                                VL_OPERATION := VL_OPERATION || ',';
                                VL_BALANCE := VL_BALANCE || ',';
                            END IF;

                            IF PA_STATUS_CODE = CSG_0 THEN

                                SP_CONS_BAL_SEQ(JSON_REC.FI_LOAN_ID, JSON_REC.FI_ADMIN_CENTER_ID, VL_BAL_SEQ,
                                                PA_STATUS_CODE,
                                                PA_STATUS_MSG);

                                IF PA_STATUS_CODE <> 0 THEN
                                    ROLLBACK;
                                    VL_LOAN_ERR := JSON_REC.FI_LOAN_ID;
                                    PA_STATUS_MSG :=
                                            CSL_ISSUE_BAL_SEQ_MSG || CSL_ISSUE_BAL_SEQ_CODE || CSG_ARROW ||
                                            PA_STATUS_CODE ||
                                            CSG_COLON ||
                                            PA_STATUS_MSG || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
                                END IF;
                            END IF;

                            VL_OPERATIONS_DETAILS := '[';
                            VL_BALANCE_DETAILS := '[';
                            VL_LOAN_ID := JSON_REC.FI_LOAN_ID;

                            VL_LOAN_OPERATION_ID := SC_CREDIT.FN_GET_NEXT_LOAN_OPERATION_ID;
                            VL_LOAN_BALANCE_ID := SC_CREDIT.FN_GET_NEXT_LOAN_BALANCE_ID;

                            IF PA_STATUS_CODE = CSG_0 THEN


                                SP_INS_LOAN_OPERATIONS(VL_LOAN_OPERATION_ID
                                    , JSON_REC.FI_ADMIN_CENTER_ID
                                    , JSON_REC.FI_LOAN_ID
                                    , VL_FI_COUNTRY_ID
                                    , VL_FI_COMPANY_ID
                                    , VL_FI_BUSINESS_UNIT_ID
                                    , JSON_OP.FI_OPERATION_TYPE_ID
                                    , JSON_OP.FC_PLATFORM_ID
                                    , JSON_OP.FC_SUB_PLATFORM_ID
                                    , JSON_OP.FN_OPERATION_AMOUNT
                                    , JSON_OP.FD_APPLICATION_DATE
                                    , JSON_OP.FD_OPERATION_DATE
                                    , CSL_FI_STATUS
                                    , CSL_END_USER
                                    , CSL_UUID_TRACKING
                                    , CSL_GPS_LATITUDE
                                    , CSL_GPS_LONGITUDE
                                    , CSL_IP_ADDRESS
                                    , CSL_DEVICE
                                    , JSON_OP.FI_TRANSACTION
                                    , PA_STATUS_CODE
                                    , PA_STATUS_MSG);

                                IF PA_STATUS_CODE <> 0 THEN
                                    ROLLBACK;
                                    VL_LOAN_ERR := JSON_REC.FI_LOAN_ID;

                                    PA_STATUS_MSG :=
                                            CSL_ISSUE_OPERATION_MSG || CSL_ISSUE_OPERATION_CODE || CSG_ARROW ||
                                            PA_STATUS_CODE ||
                                            CSG_COLON || PA_STATUS_MSG || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
                                    VL_ERROR := VL_ERROR ||
                                                JSON_OBJECT('FI_LOAN_ID' VALUE JSON_REC.FI_LOAN_ID, 'CODE' VALUE
                                                            PA_STATUS_CODE,
                                                            'DETAIL' VALUE PA_STATUS_MSG);
                                END IF;


                            END IF;

                            IF PA_STATUS_CODE = CSG_0 THEN

                                SP_INS_LOAN_BALANCE(VL_LOAN_BALANCE_ID
                                    , JSON_REC.FI_ADMIN_CENTER_ID
                                    , JSON_REC.FI_LOAN_ID
                                    , VL_LOAN_OPERATION_ID
                                    , VL_BAL_SEQ
                                    , JSON_OP.FN_PRINCIPAL_BALANCE
                                    , JSON_OP.FN_FINANCE_CHARGE_BALANCE
                                    , JSON_OP.FN_ADDITIONAL_CHARGE_BALANCE
                                    , PA_STATUS_CODE
                                    , PA_STATUS_MSG);

                                IF PA_STATUS_CODE <> 0 THEN
                                    ROLLBACK;
                                    VL_LOAN_ERR := JSON_REC.FI_LOAN_ID;

                                    PA_STATUS_MSG :=
                                            CSL_ISSUE_BALANCE_MSG || CSL_ISSUE_BALANCE_CODE || CSG_ARROW ||
                                            PA_STATUS_CODE ||
                                            CSG_COLON || PA_STATUS_MSG || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
                                    VL_ERROR := VL_ERROR ||
                                                JSON_OBJECT('FI_LOAN_ID' VALUE JSON_REC.FI_LOAN_ID, 'CODE' VALUE
                                                            PA_STATUS_CODE,
                                                            'DETAIL' VALUE PA_STATUS_MSG);

                                END IF;
                            END IF;


                            FOR JSON_REC_DETAIL IN (
                                SELECT FI_LOAN_CONCEPT_ID, FN_ITEM_AMOUNT
                                FROM JSON_TABLE(
                                        VL_DETAILS_OPERATIONS_REC, '$[*]' COLUMNS (
                                            FI_LOAN_CONCEPT_ID NUMBER PATH '$.FI_LOAN_CONCEPT_ID',
                                            FN_ITEM_AMOUNT NUMBER PATH '$.FN_ITEM_AMOUNT'
                                            )
                                     )
                                )

                                LOOP
                                    BEGIN

                                        IF VL_SECOND_OPERATION THEN
                                            VL_OPERATIONS_DETAILS := VL_OPERATIONS_DETAILS || ',';
                                        END IF;


                                        IF PA_STATUS_CODE = CSG_0 THEN


                                            IF JSON_OP.FI_OPERATION_SIGN = 2  and JSON_REC_DETAIL.FI_LOAN_CONCEPT_ID = 2 THEN

                                                VL_COMMIT := 0;

                                                SC_CREDIT.SP_UPD_PAYMENT_INTEREST(
                                                        JSON_REC.FI_LOAN_ID
                                                    , JSON_REC.FI_ADMIN_CENTER_ID
                                                    , JSON_REC_DETAIL.FN_ITEM_AMOUNT
                                                    , VL_COMMIT
                                                    , PA_STATUS_CODE
                                                    , PA_STATUS_MSG);


                                                IF PA_STATUS_CODE <> 0 THEN
                                                    ROLLBACK;

                                                    VL_LOAN_ERR := JSON_REC.FI_LOAN_ID;

                                                    PA_STATUS_MSG :=
                                                            CSL_ISSUE_UPD_PAYMENT_INTEREST_MSG ||
                                                            CSL_ISSUE_UPD_PAYMENT_INTEREST_CODE ||
                                                            CSG_ARROW || PA_STATUS_CODE || CSG_COLON || PA_STATUS_MSG ||
                                                            DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
                                                    VL_ERROR :=
                                                            VL_ERROR ||
                                                            JSON_OBJECT('FI_LOAN_ID' VALUE JSON_REC.FI_LOAN_ID, 'CODE'
                                                                        VALUE
                                                                        PA_STATUS_CODE, 'DETAIL' VALUE PA_STATUS_MSG);
                                                    EXIT;
                                                END IF;
                                            END IF;

                                        END IF;


                                        IF PA_STATUS_CODE = CSG_0 THEN


                                            SP_INS_LOAN_OPERATION_DETAIL(
                                                    JSON_REC.FI_LOAN_ID
                                                , JSON_REC.FI_ADMIN_CENTER_ID
                                                , VL_LOAN_OPERATION_ID
                                                , JSON_REC_DETAIL.FI_LOAN_CONCEPT_ID
                                                , JSON_REC_DETAIL.FN_ITEM_AMOUNT
                                                , PA_STATUS_CODE
                                                , PA_STATUS_MSG);

                                            IF PA_STATUS_CODE <> 0 THEN
                                                ROLLBACK;

                                                VL_LOAN_ERR := JSON_REC.FI_LOAN_ID;

                                                PA_STATUS_MSG :=
                                                        CSL_ISSUE_OPERATION_DETAIL_MSG ||
                                                        CSL_ISSUE_OPERATION_DETAIL_CODE ||
                                                        CSG_ARROW || PA_STATUS_CODE || CSG_COLON || PA_STATUS_MSG ||
                                                        DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
                                                VL_ERROR :=
                                                        VL_ERROR ||
                                                        JSON_OBJECT('FI_LOAN_ID' VALUE JSON_REC.FI_LOAN_ID, 'CODE' VALUE
                                                                    PA_STATUS_CODE, 'DETAIL' VALUE PA_STATUS_MSG);
                                                EXIT;
                                            END IF;
                                        END IF;


                                        VL_OPERATIONS_DETAILS := VL_OPERATIONS_DETAILS || JSON_OBJECT(
                                                'FI_LOAN_CONCEPT_ID' VALUE JSON_REC_DETAIL.FI_LOAN_CONCEPT_ID,
                                                'FN_ITEM_AMOUNT' VALUE JSON_REC_DETAIL.FN_ITEM_AMOUNT);

                                        VL_SECOND_OPERATION := TRUE;

                                    EXCEPTION
                                        WHEN OTHERS THEN
                                            PA_STATUS_CODE := SQLCODE;
                                            PA_STATUS_MSG :=
                                                            SQLERRM || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
                                            ROLLBACK;

                                    END;

                                END LOOP;

                            VL_SECOND_OPERATION := FALSE;


                            FOR JSON_REC_DETAIL IN (
                                SELECT FI_LOAN_CONCEPT_ID, FN_ITEM_AMOUNT
                                FROM JSON_TABLE(
                                        VL_DETAILS_BALANCES_REC, '$[*]' COLUMNS (
                                            FI_LOAN_CONCEPT_ID NUMBER PATH '$.FI_LOAN_CONCEPT_ID',
                                            FN_ITEM_AMOUNT NUMBER PATH '$.FN_ITEM_AMOUNT'
                                            )
                                     )
                                )

                                LOOP
                                    BEGIN

                                        IF VL_SECOND_OPERATION THEN
                                            VL_BALANCE_DETAILS := VL_BALANCE_DETAILS || ',';
                                        END IF;

                                        IF PA_STATUS_CODE = CSG_0 THEN


                                            SP_INS_LOAN_BALANCE_DETAIL(
                                                    JSON_REC.FI_LOAN_ID
                                                , JSON_REC.FI_ADMIN_CENTER_ID
                                                , VL_LOAN_BALANCE_ID
                                                , JSON_REC_DETAIL.FI_LOAN_CONCEPT_ID
                                                , JSON_REC_DETAIL.FN_ITEM_AMOUNT
                                                , PA_STATUS_CODE
                                                , PA_STATUS_MSG);
                                            IF PA_STATUS_CODE <> 0 THEN
                                                ROLLBACK;
                                                VL_LOAN_ERR := JSON_REC.FI_LOAN_ID;

                                                PA_STATUS_MSG :=
                                                        CSL_ISSUE_BALANCE_DETAIL_MSG || CSL_ISSUE_BALANCE_DETAIL_CODE ||
                                                        CSG_ARROW ||
                                                        PA_STATUS_CODE || CSG_COLON || PA_STATUS_MSG ||
                                                        DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;

                                                VL_ERROR :=
                                                        VL_ERROR ||
                                                        JSON_OBJECT('FI_LOAN_ID' VALUE JSON_REC.FI_LOAN_ID, 'CODE' VALUE
                                                                    PA_STATUS_CODE, 'DETAIL' VALUE PA_STATUS_MSG);
                                                EXIT;

                                            END IF;
                                        END IF;

                                        VL_BALANCE_DETAILS := VL_BALANCE_DETAILS || JSON_OBJECT(
                                                'FI_LOAN_CONCEPT_ID' VALUE JSON_REC_DETAIL.FI_LOAN_CONCEPT_ID,
                                                'FN_ITEM_AMOUNT' VALUE JSON_REC_DETAIL.FN_ITEM_AMOUNT);

                                        VL_SECOND_OPERATION := TRUE;

                                    EXCEPTION
                                        WHEN OTHERS THEN
                                            PA_STATUS_CODE := SQLCODE;
                                            PA_STATUS_MSG :=
                                                            SQLERRM || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
                                            ROLLBACK;

                                    END;

                                END LOOP;


                            VL_SECOND_OPERATION := FALSE;
                            VL_OPERATIONS_DETAILS := VL_OPERATIONS_DETAILS || ']';
                            VL_BALANCE_DETAILS := VL_BALANCE_DETAILS || ']';


                            VL_OPERATION := VL_OPERATION || JSON_OBJECT(
                                    'FI_LOAN_OPERATION_ID' VALUE VL_LOAN_OPERATION_ID,
                                    'FI_OPERATION_TYPE_ID' VALUE JSON_OP.FI_OPERATION_TYPE_ID,
                                    'FI_TRANSACTION' VALUE JSON_OP.FI_TRANSACTION,
                                    'FN_OPERATION_AMOUNT' VALUE JSON_OP.FN_OPERATION_AMOUNT,
                                    'STATUS' VALUE CSL_FI_STATUS,
                                    'FC_END_USER' VALUE CSL_END_USER,
                                    'FD_APPLICATION_DATE' VALUE JSON_OP.FD_APPLICATION_DATE,
                                    'DETAILS' VALUE JSON_QUERY(VL_OPERATIONS_DETAILS, '$'));


                            VL_BALANCE := VL_BALANCE || JSON_OBJECT(
                                    'FI_LOAN_BALANCE_ID' VALUE VL_LOAN_BALANCE_ID,
                                    'FI_LOAN_OPERATION_ID' VALUE VL_LOAN_OPERATION_ID,
                                    'FN_PRINCIPAL_BALANCE' VALUE JSON_OP.FN_PRINCIPAL_BALANCE,
                                    'FN_FINANCE_CHARGE_BALANCE' VALUE JSON_OP.FN_FINANCE_CHARGE_BALANCE,
                                    'FN_ADDITIONAL_CHARGE_BALANCE' VALUE JSON_OP.FN_ADDITIONAL_CHARGE_BALANCE,
                                    'LOAN_BAL_SEQ' VALUE VL_BAL_SEQ,
                                    'DETAILS' VALUE JSON_QUERY(VL_BALANCE_DETAILS, '$'));


                            VL_COUNT_OPE := VL_COUNT_OPE + 1;

                        END LOOP;

                EXCEPTION
                    WHEN OTHERS THEN
                        PA_STATUS_CODE := SQLCODE;
                        PA_STATUS_MSG := SQLERRM || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
                        ROLLBACK;

                END;


                VL_BALANCE := VL_BALANCE || ']';
                VL_OPERATION := VL_OPERATION || ']';

                IF PA_STATUS_CODE = CSG_0 THEN

                    SP_UPD_LOAN_BALANCE(
                            JSON_REC.FI_LOAN_ID
                        , JSON_REC.FI_ADMIN_CENTER_ID
                        , JSON_REC.FN_PRINCIPAL_BALANCE_LOAN
                        , JSON_REC.FN_FINANCE_CHARGE_BALANCE_LOAN
                        , JSON_REC.FN_ADDITIONAL_CHARGE_BALANCE_LOAN
                        , JSON_REC.FN_PAID_INTEREST_AMOUNT
                        , VL_BAL_SEQ
                        , PA_STATUS_CODE
                        , PA_STATUS_MSG);

                    IF PA_STATUS_CODE <> 0 THEN
                        ROLLBACK;
                        VL_LOAN_ERR := JSON_REC.FI_LOAN_ID;

                        PA_STATUS_MSG :=
                                CSL_ISSUE_LOAN_MSG || CSL_ISSUE_LOAN_CODE || CSG_ARROW ||
                                PA_STATUS_CODE ||
                                CSG_COLON || PA_STATUS_MSG || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
                        VL_ERROR := VL_ERROR ||
                                    JSON_OBJECT('FI_LOAN_ID' VALUE JSON_REC.FI_LOAN_ID, 'CODE' VALUE
                                                PA_STATUS_CODE,
                                                'DETAIL'
                                                VALUE PA_STATUS_MSG);
                    END IF;
                END IF;

                VL_LOAN := VL_LOAN || JSON_OBJECT(
                        'FC_CUSTOMER_ID' VALUE VL_FC_CUSTOMER_ID,
                        'FI_ORIGINATION_CENTER_ID' VALUE VL_FI_ORIGINATION_CENTER_ID,
                        'FI_COUNTRY_ID' VALUE VL_FI_COUNTRY_ID,
                        'FI_COMPANY_ID' VALUE VL_FI_COMPANY_ID,
                        'FI_BUSINESS_UNIT_ID' VALUE VL_FI_BUSINESS_UNIT_ID,
                        'FI_ADMIN_CENTER_ID' VALUE JSON_REC.FI_ADMIN_CENTER_ID,
                        'FC_USER' VALUE VL_FC_USER,
                        'FI_PRODUCT_ID' VALUE VL_FI_PRODUCT_ID,
                        'FN_PRINCIPAL_BALANCE' VALUE JSON_REC.FN_PRINCIPAL_BALANCE_LOAN,
                        'FN_FINANCE_CHARGE_BALANCE' VALUE JSON_REC.FN_FINANCE_CHARGE_BALANCE_LOAN,
                        'FN_ADDITIONAL_CHARGE_BALANCE' VALUE JSON_REC.FN_ADDITIONAL_CHARGE_BALANCE_LOAN,
                        'FN_PAID_INTEREST_AMOUNT' VALUE JSON_REC.FN_PAID_INTEREST_AMOUNT,
                        'FI_LOAN_STATUS_ID' VALUE VL_FI_LOAN_STATUS_ID,
                        'FI_ADDITIONAL_STATUS' VALUE VL_FI_ADDITIONAL_STATUS,
                        'FD_LOAN_STATUS_DATE' VALUE TO_CHAR(VL_FD_LOAN_STATUS_DATE, 'YYYY-MM-DDTHH24:MI:SSZ'),
                        'OPERATIONS' VALUE JSON_QUERY(VL_OPERATION, '$'),
                        'BALANCES' VALUE JSON_QUERY(VL_BALANCE, '$'));


                VL_NO_FIRST := TRUE;

                VL_COUNT_OPE := 0;
                VL_COUNT_LOAN := VL_COUNT_LOAN + 1;
                PA_STATUS_CODE := 0;
                VL_OPERATION := '[';
                VL_BALANCE := '[';


                IF VL_LOAN_ERR = 0 THEN

                    VL_JSON := VL_JSON || JSON_OBJECT(
                            'FI_LOAN_ID' VALUE JSON_REC.FI_LOAN_ID,
                            'CODE' VALUE 0,
                            'DETAIL' VALUE CSG_SUCCESS_MSG,
                            'CURRENT_BALANCE' VALUE JSON_QUERY(VL_LOAN, '$'));
                    COMMIT;

                ELSE

                    ROLLBACK;
                    VL_JSON := VL_JSON || VL_ERROR;
                    VL_ERROR := '';
                    VL_LOAN_ERR := 0;

                    CONTINUE;

                END IF;


            END LOOP;
        PA_JS_BALANCE := VL_JSON || ']';
        PA_STATUS_MSG := CSG_SUCCESS_MSG;
    EXCEPTION
        WHEN
            OTHERS THEN
            ROLLBACK;
            SC_CREDIT.SP_ERROR_LOG('PA_EXE_OUTSTANDING_BALANCE', SQLCODE, SQLERRM,
                                   DBMS_UTILITY.FORMAT_ERROR_BACKTRACE,
                                   CSG_X, 'PA_EXE_OUTSTANDING_BALANCE');

    END SP_EXE_LOAN_OPERATION;


    PROCEDURE
        SP_UPD_LOAN_BALANCE(
        PA_LOAN_ID IN SC_CREDIT.TA_LOAN.FI_LOAN_ID%TYPE
    , PA_ADMIN_CENTER_ID IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE
    , PA_PRINCIPAL_BALANCE IN SC_CREDIT.TA_LOAN.FN_PRINCIPAL_BALANCE%TYPE
    , PA_FINANCE_CHARGE_BALANCE IN SC_CREDIT.TA_LOAN.FN_FINANCE_CHARGE_BALANCE%TYPE
    , PA_ADDITIONAL_CHARGE_BALANCE IN SC_CREDIT.TA_LOAN.FN_ADDITIONAL_CHARGE_BALANCE%TYPE
    , PA_FN_PAID_INTEREST_AMOUNT IN SC_CREDIT.TA_LOAN.FN_PAID_INTEREST_AMOUNT%TYPE
    , PA_BAL_SEQ IN SC_CREDIT.TA_LOAN.FI_CURRENT_BALANCE_SEQ%TYPE
    , PA_STATUS_CODE OUT NUMBER
    , PA_STATUS_MSG OUT VARCHAR2)
        IS


        CSL_UPDATE_FAILED_CODE CONSTANT SIMPLE_INTEGER := -20304;
        CSL_UPDATE_FAILED_MSG CONSTANT  VARCHAR2(50)   := 'FAILED TO UPDATE';
        CSL_UPDATE_LOAN CONSTANT        VARCHAR2(50)   := 'SP_UPD_LOAN_BALANCE';

-- EXCEPTIONS
        EXC_UPDATE_FAILED EXCEPTION;
        PRAGMA EXCEPTION_INIT (EXC_UPDATE_FAILED, CSL_UPDATE_FAILED_CODE);

    BEGIN

        UPDATE SC_CREDIT.TA_LOAN
        SET FN_PRINCIPAL_BALANCE         = PA_PRINCIPAL_BALANCE
          , FN_FINANCE_CHARGE_BALANCE    = PA_FINANCE_CHARGE_BALANCE
          , FN_ADDITIONAL_CHARGE_BALANCE = PA_ADDITIONAL_CHARGE_BALANCE
          , FI_CURRENT_BALANCE_SEQ       = PA_BAL_SEQ
          , FD_MODIFICATION_DATE         = SYSDATE
          , FN_PAID_INTEREST_AMOUNT      = PA_FN_PAID_INTEREST_AMOUNT
        WHERE FI_LOAN_ID = PA_LOAN_ID
          AND FI_ADMIN_CENTER_ID = PA_ADMIN_CENTER_ID;

        IF SQL%ROWCOUNT = CSG_0 THEN
            RAISE EXC_UPDATE_FAILED;
        END IF;
        PA_STATUS_CODE := CSG_SUCCESS_CODE;
        PA_STATUS_MSG := CSG_SUCCESS_MSG;

    EXCEPTION
        WHEN EXC_UPDATE_FAILED THEN
            ROLLBACK;
            PA_STATUS_CODE := CSL_UPDATE_FAILED_CODE;
            PA_STATUS_MSG := CSL_UPDATE_FAILED_MSG || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;


        WHEN OTHERS THEN
            ROLLBACK;
            PA_STATUS_CODE := SQLCODE;
            PA_STATUS_MSG := SQLERRM || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;


    END SP_UPD_LOAN_BALANCE;

    PROCEDURE
        SP_INS_LOAN_OPERATIONS(
        PA_LOAN_OPERATION_ID IN SC_CREDIT.TA_LOAN_OPERATION.FI_LOAN_OPERATION_ID%TYPE
    , PA_ADMIN_CENTER_ID IN SC_CREDIT.TA_LOAN_OPERATION.FI_ADMIN_CENTER_ID%TYPE
    , PA_LOAN_ID IN SC_CREDIT.TA_LOAN_OPERATION.FI_LOAN_ID%TYPE
    , PA_COUNTRY_ID IN SC_CREDIT.TA_LOAN_OPERATION.FI_COUNTRY_ID%TYPE
    , PA_COMPANY_ID IN SC_CREDIT.TA_LOAN_OPERATION.FI_COMPANY_ID%TYPE
    , PA_BUSINESS_UNIT_ID IN SC_CREDIT.TA_LOAN_OPERATION.FI_BUSINESS_UNIT_ID%TYPE
    , PA_OPERATION_TYPE_ID IN SC_CREDIT.TA_LOAN_OPERATION.FI_OPERATION_TYPE_ID%TYPE
    , PA_PLATFORM_ID IN SC_CREDIT.TA_LOAN_OPERATION.FC_PLATFORM_ID%TYPE
    , PA_SUB_PLATFORM_ID IN SC_CREDIT.TA_LOAN_OPERATION.FC_SUB_PLATFORM_ID%TYPE
    , PA_OPERATION_AMOUNT IN SC_CREDIT.TA_LOAN_OPERATION.FN_OPERATION_AMOUNT%TYPE
    , PA_APPLICATION_DATE IN VARCHAR2 --VALIDAR
    , PA_OPERATION_DATE IN VARCHAR2
    , PA_STATUS IN SC_CREDIT.TA_LOAN_OPERATION.FI_STATUS%TYPE
    , PA_END_USER IN SC_CREDIT.TA_LOAN_OPERATION.FC_END_USER%TYPE
    , PA_UUID_TRACKING IN SC_CREDIT.TA_LOAN_OPERATION.FC_UUID_TRACKING%TYPE
    , PA_GPS_LATITUDE IN SC_CREDIT.TA_LOAN_OPERATION.FC_GPS_LATITUDE%TYPE
    , PA_GPS_LONGITUDE IN SC_CREDIT.TA_LOAN_OPERATION.FC_GPS_LONGITUDE%TYPE
    , PA_IP_ADDRESS IN SC_CREDIT.TA_LOAN_OPERATION.FC_IP_ADDRESS%TYPE
    , PA_DEVICE IN SC_CREDIT.TA_LOAN_OPERATION.FC_DEVICE%TYPE
    , PA_FI_TRANSACTION IN SC_CREDIT.TA_LOAN_OPERATION.FI_TRANSACTION%TYPE
    , PA_STATUS_CODE OUT NUMBER
    , PA_STATUS_MSG OUT VARCHAR2
    )
        IS

        -- CONSTANTS
        CSL_1 CONSTANT                       SIMPLE_INTEGER := 1;
        CSL_DATA_NOT_SAVED_CODE CONSTANT     SIMPLE_INTEGER := -20400;
        CSL_DATA_NOT_SAVED_MSG CONSTANT      VARCHAR2(50)   := 'DATA NOT SAVED';
        CSL_INSERT_OPERATION_DETAIL CONSTANT VARCHAR2(50)   := 'SP_INS_LOAN_OPERATION_DETAIL';

-- EXCEPTIONS
        EXC_DATA_NOT_SAVED EXCEPTION;
        PRAGMA EXCEPTION_INIT (EXC_DATA_NOT_SAVED, CSL_DATA_NOT_SAVED_CODE);

    BEGIN
        INSERT INTO SC_CREDIT.TA_LOAN_OPERATION ( FI_LOAN_OPERATION_ID
                                                , FI_ADMIN_CENTER_ID
                                                , FI_LOAN_ID
                                                , FI_COUNTRY_ID
                                                , FI_COMPANY_ID
                                                , FI_BUSINESS_UNIT_ID
                                                , FI_OPERATION_TYPE_ID
                                                , FC_PLATFORM_ID
                                                , FC_SUB_PLATFORM_ID
                                                , FN_OPERATION_AMOUNT
                                                , FD_APPLICATION_DATE
                                                , FD_OPERATION_DATE
                                                , FI_STATUS
                                                , FC_END_USER
                                                , FC_UUID_TRACKING
                                                , FC_GPS_LATITUDE
                                                , FC_GPS_LONGITUDE
                                                , FC_IP_ADDRESS
                                                , FC_DEVICE
                                                , FI_TRANSACTION)
        VALUES ( PA_LOAN_OPERATION_ID
               , PA_ADMIN_CENTER_ID
               , PA_LOAN_ID
               , PA_COUNTRY_ID
               , PA_COMPANY_ID
               , PA_BUSINESS_UNIT_ID
               , PA_OPERATION_TYPE_ID
               , PA_PLATFORM_ID
               , PA_SUB_PLATFORM_ID
               , PA_OPERATION_AMOUNT
               , CAST(TO_TIMESTAMP_TZ(PA_APPLICATION_DATE, 'YYYY-MM-DDTHH24:MI:SSTZH:TZM') AS DATE)
               , CAST(TO_TIMESTAMP_TZ(PA_OPERATION_DATE, 'YYYY-MM-DDTHH24:MI:SSTZH:TZM') AS DATE)
               , PA_STATUS
               , PA_END_USER
               , PA_UUID_TRACKING
               , PA_GPS_LATITUDE
               , PA_GPS_LONGITUDE
               , PA_IP_ADDRESS
               , PA_DEVICE
               , PA_FI_TRANSACTION);


        PA_STATUS_CODE := CSG_SUCCESS_CODE;


    EXCEPTION

        WHEN OTHERS THEN
            PA_STATUS_CODE := SQLCODE;
            PA_STATUS_MSG := SQLERRM || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
            ROLLBACK;

    END SP_INS_LOAN_OPERATIONS;

    PROCEDURE
        SP_INS_LOAN_BALANCE(
        PA_LOAN_BALANCE_ID IN SC_CREDIT.TA_LOAN_BALANCE.FI_LOAN_BALANCE_ID%TYPE
    , PA_FI_ADMIN_CENTER_ID IN SC_CREDIT.TA_LOAN_BALANCE.FI_ADMIN_CENTER_ID%TYPE
    , PA_LOAN_ID IN SC_CREDIT.TA_LOAN_BALANCE.FI_LOAN_ID%TYPE
    , PA_LOAN_OPERATION_ID IN SC_CREDIT.TA_LOAN_BALANCE.FI_LOAN_OPERATION_ID%TYPE
    , PA_BALANCE_SEQ IN SC_CREDIT.TA_LOAN_BALANCE.FI_BALANCE_SEQ%TYPE
    , PA_PRINCIPAL_BALANCE IN SC_CREDIT.TA_LOAN_BALANCE.FN_PRINCIPAL_BALANCE%TYPE
    , PA_FINANCE_CHARGE_BALANCE IN SC_CREDIT.TA_LOAN_BALANCE.FN_FINANCE_CHARGE_BALANCE%TYPE
    , PA_ADDITIONAL_CHARGE_BALANCE IN SC_CREDIT.TA_LOAN_BALANCE.FN_ADDITIONAL_CHARGE_BALANCE%TYPE
    , PA_STATUS_CODE OUT NUMBER
    , PA_STATUS_MSG OUT VARCHAR2)
        IS


        CSL_DATA_NOT_SAVED_CODE CONSTANT     SIMPLE_INTEGER := -20400;
        CSL_DATA_NOT_SAVED_MSG CONSTANT      VARCHAR2(50)   := 'DATA NOT SAVED';
        CSL_INSERT_OPERATION_DETAIL CONSTANT VARCHAR2(50)   := 'SP_INS_LOAN_BALANCE';

-- EXCEPTIONS
        EXC_DATA_NOT_SAVED EXCEPTION;
        PRAGMA EXCEPTION_INIT (EXC_DATA_NOT_SAVED, CSL_DATA_NOT_SAVED_CODE);

    BEGIN
        INSERT INTO SC_CREDIT.TA_LOAN_BALANCE ( FI_LOAN_BALANCE_ID
                                              , FI_ADMIN_CENTER_ID
                                              , FI_LOAN_ID
                                              , FI_LOAN_OPERATION_ID
                                              , FI_BALANCE_SEQ
                                              , FN_PRINCIPAL_BALANCE
                                              , FN_FINANCE_CHARGE_BALANCE
                                              , FN_ADDITIONAL_CHARGE_BALANCE)
        VALUES ( PA_LOAN_BALANCE_ID
               , PA_FI_ADMIN_CENTER_ID
               , PA_LOAN_ID
               , PA_LOAN_OPERATION_ID
               , PA_BALANCE_SEQ
               , PA_PRINCIPAL_BALANCE
               , PA_FINANCE_CHARGE_BALANCE
               , PA_ADDITIONAL_CHARGE_BALANCE);


        PA_STATUS_CODE := CSG_SUCCESS_CODE;
        PA_STATUS_MSG := CSG_SUCCESS_MSG;

    EXCEPTION
        WHEN OTHERS THEN
            PA_STATUS_CODE := SQLCODE;
            PA_STATUS_MSG := SQLERRM || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
            ROLLBACK;


    END SP_INS_LOAN_BALANCE;

    PROCEDURE
        SP_INS_LOAN_OPERATION_DETAIL(
        PA_LOAN_ID IN SC_CREDIT.TA_LOAN_OPERATION_DETAIL.FI_LOAN_ID%TYPE
    , PA_FI_ADMIN_CENTER_ID IN SC_CREDIT.TA_LOAN_OPERATION_DETAIL.FI_ADMIN_CENTER_ID%TYPE
    , PA_LOAN_OPERATION_ID IN SC_CREDIT.TA_LOAN_OPERATION_DETAIL.FI_LOAN_OPERATION_ID%TYPE
    , PA_LOAN_CONCEPT_ID IN SC_CREDIT.TA_LOAN_OPERATION_DETAIL.FI_LOAN_CONCEPT_ID%TYPE
    , PA_ITEM_AMOUNT IN SC_CREDIT.TA_LOAN_OPERATION_DETAIL.FN_ITEM_AMOUNT%TYPE
    , PA_STATUS_CODE OUT NUMBER
    , PA_STATUS_MSG OUT VARCHAR2)
        IS


        CSL_DATA_NOT_SAVED_CODE CONSTANT     SIMPLE_INTEGER := -20400;
        CSL_DATA_NOT_SAVED_MSG CONSTANT      VARCHAR2(50)   := 'DATA NOT SAVED';
        CSL_INSERT_OPERATION_DETAIL CONSTANT VARCHAR2(50)   := 'SP_INS_LOAN_OPERATION_DETAIL';

-- EXCEPTIONS
        EXC_DATA_NOT_SAVED EXCEPTION;
        PRAGMA EXCEPTION_INIT (EXC_DATA_NOT_SAVED, CSL_DATA_NOT_SAVED_CODE);

    BEGIN
        INSERT INTO SC_CREDIT.TA_LOAN_OPERATION_DETAIL ( FI_LOAN_ID
                                                       , FI_ADMIN_CENTER_ID
                                                       , FI_LOAN_OPERATION_ID
                                                       , FI_LOAN_CONCEPT_ID
                                                       , FN_ITEM_AMOUNT)
        VALUES ( PA_LOAN_ID
               , PA_FI_ADMIN_CENTER_ID
               , PA_LOAN_OPERATION_ID
               , PA_LOAN_CONCEPT_ID
               , PA_ITEM_AMOUNT);


        PA_STATUS_CODE := CSG_SUCCESS_CODE;
        PA_STATUS_MSG := CSG_SUCCESS_MSG;

    EXCEPTION
        WHEN OTHERS THEN
            PA_STATUS_CODE := SQLCODE;
            PA_STATUS_MSG := SQLERRM || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
            ROLLBACK;


    END SP_INS_LOAN_OPERATION_DETAIL;

    PROCEDURE
        SP_INS_LOAN_BALANCE_DETAIL(
        PA_LOAN_ID IN SC_CREDIT.TA_LOAN_BALANCE_DETAIL.FI_LOAN_ID%TYPE
    , PA_FI_ADMIN_CENTER_ID IN SC_CREDIT.TA_LOAN_BALANCE_DETAIL.FI_ADMIN_CENTER_ID%TYPE
    , PA_LOAN_BALANCE_ID IN SC_CREDIT.TA_LOAN_BALANCE_DETAIL.FI_LOAN_BALANCE_ID%TYPE
    , PA_LOAN_CONCEPT_ID IN SC_CREDIT.TA_LOAN_BALANCE_DETAIL.FI_LOAN_CONCEPT_ID%TYPE
    , PA_ITEM_AMOUNT IN SC_CREDIT.TA_LOAN_BALANCE_DETAIL.FN_ITEM_AMOUNT%TYPE
    , PA_STATUS_CODE OUT NUMBER
    , PA_STATUS_MSG OUT VARCHAR2)
        IS


        CSL_DATA_NOT_SAVED_CODE CONSTANT     SIMPLE_INTEGER := -20400;
        CSL_DATA_NOT_SAVED_MSG CONSTANT      VARCHAR2(50)   := 'DATA NOT SAVED';
        CSL_INSERT_OPERATION_DETAIL CONSTANT VARCHAR2(50)   := 'SP_INS_LOAN_BALANCE_DETAIL';
        VL_INSERT_COUNT                      NUMBER         := CSG_0;
-- EXCEPTIONS
        EXC_DATA_NOT_SAVED EXCEPTION;
        PRAGMA EXCEPTION_INIT (EXC_DATA_NOT_SAVED, CSL_DATA_NOT_SAVED_CODE);
        VL_INSERT_COUNT NUMBER := CSG_0;
    BEGIN
        INSERT INTO SC_CREDIT.TA_LOAN_BALANCE_DETAIL ( FI_LOAN_ID
                                                     , FI_ADMIN_CENTER_ID
                                                     , FI_LOAN_BALANCE_ID
                                                     , FI_LOAN_CONCEPT_ID
                                                     , FN_ITEM_AMOUNT)
        VALUES ( PA_LOAN_ID
               , PA_FI_ADMIN_CENTER_ID
               , PA_LOAN_BALANCE_ID
               , PA_LOAN_CONCEPT_ID
               , PA_ITEM_AMOUNT);


        PA_STATUS_CODE := CSG_SUCCESS_CODE;
        PA_STATUS_MSG := CSG_SUCCESS_MSG;

    EXCEPTION
        WHEN OTHERS THEN
            PA_STATUS_CODE := SQLCODE;
            PA_STATUS_MSG := SQLERRM || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
            ROLLBACK;


    END SP_INS_LOAN_BALANCE_DETAIL;

    PROCEDURE
        SP_CONS_BAL_SEQ(
        PA_LOAN_ID IN SC_CREDIT.TA_LOAN.FI_LOAN_ID%TYPE
    , PA_ADMIN_CENTER IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE
    , PA_BAL_SEQ OUT SC_CREDIT.TA_LOAN_BALANCE.FI_BALANCE_SEQ%TYPE
    , PA_STATUS_CODE OUT NUMBER
    , PA_STATUS_MSG OUT VARCHAR2
    )
        IS
    BEGIN

        SELECT MAX(FI_BALANCE_SEQ)
        INTO PA_BAL_SEQ
        FROM SC_CREDIT.TA_LOAN_BALANCE
        WHERE FI_LOAN_ID = PA_LOAN_ID
          AND FI_ADMIN_CENTER_ID = PA_ADMIN_CENTER;
        PA_BAL_SEQ := PA_BAL_SEQ + 1;

        IF PA_BAL_SEQ IS NULL THEN
            PA_BAL_SEQ := 1;
        END IF;

        PA_STATUS_CODE := CSG_SUCCESS_CODE;


    EXCEPTION
        WHEN
            OTHERS THEN
            PA_STATUS_CODE := SQLCODE;
            PA_STATUS_MSG := SQLERRM || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
            ROLLBACK;

    END SP_CONS_BAL_SEQ;

    PROCEDURE
        SP_SEL_LOAN_DATA(
        PA_LOAN_ID IN SC_CREDIT.TA_LOAN.FI_LOAN_ID%TYPE
    , PA_ADMIN_CENTER IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE
    , PA_FI_COUNTRY_ID OUT SC_CREDIT.TA_LOAN.FI_COUNTRY_ID%TYPE
    , PA_FI_COMPANY_ID OUT SC_CREDIT.TA_LOAN.FI_COMPANY_ID%TYPE
    , PA_FI_BUSINESS_UNIT_ID OUT SC_CREDIT.TA_LOAN.FI_BUSINESS_UNIT_ID%TYPE
    , PA_FC_CUSTOMER_ID OUT SC_CREDIT.TA_LOAN.FC_CUSTOMER_ID%TYPE
    , PA_FI_ORIGINATION_CENTER_ID OUT SC_CREDIT.TA_LOAN.FI_ORIGINATION_CENTER_ID%TYPE
    , PA_FI_PRODUCT_ID OUT SC_CREDIT.TA_LOAN.FI_PRODUCT_ID%TYPE
    , PA_FI_LOAN_STATUS_ID OUT SC_CREDIT.TA_LOAN.FI_LOAN_STATUS_ID%TYPE
    , PA_FI_ADDITIONAL_STATUS OUT SC_CREDIT.TA_LOAN.FI_ADDITIONAL_STATUS%TYPE
    , PA_FD_LOAN_STATUS_DATE OUT SC_CREDIT.TA_LOAN.FD_LOAN_STATUS_DATE%TYPE
    , PA_FC_USER OUT SC_CREDIT.TA_LOAN.FC_USER%TYPE
    , PA_STATUS_CODE OUT NUMBER
    , PA_STATUS_MSG OUT VARCHAR2
    ) IS
    BEGIN
        SELECT FI_COUNTRY_ID
             , FI_COMPANY_ID
             , FI_BUSINESS_UNIT_ID
             , FC_CUSTOMER_ID
             , FI_ORIGINATION_CENTER_ID
             , FI_PRODUCT_ID
             , FI_LOAN_STATUS_ID
             , FI_ADDITIONAL_STATUS
             , FD_LOAN_STATUS_DATE
             , FC_USER

        INTO
            PA_FI_COUNTRY_ID
            , PA_FI_COMPANY_ID
            , PA_FI_BUSINESS_UNIT_ID
            , PA_FC_CUSTOMER_ID
            , PA_FI_ORIGINATION_CENTER_ID
            , PA_FI_PRODUCT_ID
            , PA_FI_LOAN_STATUS_ID
            , PA_FI_ADDITIONAL_STATUS
            , PA_FD_LOAN_STATUS_DATE
            ,PA_FC_USER

        FROM SC_CREDIT.TA_LOAN
        WHERE FI_LOAN_ID = PA_LOAN_ID
          AND FI_ADMIN_CENTER_ID = PA_ADMIN_CENTER;

        PA_STATUS_CODE := 0;

    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            PA_STATUS_CODE := SQLCODE;
            PA_STATUS_MSG := SQLERRM || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
            ROLLBACK;

        WHEN OTHERS THEN
            PA_STATUS_CODE := SQLCODE;
            PA_STATUS_MSG := SQLERRM || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
            ROLLBACK;

    END SP_SEL_LOAN_DATA;

END PA_EXE_OUTSTANDING_BALANCE;

/


GRANT EXECUTE ON SC_CREDIT.PA_EXE_OUTSTANDING_BALANCE TO USRNCPCREDIT1
/
GRANT EXECUTE ON SC_CREDIT.PA_EXE_OUTSTANDING_BALANCE TO USRPURPOSEWS
/
GRANT EXECUTE ON SC_CREDIT.PA_EXE_OUTSTANDING_BALANCE TO USRCREDIT02
/
GRANT EXECUTE ON SC_CREDIT.PA_EXE_OUTSTANDING_BALANCE TO USRNCPCREDIT1
/
GRANT EXECUTE ON SC_CREDIT.PA_EXE_OUTSTANDING_BALANCE TO USRPURPOSEWS
/
GRANT EXECUTE ON SC_CREDIT.PA_EXE_OUTSTANDING_BALANCE TO USRCREDIT02
/
