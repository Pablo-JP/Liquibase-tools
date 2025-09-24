

--------------------------------------------------------
--  DDL for Package PA_TYPES
--------------------------------------------------------

  CREATE OR REPLACE  PACKAGE SC_CREDIT.PA_TYPES 
IS
/*************************************************************
  Project    :  TRANSFORMATION
  Description:  PACKAGE WITH GENERIC REF CURSOR
  Creator:      Ricardo Guti??rrez M.
  Created date: MAY-15-23



         this is the version 2
*************************************************************/
   TYPE typ_cursor IS REF CURSOR;
END PA_TYPES;

/

  GRANT EXECUTE ON SC_CREDIT.PA_TYPES TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.PA_TYPES TO USRPRODWS;
--------------------------------------------------------
--  DDL for Package Body PA_TYPES
--------------------------------------------------------
    CREATE OR REPLACE  PACKAGE BODY SC_CREDIT.PA_TYPES 
IS
/*************************************************************
  Project    :  TRANSFORMATION
  Description:  PACKAGE WITH GENERIC REF CURSOR
  Creator:      Ricardo Guti??rrez M.
  Created date: MAY-15-23



           this is the version 2
*************************************************************/

END PA_TYPES;

/

  GRANT EXECUTE ON SC_CREDIT.PA_TYPES TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.PA_TYPES TO USRPRODWS;  
--------------------------------------------------------
--  File created - Monday-January-27-2025   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure SP_ERROR_LOG
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_ERROR_LOG (
    PA_PROCESS       IN VARCHAR2,
    PA_SQL_CODE      IN NUMBER,
    PA_ERROR_MESSAGE IN VARCHAR2,
    PA_BACKTRACE     IN VARCHAR2,
    PA_UUID_TRACKING IN VARCHAR2,
    PA_ADITIONAL_MSG IN VARCHAR2
) AS
/*************************************************************
  PROJECT    :  TRANSFORMATION
  DESCRIPTION:  PACKAGE WITH ERROR LOG
  CREATOR:      RICARDO GUTI??RREZ M.
  CREATED DATE: MAY-15-23


         this is the version 2
*************************************************************/

    CSL_1    PLS_INTEGER := 1;
    CSL_36   PLS_INTEGER := 36;
    CSL_80   PLS_INTEGER := 80;
    CSL_1000 PLS_INTEGER := 1000;
    CSL_600  PLS_INTEGER := 600;
    CSL_500  PLS_INTEGER := 500;


    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
    INSERT INTO SC_CREDIT.TA_ERROR_LOG (
        FI_LOG_ID,
        FD_ERROR,
        FC_PROCESS,
        FI_SQL_CODE,
        FC_SQL_ERRM,
        FC_BACKTRACE,
        FC_UUID_TRACKING,
        FC_ADITIONAL
    ) VALUES (
        SC_CREDIT.SE_ERROR_LOG.NEXTVAL,
        SYSDATE,
        SUBSTR(PA_PROCESS, CSL_1, CSL_80),
        PA_SQL_CODE,
        SUBSTR(PA_ERROR_MESSAGE, CSL_1, CSL_1000),
        SUBSTR(PA_BACKTRACE, CSL_1, CSL_600),
        SUBSTR( NVL(PA_UUID_TRACKING,' '),CSL_1, CSL_36 ),
        SUBSTR( NVL(PA_ADITIONAL_MSG,' ') , CSL_1, CSL_500)
    );

    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
END SP_ERROR_LOG;


/

  GRANT EXECUTE ON SC_CREDIT.SP_ERROR_LOG TO USRNCPCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_BATCH_ERROR_LOG
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_BATCH_ERROR_LOG (
    PA_PROCESS       IN VARCHAR2,
    PA_SQL_CODE      IN NUMBER,
    PA_ERROR_MESSAGE IN VARCHAR2,
    PA_BACKTRACE     IN VARCHAR2,
    PA_TRANSACTION          IN NUMBER,
    PA_ADITIONAL_MSG IN VARCHAR2
) AS

    CSL_1    PLS_INTEGER := 1;
    CSL_80   PLS_INTEGER := 80;
    CSL_36   PLS_INTEGER := 36;
    CSL_1000 PLS_INTEGER := 1000;
    CSL_600  PLS_INTEGER := 600;
    CSL_500  PLS_INTEGER := 500;

/*************************************************************
  PROJECT    :  TRANSFORMATION
  DESCRIPTION:  PACKAGE WITH ERROR LOG FOR BATCH PROCESSING
  CREATOR:      RICARDO GUTI??RREZ M.
  CREATED DATE: 2024-08-01
*************************************************************/

    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN

    INSERT INTO SC_CREDIT.TA_BATCH_ERROR_LOG (
        FI_LOG_ID,
        FD_ERROR,
        FC_PROCESS,
        FI_SQL_CODE,
        FC_SQL_ERRM,
        FC_BACKTRACE,
        FI_TRANSACTION,
        FC_ADITIONAL
    ) VALUES (
        SC_CREDIT.SE_BATCH_ERROR_LOG.NEXTVAL,
        SYSDATE,
        SUBSTR(PA_PROCESS, CSL_1, CSL_80),
        PA_SQL_CODE,
        SUBSTR(PA_ERROR_MESSAGE, CSL_1, CSL_1000),
        SUBSTR(PA_BACKTRACE, CSL_1, CSL_600),
        PA_TRANSACTION,
        SUBSTR(PA_ADITIONAL_MSG, CSL_1, CSL_500)
    ) ;

    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
END SP_BATCH_ERROR_LOG;

/

  GRANT EXECUTE ON SC_CREDIT.SP_BATCH_ERROR_LOG TO USRNCPCREDIT1;
  
  --------------------------------------------------------
--  DDL for Function FN_GET_NEXT_LOAN_ID
--------------------------------------------------------

  CREATE OR REPLACE  FUNCTION SC_CREDIT.FN_GET_NEXT_LOAN_ID RETURN SC_CREDIT.TA_LOAN.FI_LOAN_ID%TYPE IS
/*************************************************************
  PROJECT    :  NCP
  DESCRIPTION:  FUNCTION TO GET THE NEXT LOAN_ID VALUE.
  CREATOR:      RICARDO GM.
  CREATED DATE: AGO-13-2024


         this is the vbersion 2
*************************************************************/
VL_NEXT_LOAN_ID SC_CREDIT.TA_LOAN.FI_LOAN_ID%TYPE;

BEGIN
    VL_NEXT_LOAN_ID := SC_CREDIT.SE_LOAN_ID.NEXTVAL;

    RETURN VL_NEXT_LOAN_ID;

EXCEPTION
    WHEN OTHERS THEN
        VL_NEXT_LOAN_ID := -1;
        SC_CREDIT.SP_ERROR_LOG('FN_GET_NEXT_LOAN_ID', SQLCODE, SQLERRM, DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, '', '');
        RETURN VL_NEXT_LOAN_ID;


END FN_GET_NEXT_LOAN_ID;

/

  GRANT EXECUTE ON SC_CREDIT.FN_GET_NEXT_LOAN_ID TO USRNCPCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_SEL_BALANCE_LOAN
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_SEL_BALANCE_LOAN (
 PA_LOANS IN CLOB
, PA_OUTPUT_BALANCE_LOAN OUT CLOB
, PA_STATUS_CODE OUT NUMBER
, PA_STATUS_MSG OUT VARCHAR2)
    IS
/*************************************************************
  * PROJECT    :  NCP-OUTSTANDING BALANCE
  * DESCRIPTION:  PROCEDURE FOR QUERYNG LOAN BALANCE
  * CREATOR:      CARLOS EDUARDO_MARTINEZ CANTERO
  * CREATED DATE: 2024-11-14
  * MODIFICATION: 2025-01-06 CARLOS EDUARDO_MARTINEZ CANTERO
  * v1.1 ADD BALANCE DETAILS
  * V1.2 REMOVE INNER JOIN AND ADD NEW PRIMARY KEY (FI_LOAN_ID) BALANCE_DETAIL,OPERATION_DETAIL
  * V1.3 ADMIN_CENTER IS ADDED TO DATA OUTPUT.
*************************************************************/

    CSG_SUCCESS_CODE CONSTANT      SIMPLE_INTEGER := 0;
    CSG_SUCCESS_MSG CONSTANT       VARCHAR2(10)   := 'SUCCESS';
    CSG_ARROW CONSTANT             VARCHAR2(5)    := ' -> ';
    CSL_ISSUE_BAL_SEQ_MSG CONSTANT VARCHAR2(50)   := 'ISSUE IN SP_SEL_BALANCE_LOAN: ';
    CSG_X CONSTANT                 VARCHAR2(3)    := 'X';
BEGIN

    WITH TBL_LOANS AS (SELECT FI_LOAN_ID, FI_ADMIN_CENTER_ID
                       FROM JSON_TABLE(PA_LOANS, '$[*]'
                                       COLUMNS (
                                           FI_LOAN_ID NUMBER PATH '$.FI_LOAN_ID',
                                           FI_ADMIN_CENTER_ID NUMBER PATH '$.FI_ADMIN_CENTER_ID'
                                           )
                            ))


    SELECT JSON_ARRAYAGG(
                   JSON_OBJECT(
                           'FI_LOAN_ID' VALUE LOAN.FI_LOAN_ID,
                           'FI_ADMIN_CENTER_ID' VALUE LOAN.FI_ADMIN_CENTER_ID,
                           'FN_PRINCIPAL_BALANCE' VALUE LOAN.FN_PRINCIPAL_BALANCE,
                           'FN_FINANCE_CHARGE_BALANCE' VALUE LOAN.FN_FINANCE_CHARGE_BALANCE,
                           'FN_ADDITIONAL_CHARGE_BALANCE' VALUE LOAN.FN_ADDITIONAL_CHARGE_BALANCE,
                           'FN_PAID_INTEREST_AMOUNT' VALUE LOAN.FN_PAID_INTEREST_AMOUNT,
                           'DETAILS' VALUE (SELECT JSON_ARRAYAGG(
                                                           JSON_OBJECT(
                                                                   'FI_LOAN_CONCEPT_ID' VALUE
                                                                   BALANCE_DETAIL.FI_LOAN_CONCEPT_ID,
                                                                   'FN_ITEM_AMOUNT' VALUE BALANCE_DETAIL.FN_ITEM_AMOUNT
                                                           )
                                                   )
                                            FROM SC_CREDIT.TA_LOAN_BALANCE_DETAIL BALANCE_DETAIL
                                                    WHERE BALANCE_DETAIL.FI_LOAN_ID = LOAN.FI_LOAN_ID
                                              AND BALANCE_DETAIL.FI_ADMIN_CENTER_ID = LOAN.FI_ADMIN_CENTER_ID
                                              AND BALANCE_DETAIL.FI_LOAN_BALANCE_ID = (SELECT MAX(MAX_DETAIL.FI_LOAN_BALANCE_ID)
                                                                              FROM SC_CREDIT.TA_LOAN_BALANCE MAX_DETAIL
                                                                              WHERE MAX_DETAIL.FI_LOAN_ID = LOAN.FI_LOAN_ID
                                                                                AND MAX_DETAIL.FI_ADMIN_CENTER_ID = LOAN.FI_ADMIN_CENTER_ID))
                   )
           )
    INTO PA_OUTPUT_BALANCE_LOAN
    FROM TBL_LOANS LOAN_LIST
             INNER JOIN SC_CREDIT.TA_LOAN LOAN
                        ON LOAN_LIST.FI_LOAN_ID = LOAN.FI_LOAN_ID
                            AND LOAN_LIST.FI_ADMIN_CENTER_ID = LOAN.FI_ADMIN_CENTER_ID;

    PA_STATUS_CODE := CSG_SUCCESS_CODE;
    PA_STATUS_MSG := CSG_SUCCESS_MSG;


EXCEPTION
    WHEN OTHERS THEN
        PA_STATUS_CODE := SQLCODE;
        PA_STATUS_MSG := SQLERRM || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;

        SC_CREDIT.SP_ERROR_LOG(CSL_ISSUE_BAL_SEQ_MSG, SQLCODE, SQLERRM,
                               DBMS_UTILITY.FORMAT_ERROR_BACKTRACE,
                               CSG_X, CSG_ARROW);

END SP_SEL_BALANCE_LOAN;

/

  GRANT EXECUTE ON SC_CREDIT.SP_SEL_BALANCE_LOAN TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_SEL_BALANCE_LOAN TO USRPURPOSEWS;
  GRANT EXECUTE ON SC_CREDIT.SP_SEL_BALANCE_LOAN TO USRCREDIT02;
--------------------------------------------------------
--  DDL for Procedure SP_INS_LOAN_OPERATION_CLEAR
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_INS_LOAN_OPERATION_CLEAR (
    PA_LOAN_ID            IN SC_CREDIT.TA_LOAN_OPERATION_CLEAR.FI_LOAN_ID%TYPE,
    PA_ADMIN_CENTER_ID    IN SC_CREDIT.TA_LOAN_OPERATION_CLEAR.FI_ADMIN_CENTER_ID%TYPE,
    PA_OPERATION_CLEAR_ID IN SC_CREDIT.TA_LOAN_OPERATION_CLEAR.FI_OPERATION_CLEAR_ID%TYPE,
    PA_OPERATION_REF_ID   IN SC_CREDIT.TA_LOAN_OPERATION_CLEAR.FI_OPERATION_REF_ID%TYPE,
    PA_USER               IN SC_CREDIT.TA_LOAN_OPERATION_CLEAR.FC_USER%TYPE,
    PA_IP_ADDRESS         IN SC_CREDIT.TA_LOAN_OPERATION_CLEAR.FC_IP_ADDRESS%TYPE,
    PA_UUID_TRACKING      IN SC_CREDIT.TA_LOAN_OPERATION_CLEAR.FC_UUID_TRACKING%TYPE,
    PA_STATUS_CODE        OUT NUMBER,
    PA_STATUS_MSG         OUT VARCHAR2
)
IS
 /* *************************************************************************
 * PROJECT: CORE LOAN
 * DESCRIPTION: PROCEDURE FOR SAVING INFORMATION REGARDING AN OPERATION
 *              WHICH IS CLEARED.
 * PRECONDITIONS: PRE-EXISTING LOANS AND OPERATIONS
 * CREATED DATE: 07/01/2025
 * CREATOR: GILBERTO CHAVEZ MUNOZ
 ****************************************************************************/
  CSL_0            CONSTANT SIMPLE_INTEGER := 0;
  CSL_1            CONSTANT SIMPLE_INTEGER := 1;
  CSL_204          CONSTANT SIMPLE_INTEGER := 204;
  CSL_ARROW        CONSTANT VARCHAR2(5)  := '->';
  CSL_JSON         CONSTANT VARCHAR2(5)  := NULL;
  CSL_SUCCESS      CONSTANT VARCHAR2(8)  := 'SUCCESS';
  CSL_NO_INSERTION CONSTANT VARCHAR2(20) := 'NO DATA INSERTED';
  CSL_SP           CONSTANT SIMPLE_INTEGER := 1;
  VG_LOAN_OPERATION_VOID_ID SC_CREDIT.TA_LOAN_OPERATION_VOID.FI_LOAN_OPERATION_VOID_ID%TYPE;

BEGIN
INSERT INTO SC_CREDIT.TA_LOAN_OPERATION_CLEAR(
    FI_LOAN_OPERATION_CLEAR_ID,
    FI_LOAN_ID,
    FI_ADMIN_CENTER_ID,
    FI_OPERATION_CLEAR_ID,
    FI_OPERATION_REF_ID,
    FC_USER,
    FC_IP_ADDRESS,
    FD_CREATED_DATE,
    FD_MODIFICATION_DATE,
    FC_UUID_TRACKING )
VALUES(SE_LOAN_OPERATION_CLEAR_ID.NEXTVAL,
       PA_LOAN_ID,
       PA_ADMIN_CENTER_ID,
       PA_OPERATION_CLEAR_ID,
       PA_OPERATION_REF_ID,
       PA_USER,
       PA_IP_ADDRESS,
       SYSDATE,
       SYSDATE,
       PA_UUID_TRACKING )
;
COMMIT;
PA_STATUS_CODE := CSL_0;
  PA_STATUS_MSG  := CSL_SUCCESS;

EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    PA_STATUS_CODE := CSL_204;
    PA_STATUS_MSG  := CSL_NO_INSERTION;
    PA_STATUS_CODE := SQLCODE;
    PA_STATUS_MSG  := SQLERRM || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    SC_CREDIT.SP_ERROR_LOG(
       UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP)
       ,SQLCODE
       ,SQLERRM
       ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
       ,NULL
       ,CSL_JSON
       );
END SP_INS_LOAN_OPERATION_CLEAR;

/

  GRANT EXECUTE ON SC_CREDIT.SP_INS_LOAN_OPERATION_CLEAR TO USRNCPCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_SYNC_BUSINESS_UNIT
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_SYNC_BUSINESS_UNIT (
    PA_SYNC_JSON   			  CLOB
    ,PA_UPDATED_ROWS	OUT NUMBER
    ,PA_STATUS_CODE		OUT NUMBER
    ,PA_STATUS_MSG  	OUT VARCHAR2)
  AS
/* **************************************************************
* PROJECT: NCP
* DESCRIPTION: CATALOG SYNCHONIZATION TC_BUSINESS_UNIT
* CREATED DATE: 2025/01/09
* CREATOR: CESAR CORTES
* MODIFICATION DATE: 2025/01/09
************************************************************** */
  CSL_0 CONSTANT SIMPLE_INTEGER := 0;
BEGIN
  PA_STATUS_CODE := CSL_0;
  PA_STATUS_MSG := 'OK';
  PA_UPDATED_ROWS  := CSL_0;

  MERGE INTO SC_CREDIT.TC_BUSINESS_UNIT A
    USING (
    SELECT
      *
    FROM
      JSON_TABLE ( PA_SYNC_JSON, '$.businessUnit[*]'
        COLUMNS (
          ID                    NUMBER PATH '$.id'
          ,COMPANY              NUMBER PATH '$.company'
          ,COUNTRY        		NUMBER PATH '$.country'
          ,DESCRIPTION          VARCHAR2 ( 50 ) PATH '$.description'
          ,STATUS               NUMBER PATH '$.status'
          ,USER_NAME            VARCHAR2 ( 50 ) PATH '$.user'
          ,CREATED_DATE         TIMESTAMP PATH '$.createdDate'
          ,MODIFICATION_DATE    TIMESTAMP PATH '$.modificationDate')))
          B ON ( A.FI_BUSINESS_UNIT_ID = B.ID AND A.FI_COMPANY_ID = B.COMPANY AND A.FI_COUNTRY_ID = B.COUNTRY )
  WHEN MATCHED THEN UPDATE
    SET
      A.FC_BUSINESS_DESC = B.DESCRIPTION
      ,A.FI_STATUS = B.STATUS
      ,A.FC_USER = B.USER_NAME
      ,A.FD_CREATED_DATE = B.CREATED_DATE
      ,A.FD_MODIFICATION_DATE = CAST(B.MODIFICATION_DATE AS DATE)
  WHEN NOT MATCHED THEN
    INSERT (
      FI_COUNTRY_ID
      ,FI_COMPANY_ID
      ,FI_BUSINESS_UNIT_ID
      ,FC_BUSINESS_DESC
      ,FI_STATUS
      ,FC_USER
      ,FD_CREATED_DATE
      ,FD_MODIFICATION_DATE)
    VALUES (
      B.COUNTRY
      ,B.COMPANY
      ,B.ID
      ,B.DESCRIPTION
      ,B.STATUS
      ,B.USER_NAME
      ,B.CREATED_DATE
      ,B.MODIFICATION_DATE);

      PA_UPDATED_ROWS := SQL%ROWCOUNT;

  COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      PA_STATUS_CODE := SQLCODE;
      PA_STATUS_MSG  := SQLERRM;
      SC_CREDIT.SP_ERROR_LOG('SP_SYNC_BUSINESS_UNIT', SQLCODE, SQLERRM,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, NULL, '');

  END SP_SYNC_BUSINESS_UNIT;

/

  GRANT EXECUTE ON SC_CREDIT.SP_SYNC_BUSINESS_UNIT TO USRPURPOSEWS;
  GRANT EXECUTE ON SC_CREDIT.SP_SYNC_BUSINESS_UNIT TO USRNCPCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_BTC_UPD_LOAN_STATUS_DETAIL
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_BTC_UPD_LOAN_STATUS_DETAIL 
   (PA_LOAN_ID                    IN NUMBER
   ,PA_ADMIN_CENTER_ID            IN NUMBER
   ,PA_PAYMENT_NUMBER_ID          IN NUMBER
   ,PA_COUNTER_DAY                IN NUMBER
   ,PA_ON_OFF                     IN NUMBER
   ,PA_TRANSACTION                IN NUMBER
   ,PA_BAN_OPERATION              IN NUMBER
   ,PA_COMMIT                     IN NUMBER
   ,PA_STATUS_CODE                OUT NUMBER
   ,PA_STATUS_MSG                 OUT VARCHAR2
   )
   IS
     /* **************************************************************
   * DESCRIPTION: PROCESS TO INSERT IN TABLE INTEREST
   * CREATED DATE: 08/11/2024
   * CREATOR: CRISTHIAN MORALES
   ************************************************************** */

  CSL_0                         CONSTANT SIMPLE_INTEGER := 0;
  CSL_1                         CONSTANT SIMPLE_INTEGER := 1;
  CSL_SP                        CONSTANT SIMPLE_INTEGER := 1;
  CSL_2002                      CONSTANT SIMPLE_INTEGER := -20002;
  CSL_ARROW                     CONSTANT VARCHAR2(20)   := '->';
  CSL_MSG_SUCCESS               CONSTANT VARCHAR2(20)   := 'SUCCESS';
  CSL_NOT_UPDATED               CONSTANT VARCHAR2(20)   := ' NOT_UPDATE ';
  CSL_LOAN_ID                   CONSTANT VARCHAR2(20)   := 'LOAN_ID';
  CSL_TA_LOAN_STATUS_DETAIL     CONSTANT VARCHAR2(20)   := ' LOAN_STATUS_DETAIL ';
  CSL_ADMIN_CENTER_ID           CONSTANT VARCHAR2(20)   := 'ADMIN_CENTER_ID';
  CSL_NULL                      CONSTANT VARCHAR2(20)   := ' PARAMETERS NULL ';
  CSL_COMA                      CONSTANT VARCHAR2(20)   := ',';
  CSL_DAY                       DATE:= SYSDATE;


  BEGIN

     PA_STATUS_CODE :=CSL_0;
     PA_STATUS_MSG  :=CSL_MSG_SUCCESS;

     IF PA_BAN_OPERATION=CSL_1 THEN
         UPDATE SC_CREDIT.TA_LOAN_STATUS_DETAIL LD
              SET LD.FI_COUNTER_DAY=NVL(PA_COUNTER_DAY,LD.FI_COUNTER_DAY)
                 ,LD.FI_ON_OFF=NVL(PA_ON_OFF,LD.FI_ON_OFF)
                 ,LD.FD_MODIFICATION_DATE=CSL_DAY
            WHERE LD.FI_LOAN_ID=PA_LOAN_ID
            AND   LD.FI_ADMIN_CENTER_ID=PA_ADMIN_CENTER_ID
            AND   LD.FI_PAYMENT_NUMBER_ID=PA_PAYMENT_NUMBER_ID
            AND   LD.FI_ON_OFF=CSL_1;
     ELSIF PA_BAN_OPERATION=CSL_0 THEN
         UPDATE SC_CREDIT.TA_LOAN_STATUS_DETAIL LD
              SET LD.FI_COUNTER_DAY=NVL(PA_COUNTER_DAY,FI_COUNTER_DAY)
                 ,LD.FI_ON_OFF=NVL(PA_ON_OFF,FI_ON_OFF)
                 ,LD.FD_MODIFICATION_DATE=CSL_DAY
            WHERE LD.FI_LOAN_ID=PA_LOAN_ID
            AND   LD.FI_ADMIN_CENTER_ID=PA_ADMIN_CENTER_ID
            AND   LD.FI_ON_OFF=CSL_1;
      END IF;

        IF SQL%ROWCOUNT = CSL_0 THEN
         RAISE_APPLICATION_ERROR(CSL_2002, CSL_NOT_UPDATED || CSL_TA_LOAN_STATUS_DETAIL);
        END IF;

         IF PA_COMMIT = CSL_1 THEN
         COMMIT;
         END IF;

        EXCEPTION
        WHEN OTHERS THEN
        ROLLBACK;
        PA_STATUS_CODE := SQLCODE;
        PA_STATUS_MSG := SQLERRM || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;

        SC_CREDIT.SP_BATCH_ERROR_LOG(UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP)
                                    ,SQLCODE
                                    ,SQLERRM
                                    ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                    ,PA_TRANSACTION
                                    ,NULL
                                    ||CSL_ADMIN_CENTER_ID ||PA_ADMIN_CENTER_ID);
  END SP_BTC_UPD_LOAN_STATUS_DETAIL;

/

  GRANT EXECUTE ON SC_CREDIT.SP_BTC_UPD_LOAN_STATUS_DETAIL TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_BTC_UPD_LOAN_STATUS_DETAIL TO USRBTCCREDIT1;
 -------------------------------------------------------
--  DDL for Procedure SP_SEL_CONCEPT_SIGN
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_SEL_CONCEPT_SIGN (
    PA_CUR_RESULT               OUT SC_CREDIT.PA_TYPES.TYP_CURSOR,
    PA_STATUS_CODE              OUT NUMBER,
    PA_STATUS_MSG               OUT VARCHAR2
    )
    IS
/*****************************************************************
  *PROJECT:      NCP
  *DESCRIPTION:  GET CATALOGS OF OPERATION TYPE AND SIGN OF THE OPERATION
  *CREATOR:      CARLOS EDUARDO_MARTINEZ CANTERO
  *CREATED DATE: NOV-22-2024
  *MODIFICATION: 2024-12-05 CARLOS EDUARDO MARTINEZ CANTERO_
*****************************************************************/
    CSL_ISSUE_CONCEPT_CODE CONSTANT              SIMPLE_INTEGER   := -20010;
    CSL_ISSUE_CONCEPT_MSG CONSTANT               VARCHAR2(100)    := 'FAILED TO IN CONCEPT_SIGN';
    CSG_SUCCESS_CODE CONSTANT SIMPLE_INTEGER := 0;
    CSG_SUCCESS_MSG CONSTANT VARCHAR2(15) := 'SUCCESS';

BEGIN
        OPEN PA_CUR_RESULT FOR
SELECT TCOP.FI_OPERATION_TYPE_ID,TCOPSIGN.FI_OPERATION_SIGN
FROM  SC_CREDIT.TC_OPERATION_TYPE TCOP
INNER JOIN
SC_CREDIT.TC_OPERATION_SIGN TCOPSIGN
ON
TCOP.FI_OPERATION_SIGN_ID=TCOPSIGN.FI_OPERATION_SIGN_ID WHERE TCOP.FI_STATUS=1 AND TCOPSIGN.FI_STATUS=1
ORDER BY TCOP.FI_OPERATION_TYPE_ID;


    PA_STATUS_CODE := CSG_SUCCESS_CODE;
    PA_STATUS_MSG := CSG_SUCCESS_MSG;
        EXCEPTION
   WHEN OTHERS THEN
    PA_STATUS_CODE := SQLCODE;
    PA_STATUS_MSG := SQLERRM;
   SC_CREDIT.SP_ERROR_LOG (CSL_ISSUE_CONCEPT_MSG, SQLCODE, SQLERRM,
                                   DBMS_UTILITY.FORMAT_ERROR_BACKTRACE,
                                   CSL_ISSUE_CONCEPT_CODE, 'SEL_CONCEPT_SIGN');
END SP_SEL_CONCEPT_SIGN;

/

  GRANT EXECUTE ON SC_CREDIT.SP_SEL_CONCEPT_SIGN TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_SEL_CONCEPT_SIGN TO USRPURPOSEWS;
  GRANT EXECUTE ON SC_CREDIT.SP_SEL_CONCEPT_SIGN TO USRCREDIT02;
--------------------------------------------------------
--  DDL for Procedure SP_UPD_PAYMENT_SCHEDULE_PAYOFF
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_UPD_PAYMENT_SCHEDULE_PAYOFF (
    PA_LOAN_ID           SC_CREDIT.TA_PAYMENT_SCHEDULE.FI_LOAN_ID%TYPE,
    PA_ADMIN_CENTER_ID   SC_CREDIT.TA_PAYMENT_SCHEDULE.FI_ADMIN_CENTER_ID%TYPE,
    PA_PAYMENT_NUMBER_ID SC_CREDIT.TA_PAYMENT_SCHEDULE.FI_PAYMENT_NUMBER_ID%TYPE,
    PA_STATUS            SC_CREDIT.TA_PAYMENT_SCHEDULE.FI_PMT_SCHEDULE_STATUS_ID%TYPE,
    PA_PAYMENT_BALANCE   SC_CREDIT.TA_PAYMENT_SCHEDULE.FN_PAYMENT_BALANCE%TYPE,
    PA_PAYMENT_DATE      VARCHAR2,
    PA_STATUS_MSG        OUT VARCHAR2,
    PA_STATUS_CODE       OUT NUMBER
)
IS
 /* ****************************************************************
 * PROJECT: CORE LOAN
 * DESCRIPTION: PROCEDURE FOR UPDATING PAYOFF INFORMATION
 *              TABLE: TA_PAYMENT_SCHEDULE
 * PRECONDITIONS: PRE-EXISTING LOANS
 * CREATED DATE: 19/11/2024
 * CREATOR: J. GILBERTO CHAVEZ MUNOZ
 * MODIFICATION: 2025-01-20 CESAR SANCHEZ HERNANDEZ
 * [NCPRDC-5152 V2.0.0]
 *****************************************************************/
  CSL_0             CONSTANT SIMPLE_INTEGER := 0;
  CSL_1             CONSTANT SIMPLE_INTEGER := 1;
  CSL_2             CONSTANT SIMPLE_INTEGER := 2;
  CSL_6             CONSTANT SIMPLE_INTEGER := 6;
  CSL_SP            CONSTANT SIMPLE_INTEGER := 1;
  CSL_200           CONSTANT SIMPLE_INTEGER := 200;
  CSL_404           CONSTANT SIMPLE_INTEGER := 404;
  CSL_ARROW         CONSTANT VARCHAR2(5) := '->';
  CSL_JSON          CONSTANT VARCHAR2(5) := NULL;
  CSL_UUID_TRACKING CONSTANT VARCHAR2(5) := NULL;
  CSL_SUCCESS       CONSTANT VARCHAR2(8) := 'OK';
  CSL_FAILURE       CONSTANT VARCHAR2(100) := 'Cannot apply payment because it was in payoff status';
  VL_NUMBER_OF_ROWS       NUMBER;
  VL_TOTAL_NUMBER_OF_ROWS NUMBER;
BEGIN
  PA_STATUS_CODE := CSL_0;
  PA_STATUS_MSG  := CSL_SUCCESS;

SELECT MAX(FI_PAYMENT_NUMBER_ID)
INTO   VL_TOTAL_NUMBER_OF_ROWS
FROM   SC_CREDIT.TA_PAYMENT_SCHEDULE
WHERE  FI_LOAN_ID = PA_LOAN_ID
  AND    FI_ADMIN_CENTER_ID = PA_ADMIN_CENTER_ID
;

IF PA_PAYMENT_NUMBER_ID <= VL_TOTAL_NUMBER_OF_ROWS THEN
SELECT COUNT(FI_LOAN_ID)
INTO   VL_NUMBER_OF_ROWS
FROM   SC_CREDIT.TA_PAYMENT_SCHEDULE
WHERE  FI_LOAN_ID = PA_LOAN_ID
  AND    FI_ADMIN_CENTER_ID = PA_ADMIN_CENTER_ID
  AND    FI_PAYMENT_NUMBER_ID <= PA_PAYMENT_NUMBER_ID
  AND   FI_PMT_SCHEDULE_STATUS_ID IN (CSL_1, CSL_2)
;

IF VL_NUMBER_OF_ROWS <> 0 THEN
UPDATE SC_CREDIT.TA_PAYMENT_SCHEDULE
SET    FI_PMT_SCHEDULE_STATUS_ID = PA_STATUS,
       FD_MODIFICATION_DATE = SYSDATE,
       FN_PAYMENT_BALANCE = PA_PAYMENT_BALANCE,
       FD_PAYMENT_DATE = TO_DATE(PA_PAYMENT_DATE, 'dd/MM/yy')
WHERE  FI_LOAN_ID = PA_LOAN_ID
  AND    FI_ADMIN_CENTER_ID = PA_ADMIN_CENTER_ID
  AND    FI_PAYMENT_NUMBER_ID <= PA_PAYMENT_NUMBER_ID
  AND    FI_PMT_SCHEDULE_STATUS_ID IN (CSL_1, CSL_2)
;

SELECT COUNT(FI_LOAN_ID)
INTO   VL_NUMBER_OF_ROWS
FROM   SC_CREDIT.TA_PAYMENT_SCHEDULE
WHERE  FI_LOAN_ID = PA_LOAN_ID
  AND    FI_ADMIN_CENTER_ID = PA_ADMIN_CENTER_ID
  AND    FI_PAYMENT_NUMBER_ID > PA_PAYMENT_NUMBER_ID
  AND    FI_PMT_SCHEDULE_STATUS_ID IN (CSL_1, CSL_2)
;

IF VL_NUMBER_OF_ROWS <> 0 THEN
UPDATE SC_CREDIT.TA_PAYMENT_SCHEDULE
SET    FI_PMT_SCHEDULE_STATUS_ID = CSL_6,
       FD_MODIFICATION_DATE = SYSDATE
WHERE  FI_LOAN_ID = PA_LOAN_ID
  AND    FI_ADMIN_CENTER_ID = PA_ADMIN_CENTER_ID
  AND    FI_PAYMENT_NUMBER_ID > PA_PAYMENT_NUMBER_ID
  AND    FI_PMT_SCHEDULE_STATUS_ID IN (CSL_1, CSL_2)
;
END IF;
ELSE
      PA_STATUS_CODE := CSL_404;
      PA_STATUS_MSG  := CSL_FAILURE;
END IF;
ELSE
    PA_STATUS_CODE := CSL_404;
    PA_STATUS_MSG  := CSL_FAILURE;
END IF;

COMMIT;

EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    PA_STATUS_CODE := SQLCODE;
    PA_STATUS_MSG := SQLERRM || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    SC_CREDIT.SP_ERROR_LOG(
       UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP)
       ,SQLCODE
       ,SQLERRM
       ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
       ,CSL_UUID_TRACKING
       ,CSL_JSON
       );
END SP_UPD_PAYMENT_SCHEDULE_PAYOFF;

/

  GRANT EXECUTE ON SC_CREDIT.SP_UPD_PAYMENT_SCHEDULE_PAYOFF TO USRNCPCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_BTC_GEN_OPERATION_BALANCE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_BTC_GEN_OPERATION_BALANCE 
   (PTAB_LOANS                     IN SC_CREDIT.TYP_TAB_BTC_LOAN
   ,PTAB_OPERATIONS                IN SC_CREDIT.TYP_TAB_BTC_OPERATION
   ,PTAB_OPERATIONS_DETAIL         IN SC_CREDIT.TYP_TAB_BTC_DETAIL
   ,PTAB_BALANCES                  IN SC_CREDIT.TYP_TAB_BTC_BALANCE
   ,PTAB_BALANCES_DETAIL           IN SC_CREDIT.TYP_TAB_BTC_DETAIL
   ,PA_USER                        IN VARCHAR2
   ,PA_DEVICE                      IN VARCHAR2
   ,PA_GPS_LATITUDE                IN VARCHAR2
   ,PA_GPS_LONGITUDE               IN VARCHAR2
   ,PA_COMMIT                      IN NUMBER
   ,PA_STATUS_CODE                 OUT NUMBER
   ,PA_STATUS_MSG                  OUT VARCHAR2)
   IS
      ----------------------------------------------------------------------
      -- CREATOR: Eduardo Cervantes Hernandez
      -- CREATED DATE:   24/10/2024
      -- DESCRIPTION: Insert Operation
      -- APPLICATION:  Process Batch of Purpose
      ----------------------------------------------------------------------
      --CONSTANTS
      CSL_0                              CONSTANT SIMPLE_INTEGER := 0;
      CSL_1                              CONSTANT SIMPLE_INTEGER := 1;
      CSL_PKG                            CONSTANT SIMPLE_INTEGER := 1;

      --CONSTANTS SUCCESS
      CSL_SUCCESS_CODE                   CONSTANT SIMPLE_INTEGER := 0;
      CSL_SUCCESS_MSG                    CONSTANT VARCHAR2(10) := 'SUCCESS';
      CSL_DATE_FORMAT                    CONSTANT VARCHAR2(40) := 'MM/DD/YYYY hh24:mi:ss';
      CSL_NOT_UPDATED                    CONSTANT VARCHAR2(40) := 'NOT UPDATED - ';
      CSL_NOT_INSERT                     CONSTANT VARCHAR2(40) := 'NOT INSERT - ';
      CSL_TA_LOAN                        CONSTANT VARCHAR2(40) := 'TA_LOAN';
      CSL_TA_LOAN_OPERATION              CONSTANT VARCHAR2(40) := 'TA_LOAN_OPERATION';
      CSL_TA_LOAN_OPERATION_DETAIL       CONSTANT VARCHAR2(40) := 'TA_LOAN_OPERATION_DETAIL';
      CSL_TA_LOAN_BALANCE                CONSTANT VARCHAR2(40) := 'TA_LOAN_BALANCE';
      CSL_TA_LOAN_BALANCE_DETAIL         CONSTANT VARCHAR2(40) := 'TA_LOAN_BALANCE_DETAIL';
      CSL_ERROR_LOAN                     CONSTANT VARCHAR2(30) := 'Loan is not found';
      CSL_ERROR_SEQ                      CONSTANT VARCHAR2(100) := 'I cant update same balance sequence';
      CSL_NUMBER_ERROR                   CONSTANT SIMPLE_INTEGER := -20012;
      CSL_ARROW                          CONSTANT VARCHAR2(20) := ' -> ';

      VL_CURRENT_BALANCE_SEQ             NUMBER(5,0) := 0;
      VL_TRANSACTION                     NUMBER(15,0) := 0;
      VL_I                               NUMBER(10,0) := 0;
      VL_TODAY                           DATE := SYSDATE;
   BEGIN
      PA_STATUS_CODE := CSL_SUCCESS_CODE;
      PA_STATUS_MSG := CSL_SUCCESS_MSG;
      VL_CURRENT_BALANCE_SEQ := 0;
      VL_I := PTAB_LOANS.FIRST;
      VL_TRANSACTION := NVL(PTAB_LOANS(VL_I).FI_TRANSACTION, CSL_0);

      --The sequence is consulted to increment it

      BEGIN
         SELECT LO.FI_CURRENT_BALANCE_SEQ  AS FI_CURRENT_BALANCE_SEQ
           INTO VL_CURRENT_BALANCE_SEQ
           FROM SC_CREDIT.TA_LOAN LO
          WHERE LO.FI_LOAN_ID = PTAB_LOANS(VL_I).FI_LOAN_ID
            AND LO.FI_ADMIN_CENTER_ID = PTAB_LOANS(VL_I).FI_ADMIN_CENTER_ID;
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(CSL_NUMBER_ERROR, CSL_ERROR_LOAN);
      END ;

      --Validate Sequence
      IF VL_CURRENT_BALANCE_SEQ >= PTAB_LOANS(1).FI_CURRENT_BALANCE_SEQ THEN
         RAISE_APPLICATION_ERROR(CSL_NUMBER_ERROR, CSL_ERROR_SEQ);
      END IF;

      --Update in the table TA_LOAN
      UPDATE SC_CREDIT.TA_LOAN LO
         SET LO.FN_PRINCIPAL_BALANCE = CASE WHEN PTAB_LOANS(VL_I).FN_PRINCIPAL_BALANCE = 0
                                            THEN LO.FN_PRINCIPAL_BALANCE
                                            ELSE PTAB_LOANS(VL_I).FN_PRINCIPAL_BALANCE
                                       END
            ,LO.FN_FINANCE_CHARGE_BALANCE = CASE WHEN PTAB_LOANS(VL_I).FN_FINANCE_CHARGE_BALANCE = 0
                                            THEN LO.FN_FINANCE_CHARGE_BALANCE
                                            ELSE PTAB_LOANS(VL_I).FN_FINANCE_CHARGE_BALANCE
                                       END
            ,LO.FN_ADDITIONAL_CHARGE_BALANCE = CASE WHEN PTAB_LOANS(VL_I).FN_ADDITIONAL_CHARGE_BALANCE = 0
                                            THEN LO.FN_ADDITIONAL_CHARGE_BALANCE
                                            ELSE PTAB_LOANS(VL_I).FN_ADDITIONAL_CHARGE_BALANCE
                                       END
            ,LO.FI_ADDITIONAL_STATUS = CASE WHEN PTAB_LOANS(VL_I).FI_ADDITIONAL_STATUS = 0
                                            THEN LO.FI_ADDITIONAL_STATUS
                                            ELSE PTAB_LOANS(VL_I).FI_ADDITIONAL_STATUS
                                       END
            ,LO.FI_CURRENT_BALANCE_SEQ = PTAB_LOANS(VL_I).FI_CURRENT_BALANCE_SEQ
            ,LO.FI_LOAN_STATUS_ID = CASE WHEN PTAB_LOANS(VL_I).FI_LOAN_STATUS_ID = 0
                                            THEN LO.FI_LOAN_STATUS_ID
                                            ELSE PTAB_LOANS(VL_I).FI_LOAN_STATUS_ID
                                       END
            ,LO.FD_LOAN_STATUS_DATE = CASE WHEN NVL(TO_DATE(PTAB_LOANS(VL_I).FC_LOAN_STATUS_DATE, CSL_DATE_FORMAT), NULL) IS NULL
                                              THEN LO.FD_LOAN_STATUS_DATE
                                              ELSE TO_DATE(PTAB_LOANS(VL_I).FC_LOAN_STATUS_DATE, CSL_DATE_FORMAT)
                                    END

            ,LO.FC_USER = PA_USER
            ,LO.FD_MODIFICATION_DATE = VL_TODAY
       WHERE LO.FI_LOAN_ID = PTAB_LOANS(VL_I).FI_LOAN_ID
         AND LO.FI_ADMIN_CENTER_ID = PTAB_LOANS(VL_I).FI_ADMIN_CENTER_ID;


      IF SQL%ROWCOUNT = CSL_0 THEN
         RAISE_APPLICATION_ERROR(CSL_NUMBER_ERROR, CSL_NOT_UPDATED || CSL_TA_LOAN);
      END IF;

      --Insert in OPERATION
      INSERT INTO SC_CREDIT.TA_LOAN_OPERATION
                 (FI_LOAN_OPERATION_ID
                 ,FI_COUNTRY_ID
                 ,FI_COMPANY_ID
                 ,FI_BUSINESS_UNIT_ID
                 ,FI_LOAN_ID
                 ,FI_ADMIN_CENTER_ID
                 ,FI_OPERATION_TYPE_ID
                 ,FI_TRANSACTION
                 ,FN_OPERATION_AMOUNT
                 ,FD_APPLICATION_DATE
                 ,FD_OPERATION_DATE
                 ,FI_STATUS
                 ,FC_END_USER
                 ,FC_UUID_TRACKING
                 ,FC_GPS_LATITUDE
                 ,FC_GPS_LONGITUDE
                 ,FC_DEVICE
                 ,FC_USER
                 ,FD_CREATED_DATE
                 ,FD_MODIFICATION_DATE)
           SELECT OP.FI_LOAN_OPERATION_ID                         AS FI_LOAN_OPERATION_ID
                 ,OP.FI_COUNTRY_ID                                AS FI_COUNTRY_ID
                 ,OP.FI_COMPANY_ID                                AS FI_COMPANY_ID
                 ,OP.FI_BUSINESS_UNIT_ID                          AS FI_BUSINESS_UNIT_ID
                 ,OP.FI_LOAN_ID                                   AS FI_LOAN_ID
                 ,OP.FI_ADMIN_CENTER_ID                           AS FI_ADMIN_CENTER_ID
                 ,OP.FI_OPERATION_TYPE_ID                         AS FI_OPERATION_TYPE_ID
                 ,VL_TRANSACTION                                  AS FI_TRANSACTION
                 ,OP.FN_OPERATION_AMOUNT                          AS FN_OPERATION_AMOUNT
                 ,TO_DATE(OP.FC_APPLICATION_DATE,CSL_DATE_FORMAT) AS FD_APPLICATION_DATE
                 ,TO_DATE(OP.FC_OPERATION_DATE,CSL_DATE_FORMAT)   AS FD_OPERATION_DATE
                 ,OP.FI_STATUS                                    AS FI_STATUS
                 ,OP.FC_END_USER                                  AS FC_END_USER
                 ,OP.FC_UUID_TRACKING                             AS FC_UUID_TRACKING
                 ,PA_GPS_LATITUDE                              AS FC_GPS_LATITUDE
                 ,PA_GPS_LONGITUDE                             AS FC_GPS_LONGITUDE
                 ,PA_DEVICE                                    AS FC_DEVICE
                 ,USER                                            AS FC_USER
                 ,VL_TODAY                                        AS FD_CREATED_DATE
                 ,VL_TODAY                                        AS FD_MODIFICATION_DATE
             FROM TABLE (PTAB_OPERATIONS) OP
            WHERE OP.FI_ADMIN_CENTER_ID = PTAB_LOANS(VL_I).FI_ADMIN_CENTER_ID
              AND OP.FI_LOAN_ID = PTAB_LOANS(VL_I).FI_LOAN_ID;

      IF SQL%ROWCOUNT = CSL_0 THEN
         RAISE_APPLICATION_ERROR(CSL_NUMBER_ERROR, CSL_NOT_INSERT || CSL_TA_LOAN_OPERATION);
      END IF;

      --IInsert detail in the table OPERATION DETAIL
      INSERT INTO SC_CREDIT.TA_LOAN_OPERATION_DETAIL
                 (FI_LOAN_OPERATION_ID
                 ,FI_ADMIN_CENTER_ID
                 ,FI_LOAN_CONCEPT_ID
                 ,FN_ITEM_AMOUNT
                 ,FC_USER
                 ,FD_CREATED_DATE
                 ,FD_MODIFICATION_DATE)
           SELECT DET.FI_LOAN_OPERATION_ID AS FI_LOAN_OPERATION_ID
                 ,DET.FI_ADMIN_CENTER_ID AS FI_ADMIN_CENTER_ID
                 ,DET.FI_LOAN_CONCEPT_ID AS FI_LOAN_CONCEPT_ID
                 ,DET.FN_ITEM_AMOUNT AS FN_ITEM_AMOUNT
                 ,USER AS FC_USER
                 ,VL_TODAY AS FD_CREATED_DATE
                 ,VL_TODAY AS FD_MODIFICATION_DATE
             FROM TABLE (PTAB_OPERATIONS_DETAIL) DET
            WHERE DET.FI_ADMIN_CENTER_ID = PTAB_LOANS(VL_I).FI_ADMIN_CENTER_ID
              AND DET.FI_LOAN_ID = PTAB_LOANS(VL_I).FI_LOAN_ID;

      IF SQL%ROWCOUNT = CSL_0 THEN
         RAISE_APPLICATION_ERROR(CSL_NUMBER_ERROR, CSL_NOT_INSERT || CSL_TA_LOAN_OPERATION_DETAIL);
      END IF;

      --Insert balance in the table LOAN_BALANCE
      INSERT INTO SC_CREDIT.TA_LOAN_BALANCE
                 (FI_LOAN_BALANCE_ID
                 ,FI_ADMIN_CENTER_ID
                 ,FI_LOAN_ID
                 ,FI_LOAN_OPERATION_ID
                 ,FI_BALANCE_SEQ
                 ,FN_PRINCIPAL_BALANCE
                 ,FN_FINANCE_CHARGE_BALANCE
                 ,FN_ADDITIONAL_CHARGE_BALANCE
                 ,FC_USER
                 ,FD_CREATED_DATE
                 ,FD_MODIFICATION_DATE)
           SELECT BL.FI_LOAN_BALANCE_ID               AS FI_LOAN_BALANCE_ID
                 ,BL.FI_ADMIN_CENTER_ID               AS FI_ADMIN_CENTER_ID
                 ,BL.FI_LOAN_ID                       AS FI_LOAN_ID
                 ,BL.FI_LOAN_OPERATION_ID             AS FI_LOAN_OPERATION_ID
                 ,BL.FI_BALANCE_SEQ                   AS FI_BALANCE_SEQ
                 ,BL.FN_PRINCIPAL_BALANCE             AS FN_PRINCIPAL_BALANCE
                 ,BL.FN_FINANCE_CHARGE_BALANCE        AS FN_FINANCE_CHARGE_BALANCE
                 ,BL.FN_ADDITIONAL_CHARGE_BALANCE     AS FN_ADDITIONAL_CHARGE_BALANCE
                 ,USER                                AS FC_USER
                 ,VL_TODAY                            AS FD_CREATED_DATE
                 ,VL_TODAY                            AS FD_MODIFICATION_DATE
             FROM TABLE (PTAB_BALANCES) BL
            WHERE BL.FI_ADMIN_CENTER_ID = PTAB_LOANS(VL_I).FI_ADMIN_CENTER_ID
              AND BL.FI_LOAN_ID = PTAB_LOANS(VL_I).FI_LOAN_ID;

      IF SQL%ROWCOUNT = CSL_0 THEN
         RAISE_APPLICATION_ERROR(CSL_NUMBER_ERROR, CSL_NOT_INSERT || CSL_TA_LOAN_BALANCE);
      END IF;

      --Insert the detail of balance in the table LOAN_BALANCE
      INSERT INTO SC_CREDIT.TA_LOAN_BALANCE_DETAIL
                 (FI_LOAN_BALANCE_ID
                 ,FI_ADMIN_CENTER_ID
                 ,FI_LOAN_CONCEPT_ID
                 ,FN_ITEM_AMOUNT
                 ,FC_USER
                 ,FD_CREATED_DATE
                 ,FD_MODIFICATION_DATE)
           SELECT DET.FI_LOAN_OPERATION_ID   AS FI_LOAN_BALANCE_ID
                 ,DET.FI_ADMIN_CENTER_ID     AS FI_ADMIN_CENTER_ID
                 ,DET.FI_LOAN_CONCEPT_ID     AS FI_LOAN_CONCEPT_ID
                 ,DET.FN_ITEM_AMOUNT         AS FN_ITEM_AMOUNT
                 ,USER                       AS FC_USER
                 ,VL_TODAY                   AS FD_CREATED_DATE
                 ,VL_TODAY                   AS FD_MODIFICATION_DATE
             FROM TABLE (PTAB_BALANCES_DETAIL) DET
            WHERE DET.FI_ADMIN_CENTER_ID = PTAB_LOANS(VL_I).FI_ADMIN_CENTER_ID
              AND DET.FI_LOAN_ID = PTAB_LOANS(VL_I).FI_LOAN_ID;

      IF SQL%ROWCOUNT = CSL_0 THEN
         RAISE_APPLICATION_ERROR(CSL_NUMBER_ERROR, CSL_NOT_INSERT || CSL_TA_LOAN_BALANCE_DETAIL);
      END IF;

      IF(PA_COMMIT = CSL_1)THEN
         COMMIT;
      END IF;

   EXCEPTION
      WHEN OTHERS THEN
         ROLLBACK;
         PA_STATUS_CODE := SQLCODE;
         PA_STATUS_MSG := SQLERRM || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;

         SC_CREDIT.SP_BATCH_ERROR_LOG(UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_PKG)
                                     ,SQLCODE
                                     ,SQLERRM
                                     ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                     ,VL_TRANSACTION
                                     ,NULL
                                     );
END SP_BTC_GEN_OPERATION_BALANCE;

/

  GRANT EXECUTE ON SC_CREDIT.SP_BTC_GEN_OPERATION_BALANCE TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_BTC_GEN_OPERATION_BALANCE TO USRBTCCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_SEL_ACCRUED_PAYMENT_INTEREST
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_SEL_ACCRUED_PAYMENT_INTEREST (
    PA_LOAN_ID               IN NUMBER
     ,PA_ADMIN_CENTER_ID      IN NUMBER
      ,PA_STATUS_CODE         OUT NUMBER
          ,PA_STATUS_MSG          OUT VARCHAR2
          ,PA_CUR_SELECT          OUT SC_CREDIT.PA_TYPES.TYP_CURSOR)
    IS

  /* **************************************************************
   * DESCRIPTION: SELECT IN TABLE INTEREST
   * PRECONDITIONS:
   * CREATED DATE: 04/11/2024
   * MODIFIC DATE: 12/11/2024
   * CREATOR: IVAN LOPEZ
   ************************************************************** */

   CSL_0                 CONSTANT SIMPLE_INTEGER := 0;
   CSL_1                 CONSTANT SIMPLE_INTEGER := 1;
   CSL_SP                CONSTANT SIMPLE_INTEGER := 1;
   CSL_2002              CONSTANT SIMPLE_INTEGER := -20002;
    CSL_MSG_SUCCESS       CONSTANT VARCHAR2(20)   := 'SUCCESS';
   CSL_NOT_DATA         CONSTANT VARCHAR2(20)   := 'NO INTEREST FOUND';
   CSL_NOT_LOAN         CONSTANT VARCHAR2(20)   := 'NO LOAN FOUND';
   CSL_PAYMENT         CONSTANT VARCHAR2(20)   := 'PAYMENT';
   CSL_DUE_DATE         CONSTANT VARCHAR2(20)   := 'DUE DATE';

    VL_LOAN  NUMBER(15,0);
    VL_ADMIN_CENTER NUMBER(8,0);
    VL_PAYMENT_NUMBER NUMBER(3,0);
    VL_DAYS_ACUM NUMBER(4,0);
    VL_ACCRUED_INTEREST_BALANCE NUMBER(14,4);
    VL_ACCRUED_INTEREST_LOAN NUMBER(14,4);
    VL_LOAN_STATUS   NUMBER(3,0);
    VL_ORDER  NUMBER(3,0);


   BEGIN

   PA_CUR_SELECT := NULL;

    --CONSULT LOAN,INTEREST,SCHEDULE
      SELECT B.FI_LOAN_ID AS FI_LOAN_ID,
            B.FI_ADMIN_CENTER_ID AS FI_ADMIN_CENTER_ID,
            NVL(A.FI_PAYMENT_NUMBER_ID,0) AS  FI_PAYMENT_NUMBER_ID,
            NVL(A.FI_DAYS_ACUM_BY_TERM,0) AS FI_DAYS_ACUM_BY_TERM,
                CASE WHEN FC_CONDITION_INTEREST IN (CSL_PAYMENT,CSL_DUE_DATE) THEN 0
                ELSE NVL(TRUNC(A.FN_ACCRUED_INTEREST_BALANCE,2),0) END AS INTEREST_BALANCE_DAY,
            NVL(TRUNC(A.FN_ACCRUED_INTEREST_LOAN - a.FN_PAYMENT_INTEREST,2),0) AS INTEREST_DAY_LOAN,
            NVL(B.FI_LOAN_STATUS_ID,0) AS STATUS_LOAN,
           NVL(ROW_NUMBER() OVER (PARTITION BY A.FI_LOAN_ID ORDER BY A.FI_PAYMENT_NUMBER_ID DESC,A.FI_DAYS_ACUM_BY_TERM DESC),0) AS ORDEN
    INTO VL_LOAN,VL_ADMIN_CENTER,VL_PAYMENT_NUMBER,VL_DAYS_ACUM,VL_ACCRUED_INTEREST_BALANCE,VL_ACCRUED_INTEREST_LOAN,VL_LOAN_STATUS,VL_ORDER
    FROM sc_credit.ta_loan B
    LEFT OUTER  JOIN SC_CREDIT.TA_LOAN_INTEREST A
    ON B.FI_LOAN_ID =A.FI_LOAN_ID
    AND B.FI_ADMIN_CENTER_ID = A.FI_ADMIN_CENTER_ID
    WHERE B.FI_LOAN_ID = PA_LOAN_ID
    AND B.FI_ADMIN_CENTER_ID = PA_ADMIN_CENTER_ID
    FETCH NEXT 1 ROW ONLY;

        PA_STATUS_CODE := CSL_0;
        PA_STATUS_MSG  := CSL_MSG_SUCCESS;

      IF  VL_PAYMENT_NUMBER = CSL_0 THEN

        PA_STATUS_MSG := CSL_NOT_DATA;

      END IF;

       OPEN PA_CUR_SELECT FOR
        SELECT VL_LOAN AS FI_LOAN_ID,
               VL_ADMIN_CENTER AS FI_ADMIN_CENTER_ID,
               VL_PAYMENT_NUMBER AS FI_PAYMENT_NUMBER_ID,
               VL_DAYS_ACUM AS FI_DAYS_ACUM_BY_TERM,
               VL_ACCRUED_INTEREST_BALANCE AS INTEREST_BALANCE_DAY,
               VL_ACCRUED_INTEREST_LOAN AS  INTEREST_DAY_LOAN,
               VL_LOAN_STATUS AS STATUS_LOAN
               ,VL_ORDER AS ORDEN FROM DUAL;


     EXCEPTION
     WHEN NO_DATA_FOUND THEN
      PA_STATUS_CODE := CSL_0;
        PA_STATUS_MSG  := CSL_NOT_LOAN;
         SC_CREDIT.SP_ERROR_LOG(UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP)
                                    ,SQLCODE
                                    ,SQLERRM
                                    ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                    ,PA_LOAN_ID || PA_ADMIN_CENTER_ID
                                    ,NULL);

      WHEN OTHERS THEN
        PA_STATUS_CODE := SQLCODE;
        PA_STATUS_MSG := SQLERRM  || ' -> ' ||  DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
         SC_CREDIT.SP_ERROR_LOG(UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP)
                                    ,SQLCODE
                                    ,SQLERRM
                                    ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                    ,PA_LOAN_ID || PA_ADMIN_CENTER_ID
                                    ,NULL);

END SP_SEL_ACCRUED_PAYMENT_INTEREST;

/

  GRANT EXECUTE ON SC_CREDIT.SP_SEL_ACCRUED_PAYMENT_INTEREST TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_SEL_ACCRUED_PAYMENT_INTEREST TO USRPURPOSEWS;
  GRANT EXECUTE ON SC_CREDIT.SP_SEL_ACCRUED_PAYMENT_INTEREST TO USRCREDIT02;
  GRANT EXECUTE ON SC_CREDIT.SP_SEL_ACCRUED_PAYMENT_INTEREST TO USRBTCCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_UPD_PAYMENT_INTEREST
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_UPD_PAYMENT_INTEREST (
    PA_FI_LOAN_ID                   IN NUMBER
   ,PA_FI_ADMIN_CENTER_ID           IN NUMBER
   ,PA_PAYMENT_INTEREST             IN NUMBER
   ,PA_COMMIT                       IN NUMBER
   ,PA_STATUS_CODE                  OUT NUMBER
   ,PA_STATUS_MSG                   OUT VARCHAR2
   )
   IS
   /* **************************************************************
   * DESCRIPTION: UPDATE IN TABLE INTEREST IN INTEREST
   * PRECONDITIONS:
   * CREATED DATE: 04/11/2024
   * CREATOR: IVAN LOPEZ
   ************************************************************** */

   CSL_0                 CONSTANT SIMPLE_INTEGER := 0;
   CSL_1                 CONSTANT SIMPLE_INTEGER := 1;
   CSL_SP                CONSTANT SIMPLE_INTEGER := 1;
   CSL_2002              CONSTANT SIMPLE_INTEGER := -20002;
   CSL_2003              CONSTANT SIMPLE_INTEGER := -20003;
   CSL_MSG_SUCCESS       CONSTANT VARCHAR2(20)   := 'SUCCESS';
   CSL_NOT_AFFECT_DATA         CONSTANT VARCHAR2(20)   := 'DOES NOT AFFECT DAT';
    CSL_NOT_DATA         CONSTANT VARCHAR2(20)   := 'NO DATA FOUND';

    CLS_CONDITION     CONSTANT VARCHAR2(20)   := 'PAYMENT';


     VL_PAYMENT_NUMBER                  NUMBER(3);
     VL_DAYS_ACUM_BY_TERM               NUMBER(14,4);
     VL_ORDER                           NUMBER(2,0);
     VL_PAYMENT                         NUMBER(14,4);

   BEGIN

      SELECT
            A.FI_PAYMENT_NUMBER_ID,
            A.FI_DAYS_ACUM_BY_TERM,
            A.FN_PAYMENT_INTEREST
          ,ROW_NUMBER() OVER (PARTITION BY A.FI_LOAN_ID ORDER BY A.FI_PAYMENT_NUMBER_ID DESC,A.FI_DAYS_ACUM_BY_TERM DESC) AS ORDEN
       INTO VL_PAYMENT_NUMBER,VL_DAYS_ACUM_BY_TERM,VL_PAYMENT  ,VL_ORDER
       FROM sc_credit.ta_loan_interest A
        WHERE A.FI_LOAN_ID = PA_FI_LOAN_ID
      AND A.FI_ADMIN_CENTER_ID = PA_FI_ADMIN_CENTER_ID
      FETCH NEXT 1 ROW ONLY;


     UPDATE sc_credit.ta_loan_interest SET FN_PAYMENT_INTEREST = VL_PAYMENT + PA_PAYMENT_INTEREST ,
                                           FD_MODIFICATION_DATE = SYSDATE,
                                           FC_CONDITION_INTEREST = CLS_CONDITION
     WHERE FI_LOAN_ID = PA_FI_LOAN_ID
     AND FI_ADMIN_CENTER_ID = PA_FI_ADMIN_CENTER_ID
     AND FI_PAYMENT_NUMBER_ID = VL_PAYMENT_NUMBER
     AND FI_DAYS_ACUM_BY_TERM = VL_DAYS_ACUM_BY_TERM;

     IF PA_COMMIT = CSL_1 THEN
         COMMIT;
     END IF;

       PA_STATUS_CODE:= CSL_0;
       PA_STATUS_MSG := CSL_MSG_SUCCESS;


      EXCEPTION
        WHEN NO_DATA_FOUND THEN
         PA_STATUS_CODE:= CSL_2003;
         PA_STATUS_MSG := CSL_NOT_DATA;
        WHEN OTHERS THEN
        ROLLBACK;
        PA_STATUS_CODE := SQLCODE;
        PA_STATUS_MSG := SQLERRM  || ' -> ' ||  DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
        SC_CREDIT.SP_ERROR_LOG(UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP)
                                    ,SQLCODE
                                    ,SQLERRM
                                    ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                    ,PA_FI_LOAN_ID || PA_FI_ADMIN_CENTER_ID
                                    ,NULL);
END SP_UPD_PAYMENT_INTEREST;

/

  GRANT EXECUTE ON SC_CREDIT.SP_UPD_PAYMENT_INTEREST TO USRNCPCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_BTC_INS_PWO_AMOUNT_DETAIL
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_BTC_INS_PWO_AMOUNT_DETAIL 
   (
   PA_FI_LOAN_ID               IN SC_CREDIT.TA_PWO_AMOUNT_DETAIL.FI_LOAN_ID%TYPE
  ,PA_FI_ADMIN_CENTER_ID       IN SC_CREDIT.TA_PWO_AMOUNT_DETAIL.FI_ADMIN_CENTER_ID%TYPE
  ,PA_FN_PAY_OFF_AMOUNT        IN SC_CREDIT.TA_PWO_AMOUNT_DETAIL.FN_PAY_OFF_AMOUNT%TYPE
  ,PA_FN_PWO_EXT_PAYMENT       IN SC_CREDIT.TA_PWO_AMOUNT_DETAIL.FN_PWO_EXT_PAYMENT%TYPE
  ,PA_FN_AMOUNT_PAID           IN SC_CREDIT.TA_PWO_AMOUNT_DETAIL.FN_AMOUNT_PAID%TYPE
  ,PA_FN_PWO_MIN_PAYMENT       IN SC_CREDIT.TA_PWO_AMOUNT_DETAIL.FN_PWO_MIN_PAYMENT%TYPE
  ,PA_FI_ADD_EXTENSION         IN SC_CREDIT.TA_PWO_AMOUNT_DETAIL.FI_ADD_EXTENSION%TYPE
  ,PA_FD_PWO_DATE              IN VARCHAR2
  ,PA_COMMIT                   IN NUMBER
  ,PA_STATUS_CODE              OUT NUMBER
  ,PA_STATUS_MSG               OUT VARCHAR2
   )
   IS
    /* **************************************************************
   * DESCRIPTION: PROCESS TO INSERT IN TABLE TA_PWO_AMOUNT_DETAIL
   * CREATED DATE: 15/11/2024
   * CREATOR: IVAN LOPEZ
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
   CSL_LOAN_ID                  CONSTANT VARCHAR2(20)   := ' LOAN_ID';
   CSL_COMA                     CONSTANT VARCHAR2(20)   := ',';
   CSL_ARROW                    CONSTANT VARCHAR2(20)   := '->';

   BEGIN

      PA_STATUS_CODE:= CSL_0;
      PA_STATUS_MSG := CSL_MSG_SUCCESS;

      INSERT INTO SC_CREDIT.TA_PWO_AMOUNT_DETAIL
                 (FI_LOAN_ID
                 ,FI_ADMIN_CENTER_ID
                 ,FN_PAY_OFF_AMOUNT
                 ,FN_PWO_EXT_PAYMENT
                 ,FN_AMOUNT_PAID
                 ,FN_PWO_MIN_PAYMENT
                 ,FI_ADD_EXTENSION
                 ,FD_PWO_DATE
                 ,FC_USER
                 ,FD_CREATED_DATE
                 ,FD_MODIFICATION_DATE)
          VALUES (PA_FI_LOAN_ID
                 ,PA_FI_ADMIN_CENTER_ID
                 ,PA_FN_PAY_OFF_AMOUNT
                 ,PA_FN_PWO_EXT_PAYMENT
                 ,PA_FN_AMOUNT_PAID
                 ,PA_FN_PWO_MIN_PAYMENT
                 ,PA_FI_ADD_EXTENSION
                 ,TO_DATE(PA_FD_PWO_DATE,CSL_DATE_FORMAT)
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
                                  ,CSL_1 --PA_FI_TRANSACTION
                                  ,CSL_LOAN_ID || PA_FI_LOAN_ID || CSL_COMA
                                  ||CSL_ADMIN_CENTER_ID ||PA_FI_ADMIN_CENTER_ID);

END SP_BTC_INS_PWO_AMOUNT_DETAIL;

/

  GRANT EXECUTE ON SC_CREDIT.SP_BTC_INS_PWO_AMOUNT_DETAIL TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_BTC_INS_PWO_AMOUNT_DETAIL TO USRBTCCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_LOAN_LAST_DETAIL
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_LOAN_LAST_DETAIL (
    PA_LOAN_ID  NUMBER,
    PA_LOAN OUT CLOB,
    PA_DESCRIPTION OUT VARCHAR2,
    PA_CODE OUT NUMBER
)
IS
    VG_LOAN_BALANCE_ID NUMBER;


     TYPE REC_BALANCE IS RECORD
     (
        FI_LOAN_BALANCE_ID              SC_CREDIT.TA_LOAN_BALANCE.FI_LOAN_BALANCE_ID%TYPE,
        FI_LOAN_OPERATION_ID            SC_CREDIT.TA_LOAN_BALANCE.FI_LOAN_OPERATION_ID%TYPE,
        FN_PRINCIPAL_BALANCE            SC_CREDIT.TA_LOAN_BALANCE.FN_PRINCIPAL_BALANCE%TYPE,
        FN_FINANCE_CHARGE_BALANCE       SC_CREDIT.TA_LOAN_BALANCE.FN_FINANCE_CHARGE_BALANCE%TYPE,
        FN_ADDITIONAL_CHARGE_BALANCE    SC_CREDIT.TA_LOAN_BALANCE.FN_ADDITIONAL_CHARGE_BALANCE%TYPE
     );

     TYPE TAB_VALUE_BALANCE IS TABLE OF REC_BALANCE;
     VL_TABVALUE_BALANCE TAB_VALUE_BALANCE;

     TYPE REC_DETAIL IS RECORD
     (
        FI_LOAN_CONCEPT_ID SC_CREDIT.TA_LOAN_BALANCE_DETAIL.FI_LOAN_CONCEPT_ID%TYPE,
        FN_ITEM_AMOUNT SC_CREDIT.TA_LOAN_BALANCE_DETAIL.FN_ITEM_AMOUNT%TYPE
     );

     TYPE TAB_VALUE_DETAIL IS TABLE OF REC_DETAIL;
     VL_TABVALUE_DETAIL TAB_VALUE_DETAIL;

     VG_JO_BALANCE_TYPE JSON_OBJECT_T := JSON_OBJECT_T();
     VG_JA_BALANCE_TYPE      JSON_ARRAY_T  := JSON_ARRAY_T();
     VG_BALANCE_TYPE CLOB;

     VG_JO_DETAIL_TYPE JSON_OBJECT_T := JSON_OBJECT_T();
     VG_JA_DETAIL_TYPE      JSON_ARRAY_T  := JSON_ARRAY_T();
     VG_DETAIL_TYPE CLOB;

BEGIN
    PA_CODE:=0;
    VG_LOAN_BALANCE_ID:=0;


    SELECT MAX(FI_LOAN_BALANCE_ID) INTO VG_LOAN_BALANCE_ID FROM SC_CREDIT.TA_LOAN_BALANCE WHERE FI_LOAN_ID=PA_LOAN_ID;

    SELECT FI_LOAN_CONCEPT_ID,FN_ITEM_AMOUNT
    BULK COLLECT INTO VL_TABVALUE_DETAIL
    FROM SC_CREDIT.TA_LOAN_BALANCE_DETAIL
    WHERE FI_LOAN_BALANCE_ID=VG_LOAN_BALANCE_ID
    ORDER BY FD_CREATED_DATE;

    VG_JO_DETAIL_TYPE := JSON_OBJECT_T();
    VG_JA_DETAIL_TYPE := JSON_ARRAY_T();

     FOR i IN 1.. VL_TABVALUE_DETAIL.COUNT LOOP

           VG_JO_DETAIL_TYPE.put('conceptId',VL_TABVALUE_DETAIL(i).FI_LOAN_CONCEPT_ID );
           VG_JO_DETAIL_TYPE.put('itemAmount',VL_TABVALUE_DETAIL(i).FN_ITEM_AMOUNT );

           VG_JA_DETAIL_TYPE.append(VG_JO_DETAIL_TYPE);

    END LOOP;

    VG_DETAIL_TYPE := VG_JA_DETAIL_TYPE.to_string;

    SELECT FI_LOAN_BALANCE_ID, FI_LOAN_OPERATION_ID, FN_PRINCIPAL_BALANCE, FN_FINANCE_CHARGE_BALANCE, FN_ADDITIONAL_CHARGE_BALANCE
    BULK COLLECT INTO VL_TABVALUE_BALANCE
    FROM SC_CREDIT.TA_LOAN_BALANCE
    WHERE FI_LOAN_BALANCE_ID=VG_LOAN_BALANCE_ID;

    VG_JO_BALANCE_TYPE := JSON_OBJECT_T();
    VG_JA_BALANCE_TYPE := JSON_ARRAY_T();

    FOR i IN 1.. VL_TABVALUE_BALANCE.COUNT LOOP

           VG_JO_BALANCE_TYPE.put('id',VL_TABVALUE_BALANCE(i).FI_LOAN_BALANCE_ID );
           VG_JO_BALANCE_TYPE.put('loanOperationId',VL_TABVALUE_BALANCE(i).FI_LOAN_OPERATION_ID );
           VG_JO_BALANCE_TYPE.put('principalBalance',VL_TABVALUE_BALANCE(i).FN_PRINCIPAL_BALANCE );
           VG_JO_BALANCE_TYPE.put('financeChargeBalance',VL_TABVALUE_BALANCE(i).FN_FINANCE_CHARGE_BALANCE );
           VG_JO_BALANCE_TYPE.put('additionalChargeBalance',VL_TABVALUE_BALANCE(i).FN_ADDITIONAL_CHARGE_BALANCE );
           VG_JO_BALANCE_TYPE.put('TA_BALANCE_DETAIL',VG_JA_DETAIL_TYPE);


           VG_JA_BALANCE_TYPE.append(VG_JO_BALANCE_TYPE);

    END LOOP;

    VG_BALANCE_TYPE := VG_JA_BALANCE_TYPE.to_string;



    SELECT json_object('TA_LOAN' VALUE json_object('customerId'            VALUE LN.FC_CUSTOMER_ID,
                                                 'originationCenterId'     VALUE LN.FI_ORIGINATION_CENTER_ID,
                                                 'adminCenterId'           VALUE LN.FI_ADMIN_CENTER_ID,
                                                 'status'                  VALUE LN.FI_LOAN_STATUS_ID,
                                                 'additionalStatus'        VALUE LN.FI_ADDITIONAL_STATUS,
                                                 'statusDate'              VALUE TO_CHAR(LN.FD_LOAN_STATUS_DATE,'YYYY-MM-DD'),
                                                 'principalBalance'        VALUE LN.FN_PRINCIPAL_BALANCE,
                                                 'financeChargeBalance'    VALUE LN.FN_FINANCE_CHARGE_BALANCE,
                                                                                             'additionalChargeBalance' VALUE LN.FN_ADDITIONAL_CHARGE_BALANCE,
                                                 'TA_BALANCE'          VALUE VG_BALANCE_TYPE FORMAT JSON
                                                )
    )
    INTO   PA_LOAN
    FROM   SC_CREDIT.TA_LOAN LN
    WHERE  FI_LOAN_ID= PA_LOAN_ID;


    EXCEPTION
      WHEN OTHERS THEN
        PA_CODE := 500;
        PA_DESCRIPTION:='ERROR:'||SQLCODE||':'||SQLERRM||'';
        SC_CREDIT.SP_ERROR_LOG(UTL_CALL_STACK.SUBPROGRAM(1)(1)
                                    ,SQLCODE
                                    ,SQLERRM
                                    ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                    ,PA_LOAN_ID
                                    ,NULL);



END SP_LOAN_LAST_DETAIL;

/

  GRANT EXECUTE ON SC_CREDIT.SP_LOAN_LAST_DETAIL TO USRNCPCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_EXE_CUSTOMER_INFO
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_EXE_CUSTOMER_INFO (
    PA_CUSTOMER_ID IN SC_CREDIT.TA_LOAN.FC_CUSTOMER_ID%TYPE,
    PA_LOAN_STATUS_ID IN SC_CREDIT.TA_LOAN.FI_LOAN_STATUS_ID%TYPE DEFAULT NULL,
    PA_CUR_MAIN_INFO OUT SYS_REFCURSOR,
    PA_STATUS_CODE OUT NUMBER,
    PA_STATUS_MSG OUT VARCHAR2)

  AS
  -- GLOBAL CONSTANTS
  CSG_0                     CONSTANT SIMPLE_INTEGER := 0;
  CSG_1                     CONSTANT SIMPLE_INTEGER := 1;
  CSG_ARROW                 CONSTANT VARCHAR2(5) := ' -> ';
  CSG_X                     CONSTANT VARCHAR2(5) := 'X';
  CSG_SUCCESS_CODE          CONSTANT SIMPLE_INTEGER := 0;
  CSG_SUCCESS_MSG           CONSTANT VARCHAR2(10) := 'SUCCESS';
  CSG_NO_DATA_FOUND_CODE    CONSTANT SIMPLE_INTEGER := -20204;
  CSG_NO_DATA_FOUND_MSG     CONSTANT VARCHAR2(50) := 'THE DATA DOES NOT EXIST';
  CSG_SP_EXE_CUSTOMER_INFO  CONSTANT VARCHAR2(50) := 'SP_EXE_CUSTOMER_INFO';
/*************************************************************
  PROJECT    :  NCP-OUTSTANDING BALANCE
  DESCRIPTION:  STORED PROCEDURE TO SELECT THE CUSTOMER INFO
  CREATOR:      JOS??? DE JES???S BRAVO AGUILAR.
  CREATED DATE: AGO-29-2024
  MODIFICATION DATE: OCT-24-2024
*************************************************************/

    EXC_NO_DATA_FOUND EXCEPTION;
    PRAGMA EXCEPTION_INIT(EXC_NO_DATA_FOUND, CSG_NO_DATA_FOUND_CODE);

    VL_CUSTOMER_ID SC_CREDIT.TA_LOAN.FC_CUSTOMER_ID%TYPE;

  BEGIN
    PA_STATUS_CODE := CSG_SUCCESS_CODE;
    PA_STATUS_MSG := CSG_SUCCESS_MSG;

  SELECT COUNT(CSG_1)
  INTO VL_CUSTOMER_ID
  FROM SC_CREDIT.TA_LOAN
  WHERE FC_CUSTOMER_ID = PA_CUSTOMER_ID;
  IF VL_CUSTOMER_ID = CSG_0 THEN
    RAISE EXC_NO_DATA_FOUND;
  END IF;

  OPEN PA_CUR_MAIN_INFO FOR
  SELECT DISTINCT
    L.FI_LOAN_ID
    ,L.FI_COUNTRY_ID
    ,L.FI_COMPANY_ID
    ,L.FI_BUSINESS_UNIT_ID
    ,L.FI_ADMIN_CENTER_ID
    ,L.FI_ORIGINATION_CENTER_ID
    ,L.FC_PLATFORM_ID
    ,L.FC_SUB_PLATFORM_ID
    ,L.FC_CUSTOMER_ID
    ,L.FI_PRODUCT_ID
    ,L.FN_PRINCIPAL_AMOUNT
    ,L.FN_FINANCE_CHARGE_AMOUNT
    ,L.FN_PRINCIPAL_BALANCE
    ,L.FN_FINANCE_CHARGE_BALANCE
    ,L.FN_ADDITIONAL_CHARGE_BALANCE
    ,L.FD_ORIGINATION_DATE
    ,L.FD_FIRST_PAYMENT
    ,L.FD_DUE_DATE
    ,L.FN_APR
    ,L.FI_ADDITIONAL_STATUS
    ,L.FI_CURRENT_BALANCE_SEQ
    ,L.FN_INTEREST_RATE
    ,L.FI_NUMBER_OF_PAYMENTS
    ,L.FI_TERM_TYPE
    ,L.FI_LOAN_STATUS_ID
    ,L.FC_SUBPRODUCT_ID
    ,L.FI_ACCRUED_TYPE_ID
    ,L.FI_RULE_ID
    ,L.FC_END_USER
    ,L.FI_TRANSACTION
    ,L.FD_OPERATION_DATE
    ,L.FC_UUID_TRACKING
    ,L.FC_IP_ADDRESS
    ,L.FC_DEVICE
    ,LB.FI_LOAN_BALANCE_ID
    ,LB.FI_LOAN_OPERATION_ID
    ,LB.FI_BALANCE_SEQ
    ,LB.FN_PRINCIPAL_BALANCE
    ,LB.FN_FINANCE_CHARGE_BALANCE
    ,LB.FN_ADDITIONAL_CHARGE_BALANCE
  FROM
    SC_CREDIT.TA_LOAN L
  LEFT OUTER JOIN SC_CREDIT.TA_LOAN_BALANCE LB
  ON L.FI_LOAN_ID = LB.FI_LOAN_ID
  AND L.FI_CURRENT_BALANCE_SEQ = LB.FI_BALANCE_SEQ
  WHERE
    L.FC_CUSTOMER_ID = PA_CUSTOMER_ID
    AND (PA_LOAN_STATUS_ID IS NULL OR L.FI_LOAN_STATUS_ID = PA_LOAN_STATUS_ID);

  EXCEPTION
  WHEN EXC_NO_DATA_FOUND THEN
  PA_STATUS_CODE := CSG_NO_DATA_FOUND_CODE;
  PA_STATUS_MSG := CSG_NO_DATA_FOUND_MSG || '->' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
  OPEN PA_CUR_MAIN_INFO FOR
  SELECT
    NULL AS FI_LOAN_ID
    ,NULL AS FI_COUNTRY_ID
    ,NULL AS FI_COMPANY_ID
    ,NULL AS FI_BUSINESS_UNIT_ID
    ,NULL AS FC_ADMIN_CENTER_ID
    ,NULL AS FC_ORIGINATION_CENTER_ID
    ,NULL AS FC_PLATFORM_ID
    ,NULL AS FC_SUB_PLATFORM_ID
    ,NULL AS FC_CUSTOMER_ID
    ,NULL AS FI_PRODUCT_ID
    ,NULL AS FN_PRINCIPAL_AMOUNT
    ,NULL AS FN_FINANCE_CHARGE_AMOUNT
    ,NULL AS FN_PRINCIPAL_BALANCE
    ,NULL AS FN_FINANCE_CHARGE_BALANCE
    ,NULL AS FN_ADDITIONAL_CHARGE_BALANCE
    ,NULL AS FD_ORIGINATION_DATE
    ,NULL AS FD_FIRST_PAYMENT
    ,NULL AS FD_DUE_DATE
    ,NULL AS FN_APR
    ,NULL AS FI_ADDITIONAL_STATUS
    ,NULL AS FI_CURRENT_BALANCE_SEQ
    ,NULL AS FN_INTEREST_RATE
    ,NULL AS FI_NUMBER_OF_PAYMENTS
    ,NULL AS FI_TERM_TYPE
    ,NULL AS FI_LOAN_STATUS_ID
    ,NULL AS FC_SUBPRODUCT_ID
    ,NULL AS FI_ACCRUED_TYPE_ID
    ,NULL AS FI_RULE_ID
    ,NULL AS FC_END_USER
    ,NULL AS FI_TRANSACTION
    ,NULL AS FD_OPERATION_DATE
    ,NULL AS FC_UUID_TRACKING
    ,NULL AS FC_IP_ADDRESS
    ,NULL AS FC_DEVICE
    ,NULL AS FI_LOAN_BALANCE_ID
    ,NULL AS FI_LOAN_OPERATION_ID
    ,NULL AS FI_BALANCE_SEQ
    ,NULL AS FN_PRINCIPAL_BALANCE
    ,NULL AS FN_FINANCE_CHARGE_BALANCE
    ,NULL AS FN_ADDITIONAL_CHARGE_BALANCE
    FROM DUAL WHERE CSG_1 = CSG_0;
    SC_CREDIT.SP_ERROR_LOG(CSG_SP_EXE_CUSTOMER_INFO, SQLCODE, SQLERRM,
      DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, CSG_X);

  WHEN OTHERS THEN
  PA_STATUS_CODE := SQLCODE;
  PA_STATUS_MSG := SQLERRM || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
  SC_CREDIT.SP_ERROR_LOG(CSG_SP_EXE_CUSTOMER_INFO, SQLCODE, SQLERRM,
    DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, CSG_X);

  END SP_EXE_CUSTOMER_INFO;

/

  GRANT EXECUTE ON SC_CREDIT.SP_EXE_CUSTOMER_INFO TO USRNCPCREDIT1;
  
--------------------------------------------------------
--  DDL for Function FN_SEL_LOAN_INTEREST_JSON
--------------------------------------------------------

  CREATE OR REPLACE  FUNCTION SC_CREDIT.FN_SEL_LOAN_INTEREST_JSON (
   PA_LOAN_ID                IN SC_CREDIT.TA_LOAN_BALANCE.FI_LOAN_ID%TYPE
   ,PA_ADMIN_CENTER_ID       IN SC_CREDIT.TA_LOAN_BALANCE.FI_ADMIN_CENTER_ID%TYPE
   ,PA_UUID_TRACKING         IN SC_CREDIT.TA_LOAN.FC_UUID_TRACKING%TYPE)
RETURN CLOB IS
   /* **************************************************************
   * PROJECT: LOAN LIFE CIRCLE
   * DESCRIPTION: SELECT INTEREST AND CONVERT TO JSON
   * CREATED DATE: 02/01/2025
   * CREATOR: IVAN LOPEZ
   ************************************************************** */
   CSL_0                     CONSTANT SIMPLE_INTEGER := 0;
   CSL_1                     CONSTANT SIMPLE_INTEGER := 1;
   CSL_2                     CONSTANT SIMPLE_INTEGER := 2;
   CSL_FN                    CONSTANT SIMPLE_INTEGER := 1;
   VL_JSON                   CLOB := NULL;
   CSL_PAYMENT               CONSTANT VARCHAR2(8)   := 'PAYMENT';
   CSL_DUE_DATE              CONSTANT VARCHAR2(9)   := 'DUE DATE';

BEGIN

   SELECT JSON_ARRAYAGG(
             JSON_OBJECT(
                KEY 'loanId' VALUE FI_LOAN_ID,
                KEY 'adminCenterId' VALUE FI_ADMIN_CENTER_ID,
                KEY 'paymentNumberId' VALUE FI_PAYMENT_NUMBER_ID,
                KEY 'daysAcumByTerm' VALUE FI_DAYS_ACUM_BY_TERM,
                KEY 'interestBalanceDay' VALUE INTEREST_BALANCE_DAY,
                KEY 'interestDayLoan' VALUE INTEREST_DAY_LOAN ,
                KEY 'statusLoan' VALUE STATUS_LOAN ,
                KEY 'orden' VALUE ORDEN
             )
          ) AS JSON_ARRAY
   INTO
      VL_JSON
   FROM (SELECT B.FI_LOAN_ID AS FI_LOAN_ID,
                B.FI_ADMIN_CENTER_ID AS FI_ADMIN_CENTER_ID,
                NVL(A.FI_PAYMENT_NUMBER_ID,CSL_0) AS  FI_PAYMENT_NUMBER_ID,
                NVL(A.FI_DAYS_ACUM_BY_TERM,CSL_0) AS FI_DAYS_ACUM_BY_TERM,
                CASE WHEN FC_CONDITION_INTEREST IN (CSL_PAYMENT,CSL_DUE_DATE) THEN CSL_0
                   ELSE NVL(TRUNC(A.FN_ACCRUED_INTEREST_BALANCE,CSL_2),CSL_0)
                END AS INTEREST_BALANCE_DAY,
                NVL(TRUNC(A.FN_ACCRUED_INTEREST_LOAN - a.FN_PAYMENT_INTEREST,CSL_2),CSL_0) AS INTEREST_DAY_LOAN,
                NVL(B.FI_LOAN_STATUS_ID,CSL_0) AS STATUS_LOAN,
                NVL(ROW_NUMBER() OVER (PARTITION BY A.FI_LOAN_ID
                   ORDER BY A.FI_PAYMENT_NUMBER_ID DESC,A.FI_DAYS_ACUM_BY_TERM DESC),CSL_0) AS ORDEN
         FROM SC_CREDIT.TA_LOAN B
            LEFT OUTER JOIN SC_CREDIT.TA_LOAN_INTEREST A
                         ON B.FI_LOAN_ID =A.FI_LOAN_ID
                        AND B.FI_ADMIN_CENTER_ID = A.FI_ADMIN_CENTER_ID
         WHERE B.FI_LOAN_ID = PA_LOAN_ID
           AND B.FI_ADMIN_CENTER_ID = PA_ADMIN_CENTER_ID
         FETCH NEXT 1 ROW ONLY
   );

   RETURN VL_JSON;

EXCEPTION
   WHEN NO_DATA_FOUND THEN
      SC_CREDIT.SP_ERROR_LOG(
         UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_FN)
         ,SQLCODE
         ,SQLERRM
         ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
         ,PA_UUID_TRACKING
         ,NULL
         );
      RETURN NULL;
   WHEN OTHERS THEN
      SC_CREDIT.SP_ERROR_LOG(
         UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_FN)
         ,SQLCODE
         ,SQLERRM
         ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
         ,PA_UUID_TRACKING
         ,NULL
         );
      RETURN NULL;
END FN_SEL_LOAN_INTEREST_JSON;

/

  GRANT EXECUTE ON SC_CREDIT.FN_SEL_LOAN_INTEREST_JSON TO USRNCPCREDIT1;
 --------------------------------------------------------
--  DDL for Function FN_SEL_LOAN_BALANCE_DET_JSON
--------------------------------------------------------

  CREATE OR REPLACE  FUNCTION SC_CREDIT.FN_SEL_LOAN_BALANCE_DET_JSON (
   PA_LOAN_ID                IN SC_CREDIT.TA_LOAN_BALANCE.FI_LOAN_ID%TYPE
   ,PA_ADMIN_CENTER_ID       IN SC_CREDIT.TA_LOAN_BALANCE.FI_ADMIN_CENTER_ID%TYPE
   ,PA_BALANCE_SEQ           IN SC_CREDIT.TA_LOAN_BALANCE.FI_BALANCE_SEQ%TYPE
   ,PA_UUID_TRACKING         IN SC_CREDIT.TA_LOAN.FC_UUID_TRACKING%TYPE)
RETURN CLOB IS
   /* **************************************************************
   * PROJECT: PURPOSE CORE
   * DESCRIPTION: SELECT BALANCE DETAIL AND CONVERT TO JSON
   * CREATED DATE: 12/11/2024
   * CREATOR: LUIS RAMIREZ
   * MODIFICATION DATE: 26/12/2024
   * PERFORMANCE MODIFICATIONS - LUIS RAMIREZ
   ************************************************************** */
   CSL_0                     CONSTANT SIMPLE_INTEGER := 0;
   CSL_1                     CONSTANT SIMPLE_INTEGER := 1;
   CSL_FN                    CONSTANT SIMPLE_INTEGER := 1;
   VL_JSON                   CLOB := NULL;

BEGIN

   SELECT JSON_ARRAYAGG(
             JSON_OBJECT(
                KEY 'conceptId' VALUE DET.FI_LOAN_CONCEPT_ID,
                KEY 'itemAmount' VALUE DET.FN_ITEM_AMOUNT
             )
          ) AS JSON_ARRAY
   INTO
      VL_JSON
   FROM SC_CREDIT.TA_LOAN_BALANCE BA
      INNER JOIN SC_CREDIT.TA_LOAN_BALANCE_DETAIL DET
              ON DET.FI_LOAN_ID = BA.FI_LOAN_ID
             AND DET.FI_ADMIN_CENTER_ID = BA.FI_ADMIN_CENTER_ID
             AND DET.FI_LOAN_BALANCE_ID = BA.FI_LOAN_BALANCE_ID
             AND DET.FI_LOAN_CONCEPT_ID >= CSL_0
   WHERE BA.FI_ADMIN_CENTER_ID = PA_ADMIN_CENTER_ID
     AND BA.FI_LOAN_ID = PA_LOAN_ID
     AND BA.FI_BALANCE_SEQ = PA_BALANCE_SEQ;

   RETURN VL_JSON;

EXCEPTION
   WHEN NO_DATA_FOUND THEN
      SC_CREDIT.SP_ERROR_LOG(
         UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_FN)
         ,SQLCODE
         ,SQLERRM
         ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
         ,PA_UUID_TRACKING
         ,NULL
         );
      RETURN NULL;
   WHEN OTHERS THEN
      SC_CREDIT.SP_ERROR_LOG(
         UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_FN)
         ,SQLCODE
         ,SQLERRM
         ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
         ,PA_UUID_TRACKING
         ,NULL
         );
      RETURN NULL;
END FN_SEL_LOAN_BALANCE_DET_JSON;

/

  GRANT EXECUTE ON SC_CREDIT.FN_SEL_LOAN_BALANCE_DET_JSON TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.FN_SEL_LOAN_BALANCE_DET_JSON TO USRBTCCREDIT1;   
--------------------------------------------------------
--  DDL for Procedure SP_TMP_BTC_SEL_CHANGE_DEFAULT
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_TMP_BTC_SEL_CHANGE_DEFAULT 
   (PA_FIRST_CENTER_ID        IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE
   ,PA_END_CENTER_ID          IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE
   ,PA_PROCESS                IN NUMBER
   ,PA_TRACK                  IN NUMBER
   ,PA_STATUS_CODE            OUT NUMBER
   ,PA_STATUS_MSG             OUT VARCHAR2
   ,PA_CUR_SELECT             OUT SC_CREDIT.PA_TYPES.TYP_CURSOR)

   IS
      ----------------------------------------------------------------------
          -- PROJECT: LOAN LIFE CYCLE
      -- CREATOR: IVAN LOPEZ
      -- CREATED DATE:   02/01/2025
      -- DESCRIPTION: Select CHANGE DEFAULT
      -- APPLICATION:  Process Batch of Purpose TEMP
      --MODIFIED DATE:
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
          SELECT FI_LOAN_ID
               ,FI_ADMIN_CENTER_ID
               ,FC_CUSTOMER_ID
               ,FI_COUNTRY_ID
               ,FI_COMPANY_ID
               ,FI_BUSINESS_UNIT_ID
               ,FN_PRINCIPAL_BALANCE
               ,FN_FINANCE_CHARGE_BALANCE
               ,FN_ADDITIONAL_CHARGE_BALANCE
               ,FI_PRODUCT_ID
               ,FI_RULE_ID
               ,FI_LOAN_STATUS_ID
               ,FI_CURRENT_BALANCE_SEQ
               ,FC_PLATFORM_ID
               ,FC_SUB_PLATFORM_ID
               ,FI_REGISTRATION_NUMBER
               ,FI_COUNTER_DAY
               ,FI_PAYMENT_NUMBER_ID
               ,FI_ACTION_DETAIL_ID
               ,FD_INITIAL_DATE
               ,BALANCE_DET_JSON
               ,INTEREST_JSON
           FROM(
         SELECT LO.FI_LOAN_ID
               ,LO.FI_ADMIN_CENTER_ID
               ,LO.FC_CUSTOMER_ID
               ,LO.FI_COUNTRY_ID
               ,LO.FI_COMPANY_ID
               ,LO.FI_BUSINESS_UNIT_ID
               ,LO.FN_PRINCIPAL_BALANCE
               ,LO.FN_FINANCE_CHARGE_BALANCE
               ,LO.FN_ADDITIONAL_CHARGE_BALANCE
               ,LO.FI_PRODUCT_ID
               ,LO.FI_RULE_ID
               ,LO.FI_LOAN_STATUS_ID
               ,LO.FI_CURRENT_BALANCE_SEQ
               ,LO.FC_PLATFORM_ID
               ,LO.FC_SUB_PLATFORM_ID
               ,SD.FI_REGISTRATION_NUMBER
               ,SD.FI_COUNTER_DAY
               ,SD.FI_PAYMENT_NUMBER_ID
               ,SD.FI_ACTION_DETAIL_ID
               ,TO_CHAR(SD.FD_INITIAL_DATE,CSL_DATE) AS FD_INITIAL_DATE
               ,SC_CREDIT.FN_SEL_LOAN_BALANCE_DET_JSON
                  (LO.FI_LOAN_ID
                  ,LO.FI_ADMIN_CENTER_ID
                  ,LO.FI_CURRENT_BALANCE_SEQ
                  ,LO.FC_UUID_TRACKING) AS BALANCE_DET_JSON
              ,SC_CREDIT.FN_SEL_LOAN_INTEREST_JSON
                  (LO.FI_LOAN_ID
                  ,LO.FI_ADMIN_CENTER_ID
                  ,LO.FC_UUID_TRACKING) AS INTEREST_JSON
               ,ROW_NUMBER() OVER (PARTITION BY LO.FI_LOAN_ID ORDER BY SD.FI_PAYMENT_NUMBER_ID) TAB_LOAN
           FROM SC_CREDIT.TA_TMP_LOAN_PROCESS L
     INNER JOIN   SC_CREDIT.TA_LOAN LO
         ON L.FI_LOAN_ID = LO.FI_LOAN_ID
         AND L.FI_ADMIN_CENTER_ID = LO.FI_ADMIN_CENTER_ID
     INNER JOIN SC_CREDIT.TA_LOAN_STATUS_DETAIL SD
             ON SD.FI_LOAN_ID = L.FI_LOAN_ID
            AND SD.FI_ADMIN_CENTER_ID = L.FI_ADMIN_CENTER_ID
            AND FI_REGISTRATION_NUMBER  >  CSL_0
            AND SD.FI_ON_OFF = CSL_1
            AND SD.FI_ACTION_DETAIL_ID = CSL_3
          WHERE LO.FI_LOAN_STATUS_ID = CSL_3
          AND   L.FI_PROCESS = PA_PROCESS
               AND L.FI_TRACK = PA_TRACK
       )
        WHERE TAB_LOAN=1;

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
END SP_TMP_BTC_SEL_CHANGE_DEFAULT;

/

  GRANT EXECUTE ON SC_CREDIT.SP_TMP_BTC_SEL_CHANGE_DEFAULT TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_TMP_BTC_SEL_CHANGE_DEFAULT TO USRBTCCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_TMP_BTC_SEL_CHANGE_DEFAULT TO USRCREDIT02;
--------------------------------------------------------
--  DDL for Procedure SP_SEL_LOAN_PRODUCT
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_SEL_LOAN_PRODUCT (
    PA_LOAN_ID                  IN      SC_CREDIT.TA_LOAN.FI_LOAN_ID%TYPE
   ,PA_CUR_RESULTS              OUT     SC_CREDIT.PA_TYPES.TYP_CURSOR
   ,PA_STATUS_CODE              OUT     INTEGER
   ,PA_STATUS_MSG               OUT     VARCHAR2)

/* **************************************************************
* PROJECT: NCP-OUTSTANDING BALANCE
* DESCRIPTION: PROCEDURE TO SELECT THE LOAN PRODUCT AND RULE
* CREATED DATE: NOV/07/2024
* CREATOR: RICARDO HAZAEL GOMEZ ALVAREZ
************************************************************** */
AS
    CSG_ARROW                   CONSTANT VARCHAR2(5) := ' -> ';
    CSG_X                       CONSTANT VARCHAR2(5) := 'X';
    CSG_0                       CONSTANT SIMPLE_INTEGER := 0;
    CSG_SUCCESS_CODE            CONSTANT SIMPLE_INTEGER := 0;
    CSG_SUCCESS_MSG             CONSTANT VARCHAR2(10) := 'SUCCESS';
    CSG_NO_DATA_FOUND_CODE      CONSTANT SIMPLE_INTEGER := -20204;
    CSG_NO_DATA_FOUND_MSG       CONSTANT VARCHAR2(50) := 'THE DATA DOES NOT EXIST';
    CSG_SP_SEL_LOAN_PRODUCT  CONSTANT VARCHAR2(50) := 'SP_SEL_LOAN_PRODUCT';

    VL_LOAN_ID                SC_CREDIT.TA_PAYMENT_SCHEDULE.FI_LOAN_ID%TYPE;

     EXC_NO_DATA_FOUND         EXCEPTION;
     PRAGMA EXCEPTION_INIT (EXC_NO_DATA_FOUND, CSG_NO_DATA_FOUND_CODE);
BEGIN

    SELECT COUNT(*)
    INTO VL_LOAN_ID
    FROM SC_CREDIT.TA_LOAN
    WHERE FI_LOAN_ID = PA_LOAN_ID;
    IF VL_LOAN_ID = CSG_0 THEN
    RAISE EXC_NO_DATA_FOUND;
    END IF;

    OPEN PA_CUR_RESULTS FOR
        SELECT L.FI_PRODUCT_ID
              ,L.FI_RULE_ID
          FROM SC_CREDIT.TA_LOAN L
         WHERE L.FI_LOAN_ID = PA_LOAN_ID;

    PA_STATUS_CODE := CSG_SUCCESS_CODE;
    PA_STATUS_MSG := CSG_SUCCESS_MSG;

EXCEPTION
        WHEN EXC_NO_DATA_FOUND THEN
        PA_STATUS_CODE := CSG_NO_DATA_FOUND_CODE;
        PA_STATUS_MSG := CSG_NO_DATA_FOUND_MSG || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
        OPEN PA_CUR_RESULTS FOR
            SELECT
            NULL FI_PRODUCT_ID
            ,NULL FI_RULE_ID
            FROM DUAL;

        WHEN OTHERS THEN
        PA_STATUS_CODE := SQLCODE;
        PA_STATUS_MSG := SQLERRM || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
        OPEN PA_CUR_RESULTS FOR
            SELECT
            NULL FI_PRODUCT_ID
            ,NULL FI_RULE_ID
            FROM DUAL;
        SC_CREDIT.SP_ERROR_LOG(CSG_SP_SEL_LOAN_PRODUCT, SQLCODE, SQLERRM,
        DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, CSG_X);
END SP_SEL_LOAN_PRODUCT;

/

  GRANT EXECUTE ON SC_CREDIT.SP_SEL_LOAN_PRODUCT TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_SEL_LOAN_PRODUCT TO USRPURPOSEWS;
--------------------------------------------------------
--  DDL for Procedure SP_BTC_UPD_LOAN
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_BTC_UPD_LOAN 
   (PA_LOAN_ID            IN NUMBER
   ,PA_ADMIN_CENTER_ID    IN NUMBER
   ,PA_LOAN_STATUS_ID     IN NUMBER
   ,PA_COMMIT             IN NUMBER
   ,PA_STATUS_CODE        OUT NUMBER
   ,PA_STATUS_MSG         OUT VARCHAR2)
IS
   /* **************************************************************
   * DESCRIPTION: PROCESS TO UPDATE TABLE TA_LOAN
   * CREATED DATE: 19/11/2024
   * CREATOR: CRISTHIAN MORALES
   ************************************************************** */
    CSL_0                         CONSTANT SIMPLE_INTEGER := 0;
    CSL_1                         CONSTANT SIMPLE_INTEGER := 1;
    CSL_SP                        CONSTANT SIMPLE_INTEGER := 1;
    CSL_2002                      CONSTANT SIMPLE_INTEGER := -20002;
    CSL_ARROW                     CONSTANT VARCHAR2(20)   := '->';
    CSL_MSG_SUCCESS               CONSTANT VARCHAR2(20)   := 'SUCCESS';
    CSL_NOT_UPDATED               CONSTANT VARCHAR2(20)   := ' NOT_UPDATE ';
    CSL_TA_LOAN                   CONSTANT VARCHAR2(20)   := ' LOAN ';
    CSL_ADMIN_CENTER_ID           CONSTANT VARCHAR2(20)   := 'ADMIN_CENTER_ID';
BEGIN
   PA_STATUS_CODE := CSL_0;
   PA_STATUS_MSG  := CSL_MSG_SUCCESS;

   UPDATE SC_CREDIT.TA_LOAN LO
     SET LO.FD_LOAN_STATUS_DATE = SYSDATE
        ,LO.FD_MODIFICATION_DATE = SYSDATE
        ,LO.FI_LOAN_STATUS_ID=PA_LOAN_STATUS_ID
     WHERE LO.FI_LOAN_ID=PA_LOAN_ID
     AND LO.FI_ADMIN_CENTER_ID=PA_ADMIN_CENTER_ID;

    IF SQL%ROWCOUNT = CSL_0 THEN
     RAISE_APPLICATION_ERROR(CSL_2002, CSL_NOT_UPDATED || CSL_TA_LOAN);
    END IF;

    IF PA_COMMIT = CSL_1 THEN
      COMMIT;
    END IF;
EXCEPTION
 WHEN OTHERS THEN
    ROLLBACK;
    PA_STATUS_CODE := SQLCODE;
    PA_STATUS_MSG := SQLERRM || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;

    SC_CREDIT.SP_BATCH_ERROR_LOG(UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP)
                                ,SQLCODE
                                ,SQLERRM
                                ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                ,CSL_1
                                ,NULL
                                ||CSL_ADMIN_CENTER_ID ||PA_ADMIN_CENTER_ID);
END SP_BTC_UPD_LOAN;

/

  GRANT EXECUTE ON SC_CREDIT.SP_BTC_UPD_LOAN TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_BTC_UPD_LOAN TO USRBTCCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_SYNC_SUBTERM_TYPE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_SYNC_SUBTERM_TYPE (
    PA_SYNC_JSON   CLOB
    ,PA_UPDATED_ROWS OUT NUMBER
    ,PA_STATUS_CODE OUT NUMBER
    ,PA_STATUS_MSG  OUT VARCHAR2)
  AS
/*************************************************************
	* PROJECT : NCP-LOAN DESIGNER PARAMETRIA
	* DESCRIPTION: PACKAGE ASYNC CATALOGS
	* CREATOR: JOSE DE JESUS BRAVO AGUILAR
	* CREATED DATE: 2025-01-21
	* MODIFICATION: 2025-01-21
	* [NCPACS-4796 V1] UPDATE OR INSERT CATALOG
************************************************************** */
  BEGIN
    PA_STATUS_CODE := 0;
    PA_STATUS_MSG  := 'OK';

  MERGE INTO SC_CREDIT.TC_SUBTERM_TYPE A
    USING (
    SELECT
      *
    FROM
      JSON_TABLE ( PA_SYNC_JSON, '$.subtermTypes[*]'
        COLUMNS (
          ID                    NUMBER PATH '$.id'
          ,DESCRIPTION          VARCHAR2 ( 50 ) PATH '$.description'
          ,STATUS               NUMBER PATH '$.status'
          ,USER_NAME            VARCHAR2 ( 50 ) PATH '$.user'
          ,CREATED_DATE         TIMESTAMP PATH '$.createdDate'
          ,MODIFICATION_DATE    TIMESTAMP PATH '$.modificationDate')))
          B ON ( A.FI_SUBTERM_TYPE_ID = B.ID)
  WHEN MATCHED THEN UPDATE
    SET
      A.FC_SUBTERM_TYPE_DESC = B.DESCRIPTION
      ,A.FI_STATUS = B.STATUS
      ,A.FC_USER = B.USER_NAME
      ,A.FD_MODIFICATION_DATE = CAST(B.MODIFICATION_DATE AS DATE)
  WHEN NOT MATCHED THEN
    INSERT (
      FI_SUBTERM_TYPE_ID
      ,FC_SUBTERM_TYPE_DESC
      ,FI_STATUS
      ,FC_USER
      ,FD_CREATED_DATE
      ,FD_MODIFICATION_DATE)
    VALUES (
      B.ID
      ,B.DESCRIPTION
      ,B.STATUS
      ,B.USER_NAME
      ,B.CREATED_DATE
      ,B.MODIFICATION_DATE);

      PA_UPDATED_ROWS := SQL%ROWCOUNT;

  COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      PA_STATUS_CODE := SQLCODE;
      PA_STATUS_MSG  := SQLERRM;
      SC_CREDIT.SP_ERROR_LOG('SP_SYNC_SUBTERM_TYPE', SQLCODE, SQLERRM,
        DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, NULL, '');
  END SP_SYNC_SUBTERM_TYPE;

/

--------------------------------------------------------
--  DDL for Procedure SP_INS_LOAN_REF_PAYMEN
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_INS_LOAN_REF_PAYMEN (
    PA_LOAN_ID               IN SC_CREDIT.TA_LOAN_OPERATION_REF_PAYMENT.FI_LOAN_ID%TYPE,
    PA_ADMIN_CENTER_ID       IN SC_CREDIT.TA_LOAN_OPERATION_REF_PAYMENT.FI_ADMIN_CENTER_ID%TYPE,
    PA_OPERATION_REF_ID      IN SC_CREDIT.TA_LOAN_OPERATION_REF_PAYMENT.FI_OPERATION_REF_ID%TYPE,
    PA_REFERENCE_ID          IN SC_CREDIT.TA_LOAN_OPERATION_REF_PAYMENT.FC_REFERENCE_ID%TYPE,
    PA_USER                  IN SC_CREDIT.TA_LOAN_OPERATION_REF_PAYMENT.FC_USER%TYPE,
    PA_IP_ADDRESS            IN SC_CREDIT.TA_LOAN_OPERATION_REF_PAYMENT.FC_IP_ADDRESS%TYPE,
    PA_UUID_TRACKING         IN SC_CREDIT.TA_LOAN_OPERATION_REF_PAYMENT.FC_UUID_TRACKING%TYPE,
    PA_STATUS_CODE           OUT NUMBER,
    PA_STATUS_MSG            OUT VARCHAR2
)
IS
 /* **********************************************************************
 * PROJECT: CORE LOAN
 * DESCRIPTION: DESCRIPTION: PROCEDURE TO SAVE INFORMATION FROM THE
                TA_LOAN_OPERATION_REF_PAYMENT TABLE WHEN IT HAS A REFERENCE.
 * PRECONDITIONS: PRE-EXISTING LOANS AND OPERATIONS
 * CREATED DATE: 07/01/2025
 * CREATOR: CESAR SANCHEZ HERNANDEZ
 * MODIFICATION: 2025-01-15 CESAR SANCHEZ HERNANDEZ
 * [NCPRDC-5152 V2.0.0]
 ***********************************************************************/
  CSL_0            CONSTANT SIMPLE_INTEGER := 0;
  CSL_1            CONSTANT SIMPLE_INTEGER := 1;
  CSL_204          CONSTANT SIMPLE_INTEGER := 204;
  CSL_409          CONSTANT SIMPLE_INTEGER := 409;
  CSL_ARROW        CONSTANT VARCHAR2(5) := '->';
  CSL_JSON         CONSTANT VARCHAR2(5) := NULL;
  CSL_SUCCESS      CONSTANT VARCHAR2(8) := 'SUCCESS';
  CSL_NO_INSERTION CONSTANT VARCHAR2(55) := 'NO DATA INSERTED';
  CSL_DUPLICATE    CONSTANT VARCHAR2(50) := 'RECORD ALREADY EXISTS';
  CSL_SP           CONSTANT SIMPLE_INTEGER := 1;
  VL_EXISTE        NUMBER := 0;

BEGIN
SELECT COUNT(1)
INTO VL_EXISTE
FROM SC_CREDIT.TA_LOAN_OPERATION_REF_PAYMENT
WHERE FI_OPERATION_REF_ID = PA_OPERATION_REF_ID
  AND FC_REFERENCE_ID = PA_REFERENCE_ID;

IF VL_EXISTE > 0 THEN
        PA_STATUS_CODE := CSL_409;
        PA_STATUS_MSG := CSL_DUPLICATE;
ROLLBACK;
RETURN;
END IF;

INSERT INTO SC_CREDIT.TA_LOAN_OPERATION_REF_PAYMENT(
    FI_LOAN_OPERATION_REF_PAYMENT_ID,
    FI_LOAN_ID,
    FI_ADMIN_CENTER_ID,
    FI_OPERATION_REF_ID,
    FC_REFERENCE_ID,
    FC_USER,
    FC_IP_ADDRESS,
    FD_CREATED_DATE,
    FD_MODIFICATION_DATE,
    FC_UUID_TRACKING)
VALUES(
          SC_CREDIT.SE_LOAN_OPERATION_REF_PAYMENT_ID.NEXTVAL,
          PA_LOAN_ID,
          PA_ADMIN_CENTER_ID,
          PA_OPERATION_REF_ID,
          PA_REFERENCE_ID,
          PA_USER,
          PA_IP_ADDRESS,
          SYSDATE,
          SYSDATE,
          PA_UUID_TRACKING);


COMMIT;
PA_STATUS_CODE := CSL_0;
        PA_STATUS_MSG  := CSL_SUCCESS;


EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    PA_STATUS_CODE := CSL_204;
    PA_STATUS_MSG := CSL_NO_INSERTION;
    PA_STATUS_CODE := SQLCODE;
    PA_STATUS_MSG := SQLERRM || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    SC_CREDIT.SP_ERROR_LOG(
       UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP)
       ,SQLCODE
       ,SQLERRM
       ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
       ,PA_UUID_TRACKING
       ,CSL_JSON
       );

END SP_INS_LOAN_REF_PAYMEN;

/

  GRANT EXECUTE ON SC_CREDIT.SP_INS_LOAN_REF_PAYMEN TO USRNCPCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_BTC_INS_LOAN_STATUS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_BTC_INS_LOAN_STATUS 
  (
    PA_LOAN_ID                IN  NUMBER
   ,PA_ADMIN_CENTER_ID        IN  NUMBER
   ,PA_LOAN_OPERATION_ID      IN  NUMBER
   ,PA_LOAN_STATUS_ID         IN  NUMBER
   ,PA_LOAN_STATUS_OLD_ID     IN  NUMBER
   ,PA_TRIGGER_ID             IN  NUMBER
   ,PA_LOAN_STATUS_DATE       IN  VARCHAR2
   ,PA_COMMIT                 IN  NUMBER
   ,PA_TRANSACTION         IN  NUMBER
   ,PA_STATUS_CODE            OUT NUMBER
   ,PA_STATUS_MSG             OUT VARCHAR2
   )
   IS
   /* **************************************************************
   * DESCRIPTION: PROCESS TO INSERT IN TABLE TA_LOAN_STATUS
   * CREATED DATE: 12/11/2024
   * CREATOR: CRISTHIAN MORALES
   ************************************************************** */
   CSL_0                 CONSTANT SIMPLE_INTEGER := 0;
   CSL_1                 CONSTANT SIMPLE_INTEGER := 1;
   CSL_SP                CONSTANT SIMPLE_INTEGER := 1;
   CSL_2002              CONSTANT SIMPLE_INTEGER := -20002;
   CSL_DATE_FORMAT       CONSTANT VARCHAR2(40)   := 'MM/DD/YYYY hh24:mi:ss';
   CSL_MSG_SUCCESS        CONSTANT VARCHAR2(20)   := 'SUCCESS';
   CSL_ARROW             CONSTANT VARCHAR2(5)    := '->';
   CSL_TA_LOAN_STATUS    CONSTANT VARCHAR2(20)   := ' LOAN_STATUS_DETAIL';
   CSL_NOT_INSERT        CONSTANT VARCHAR2(50)   := 'NOT_INSERT';
   CSL_ADMIN_CENTER_ID   CONSTANT VARCHAR2(20)   := 'ADMIN_CENTER_ID';

   BEGIN

     PA_STATUS_CODE:= CSL_0;
     PA_STATUS_MSG:=CSL_MSG_SUCCESS;

   INSERT INTO SC_CREDIT.TA_LOAN_STATUS
             (FI_LOAN_ID
             ,FI_ADMIN_CENTER_ID
             ,FI_LOAN_OPERATION_ID
             ,FI_LOAN_STATUS_ID
             ,FI_LOAN_STATUS_OLD_ID
             ,FI_TRIGGER_ID
             ,FD_LOAN_STATUS_DATE
             ,FC_USER
             ,FD_CREATED_DATE
             ,FD_MODIFICATION_DATE
             )
       VALUES(PA_LOAN_ID
             ,PA_ADMIN_CENTER_ID
             ,PA_LOAN_OPERATION_ID
             ,PA_LOAN_STATUS_ID
             ,PA_LOAN_STATUS_OLD_ID
             ,PA_TRIGGER_ID
             ,TO_DATE(PA_LOAN_STATUS_DATE,CSL_DATE_FORMAT)
             ,USER
             ,SYSDATE
             ,SYSDATE
             );

   IF SQL%ROWCOUNT = CSL_0 THEN
      RAISE_APPLICATION_ERROR(CSL_2002, CSL_TA_LOAN_STATUS || CSL_NOT_INSERT);
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
                                  ,PA_TRANSACTION
                                  ,NULL
                                  ||CSL_ADMIN_CENTER_ID ||PA_ADMIN_CENTER_ID);

  END SP_BTC_INS_LOAN_STATUS;

/

  GRANT EXECUTE ON SC_CREDIT.SP_BTC_INS_LOAN_STATUS TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_BTC_INS_LOAN_STATUS TO USRBTCCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_BTC_INS_DAILY_INTEREST
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_BTC_INS_DAILY_INTEREST (
    PA_FI_LOAN_ID                   IN SC_CREDIT.TA_LOAN_INTEREST.FI_LOAN_ID%TYPE
   ,PA_FI_ADMIN_CENTER_ID           IN SC_CREDIT.TA_LOAN_INTEREST.FI_ADMIN_CENTER_ID%TYPE
   ,PA_FI_PAYMENT_NUMBER_ID         IN SC_CREDIT.TA_LOAN_INTEREST.FI_PAYMENT_NUMBER_ID%TYPE
   ,PA_FI_DAYS_ACUM_BY_TERM         IN SC_CREDIT.TA_LOAN_INTEREST.FI_DAYS_ACUM_BY_TERM%TYPE
   ,PA_FN_DAILY_INTEREST            IN SC_CREDIT.TA_LOAN_INTEREST.FN_DAILY_INTEREST%TYPE
   ,PA_FN_ACCRUED_INTEREST_BALANCE  IN SC_CREDIT.TA_LOAN_INTEREST.FN_ACCRUED_INTEREST_BALANCE%TYPE
   ,PA_FN_ACCRUED_INTEREST_LOAN     IN SC_CREDIT.TA_LOAN_INTEREST.FN_ACCRUED_INTEREST_LOAN%TYPE
   ,PA_FN_PAYMENT_INTEREST          IN SC_CREDIT.TA_LOAN_INTEREST.FI_PAYMENT_NUMBER_ID%TYPE
   ,PA_FC_CONDITION_INTEREST        IN SC_CREDIT.TA_LOAN_INTEREST.FC_CONDITION_INTEREST%TYPE
   ,PA_FD_OPERATION_DATE            IN VARCHAR2
   ,PA_FD_APPLICATION_DATE          IN VARCHAR2
   ,PA_FI_TRANSACTION               IN NUMBER
   ,PA_COMMIT                       IN NUMBER
   ,PA_STATUS_CODE                  OUT NUMBER
   ,PA_STATUS_MSG                   OUT VARCHAR2
   )
   IS

   /* **************************************************************
   * PROJECT: LOAN LIFE CYCLE
   * DESCRIPTION: V3 - PROCESS TO APPLY DAILY INTEREST
   * CREATED DATE: 10/24/2024
   * CREATOR: CRISTHIAN MORALES
   * MODIFICATION DATE: 12/23/2024
   * PERFORMANCE MODIFICATIONS - VICTOR GARCIA
   ************************************************************** */

   --CONSTANT
   CSL_ARROW             CONSTANT VARCHAR2(5)   := '->';
   CSL_0                 CONSTANT SIMPLE_INTEGER := 0;
   CSL_1                 CONSTANT SIMPLE_INTEGER := 1;
   CSL_SP                CONSTANT SIMPLE_INTEGER := 1;
   CSL_20002              CONSTANT SIMPLE_INTEGER := -20002;
   CSL_MSG_SUCCESS       CONSTANT VARCHAR2(10)   := 'SUCCESS';
   CSL_TA_LOAN_INTEREST  CONSTANT VARCHAR2(20)   := ' LOAN INTEREST ';
   CSL_COMMA             CONSTANT VARCHAR2(5) := ' , ';
   CSL_LOANID            CONSTANT VARCHAR2(10) := 'LOANID: ';
   CSL_ADMINCENTERID     CONSTANT VARCHAR2(20) := 'ADMINCENTERID: ';
   CSL_DATE_FORMAT       CONSTANT VARCHAR2(30) := 'MM/DD/YYYY hh24:mi:ss';

   BEGIN
      PA_STATUS_CODE:= CSL_0;
      PA_STATUS_MSG := CSL_MSG_SUCCESS;

      INSERT INTO SC_CREDIT.TA_LOAN_INTEREST
               (FI_LOAN_ID
               ,FI_ADMIN_CENTER_ID
               ,FI_PAYMENT_NUMBER_ID
               ,FI_DAYS_ACUM_BY_TERM
               ,FN_DAILY_INTEREST
               ,FN_ACCRUED_INTEREST_BALANCE
               ,FN_ACCRUED_INTEREST_LOAN
               ,FN_PAYMENT_INTEREST
               ,FC_CONDITION_INTEREST
               ,FD_OPERATION_DATE
               ,FD_APPLICATION_DATE
               ,FC_USER
               ,FD_CREATED_DATE
               ,FD_MODIFICATION_DATE)
         VALUES(
                PA_FI_LOAN_ID
               ,PA_FI_ADMIN_CENTER_ID
               ,PA_FI_PAYMENT_NUMBER_ID
               ,PA_FI_DAYS_ACUM_BY_TERM
               ,PA_FN_DAILY_INTEREST
               ,PA_FN_ACCRUED_INTEREST_BALANCE
               ,PA_FN_ACCRUED_INTEREST_LOAN
               ,PA_FN_PAYMENT_INTEREST
               ,PA_FC_CONDITION_INTEREST
               ,TO_DATE(PA_FD_OPERATION_DATE,CSL_DATE_FORMAT)
               ,TO_DATE(PA_FD_APPLICATION_DATE,CSL_DATE_FORMAT)
               ,USER
               ,SYSDATE
               ,SYSDATE
               );

      IF SQL%ROWCOUNT = CSL_0 THEN
         RAISE_APPLICATION_ERROR(CSL_20002, CSL_TA_LOAN_INTEREST || CSL_TA_LOAN_INTEREST);
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
                                    ,PA_FI_TRANSACTION
                                    ,CSL_LOANID || PA_FI_LOAN_ID || CSL_COMMA
                                       ||CSL_ADMINCENTERID ||PA_FI_ADMIN_CENTER_ID|| CSL_COMMA
                                       ||PA_FI_PAYMENT_NUMBER_ID|| CSL_COMMA
                                       ||PA_FI_DAYS_ACUM_BY_TERM|| CSL_COMMA
                                       ||PA_FD_APPLICATION_DATE|| CSL_COMMA
           );

     END SP_BTC_INS_DAILY_INTEREST;

/

  GRANT EXECUTE ON SC_CREDIT.SP_BTC_INS_DAILY_INTEREST TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_BTC_INS_DAILY_INTEREST TO USRBTCCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_BTC_INS_LOAN_STATUS_DETAIL
--------------------------------------------------------
set define off;

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

  GRANT EXECUTE ON SC_CREDIT.SP_BTC_INS_LOAN_STATUS_DETAIL TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_BTC_INS_LOAN_STATUS_DETAIL TO USRBTCCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_BTC_EXE_OPERATION_BALANCE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_BTC_EXE_OPERATION_BALANCE 
   (PREC_LOANS                     IN SC_CREDIT.TYP_REC_BTC_LOAN
   ,PTAB_OPERATIONS                IN SC_CREDIT.TYP_TAB_BTC_OPERATION
   ,PTAB_OPERATIONS_DETAIL         IN SC_CREDIT.TYP_TAB_BTC_DETAIL
   ,PTAB_BALANCES                  IN SC_CREDIT.TYP_TAB_BTC_BALANCE
   ,PTAB_BALANCES_DETAIL           IN SC_CREDIT.TYP_TAB_BTC_DETAIL
   ,PA_DEVICE                      IN SC_CREDIT.TA_LOAN_OPERATION.FC_DEVICE%TYPE
   ,PA_GPS_LATITUDE                IN SC_CREDIT.TA_LOAN_OPERATION.FC_GPS_LATITUDE%TYPE
   ,PA_GPS_LONGITUDE               IN SC_CREDIT.TA_LOAN_OPERATION.FC_GPS_LONGITUDE%TYPE
   ,PA_COMMIT                      IN NUMBER
   ,PA_STATUS_CODE                 OUT NUMBER
   ,PA_STATUS_MSG                  OUT VARCHAR2)
IS
   ----------------------------------------------------------------------
   -- PROJECT: LOAN LIFE CYCLE
   -- CREATOR: Eduardo Cervantes Hernandez
   -- CREATED DATE:   24/10/2024
   -- DESCRIPTION: Insert Operation
   -- APPLICATION:  Process Batch of Purpose
   -- MODIFICATION DATE: 26/12/2024
   -- PERFORMANCE MODIFICATIONS - LUIS RAMIREZ
   ----------------------------------------------------------------------
   --CONSTANTS
   CSL_0                              CONSTANT SIMPLE_INTEGER := 0;
   CSL_1                              CONSTANT SIMPLE_INTEGER := 1;
   CSL_SP                             CONSTANT SIMPLE_INTEGER := 1;

   --CONSTANTS SUCCESS
   CSL_SUCCESS_CODE                   CONSTANT SIMPLE_INTEGER := 0;
   CSL_SUCCESS_MSG                    CONSTANT VARCHAR2(7) := 'SUCCESS';
   CSL_DATE_FORMAT                    CONSTANT VARCHAR2(21) := 'MM/DD/YYYY hh24:mi:ss';

   --CONSTANTS ERRORS
   CSL_CODE_ERROR                     CONSTANT SIMPLE_INTEGER := -20012;

    --CONSTANTS MESSAGES
   CSL_NOT_UPDATED                    CONSTANT VARCHAR2(15) := 'NOT UPDATED - ';
   CSL_NOT_INSERT                     CONSTANT VARCHAR2(13) := 'NOT INSERT - ';
   CSL_TA_LOAN                        CONSTANT VARCHAR2(7) := 'TA_LOAN';
   CSL_TA_LOAN_OPERATION              CONSTANT VARCHAR2(17) := 'TA_LOAN_OPERATION';
   CSL_TA_LOAN_OPERATION_DETAIL       CONSTANT VARCHAR2(24) := 'TA_LOAN_OPERATION_DETAIL';
   CSL_TA_LOAN_BALANCE                CONSTANT VARCHAR2(15) := 'TA_LOAN_BALANCE';
   CSL_TA_LOAN_BALANCE_DETAIL         CONSTANT VARCHAR2(22) := 'TA_LOAN_BALANCE_DETAIL';
   CSL_ERROR_LOAN                     CONSTANT VARCHAR2(17) := 'Loan is not found';
   CSL_ERROR_SEQ                      CONSTANT VARCHAR2(35) := 'I cant update same balance sequence';
   CSL_ARROW                          CONSTANT VARCHAR2(5) := ' -> ';
   CSL_COMMA                          CONSTANT VARCHAR2(5) := ' , ';

   --VARIABLES
   VL_CURRENT_BALANCE_SEQ             SC_CREDIT.TA_LOAN.FI_CURRENT_BALANCE_SEQ%TYPE := 0;
   VL_TRANSACTION                     SC_CREDIT.TA_LOAN_OPERATION.FI_TRANSACTION%TYPE := 0;

   --EXCEPTIONS
   EXC_BULK_ERRORS EXCEPTION;
   PRAGMA EXCEPTION_INIT(EXC_BULK_ERRORS, -24381);

BEGIN
   PA_STATUS_CODE := CSL_SUCCESS_CODE;
   PA_STATUS_MSG := CSL_SUCCESS_MSG;
   VL_TRANSACTION := NVL(PREC_LOANS.FI_TRANSACTION, CSL_0);

   <<selectLoan>>
   BEGIN
      SELECT LO.FI_CURRENT_BALANCE_SEQ  AS FI_CURRENT_BALANCE_SEQ
        INTO VL_CURRENT_BALANCE_SEQ
        FROM SC_CREDIT.TA_LOAN LO
       WHERE LO.FI_LOAN_ID = PREC_LOANS.FI_LOAN_ID
         AND LO.FI_ADMIN_CENTER_ID = PREC_LOANS.FI_ADMIN_CENTER_ID;
   EXCEPTION
      WHEN NO_DATA_FOUND THEN
         RAISE_APPLICATION_ERROR(CSL_CODE_ERROR, CSL_ERROR_LOAN);
   END;

   --Validate Sequence
   IF VL_CURRENT_BALANCE_SEQ >= PREC_LOANS.FI_CURRENT_BALANCE_SEQ THEN
      RAISE_APPLICATION_ERROR(CSL_CODE_ERROR, CSL_ERROR_SEQ);
   END IF;

   --Update in the table TA_LOAN
   UPDATE SC_CREDIT.TA_LOAN LO
      SET LO.FN_PRINCIPAL_BALANCE = NVL(PREC_LOANS.FN_PRINCIPAL_BALANCE,LO.FN_PRINCIPAL_BALANCE)
         ,LO.FN_FINANCE_CHARGE_BALANCE = NVL(PREC_LOANS.FN_FINANCE_CHARGE_BALANCE,LO.FN_FINANCE_CHARGE_BALANCE)
         ,LO.FN_ADDITIONAL_CHARGE_BALANCE = NVL(PREC_LOANS.FN_ADDITIONAL_CHARGE_BALANCE,LO.FN_ADDITIONAL_CHARGE_BALANCE)
         ,LO.FI_ADDITIONAL_STATUS = NVL(PREC_LOANS.FI_ADDITIONAL_STATUS,LO.FI_ADDITIONAL_STATUS)
         ,LO.FI_CURRENT_BALANCE_SEQ = PREC_LOANS.FI_CURRENT_BALANCE_SEQ
         ,LO.FI_LOAN_STATUS_ID = NVL(PREC_LOANS.FI_LOAN_STATUS_ID,LO.FI_LOAN_STATUS_ID)
         ,LO.FD_LOAN_STATUS_DATE = NVL(TO_DATE(PREC_LOANS.FC_LOAN_STATUS_DATE, CSL_DATE_FORMAT),LO.FD_LOAN_STATUS_DATE)
         ,LO.FC_USER = USER
         ,LO.FD_MODIFICATION_DATE = SYSDATE
   WHERE LO.FI_LOAN_ID = PREC_LOANS.FI_LOAN_ID
     AND LO.FI_ADMIN_CENTER_ID = PREC_LOANS.FI_ADMIN_CENTER_ID;

   IF SQL%ROWCOUNT = CSL_0 THEN
      RAISE_APPLICATION_ERROR(CSL_CODE_ERROR, CSL_NOT_UPDATED || CSL_TA_LOAN);
   END IF;

   --Insert in OPERATION
   FORALL B IN PTAB_OPERATIONS.FIRST .. PTAB_OPERATIONS.LAST SAVE EXCEPTIONS
      INSERT INTO SC_CREDIT.TA_LOAN_OPERATION
         (FI_LOAN_OPERATION_ID
         ,FI_COUNTRY_ID
         ,FI_COMPANY_ID
         ,FI_BUSINESS_UNIT_ID
         ,FI_LOAN_ID
         ,FI_ADMIN_CENTER_ID
         ,FI_OPERATION_TYPE_ID
         ,FI_TRANSACTION
         ,FN_OPERATION_AMOUNT
         ,FD_APPLICATION_DATE
         ,FD_OPERATION_DATE
         ,FI_STATUS
         ,FC_END_USER
         ,FC_UUID_TRACKING
         ,FC_GPS_LATITUDE
         ,FC_GPS_LONGITUDE
         ,FC_DEVICE
         ,FC_USER
         ,FD_CREATED_DATE
         ,FD_MODIFICATION_DATE)
      VALUES (
         PTAB_OPERATIONS(B).FI_LOAN_OPERATION_ID
         ,PTAB_OPERATIONS(B).FI_COUNTRY_ID
         ,PTAB_OPERATIONS(B).FI_COMPANY_ID
         ,PTAB_OPERATIONS(B).FI_BUSINESS_UNIT_ID
         ,PTAB_OPERATIONS(B).FI_LOAN_ID
         ,PTAB_OPERATIONS(B).FI_ADMIN_CENTER_ID
         ,PTAB_OPERATIONS(B).FI_OPERATION_TYPE_ID
         ,VL_TRANSACTION
         ,PTAB_OPERATIONS(B).FN_OPERATION_AMOUNT
         ,TO_DATE(PTAB_OPERATIONS(B).FC_APPLICATION_DATE,CSL_DATE_FORMAT)
         ,TO_DATE(PTAB_OPERATIONS(B).FC_OPERATION_DATE,CSL_DATE_FORMAT)
         ,PTAB_OPERATIONS(B).FI_STATUS
         ,PTAB_OPERATIONS(B).FC_END_USER
         ,PTAB_OPERATIONS(B).FC_UUID_TRACKING
         ,PA_GPS_LATITUDE
         ,PA_GPS_LONGITUDE
         ,PA_DEVICE
         ,USER
         ,SYSDATE
         ,SYSDATE
      );
   IF SQL%ROWCOUNT = CSL_0 THEN
      RAISE_APPLICATION_ERROR(CSL_CODE_ERROR, CSL_NOT_INSERT || CSL_TA_LOAN_OPERATION);
   END IF;

   --Insert detail in the table OPERATION DETAIL
   FORALL C IN PTAB_OPERATIONS_DETAIL.FIRST .. PTAB_OPERATIONS_DETAIL.LAST SAVE EXCEPTIONS
   INSERT INTO SC_CREDIT.TA_LOAN_OPERATION_DETAIL
      (FI_LOAN_OPERATION_ID
      ,FI_ADMIN_CENTER_ID
      ,FI_LOAN_ID
      ,FI_LOAN_CONCEPT_ID
      ,FN_ITEM_AMOUNT
      ,FC_USER
      ,FD_CREATED_DATE
      ,FD_MODIFICATION_DATE)
   VALUES(
      PTAB_OPERATIONS_DETAIL(C).FI_LOAN_OPERATION_ID
      ,PTAB_OPERATIONS_DETAIL(C).FI_ADMIN_CENTER_ID
      ,PTAB_OPERATIONS_DETAIL(C).FI_LOAN_ID
      ,PTAB_OPERATIONS_DETAIL(C).FI_LOAN_CONCEPT_ID
      ,PTAB_OPERATIONS_DETAIL(C).FN_ITEM_AMOUNT
      ,USER
      ,SYSDATE
      ,SYSDATE
   );
   IF SQL%ROWCOUNT = CSL_0 THEN
      RAISE_APPLICATION_ERROR(CSL_CODE_ERROR, CSL_NOT_INSERT || CSL_TA_LOAN_OPERATION_DETAIL);
   END IF;

   --Insert balance in the table LOAN_BALANCE
   FORALL D IN PTAB_BALANCES.FIRST .. PTAB_BALANCES.LAST SAVE EXCEPTIONS
   INSERT INTO SC_CREDIT.TA_LOAN_BALANCE
      (FI_LOAN_BALANCE_ID
      ,FI_ADMIN_CENTER_ID
      ,FI_LOAN_ID
      ,FI_LOAN_OPERATION_ID
      ,FI_BALANCE_SEQ
      ,FN_PRINCIPAL_BALANCE
      ,FN_FINANCE_CHARGE_BALANCE
      ,FN_ADDITIONAL_CHARGE_BALANCE
      ,FC_USER
      ,FD_CREATED_DATE
      ,FD_MODIFICATION_DATE)
   VALUES (
      PTAB_BALANCES(D).FI_LOAN_BALANCE_ID
      ,PTAB_BALANCES(D).FI_ADMIN_CENTER_ID
      ,PTAB_BALANCES(D).FI_LOAN_ID
      ,PTAB_BALANCES(D).FI_LOAN_OPERATION_ID
      ,PTAB_BALANCES(D).FI_BALANCE_SEQ
      ,PTAB_BALANCES(D).FN_PRINCIPAL_BALANCE
      ,PTAB_BALANCES(D).FN_FINANCE_CHARGE_BALANCE
      ,PTAB_BALANCES(D).FN_ADDITIONAL_CHARGE_BALANCE
      ,USER
      ,SYSDATE
      ,SYSDATE
   );
   IF SQL%ROWCOUNT = CSL_0 THEN
      RAISE_APPLICATION_ERROR(CSL_CODE_ERROR, CSL_NOT_INSERT || CSL_TA_LOAN_BALANCE);
   END IF;

   --Insert the detail of balance in the table LOAN_BALANCE
   FORALL E IN PTAB_BALANCES_DETAIL.FIRST .. PTAB_BALANCES_DETAIL.LAST SAVE EXCEPTIONS
   INSERT INTO SC_CREDIT.TA_LOAN_BALANCE_DETAIL
      (FI_LOAN_BALANCE_ID
      ,FI_ADMIN_CENTER_ID
      ,FI_LOAN_ID
      ,FI_LOAN_CONCEPT_ID
      ,FN_ITEM_AMOUNT
      ,FC_USER
      ,FD_CREATED_DATE
      ,FD_MODIFICATION_DATE)
   VALUES(
       PTAB_BALANCES_DETAIL(E).FI_LOAN_OPERATION_ID
      ,PTAB_BALANCES_DETAIL(E).FI_ADMIN_CENTER_ID
      ,PTAB_BALANCES_DETAIL(E).FI_LOAN_ID
      ,PTAB_BALANCES_DETAIL(E).FI_LOAN_CONCEPT_ID
      ,PTAB_BALANCES_DETAIL(E).FN_ITEM_AMOUNT
      ,USER
      ,SYSDATE
      ,SYSDATE
      );
   IF SQL%ROWCOUNT = CSL_0 THEN
      RAISE_APPLICATION_ERROR(CSL_CODE_ERROR, CSL_NOT_INSERT || CSL_TA_LOAN_BALANCE_DETAIL);
   END IF;

   IF(PA_COMMIT = CSL_1)THEN
      COMMIT;
   END IF;

EXCEPTION
   WHEN EXC_BULK_ERRORS THEN
      -- Explicitly each exception individually by looping over SQL%BULK_EXCEPTIONS
      ROLLBACK;
      PA_STATUS_CODE := SQLCODE;
      PA_STATUS_MSG := SQLERRM || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;

      SC_CREDIT.SP_BATCH_ERROR_LOG(
         UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP)
         ,SQLCODE
         ,SQLERRM
         ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
         ,VL_TRANSACTION
         ,PREC_LOANS.FI_ADMIN_CENTER_ID
            ||CSL_COMMA
            ||PREC_LOANS.FI_LOAN_ID
      );
   WHEN OTHERS THEN
      ROLLBACK;
      PA_STATUS_CODE := SQLCODE;
      PA_STATUS_MSG := SQLERRM || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;

      SC_CREDIT.SP_BATCH_ERROR_LOG(
         UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP)
         ,SQLCODE
         ,SQLERRM
         ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
         ,VL_TRANSACTION
         ,PREC_LOANS.FI_ADMIN_CENTER_ID
            ||CSL_COMMA
            ||PREC_LOANS.FI_LOAN_ID
      );
END SP_BTC_EXE_OPERATION_BALANCE;

/

  GRANT EXECUTE ON SC_CREDIT.SP_BTC_EXE_OPERATION_BALANCE TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_BTC_EXE_OPERATION_BALANCE TO USRBTCCREDIT1;  
--------------------------------------------------------
--  DDL for Procedure SP_BTC_EXE_DELINQ_NEXT_INSTALL
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_BTC_EXE_DELINQ_NEXT_INSTALL (
    PTAB_STATUS_DETAIL          IN SC_CREDIT.TYP_TAB_BTC_STATUS_DETAIL
   ,PA_STATUS_CODE              OUT NUMBER
   ,PA_STATUS_MSG               OUT VARCHAR2
   ,PA_RECORDS_READ             OUT NUMBER
   ,PA_RECORDS_SUCCESS          OUT NUMBER
   ,PA_RECORDS_ERROR            OUT NUMBER
   ,PTAB_ERROR_RECORDS          OUT SC_CREDIT.TYP_TAB_BTC_ERROR
)
IS

   /* **************************************************************
   * PROJECT: LOAN LIFE CYCLE
   * DESCRIPTION: V1 - PROCESS TO APPLY NEXT INSTALLMENTE IN DELONQUENT
   * CREATED DATE: 17/12/2024
   * CREATOR: IVAN lOPEZ
   * MODIFICATION DATE:
   * PERFORMANCE MODIFICATIONS :
   ************************************************************** */

   --CONSTANTS
   CSL_0                              CONSTANT SIMPLE_INTEGER := 0;
   CSL_1                              CONSTANT SIMPLE_INTEGER := 1;
   CSL_SP                             CONSTANT SIMPLE_INTEGER := 1;

   --CONSTANTS SUCCESS
   CSL_SUCCESS_CODE                   CONSTANT SIMPLE_INTEGER := 0;
   CSL_SUCCESS_MSG                    CONSTANT VARCHAR2(7) := 'SUCCESS';
   CSL_CODE_ERROR                     CONSTANT SIMPLE_INTEGER := -20012;
   CSL_SPACE                          CONSTANT VARCHAR2(2) := ' ';
   CSL_SUCCESS_ERROR                  CONSTANT VARCHAR2(28) := 'SUCCESS, WITH ERRORS RECORDS';
   CSL_TYPE_NULL                      CONSTANT VARCHAR2(15) := 'TYPE FEES NULL';
   CSL_ARROW                          CONSTANT VARCHAR2(5) := ' -> ';
   CSL_COMMA                          CONSTANT VARCHAR2(5) := ' , ';
   CSL_INS_STATUS_DETAIL              CONSTANT VARCHAR2(30) := 'SP_BTC_INS_LOAN_STATUS_DETAIL';
   CSL_EXE_OPERATION                  CONSTANT VARCHAR2(30) := 'SP_BTC_EXE_OPERATION_BALANCE';
   CSL_INS_LOAN_STATUS                CONSTANT VARCHAR2(30) := 'SP_BTC_INS_LOAN_STATUS';
   VL_I                               NUMBER(10,0) := 0;
   VL_STATUS_CODE                     NUMBER(10,0) := 0;
   VL_STATUS_MSG                      VARCHAR2(1000);

   --VARIABLES INTERNAL TYPES ASSIGNMENT
   VLTAB_ERRORS                       SC_CREDIT.TYP_TAB_BTC_ERROR;

   t1 timestamp;--TODO QUITAR
   t2 timestamp;--TODO QUITAR
   VL_DESC_COUNT                     VARCHAR2(500);

BEGIN
   PA_STATUS_CODE := CSL_SUCCESS_CODE;
   PA_STATUS_MSG := CSL_SUCCESS_MSG;
   PA_RECORDS_SUCCESS := CSL_0;
   PA_RECORDS_ERROR := CSL_0;
   PA_RECORDS_READ := CSL_0;
   VLTAB_ERRORS := SC_CREDIT.TYP_TAB_BTC_ERROR();

 IF PTAB_STATUS_DETAIL IS NULL THEN
      RAISE_APPLICATION_ERROR(CSL_CODE_ERROR, CSL_TYPE_NULL);
   END IF;

   VL_I := PTAB_STATUS_DETAIL.FIRST;
   PA_RECORDS_READ := PTAB_STATUS_DETAIL.COUNT;

   WHILE (VL_I IS NOT NULL) LOOP
      BEGIN

         SC_CREDIT.SP_BTC_INS_LOAN_STATUS_DETAIL
            (PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID
            ,PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID
            ,PTAB_STATUS_DETAIL(VL_I).FI_LOAN_STATUS_ID
            ,PTAB_STATUS_DETAIL(VL_I).FI_ACTION_DETAIL_ID
            ,PTAB_STATUS_DETAIL(VL_I).FI_COUNTER_DAY
            ,PTAB_STATUS_DETAIL(VL_I).FD_INITIAL_DATE
            ,PTAB_STATUS_DETAIL(VL_I).FI_PAYMENT_NUMBER_ID
            ,PTAB_STATUS_DETAIL(VL_I).FD_FINAL_DATE
            ,PTAB_STATUS_DETAIL(VL_I).FI_ON_OFF
            ,CSL_0
            ,VL_STATUS_CODE
            ,VL_STATUS_MSG);

         IF(VL_STATUS_CODE != CSL_0)THEN
            RAISE_APPLICATION_ERROR( CSL_CODE_ERROR, CSL_INS_STATUS_DETAIL || CSL_SPACE || VL_STATUS_MSG);
         END IF;

       PA_RECORDS_SUCCESS := PA_RECORDS_SUCCESS + CSL_1;

      EXCEPTION
         WHEN OTHERS THEN
            ROLLBACK;
            SC_CREDIT.SP_BATCH_ERROR_LOG(
               UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP)
               ,SQLCODE
               ,SQLERRM
               ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
               ,CSL_0
               ,PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID
                  ||CSL_COMMA
                  ||PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID);
            PA_RECORDS_ERROR :=PA_RECORDS_ERROR + CSL_1;

            VLTAB_ERRORS.EXTEND;
            VLTAB_ERRORS(VLTAB_ERRORS.LAST) :=
               SC_CREDIT.TYP_REC_BTC_ERROR(PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID
                                          ,PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID
                                          , UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP)
                                          ,SQLCODE
                                          ,SQLERRM
                                          ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                          ,SYSDATE
                                          ,CSL_0
                                          ,NULL);
      END;
      VL_I := PTAB_STATUS_DETAIL.NEXT(VL_I);
      COMMIT;
   END LOOP;

   COMMIT;

   PTAB_ERROR_RECORDS := VLTAB_ERRORS;
   IF(PA_RECORDS_ERROR > CSL_0)THEN
      PA_STATUS_CODE := CSL_1;
      PA_STATUS_MSG := CSL_SUCCESS_ERROR;
   END IF;

   PA_STATUS_MSG := PA_STATUS_MSG
      || ' ' || 'Elapsed Seconds: '||TO_CHAR(t2-t1, 'SSSS.FF')
      || ' ' || VL_DESC_COUNT;--TODO QUITAR

EXCEPTION
   WHEN OTHERS THEN
      PA_STATUS_CODE := SQLCODE;
      PA_STATUS_MSG := SQLERRM || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;

      SC_CREDIT.SP_BATCH_ERROR_LOG(UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP)
                                  ,SQLCODE
                                  ,SQLERRM
                                  ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                  ,CSL_0
                                  ,NULL);
END SP_BTC_EXE_DELINQ_NEXT_INSTALL;

/

  GRANT EXECUTE ON SC_CREDIT.SP_BTC_EXE_DELINQ_NEXT_INSTALL TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_BTC_EXE_DELINQ_NEXT_INSTALL TO USRBTCCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_SYNC_TRIGGER
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_SYNC_TRIGGER 
 (
    PA_SYNC_JSON        CLOB,
    PA_UPDATED_ROWS OUT NUMBER,
    PA_STATUS_CODE  OUT NUMBER,
    PA_STATUS_MSG   OUT VARCHAR2
  ) IS
/* **************************************************************
* PROJECT: NCP
* DESCRIPTION: CATALOG SYNCHONIZATION TC_TRIGGER
* CREATED DATE: 2025/01/09
* CREATOR: CESAR CORTES
* MODIFICATION DATE: 2025/01/09
************************************************************** */
  BEGIN
    PA_STATUS_CODE := 0;
    PA_STATUS_MSG  := 'OK';


     MERGE INTO SC_CREDIT.TC_TRIGGER A
  USING (
    SELECT
      *
    FROM
      JSON_TABLE ( PA_SYNC_JSON, '$.trigger[*]'
        COLUMNS (
          ID NUMBER PATH '$.id',
          DESCRIPTION VARCHAR2 ( 50 ) PATH '$.description',
          ACRONYM NUMBER PATH '$.acronym',
          STATUS NUMBER PATH '$.status',
          USER_NAME VARCHAR2 ( 50 ) PATH '$.user',
          CREATED_DATE TIMESTAMP PATH '$.createdDate',
          MODIFICATION_DATE TIMESTAMP PATH '$.modificationDate'
        )
      )
  ) B ON ( A.FI_TRIGGER_ID = B.ID )
  WHEN MATCHED THEN UPDATE
  SET A.FC_TRIGGER_DESC = B.DESCRIPTION,
      A.FC_ACRONYM_TRIGGER = B.ACRONYM,
	    A.FI_STATUS=B.STATUS,
      A.FC_USER = B.USER_NAME,
      A.FD_CREATED_DATE = B.CREATED_DATE,
      A.FD_MODIFICATION_DATE = CAST(B.MODIFICATION_DATE AS DATE)
  WHEN NOT MATCHED THEN
  INSERT (
    FI_TRIGGER_ID,
    FC_TRIGGER_DESC,
    FC_ACRONYM_TRIGGER,
    FI_STATUS,
    FC_USER,
    FD_CREATED_DATE,
    FD_MODIFICATION_DATE )
  VALUES
    ( B.ID,
      B.description,
      B.ACRONYM,
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
      SC_CREDIT.SP_ERROR_LOG('SP_SYNC_TRIGGER', SQLCODE, SQLERRM, DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, NULL,'');

END SP_SYNC_TRIGGER;

/

  GRANT EXECUTE ON SC_CREDIT.SP_SYNC_TRIGGER TO USRPURPOSEWS;
  GRANT EXECUTE ON SC_CREDIT.SP_SYNC_TRIGGER TO USRNCPCREDIT1;
  
--------------------------------------------------------
--  DDL for Procedure SP_TMP_BTC_SEL_DAILY_INTEREST
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_TMP_BTC_SEL_DAILY_INTEREST 
  (
   PA_FIRST_CENTER_ID     IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE
   ,PA_END_CENTER_ID       IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE
   ,PA_TODAY               IN VARCHAR2
   ,PA_PROCESS             IN NUMBER
   ,PA_TRACK               IN NUMBER
   ,PA_STATUS_CODE         OUT NUMBER
   ,PA_STATUS_MSG          OUT VARCHAR2
   ,PA_CUR_SELECT          OUT SC_CREDIT.PA_TYPES.TYP_CURSOR
   )
   IS
   /* **************************************************************
   * PROYECT: LOAN LIFE CYCLE
   * DESCRIPTION: INTEREST CALCULATION PROCEDURE (TEST VICTOR)
   * CREATED DATE: 02/12/2024
   * CREATOR:IVAN LOPEZ
   ************************************************************** */

     --CONSTANT
   CSL_ARROW        CONSTANT VARCHAR2(2)   := '->';
   CSL_DATE         CONSTANT VARCHAR2(10)   := 'MM/DD/YYYY';
   CSL_0            CONSTANT SIMPLE_INTEGER := 0;
   CSL_1            CONSTANT SIMPLE_INTEGER := 1;
   CSL_2            CONSTANT SIMPLE_INTEGER := 2;
   CSL_3            CONSTANT SIMPLE_INTEGER := 3;
   CSL_MSG_SUCCESS  CONSTANT VARCHAR2(7)   := 'SUCCESS';
   CSL_SP          CONSTANT SIMPLE_INTEGER := 1;

   BEGIN

    PA_STATUS_CODE:=CSL_0;
    PA_STATUS_MSG:=CSL_MSG_SUCCESS;
    PA_CUR_SELECT:=NULL;

          --CONSULT LOAN,INTEREST,SCHEDULE
      OPEN PA_CUR_SELECT FOR
       WITH TABPED AS
           (
SELECT L.FI_LOAN_ID                                             AS FI_LOAN_ID
                  ,L.FI_ADMIN_CENTER_ID                         AS FI_ADMIN_CENTER_ID
                  ,LO.FI_COUNTRY_ID                                                                           AS FI_COUNTRY_ID
                  ,LO.FI_COMPANY_ID                                                                           AS FI_COMPANY_ID
                  ,LO.FI_BUSINESS_UNIT_ID                                                     AS FI_BUSINESS_UNIT_ID
                  ,LO.FC_CUSTOMER_ID                                                                  AS FC_CUSTOMER_ID
                  ,LO.FN_FINANCE_CHARGE_AMOUNT                  AS FN_FINANCE_CHARGE_AMOUNT
                  ,LO.FN_PAID_INTEREST_AMOUNT                   AS FN_PAID_INTEREST_AMOUNT
                  ,LO.FN_PRINCIPAL_BALANCE                      AS FN_PRINCIPAL_BALANCE
                  ,LO.FN_FINANCE_CHARGE_BALANCE                 AS FN_FINANCE_CHARGE_BALANCE
                  ,LO.FN_ADDITIONAL_CHARGE_BALANCE              AS FN_ADDITIONAL_CHARGE_BALANCE
                  ,TO_CHAR(LO.FD_ORIGINATION_DATE, CSL_DATE)    AS FD_ORIGINATION_DATE
                  ,TO_CHAR(LO.FD_FIRST_PAYMENT, CSL_DATE)       AS FD_FIRST_PAYMENT
                  ,LO.FI_CURRENT_BALANCE_SEQ                    AS FI_CURRENT_BALANCE_SEQ
                  ,LO.FN_INTEREST_RATE                          AS FN_INTEREST_RATE
                  ,LO.FI_NUMBER_OF_PAYMENTS                     AS FI_NUMBER_OF_PAYMENTS
                  ,LO.FI_TERM_TYPE                              AS FI_TERM_TYPE
                  ,LO.FI_LOAN_STATUS_ID                                                             AS FI_LOAN_STATUS_ID
                  ,LO.FI_RULE_ID                                AS FI_RULE_ID
                  ,TO_CHAR(LO.FD_LOAN_EFFECTIVE_DATE, CSL_DATE) AS FD_LOAN_EFFECTIVE_DATE
                  ,LI.FI_PAYMENT_NUMBER_ID                      AS FI_PAYMENT_NUMBER_ID
                  ,LI.FI_DAYS_ACUM_BY_TERM                      AS FI_DAYS_ACUM_BY_TERM
                  ,LI.FN_DAILY_INTEREST                         AS FN_DAILY_INTEREST
                  ,LI.FN_ACCRUED_INTEREST_BALANCE               AS FN_ACCRUED_INTEREST_BALANCE
                  ,LI.FN_ACCRUED_INTEREST_LOAN                                          AS FN_ACCRUED_INTEREST_LOAN
                  ,LI.FN_PAYMENT_INTEREST                                                           AS FN_PAYMENT_INTEREST
                  ,LI.FC_CONDITION_INTEREST                     AS FC_CONDITION_INTEREST
                  ,TO_CHAR(LI.FD_APPLICATION_DATE, CSL_DATE)    AS FD_APPLICATION_DATE
                   ,ROW_NUMBER() OVER (PARTITION BY LO.FI_LOAN_ID ORDER BY LI.FI_PAYMENT_NUMBER_ID DESC,LI.FI_DAYS_ACUM_BY_TERM DESC) AS ORDEN
              FROM SC_CREDIT.TA_TMP_LOAN_PROCESS L
              INNER JOIN SC_CREDIT.TA_LOAN LO
                ON L.FI_LOAN_ID = LO.FI_LOAN_ID
               AND L.FI_ADMIN_CENTER_ID = LO.FI_ADMIN_CENTER_ID
               LEFT JOIN SC_CREDIT.TA_LOAN_INTEREST LI
                ON LI.FI_LOAN_ID = L.FI_LOAN_ID
               AND LI.FI_ADMIN_CENTER_ID = L.FI_ADMIN_CENTER_ID
               WHERE L.FI_PROCESS = PA_PROCESS
               AND L.FI_TRACK = PA_TRACK
               AND LO.FI_LOAN_STATUS_ID IN (CSL_2, CSL_3))
            SELECT A.FI_LOAN_ID                                 AS FI_LOAN_ID
                  ,A.FI_ADMIN_CENTER_ID                         AS FI_ADMIN_CENTER_ID
                  ,A.FI_COUNTRY_ID                                                                            AS FI_COUNTRY_ID
                  ,A.FI_COMPANY_ID                                                                            AS FI_COMPANY_ID
                  ,A.FI_BUSINESS_UNIT_ID                                                            AS FI_BUSINESS_UNIT_ID
                  ,A.FC_CUSTOMER_ID                                                                     AS FC_CUSTOMER_ID
                  ,A.FN_FINANCE_CHARGE_AMOUNT                   AS FN_FINANCE_CHARGE_AMOUNT
                  ,A.FN_PAID_INTEREST_AMOUNT                    AS FN_PAID_INTEREST_AMOUNT
                  ,A.FN_PRINCIPAL_BALANCE                       AS FN_PRINCIPAL_BALANCE
                  ,A.FN_FINANCE_CHARGE_BALANCE                  AS FN_FINANCE_CHARGE_BALANCE
                  ,A.FN_ADDITIONAL_CHARGE_BALANCE               AS FN_ADDITIONAL_CHARGE_BALANCE
                  ,A.FD_ORIGINATION_DATE                        AS FD_ORIGINATION_DATE
                  ,A.FD_FIRST_PAYMENT                           AS FD_FIRST_PAYMENT
                  ,A.FI_CURRENT_BALANCE_SEQ                     AS FI_CURRENT_BALANCE_SEQ
                  ,A.FN_INTEREST_RATE                           AS FN_INTEREST_RATE
                  ,A.FI_NUMBER_OF_PAYMENTS                      AS FI_NUMBER_OF_PAYMENTS
                  ,A.FI_TERM_TYPE                               AS FI_TERM_TYPE
                  ,A.FI_LOAN_STATUS_ID                                                              AS FI_LOAN_STATUS_ID
                  ,A.FI_RULE_ID                                 AS FI_RULE_ID
                  ,A.FD_LOAN_EFFECTIVE_DATE                     AS FD_LOAN_EFFECTIVE_DATE
                  ,A.FI_PAYMENT_NUMBER_ID                       AS FI_PAYMENT_NUMBER_ID
                  ,A.FI_DAYS_ACUM_BY_TERM                       AS FI_DAYS_ACUM_BY_TERM
                  ,A.FN_DAILY_INTEREST                          AS FN_DAILY_INTEREST
                  ,A.FN_ACCRUED_INTEREST_BALANCE                AS FN_ACCRUED_INTEREST_BALANCE
                  ,A.FN_ACCRUED_INTEREST_LOAN                                             AS FN_ACCRUED_INTEREST_LOAN
                  ,A.FN_PAYMENT_INTEREST                                                            AS FN_PAYMENT_INTEREST
                  ,A.FC_CONDITION_INTEREST                      AS FC_CONDITION_INTEREST
                  ,A.FD_APPLICATION_DATE                        AS FD_APPLICATION_DATE
                  ,(SELECT TO_CHAR(PSA.FD_DUE_DATE, CSL_DATE)   AS FD_DUE_DATE_ANT
                      FROM SC_CREDIT.TA_PAYMENT_SCHEDULE PSA
                     WHERE PSA.FI_LOAN_ID = A.FI_LOAN_ID
                       AND PSA.FI_ADMIN_CENTER_ID = A.FI_ADMIN_CENTER_ID
                       AND PSA.FD_DUE_DATE < TO_DATE(PA_TODAY, CSL_DATE)
                       ORDER BY PSA.FI_PAYMENT_NUMBER_ID DESC
                       FETCH FIRST 1 ROW ONLY)                      AS FD_DUE_DATE_BEFORE
                  ,(SELECT PSA.FI_PAYMENT_NUMBER_ID             AS FI_PAYMENT
                      FROM SC_CREDIT.TA_PAYMENT_SCHEDULE PSA
                     WHERE PSA.FI_LOAN_ID = A.FI_LOAN_ID
                       AND PSA.FI_ADMIN_CENTER_ID = A.FI_ADMIN_CENTER_ID
                       AND PSA.FD_DUE_DATE >= TO_DATE(PA_TODAY, CSL_DATE)
                       AND ROWNUM = CSL_1)                      AS FI_PAYMENT_ID
                  ,(SELECT TO_CHAR(PSA.FD_DUE_DATE,CSL_DATE)   AS FD_DUE_DATE_NEXT
                      FROM SC_CREDIT.TA_PAYMENT_SCHEDULE PSA
                     WHERE PSA.FI_LOAN_ID = A.FI_LOAN_ID
                       AND PSA.FI_ADMIN_CENTER_ID = A.FI_ADMIN_CENTER_ID
                       AND PSA.FD_DUE_DATE >= TO_DATE(PA_TODAY, CSL_DATE)
                       AND ROWNUM = CSL_1)                      AS FD_DUE_DATE_AFTER
                  ,SC_CREDIT.FN_SEL_LOAN_BALANCE_DET_JSON
                     (A.FI_LOAN_ID
                     ,A.FI_ADMIN_CENTER_ID
                     ,A.FI_CURRENT_BALANCE_SEQ
                     ,NULL) AS BALANCE_DET_JSON
              FROM TABPED A
             WHERE A.ORDEN =CSL_1;


      EXCEPTION
      WHEN OTHERS THEN
         PA_STATUS_CODE := SQLCODE;
         PA_STATUS_MSG := SQLERRM || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;

         SC_CREDIT.SP_BATCH_ERROR_LOG(UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP)
                                     ,SQLCODE
                                     ,SQLERRM
                                     ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                     ,CSL_0
                                     ,PA_FIRST_CENTER_ID ||','|| PA_END_CENTER_ID ||',' ||PA_TODAY
                                     );
END SP_TMP_BTC_SEL_DAILY_INTEREST;

/

  GRANT EXECUTE ON SC_CREDIT.SP_TMP_BTC_SEL_DAILY_INTEREST TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_TMP_BTC_SEL_DAILY_INTEREST TO USRBTCCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_TMP_BTC_SEL_DAILY_INTEREST TO USRCREDIT02;
--------------------------------------------------------
--  DDL for Procedure SP_SEL_LOAN_DATA
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_SEL_LOAN_DATA (
    PA_LOAN_ID       IN  NUMBER,
    PA_UUID_TRACKING IN  SC_CREDIT.TA_LOAN.FC_UUID_TRACKING%TYPE,
    PA_JSON_OBJECT   OUT CLOB,
    PA_STATUS_CODE   OUT NUMBER,
    PA_STATUS_MSG    OUT VARCHAR2
)
AS
 /* **************************************************************
 * PROJECT: CORE LOAN
 * DESCRIPTION: PROCEDURE FOR QUERYING LOANS DATA
 * PRECONDITIONS: PRE-EXISTING LOANS
 * CREATED DATE: 23/10/2024
 * CREATOR: GILBERTO CHAVEZ MUNOZ
 *****************************************************************/

  CSL_0             CONSTANT SIMPLE_INTEGER := 0;
  CSL_1             CONSTANT SIMPLE_INTEGER := 1;
  CSL_SP            CONSTANT SIMPLE_INTEGER := 1;
  CSL_ARROW         CONSTANT VARCHAR2(5) := '->';
  CSL_JSON          CONSTANT VARCHAR2(5) := NULL;
  CSL_SUCCESS       CONSTANT VARCHAR2(8) := 'SUCCESS';

  VG_OPERATION_TYPE CLOB;
  VG_LOAN_CONCEPT   CLOB;
  VG_PAY_SCHEDULE   CLOB;
  VG_PAY_SCH_STATUS CLOB;

  TYPE REC_VALUE IS RECORD
     (
        fi_operation_type_id   SC_CREDIT.TC_OPERATION_TYPE.FI_OPERATION_TYPE_ID%TYPE,
        fc_operation_type_desc SC_CREDIT.TC_OPERATION_TYPE.FC_OPERATION_TYPE_DESC%TYPE
     );

  TYPE REC_LOAN_CONCEPT IS RECORD
     (
        FI_LOAN_CONCEPT_ID      SC_CREDIT.TC_LOAN_CONCEPT.FI_LOAN_CONCEPT_ID%TYPE,
        FC_LOAN_CONCEPT_DESC    SC_CREDIT.TC_LOAN_CONCEPT.FC_LOAN_CONCEPT_DESC%TYPE,
        FI_LOAN_CONCEP_TYPE_ID  SC_CREDIT.TC_LOAN_CONCEPT.FI_LOAN_CONCEPT_TYPE_ID%TYPE
     );

  TYPE REC_PAY_SCHEDULE IS RECORD
     (
       FI_PAYMENT_NUMBER_ID SC_CREDIT.TA_PAYMENT_SCHEDULE.FI_PAYMENT_NUMBER_ID%TYPE
     );

  TYPE REC_PAY_SCHEDULE_STATUS IS RECORD
     (
       FI_PMT_SCHEDULE_STATUS_DESC SC_CREDIT.TC_PAYMENT_SCHEDULE_STATUS.FI_PMT_SCHEDULE_STATUS_DESC%TYPE,
       FI_PMT_SCHEDULE_STATUS_ID   SC_CREDIT.TC_PAYMENT_SCHEDULE_STATUS.FI_PMT_SCHEDULE_STATUS_ID%TYPE
     );

  TYPE TAB_VALUE IS TABLE OF REC_VALUE;
  VL_TABVALUE_LOD TAB_VALUE;

  TYPE TAB_LOAN_CONCEPT IS TABLE OF REC_LOAN_CONCEPT;
  VL_TAB_LOAN_CONCEPT TAB_LOAN_CONCEPT;

  TYPE TAB_PAY_SCHEDULE IS TABLE OF REC_PAY_SCHEDULE;
  VL_TAB_PAY_SCHEDULE TAB_PAY_SCHEDULE;

  TYPE TAB_PAY_SCHEDULE_STATUS IS TABLE OF REC_PAY_SCHEDULE_STATUS;
  VL_TAB_PAY_SCHEDULE_STATUS TAB_PAY_SCHEDULE_STATUS;

  VG_JO_OPERATION_TYPE      JSON_OBJECT_T := JSON_OBJECT_T();
  VG_JA_OPERATION_TYPE      JSON_ARRAY_T  := JSON_ARRAY_T();
  VG_JO_LOAN_CONCEPT        JSON_OBJECT_T := JSON_OBJECT_T();
  VG_JA_LOAN_CONCEPT        JSON_ARRAY_T  := JSON_ARRAY_T();
  VG_JA_PAY_SCHEDULE        JSON_ARRAY_T  := JSON_ARRAY_T();
  VG_JO_PAY_SCHEDULE_STATUS JSON_OBJECT_T := JSON_OBJECT_T();

BEGIN
  SELECT fi_operation_type_id, fc_operation_type_desc
  BULK COLLECT INTO VL_TABVALUE_LOD
  FROM   SC_CREDIT.tc_operation_type
  WHERE  fi_status = 1;

  VG_JO_OPERATION_TYPE := JSON_OBJECT_T();

  FOR i IN 1.. VL_TABVALUE_LOD.COUNT LOOP
      VG_JO_OPERATION_TYPE.put(VL_TABVALUE_LOD(i).FC_OPERATION_TYPE_DESC, VL_TABVALUE_LOD(i).FI_OPERATION_TYPE_ID);
  END LOOP;

  VG_JA_OPERATION_TYPE := JSON_ARRAY_T();
  VG_JA_OPERATION_TYPE.append(VG_JO_OPERATION_TYPE);

  VG_OPERATION_TYPE := VG_JO_OPERATION_TYPE.to_string;

  SELECT FI_LOAN_CONCEPT_ID, FC_LOAN_CONCEPT_DESC, FI_LOAN_CONCEPT_TYPE_ID
  BULK COLLECT INTO VL_TAB_LOAN_CONCEPT
  FROM   SC_CREDIT.TC_LOAN_CONCEPT
  WHERE  FI_STATUS = 1;

  VG_JO_LOAN_CONCEPT := JSON_OBJECT_T();

  FOR i IN 1..VL_TAB_LOAN_CONCEPT.COUNT LOOP
       VG_JO_LOAN_CONCEPT.put(VL_TAB_LOAN_CONCEPT(i).FC_LOAN_CONCEPT_DESC, VL_TAB_LOAN_CONCEPT(i).FI_LOAN_CONCEPT_ID);
  END LOOP;
  VG_JA_LOAN_CONCEPT := JSON_ARRAY_T();
  VG_JA_LOAN_CONCEPT.append( VG_JO_LOAN_CONCEPT );

  VG_LOAN_CONCEPT := VG_JO_LOAN_CONCEPT.to_string;

  SELECT FI_PMT_SCHEDULE_STATUS_DESC, FI_PMT_SCHEDULE_STATUS_ID
  BULK COLLECT INTO VL_TAB_PAY_SCHEDULE_STATUS
  FROM   SC_CREDIT.TC_PAYMENT_SCHEDULE_STATUS
  WHERE  FI_STATUS = 1;

  VG_JO_PAY_SCHEDULE_STATUS := JSON_OBJECT_T();

  FOR i IN 1..VL_TAB_PAY_SCHEDULE_STATUS.COUNT LOOP
       VG_JO_PAY_SCHEDULE_STATUS.put(VL_TAB_PAY_SCHEDULE_STATUS(i).FI_PMT_SCHEDULE_STATUS_DESC, VL_TAB_PAY_SCHEDULE_STATUS(i).FI_PMT_SCHEDULE_STATUS_ID);
  END LOOP;

  VG_PAY_SCH_STATUS := VG_JO_PAY_SCHEDULE_STATUS.to_string;

  SELECT FI_PAYMENT_NUMBER_ID
  BULK COLLECT INTO VL_TAB_PAY_SCHEDULE
  FROM   SC_CREDIT.TA_PAYMENT_SCHEDULE
  WHERE  FI_LOAN_ID = PA_LOAN_ID
  AND    FI_PMT_SCHEDULE_STATUS_ID = 1
  ORDER BY FI_PAYMENT_NUMBER_ID;

  VG_JA_PAY_SCHEDULE := JSON_ARRAY_T();

  FOR i IN 1..VL_TAB_PAY_SCHEDULE.COUNT LOOP
       VG_JA_PAY_SCHEDULE.append(VL_TAB_PAY_SCHEDULE(i).FI_PAYMENT_NUMBER_ID);
  END LOOP;

  VG_PAY_SCHEDULE := VG_JA_PAY_SCHEDULE.to_string;

  SELECT json_object('TA_LOAN' VALUE json_object('PAYMENT_AMOUNT'            VALUE LN.FN_PRINCIPAL_AMOUNT,
                                                 'FINANCE_CHARGE_AMOUNT'     VALUE LN.FN_FINANCE_CHARGE_AMOUNT,
                                                 'PRINCIPAL_BALANCE'         VALUE LN.FN_PRINCIPAL_BALANCE,
                                                 'FINANCE_CHARGE_BALANCE'    VALUE LN.FN_FINANCE_CHARGE_BALANCE,
                                                 'ADDITIONAL_CHARGE_BALANCE' VALUE LN.FN_ADDITIONAL_CHARGE_BALANCE,
                                                 'TRANSACTION'               VALUE LN.FI_TRANSACTION,
                                                                                             'STATUS'                    VALUE LN.FI_LOAN_STATUS_ID
                                                ),
/*                     'TA_PAYMENT_TYPE_DETAIL' VALUE json_object('PAYMENT_TYPE_ID' VALUE PTD.FI_PAYMENT_TYPE_ID,
                                                                                                        'PAYMENT_AMOUNT'  VALUE PTD.FN_PAYMENT_AMOUNT
                                                               ),*/
                                                              'OPERATION_TYPE'          VALUE VG_OPERATION_TYPE FORMAT JSON,
                                                              'LOAN_CONCEPT'            VALUE VG_LOAN_CONCEPT   FORMAT JSON,
                                                              'PAYMENT_SCHEDULE'        VALUE VG_PAY_SCHEDULE   FORMAT JSON,
                                                              'PAYMENT_SCHEDULE_STATUS' VALUE VG_PAY_SCH_STATUS FORMAT JSON
                    )
  INTO   PA_JSON_OBJECT
  FROM   SC_CREDIT.TA_LOAN LN
  WHERE  LN.FI_LOAN_ID = PA_LOAN_ID
  ;

  PA_STATUS_CODE := CSL_0;
  PA_STATUS_MSG := CSL_SUCCESS;
EXCEPTION
  WHEN OTHERS THEN
    PA_STATUS_CODE := SQLCODE;
    PA_STATUS_MSG := SQLERRM || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    SC_CREDIT.SP_ERROR_LOG(
       UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP)
       ,SQLCODE
       ,SQLERRM
       ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
       ,PA_UUID_TRACKING
       ,CSL_JSON
       );
END SP_SEL_LOAN_DATA;

/

  GRANT EXECUTE ON SC_CREDIT.SP_SEL_LOAN_DATA TO USRNCPCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_UPD_WOFF
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_UPD_WOFF (
    PTAB_PWO_AMOUNT_DETAIL          IN SC_CREDIT.TYP_TAB_BTC_STATUS_PWO
   ,PTAB_STATUS_DETAIL              IN SC_CREDIT.TYP_TAB_BTC_STATUS_DETAIL
   ,PA_UUID_TRACKING                IN  VARCHAR2
   ,PA_COMMIT                       IN NUMBER
   ,PA_STATUS_CODE                  OUT NUMBER
   ,PA_STATUS_MSG                   OUT VARCHAR2
   )
   IS
   /* **************************************************************
   * DESCRIPTION: UPDATE IN TABLE
   * PRECONDITIONS:
   * CREATED DATE: 21/11/2024
   * CREATOR: IVAN LOPEZ
   * MODIFICATION USER: AIXA SARMIENTO
   * MODIFICACTION DATE: 02/01/2025
   ************************************************************** */

   CSL_0                 CONSTANT SIMPLE_INTEGER := 0;
   CSL_1                 CONSTANT SIMPLE_INTEGER := 1;
   CSL_SP                CONSTANT SIMPLE_INTEGER := 1; --
   CSL_DATE_FORMAT       CONSTANT VARCHAR2(40)   := 'MM/DD/YYYY hh24:mi:ss';
   CSL_MSG_SUCCESS       CONSTANT VARCHAR2(20)   := 'SUCCESS';
   CSL_ARROW             CONSTANT VARCHAR2(5)    := '-->';                                      -- AESTHETICS SIGN
   CSL_SPACE             CONSTANT VARCHAR2(5)    := ' ';
   VL_I                  NUMBER(10) := 0;
   VL_II                 NUMBER(10) := 0;
   VL_STATUS_DETAIL      SC_CREDIT.TYP_TAB_BTC_STATUS_DETAIL;
   VL_DATA_IN            VARCHAR2(50);

   BEGIN

   PA_STATUS_CODE:= CSL_0;
   PA_STATUS_MSG := CSL_MSG_SUCCESS;

   VL_I := PTAB_STATUS_DETAIL.FIRST;
   VL_II := PTAB_PWO_AMOUNT_DETAIL.FIRST;

   UPDATE SC_CREDIT.TA_PWO_AMOUNT_DETAIL
      SET FD_MODIFICATION_DATE = SYSDATE,
          FN_AMOUNT_PAID = PTAB_PWO_AMOUNT_DETAIL(VL_II).FN_AMOUNT_PAID,
          FI_ADD_EXTENSION =PTAB_PWO_AMOUNT_DETAIL(VL_II).FI_ADD_EXTENSION,
          FN_PWO_MIN_PAYMENT = PTAB_PWO_AMOUNT_DETAIL(VL_II).FN_PWO_MIN_PAYMENT,
          FD_PWO_DATE = TO_DATE(PTAB_PWO_AMOUNT_DETAIL(VL_II).FD_PWO_DATE,CSL_DATE_FORMAT)
    WHERE FI_LOAN_ID = PTAB_PWO_AMOUNT_DETAIL(VL_II).FI_LOAN_ID
      AND FI_ADMIN_CENTER_ID = PTAB_PWO_AMOUNT_DETAIL(VL_II).FI_ADMIN_CENTER_ID;

   IF PA_COMMIT = CSL_1 THEN
         COMMIT;
   END IF;

   WHILE (VL_I IS NOT NULL) LOOP
      BEGIN
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
          VALUES (PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID
                 ,PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID
                 ,SC_CREDIT.SE_LOAN_STATUS_DETAIL.NEXTVAL
                 ,PTAB_STATUS_DETAIL(VL_I).FI_LOAN_STATUS_ID
                 ,PTAB_STATUS_DETAIL(VL_I).FI_ACTION_DETAIL_ID
                 ,PTAB_STATUS_DETAIL(VL_I).FI_COUNTER_DAY
                 ,TO_DATE(PTAB_STATUS_DETAIL(VL_I).FD_INITIAL_DATE,CSL_DATE_FORMAT)
                 ,PTAB_STATUS_DETAIL(VL_I).FI_PAYMENT_NUMBER_ID
                 ,TO_DATE(PTAB_STATUS_DETAIL(VL_I).FD_FINAL_DATE,CSL_DATE_FORMAT)
                 ,PTAB_STATUS_DETAIL(VL_I).FI_ON_OFF
                 ,USER
                 ,SYSDATE
                 ,SYSDATE
                 );
      EXCEPTION
        WHEN OTHERS THEN
           PA_STATUS_CODE := SQLCODE;
           PA_STATUS_MSG  := SQLERRM || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;

           VL_DATA_IN := PTAB_STATUS_DETAIL(VL_I).FI_LOAN_STATUS_ID
                         ||CSL_SPACE || PTAB_STATUS_DETAIL(VL_I).FI_ACTION_DETAIL_ID
                         ||CSL_SPACE || TO_DATE(PTAB_STATUS_DETAIL(VL_I).FD_INITIAL_DATE,CSL_DATE_FORMAT)
                         ||CSL_SPACE || TO_DATE(PTAB_STATUS_DETAIL(VL_I).FD_FINAL_DATE,CSL_DATE_FORMAT);

           SC_CREDIT.SP_ERROR_LOG('SP_UPD_WOFF', SQLCODE, SQLERRM,
             DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID ||CSL_SPACE || PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID , VL_DATA_IN);
      END;
      VL_I := PTAB_STATUS_DETAIL.NEXT(VL_I);
      IF PA_COMMIT = CSL_1 THEN
         COMMIT;
      END IF;
   END LOOP;

EXCEPTION
   WHEN OTHERS THEN
      ROLLBACK;
         PA_STATUS_CODE := SQLCODE;
         PA_STATUS_MSG := SQLERRM  || ' -> ' ||  DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
         SC_CREDIT.SP_ERROR_LOG(UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP)
                                    ,SQLCODE
                                    ,SQLERRM
                                    ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                    ,PA_UUID_TRACKING
                                    ,NULL);
END SP_UPD_WOFF;

/

  GRANT EXECUTE ON SC_CREDIT.SP_UPD_WOFF TO USRNCPCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_BTC_APPLY_DAILY_INTEREST
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_BTC_APPLY_DAILY_INTEREST 
   (PTAB_INTEREST                  IN SC_CREDIT.TYP_TAB_BTC_INTEREST
   ,PTAB_LOANS                     IN SC_CREDIT.TYP_TAB_BTC_LOAN
   ,PTAB_OPERATIONS                IN SC_CREDIT.TYP_TAB_BTC_OPERATION
   ,PTAB_OPERATIONS_DETAIL         IN SC_CREDIT.TYP_TAB_BTC_DETAIL
   ,PTAB_BALANCES                  IN SC_CREDIT.TYP_TAB_BTC_BALANCE
   ,PTAB_BALANCES_DETAIL           IN SC_CREDIT.TYP_TAB_BTC_DETAIL
   ,PA_DEVICE                      IN SC_CREDIT.TA_LOAN_OPERATION.FC_DEVICE%TYPE
   ,PA_GPS_LATITUDE                IN SC_CREDIT.TA_LOAN_OPERATION.FC_GPS_LATITUDE%TYPE
   ,PA_GPS_LONGITUDE               IN SC_CREDIT.TA_LOAN_OPERATION.FC_GPS_LONGITUDE%TYPE
   ,PA_STATUS_CODE                 OUT NUMBER
   ,PA_STATUS_MSG                  OUT VARCHAR2
   ,PA_RECORDS_READ                OUT NUMBER
   ,PA_RECORDS_SUCCESS             OUT NUMBER
   ,PA_RECORDS_ERROR               OUT NUMBER
   ,PTAB_ERROR_RECORDS             OUT SC_CREDIT.TYP_TAB_BTC_ERROR)
IS

   /* **************************************************************
   * PROJECT: LOAN LIFE CYCLE
   * DESCRIPTION: V3 - PROCESS TO APPLY DAILY INTEREST
   * CREATED DATE: 24/10/2024
   * CREATOR: EDUARDO CERVANTES HERNANDEZ
   * MODIFICATION DATE: 12/12/2024
   * PERFORMANCE MODIFICATIONS - LUIS RAMIREZ
   ************************************************************** */

   --CONSTANTS
   CSL_0                              CONSTANT SIMPLE_INTEGER := 0;
   CSL_1                              CONSTANT SIMPLE_INTEGER := 1;
   CSL_SP                             CONSTANT SIMPLE_INTEGER := 1;

   --CONSTANTS SUCCESS
   CSL_SUCCESS_CODE                   CONSTANT SIMPLE_INTEGER := 0;
   CSL_SUCCESS_MSG                    CONSTANT VARCHAR2(7) := 'SUCCESS';

   --CONSTANTS ERRORS
   CSL_CODE_ERROR                     CONSTANT SIMPLE_INTEGER := -20012;

   --CONSTANTS MESSAGES
   CSL_SUCCESS_ERROR                  CONSTANT VARCHAR2(28) := 'SUCCESS, WITH ERRORS RECORDS';
   CSL_TYPE_NULL                      CONSTANT VARCHAR2(20) := 'TYPE INTEREST NULL';
   CSL_SPACE                          CONSTANT VARCHAR2(2) := ' ';
   CSL_ARROW                          CONSTANT VARCHAR2(5) := ' -> ';
   CSL_COMMA                          CONSTANT VARCHAR2(5) := ' , ';

   CSL_GEN_OPERATION                  CONSTANT VARCHAR2(30) := 'SP_BTC_EXE_OPERATION_BALANCE';
   CSL_INS_LOAN_INTEREST              CONSTANT VARCHAR2(30) := 'SP_BTC_INS_LOAN_INTEREST';

   VL_I                               NUMBER(10,0) := 0;

   VL_STATUS_CODE                     NUMBER(10,0) := 0;
   VL_STATUS_MSG                      VARCHAR2(1000);

   --VARIABLES INTERNAL TYPES ASSIGNMENT
   VLTAB_ERRORS                       SC_CREDIT.TYP_TAB_BTC_ERROR;
   VLTAB_LOANS                        SC_CREDIT.TYP_TAB_BTC_LOAN;
   VLTAB_OPERATIONS                   SC_CREDIT.TYP_TAB_BTC_OPERATION;
   VLTAB_OPERATIONS_DETAIL            SC_CREDIT.TYP_TAB_BTC_DETAIL;
   VLTAB_BALANCES                     SC_CREDIT.TYP_TAB_BTC_BALANCE;
   VLTAB_BALANCES_DETAIL              SC_CREDIT.TYP_TAB_BTC_DETAIL;

   --VARIABLES OF ITERATION BY LOAN
   VLREC_LOAN                        SC_CREDIT.TYP_REC_BTC_LOAN;
   VLTAB_OPERATIONS_BY_LOAN          SC_CREDIT.TYP_TAB_BTC_OPERATION;
   VLTAB_OPERATIONS_DET_BY_LOAN      SC_CREDIT.TYP_TAB_BTC_DETAIL;
   VLTAB_BALANCES_BY_LOAN            SC_CREDIT.TYP_TAB_BTC_BALANCE;
   VLTAB_BALANCES_DET_BY_LOAN        SC_CREDIT.TYP_TAB_BTC_DETAIL;

   VL_DESC_COUNT                     VARCHAR2(500);
   VL_T1                             TIMESTAMP;
   VL_T2                             TIMESTAMP;

BEGIN
   PA_STATUS_CODE := CSL_SUCCESS_CODE;
   PA_STATUS_MSG := CSL_SUCCESS_MSG;
   PA_RECORDS_SUCCESS := CSL_0;
   PA_RECORDS_ERROR := CSL_0;
   PA_RECORDS_READ := CSL_0;
   VLTAB_ERRORS := SC_CREDIT.TYP_TAB_BTC_ERROR();
   VL_T1 := systimestamp;

   IF PTAB_INTEREST IS NULL OR PTAB_LOANS IS NULL
         OR PTAB_OPERATIONS IS NULL OR PTAB_OPERATIONS_DETAIL IS NULL
         OR PTAB_BALANCES IS NULL OR PTAB_BALANCES_DETAIL IS NULL THEN
      RAISE_APPLICATION_ERROR(CSL_CODE_ERROR, CSL_TYPE_NULL);
   END IF;

   VL_I := PTAB_INTEREST.FIRST;
   PA_RECORDS_READ := PTAB_INTEREST.COUNT;

   --INTERNAL TYPES ASSIGNMENT
   VLTAB_LOANS             := PTAB_LOANS;
   VLTAB_OPERATIONS        := PTAB_OPERATIONS;
   VLTAB_OPERATIONS_DETAIL := PTAB_OPERATIONS_DETAIL;
   VLTAB_BALANCES          := PTAB_BALANCES;
   VLTAB_BALANCES_DETAIL   := PTAB_BALANCES_DETAIL;

   WHILE (VL_I IS NOT NULL) LOOP
      BEGIN

         VLTAB_OPERATIONS_BY_LOAN := SC_CREDIT.TYP_TAB_BTC_OPERATION();
         VLTAB_OPERATIONS_DET_BY_LOAN := SC_CREDIT.TYP_TAB_BTC_DETAIL();
         VLTAB_BALANCES_BY_LOAN := SC_CREDIT.TYP_TAB_BTC_BALANCE();
         VLTAB_BALANCES_DET_BY_LOAN := SC_CREDIT.TYP_TAB_BTC_DETAIL();

         IF(PTAB_INTEREST(VL_I).FI_BAN_OPERATION = CSL_1)THEN
            --TAB BY LOAN ASSIGNMENT
            <<loopLoanAssignment>>
            WHILE VLTAB_LOANS.COUNT > CSL_0 AND PTAB_INTEREST.EXISTS(VL_I) LOOP
               IF VLTAB_LOANS(VLTAB_LOANS.FIRST).FI_LOAN_ID IS NULL AND VLTAB_LOANS(VLTAB_LOANS.FIRST).FI_ADMIN_CENTER_ID IS NULL THEN
                  VLTAB_LOANS.DELETE(VLTAB_LOANS.FIRST);
               ELSIF VLTAB_LOANS(VLTAB_LOANS.FIRST).FI_LOAN_ID = PTAB_INTEREST(VL_I).FI_LOAN_ID
                  AND VLTAB_LOANS(VLTAB_LOANS.FIRST).FI_ADMIN_CENTER_ID = PTAB_INTEREST(VL_I).FI_ADMIN_CENTER_ID THEN

                  VLREC_LOAN := VLTAB_LOANS(VLTAB_LOANS.FIRST);
                  VLTAB_LOANS.DELETE(VLTAB_LOANS.FIRST);
               ELSE
                  EXIT loopLoanAssignment;
               END IF;
            END LOOP loopLoanAssignment;

            --TAB BY OPERATION ASSIGNMENT
            <<loopOperationAssignment>>
            WHILE VLTAB_OPERATIONS.COUNT > CSL_0 AND PTAB_INTEREST.EXISTS(VL_I) LOOP
               IF VLTAB_OPERATIONS(VLTAB_OPERATIONS.FIRST).FI_LOAN_ID IS NULL AND VLTAB_OPERATIONS(VLTAB_OPERATIONS.FIRST).FI_ADMIN_CENTER_ID IS NULL THEN
                  VLTAB_OPERATIONS.DELETE(VLTAB_OPERATIONS.FIRST);
               ELSIF VLTAB_OPERATIONS(VLTAB_OPERATIONS.FIRST).FI_LOAN_ID = PTAB_INTEREST(VL_I).FI_LOAN_ID
                  AND VLTAB_OPERATIONS(VLTAB_OPERATIONS.FIRST).FI_ADMIN_CENTER_ID = PTAB_INTEREST(VL_I).FI_ADMIN_CENTER_ID THEN

                  VLTAB_OPERATIONS_BY_LOAN.EXTEND;
                  VLTAB_OPERATIONS_BY_LOAN(VLTAB_OPERATIONS_BY_LOAN.LAST) := VLTAB_OPERATIONS(VLTAB_OPERATIONS.FIRST);
                  VLTAB_OPERATIONS.DELETE(VLTAB_OPERATIONS.FIRST);
               ELSE
                  EXIT loopOperationAssignment;
               END IF;
            END LOOP loopOperationAssignment;

            --TAB BY OPERATION DET ASSIGNMENT
            <<loopOperationDetAssignment>>
            WHILE VLTAB_OPERATIONS_DETAIL.COUNT > CSL_0 AND PTAB_INTEREST.EXISTS(VL_I) LOOP
               IF VLTAB_OPERATIONS_DETAIL(VLTAB_OPERATIONS_DETAIL.FIRST).FI_LOAN_ID IS NULL AND VLTAB_OPERATIONS_DETAIL(VLTAB_OPERATIONS_DETAIL.FIRST).FI_ADMIN_CENTER_ID IS NULL THEN
                  VLTAB_OPERATIONS_DETAIL.DELETE(VLTAB_OPERATIONS_DETAIL.FIRST);
               ELSIF VLTAB_OPERATIONS_DETAIL(VLTAB_OPERATIONS_DETAIL.FIRST).FI_LOAN_ID = PTAB_INTEREST(VL_I).FI_LOAN_ID
                  AND VLTAB_OPERATIONS_DETAIL(VLTAB_OPERATIONS_DETAIL.FIRST).FI_ADMIN_CENTER_ID = PTAB_INTEREST(VL_I).FI_ADMIN_CENTER_ID THEN

                  VLTAB_OPERATIONS_DET_BY_LOAN.EXTEND;
                  VLTAB_OPERATIONS_DET_BY_LOAN(VLTAB_OPERATIONS_DET_BY_LOAN.LAST) := VLTAB_OPERATIONS_DETAIL(VLTAB_OPERATIONS_DETAIL.FIRST);
                  VLTAB_OPERATIONS_DETAIL.DELETE(VLTAB_OPERATIONS_DETAIL.FIRST);
               ELSE
                  EXIT loopOperationDetAssignment;
               END IF;
            END LOOP loopOperationDetAssignment;

            --TAB BY BALANCES ASSIGNMENT
            <<loopBalanceAssignment>>
            WHILE VLTAB_BALANCES.COUNT > CSL_0 AND PTAB_INTEREST.EXISTS(VL_I) LOOP
               IF VLTAB_BALANCES(VLTAB_BALANCES.FIRST).FI_LOAN_ID IS NULL AND VLTAB_BALANCES(VLTAB_BALANCES.FIRST).FI_ADMIN_CENTER_ID IS NULL THEN
                  VLTAB_BALANCES.DELETE(VLTAB_BALANCES.FIRST);
               ELSIF VLTAB_BALANCES(VLTAB_BALANCES.FIRST).FI_LOAN_ID = PTAB_INTEREST(VL_I).FI_LOAN_ID
                  AND VLTAB_BALANCES(VLTAB_BALANCES.FIRST).FI_ADMIN_CENTER_ID = PTAB_INTEREST(VL_I).FI_ADMIN_CENTER_ID THEN

                  VLTAB_BALANCES_BY_LOAN.EXTEND;
                  VLTAB_BALANCES_BY_LOAN(VLTAB_BALANCES_BY_LOAN.LAST) := VLTAB_BALANCES(VLTAB_BALANCES.FIRST);
                  VLTAB_BALANCES.DELETE(VLTAB_BALANCES.FIRST);
               ELSE
                  EXIT loopBalanceAssignment;
               END IF;
            END LOOP loopBalanceAssignment;

            --TAB BY BALANCES DET ASSIGNMENT
            <<loopBalanceDetAssignment>>
            WHILE VLTAB_BALANCES_DETAIL.COUNT > CSL_0 AND PTAB_INTEREST.EXISTS(VL_I) LOOP
               IF VLTAB_BALANCES_DETAIL(VLTAB_BALANCES_DETAIL.FIRST).FI_LOAN_ID IS NULL AND VLTAB_BALANCES_DETAIL(VLTAB_BALANCES_DETAIL.FIRST).FI_ADMIN_CENTER_ID IS NULL THEN
                  VLTAB_BALANCES_DETAIL.DELETE(VLTAB_BALANCES_DETAIL.FIRST);
               ELSIF VLTAB_BALANCES_DETAIL(VLTAB_BALANCES_DETAIL.FIRST).FI_LOAN_ID = PTAB_INTEREST(VL_I).FI_LOAN_ID
                  AND VLTAB_BALANCES_DETAIL(VLTAB_BALANCES_DETAIL.FIRST).FI_ADMIN_CENTER_ID = PTAB_INTEREST(VL_I).FI_ADMIN_CENTER_ID THEN

                  VLTAB_BALANCES_DET_BY_LOAN.EXTEND;
                  VLTAB_BALANCES_DET_BY_LOAN(VLTAB_BALANCES_DET_BY_LOAN.LAST) := VLTAB_BALANCES_DETAIL(VLTAB_BALANCES_DETAIL.FIRST);
                  VLTAB_BALANCES_DETAIL.DELETE(VLTAB_BALANCES_DETAIL.FIRST);
               ELSE
                  EXIT loopBalanceDetAssignment;
               END IF;
            END LOOP loopBalanceDetAssignment;

            --EXECUTE PROCESS TO AFFECT LOAN, OPERATIONS AND BALANCES
            SC_CREDIT.SP_BTC_EXE_OPERATION_BALANCE(
               VLREC_LOAN
               ,VLTAB_OPERATIONS_BY_LOAN
               ,VLTAB_OPERATIONS_DET_BY_LOAN
               ,VLTAB_BALANCES_BY_LOAN
               ,VLTAB_BALANCES_DET_BY_LOAN
               ,PA_DEVICE
               ,PA_GPS_LATITUDE
               ,PA_GPS_LONGITUDE
               ,CSL_0
               ,VL_STATUS_CODE
               ,VL_STATUS_MSG);

            IF(VL_STATUS_CODE != CSL_0)THEN
               RAISE_APPLICATION_ERROR( CSL_CODE_ERROR, CSL_GEN_OPERATION || CSL_SPACE || VL_STATUS_MSG);
            END IF;

         END IF;

         --Insert in the table of interest
         SC_CREDIT.SP_BTC_INS_DAILY_INTEREST
            (PTAB_INTEREST(VL_I).FI_LOAN_ID
            ,PTAB_INTEREST(VL_I).FI_ADMIN_CENTER_ID
            ,PTAB_INTEREST(VL_I).FI_PAYMENT_NUMBER_ID
            ,PTAB_INTEREST(VL_I).FI_DAYS_ACUM_BY_TERM
            ,PTAB_INTEREST(VL_I).FN_DAILY_INTEREST
            ,PTAB_INTEREST(VL_I).FN_ACCRUED_INTEREST_BALANCE
            ,PTAB_INTEREST(VL_I).FN_ACCRUED_INTEREST_LOAN
            ,PTAB_INTEREST(VL_I).FN_PAYMENT_INTEREST
            ,PTAB_INTEREST(VL_I).FC_CONDITION_INTEREST
            ,PTAB_INTEREST(VL_I).FD_OPERATION_DATE
            ,PTAB_INTEREST(VL_I).FD_APPLICATION_DATE
            ,PTAB_INTEREST(VL_I).FI_TRANSACTION
            ,CSL_0
            ,VL_STATUS_CODE
            ,VL_STATUS_MSG);

         IF(VL_STATUS_CODE != CSL_0)THEN
            RAISE_APPLICATION_ERROR( CSL_CODE_ERROR, CSL_INS_LOAN_INTEREST || CSL_SPACE || VL_STATUS_MSG);
         END IF;

         --DELETE TYPES BY LOAN (CYCLE)
         VLTAB_OPERATIONS_BY_LOAN.DELETE;
         VLTAB_OPERATIONS_DET_BY_LOAN.DELETE;
         VLTAB_BALANCES_BY_LOAN.DELETE;
         VLTAB_BALANCES_DET_BY_LOAN.DELETE;

         PA_RECORDS_SUCCESS := PA_RECORDS_SUCCESS + CSL_1;

      EXCEPTION
         WHEN OTHERS THEN
            ROLLBACK;
            SC_CREDIT.SP_BATCH_ERROR_LOG(
               UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP)
               ,SQLCODE
               ,SQLERRM
               ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
               ,PTAB_INTEREST(VL_I).FI_TRANSACTION--TODO VALIDATE
               ,PTAB_INTEREST(VL_I).FI_ADMIN_CENTER_ID
                  ||CSL_COMMA
                  ||PTAB_INTEREST(VL_I).FI_LOAN_ID);

            PA_RECORDS_ERROR :=PA_RECORDS_ERROR + CSL_1;

            --Adding to a collection, loans with error, detail
            VLTAB_ERRORS.EXTEND;
            VLTAB_ERRORS(VLTAB_ERRORS.LAST) :=
               SC_CREDIT.TYP_REC_BTC_ERROR(
                  PTAB_INTEREST(VL_I).FI_ADMIN_CENTER_ID
                  ,PTAB_INTEREST(VL_I).FI_LOAN_ID
                  ,UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP)
                  ,SQLCODE
                  ,SQLERRM
                  ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                  ,SYSDATE
                  ,PTAB_INTEREST(VL_I).FI_TRANSACTION
                  ,NULL);
      END;
      VL_I := PTAB_INTEREST.NEXT(VL_I);
      COMMIT;
   END LOOP;

   COMMIT;

   VL_DESC_COUNT := ' TAB_LOANS '||VLTAB_LOANS.COUNT
                  ||' TAB_OPERATIONS '||VLTAB_OPERATIONS.COUNT
                  ||' TAB_OPERATIONS_DETAIL '||VLTAB_OPERATIONS_DETAIL.COUNT
                  ||' TAB_BALANCES '||VLTAB_BALANCES.COUNT
                  ||' TAB_BALANCES_DETAIL '||VLTAB_BALANCES_DETAIL.COUNT;

   --DELETE TYPES INTERNS
   VLTAB_LOANS.DELETE;
   VLTAB_OPERATIONS.DELETE;
   VLTAB_OPERATIONS_DETAIL.DELETE;
   VLTAB_BALANCES.DELETE;
   VLTAB_BALANCES_DETAIL.DELETE;

   PTAB_ERROR_RECORDS := VLTAB_ERRORS;
   IF(PA_RECORDS_ERROR > CSL_0)THEN
      PA_STATUS_CODE := CSL_1;
      PA_STATUS_MSG := CSL_SUCCESS_ERROR;
   END IF;

   VL_T2 := systimestamp;
   PA_STATUS_MSG := PA_STATUS_MSG
      || ' ' || 'Elapsed Seconds: '||TO_CHAR(VL_T2-VL_T1, 'SSSS.FF')
      || ' ' || VL_DESC_COUNT;

EXCEPTION
   WHEN OTHERS THEN
      PA_STATUS_CODE := SQLCODE;
      PA_STATUS_MSG := SQLERRM || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;

      SC_CREDIT.SP_BATCH_ERROR_LOG(UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP)
                                  ,SQLCODE
                                  ,SQLERRM
                                  ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                  ,CSL_0
                                  ,NULL);
END SP_BTC_APPLY_DAILY_INTEREST;

/

  GRANT EXECUTE ON SC_CREDIT.SP_BTC_APPLY_DAILY_INTEREST TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_BTC_APPLY_DAILY_INTEREST TO USRBTCCREDIT1;

--------------------------------------------------------
--  DDL for Procedure SP_BTC_SEL_NO_PAYMENT
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_BTC_SEL_NO_PAYMENT 
   (PA_FIRST_CENTER_ID        IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE
   ,PA_END_CENTER_ID          IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE
   ,PA_TODAY                  IN VARCHAR2
   ,PA_STATUS_CODE            OUT NUMBER
   ,PA_STATUS_MSG             OUT VARCHAR2
   ,PA_CUR_SELECT             OUT SC_CREDIT.PA_TYPES.TYP_CURSOR)
 IS
      /* **************************************************************
      * CREATOR: Eduardo Cervantes Hernandez
      * CREATED DATE:   07/11/2024
      * DESCRIPTION: Select loan delinquent
      * APPLICATION:  Process Batch of Purpose
      * MODIFICATION DATE: 23/01/2025
	  * [NCPADC-4631-1V2]
      ************************************************************** */

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
                  ,LO.FI_COUNTRY_ID                     AS FI_COUNTRY_ID
                  ,LO.FI_COMPANY_ID                     AS FI_COMPANY_ID
                  ,LO.FI_BUSINESS_UNIT_ID               AS FI_BUSINESS_UNIT_ID
                  ,LO.FI_CURRENT_BALANCE_SEQ            AS FI_CURRENT_BALANCE_SEQ
                  ,LO.FI_LOAN_STATUS_ID                 AS FI_LOAN_STATUS_ID
                  ,LO.FI_PRODUCT_ID                     AS FI_PRODUCT_ID
                  ,LO.FI_RULE_ID                        AS FI_RULE_ID
                  ,LO.FC_UUID_TRACKING                  AS FC_UUID_TRACKING
                  ,LO.FN_PRINCIPAL_BALANCE              AS FN_PRINCIPAL_BALANCE
                  ,LO.FN_FINANCE_CHARGE_BALANCE         AS FN_FINANCE_CHARGE_BALANCE
                  ,LO.FN_ADDITIONAL_CHARGE_BALANCE      AS FN_ADDITIONAL_CHARGE_BALANCE
                  ,LO.FC_PLATFORM_ID                    AS FC_PLATFORM_ID
                  ,LO.FC_SUB_PLATFORM_ID                AS FC_SUB_PLATFORM_ID
                  ,LO.FC_CUSTOMER_ID                    AS FC_CUSTOMER_ID
                  ,PS.FI_PAYMENT_NUMBER_ID              AS FI_PAYMENT_NUMBER_ID
                  ,TRUNC(TO_DATE(PA_TODAY, CSL_DATE) - PS.FD_DUE_DATE) AS FI_DAYS_DELINQUENT
                  ,ROW_NUMBER() OVER (PARTITION BY LO.FI_LOAN_ID ORDER BY PS.FI_PAYMENT_NUMBER_ID ASC,PS.FI_PAYMENT_NUMBER_ID ASC) AS ORDEN
              FROM SC_CREDIT.TA_LOAN LO
        INNER JOIN SC_CREDIT.TA_PAYMENT_SCHEDULE PS
                ON LO.FI_LOAN_ID = PS.FI_LOAN_ID
               AND LO.FI_ADMIN_CENTER_ID = PS.FI_ADMIN_CENTER_ID
               AND PS.FI_PMT_SCHEDULE_STATUS_ID = CSL_1
               AND PS.FD_DUE_DATE <= TO_DATE(PA_TODAY, CSL_DATE)
             WHERE LO.FI_LOAN_STATUS_ID = CSL_2
               AND LO.FI_ADMIN_CENTER_ID BETWEEN PA_FIRST_CENTER_ID AND PA_END_CENTER_ID
               AND PS.FI_PAYMENT_SCHEDULE_ID  >= CSL_1
               AND PS.FI_LOAN_ID >= CSL_1
         )
         SELECT FI_LOAN_ID                                  AS FI_LOAN_ID
               ,FI_ADMIN_CENTER_ID                          AS FI_ADMIN_CENTER_ID
               ,FI_COUNTRY_ID                               AS FI_COUNTRY_ID
               ,FI_COMPANY_ID                               AS FI_COMPANY_ID
               ,FI_BUSINESS_UNIT_ID                         AS FI_BUSINESS_UNIT_ID
               ,FI_CURRENT_BALANCE_SEQ                      AS FI_CURRENT_BALANCE_SEQ
               ,FI_LOAN_STATUS_ID                           AS FI_LOAN_STATUS_ID
               ,NVL(FI_PRODUCT_ID, CSL_0)                   AS FI_PRODUCT_ID
               ,FI_RULE_ID                                  AS FI_RULE_ID
               ,FI_PAYMENT_NUMBER_ID                        AS FI_PAYMENT_NUMBER_ID
               ,FI_DAYS_DELINQUENT                          AS FI_DAYS_DELINQUENT
               ,FN_PRINCIPAL_BALANCE                        AS FN_PRINCIPAL_BALANCE
               ,FN_FINANCE_CHARGE_BALANCE                   AS FN_FINANCE_CHARGE_BALANCE
               ,FN_ADDITIONAL_CHARGE_BALANCE                AS FN_ADDITIONAL_CHARGE_BALANCE
               ,SC_CREDIT.FN_SEL_LOAN_BALANCE_DET_JSON
                  (LO.FI_LOAN_ID
                  ,LO.FI_ADMIN_CENTER_ID
                  ,LO.FI_CURRENT_BALANCE_SEQ
                  ,FC_UUID_TRACKING) AS BALANCE_DET_JSON
               ,FC_PLATFORM_ID                              AS FC_PLATFORM_ID
               ,FC_SUB_PLATFORM_ID                          AS FC_SUB_PLATFORM_ID
               ,FC_CUSTOMER_ID                              AS FC_CUSTOMER_ID
           FROM TABLOAN LO
          WHERE LO.FI_DAYS_DELINQUENT >= CSL_0
            AND LO.ORDEN = CSL_1;


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
END SP_BTC_SEL_NO_PAYMENT;

/

  GRANT EXECUTE ON SC_CREDIT.SP_BTC_SEL_NO_PAYMENT TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_BTC_SEL_NO_PAYMENT TO USRBTCCREDIT1;

--------------------------------------------------------
--  DDL for Procedure SP_TMP_BTC_SEL_LATE_FEE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_TMP_BTC_SEL_LATE_FEE 
    (PA_FIRST_CENTER_ID        IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE
    ,PA_END_CENTER_ID          IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE
    ,PA_OPERATION_DATE         IN VARCHAR2
    ,PA_PROCESS                IN SC_CREDIT.TA_TMP_LOAN_PROCESS.FI_PROCESS%TYPE
    ,PA_TRACK                  IN SC_CREDIT.TA_TMP_LOAN_PROCESS.FI_TRACK%TYPE
    ,PA_STATUS_CODE            OUT NUMBER
    ,PA_STATUS_MSG             OUT VARCHAR2
    ,PA_CUR_FEES               OUT SC_CREDIT.PA_TYPES.TYP_CURSOR)
IS
    /* **************************************************************
    * PROJECT: LOAN LIFE CYCLE
    * DESCRIPTION: SELECT LOAN TO APPLY LATE FEE TMP
    * CREATED DATE: 26/12/2024
    * CREATOR: LUIS RAMIREZ
    ************************************************************** */
    CSL_DATE         CONSTANT VARCHAR2(22)   := 'MM/DD/YYYY HH24:MI:SS';
    CSL_0            CONSTANT SIMPLE_INTEGER := 0;
    CSL_1            CONSTANT SIMPLE_INTEGER := 1;
    CSL_3            CONSTANT SIMPLE_INTEGER := 3;
    CSL_MSG_SUCCESS  CONSTANT VARCHAR2(7)   := 'SUCCESS';
    CSL_SP           CONSTANT SIMPLE_INTEGER := 2;
    CSL_ARROW        CONSTANT VARCHAR2(2)    := '->';
    CSL_COMMA        CONSTANT VARCHAR2(3) := ' , ';
BEGIN
    PA_STATUS_CODE := CSL_0;
    PA_STATUS_MSG  := CSL_MSG_SUCCESS;
    PA_CUR_FEES :=  NULL;

    OPEN PA_CUR_FEES FOR
      WITH TAB_PED AS (
         SELECT TL.FI_LOAN_ID                         AS FI_LOAN_ID
               ,TL.FI_ADMIN_CENTER_ID                 AS FI_ADMIN_CENTER_ID
               ,TL.FC_CUSTOMER_ID                     AS FC_CUSTOMER_ID
               ,TL.FI_COUNTRY_ID                      AS FI_COUNTRY_ID
               ,TL.FI_COMPANY_ID                      AS FI_COMPANY_ID
               ,TL.FI_BUSINESS_UNIT_ID                AS FI_BUSINESS_UNIT_ID
               ,TL.FN_PRINCIPAL_BALANCE               AS FN_PRINCIPAL_BALANCE
               ,TL.FN_ADDITIONAL_CHARGE_BALANCE       AS FN_ADDITIONAL_CHARGE_BALANCE
               ,TL.FN_FINANCE_CHARGE_BALANCE          AS FN_FINANCE_CHARGE_BALANCE
               ,TL.FI_PRODUCT_ID                      AS FI_PRODUCT_ID
               ,TL.FI_RULE_ID                         AS FI_RULE_ID
               ,TL.FI_CURRENT_BALANCE_SEQ             AS FI_CURRENT_BALANCE_SEQ
               ,TL.FI_LOAN_STATUS_ID                  AS FI_LOAN_STATUS_ID
               ,TLSD.FI_REGISTRATION_NUMBER           AS FI_REGISTRATION_NUMBER
               ,TLSD.FI_COUNTER_DAY                   AS FI_COUNTER_DAY
               ,TLSD.FI_PAYMENT_NUMBER_ID             AS FI_PAYMENT_NUMBER_ID
               ,TLSD.FI_ACTION_DETAIL_ID              AS FI_ACTION_DETAIL_ID
               ,SC_CREDIT.FN_SEL_LOAN_BALANCE_DET_JSON
                  (TL.FI_LOAN_ID
                  , TL.FI_ADMIN_CENTER_ID
                  , TL.FI_CURRENT_BALANCE_SEQ
                  , NULL)            AS BALANCE_DET_JSON
               ,(SELECT PS.FN_PAYMENT_BALANCE
                FROM SC_CREDIT.TA_PAYMENT_SCHEDULE PS
                WHERE PS.FI_PAYMENT_SCHEDULE_ID > CSL_0
                  AND PS.FI_LOAN_ID = TL.FI_LOAN_ID
                  AND PS.FI_ADMIN_CENTER_ID = TL.FI_ADMIN_CENTER_ID
                  AND PS.FI_PAYMENT_NUMBER_ID = TLSD.FI_PAYMENT_NUMBER_ID
                  AND PS.FI_STATUS = CSL_1)              AS FN_PAYMENT_BALANCE
               ,(SELECT COUNT(0) AS FLAG_LATE_FEE
                FROM SC_CREDIT.TA_LOAN_STATUS_DETAIL SDFEE
                WHERE SDFEE.FI_LOAN_ID = TLSD.FI_LOAN_ID
                  AND SDFEE.FI_ADMIN_CENTER_ID = TLSD.FI_ADMIN_CENTER_ID
                  AND SDFEE.FI_PAYMENT_NUMBER_ID = TLSD.FI_PAYMENT_NUMBER_ID
                  AND SDFEE.FI_ON_OFF = CSL_0
                  AND SDFEE.FI_ACTION_DETAIL_ID = CSL_1) AS FLAG_LATE_FEE
               ,TO_CHAR(TLSD.FD_INITIAL_DATE,CSL_DATE) AS FD_INITIAL_DATE
         FROM SC_CREDIT.TA_TMP_LOAN_PROCESS L
            INNER JOIN SC_CREDIT.TA_LOAN TL
                    ON TL.FI_ADMIN_CENTER_ID = L.FI_ADMIN_CENTER_ID
                   AND TL.FI_LOAN_ID = L.FI_LOAN_ID
                   AND TL.FI_LOAN_STATUS_ID = CSL_3
            INNER JOIN SC_CREDIT.TA_LOAN_STATUS_DETAIL TLSD
                    ON TLSD.FI_LOAN_ID = TL.FI_LOAN_ID
                    AND TLSD.FI_ADMIN_CENTER_ID = TL.FI_ADMIN_CENTER_ID
                    AND TLSD.FI_PAYMENT_NUMBER_ID > CSL_0
                    AND TLSD.FI_ON_OFF = CSL_1
                    AND TLSD.FI_ACTION_DETAIL_ID = CSL_3
         WHERE L.FI_PROCESS = PA_PROCESS
           AND L.FI_TRACK = PA_TRACK )
      SELECT FI_LOAN_ID
            ,FI_ADMIN_CENTER_ID
            ,FC_CUSTOMER_ID
            ,FI_COUNTRY_ID
            ,FI_COMPANY_ID
            ,FI_BUSINESS_UNIT_ID
            ,FN_PRINCIPAL_BALANCE
            ,FN_ADDITIONAL_CHARGE_BALANCE
            ,FN_FINANCE_CHARGE_BALANCE
            ,FI_PRODUCT_ID
            ,FI_RULE_ID
            ,FI_CURRENT_BALANCE_SEQ
            ,FI_LOAN_STATUS_ID
            ,FI_REGISTRATION_NUMBER
            ,FI_COUNTER_DAY
            ,FI_PAYMENT_NUMBER_ID
            ,FI_ACTION_DETAIL_ID
            ,BALANCE_DET_JSON
            ,FN_PAYMENT_BALANCE
            ,FD_INITIAL_DATE
      FROM TAB_PED
      WHERE FLAG_LATE_FEE = CSL_0;
EXCEPTION
WHEN OTHERS THEN
    PA_STATUS_CODE := SQLCODE;
    PA_STATUS_MSG := SQLERRM || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;

    SC_CREDIT.SP_BATCH_ERROR_LOG (
       UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP)
       ,SQLCODE
       ,SQLERRM
       ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
       ,CSL_0
       ,PA_FIRST_CENTER_ID || CSL_COMMA || PA_END_CENTER_ID || CSL_COMMA || PA_OPERATION_DATE
       );
END SP_TMP_BTC_SEL_LATE_FEE;

/

  GRANT EXECUTE ON SC_CREDIT.SP_TMP_BTC_SEL_LATE_FEE TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_TMP_BTC_SEL_LATE_FEE TO USRBTCCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_TMP_BTC_SEL_LATE_FEE TO USRCREDIT02;
--------------------------------------------------------
--  DDL for Procedure SP_BTC_APPLY_ACTIONS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_BTC_APPLY_ACTIONS 
 (PTAB_STATUS_DETAIL             IN SC_CREDIT.TYP_TAB_BTC_STATUS_DETAIL
    ,PA_STATUS_CODE              OUT NUMBER
    ,PA_STATUS_MSG               OUT VARCHAR2
    ,PA_RECORDS_READ             OUT NUMBER
    ,PA_RECORDS_SUCCESS          OUT NUMBER
    ,PA_RECORDS_ERROR            OUT NUMBER
    ,PTAB_ERROR_RECORDS          OUT SC_CREDIT.TYP_TAB_BTC_ERROR)
AS
    /* **************************************************************
    * PROJECT: LOAN-LIFE-CYCLE
    * DESCRIPTION: PROCESS TO GENERATE ACTIONS
    * CREATED DATE: 14/11/2024
    * CREATOR: ITZEL TRINIDAD RAMOS/CRISTHIAN MORALES
    * MODIFICATION DATE: 05/12/2024
    ************************************************************** */
    CSL_0            CONSTANT SIMPLE_INTEGER := 0;
    CSL_1            CONSTANT SIMPLE_INTEGER := 1;
    CSL_2            CONSTANT SIMPLE_INTEGER := 2;
    CSL_4            CONSTANT SIMPLE_INTEGER := 4;
    CSL_NUMERROR     CONSTANT SIMPLE_INTEGER := -20012;
    CSL_STATUS_DETAIL CONSTANT VARCHAR2(30) := 'SP_BTC_INS_LOAN_STATUS_DETAIL';
    CSL_UPD_STATUS_DETAIL CONSTANT VARCHAR2(30) := 'SP_BTC_UPD_LOAN_STATUS_DETAIL';
    CSL_UPD_LOAN     CONSTANT VARCHAR2(15) := 'SP_BTC_UPD_LOAN';
    CSL_MSG_SUCCESS  CONSTANT VARCHAR2(7)   := 'SUCCESS';
    CSG_SUCCESS_ERROR CONSTANT VARCHAR2(30) := 'SUCCESS, WITH ERRORS RECORDS';
    CSL_TYPE_NULL    CONSTANT VARCHAR2(23) := 'TYPE STATUS DETAIL NULL';
    CSL_PKG          CONSTANT SIMPLE_INTEGER := 1;
    CSL_ARROW        CONSTANT VARCHAR2(2)    := '->';
    CSL_SPACE        CONSTANT VARCHAR2(1) := ' ';
    CSL_COMMA        CONSTANT VARCHAR2(3) := ' , ';

    VL_I                          NUMBER(10,0) := 0;
    VL_TAB_ERRORS                 SC_CREDIT.TYP_TAB_BTC_ERROR;
    VL_STATUS_CODE                NUMBER(10,0) := 0;
    VL_STATUS_MSG                 VARCHAR2(1000);
    VL_PROCESS_DESC               VARCHAR2(150);
BEGIN
    PA_STATUS_CODE := CSL_0;
    PA_STATUS_MSG  := CSL_MSG_SUCCESS;
    PA_RECORDS_SUCCESS := CSL_0;
    PA_RECORDS_ERROR := CSL_0;
    PA_RECORDS_READ := CSL_0;
    VL_TAB_ERRORS := SC_CREDIT.TYP_TAB_BTC_ERROR();

    IF PTAB_STATUS_DETAIL IS NULL THEN
     RAISE_APPLICATION_ERROR( CSL_NUMERROR, CSL_TYPE_NULL);
    END IF;

    VL_I := PTAB_STATUS_DETAIL.FIRST;
    PA_RECORDS_READ := PTAB_STATUS_DETAIL.COUNT;

    WHILE (VL_I IS NOT NULL) LOOP
    BEGIN
        IF PTAB_STATUS_DETAIL(VL_I).FI_OPTION = CSL_1 THEN
           IF PTAB_STATUS_DETAIL(VL_I).FI_ON_OFF = CSL_1 THEN
                --UPDATE COUNT DAYS BY INSTALLMENT
                VL_PROCESS_DESC := CSL_UPD_STATUS_DETAIL;
                SC_CREDIT.SP_BTC_UPD_LOAN_STATUS_DETAIL
                   (PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID
                   ,PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID
                   ,PTAB_STATUS_DETAIL(VL_I).FI_PAYMENT_NUMBER_ID
                   ,PTAB_STATUS_DETAIL(VL_I).FI_COUNTER_DAY
                   ,CSL_1
                   ,NULL
                   ,CSL_1
                   ,CSL_0
                   ,VL_STATUS_CODE
                   ,VL_STATUS_MSG);

                IF(VL_STATUS_CODE != CSL_0)THEN
                   RAISE_APPLICATION_ERROR( CSL_NUMERROR, VL_PROCESS_DESC || CSL_SPACE || VL_STATUS_MSG);
                END IF;
            END IF;
        ELSE
            IF PTAB_STATUS_DETAIL(VL_I).FI_OPTION = CSL_2 THEN
                --OFF PREVIOUS ACTIONS BY INSTALLMENT
                VL_PROCESS_DESC := CSL_UPD_STATUS_DETAIL;
                SC_CREDIT.SP_BTC_UPD_LOAN_STATUS_DETAIL
                   (PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID
                   ,PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID
                   ,PTAB_STATUS_DETAIL(VL_I).FI_PAYMENT_NUMBER_ID
                   ,NULL
                   ,CSL_0
                   ,NULL
                   ,CSL_1
                   ,CSL_0
                   ,VL_STATUS_CODE
                   ,VL_STATUS_MSG);

                IF(VL_STATUS_CODE != CSL_0)THEN
                   RAISE_APPLICATION_ERROR( CSL_NUMERROR, VL_PROCESS_DESC || CSL_SPACE || VL_STATUS_MSG);
                END IF;
            ELSE
                --OFF PREVIOUS ACTIONS BY LOAN
                VL_PROCESS_DESC := CSL_UPD_STATUS_DETAIL;
                SC_CREDIT.SP_BTC_UPD_LOAN_STATUS_DETAIL
                   (PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID
                   ,PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID
                   ,PTAB_STATUS_DETAIL(VL_I).FI_PAYMENT_NUMBER_ID
                   ,NULL
                   ,CSL_0
                   ,NULL
                   ,CSL_0
                   ,CSL_0
                   ,VL_STATUS_CODE
                   ,VL_STATUS_MSG);

                IF(VL_STATUS_CODE != CSL_0)THEN
                   RAISE_APPLICATION_ERROR( CSL_NUMERROR, VL_PROCESS_DESC || CSL_SPACE || VL_STATUS_MSG);
                END IF;

                --UPDATE STATUS DEFAULT
                VL_PROCESS_DESC := CSL_UPD_LOAN;
                SC_CREDIT.SP_BTC_UPD_LOAN
                (PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID
                ,PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID
                ,CSL_4
                ,CSL_0
                ,VL_STATUS_CODE
                ,VL_STATUS_MSG
                );

                IF(VL_STATUS_CODE != CSL_0)THEN
                   RAISE_APPLICATION_ERROR( CSL_NUMERROR, VL_PROCESS_DESC || CSL_SPACE || VL_STATUS_MSG);
                END IF;
            END IF;

            --INSERT ACTION(Cure Period Non-compliance)
            VL_PROCESS_DESC := CSL_STATUS_DETAIL;
            SC_CREDIT.SP_BTC_INS_LOAN_STATUS_DETAIL
               (PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID
               ,PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID
               ,PTAB_STATUS_DETAIL(VL_I).FI_LOAN_STATUS_ID
               ,PTAB_STATUS_DETAIL(VL_I).FI_ACTION_DETAIL_ID
               ,PTAB_STATUS_DETAIL(VL_I).FI_COUNTER_DAY
               ,PTAB_STATUS_DETAIL(VL_I).FD_INITIAL_DATE
               ,PTAB_STATUS_DETAIL(VL_I).FI_PAYMENT_NUMBER_ID
               ,PTAB_STATUS_DETAIL(VL_I).FD_FINAL_DATE
               ,PTAB_STATUS_DETAIL(VL_I).FI_ON_OFF
               ,CSL_0
               ,VL_STATUS_CODE
               ,VL_STATUS_MSG);

            IF(VL_STATUS_CODE != CSL_0)THEN
               RAISE_APPLICATION_ERROR( CSL_NUMERROR, VL_PROCESS_DESC || CSL_SPACE || VL_STATUS_MSG);
            END IF;
        END IF;

        PA_RECORDS_SUCCESS := PA_RECORDS_SUCCESS + CSL_1;
     --Exception block
     EXCEPTION
        WHEN OTHERS THEN
           ROLLBACK;
             SC_CREDIT.SP_BATCH_ERROR_LOG(UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_PKG)
                                         ,SQLCODE
                                         ,SQLERRM
                                         ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                         ,CSL_1
                                         ,PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID || CSL_COMMA || PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID);

             PA_RECORDS_ERROR :=PA_RECORDS_ERROR + CSL_1;

             --Adding to a collection, loans with error, detail
             VL_TAB_ERRORS.EXTEND;
             VL_TAB_ERRORS(VL_TAB_ERRORS.LAST) :=
             SC_CREDIT.TYP_REC_BTC_ERROR(PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID
                                        ,PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID
                                        ,UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_PKG)
                                        ,SQLCODE
                                        ,SQLERRM
                                        ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                        ,SYSDATE
                                        ,CSL_1
                                        ,NULL);
     END;

     VL_I := PTAB_STATUS_DETAIL.NEXT(VL_I);
     COMMIT;
    END LOOP;

    PTAB_ERROR_RECORDS := VL_TAB_ERRORS;
    IF(PA_RECORDS_ERROR > CSL_0)THEN
        PA_STATUS_CODE := CSL_1;
        PA_STATUS_MSG := CSG_SUCCESS_ERROR;
    END IF;
EXCEPTION
  WHEN OTHERS THEN
     PA_STATUS_CODE := SQLCODE;
     PA_STATUS_MSG := SQLERRM || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;

    SC_CREDIT.SP_BATCH_ERROR_LOG(UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_PKG)
                                     ,SQLCODE
                                     ,SQLERRM
                                     ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                     ,CSL_0
                                     ,NULL);
END SP_BTC_APPLY_ACTIONS;

/

  GRANT EXECUTE ON SC_CREDIT.SP_BTC_APPLY_ACTIONS TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_BTC_APPLY_ACTIONS TO USRBTCCREDIT1;

--------------------------------------------------------
--  DDL for Procedure SP_DELETE_LOAN
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_DELETE_LOAN (
    pa_loan_id         sc_credit.ta_loan.fi_loan_id%TYPE,
    pa_admin_center_id sc_credit.ta_loan.fi_admin_center_id%TYPE,
    pa_status_code     OUT NUMBER,
    pa_status_message  OUT VARCHAR2
) IS
/**********************************************************************************************************************************************
PROJECT:            LOAN MANAGEMENT SYSTEM
DESCRIPTION:        QA. This procedure do erase all the information about a loan.
CREATED DATE:       2024-12-10
CREATOR:            Ricardo Guti??rrez M.
MODIFICATION DATE:  2024-12-10
USER MODIFICATION : Ricardo Guti??rrez M.
***********************************************************************************************************************************************/
BEGIN
pa_status_code := 0;

    DELETE sc_credit.ta_loan_balance_detail
    WHERE
        ( fi_loan_balance_id, fi_admin_center_id ) IN (
            SELECT
                fi_loan_balance_id, fi_admin_center_id
            FROM
                sc_credit.ta_loan_balance
            WHERE
                    fi_loan_id = pa_loan_id
                AND fi_admin_center_id = pa_admin_center_id
        );

    DELETE sc_credit.ta_loan_balance
    WHERE
            fi_loan_id = pa_loan_id
        AND fi_admin_center_id = pa_admin_center_id;

    DELETE sc_credit.ta_loan_interest
    WHERE
            fi_loan_id = pa_loan_id
        AND fi_admin_center_id = pa_admin_center_id;

    DELETE sc_credit.ta_loan_operation_tender
    WHERE
        ( fi_loan_operation_id, fi_admin_center_id ) IN (
            SELECT
                fi_loan_operation_id, fi_admin_center_id
            FROM
                sc_credit.ta_loan_operation
            WHERE
                    fi_loan_id = pa_loan_id
                AND fi_admin_center_id = pa_admin_center_id
        );

    DELETE sc_credit.ta_loan_operation_detail
    WHERE
        ( fi_loan_operation_id, fi_admin_center_id ) IN (
            SELECT
                fi_loan_operation_id, fi_admin_center_id
            FROM
                sc_credit.ta_loan_operation
            WHERE
                    fi_loan_id = pa_loan_id
                AND fi_admin_center_id = pa_admin_center_id
        );

    DELETE sc_credit.ta_loan_operation_tender
    WHERE
        ( fi_loan_operation_id, fi_admin_center_id ) IN (
            SELECT
                fi_loan_operation_id, fi_admin_center_id
            FROM
                sc_credit.ta_loan_operation
            WHERE
                    fi_loan_id = pa_loan_id
                AND fi_admin_center_id = pa_admin_center_id
        );

    DELETE sc_credit.ta_loan_status
    WHERE
            fi_loan_id = pa_loan_id
        AND fi_admin_center_id = pa_admin_center_id;

    DELETE sc_credit.ta_loan_operation
    WHERE
            fi_loan_id = pa_loan_id
        AND fi_admin_center_id = pa_admin_center_id;

    DELETE sc_credit.ta_loan_status_detail
    WHERE
            fi_admin_center_id = pa_admin_center_id
        AND fi_loan_id = pa_loan_id;

    DELETE sc_credit.ta_payment_schedule
    WHERE
            fi_loan_id = pa_loan_id
        AND fi_admin_center_id = pa_admin_center_id;

    DELETE sc_credit.ta_payment_type_detail
    WHERE
            fi_loan_id = pa_loan_id
        AND fi_admin_center_id = pa_admin_center_id;

    DELETE sc_credit.ta_loan
    WHERE
            fi_loan_id = pa_loan_id
        AND fi_admin_center_id = pa_admin_center_id;

    COMMIT;

EXCEPTION
 WHEN NO_DATA_FOUND THEN
      ROLLBACK;

         PA_STATUS_CODE    := sqlcode;
         PA_STATUS_MESSAGE := sqlerrm;
                 SC_CREDIT.SP_ERROR_LOG('SP_DELETE_LOAN', SQLCODE, SQLERRM,
         DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, '','' );

   WHEN OTHERS THEN
      ROLLBACK;

         PA_STATUS_CODE    := sqlcode;
         PA_STATUS_MESSAGE := sqlerrm;
                 SC_CREDIT.SP_ERROR_LOG('SP_DELETE_LOAN', SQLCODE, SQLERRM,
         DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, '','' );



END sp_delete_loan;

/

  GRANT EXECUTE ON SC_CREDIT.SP_DELETE_LOAN TO USRNCPCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_EXE_LOAN_BALANCE_INFO
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_EXE_LOAN_BALANCE_INFO (
    PA_LOAN_ID                  SC_CREDIT.TA_PAYMENT_SCHEDULE.FI_LOAN_ID%TYPE,
    PA_CUR_LOAN                 OUT SYS_REFCURSOR,
    PA_CUR_BALANCE              OUT SYS_REFCURSOR,
    PA_STATUS_CODE              OUT NUMBER,
    PA_STATUS_MSG               OUT VARCHAR2)

  AS
  -- GLOBAL CONSTANTS
  CSG_0                     CONSTANT SIMPLE_INTEGER := 0;
  CSG_1                     CONSTANT SIMPLE_INTEGER := 1;
  CSG_ARROW                 CONSTANT VARCHAR2(5) := ' -> ';
  CSG_X                     CONSTANT VARCHAR2(5) := 'X';
  CSG_SUCCESS_CODE          CONSTANT SIMPLE_INTEGER := 0;
  CSG_SUCCESS_MSG           CONSTANT VARCHAR2(10) := 'SUCCESS';
  CSG_NO_DATA_FOUND_CODE    CONSTANT SIMPLE_INTEGER := -20204;
  CSG_NO_DATA_FOUND_MSG     CONSTANT VARCHAR2(50) := 'THE DATA DOES NOT EXIST';
  CSG_SP_EXE_LOAN_BALANCE_INFO  CONSTANT VARCHAR2(50) := 'SP_EXE_LOAN_BALANCE_INFO';
/*************************************************************
  PROJECT    :  NCP-OUTSTANDING BALANCE
  DESCRIPTION:  STORED PROCEDURE TO SELECT A LOAN
  CREATOR:      JOSE DE JESUS BRAVO AGUILAR.
  CREATED DATE: AGO-02-2024
  MODIFICATION: RICARDO HAZAEL GOMEZ ALVAREZ
  MODIFICATION DATE: OCT-09-2024
*************************************************************/
    VL_LOAN_ID                  SC_CREDIT.TA_LOAN.FI_LOAN_ID%TYPE;

    EXC_NO_DATA_FOUND           EXCEPTION;
    PRAGMA EXCEPTION_INIT (EXC_NO_DATA_FOUND, CSG_NO_DATA_FOUND_CODE);

  BEGIN
    PA_STATUS_CODE := CSG_SUCCESS_CODE;
    PA_STATUS_MSG := CSG_SUCCESS_MSG;

  SELECT COUNT(CSG_1)
  INTO VL_LOAN_ID
  FROM SC_CREDIT.TA_LOAN
  WHERE FI_LOAN_ID = PA_LOAN_ID;
  IF VL_LOAN_ID = CSG_0 THEN
    RAISE EXC_NO_DATA_FOUND;
  END IF;

  OPEN PA_CUR_LOAN  FOR
  SELECT
    FI_LOAN_ID,
    FI_COUNTRY_ID,
    FI_COMPANY_ID,
    FI_BUSINESS_UNIT_ID,
    FI_ADMIN_CENTER_ID,
    FI_ORIGINATION_CENTER_ID,
    FC_PLATFORM_ID,
    FC_SUB_PLATFORM_ID,
    FC_CUSTOMER_ID,
    FI_PRODUCT_ID,
    FN_PRINCIPAL_AMOUNT,
    FN_FINANCE_CHARGE_AMOUNT,
    FN_PRINCIPAL_BALANCE,
    FN_FINANCE_CHARGE_BALANCE,
    FN_ADDITIONAL_CHARGE_BALANCE,
    FD_ORIGINATION_DATE,
    FD_FIRST_PAYMENT,
    FD_DUE_DATE,
    FN_APR,
    FI_ADDITIONAL_STATUS,
    FI_CURRENT_BALANCE_SEQ,
    FN_INTEREST_RATE,
    FI_NUMBER_OF_PAYMENTS,
    FI_TERM_TYPE,
    FI_LOAN_STATUS_ID,
    FI_ACCRUED_TYPE_ID,
    FC_END_USER,
    FC_UUID_TRACKING,
    FC_IP_ADDRESS,
    FC_DEVICE
  FROM SC_CREDIT.TA_LOAN
  WHERE FI_LOAN_ID = PA_LOAN_ID;

  OPEN PA_CUR_BALANCE FOR
  SELECT
    LC.FC_LOAN_CONCEPT_DESC,
    LBD.FN_ITEM_AMOUNT
  FROM
    SC_CREDIT.TA_LOAN L
  LEFT OUTER JOIN SC_CREDIT.TA_LOAN_BALANCE LB
  ON L.FI_LOAN_ID = LB.FI_LOAN_ID
  AND L.FI_CURRENT_BALANCE_SEQ = LB.FI_BALANCE_SEQ
  LEFT OUTER JOIN SC_CREDIT.TA_LOAN_BALANCE_DETAIL LBD
  ON LB.FI_LOAN_BALANCE_ID = LBD.FI_LOAN_BALANCE_ID
  LEFT OUTER JOIN SC_CREDIT.TC_LOAN_CONCEPT LC
  ON LBD.FI_LOAN_CONCEPT_ID = LC.FI_LOAN_CONCEPT_ID
  WHERE
    L.FI_LOAN_ID = PA_LOAN_ID
    ORDER BY
    LB.FI_LOAN_BALANCE_ID ASC;

  EXCEPTION
  WHEN EXC_NO_DATA_FOUND THEN
    PA_STATUS_CODE := CSG_NO_DATA_FOUND_CODE;
    PA_STATUS_MSG := CSG_NO_DATA_FOUND_MSG || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    OPEN PA_CUR_LOAN FOR SELECT
    NULL AS FI_COUNTRY_ID,
    NULL AS FI_COMPANY_ID,
    NULL AS FI_BUSINESS_UNIT_ID,
    NULL AS FI_ADMIN_CENTER_ID,
    NULL AS FI_LOAN_ID,
    NULL AS FI_ORIGINATION_CENTER_ID,
    NULL AS FC_PLATFORM_ID,
    NULL AS FC_SUB_PLATFORM_ID,
    NULL AS FC_CUSTOMER_ID,
    NULL AS FI_PRODUCT_ID,
    NULL AS FN_PRINCIPAL_AMOUNT,
    NULL AS FN_FINANCE_CHARGE_AMOUNT,
    NULL AS FN_PRINCIPAL_BALANCE,
    NULL AS FN_FINANCE_CHARGE_BALANCE,
    NULL AS FN_ADDITIONAL_CHARGE_BALANCE,
    NULL AS FD_ORIGINATION_DATE,
    NULL AS FD_FIRST_PAYMENT,
    NULL AS FD_DUE_DATE,
    NULL AS FN_APR,
    NULL AS FI_ADDITIONAL_STATUS,
    NULL AS FI_CURRENT_BALANCE_SEQ,
    NULL AS FN_INTEREST_RATE,
    NULL AS FI_NUMBER_OF_PAYMENTS,
    NULL AS FI_TERM_TYPE,
    NULL AS FI_LOAN_STATUS,
    NULL AS FI_ACCRUED_TYPE_ID,
    NULL AS FC_END_USER,
    NULL AS FC_UUID_TRACKING,
    NULL AS FC_IP_ADDRESS,
    NULL AS FC_DEVICE
   FROM DUAL WHERE CSG_1 = CSG_0;

     OPEN PA_CUR_BALANCE FOR SELECT
    NULL AS FC_CONCEPT,
    NULL AS FN_ITEM_AMOUNT
    FROM DUAL WHERE CSG_1 = CSG_0;

    SC_CREDIT.SP_ERROR_LOG(CSG_SP_EXE_LOAN_BALANCE_INFO, SQLCODE, SQLERRM,
      DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, CSG_X);

  WHEN OTHERS THEN
    PA_STATUS_CODE := SQLCODE;
    PA_STATUS_MSG := SQLERRM || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    SC_CREDIT.SP_ERROR_LOG(CSG_SP_EXE_LOAN_BALANCE_INFO, SQLCODE, SQLERRM,
      DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, CSG_X);

  END SP_EXE_LOAN_BALANCE_INFO;

/

  GRANT EXECUTE ON SC_CREDIT.SP_EXE_LOAN_BALANCE_INFO TO USRNCPCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_SEL_LOAN_STATUS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_SEL_LOAN_STATUS (
   PA_LOAN_ID                IN      SC_CREDIT.TA_LOAN_STATUS.FI_LOAN_ID%TYPE
  ,PA_LOAN_ADMIN_CENTER_ID   IN      SC_CREDIT.TA_LOAN_STATUS.FI_ADMIN_CENTER_ID%TYPE
  ,PA_DATE_OPERATION         IN      VARCHAR2
  ,PA_CUR_RESULTS            OUT     SC_CREDIT.PA_TYPES.TYP_CURSOR
  ,PA_STATUS_CODE            OUT     NUMBER
  ,PA_STATUS_MSG             OUT     VARCHAR2)
IS
/************************************************************************************************************************************************************************************************************************************************
PROJECT:            PURPOSE_LIFE_LOAN_CYCLE
DESCRIPTION:        THIS STORE PROCEDURE EXECUTES A CONSULT ON TA_LOAN TABLE, TA_PAYMENT_SCHEDULE, TA_LOAN_BALANCE AND TA_LOAN_BALANCE_DETAIL AND RETURNS A CURSOR WITH THE INFORMATION OF THE LOANS WITH DUE DATE SMALLER THAN LOAN_STATUS_DATE
PRECONDITIONS:      IT MUST RECEIVE A LOAN_ID AND A LOAN_ADMIN_CENTER_ID
CREATOR:            CESAR MEDINA
CREATED DATE:       13/11/2024
MODIFICATION DATE:  15/01/2025
USER MODIFICATION:  AIXA SARMIENTO
*************************************************************************************************************************************************************************************************************************************************/
   CSL_ARROW        CONSTANT VARCHAR2(5)    := '-->';					-- AESTHETICS SIGN
   CSL_DATE         CONSTANT VARCHAR2(12)   := 'MM/DD/YYYY';			-- CONSTANT FOR DATE CONVERSION
   CSL_0            CONSTANT SIMPLE_INTEGER := 0;						-- CONSTANT WITH VALUE EQUAL TO 0
   CSL_1            CONSTANT SIMPLE_INTEGER := 1;						-- CONSTANT WITH VALUE EQUAL TO 1
   CSL_3            CONSTANT SIMPLE_INTEGER := 3;						-- CONSTANT WITH VALUE EQUAL TO 3
   CSL_SUCCESS_MSG  CONSTANT VARCHAR2(10)   := 'SUCCESS';				-- MESSAGE SUCCESS
   CSL_PKG          CONSTANT SIMPLE_INTEGER := 1;						-- CONSTANT WITH VALUE EQUAL TO 1, FOR THE SP_BATCH_ERROR_LOG
   CSL_DATE_FORMAT  CONSTANT VARCHAR2(25)   := 'MM/DD/YYYY hh24:mi:ss';	-- CONSTANT FOR DATE CONVERSION WITH HOURS
   VL_JSON_PAYMENTS VARCHAR2(4000);
   VL_JSON_BALANCE_DETAIL VARCHAR2(4000);

BEGIN
   SELECT
      COALESCE(
         NULLIF(
            JSON_ARRAYAGG (
               JSON_OBJECT(
                        'paymentNumberId' VALUE B.FI_PAYMENT_NUMBER_ID,
                        'paymentAmount'   VALUE B.FN_PAYMENT_AMOUNT,
                        'dueDate'         VALUE TO_CHAR(B.FD_DUE_DATE,CSL_DATE),
                        'status'          VALUE B.FI_STATUS
               )
            ), '[{paymentNumberId:null,paymentAmount:null,dueDate:null,status:null}]'
         ),'[]'
      ) AS FJ_PAYMENTS
     INTO VL_JSON_PAYMENTS
     FROM SC_CREDIT.TA_PAYMENT_SCHEDULE B
    WHERE B.FI_LOAN_ID = PA_LOAN_ID
      AND B.FI_ADMIN_CENTER_ID = PA_LOAN_ADMIN_CENTER_ID
      AND B.FD_DUE_DATE <=  TO_DATE(PA_DATE_OPERATION,CSL_DATE_FORMAT)
      AND B.FI_PMT_SCHEDULE_STATUS_ID IN (CSL_1,CSL_3)
      AND B.FI_STATUS = CSL_1;

   SELECT
      COALESCE(
         NULLIF(
            JSON_ARRAYAGG (
               JSON_OBJECT(
                           'loanConceptId' VALUE E.FI_LOAN_CONCEPT_ID,
                           'itemAmount'    VALUE E.FN_ITEM_AMOUNT
               )
            ), '[{loanConceptId:null,itemAmount:null}]'
         ),'[]'
      ) AS FJ_BALANCE_DETAIL
      INTO VL_JSON_BALANCE_DETAIL
      FROM SC_CREDIT.TA_LOAN A
INNER JOIN SC_CREDIT.TA_LOAN_BALANCE D
        ON A.FI_LOAN_ID = D.FI_LOAN_ID
       AND A.FI_ADMIN_CENTER_ID = D.FI_ADMIN_CENTER_ID
       AND A.FI_CURRENT_BALANCE_SEQ = D.FI_BALANCE_SEQ
INNER JOIN SC_CREDIT.TA_LOAN_BALANCE_DETAIL E
        ON D.FI_LOAN_ID = E.FI_LOAN_ID
       AND D.FI_ADMIN_CENTER_ID = E.FI_ADMIN_CENTER_ID
       AND D.FI_LOAN_BALANCE_ID = E.FI_LOAN_BALANCE_ID
     WHERE D.FI_ADMIN_CENTER_ID = PA_LOAN_ADMIN_CENTER_ID
       AND D.FI_LOAN_ID = PA_LOAN_ID;

   OPEN PA_CUR_RESULTS FOR
      SELECT A.FI_LOAN_ID,
			 A.FI_ADMIN_CENTER_ID,
			 A.FI_PRODUCT_ID,
             A.FI_RULE_ID,
             A.FC_CUSTOMER_ID,
             A.FI_LOAN_STATUS_ID,
             TO_CHAR(A.FD_LOAN_STATUS_DATE,CSL_DATE_FORMAT) AS FD_LOAN_STATUS_DATE,
             A.FI_NUMBER_OF_PAYMENTS,
             A.FN_PRINCIPAL_AMOUNT,
             A.FN_PRINCIPAL_BALANCE,
             A.FN_FINANCE_CHARGE_BALANCE,
             A.FN_ADDITIONAL_CHARGE_BALANCE,
             VL_JSON_PAYMENTS AS FJ_PAYMENTS,
             C.FN_PAY_OFF_AMOUNT,
             C.FI_ADD_EXTENSION,
             C.FN_AMOUNT_PAID,
             C.FN_PWO_MIN_PAYMENT,
             C.FN_PWO_EXT_PAYMENT,
             TO_CHAR(C.FD_PWO_DATE,CSL_DATE_FORMAT) AS FD_PWO_DATE,
             VL_JSON_BALANCE_DETAIL AS FJ_BALANCE_DETAIL
        FROM SC_CREDIT.TA_LOAN A
        LEFT JOIN SC_CREDIT.TA_PWO_AMOUNT_DETAIL C
          ON C.FI_LOAN_ID = A.FI_LOAN_ID
         AND C.FI_ADMIN_CENTER_ID = A.FI_ADMIN_CENTER_ID
       WHERE A.FI_LOAN_ID = PA_LOAN_ID
         AND A.FI_ADMIN_CENTER_ID = PA_LOAN_ADMIN_CENTER_ID;

   PA_STATUS_CODE   := CSL_0;
   PA_STATUS_MSG    := CSL_SUCCESS_MSG;

EXCEPTION
    WHEN OTHERS THEN
       PA_STATUS_CODE := SQLCODE;
       PA_STATUS_MSG  := SQLERRM || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;

       SC_CREDIT.SP_BATCH_ERROR_LOG (UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_PKG)
                                     ,SQLCODE
                                     ,SQLERRM
                                     ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                     ,CSL_0
                                     ,PA_STATUS_CODE || PA_STATUS_MSG
                                     );
END SP_SEL_LOAN_STATUS;

/

  GRANT EXECUTE ON SC_CREDIT.SP_SEL_LOAN_STATUS TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_SEL_LOAN_STATUS TO USRBTCCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_UPD_LOAN_STATUS_DETAIL
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_UPD_LOAN_STATUS_DETAIL 
   (PA_LOAN_ID             IN SC_CREDIT.TA_LOAN_STATUS_DETAIL.FI_LOAN_ID%TYPE,
    PA_ADMIN_CENTER_ID     IN SC_CREDIT.TA_LOAN_STATUS_DETAIL.FI_ADMIN_CENTER_ID%TYPE,
	PA_JSON_INPUT          IN VARCHAR2,
    PA_JSON_OUPUT          OUT VARCHAR2,
    PA_STATUS_CODE         OUT NUMBER,
    PA_STATUS_MESSAGE      OUT VARCHAR2)
IS
/**********************************************************************************************************************************************
PROJECT:            PURPOSE_LIFE_LOAN_CYCLE
DESCRIPTION:        STORED PROCEDURE THAT UPDATES SETTLED PAYMENTS
PRECONDITIONS:      IT MUST RECEIVE A JSON OBJECT
CREATED DATE:       15/12/2024
CREATOR:            AIXA SARMIENTO
MODIFICATION DATE:  15/01/2025
USER MODIFICATION:  AIXA SARMIENTO
***********************************************************************************************************************************************/
   CSL_0                    	CONSTANT SIMPLE_INTEGER := 0;			-- CONSTANT WITH VALUE EQUAL TO 0
   CSL_1                    	CONSTANT SIMPLE_INTEGER := 1;		    -- CONSTANT WITH VALUE EQUAL TO 1
   CSL_3                    	CONSTANT SIMPLE_INTEGER := 3;		    -- CONSTANT WITH VALUE EQUAL TO 1
   CSL_4                        CONSTANT SIMPLE_INTEGER := 4;           -- CONSTANT WITH VALUE EQUAL TO 4
   CSL_MSG_SUCCESS          	CONSTANT VARCHAR2(20)   := 'SUCCESS';	-- MESSAGE SUCCESS
   CSL_ARROW                    CONSTANT VARCHAR2(5)    := '-->';		-- AESTHETICS SIGN
   VL_FINAL_DATE                VARCHAR2(25);
   VL_PAYMENT_NUMBER_ID         NUMBER(5)               := 0;
   VL_DATE_FORMAT               VARCHAR2(12)             := 'DD/MM/YYYY';
   VL_DATE_DEFAULT              VARCHAR2(25)             := '01/01/1991';
   VL_SPACE                     VARCHAR2(5)              := ' ';
   VL_DATA_OUT                  VARCHAR2(4000)           :=  PA_LOAN_ID || VL_SPACE || PA_ADMIN_CENTER_ID || VL_SPACE || PA_JSON_INPUT;

BEGIN

--- PAYMENTS ARE UPDATED IN TA_LOAN_STATUS_DETAIL

      UPDATE SC_CREDIT.TA_LOAN_STATUS_DETAIL T
         SET T.FI_ON_OFF = CSL_0  -- OFF
       WHERE FI_LOAN_ID = PA_LOAN_ID
		AND FI_ADMIN_CENTER_ID = PA_ADMIN_CENTER_ID
		AND FI_ON_OFF = CSL_1 -- RECORDS ON
		AND FI_LOAN_STATUS_ID = CSL_3
		AND FI_ACTION_DETAIL_ID = CSL_3
		AND T.FI_PAYMENT_NUMBER_ID IN (
						  SELECT FI_PAYMENT_NUMBER_ID
							FROM JSON_TABLE(PA_JSON_INPUT, '$.payment.schedules[*]'
                                            COLUMNS(
                                                     FI_PAYMENT_NUMBER_ID        NUMBER(15)    PATH   '$.paymentNumber'
                                                    )
                             ));
   COMMIT;


PA_STATUS_CODE    :=  CSL_0;
PA_STATUS_MESSAGE := CSL_MSG_SUCCESS;

PA_JSON_OUPUT 	 := JSON_OBJECT('PA_STATUS_CODE'    VALUE PA_STATUS_CODE,
                                    'PA_STATUS_MESSAGE' VALUE PA_STATUS_MESSAGE
									);


EXCEPTION

  WHEN NO_DATA_FOUND THEN
      ROLLBACK;

         PA_STATUS_CODE    := 101;                     -- NO UPDATED CODE
         PA_STATUS_MESSAGE := 'No data updated';       -- NO LOAN MESSAGE
         PA_JSON_OUPUT     := JSON_OBJECT('PA_STATUS_CODE'   VALUE SQLCODE,
									      'PA_STATUS_MESSAGE' VALUE SQLERRM  || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
									      FORMAT JSON);

		 SC_CREDIT.SP_ERROR_LOG('SP_UPD_LOAN_STATUS_DETAIL', SQLCODE, SQLERRM,
         DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, PA_LOAN_ID , VL_DATA_OUT );

   WHEN OTHERS THEN
      ROLLBACK;

         PA_STATUS_CODE    := 100;                               -- NO DATA FOUND CODE
         PA_STATUS_MESSAGE := 'No update/insertion done';        -- NO DATA FOUND MESSAGE
         PA_JSON_OUPUT     := JSON_OBJECT('PA_STATUS_CODE'   VALUE SQLCODE,
									      'PA_STATUS_MESSAGE' VALUE SQLERRM  || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
									      FORMAT JSON);

         SC_CREDIT.SP_ERROR_LOG('SP_UPD_LOAN_STATUS_DETAIL', SQLCODE, SQLERRM,
         DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, PA_LOAN_ID , VL_DATA_OUT );

END SP_UPD_LOAN_STATUS_DETAIL;

/

  GRANT EXECUTE ON SC_CREDIT.SP_UPD_LOAN_STATUS_DETAIL TO USRNCPCREDIT1;  
--------------------------------------------------------
--  DDL for Procedure SP_UPD_LOAN_STATUS
--------------------------------------------------------
set define off;

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

  GRANT EXECUTE ON SC_CREDIT.SP_UPD_LOAN_STATUS TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_UPD_LOAN_STATUS TO USRBTCCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_BTC_GEN_STA_WAI_CLOS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_BTC_GEN_STA_WAI_CLOS 
   (PTAB_STATUSWC                 IN SC_CREDIT.TYP_TAB_BTC_STATUS
   ,PTAB_LOANSWC                  IN SC_CREDIT.TYP_TAB_BTC_LOAN
   ,PTAB_OPERATIONSWC             IN SC_CREDIT.TYP_TAB_BTC_OPERATION
   ,PTAB_OPERATIONS_DETAILWC      IN SC_CREDIT.TYP_TAB_BTC_DETAIL
   ,PTAB_BALANCESWC               IN SC_CREDIT.TYP_TAB_BTC_BALANCE
   ,PTAB_BALANCES_DETAILWC        IN SC_CREDIT.TYP_TAB_BTC_DETAIL
   ,PA_DEVICE                     IN SC_CREDIT.TA_LOAN_OPERATION.FC_DEVICE%TYPE
   ,PA_GPS_LATITUDE               IN SC_CREDIT.TA_LOAN_OPERATION.FC_GPS_LATITUDE%TYPE
   ,PA_GPS_LONGITUDE              IN SC_CREDIT.TA_LOAN_OPERATION.FC_GPS_LONGITUDE%TYPE
   ,PA_STATUS_CODE                OUT NUMBER
   ,PA_STATUS_MSG                 OUT VARCHAR2
   ,PA_RECORDS_READ               OUT NUMBER
   ,PA_RECORDS_SUCCESS            OUT NUMBER
   ,PA_RECORDS_ERROR              OUT NUMBER
   ,PTAB_ERROR_RECORDS            OUT SC_CREDIT.TYP_TAB_BTC_ERROR
   )
IS
/************************************************************************************************************************
PROJECT:            PURPOSE_LIFE_LOAN_CYCLE
DESCRIPTION:        UPDATES OR INSERTS THE WAITING CLOSED PROCESS
PRECONDITIONS:      NONE
CREATOR:            IVAN LOPEZ
CREATED DATE:       13/11/2024
MODIFICATION DATE:  09/01/2025
USER MODIFICATION:  AIXA SARMIENTO
*************************************************************************************************************************/
   --CONSTANTS
   CSL_0                              CONSTANT SIMPLE_INTEGER := 0;
   CSL_1                              CONSTANT SIMPLE_INTEGER := 1;
   CSL_PKG                            CONSTANT SIMPLE_INTEGER := 1;
   --CONSTANTS SUCCESS
   CSL_SUCCESS_CODE                   CONSTANT SIMPLE_INTEGER := 0;
   CSL_SUCCESS_MSG                    CONSTANT VARCHAR2(8) := 'SUCCESS';
   CSL_NUMERROR                       CONSTANT SIMPLE_INTEGER := -20012;
   CSL_SPACE                          CONSTANT VARCHAR2(1) := ' ';
   CSL_SUCCESS_ERROR                  CONSTANT VARCHAR2(30) := 'SUCCESS, WITH ERRORS RECORDS';
   CSL_ARROW                          CONSTANT VARCHAR2(5) := ' -> ';
   CSL_TYPE_NULL                      CONSTANT VARCHAR2(18)   := 'TYPE STATUS NULL';
   CSL_COMMA                          CONSTANT VARCHAR2(5) := ', ';
   VL_I                               NUMBER(10,0) := 0;
   VL_STATUS_CODE                     NUMBER(10,0) := 0;
   VL_STATUS_MSG                      VARCHAR2(1000);
   VL_PROCESS_DESC                    VARCHAR2(30);
   --VARIABLES INTERNAL TYPES ASSIGNMENT
   VL_TAB_ERRORS                      SC_CREDIT.TYP_TAB_BTC_ERROR;
   VL_TAB_LOANS                       SC_CREDIT.TYP_TAB_BTC_LOAN;
   VL_TAB_OPERATIONS                   SC_CREDIT.TYP_TAB_BTC_OPERATION;
   VL_TAB_OPERATIONS_DETAIL            SC_CREDIT.TYP_TAB_BTC_DETAIL;
   VL_TAB_BALANCES                     SC_CREDIT.TYP_TAB_BTC_BALANCE;
   VL_TAB_BALANCES_DETAIL              SC_CREDIT.TYP_TAB_BTC_DETAIL;

   --VARIABLES OF ITERATION BY LOAN
   VL_REC_LOAN                        SC_CREDIT.TYP_REC_BTC_LOAN;
   VL_TAB_OPERATIONS_BY_LOAN          SC_CREDIT.TYP_TAB_BTC_OPERATION;
   VL_TAB_OPERATIONS_DET_BY_LOAN      SC_CREDIT.TYP_TAB_BTC_DETAIL;
   VL_TAB_BALANCES_BY_LOAN            SC_CREDIT.TYP_TAB_BTC_BALANCE;
   VL_TAB_BALANCES_DET_BY_LOAN        SC_CREDIT.TYP_TAB_BTC_DETAIL;

   VL_DESC_COUNT                     VARCHAR2(500);
BEGIN
   PA_STATUS_CODE := CSL_SUCCESS_CODE;
   PA_STATUS_MSG := CSL_SUCCESS_MSG;
   PA_RECORDS_SUCCESS := 0;
   PA_RECORDS_ERROR := 0;
   PA_RECORDS_READ := 0;
   VL_TAB_ERRORS := SC_CREDIT.TYP_TAB_BTC_ERROR();

   IF PTAB_STATUSWC IS NULL THEN
      RAISE_APPLICATION_ERROR( CSL_NUMERROR, CSL_TYPE_NULL);
   END IF;

   VL_I := PTAB_STATUSWC.FIRST;
   PA_RECORDS_READ := PTAB_STATUSWC.COUNT;

   --INTERNAL TYPES ASSIGNMENT
   VL_TAB_LOANS             := SC_CREDIT.TYP_TAB_BTC_LOAN();
   VL_TAB_LOANS             := PTAB_LOANSWC;
   VL_TAB_OPERATIONS        := SC_CREDIT.TYP_TAB_BTC_OPERATION();
   VL_TAB_OPERATIONS        := PTAB_OPERATIONSWC;
   VL_TAB_OPERATIONS_DETAIL := SC_CREDIT.TYP_TAB_BTC_DETAIL();
   VL_TAB_OPERATIONS_DETAIL := PTAB_OPERATIONS_DETAILWC;
   VL_TAB_BALANCES          := SC_CREDIT.TYP_TAB_BTC_BALANCE();
   VL_TAB_BALANCES          := PTAB_BALANCESWC;
   VL_TAB_BALANCES_DETAIL   := SC_CREDIT.TYP_TAB_BTC_DETAIL();
   VL_TAB_BALANCES_DETAIL   := PTAB_BALANCES_DETAILWC;


   WHILE (VL_I IS NOT NULL) LOOP
      BEGIN

         VL_TAB_OPERATIONS_BY_LOAN := SC_CREDIT.TYP_TAB_BTC_OPERATION();
         VL_TAB_OPERATIONS_DET_BY_LOAN := SC_CREDIT.TYP_TAB_BTC_DETAIL();
         VL_TAB_BALANCES_BY_LOAN := SC_CREDIT.TYP_TAB_BTC_BALANCE();
         VL_TAB_BALANCES_DET_BY_LOAN := SC_CREDIT.TYP_TAB_BTC_DETAIL();

          --TAB BY LOAN ASSIGNMENT
         <<loopLoanAssignment>>
         WHILE VL_TAB_LOANS.COUNT > CSL_0 AND PTAB_STATUSWC.EXISTS(VL_I) LOOP
            IF VL_TAB_LOANS(VL_TAB_LOANS.FIRST).FI_LOAN_ID = PTAB_STATUSWC(VL_I).FI_LOAN_ID AND
			   VL_TAB_LOANS(VL_TAB_LOANS.FIRST).FI_ADMIN_CENTER_ID = PTAB_STATUSWC(VL_I).FI_ADMIN_CENTER_ID THEN

               VL_REC_LOAN := VL_TAB_LOANS(VL_TAB_LOANS.FIRST);
               VL_TAB_LOANS.DELETE(VL_TAB_LOANS.FIRST);
            ELSE
               EXIT loopLoanAssignment;
            END IF;
         END LOOP loopLoanAssignment;

         --TAB BY OPERATION ASSIGNMENT
         <<loopOperationAssignment>>
         WHILE VL_TAB_OPERATIONS.COUNT > CSL_0 AND PTAB_STATUSWC.EXISTS(VL_I) LOOP
            IF VL_TAB_OPERATIONS(VL_TAB_OPERATIONS.FIRST).FI_LOAN_ID = PTAB_STATUSWC(VL_I).FI_LOAN_ID
               AND VL_TAB_OPERATIONS(VL_TAB_OPERATIONS.FIRST).FI_ADMIN_CENTER_ID = PTAB_STATUSWC(VL_I).FI_ADMIN_CENTER_ID THEN

               VL_TAB_OPERATIONS_BY_LOAN.EXTEND;
               VL_TAB_OPERATIONS_BY_LOAN(VL_TAB_OPERATIONS_BY_LOAN.LAST) := VL_TAB_OPERATIONS(VL_TAB_OPERATIONS.FIRST);
               VL_TAB_OPERATIONS.DELETE(VL_TAB_OPERATIONS.FIRST);
            ELSE
               EXIT loopOperationAssignment;
            END IF;
         END LOOP loopOperationAssignment;

         --TAB BY OPERATION DET ASSIGNMENT
         <<loopOperationDetAssignment>>
         WHILE VL_TAB_OPERATIONS_DETAIL.COUNT > CSL_0 AND PTAB_STATUSWC.EXISTS(VL_I) LOOP
            IF VL_TAB_OPERATIONS_DETAIL(VL_TAB_OPERATIONS_DETAIL.FIRST).FI_LOAN_ID = PTAB_STATUSWC(VL_I).FI_LOAN_ID
               AND VL_TAB_OPERATIONS_DETAIL(VL_TAB_OPERATIONS_DETAIL.FIRST).FI_ADMIN_CENTER_ID = PTAB_STATUSWC(VL_I).FI_ADMIN_CENTER_ID THEN

               VL_TAB_OPERATIONS_DET_BY_LOAN.EXTEND;
               VL_TAB_OPERATIONS_DET_BY_LOAN(VL_TAB_OPERATIONS_DET_BY_LOAN.LAST) := VL_TAB_OPERATIONS_DETAIL(VL_TAB_OPERATIONS_DETAIL.FIRST);
               VL_TAB_OPERATIONS_DETAIL.DELETE(VL_TAB_OPERATIONS_DETAIL.FIRST);
            ELSE
               EXIT loopOperationDetAssignment;
            END IF;
         END LOOP loopOperationDetAssignment;

         --TAB BY BALANCES ASSIGNMENT
         <<loopBalanceAssignment>>
         WHILE VL_TAB_BALANCES.COUNT > CSL_0 AND PTAB_STATUSWC.EXISTS(VL_I) LOOP
            IF VL_TAB_BALANCES(VL_TAB_BALANCES.FIRST).FI_LOAN_ID = PTAB_STATUSWC(VL_I).FI_LOAN_ID
               AND VL_TAB_BALANCES(VL_TAB_BALANCES.FIRST).FI_ADMIN_CENTER_ID = PTAB_STATUSWC(VL_I).FI_ADMIN_CENTER_ID THEN

               VL_TAB_BALANCES_BY_LOAN.EXTEND;
               VL_TAB_BALANCES_BY_LOAN(VL_TAB_BALANCES_BY_LOAN.LAST) := VL_TAB_BALANCES(VL_TAB_BALANCES.FIRST);
               VL_TAB_BALANCES.DELETE(VL_TAB_BALANCES.FIRST);
            ELSE
               EXIT loopBalanceAssignment;
            END IF;
         END LOOP loopBalanceAssignment;

         --TAB BY BALANCES DET ASSIGNMENT
         <<loopBalanceDetAssignment>>
         WHILE VL_TAB_BALANCES_DETAIL.COUNT > CSL_0 AND PTAB_STATUSWC.EXISTS(VL_I) LOOP
            IF VL_TAB_BALANCES_DETAIL(VL_TAB_BALANCES_DETAIL.FIRST).FI_LOAN_ID = PTAB_STATUSWC(VL_I).FI_LOAN_ID
               AND VL_TAB_BALANCES_DETAIL(VL_TAB_BALANCES_DETAIL.FIRST).FI_ADMIN_CENTER_ID = PTAB_STATUSWC(VL_I).FI_ADMIN_CENTER_ID THEN

               VL_TAB_BALANCES_DET_BY_LOAN.EXTEND;
               VL_TAB_BALANCES_DET_BY_LOAN(VL_TAB_BALANCES_DET_BY_LOAN.LAST) := VL_TAB_BALANCES_DETAIL(VL_TAB_BALANCES_DETAIL.FIRST);
               VL_TAB_BALANCES_DETAIL.DELETE(VL_TAB_BALANCES_DETAIL.FIRST);
            ELSE
               EXIT loopBalanceDetAssignment;
            END IF;
         END LOOP loopBalanceDetAssignment;


         VL_PROCESS_DESC := 'SP_BTC_GEN_OPERATION_BALANCE';
         SC_CREDIT.SP_BTC_EXE_OPERATION_BALANCE(VL_REC_LOAN
                                               ,VL_TAB_OPERATIONS_BY_LOAN
                                               ,VL_TAB_OPERATIONS_DET_BY_LOAN
                                               ,VL_TAB_BALANCES_BY_LOAN
                                               ,VL_TAB_BALANCES_DET_BY_LOAN
                                               ,PA_DEVICE
                                               ,PA_GPS_LATITUDE
                                               ,PA_GPS_LONGITUDE
                                               ,CSL_0
                                               ,VL_STATUS_CODE
                                               ,VL_STATUS_MSG);

         IF(VL_STATUS_CODE != CSL_0)THEN
            RAISE_APPLICATION_ERROR( CSL_NUMERROR, VL_PROCESS_DESC || CSL_SPACE || VL_STATUS_MSG);
         END IF;

         VL_PROCESS_DESC := 'SP_BTC_INS_LOAN_STATUS';
         SC_CREDIT.SP_BTC_INS_LOAN_STATUS(PTAB_STATUSWC(VL_I).FI_LOAN_ID
                                         ,PTAB_STATUSWC(VL_I).FI_ADMIN_CENTER_ID
                                         ,PTAB_STATUSWC(VL_I).FI_LOAN_OPERATION_ID
                                         ,PTAB_STATUSWC(VL_I).FI_LOAN_STATUS_ID
                                         ,PTAB_STATUSWC(VL_I).FI_LOAN_STATUS_OLD_ID
                                         ,PTAB_STATUSWC(VL_I).FI_TRIGGER_ID
                                         ,PTAB_STATUSWC(VL_I).FD_LOAN_STATUS_DATE
                                         ,CSL_1
                                         ,CSL_0
                                         ,VL_STATUS_CODE
                                         ,VL_STATUS_MSG);

         IF(VL_STATUS_CODE != CSL_0)THEN
            RAISE_APPLICATION_ERROR( CSL_NUMERROR, VL_PROCESS_DESC || CSL_SPACE || VL_STATUS_MSG);
         END IF;

         VL_TAB_OPERATIONS_BY_LOAN.DELETE;
         VL_TAB_OPERATIONS_DET_BY_LOAN.DELETE;
         VL_TAB_BALANCES_BY_LOAN.DELETE;
         VL_TAB_BALANCES_DET_BY_LOAN.DELETE;

         PA_RECORDS_SUCCESS := PA_RECORDS_SUCCESS + CSL_1;

         --EXCEPTION
         EXCEPTION
            WHEN OTHERS THEN
               ROLLBACK;
                  SC_CREDIT.SP_BATCH_ERROR_LOG(UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_PKG)
                                              ,SQLCODE
                                              ,SQLERRM
                                              ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                              ,CSL_1
                                              ,PTAB_STATUSWC(VL_I).FI_ADMIN_CENTER_ID ||CSL_COMMA
                                              ||PTAB_STATUSWC(VL_I).FI_LOAN_ID);

                  PA_RECORDS_ERROR :=PA_RECORDS_ERROR + CSL_1;

                  VL_TAB_ERRORS.EXTEND;
                  VL_TAB_ERRORS(VL_TAB_ERRORS.LAST) :=
                  SC_CREDIT.TYP_REC_BTC_ERROR(PTAB_STATUSWC(VL_I).FI_ADMIN_CENTER_ID
                                             ,PTAB_STATUSWC(VL_I).FI_LOAN_ID
                                             ,UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_PKG)
                                             ,SQLCODE
                                             ,SQLERRM
                                             ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                             ,SYSDATE
                                             ,CSL_1
                                             ,NULL);
      END;
      VL_I := PTAB_STATUSWC.NEXT(VL_I);
      COMMIT;
   END LOOP;

   VL_DESC_COUNT := ' TAB_LOANS '||VL_TAB_LOANS.COUNT
                  ||' TAB_OPERATIONS '||VL_TAB_OPERATIONS.COUNT
                  ||' TAB_OPERATIONS_DETAIL '||VL_TAB_OPERATIONS_DETAIL.COUNT
                  ||' TAB_BALANCES '||VL_TAB_BALANCES.COUNT
                  ||' TAB_BALANCES_DETAIL '||VL_TAB_BALANCES_DETAIL.COUNT;

   --DELETE TYPES INTERNS
   VL_TAB_LOANS.DELETE;
   VL_TAB_OPERATIONS.DELETE;
   VL_TAB_OPERATIONS_DETAIL.DELETE;
   VL_TAB_BALANCES.DELETE;
   VL_TAB_BALANCES_DETAIL.DELETE;

   PTAB_ERROR_RECORDS := VL_TAB_ERRORS;
   IF(PA_RECORDS_ERROR > CSL_0)THEN
      PA_STATUS_CODE := CSL_1;
      PA_STATUS_MSG := CSL_SUCCESS_ERROR;
   END IF;

    PA_STATUS_MSG := PA_STATUS_MSG
      || ' ' || VL_DESC_COUNT;
EXCEPTION
   WHEN OTHERS THEN
      PA_STATUS_CODE := SQLCODE;
      PA_STATUS_MSG := SQLERRM || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;

      SC_CREDIT.SP_BATCH_ERROR_LOG(UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_PKG)
                                  ,SQLCODE
                                  ,SQLERRM
                                  ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                  ,CSL_0
                                  ,NULL);
END SP_BTC_GEN_STA_WAI_CLOS;

/

  GRANT EXECUTE ON SC_CREDIT.SP_BTC_GEN_STA_WAI_CLOS TO USRBTCCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_BTC_GEN_STA_WAI_CLOS TO USRNCPCREDIT1;

--------------------------------------------------------
--  DDL for Procedure SP_SEL_LOAN_CONCEPT
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_SEL_LOAN_CONCEPT (
    PA_JSON_OBJECT OUT CLOB,
    PA_STATUS_CODE OUT NUMBER,
    PA_STATUS_MSG OUT VARCHAR2
)
    IS

    CSL_0 CONSTANT       SIMPLE_INTEGER := 0;
    CSL_1 CONSTANT       SIMPLE_INTEGER := 1;
    CSG_X CONSTANT       VARCHAR2(5)    := 'X';
    CSL_SP CONSTANT      SIMPLE_INTEGER := 1;
    CSL_ARROW CONSTANT   VARCHAR2(5)    := '->';
    CSL_SUCCESS CONSTANT VARCHAR2(8)    := 'SUCCESS';

BEGIN
    SELECT JSON_ARRAYAGG(
                   JSON_OBJECT(
                           'FI_LOAN_CONCEPT_ID' VALUE lc.FI_LOAN_CONCEPT_ID,
                           'FI_LOAN_CONCEPT_TYPE_ID' VALUE lc.FI_LOAN_CONCEPT_TYPE_ID
                   )
           )
    INTO PA_JSON_OBJECT
    FROM SC_CREDIT.TC_LOAN_CONCEPT LC;

    PA_STATUS_CODE := CSL_0;
    PA_STATUS_MSG := CSL_SUCCESS;

EXCEPTION
    WHEN OTHERS THEN
        PA_STATUS_CODE := SQLCODE;
        PA_STATUS_MSG := SQLERRM || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
        SC_CREDIT.SP_ERROR_LOG(
                        UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP)
            , SQLCODE
            , SQLERRM
            , DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
            , CSG_X
            , CSL_1
        );
END SP_SEL_LOAN_CONCEPT;

/

  GRANT EXECUTE ON SC_CREDIT.SP_SEL_LOAN_CONCEPT TO USRNCPCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_SEL_TRIGGER
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_SEL_TRIGGER 
        (
          PA_CUR_RESULTS     OUT     SC_CREDIT.PA_TYPES.TYP_CURSOR
         ,PA_STATUS_CODE     OUT     NUMBER
         ,PA_STATUS_MSG      OUT     VARCHAR2
         )
IS
/***************************************************************************************************************
* PROJECT:            PURPOSE_LIFE_LOAN_CYCLE
* DESCRIPTION:        THIS STORE PROCEDURE GIVES A CATALOG OF THE ACTUAL STATUS, THE TRIGGER AND IT??S NEXT STATUS
* PRECONDITIONS:      DOESN??T NEED ANY PRECONDITION
* CREATOR:            C??SAR MEDINA
* CREATED DATE:       13/11/2024
* MODIFICATION DATE:  14/11/2024
***************************************************************************************************************/
            CSL_ARROW                      CONSTANT VARCHAR2(5)    := '-->';
            CSL_0                          CONSTANT SIMPLE_INTEGER := 0;
            CSL_1                          CONSTANT SIMPLE_INTEGER := 1;
            CSL_SP                        CONSTANT SIMPLE_INTEGER := 1;
            CSL_SUCCESS_MESSAGE            CONSTANT VARCHAR2(10)   := 'SUCCESS';
            CSL_NOT_DATA         CONSTANT VARCHAR2(20)   := 'NO DATA FOUND';

BEGIN
            PA_CUR_RESULTS := NULL;
            PA_STATUS_CODE      := CSL_0;
            PA_STATUS_MSG       := CSL_SUCCESS_MESSAGE;

   OPEN PA_CUR_RESULTS FOR
      SELECT
              LST.FI_LOAN_STATUS_ID       AS  FI_LOAN_STATUS_ID
             ,LS1.FC_LOAN_STATUS_DESC     AS  FC_LOAN_STATUS_DESC
             ,LST.FI_NEXT_STATUS          AS  FI_LOAN_NEXT_STATUS_ID
             ,LS2.FC_LOAN_STATUS_DESC     AS  FC_LOAN_NEXT_STATUS_DESC
             ,LST.FI_TRIGGER_ID           AS  FI_TRIGGER_ID
             ,T.FC_TRIGGER_DESC           AS  FC_TRIGGER_DESC
        FROM SC_CREDIT.TC_LOAN_STATUS_TRIGGER LST
   LEFT JOIN SC_CREDIT.TC_LOAN_STATUS LS1 ON LST.FI_LOAN_STATUS_ID  =   LS1.FI_LOAN_STATUS_ID
   LEFT JOIN SC_CREDIT.TC_LOAN_STATUS LS2 ON LST.FI_NEXT_STATUS     =   LS2.FI_LOAN_STATUS_ID
   LEFT JOIN SC_CREDIT.TC_TRIGGER       T ON LST.FI_TRIGGER_ID      =   T.FI_TRIGGER_ID
   WHERE LST.FI_LOAN_STATUS_ID >= CSL_0
   AND LST.FI_TRIGGER_ID  >= CSL_0;

EXCEPTION
 WHEN NO_DATA_FOUND THEN
        PA_STATUS_CODE := CSL_0;
        PA_STATUS_MSG  := CSL_NOT_DATA;
         SC_CREDIT.SP_ERROR_LOG(UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP)
                                    ,SQLCODE
                                    ,SQLERRM
                                    ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                    ,CSL_NOT_DATA
                                    ,NULL);
WHEN OTHERS THEN
        PA_STATUS_CODE      := SQLCODE;
        PA_STATUS_MSG       := SQLERRM || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;

       SC_CREDIT.SP_ERROR_LOG(UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP)
                                    ,SQLCODE
                                    ,SQLERRM
                                    ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                    ,CSL_NOT_DATA
                                    ,NULL);
END SP_SEL_TRIGGER;

/

  GRANT EXECUTE ON SC_CREDIT.SP_SEL_TRIGGER TO USRNCPCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_BTC_EXE_APPLY_LATE_FEE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_BTC_EXE_APPLY_LATE_FEE (
   PTAB_LOANS                   IN SC_CREDIT.TYP_TAB_BTC_LOAN
   ,PTAB_OPERATIONS             IN SC_CREDIT.TYP_TAB_BTC_OPERATION
   ,PTAB_OPERATIONS_DETAIL      IN SC_CREDIT.TYP_TAB_BTC_DETAIL
   ,PTAB_BALANCES               IN SC_CREDIT.TYP_TAB_BTC_BALANCE
   ,PTAB_BALANCES_DETAIL        IN SC_CREDIT.TYP_TAB_BTC_DETAIL
   ,PTAB_STATUS_DETAIL          IN SC_CREDIT.TYP_TAB_BTC_STATUS_DETAIL
   ,PA_DEVICE                   IN SC_CREDIT.TA_LOAN_OPERATION.FC_DEVICE%TYPE
   ,PA_GPS_LATITUDE             IN SC_CREDIT.TA_LOAN_OPERATION.FC_GPS_LATITUDE%TYPE
   ,PA_GPS_LONGITUDE            IN SC_CREDIT.TA_LOAN_OPERATION.FC_GPS_LONGITUDE%TYPE
   ,PA_STATUS_CODE              OUT NUMBER
   ,PA_STATUS_MSG               OUT VARCHAR2
   ,PA_RECORDS_READ             OUT NUMBER
   ,PA_RECORDS_SUCCESS          OUT NUMBER
   ,PA_RECORDS_ERROR            OUT NUMBER
   ,PTAB_ERROR_RECORDS          OUT SC_CREDIT.TYP_TAB_BTC_ERROR
)
IS
   /* **************************************************************
   * PROJECT: LOAN LIFE CYCLE
   * DESCRIPTION: PROCESS TO APPLY FEES WITH STATUS DETAIL
   * CREATED DATE: 12/19/2024
   * CREATOR: LUIS RAMIREZ
   ************************************************************** */

   --CONSTANTS
   CSL_0                              CONSTANT SIMPLE_INTEGER := 0;
   CSL_1                              CONSTANT SIMPLE_INTEGER := 1;
   CSL_SP                             CONSTANT SIMPLE_INTEGER := 1;

   --CONSTANTS SUCCESS
   CSL_SUCCESS_CODE                   CONSTANT SIMPLE_INTEGER := 0;
   CSL_SUCCESS_MSG                    CONSTANT VARCHAR2(7) := 'SUCCESS';
   CSL_CODE_ERROR                     CONSTANT SIMPLE_INTEGER := -20012;
   CSL_SPACE                          CONSTANT VARCHAR2(2) := ' ';
   CSL_SUCCESS_ERROR                  CONSTANT VARCHAR2(28) := 'SUCCESS, WITH ERRORS RECORDS';
   CSL_TYPE_NULL                      CONSTANT VARCHAR2(14) := 'TYPE FEES NULL';
   CSL_ARROW                          CONSTANT VARCHAR2(5) := ' -> ';
   CSL_COMMA                          CONSTANT VARCHAR2(5) := ' , ';
   CSL_INS_STATUS_DETAIL              CONSTANT VARCHAR2(30) := 'SP_BTC_INS_LOAN_STATUS_DETAIL';
   CSL_UPD_STATUS_DETAIL              CONSTANT VARCHAR2(30) := 'SP_BTC_UPD_LOAN_STATUS_DETAIL';
   CSL_GEN_OPERATION                  CONSTANT VARCHAR2(30) := 'SP_BTC_EXE_OPERATION_BALANCE';

   VL_INDEX                               NUMBER(10,0) := 0;
   VL_STATUS_CODE                     NUMBER(10,0) := 0;
   VL_STATUS_MSG                      VARCHAR2(1000);
   --VARIABLES INTERNAL TYPES ASSIGNMENT
   VLTAB_ERRORS                       SC_CREDIT.TYP_TAB_BTC_ERROR;
   VLTAB_LOANS                        SC_CREDIT.TYP_TAB_BTC_LOAN;
   VLTAB_OPERATIONS                   SC_CREDIT.TYP_TAB_BTC_OPERATION;
   VLTAB_OPERATIONS_DETAIL            SC_CREDIT.TYP_TAB_BTC_DETAIL;
   VLTAB_BALANCES                     SC_CREDIT.TYP_TAB_BTC_BALANCE;
   VLTAB_BALANCES_DETAIL              SC_CREDIT.TYP_TAB_BTC_DETAIL;
   VLTAB_STATUS_DETAIL                SC_CREDIT.TYP_TAB_BTC_STATUS_DETAIL;

   --VARIABLES OF ITERATION BY LOAN
   VLREC_LOAN                        SC_CREDIT.TYP_REC_BTC_LOAN;
   VLTAB_OPERATIONS_BY_LOAN          SC_CREDIT.TYP_TAB_BTC_OPERATION;
   VLTAB_OPERATIONS_DET_BY_LOAN      SC_CREDIT.TYP_TAB_BTC_DETAIL;
   VLTAB_BALANCES_BY_LOAN            SC_CREDIT.TYP_TAB_BTC_BALANCE;
   VLTAB_BALANCES_DET_BY_LOAN        SC_CREDIT.TYP_TAB_BTC_DETAIL;

   VL_T1                             TIMESTAMP;
   VL_T2                             TIMESTAMP;
   VL_DESC_COUNT                     VARCHAR2(500);


BEGIN
   PA_STATUS_CODE := CSL_SUCCESS_CODE;
   PA_STATUS_MSG := CSL_SUCCESS_MSG;
   PA_RECORDS_SUCCESS := CSL_0;
   PA_RECORDS_ERROR := CSL_0;
   PA_RECORDS_READ := CSL_0;
   VLTAB_ERRORS := SC_CREDIT.TYP_TAB_BTC_ERROR();
   VL_T1 := SYSTIMESTAMP;

   IF PTAB_STATUS_DETAIL IS NULL OR PTAB_LOANS IS NULL
         OR PTAB_OPERATIONS IS NULL OR PTAB_OPERATIONS_DETAIL IS NULL
         OR PTAB_BALANCES IS NULL OR PTAB_BALANCES_DETAIL IS NULL THEN
      RAISE_APPLICATION_ERROR(CSL_CODE_ERROR, CSL_TYPE_NULL);
   END IF;

   --INTERNAL TYPES ASSIGNMENT
   VLTAB_LOANS             := PTAB_LOANS;
   VLTAB_OPERATIONS        := PTAB_OPERATIONS;
   VLTAB_OPERATIONS_DETAIL := PTAB_OPERATIONS_DETAIL;
   VLTAB_BALANCES          := PTAB_BALANCES;
   VLTAB_BALANCES_DETAIL   := PTAB_BALANCES_DETAIL;
   VLTAB_STATUS_DETAIL     := PTAB_STATUS_DETAIL;

   VL_INDEX := VLTAB_LOANS.FIRST;
   PA_RECORDS_READ := VLTAB_LOANS.COUNT;

   WHILE (VL_INDEX IS NOT NULL) LOOP
      BEGIN
         VLTAB_OPERATIONS_BY_LOAN := SC_CREDIT.TYP_TAB_BTC_OPERATION();
         VLTAB_OPERATIONS_DET_BY_LOAN := SC_CREDIT.TYP_TAB_BTC_DETAIL();
         VLTAB_BALANCES_BY_LOAN := SC_CREDIT.TYP_TAB_BTC_BALANCE();
         VLTAB_BALANCES_DET_BY_LOAN := SC_CREDIT.TYP_TAB_BTC_DETAIL();

         VLREC_LOAN := VLTAB_LOANS(VL_INDEX);

         --TAB BY OPERATION ASSIGNMENT
         <<loopOperationAssignment>>
         WHILE VLTAB_OPERATIONS.COUNT > CSL_0 AND VLTAB_LOANS.EXISTS(VL_INDEX) LOOP
            IF VLTAB_OPERATIONS(VLTAB_OPERATIONS.FIRST).FI_LOAN_ID = VLTAB_LOANS(VL_INDEX).FI_LOAN_ID
               AND VLTAB_OPERATIONS(VLTAB_OPERATIONS.FIRST).FI_ADMIN_CENTER_ID = VLTAB_LOANS(VL_INDEX).FI_ADMIN_CENTER_ID THEN

               VLTAB_OPERATIONS_BY_LOAN.EXTEND;
               VLTAB_OPERATIONS_BY_LOAN(VLTAB_OPERATIONS_BY_LOAN.LAST) := VLTAB_OPERATIONS(VLTAB_OPERATIONS.FIRST);
               VLTAB_OPERATIONS.DELETE(VLTAB_OPERATIONS.FIRST);
            ELSE
               EXIT loopOperationAssignment;
            END IF;
         END LOOP loopOperationAssignment;

         --TAB BY OPERATION DET ASSIGNMENT
         <<loopOperationDetAssignment>>
         WHILE VLTAB_OPERATIONS_DETAIL.COUNT > CSL_0 AND VLTAB_LOANS.EXISTS(VL_INDEX) LOOP
            IF VLTAB_OPERATIONS_DETAIL(VLTAB_OPERATIONS_DETAIL.FIRST).FI_LOAN_ID = VLTAB_LOANS(VL_INDEX).FI_LOAN_ID
               AND VLTAB_OPERATIONS_DETAIL(VLTAB_OPERATIONS_DETAIL.FIRST).FI_ADMIN_CENTER_ID = VLTAB_LOANS(VL_INDEX).FI_ADMIN_CENTER_ID THEN

               VLTAB_OPERATIONS_DET_BY_LOAN.EXTEND;
               VLTAB_OPERATIONS_DET_BY_LOAN(VLTAB_OPERATIONS_DET_BY_LOAN.LAST) := VLTAB_OPERATIONS_DETAIL(VLTAB_OPERATIONS_DETAIL.FIRST);
               VLTAB_OPERATIONS_DETAIL.DELETE(VLTAB_OPERATIONS_DETAIL.FIRST);
            ELSE
               EXIT loopOperationDetAssignment;
            END IF;
         END LOOP loopOperationDetAssignment;

         --TAB BY BALANCES ASSIGNMENT
         <<loopBalanceAssignment>>
         WHILE VLTAB_BALANCES.COUNT > CSL_0 AND VLTAB_LOANS.EXISTS(VL_INDEX) LOOP
            IF VLTAB_BALANCES(VLTAB_BALANCES.FIRST).FI_LOAN_ID = VLTAB_LOANS(VL_INDEX).FI_LOAN_ID
               AND VLTAB_BALANCES(VLTAB_BALANCES.FIRST).FI_ADMIN_CENTER_ID = VLTAB_LOANS(VL_INDEX).FI_ADMIN_CENTER_ID THEN

               VLTAB_BALANCES_BY_LOAN.EXTEND;
               VLTAB_BALANCES_BY_LOAN(VLTAB_BALANCES_BY_LOAN.LAST) := VLTAB_BALANCES(VLTAB_BALANCES.FIRST);
               VLTAB_BALANCES.DELETE(VLTAB_BALANCES.FIRST);
            ELSE
               EXIT loopBalanceAssignment;
            END IF;
         END LOOP loopBalanceAssignment;

         --TAB BY BALANCES DET ASSIGNMENT
         <<loopBalanceDetAssignment>>
         WHILE VLTAB_BALANCES_DETAIL.COUNT > CSL_0 AND VLTAB_LOANS.EXISTS(VL_INDEX) LOOP
            IF VLTAB_BALANCES_DETAIL(VLTAB_BALANCES_DETAIL.FIRST).FI_LOAN_ID = VLTAB_LOANS(VL_INDEX).FI_LOAN_ID
               AND VLTAB_BALANCES_DETAIL(VLTAB_BALANCES_DETAIL.FIRST).FI_ADMIN_CENTER_ID = VLTAB_LOANS(VL_INDEX).FI_ADMIN_CENTER_ID THEN

               VLTAB_BALANCES_DET_BY_LOAN.EXTEND;
               VLTAB_BALANCES_DET_BY_LOAN(VLTAB_BALANCES_DET_BY_LOAN.LAST) := VLTAB_BALANCES_DETAIL(VLTAB_BALANCES_DETAIL.FIRST);
               VLTAB_BALANCES_DETAIL.DELETE(VLTAB_BALANCES_DETAIL.FIRST);
            ELSE
               EXIT loopBalanceDetAssignment;
            END IF;
         END LOOP loopBalanceDetAssignment;

         --EXECUTE PROCESS TO AFFECT LOAN, OPERATIONS AND BALANCES
         SC_CREDIT.SP_BTC_EXE_OPERATION_BALANCE(
            VLREC_LOAN
            ,VLTAB_OPERATIONS_BY_LOAN
            ,VLTAB_OPERATIONS_DET_BY_LOAN
            ,VLTAB_BALANCES_BY_LOAN
            ,VLTAB_BALANCES_DET_BY_LOAN
            ,PA_DEVICE
            ,PA_GPS_LATITUDE
            ,PA_GPS_LONGITUDE
            ,CSL_0
            ,VL_STATUS_CODE
            ,VL_STATUS_MSG);

         IF(VL_STATUS_CODE != CSL_0)THEN
            RAISE_APPLICATION_ERROR( CSL_CODE_ERROR, CSL_GEN_OPERATION || CSL_SPACE || VL_STATUS_MSG);
         END IF;

         --TAB BY STATUS DETAIL ASSIGNMENT
         <<loopStatusDetAssignment>>
         WHILE VLTAB_STATUS_DETAIL.COUNT > CSL_0 AND VLTAB_LOANS.EXISTS(VL_INDEX) LOOP
            IF VLTAB_STATUS_DETAIL(VLTAB_STATUS_DETAIL.FIRST).FI_LOAN_ID = VLTAB_LOANS(VL_INDEX).FI_LOAN_ID
               AND VLTAB_STATUS_DETAIL(VLTAB_STATUS_DETAIL.FIRST).FI_ADMIN_CENTER_ID = VLTAB_LOANS(VL_INDEX).FI_ADMIN_CENTER_ID THEN

               SC_CREDIT.SP_BTC_UPD_LOAN_STATUS_DETAIL
                  (VLTAB_STATUS_DETAIL(VLTAB_STATUS_DETAIL.FIRST).FI_LOAN_ID
                  ,VLTAB_STATUS_DETAIL(VLTAB_STATUS_DETAIL.FIRST).FI_ADMIN_CENTER_ID
                  ,VLTAB_STATUS_DETAIL(VLTAB_STATUS_DETAIL.FIRST).FI_PAYMENT_NUMBER_ID
                  ,VLTAB_STATUS_DETAIL(VLTAB_STATUS_DETAIL.FIRST).FI_COUNTER_DAY
                  ,NULL
                  ,NULL
                  ,CSL_1
                  ,CSL_0
                  ,VL_STATUS_CODE
                  ,VL_STATUS_MSG);

               IF(VL_STATUS_CODE != CSL_0)THEN
                  RAISE_APPLICATION_ERROR( CSL_CODE_ERROR, CSL_UPD_STATUS_DETAIL || CSL_SPACE || VL_STATUS_MSG);
               END IF;

               SC_CREDIT.SP_BTC_INS_LOAN_STATUS_DETAIL
                  (VLTAB_STATUS_DETAIL(VLTAB_STATUS_DETAIL.FIRST).FI_LOAN_ID
                  ,VLTAB_STATUS_DETAIL(VLTAB_STATUS_DETAIL.FIRST).FI_ADMIN_CENTER_ID
                  ,VLTAB_STATUS_DETAIL(VLTAB_STATUS_DETAIL.FIRST).FI_LOAN_STATUS_ID
                  ,VLTAB_STATUS_DETAIL(VLTAB_STATUS_DETAIL.FIRST).FI_ACTION_DETAIL_ID
                  ,VLTAB_STATUS_DETAIL(VLTAB_STATUS_DETAIL.FIRST).FI_COUNTER_DAY
                  ,VLTAB_STATUS_DETAIL(VLTAB_STATUS_DETAIL.FIRST).FD_INITIAL_DATE
                  ,VLTAB_STATUS_DETAIL(VLTAB_STATUS_DETAIL.FIRST).FI_PAYMENT_NUMBER_ID
                  ,VLTAB_STATUS_DETAIL(VLTAB_STATUS_DETAIL.FIRST).FD_FINAL_DATE
                  ,VLTAB_STATUS_DETAIL(VLTAB_STATUS_DETAIL.FIRST).FI_ON_OFF
                  ,CSL_0
                  ,VL_STATUS_CODE
                  ,VL_STATUS_MSG);

               IF(VL_STATUS_CODE != CSL_0)THEN
                  RAISE_APPLICATION_ERROR( CSL_CODE_ERROR, CSL_INS_STATUS_DETAIL || CSL_SPACE || VL_STATUS_MSG);
               END IF;

               VLTAB_STATUS_DETAIL.DELETE(VLTAB_STATUS_DETAIL.FIRST);
            ELSE
               EXIT loopStatusDetAssignment;
            END IF;
         END LOOP loopStatusDetAssignment;

         --DELETE TYPES BY LOAN (CYCLE)
         VLTAB_OPERATIONS_BY_LOAN.DELETE;
         VLTAB_OPERATIONS_DET_BY_LOAN.DELETE;
         VLTAB_BALANCES_BY_LOAN.DELETE;
         VLTAB_BALANCES_DET_BY_LOAN.DELETE;

         PA_RECORDS_SUCCESS := PA_RECORDS_SUCCESS + CSL_1;

      EXCEPTION
         WHEN OTHERS THEN
            ROLLBACK;
            SC_CREDIT.SP_BATCH_ERROR_LOG(
               UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP)
               ,SQLCODE
               ,SQLERRM
               ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
               ,CSL_0
               ,VLTAB_LOANS(VL_INDEX).FI_ADMIN_CENTER_ID
                  ||CSL_COMMA
                  ||VLTAB_LOANS(VL_INDEX).FI_LOAN_ID);
            PA_RECORDS_ERROR :=PA_RECORDS_ERROR + CSL_1;

            VLTAB_ERRORS.EXTEND;
            VLTAB_ERRORS(VLTAB_ERRORS.LAST) :=
               SC_CREDIT.TYP_REC_BTC_ERROR(VLTAB_LOANS(VL_INDEX).FI_ADMIN_CENTER_ID
                                          ,VLTAB_LOANS(VL_INDEX).FI_LOAN_ID
                                          , UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP)
                                          ,SQLCODE
                                          ,SQLERRM
                                          ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                          ,SYSDATE
                                          ,CSL_0
                                          ,NULL);
      END;
      VL_INDEX := VLTAB_LOANS.NEXT(VL_INDEX);
      COMMIT;
   END LOOP;

   COMMIT;

   VL_DESC_COUNT := ' TAB_LOANS '||VLTAB_LOANS.COUNT
                  ||' TAB_OPERATIONS '||VLTAB_OPERATIONS.COUNT
                  ||' TAB_OPERATIONS_DETAIL '||VLTAB_OPERATIONS_DETAIL.COUNT
                  ||' TAB_BALANCES '||VLTAB_BALANCES.COUNT
                  ||' TAB_BALANCES_DETAIL '||VLTAB_BALANCES_DETAIL.COUNT
                  ||' TAB_STATUS_DETAIL '||VLTAB_STATUS_DETAIL.COUNT;
   --DELETE TYPES INTERNS
   VLTAB_OPERATIONS.DELETE;
   VLTAB_OPERATIONS_DETAIL.DELETE;
   VLTAB_BALANCES.DELETE;
   VLTAB_BALANCES_DETAIL.DELETE;
   VLTAB_STATUS_DETAIL.DELETE;

   PTAB_ERROR_RECORDS := VLTAB_ERRORS;
   IF(PA_RECORDS_ERROR > CSL_0)THEN
      PA_STATUS_CODE := CSL_1;
      PA_STATUS_MSG := CSL_SUCCESS_ERROR;
   END IF;
   VL_T2 := SYSTIMESTAMP;
   PA_STATUS_MSG := PA_STATUS_MSG
      || ' ' || 'Elapsed Seconds: '||TO_CHAR(VL_T2-VL_T1, 'SSSS.FF')
      || ' ' || VL_DESC_COUNT;

EXCEPTION
   WHEN OTHERS THEN
      PA_STATUS_CODE := SQLCODE;
      PA_STATUS_MSG := SQLERRM || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;

      SC_CREDIT.SP_BATCH_ERROR_LOG(UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP)
                                  ,SQLCODE
                                  ,SQLERRM
                                  ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                  ,CSL_0
                                  ,NULL);
END SP_BTC_EXE_APPLY_LATE_FEE;

/

  GRANT EXECUTE ON SC_CREDIT.SP_BTC_EXE_APPLY_LATE_FEE TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_BTC_EXE_APPLY_LATE_FEE TO USRBTCCREDIT1;
--------------------------------------------------------
--  DDL for Function FN_GET_NEXT_LOAN_BALANCE_ID
--------------------------------------------------------

  CREATE OR REPLACE  FUNCTION SC_CREDIT.FN_GET_NEXT_LOAN_BALANCE_ID RETURN SC_CREDIT.TA_LOAN_BALANCE.FI_LOAN_BALANCE_ID%TYPE IS
/*************************************************************
  PROJECT    :  NCP
  DESCRIPTION:  FUNCTION TO GET THE NEXT LOAN_BALANCE_ID VALUE.
  CREATOR:      JESUS BRAVO.
  CREATED DATE: AGO-13-2024
*************************************************************/
    VL_NEXT_LOAN_BALANCE_ID     SC_CREDIT.TA_LOAN_BALANCE.FI_LOAN_BALANCE_ID%TYPE;

  BEGIN
    VL_NEXT_LOAN_BALANCE_ID  := SC_CREDIT.SE_LOAN_BALANCE_ID.NEXTVAL;

    RETURN VL_NEXT_LOAN_BALANCE_ID;

  EXCEPTION
    WHEN OTHERS THEN
        VL_NEXT_LOAN_BALANCE_ID := -1;
        SC_CREDIT.SP_ERROR_LOG('FN_GET_NEXT_LOAN_BALANCE_ID', SQLCODE, SQLERRM, DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, '', '');
        RETURN VL_NEXT_LOAN_BALANCE_ID;


END FN_GET_NEXT_LOAN_BALANCE_ID;

/

  GRANT EXECUTE ON SC_CREDIT.FN_GET_NEXT_LOAN_BALANCE_ID TO USRNCPCREDIT1;
  
  --------------------------------------------------------
--  DDL for Function FN_GET_NEXT_LOAN_OPERATION_ID
--------------------------------------------------------

  CREATE OR REPLACE  FUNCTION SC_CREDIT.FN_GET_NEXT_LOAN_OPERATION_ID RETURN SC_CREDIT.TA_LOAN_OPERATION.FI_LOAN_OPERATION_ID%TYPE AS
/*************************************************************
  PROJECT    :  NCP
  DESCRIPTION:  FUNCTION TO GET THE NEXT LOAN OPERATION ID VALUE
  CREATOR:      RICARDO GM.
  CREATED DATE: 2024-10-10
*************************************************************/

BEGIN
 RETURN SC_CREDIT.SE_LOAN_OPERATION_ID.NEXTVAL;

EXCEPTION
    WHEN OTHERS THEN

        SC_CREDIT.SP_ERROR_LOG('FN_GET_NEXT_LOAN_ID', SQLCODE, SQLERRM, DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, '', '');
        RETURN -1;



END FN_GET_NEXT_LOAN_OPERATION_ID;

/

  GRANT EXECUTE ON SC_CREDIT.FN_GET_NEXT_LOAN_OPERATION_ID TO USRNCPCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_BTC_GENERATE_SEQUENCE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_BTC_GENERATE_SEQUENCE 
   (
    PA_NUMBER_SEQUENCES     IN NUMBER
   ,PA_STATUS_CODE          OUT NUMBER
   ,PA_STATUS_MSG           OUT VARCHAR2
   ,PA_CUR_SEQUENCE         OUT SC_CREDIT.PA_TYPES.TYP_CURSOR
   )
   IS
  /* **************************************************************
   * DESCRIPTION: SEQUENCE CALCULATION PROCEDURE
   * PRECONDITIONS: LOAN FORMALIZATION
   * CREATED DATE: 30/10/2024
   * CREATOR: CRISTHIAN MORALES
   ************************************************************** */

   --CONSTANTS
   CSL_ARROW             CONSTANT VARCHAR2(20)   := '->';
   CSL_0                 CONSTANT SIMPLE_INTEGER := 0;
   CSL_MSG_SUCCESS       CONSTANT VARCHAR2(20)   := 'SUCCESS';
   CSL_1                 CONSTANT SIMPLE_INTEGER := 1;
   CSL_PKG               CONSTANT SIMPLE_INTEGER := 1;
   CSL_INVALID_SEQUENCE  CONSTANT VARCHAR2(20)   := 'INVALID SEQUENCE';

   EXC_INVALID_SEQUENCE_PARAMS EXCEPTION;
   BEGIN

   --VALIDATION PA_NUMER_SEQUENCES IS NOT NULL
   IF PA_NUMBER_SEQUENCES IS NULL OR PA_NUMBER_SEQUENCES <= 0 THEN
      RAISE EXC_INVALID_SEQUENCE_PARAMS;
   END IF;

      PA_STATUS_CODE:=CSL_0;
      PA_STATUS_MSG :=CSL_MSG_SUCCESS;
      PA_CUR_SEQUENCE :=NULL;

   OPEN PA_CUR_SEQUENCE FOR
      SELECT ROWNUM AS ORDEN
            ,SC_CREDIT.FN_GET_NEXT_LOAN_OPERATION_ID   AS OPERATION_SEQ
            ,SC_CREDIT.FN_GET_NEXT_LOAN_BALANCE_ID     AS BALANCE_SEQ
        FROM DUAL CONNECT BY
       LEVEL <= PA_NUMBER_SEQUENCES;

   --EXCEPTION HANDLING
   EXCEPTION
      WHEN EXC_INVALID_SEQUENCE_PARAMS THEN
      PA_STATUS_CODE := CSL_1;
      PA_STATUS_MSG := CSL_INVALID_SEQUENCE;

      SC_CREDIT.SP_BATCH_ERROR_LOG(
            UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_PKG)
            ,SQLCODE
            ,CSL_INVALID_SEQUENCE
            ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
            ,CSL_0
            ,NULL
            );

   WHEN OTHERS THEN
      ROLLBACK;
      PA_STATUS_CODE := SQLCODE;
      PA_STATUS_MSG := SQLERRM || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;

       SC_CREDIT.SP_BATCH_ERROR_LOG(
            UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_PKG)
            ,SQLCODE
            ,SQLERRM
            ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
            ,CSL_0
            ,NULL
            );

   END SP_BTC_GENERATE_SEQUENCE;

/

  GRANT EXECUTE ON SC_CREDIT.SP_BTC_GENERATE_SEQUENCE TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_BTC_GENERATE_SEQUENCE TO USRBTCCREDIT1;

--------------------------------------------------------
--  DDL for Procedure SP_SYNC_ACCRUED_TYPE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_SYNC_ACCRUED_TYPE (
    PA_SYNC_JSON  		  VARCHAR2,
    PA_UPDATED_ROWS	OUT NUMBER,
    PA_STATUS_CODE	OUT NUMBER,
    PA_STATUS_MSG   OUT VARCHAR2
  )
/* **************************************************************
* PROJECT: NCP
* DESCRIPTION: CATALOG SYNCHONIZATION TC_ACCRUED_TYPE
* CREATED DATE: 2025/01/09
* CREATOR: CESAR CORTES
* MODIFICATION DATE: 2025/01/09
************************************************************** */
IS
BEGIN
	PA_STATUS_CODE := 0;
  PA_STATUS_MSG  := 'OK';

  MERGE INTO SC_CREDIT.TC_ACCRUED_TYPE A
  USING (
    SELECT
      *
    FROM
      JSON_TABLE ( PA_SYNC_JSON, '$.accruedType[*]'
        COLUMNS (
          ID NUMBER PATH '$.id',
          DESCRIPTION VARCHAR2 ( 50 ) PATH '$.description',
          STATUS NUMBER PATH '$.status',
          USER_NAME VARCHAR2 ( 50 ) PATH '$.user',
          CREATED_DATE TIMESTAMP PATH '$.createdDate',
          MODIFICATION_DATE TIMESTAMP PATH '$.modificationDate'
        )
      )
  ) B ON ( A.FI_ACCRUED_TYPE_ID = B.ID )
  WHEN MATCHED THEN UPDATE
  SET A.FC_ACCRUED_TYPE_DESC = B.DESCRIPTION,
      A.FI_STATUS = B.STATUS,
      A.FC_USER = B.USER_NAME,
      A.FD_MODIFICATION_DATE = CAST(B.MODIFICATION_DATE AS DATE)
  WHEN NOT MATCHED THEN
  INSERT (
    FI_ACCRUED_TYPE_ID,
    FC_ACCRUED_TYPE_DESC,
    FI_STATUS,
    FC_USER,
    FD_CREATED_DATE,
    FD_MODIFICATION_DATE )
  VALUES
    ( B.ID,
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
      SC_CREDIT.SP_ERROR_LOG('SP_SYNC_ACCRUED_TYPE', SQLCODE, SQLERRM, DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, NULL, '');

END SP_SYNC_ACCRUED_TYPE;

/

  GRANT EXECUTE ON SC_CREDIT.SP_SYNC_ACCRUED_TYPE TO USRPURPOSEWS;
  GRANT EXECUTE ON SC_CREDIT.SP_SYNC_ACCRUED_TYPE TO USRNCPCREDIT1;

--------------------------------------------------------
--  DDL for Procedure SP_TMP_BTC_SEL_NO_PAYMENT
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_TMP_BTC_SEL_NO_PAYMENT 
   (PA_FIRST_CENTER_ID        IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE
   ,PA_END_CENTER_ID          IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE
   ,PA_TODAY                  IN VARCHAR2
   ,PA_PROCESS                IN SC_CREDIT.TA_TMP_LOAN_PROCESS.FI_PROCESS%TYPE
   ,PA_TRACK                  IN SC_CREDIT.TA_TMP_LOAN_PROCESS.FI_TRACK%TYPE
   ,PA_STATUS_CODE            OUT NUMBER
   ,PA_STATUS_MSG             OUT VARCHAR2
   ,PA_CUR_SELECT             OUT SC_CREDIT.PA_TYPES.TYP_CURSOR)

   IS
      ----------------------------------------------------------------------
      -- CREATOR: Eduardo Cervantes Hernandez
      -- CREATED DATE:   26/12/2024
      -- DESCRIPTION: Select loan delinquent
      -- APPLICATION:  Process Batch of Purpose
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
                  ,LO.FI_COUNTRY_ID                     AS FI_COUNTRY_ID
                  ,LO.FI_COMPANY_ID                     AS FI_COMPANY_ID
                  ,LO.FI_BUSINESS_UNIT_ID               AS FI_BUSINESS_UNIT_ID
                  ,LO.FI_CURRENT_BALANCE_SEQ            AS FI_CURRENT_BALANCE_SEQ
                  ,LO.FI_LOAN_STATUS_ID                 AS FI_LOAN_STATUS_ID
                  ,LO.FI_PRODUCT_ID                     AS FI_PRODUCT_ID
                  ,LO.FI_RULE_ID                        AS FI_RULE_ID
                  ,LO.FC_UUID_TRACKING                  AS FC_UUID_TRACKING
                  ,LO.FN_PRINCIPAL_BALANCE              AS FN_PRINCIPAL_BALANCE
                  ,LO.FN_FINANCE_CHARGE_BALANCE         AS FN_FINANCE_CHARGE_BALANCE
                  ,LO.FN_ADDITIONAL_CHARGE_BALANCE      AS FN_ADDITIONAL_CHARGE_BALANCE
                  ,LO.FC_PLATFORM_ID                    AS FC_PLATFORM_ID
                  ,LO.FC_SUB_PLATFORM_ID                AS FC_SUB_PLATFORM_ID
                  ,LO.FC_CUSTOMER_ID                    AS FC_CUSTOMER_ID
                  ,PS.FI_PAYMENT_NUMBER_ID              AS FI_PAYMENT_NUMBER_ID
                  ,TRUNC(TO_DATE(PA_TODAY, CSL_DATE) - PS.FD_DUE_DATE) AS FI_DAYS_DELINQUENT
                  ,ROW_NUMBER() OVER (PARTITION BY LO.FI_LOAN_ID ORDER BY PS.FI_PAYMENT_NUMBER_ID ASC,PS.FI_PAYMENT_NUMBER_ID ASC) AS ORDEN
              FROM SC_CREDIT.TA_TMP_LOAN_PROCESS LP
        INNER JOIN SC_CREDIT.TA_LOAN LO
                ON LP.FI_LOAN_ID = LO.FI_LOAN_ID
               AND LP.FI_ADMIN_CENTER_ID = LO.FI_ADMIN_CENTER_ID
        INNER JOIN SC_CREDIT.TA_PAYMENT_SCHEDULE PS
                ON LO.FI_LOAN_ID = PS.FI_LOAN_ID
               AND LO.FI_ADMIN_CENTER_ID = PS.FI_ADMIN_CENTER_ID
               AND PS.FI_PMT_SCHEDULE_STATUS_ID = CSL_1
               AND PS.FD_DUE_DATE <= TO_DATE(PA_TODAY, CSL_DATE)
             WHERE LO.FI_LOAN_STATUS_ID = CSL_2
               AND PS.FI_PAYMENT_SCHEDULE_ID >= CSL_1    
               AND PS.FI_LOAN_ID >= CSL_1
               AND LP.FI_PROCESS = PA_PROCESS
               AND LP.FI_TRACK = PA_TRACK
         )
         SELECT FI_LOAN_ID                                  AS FI_LOAN_ID
               ,FI_ADMIN_CENTER_ID                          AS FI_ADMIN_CENTER_ID
               ,FI_COUNTRY_ID                               AS FI_COUNTRY_ID
               ,FI_COMPANY_ID                               AS FI_COMPANY_ID
               ,FI_BUSINESS_UNIT_ID                         AS FI_BUSINESS_UNIT_ID
               ,FI_CURRENT_BALANCE_SEQ                      AS FI_CURRENT_BALANCE_SEQ
               ,FI_LOAN_STATUS_ID                           AS FI_LOAN_STATUS_ID
               ,NVL(FI_PRODUCT_ID, CSL_0)                   AS FI_PRODUCT_ID
               ,FI_RULE_ID                                  AS FI_RULE_ID
               ,FI_PAYMENT_NUMBER_ID                        AS FI_PAYMENT_NUMBER_ID
               ,FI_DAYS_DELINQUENT                          AS FI_DAYS_DELINQUENT
               ,FN_PRINCIPAL_BALANCE                        AS FN_PRINCIPAL_BALANCE
               ,FN_FINANCE_CHARGE_BALANCE                   AS FN_FINANCE_CHARGE_BALANCE
               ,FN_ADDITIONAL_CHARGE_BALANCE                AS FN_ADDITIONAL_CHARGE_BALANCE
               ,SC_CREDIT.FN_SEL_LOAN_BALANCE_DET_JSON
                  (LO.FI_LOAN_ID
                  ,LO.FI_ADMIN_CENTER_ID
                  ,LO.FI_CURRENT_BALANCE_SEQ
                  ,FC_UUID_TRACKING) AS BALANCE_DET_JSON
               ,FC_PLATFORM_ID                              AS FC_PLATFORM_ID
               ,FC_SUB_PLATFORM_ID                          AS FC_SUB_PLATFORM_ID
               ,FC_CUSTOMER_ID                              AS FC_CUSTOMER_ID
           FROM TABLOAN LO
          WHERE LO.FI_DAYS_DELINQUENT >= CSL_0
            AND LO.ORDEN = CSL_1;


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

END SP_TMP_BTC_SEL_NO_PAYMENT;

/

  GRANT EXECUTE ON SC_CREDIT.SP_TMP_BTC_SEL_NO_PAYMENT TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_TMP_BTC_SEL_NO_PAYMENT TO USRBTCCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_TMP_BTC_SEL_NO_PAYMENT TO USRCREDIT02;
--------------------------------------------------------
--  DDL for Procedure SP_SYNC_TENDER_TYPE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_SYNC_TENDER_TYPE 
 (
    PA_SYNC_JSON        CLOB,
    PA_UPDATED_ROWS OUT NUMBER,
    PA_STATUS_CODE  OUT NUMBER,
    PA_STATUS_MSG   OUT VARCHAR2
  ) IS
/* **************************************************************
* PROJECT: NCP
* DESCRIPTION: CATALOG SYNCHONIZATION TC_TENDER_TYPE
* CREATED DATE: 2025/01/09
* CREATOR: CESAR CORTES
* MODIFICATION DATE: 2025/01/09
************************************************************** */
  BEGIN
    PA_STATUS_CODE := 0;
    PA_STATUS_MSG  := 'OK';

     MERGE INTO SC_CREDIT.TC_TENDER_TYPE A
  USING (
    SELECT
      *
    FROM
      JSON_TABLE ( PA_SYNC_JSON, '$.tenderType[*]'
        COLUMNS (
          ID NUMBER PATH '$.id',
          description VARCHAR2 ( 50 ) PATH '$.description',
          CATEGORY_ID NUMBER PATH '$.category',
          STATUS NUMBER PATH '$.status',
          USER_NAME VARCHAR2 ( 50 ) PATH '$.user',
          CREATED_DATE TIMESTAMP PATH '$.createdDate',
          MODIFICATION_DATE TIMESTAMP PATH '$.modificationDate'
        )
      )
  ) B ON ( A.FI_TENDER_TYPE_ID = B.ID )
  WHEN MATCHED THEN UPDATE
  SET A.FC_TENDER_TYPE_DESC = B.description,
      A.FI_TENDER_CATEGORY_TYPE_ID = B.CATEGORY_ID,
	    A.FI_STATUS=B.STATUS,
      A.FC_USER = B.USER_NAME,
      A.FD_CREATED_DATE = B.CREATED_DATE,
      A.FD_MODIFICATION_DATE = CAST(B.MODIFICATION_DATE AS DATE)
  WHEN NOT MATCHED THEN
  INSERT (
    FI_TENDER_TYPE_ID,
    FC_TENDER_TYPE_DESC,
    FI_TENDER_CATEGORY_TYPE_ID,
    FI_STATUS,
    FC_USER,
    FD_CREATED_DATE,
    FD_MODIFICATION_DATE )
  VALUES
    ( B.ID,
      B.description,
      B.CATEGORY_ID,
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
      SC_CREDIT.SP_ERROR_LOG('SP_SYNC_TENDER_TYPE', SQLCODE, SQLERRM, DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, NULL,'');

END SP_SYNC_TENDER_TYPE;

/

  GRANT EXECUTE ON SC_CREDIT.SP_SYNC_TENDER_TYPE TO USRPURPOSEWS;
  GRANT EXECUTE ON SC_CREDIT.SP_SYNC_TENDER_TYPE TO USRNCPCREDIT1;

--------------------------------------------------------
--  DDL for Procedure SP_INS_LOAN_OPERATION_VOID
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_INS_LOAN_OPERATION_VOID (
    PA_LOAN_ID               IN SC_CREDIT.TA_LOAN_OPERATION_VOID.FI_LOAN_ID%TYPE,
    PA_ADMIN_CENTER_ID       IN SC_CREDIT.TA_LOAN_OPERATION_VOID.FI_ADMIN_CENTER_ID%TYPE,
    PA_OPERATION_REQUEST_ID  IN SC_CREDIT.TA_LOAN_OPERATION_VOID.FI_OPERATION_REQUEST_ID%TYPE,
    PA_OPERATION_TYPE_ID     IN SC_CREDIT.TA_LOAN_OPERATION_VOID.FI_OPERATION_TYPE_ID%TYPE,
    PA_OPERATION_REVERSED_ID IN SC_CREDIT.TA_LOAN_OPERATION_VOID.FI_OPERATION_REVERSED_ID%TYPE,
    PA_REVERSE_REASON        IN SC_CREDIT.TA_LOAN_OPERATION_VOID.FC_REVERSE_REASON%TYPE,
    PA_LOAN_VOID_TOKEN       IN SC_CREDIT.TA_LOAN_TOKEN_VOID.FI_LOAN_VOID_TOKEN%TYPE,
    PA_USER                  IN SC_CREDIT.TA_LOAN_OPERATION_VOID.FC_USER%TYPE,
    PA_IP_ADDRESS            IN SC_CREDIT.TA_LOAN_OPERATION_VOID.FC_IP_ADDRESS%TYPE,
    PA_UUID_TRACKING         IN SC_CREDIT.TA_LOAN_OPERATION_VOID.FC_UUID_TRACKING%TYPE,
    PA_STATUS_CODE           OUT NUMBER,
    PA_STATUS_MSG            OUT VARCHAR2
)
IS
 /* ********************************************************************
 * PROJECT: CORE LOAN
 * DESCRIPTION: PROCEDURE FOR SAVING INFORMATION OF THE OPERATION_ID WHICH
 *              REQUESTS REVERSING OTHER OPERATION_ID; AND IF IT IS THE CASE,
 *              A TOKEN IS SAVED AS WELL FOR POSTERIOR REVERSING ONCE ONE DAY
 *              HAS PASSED AFTER THE INITIAL REVERSING REQUEST.
 * PRECONDITIONS: PRE-EXISTING LOANS AND OPERATIONS
 * CREATED DATE: 06/01/2025
 * CREATOR: GILBERTO CHAVEZ MUNOZ
 * UPDATE: VICTOR DANIEL GUTIERREZ RODIRGUEZ
 ***********************************************************************/
  CSL_0            CONSTANT SIMPLE_INTEGER := 0;
  CSL_1            CONSTANT SIMPLE_INTEGER := 1;
  CSL_204          CONSTANT SIMPLE_INTEGER := 204;
  CSL_ARROW        CONSTANT VARCHAR2(5) := '->';
  CSL_JSON         CONSTANT VARCHAR2(5) := NULL;
  CSL_SUCCESS      CONSTANT VARCHAR2(8) := 'SUCCESS';
  CSL_NO_INSERTION CONSTANT VARCHAR2(20) := 'NO DATA INSERTED';
  CSL_SP           CONSTANT SIMPLE_INTEGER := 1;
  VG_LOAN_OPERATION_VOID_ID SC_CREDIT.TA_LOAN_OPERATION_VOID.FI_LOAN_OPERATION_VOID_ID%TYPE;

BEGIN



INSERT INTO SC_CREDIT.TA_LOAN_OPERATION_VOID(
    FI_LOAN_OPERATION_VOID_ID,
    FI_LOAN_ID,
    FI_ADMIN_CENTER_ID,
    FI_OPERATION_REQUEST_ID,
    FI_OPERATION_TYPE_ID,
    FI_OPERATION_REVERSED_ID,
    FC_REVERSE_REASON,
    FC_USER,
    FC_IP_ADDRESS,
    FD_CREATED_DATE,
    FD_MODIFICATION_DATE,
    FC_UUID_TRACKING)
VALUES(
          SC_CREDIT.SE_LOAN_OPERATION_VOID_ID.NEXTVAL,
          PA_LOAN_ID,
          PA_ADMIN_CENTER_ID,
          PA_OPERATION_REQUEST_ID,
          PA_OPERATION_TYPE_ID,
          PA_OPERATION_REVERSED_ID,
          PA_REVERSE_REASON,
          PA_USER,
          PA_IP_ADDRESS,
          SYSDATE,
          SYSDATE,
          PA_UUID_TRACKING)
    RETURNING FI_LOAN_OPERATION_VOID_ID INTO VG_LOAN_OPERATION_VOID_ID ;


IF PA_LOAN_VOID_TOKEN > CSL_0 THEN

            INSERT INTO SC_CREDIT.TA_LOAN_TOKEN_VOID(
                FI_LOAN_TOKEN_VOID_ID,
                FI_LOAN_OPERATION_VOID_ID,
                FI_LOAN_VOID_TOKEN,
                FC_USER,
                FC_IP_ADDRESS,
                FD_CREATED_DATE,
                FD_MODIFICATION_DATE,
                FC_UUID_TRACKING)
            VALUES(
                SC_CREDIT.SE_LOAN_TOKEN_VOID_ID.NEXTVAL,
                VG_LOAN_OPERATION_VOID_ID,
                PA_LOAN_VOID_TOKEN,
                PA_USER,
                PA_IP_ADDRESS,
                SYSDATE,
                SYSDATE,
                PA_UUID_TRACKING);

END IF;

COMMIT;
PA_STATUS_CODE := CSL_0;
        PA_STATUS_MSG  := CSL_SUCCESS;


EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    PA_STATUS_CODE := CSL_204;
    PA_STATUS_MSG := CSL_NO_INSERTION;
    PA_STATUS_CODE := SQLCODE;
    PA_STATUS_MSG := SQLERRM || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    SC_CREDIT.SP_ERROR_LOG(
       UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP)
       ,SQLCODE
       ,SQLERRM
       ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
       ,NULL
       ,CSL_JSON
       );

END SP_INS_LOAN_OPERATION_VOID;

/

  GRANT EXECUTE ON SC_CREDIT.SP_INS_LOAN_OPERATION_VOID TO USRNCPCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_SEL_CONCEPT
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_SEL_CONCEPT (
    PA_CUR_RESULT               OUT SC_CREDIT.PA_TYPES.TYP_CURSOR,
    PA_STATUS_CODE              OUT NUMBER,
    PA_STATUS_MSG               OUT VARCHAR2
    )
    IS

/*****************************************************************
  *PROJECT:      NCP
  *DESCRIPTION:  GET CATALOGS OF OPERATION TYPE AND SIGN OF THE OPERATION
  *CREATOR:      CARLOS EDUARDO_MARTINEZ CANTERO
  *CREATED DATE: NOV-22-2024
  *MODIFICATION: 2024-12-05 CARLOS EDUARDO MARTINEZ CANTERO
  V1.1: ADD STORED SC_CREDIT.SP_ERROR_LOG_
*****************************************************************/
        CSL_ISSUE_CONCEPT_CODE CONSTANT              SIMPLE_INTEGER   := -20010;
    CSL_ISSUE_CONCEPT_MSG CONSTANT               VARCHAR2(100)    := 'FAILED TO IN SP_SEL_CONCEPT';
    CSG_SUCCESS_CODE CONSTANT SIMPLE_INTEGER := 0;
    CSG_SUCCESS_MSG CONSTANT VARCHAR2(15) := 'SUCCESS';

BEGIN
        OPEN PA_CUR_RESULT FOR
SELECT FI_LOAN_CONCEPT_ID, FI_BALANCE_CATEGORY_ID  FROM SC_CREDIT.TC_LOAN_CONCEPT WHERE FI_STATUS=1;

    PA_STATUS_CODE := CSG_SUCCESS_CODE;
    PA_STATUS_MSG := CSG_SUCCESS_MSG;
        EXCEPTION
   WHEN OTHERS THEN
    PA_STATUS_CODE := SQLCODE;
    PA_STATUS_MSG := SQLERRM;
   SC_CREDIT.SP_ERROR_LOG (CSL_ISSUE_CONCEPT_MSG, SQLCODE, SQLERRM,
                                   DBMS_UTILITY.FORMAT_ERROR_BACKTRACE,
                                   CSL_ISSUE_CONCEPT_CODE, 'SP_SEL_CONCEPT_P');

END SP_SEL_CONCEPT;

/

  GRANT EXECUTE ON SC_CREDIT.SP_SEL_CONCEPT TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_SEL_CONCEPT TO USRPURPOSEWS;
  GRANT EXECUTE ON SC_CREDIT.SP_SEL_CONCEPT TO USRCREDIT02;
--------------------------------------------------------
--  DDL for Procedure SP_SYNC_LOAN_CONCEPT
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_SYNC_LOAN_CONCEPT (
    PA_SYNC_JSON   		CLOB,
    PA_UPDATED_ROWS	OUT NUMBER,
    PA_STATUS_CODE	OUT NUMBER,
    PA_STATUS_MSG	OUT VARCHAR2
  ) IS
  /*************************************************************
	* PROJECT : NCP-LOAN DESIGNER PARAMETRIA
	* DESCRIPTION: PACKAGE ASYNC CATALOGS
	* CREATOR: TALLER DE PRODUCTOS
	* CREATED DATE: 2024-11-16
	* MODIFICATION: 2025-01-21
	* [NCPTPR-10763 V1] FOR ADDED COLUM CATALOG
************************************************************** */
  CSL_0 CONSTANT SIMPLE_INTEGER := 0;
BEGIN
  PA_STATUS_CODE := CSL_0;
  PA_STATUS_MSG := 'OK';
  PA_UPDATED_ROWS  := CSL_0;

  MERGE INTO SC_CREDIT.TC_LOAN_CONCEPT A
  USING (
    SELECT
      *
    FROM
      JSON_TABLE ( PA_SYNC_JSON, '$.loanConcept[*]'
        COLUMNS (
          ID NUMBER PATH '$.id',
          description VARCHAR2 ( 50 ) PATH '$.description',
          conceptType NUMBER PATH '$.conceptType',
          FC_KEY VARCHAR2 ( 50 ) PATH '$.key',
          STATUS NUMBER PATH '$.status',
          USER_NAME VARCHAR2 ( 50 ) PATH '$.user',
          CREATED_DATE TIMESTAMP PATH '$.createdDate',
          MODIFICATION_DATE TIMESTAMP PATH '$.modificationDate',
          BALANCECATEGORY NUMBER PATH '$.balanceCategory'
        )
      )
  ) B ON ( A.FI_LOAN_CONCEPT_ID = B.ID )
  WHEN MATCHED THEN UPDATE
  SET A.FC_LOAN_CONCEPT_DESC = B.description,
	  A.FI_LOAN_CONCEPT_TYPE_ID =B.conceptType,
	  A.FC_KEY=B.FC_KEY,
      A.FI_STATUS = B.STATUS,
      A.FC_USER = B.USER_NAME,
      A.FD_MODIFICATION_DATE = CAST(B.MODIFICATION_DATE AS DATE),
      A.FI_BALANCE_CATEGORY_ID = B.BALANCECATEGORY
  WHEN NOT MATCHED THEN
  INSERT (
    FI_LOAN_CONCEPT_ID,
    FC_LOAN_CONCEPT_DESC,
    FI_LOAN_CONCEPT_TYPE_ID,
    FC_KEY,
    FI_STATUS,
    FC_USER,
    FD_CREATED_DATE,
    FD_MODIFICATION_DATE,
    FI_BALANCE_CATEGORY_ID)
  VALUES
    ( B.ID,
      B.description,
      B.conceptType,
      B.FC_KEY,
      B.STATUS,
      B.USER_NAME,
      B.CREATED_DATE,
      B.MODIFICATION_DATE,
      B.BALANCECATEGORY);

      PA_UPDATED_ROWS := SQL%ROWCOUNT;

   COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      PA_STATUS_CODE := SQLCODE;
    PA_STATUS_MSG  := SQLERRM;
      SC_CREDIT.SP_ERROR_LOG('PA_ACCRUED_TYPE->SP_SYNC_LOAN_CONCEPT', SQLCODE, SQLERRM, DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, NULL,'');


END SP_SYNC_LOAN_CONCEPT;

/

  GRANT EXECUTE ON SC_CREDIT.SP_SYNC_LOAN_CONCEPT TO USRPURPOSEWS;
  GRANT EXECUTE ON SC_CREDIT.SP_SYNC_LOAN_CONCEPT TO USRNCPCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_SYNC_PRODUCT
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_SYNC_PRODUCT (
  PA_SYNC_JSON    IN  CLOB,
  PA_UPDATED_ROWS OUT NUMBER,
  PA_STATUS_CODE  OUT NUMBER,
  PA_STATUS_MSG   OUT VARCHAR2
) IS
/* **************************************************************
* PROJECT: NCP
* DESCRIPTION: CATALOG SYNCHONIZATION TC_PRODUCT
* CREATED DATE: 2025/01/09
* CREATOR: CESAR CORTES
* MODIFICATION DATE: 2025/01/09
************************************************************** */
BEGIN
  PA_STATUS_CODE := 0;
  PA_STATUS_MSG := 'OK';
  MERGE INTO SC_CREDIT.TC_PRODUCT A
  USING (
    SELECT
      *
    FROM
      JSON_TABLE ( PA_SYNC_JSON, '$.product[*]'
        COLUMNS (
          FI_PRODUCT_ID NUMBER ( 10 ) PATH '$.id',
          FI_COUNTRY_ID NUMBER ( 3 ) PATH '$.country',
          FI_COMPANY_ID NUMBER ( 3 ) PATH '$.company',
          FC_PRODUCT_NAME VARCHAR2 ( 50 ) PATH '$.name',
          FC_PRODUCT_DESC VARCHAR2 ( 150 ) PATH '$.description',
          FD_RELEASE_DATE TIMESTAMP PATH '$.releaseDate',
          FC_PRODUCT_ADAPTER_ID VARCHAR2 ( 32 ) PATH '$.adapterId',
          FI_STATUS NUMBER ( 2 ) PATH '$.status',
          FC_USER VARCHAR2 ( 30 ) PATH '$.user',
          FD_CREATED_DATE TIMESTAMP PATH '$.createdDate',
          FD_MODIFICATION_DATE TIMESTAMP PATH '$.modificationDate'
        )
      )
  ) B ON ( A.FI_PRODUCT_ID = B.FI_PRODUCT_ID )
  WHEN MATCHED THEN UPDATE
  SET A.FI_COUNTRY_ID = B.FI_COUNTRY_ID,
      A.FI_COMPANY_ID = B.FI_COMPANY_ID,
      A.FC_PRODUCT_NAME = B.FC_PRODUCT_NAME,
      A.FC_PRODUCT_DESC = B.FC_PRODUCT_DESC,
      A.FD_RELEASE_DATE = CAST(B.FD_RELEASE_DATE AS DATE),
      A.FC_PRODUCT_ADAPTER_ID = B.FC_PRODUCT_ADAPTER_ID,
      A.FI_STATUS = B.FI_STATUS,
      A.FC_USER = B.FC_USER,
      A.FD_MODIFICATION_DATE = CAST(B.FD_MODIFICATION_DATE AS DATE)
  WHEN NOT MATCHED THEN
  INSERT (
    FI_PRODUCT_ID,
    FI_COUNTRY_ID,
    FI_COMPANY_ID,
    FC_PRODUCT_NAME,
    FC_PRODUCT_DESC,
    FD_RELEASE_DATE,
    FC_PRODUCT_ADAPTER_ID,
    FI_STATUS,
    FC_USER,
    FD_CREATED_DATE,
    FD_MODIFICATION_DATE )
  VALUES
    ( B.FI_PRODUCT_ID,
      B.FI_COUNTRY_ID,
      B.FI_COMPANY_ID,
      B.FC_PRODUCT_NAME,
      B.FC_PRODUCT_DESC,
    CAST(B.FD_RELEASE_DATE AS DATE),
      B.FC_PRODUCT_ADAPTER_ID,
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
    PA_STATUS_MSG := SQLERRM;
    SC_CREDIT.SP_ERROR_LOG('SP_SYNC_PRODUCT', SQLCODE, SQLERRM, DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, NULL,'');

END SP_SYNC_PRODUCT;

/

  GRANT EXECUTE ON SC_CREDIT.SP_SYNC_PRODUCT TO USRPURPOSEWS;
  GRANT EXECUTE ON SC_CREDIT.SP_SYNC_PRODUCT TO USRNCPCREDIT1;

--------------------------------------------------------
--  DDL for Procedure SP_SYNC_COMPANY
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_SYNC_COMPANY (
    PA_SYNC_JSON  		  CLOB,
    PA_UPDATED_ROWS OUT NUMBER,
    PA_STATUS_CODE	OUT NUMBER,
    PA_STATUS_MSG	  OUT VARCHAR2
  ) IS
/* **************************************************************
* PROJECT: NCP
* DESCRIPTION: CATALOG SYNCHONIZATION TC_COMPANY
* CREATED DATE: 2025/01/09
* CREATOR: CESAR CORTES
* MODIFICATION DATE: 2025/01/09
************************************************************** */
  CSL_0 CONSTANT SIMPLE_INTEGER := 0;
BEGIN
  PA_STATUS_CODE := CSL_0;
  PA_STATUS_MSG := 'OK';
  PA_UPDATED_ROWS  := CSL_0;

  MERGE INTO SC_CREDIT.TC_COMPANY A
    USING (
    SELECT
      *
    FROM
      JSON_TABLE ( PA_SYNC_JSON, '$.company[*]'
        COLUMNS (
          ID NUMBER PATH '$.id',
          COUNTRY NUMBER PATH '$.country',
          NAME VARCHAR2 ( 50 ) PATH '$.name',
          STATUS NUMBER PATH '$.status',
          USER_NAME VARCHAR2 ( 50 ) PATH '$.user',
          CREATED_DATE TIMESTAMP PATH '$.createdDate',
          MODIFICATION_DATE TIMESTAMP PATH '$.modificationDate'
        )
      )
  ) B ON ( A.FI_COMPANY_ID = B.ID AND A.FI_COUNTRY_ID = B.COUNTRY)
  WHEN MATCHED THEN UPDATE
  SET A.FC_COMPANY_NAME = B.NAME,
      A.FI_STATUS = B.STATUS,
      A.FC_USER = B.USER_NAME,
      A.FD_MODIFICATION_DATE = CAST(B.MODIFICATION_DATE AS DATE)
  WHEN NOT MATCHED THEN
  INSERT (
    FI_COMPANY_ID,
    FI_COUNTRY_ID,
    FC_COMPANY_NAME,
    FI_STATUS,
    FC_USER,
    FD_CREATED_DATE,
    FD_MODIFICATION_DATE )
  VALUES
    ( B.ID,
      B.COUNTRY,
      B.NAME,
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
      SC_CREDIT.SP_ERROR_LOG('SP_SYNC_COMPANY', SQLCODE, SQLERRM, DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, NULL,'');

  END SP_SYNC_COMPANY;

/

  GRANT EXECUTE ON SC_CREDIT.SP_SYNC_COMPANY TO USRPURPOSEWS;
  GRANT EXECUTE ON SC_CREDIT.SP_SYNC_COMPANY TO USRNCPCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_BTC_GEN_STA_DEF_WOFF
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_BTC_GEN_STA_DEF_WOFF 
   (PTAB_PWO_AMOUNT_DETAIL      IN SC_CREDIT.TYP_TAB_BTC_STATUS_PWO
   ,PTAB_STATUS_DETAIL          IN SC_CREDIT.TYP_TAB_BTC_STATUS_DETAIL
   ,PTAB_STATUS                 IN SC_CREDIT.TYP_TAB_BTC_STATUS
   ,PTAB_LOANS                  IN SC_CREDIT.TYP_TAB_BTC_LOAN
   ,PTAB_OPERATIONS             IN SC_CREDIT.TYP_TAB_BTC_OPERATION
   ,PTAB_OPERATIONS_DETAIL      IN SC_CREDIT.TYP_TAB_BTC_DETAIL
   ,PTAB_BALANCES               IN SC_CREDIT.TYP_TAB_BTC_BALANCE
   ,PTAB_BALANCES_DETAIL        IN SC_CREDIT.TYP_TAB_BTC_DETAIL
   ,PA_DEVICE                   IN SC_CREDIT.TA_LOAN_OPERATION.FC_DEVICE%TYPE
   ,PA_GPS_LATITUDE             IN SC_CREDIT.TA_LOAN_OPERATION.FC_GPS_LATITUDE%TYPE
   ,PA_GPS_LONGITUDE            IN SC_CREDIT.TA_LOAN_OPERATION.FC_GPS_LONGITUDE%TYPE
   ,PA_STATUS_CODE              OUT NUMBER
   ,PA_STATUS_MSG               OUT VARCHAR2
   ,PA_RECORDS_READ             OUT NUMBER
   ,PA_RECORDS_SUCCESS          OUT NUMBER

   ,PA_RECORDS_ERROR            OUT NUMBER
   ,PTAB_ERROR_RECORDS          OUT SC_CREDIT.TYP_TAB_BTC_ERROR
   )
IS

 /* **************************************************************
  * DESCRIPTION: PROCESS TO INSERT IN TABLE LOAN_STATUS_DETAIL,LOAN,STATUS,LOAN_OPERATION,TA_PWO_AMOUNT_DETAIL
               LOAN_BALANCE
  * CREATOR: IVAN LOPEZ
  * CREATED DATE:       15/11/2024
  * MODIFICATION DATE:  09/01/2025
  * USER MODIFICATION:  AIXA SARMIENTO
************************************************************** */
   --CONSTANTS
   CSL_0                  CONSTANT SIMPLE_INTEGER := 0;
   CSL_1                  CONSTANT SIMPLE_INTEGER := 1;
   CSL_6                  CONSTANT SIMPLE_INTEGER := 6;
   CSL_4                  CONSTANT SIMPLE_INTEGER := 4;
   CSL_PKG                CONSTANT SIMPLE_INTEGER := 1;

   --CONSTANTS SUCCESS
   CSL_SUCCESS_CODE       CONSTANT SIMPLE_INTEGER := 0;
   CSL_SUCCESS_MSG        CONSTANT VARCHAR2(10)   := 'SUCCESS';
   CSL_NUMERROR           CONSTANT SIMPLE_INTEGER := -20012;
   CSL_SPACE              CONSTANT VARCHAR2(2)    := ' ';
   CSL_SUCCESS_ERROR      CONSTANT VARCHAR2(30)   := 'SUCCESS, WITH ERRORS RECORDS';
   CSL_TYPE_NULL          CONSTANT VARCHAR2(15)   := 'TYPE PWO NULL';
   CSL_DATE_FORMAT        CONSTANT VARCHAR2(40)   := 'MM/DD/YYYY hh24:mi:ss';
   CSL_ARROW              CONSTANT VARCHAR2(5)    := ' -> ';
   VL_DASH                CONSTANT VARCHAR2(5)    :=  '-';
   VL_STATUS_CODE         NUMBER(10,0) := 0;
   VL_STATUS_MSG          VARCHAR2(1000);
   VL_PROCESS_DESC        VARCHAR2(150);
   VL_I                   NUMBER(10,0) := 0;
   CSL_DAY                CONSTANT DATE:= SYSDATE;
   --VARIABLES INTERNAL TYPES ASSIGNMENT
   VL_TAB_ERRORS             SC_CREDIT.TYP_TAB_BTC_ERROR;
   VL_TAB_LOANS              SC_CREDIT.TYP_TAB_BTC_LOAN;
   VL_TAB_OPERATIONS         SC_CREDIT.TYP_TAB_BTC_OPERATION;
   VL_TAB_OPERATIONS_DETAIL  SC_CREDIT.TYP_TAB_BTC_DETAIL;
   VL_TAB_BALANCES           SC_CREDIT.TYP_TAB_BTC_BALANCE;
   VL_TAB_BALANCES_DETAIL    SC_CREDIT.TYP_TAB_BTC_DETAIL;

      --VARIABLES OF ITERATION BY LOAN
   VL_REC_LOAN                        SC_CREDIT.TYP_REC_BTC_LOAN;
   VL_TAB_OPERATIONS_BY_LOAN          SC_CREDIT.TYP_TAB_BTC_OPERATION;
   VL_TAB_OPERATIONS_DET_BY_LOAN      SC_CREDIT.TYP_TAB_BTC_DETAIL;
   VL_TAB_BALANCES_BY_LOAN            SC_CREDIT.TYP_TAB_BTC_BALANCE;
   VL_TAB_BALANCES_DET_BY_LOAN        SC_CREDIT.TYP_TAB_BTC_DETAIL;

   VL_DESC_COUNT                     VARCHAR2(500);

BEGIN
   PA_STATUS_CODE := CSL_SUCCESS_CODE;
   PA_STATUS_MSG := CSL_SUCCESS_MSG;
   PA_RECORDS_SUCCESS := 0;
   PA_RECORDS_ERROR := 0;
   PA_RECORDS_READ := 0;
   VL_TAB_ERRORS := SC_CREDIT.TYP_TAB_BTC_ERROR();


   IF PTAB_STATUS_DETAIL IS NULL THEN
      RAISE_APPLICATION_ERROR( CSL_NUMERROR, CSL_TYPE_NULL);
   END IF;

   VL_I := PTAB_STATUS_DETAIL.FIRST;
   PA_RECORDS_READ := PTAB_STATUS_DETAIL.COUNT;

--INTERNAL TYPES ASSIGNMENT
   VL_TAB_LOANS             := SC_CREDIT.TYP_TAB_BTC_LOAN();
   VL_TAB_LOANS             := PTAB_LOANS;
   VL_TAB_OPERATIONS        := SC_CREDIT.TYP_TAB_BTC_OPERATION();
   VL_TAB_OPERATIONS        := PTAB_OPERATIONS;
   VL_TAB_OPERATIONS_DETAIL := SC_CREDIT.TYP_TAB_BTC_DETAIL();
   VL_TAB_OPERATIONS_DETAIL := PTAB_OPERATIONS_DETAIL;
   VL_TAB_BALANCES          := SC_CREDIT.TYP_TAB_BTC_BALANCE();
   VL_TAB_BALANCES          := PTAB_BALANCES;
   VL_TAB_BALANCES_DETAIL   := SC_CREDIT.TYP_TAB_BTC_DETAIL();
   VL_TAB_BALANCES_DETAIL   := PTAB_BALANCES_DETAIL;

   WHILE (VL_I IS NOT NULL) LOOP
      BEGIN

         VL_TAB_OPERATIONS_BY_LOAN := SC_CREDIT.TYP_TAB_BTC_OPERATION();
         VL_TAB_OPERATIONS_DET_BY_LOAN := SC_CREDIT.TYP_TAB_BTC_DETAIL();
         VL_TAB_BALANCES_BY_LOAN := SC_CREDIT.TYP_TAB_BTC_BALANCE();
         VL_TAB_BALANCES_DET_BY_LOAN := SC_CREDIT.TYP_TAB_BTC_DETAIL();


		 --TAB BY LOAN ASSIGNMENT
		 <<loopLoanAssignment>>
		 WHILE VL_TAB_LOANS.COUNT > CSL_0 AND PTAB_STATUS_DETAIL.EXISTS(VL_I) LOOP
		    IF VL_TAB_LOANS(VL_TAB_LOANS.FIRST).FI_LOAN_ID = PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID AND
		       VL_TAB_LOANS(VL_TAB_LOANS.FIRST).FI_ADMIN_CENTER_ID = PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID THEN --

		          VL_REC_LOAN := VL_TAB_LOANS(VL_TAB_LOANS.FIRST);
		          VL_TAB_LOANS.DELETE(VL_TAB_LOANS.FIRST);
		    ELSE
		       EXIT loopLoanAssignment;
		    END IF;
		 END LOOP loopLoanAssignment;

            --TAB BY OPERATION ASSIGNMENT
		 <<loopOperationAssignment>>
		 WHILE VL_TAB_OPERATIONS.COUNT > CSL_0 AND PTAB_STATUS_DETAIL.EXISTS(VL_I) LOOP
		    IF VL_TAB_OPERATIONS(VL_TAB_OPERATIONS.FIRST).FI_LOAN_ID = PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID AND
		       VL_TAB_OPERATIONS(VL_TAB_OPERATIONS.FIRST).FI_ADMIN_CENTER_ID = PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID THEN

		          VL_TAB_OPERATIONS_BY_LOAN.EXTEND;
		          VL_TAB_OPERATIONS_BY_LOAN(VL_TAB_OPERATIONS_BY_LOAN.LAST) := VL_TAB_OPERATIONS(VL_TAB_OPERATIONS.FIRST);
		          VL_TAB_OPERATIONS.DELETE(VL_TAB_OPERATIONS.FIRST);
		    ELSE
		       EXIT loopOperationAssignment;
		    END IF;
		 END LOOP loopOperationAssignment;

		    --TAB BY OPERATION DET ASSIGNMENT
		 <<loopOperationDetAssignment>>
		 WHILE VL_TAB_OPERATIONS_DETAIL.COUNT > CSL_0 AND PTAB_STATUS_DETAIL.EXISTS(VL_I) LOOP
		    IF VL_TAB_OPERATIONS_DETAIL(VL_TAB_OPERATIONS_DETAIL.FIRST).FI_LOAN_ID = PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID AND
		       VL_TAB_OPERATIONS_DETAIL(VL_TAB_OPERATIONS_DETAIL.FIRST).FI_ADMIN_CENTER_ID = PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID THEN

		          VL_TAB_OPERATIONS_DET_BY_LOAN.EXTEND;
		          VL_TAB_OPERATIONS_DET_BY_LOAN(VL_TAB_OPERATIONS_DET_BY_LOAN.LAST) := VL_TAB_OPERATIONS_DETAIL(VL_TAB_OPERATIONS_DETAIL.FIRST);
		          VL_TAB_OPERATIONS_DETAIL.DELETE(VL_TAB_OPERATIONS_DETAIL.FIRST);
		    ELSE
		       EXIT loopOperationDetAssignment;
		    END IF;
		 END LOOP loopOperationDetAssignment;

		    --TAB BY BALANCES ASSIGNMENT
		 <<loopBalanceAssignment>>
		 WHILE VL_TAB_BALANCES.COUNT > CSL_0 AND PTAB_STATUS_DETAIL.EXISTS(VL_I) LOOP
		    IF VL_TAB_BALANCES(VL_TAB_BALANCES.FIRST).FI_LOAN_ID = PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID AND
		       VL_TAB_BALANCES(VL_TAB_BALANCES.FIRST).FI_ADMIN_CENTER_ID = PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID THEN

		          VL_TAB_BALANCES_BY_LOAN.EXTEND;
		          VL_TAB_BALANCES_BY_LOAN(VL_TAB_BALANCES_BY_LOAN.LAST) := VL_TAB_BALANCES(VL_TAB_BALANCES.FIRST);
		          VL_TAB_BALANCES.DELETE(VL_TAB_BALANCES.FIRST);
		    ELSE
		       EXIT loopBalanceAssignment;
		    END IF;
		 END LOOP loopBalanceAssignment;

		    --TAB BY BALANCES DET ASSIGNMENT
		 <<loopBalanceDetAssignment>>
		 WHILE VL_TAB_BALANCES_DETAIL.COUNT > CSL_0 AND PTAB_STATUS_DETAIL.EXISTS(VL_I) LOOP
		    IF VL_TAB_BALANCES_DETAIL(VL_TAB_BALANCES_DETAIL.FIRST).FI_LOAN_ID = PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID AND
		       VL_TAB_BALANCES_DETAIL(VL_TAB_BALANCES_DETAIL.FIRST).FI_ADMIN_CENTER_ID = PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID THEN

		          VL_TAB_BALANCES_DET_BY_LOAN.EXTEND;
		          VL_TAB_BALANCES_DET_BY_LOAN(VL_TAB_BALANCES_DET_BY_LOAN.LAST) := VL_TAB_BALANCES_DETAIL(VL_TAB_BALANCES_DETAIL.FIRST);
		          VL_TAB_BALANCES_DETAIL.DELETE(VL_TAB_BALANCES_DETAIL.FIRST);
		    ELSE
		       EXIT loopBalanceDetAssignment;
		    END IF;
		 END LOOP loopBalanceDetAssignment;

		 IF PTAB_STATUS_DETAIL(VL_I).FI_LOAN_STATUS_ID = CSL_6 THEN

		    VL_PROCESS_DESC := 'SP_BTC_INS_LOAN_STATUS_DETAIL';
		    SC_CREDIT.SP_BTC_INS_LOAN_STATUS_DETAIL
		        (PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID
		        ,PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID
		        ,PTAB_STATUS_DETAIL(VL_I).FI_LOAN_STATUS_ID
		        ,PTAB_STATUS_DETAIL(VL_I).FI_ACTION_DETAIL_ID
		        ,PTAB_STATUS_DETAIL(VL_I).FI_COUNTER_DAY
		        ,PTAB_STATUS_DETAIL(VL_I).FD_INITIAL_DATE
		        ,PTAB_STATUS_DETAIL(VL_I).FI_PAYMENT_NUMBER_ID
		        ,PTAB_STATUS_DETAIL(VL_I).FD_FINAL_DATE
		        ,PTAB_STATUS_DETAIL(VL_I).FI_ON_OFF
		        ,CSL_0
		        ,VL_STATUS_CODE
		        ,VL_STATUS_MSG);

		    IF(VL_STATUS_CODE != CSL_0)THEN
		        RAISE_APPLICATION_ERROR( CSL_NUMERROR, VL_PROCESS_DESC || CSL_SPACE || VL_STATUS_MSG);
		    END IF;

		    VL_PROCESS_DESC := 'SP_BTC_EXE_OPERATION_BALANCE';
		    SC_CREDIT.SP_BTC_EXE_OPERATION_BALANCE
		        (VL_REC_LOAN
		        ,VL_TAB_OPERATIONS_BY_LOAN
		        ,VL_TAB_OPERATIONS_DET_BY_LOAN
		        ,VL_TAB_BALANCES_BY_LOAN
		        ,VL_TAB_BALANCES_DET_BY_LOAN
		        ,PA_DEVICE
		        ,PA_GPS_LATITUDE
		        ,PA_GPS_LONGITUDE
		        ,CSL_0
		        ,VL_STATUS_CODE
		        ,VL_STATUS_MSG);

		    IF(VL_STATUS_CODE != CSL_0)THEN
		        RAISE_APPLICATION_ERROR( CSL_NUMERROR, VL_PROCESS_DESC || CSL_SPACE || VL_STATUS_MSG);
		    END IF;

		    VL_PROCESS_DESC := 'SP_BTC_INS_LOAN_STATUS';
		    SC_CREDIT.SP_BTC_INS_LOAN_STATUS
		        (PTAB_STATUS(VL_I).FI_LOAN_ID
		        ,PTAB_STATUS(VL_I).FI_ADMIN_CENTER_ID
		        ,PTAB_STATUS(VL_I).FI_LOAN_OPERATION_ID
		        ,PTAB_STATUS(VL_I).FI_LOAN_STATUS_ID
		        ,PTAB_STATUS(VL_I).FI_LOAN_STATUS_OLD_ID
		        ,PTAB_STATUS(VL_I).FI_TRIGGER_ID
		        ,PTAB_STATUS(VL_I).FD_LOAN_STATUS_DATE
		        ,CSL_1
		        ,CSL_0
		        ,VL_STATUS_CODE
		        ,VL_STATUS_MSG);

		    IF(VL_STATUS_CODE != CSL_0)THEN
		        RAISE_APPLICATION_ERROR( CSL_NUMERROR, VL_PROCESS_DESC || CSL_SPACE || VL_STATUS_MSG);
		    END IF;

		 ELSE

		    IF PTAB_PWO_AMOUNT_DETAIL(VL_I).FI_LOAN_ID != CSL_0 THEN
               VL_PROCESS_DESC := 'SP_BTC_INS_PWO_AMOUNT_DETAIL';
               SC_CREDIT.SP_BTC_INS_PWO_AMOUNT_DETAIL(
                  PTAB_PWO_AMOUNT_DETAIL(VL_I).FI_LOAN_ID,
                  PTAB_PWO_AMOUNT_DETAIL(VL_I).FI_ADMIN_CENTER_ID,
                  PTAB_PWO_AMOUNT_DETAIL(VL_I).FN_PAY_OFF_AMOUNT,
                  PTAB_PWO_AMOUNT_DETAIL(VL_I).FN_PWO_EXT_PAYMENT,
                  PTAB_PWO_AMOUNT_DETAIL(VL_I).FN_AMOUNT_PAID,
                  PTAB_PWO_AMOUNT_DETAIL(VL_I).FN_PWO_MIN_PAYMENT,
                  PTAB_PWO_AMOUNT_DETAIL(VL_I).FI_ADD_EXTENSION,
                  PTAB_PWO_AMOUNT_DETAIL(VL_I).FD_PWO_DATE,
                  CSL_0,
                  VL_STATUS_CODE,
                  VL_STATUS_MSG);

                  IF(VL_STATUS_CODE != CSL_0)THEN
                     RAISE_APPLICATION_ERROR( CSL_NUMERROR, VL_PROCESS_DESC || CSL_SPACE || VL_STATUS_MSG);
                  END IF;
		    END IF;

		    UPDATE SC_CREDIT.TA_LOAN_STATUS_DETAIL LD
               SET LD.FD_FINAL_DATE = TO_DATE(PTAB_STATUS_DETAIL(VL_I).FD_FINAL_DATE,CSL_DATE_FORMAT)
                  ,LD.FD_MODIFICATION_DATE = CSL_DAY
		     WHERE LD.FI_LOAN_ID = PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID
               AND LD.FI_ADMIN_CENTER_ID = PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID
               AND LD.FI_LOAN_STATUS_ID = PTAB_STATUS_DETAIL(VL_I).FI_LOAN_STATUS_ID
               AND LD.FI_ACTION_DETAIL_ID = PTAB_STATUS_DETAIL(VL_I).FI_ACTION_DETAIL_ID
               AND LD.FI_ON_OFF=CSL_1;

		 END IF;

         --DELETE TYPES BY LOAN (CYCLE)
		 VL_TAB_OPERATIONS_BY_LOAN.DELETE;
         VL_TAB_OPERATIONS_DET_BY_LOAN.DELETE;
         VL_TAB_BALANCES_BY_LOAN.DELETE;
         VL_TAB_BALANCES_DET_BY_LOAN.DELETE;

         PA_RECORDS_SUCCESS := PA_RECORDS_SUCCESS + CSL_1;

         --EXCEPTION
      EXCEPTION
         WHEN OTHERS THEN
            ROLLBACK;
               SC_CREDIT.SP_BATCH_ERROR_LOG(UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_PKG)
                                           ,SQLCODE
                                           ,SQLERRM
                                           ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                           ,CSL_1
                                           ,PTAB_OPERATIONS(VL_I).FI_LOAN_ID ||VL_DASH || PTAB_OPERATIONS(VL_I).FI_ADMIN_CENTER_ID || VL_DASH || PTAB_OPERATIONS(VL_I).FI_LOAN_OPERATION_ID);

               PA_RECORDS_ERROR :=PA_RECORDS_ERROR + CSL_1;

               VL_TAB_ERRORS.EXTEND;
               VL_TAB_ERRORS(VL_TAB_ERRORS.LAST) :=
               SC_CREDIT.TYP_REC_BTC_ERROR(PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID
                                          ,PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID
                                          ,UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_PKG)
                                          ,SQLCODE
                                          ,SQLERRM
                                          ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                          ,SYSDATE
                                          ,CSL_1
                                          ,NULL);
      END;
      VL_I := PTAB_STATUS_DETAIL.NEXT(VL_I);
      COMMIT;
   END LOOP;

   VL_DESC_COUNT := ' TAB_LOANS '||VL_TAB_LOANS.COUNT
                  ||' TAB_OPERATIONS '||VL_TAB_OPERATIONS.COUNT
                  ||' TAB_OPERATIONS_DETAIL '||VL_TAB_OPERATIONS_DETAIL.COUNT
                  ||' TAB_BALANCES '||VL_TAB_BALANCES.COUNT
                  ||' TAB_BALANCES_DETAIL '||VL_TAB_BALANCES_DETAIL.COUNT;

   --DELETE TYPES INTERNS
   VL_TAB_LOANS.DELETE;
   VL_TAB_OPERATIONS.DELETE;
   VL_TAB_OPERATIONS_DETAIL.DELETE;
   VL_TAB_BALANCES.DELETE;
   VL_TAB_BALANCES_DETAIL.DELETE;

   PTAB_ERROR_RECORDS := VL_TAB_ERRORS;
   IF(PA_RECORDS_ERROR > CSL_0)THEN
      PA_STATUS_CODE := CSL_1;
      PA_STATUS_MSG := CSL_SUCCESS_ERROR;
   END IF;

   PA_STATUS_MSG := PA_STATUS_MSG
      || ' ' || VL_DESC_COUNT;

EXCEPTION
   WHEN OTHERS THEN
      PA_STATUS_CODE := SQLCODE;
      PA_STATUS_MSG := SQLERRM || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;

   SC_CREDIT.SP_BATCH_ERROR_LOG(UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_PKG)
                               ,SQLCODE
                               ,SQLERRM
                               ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                               ,CSL_0
                               ,PTAB_OPERATIONS(VL_I).FI_LOAN_ID ||VL_DASH || PTAB_OPERATIONS(VL_I).FI_ADMIN_CENTER_ID || VL_DASH || PTAB_OPERATIONS(VL_I).FI_LOAN_OPERATION_ID);
END SP_BTC_GEN_STA_DEF_WOFF;

/

  GRANT EXECUTE ON SC_CREDIT.SP_BTC_GEN_STA_DEF_WOFF TO USRBTCCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_BTC_GEN_STA_DEF_WOFF TO USRNCPCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_BTC_SEL_DELINQUENT
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_BTC_SEL_DELINQUENT 
    (PA_FIRST_CENTER_ID        IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE
    ,PA_END_CENTER_ID          IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE
    ,PA_TODAY                  IN VARCHAR2
    ,PA_STATUS_CODE            OUT NUMBER
    ,PA_STATUS_MSG             OUT VARCHAR2
    ,PA_CUR_FEES               OUT SC_CREDIT.PA_TYPES.TYP_CURSOR)
IS
    /* **************************************************************
    * PROJECT: LOAN-LIFE-CYCLE
    * DESCRIPTION: SELECT LOAN DELINQUENT
    * CREATED DATE: 14/11/2024
    * CREATOR: ITZEL TRINIDAD RAMOS
    * MODIFICATION DATE: 05/12/2024
    ************************************************************** */
    CSL_DATE         CONSTANT VARCHAR2(10)   := 'MM/DD/YYYY';
    CSL_0            CONSTANT SIMPLE_INTEGER := 0;
    CSL_1            CONSTANT SIMPLE_INTEGER := 1;
    CSL_3            CONSTANT SIMPLE_INTEGER := 3;
    CSL_MSG_SUCCESS  CONSTANT VARCHAR2(7)   := 'SUCCESS';
    CSL_SP           CONSTANT SIMPLE_INTEGER := 2;
    CSL_ARROW        CONSTANT VARCHAR2(2)    := '->';
    CSL_COMMA        CONSTANT VARCHAR2(3) := ' , ';
BEGIN
    PA_STATUS_CODE := CSL_0;
    PA_STATUS_MSG  := CSL_MSG_SUCCESS;
    PA_CUR_FEES :=  NULL;

    OPEN PA_CUR_FEES FOR
        SELECT TL.FI_LOAN_ID AS FI_LOAN_ID
             ,TL.FI_ADMIN_CENTER_ID AS FI_ADMIN_CENTER_ID
             ,TL.FC_CUSTOMER_ID AS FC_CUSTOMER_ID
             ,TL.FI_COUNTRY_ID AS FI_COUNTRY_ID
             ,TL.FI_COMPANY_ID AS FI_COMPANY_ID
             ,TL.FI_BUSINESS_UNIT_ID AS FI_BUSINESS_UNIT_ID
             ,TL.FN_PRINCIPAL_BALANCE AS FN_PRINCIPAL_BALANCE
             ,TL.FN_ADDITIONAL_CHARGE_BALANCE AS FN_ADDITIONAL_CHARGE_BALANCE
             ,TL.FN_FINANCE_CHARGE_BALANCE AS FN_FINANCE_CHARGE_BALANCE
             ,TL.FI_PRODUCT_ID AS FI_PRODUCT_ID
             ,TL.FI_RULE_ID AS FI_RULE_ID
             ,TL.FI_CURRENT_BALANCE_SEQ AS FI_CURRENT_BALANCE_SEQ
             ,TL.FI_LOAN_STATUS_ID AS FI_LOAN_STATUS_ID
             ,TLSD.FI_COUNTER_DAY AS FI_COUNTER_DAY
             ,TLSD.FI_PAYMENT_NUMBER_ID AS FI_PAYMENT_NUMBER_ID
             ,TLSD.FI_ACTION_DETAIL_ID AS FI_ACTION_DETAIL_ID
             ,PS.FN_PAYMENT_AMOUNT AS FN_PAYMENT_AMOUNT
             ,SC_CREDIT.FN_SEL_LOAN_BALANCE_DET_JSON
                      (TL.FI_LOAN_ID
                      ,TL.FI_ADMIN_CENTER_ID
                      ,TL.FI_CURRENT_BALANCE_SEQ
                      ,NULL) AS BALANCE_DET_JSON
        FROM SC_CREDIT.TA_LOAN TL
        INNER JOIN SC_CREDIT.TA_LOAN_STATUS_DETAIL TLSD
            ON TL.FI_LOAN_ID = TLSD.FI_LOAN_ID
            AND TL.FI_ADMIN_CENTER_ID = TLSD.FI_ADMIN_CENTER_ID
            AND TL.FI_LOAN_STATUS_ID = TLSD.FI_LOAN_STATUS_ID
        INNER JOIN SC_CREDIT.TA_PAYMENT_SCHEDULE PS
            ON TL.FI_LOAN_ID = PS.FI_LOAN_ID
            AND TL.FI_ADMIN_CENTER_ID = PS.FI_ADMIN_CENTER_ID
            AND TLSD.FI_PAYMENT_NUMBER_ID = PS.FI_PAYMENT_NUMBER_ID
        WHERE TLSD.FI_LOAN_STATUS_ID = CSL_3
        AND TLSD.FI_ON_OFF = CSL_1
        AND TLSD.FI_COUNTER_DAY >= CSL_1
        AND TLSD.FI_ADMIN_CENTER_ID BETWEEN PA_FIRST_CENTER_ID AND PA_END_CENTER_ID
        AND TLSD.FD_MODIFICATION_DATE < TO_DATE(PA_TODAY, CSL_DATE)
        AND TLSD.FI_REGISTRATION_NUMBER >= CSL_1;
EXCEPTION
WHEN OTHERS THEN
    PA_STATUS_CODE := SQLCODE;
    PA_STATUS_MSG := SQLERRM || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;

    SC_CREDIT.SP_BATCH_ERROR_LOG (UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP)
                                     ,SQLCODE
                                     ,SQLERRM
                                     ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                     ,CSL_0
                                     ,PA_FIRST_CENTER_ID || CSL_COMMA || PA_END_CENTER_ID || CSL_COMMA || PA_TODAY
                                     );
END SP_BTC_SEL_DELINQUENT;

/

  GRANT EXECUTE ON SC_CREDIT.SP_BTC_SEL_DELINQUENT TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_BTC_SEL_DELINQUENT TO USRBTCCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_SYNC_LOAN_STATUS_TRIGGER
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_SYNC_LOAN_STATUS_TRIGGER (
    PA_SYNC_JSON   		  CLOB,
    PA_UPDATED_ROWS	OUT NUMBER,
    PA_STATUS_CODE 	OUT NUMBER,
    PA_STATUS_MSG  	OUT VARCHAR2
  ) IS
/* **************************************************************
* PROJECT: NCP
* DESCRIPTION: CATALOG SYNCHONIZATION TC_LOAN_STATUS_TRIGGER
* CREATED DATE: 2025/01/09
* CREATOR: CESAR CORTES
* MODIFICATION DATE: 2025/01/09
************************************************************** */
  CSL_0 CONSTANT SIMPLE_INTEGER := 0;
BEGIN
  PA_STATUS_CODE := CSL_0;
  PA_STATUS_MSG := 'OK';
  PA_UPDATED_ROWS  := CSL_0;

  MERGE INTO SC_CREDIT.TC_LOAN_STATUS_TRIGGER A
  USING (
    SELECT
      *
    FROM
      JSON_TABLE ( PA_SYNC_JSON, '$.loanStatusTrigger[*]'
        COLUMNS (
          ID NUMBER PATH '$.id',
          TRIGGER_ID NUMBER PATH '$.trigger',
          TRIGGERS NUMBER PATH '$.triggers',
          isStayDay NUMBER PATH '$.isStayDay',
          stayDay NUMBER PATH '$.stayDay',
          nextStatus NUMBER PATH '$.nextStatus',
          STATUS NUMBER PATH '$.status',
          USER_NAME VARCHAR2 ( 50 ) PATH '$.user',
          CREATED_DATE TIMESTAMP PATH '$.createdDate',
          MODIFICATION_DATE TIMESTAMP PATH '$.modificationDate'
        )
      )
  ) B ON ( A.FI_LOAN_STATUS_ID = B.ID and a.FI_TRIGGER_ID = b.TRIGGER_ID AND a.FI_NEXT_STATUS = nextStatus )
  WHEN MATCHED THEN UPDATE
  SET A.FI_IS_TRIGGERS=B.TRIGGERS,
      A.FI_IS_STAY_DAY = B.isStayDay,
      A.FI_STAY_DAY = B.stayDay,
      A.FI_STATUS= B.STATUS,
      A.FC_USER = B.USER_NAME,
      A.FD_MODIFICATION_DATE = CAST(B.MODIFICATION_DATE AS DATE)
  WHEN NOT MATCHED THEN
  INSERT (
    FI_LOAN_STATUS_ID,
    FI_TRIGGER_ID,
    FI_IS_TRIGGERS,
    FI_IS_STAY_DAY,
    FI_STAY_DAY,
    FI_NEXT_STATUS,
    FI_STATUS,
    FC_USER,
    FD_CREATED_DATE,
    FD_MODIFICATION_DATE )
  VALUES
    ( B.ID,
      B.TRIGGER_ID,
      B.TRIGGERS,
      B.isStayDay,
      B.stayDay,
      B.nextStatus,
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
      SC_CREDIT.SP_ERROR_LOG('SP_SYNC_LOAN_STATUS_TRIGGER', SQLCODE, SQLERRM, DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, NULL,'');

  END SP_SYNC_LOAN_STATUS_TRIGGER;

/

  GRANT EXECUTE ON SC_CREDIT.SP_SYNC_LOAN_STATUS_TRIGGER TO USRPURPOSEWS;
  GRANT EXECUTE ON SC_CREDIT.SP_SYNC_LOAN_STATUS_TRIGGER TO USRNCPCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_BTC_SEL_DEFAULT_WRITE_OFF
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_BTC_SEL_DEFAULT_WRITE_OFF 
   (PA_FIRST_CENTER_ID        IN   SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE
   ,PA_END_CENTER_ID          IN   SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE
   ,PA_LOAN_STATUS_DATE       IN   VARCHAR2
   ,PA_STATUS_ID              IN   SC_CREDIT.TA_LOAN.FI_LOAN_STATUS_ID%TYPE
   ,PA_STATUS_CODE            OUT  NUMBER
   ,PA_STATUS_MSG             OUT  VARCHAR2
   ,PA_CUR_RESULTS            OUT  SC_CREDIT.PA_TYPES.TYP_CURSOR
   )
IS

/************************************************************************************************************************
PROJECT:            PURPOSE_LIFE_LOAN_CYCLE
DESCRIPTION:        THIS STORE PROCEDURE EXECUTES A CONSULT ON TA_LOAN TABLE AND TA_PWO_AMOUNT_DETAIL AND RETURNS A CURSOR
                    WITH THE INFORMATION OF THE LOANS WITH WAITING STATUS
PRECONDITIONS:      IT MUST RECEIVE A RANGE OF ADMIN CENTERS, THE DESIRED LOAN STATUS DATE AND THE STATUS ID
CREATOR:            CESAR MEDINA
CREATED DATE:       13/11/2024
MODIFICATION DATE:  09/01/2025
USER MODIFICATION:  AIXA SARMIENTO
*************************************************************************************************************************/
   CSL_ARROW            CONSTANT VARCHAR2(5)    :=  '-->';
   CSL_0                CONSTANT SIMPLE_INTEGER :=  0;
   CSL_1                CONSTANT SIMPLE_INTEGER :=  1;
   CSL_SUCCESS_MESSAGE  CONSTANT VARCHAR2(10)   :=  'SUCCESS';
   CSL_SP               CONSTANT SIMPLE_INTEGER :=  1;
   CSL_DATE_FORMAT      CONSTANT VARCHAR2(12)   := 'MM/DD/YYYY';
   VL_DATE_STATUS       DATE;

BEGIN
   PA_CUR_RESULTS   :=   NULL;
   PA_STATUS_CODE   :=   CSL_0;
   PA_STATUS_MSG    :=   CSL_SUCCESS_MESSAGE;
   VL_DATE_STATUS      := TO_DATE(PA_LOAN_STATUS_DATE,CSL_DATE_FORMAT);

-- SP CONSULT TA_LOAN

   OPEN PA_CUR_RESULTS FOR
      SELECT A.FI_LOAN_ID
            ,A.FI_ADMIN_CENTER_ID
            ,A.FI_COUNTRY_ID
            ,A.FI_COMPANY_ID
            ,A.FI_BUSINESS_UNIT_ID
            ,A.FI_PRODUCT_ID
            ,A.FC_CUSTOMER_ID
            ,A.FN_PRINCIPAL_AMOUNT
            ,A.FN_FINANCE_CHARGE_AMOUNT
            ,A.FN_PRINCIPAL_BALANCE
            ,A.FN_FINANCE_CHARGE_BALANCE
            ,A.FN_ADDITIONAL_CHARGE_BALANCE
            ,A.FI_CURRENT_BALANCE_SEQ
            ,A.FI_LOAN_STATUS_ID
            ,TO_CHAR(A.FD_LOAN_STATUS_DATE,CSL_DATE_FORMAT) AS FD_LOAN_STATUS_DATE
            ,A.FI_RULE_ID
            ,TO_CHAR(C.FD_PWO_DATE,CSL_DATE_FORMAT) AS FD_PWO_DATE
            ,NVL(SC_CREDIT.FN_SEL_LOAN_BALANCE_DET_JSON(A.FI_LOAN_ID
                                                       ,A.FI_ADMIN_CENTER_ID
                                                       ,A.FI_CURRENT_BALANCE_SEQ
                                                       ,NULL),'[]') AS FJ_BALANCE_DETAIL
        FROM SC_CREDIT.TA_LOAN A
        LEFT JOIN SC_CREDIT.TA_PWO_AMOUNT_DETAIL C
          ON C.FI_LOAN_ID = A.FI_LOAN_ID
         AND C.FI_ADMIN_CENTER_ID = A.FI_ADMIN_CENTER_ID
       WHERE A.FI_LOAN_STATUS_ID = PA_STATUS_ID
         AND A.FI_ADMIN_CENTER_ID BETWEEN PA_FIRST_CENTER_ID AND PA_END_CENTER_ID
         AND ((TRUNC(A.FD_LOAN_STATUS_DATE)  <= VL_DATE_STATUS AND C.FD_PWO_DATE IS NULL) OR ( C.FD_PWO_DATE <  VL_DATE_STATUS ));

EXCEPTION
   WHEN OTHERS THEN
      PA_STATUS_CODE   := SQLCODE;
      PA_STATUS_MSG    := SQLERRM || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;

      SC_CREDIT.SP_BATCH_ERROR_LOG (UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP)
                                   ,SQLCODE
                                   ,SQLERRM
                                   ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                   ,CSL_0
                                   ,PA_FIRST_CENTER_ID || PA_END_CENTER_ID || PA_LOAN_STATUS_DATE || PA_STATUS_ID);
END SP_BTC_SEL_DEFAULT_WRITE_OFF;

/

  GRANT EXECUTE ON SC_CREDIT.SP_BTC_SEL_DEFAULT_WRITE_OFF TO USRBTCCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_BTC_SEL_DEFAULT_WRITE_OFF TO USRNCPCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_SYNC_DAY_TYPES
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_SYNC_DAY_TYPES 
 (
    PA_SYNC_JSON        CLOB,
    PA_UPDATED_ROWS OUT NUMBER,
    PA_STATUS_CODE  OUT NUMBER,
    PA_STATUS_MSG   OUT VARCHAR2
  ) IS
/* **************************************************************
* PROJECT: NCP
* DESCRIPTION: CATALOG SYNCHONIZATION TC_DAY_TYPE
* CREATED DATE: 2025/01/09
* CREATOR: CESAR CORTES
* MODIFICATION DATE: 2025/01/09
************************************************************** */
  BEGIN
    PA_STATUS_CODE := 0;
    PA_STATUS_MSG  := 'OK';

     MERGE INTO SC_CREDIT.TC_DAY_TYPE A
  USING (
    SELECT
      *
    FROM
      JSON_TABLE ( PA_SYNC_JSON, '$.dayTypes[*]'
        COLUMNS (
          ID NUMBER PATH '$.id',
          DESCRIPTION VARCHAR2 ( 50 ) PATH '$.description',
          RANGE_DAY VARCHAR2 ( 50 ) PATH '$.rangeDay',
          STATUS NUMBER PATH '$.status',
          USER_NAME VARCHAR2 ( 50 ) PATH '$.user',
          CREATED_DATE TIMESTAMP PATH '$.createdDate',
          MODIFICATION_DATE TIMESTAMP PATH '$.modificationDate'
        )
      )
  ) B ON ( A.FI_DAY_TYPE_ID = B.ID )
  WHEN MATCHED THEN UPDATE
  SET A.FC_DAY_TYPE_DESC = B.DESCRIPTION,
      A.FC_RANGE_DAY = B.RANGE_DAY,
      A.FI_STATUS=B.STATUS,
      A.FC_USER = B.USER_NAME,
      A.FD_CREATED_DATE = B.CREATED_DATE,
      A.FD_MODIFICATION_DATE = CAST(B.MODIFICATION_DATE AS DATE)
  WHEN NOT MATCHED THEN
  INSERT (
    FI_DAY_TYPE_ID,
    FC_DAY_TYPE_DESC,
    FC_RANGE_DAY,
    FI_STATUS,
    FC_USER,
    FD_CREATED_DATE,
    FD_MODIFICATION_DATE )
  VALUES
    ( B.ID,
      B.DESCRIPTION,
      B.RANGE_DAY,
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
      SC_CREDIT.SP_ERROR_LOG('SP_SYNC_DAY_TYPES', SQLCODE, SQLERRM, DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, NULL,'');

  END SP_SYNC_DAY_TYPES;

/

  GRANT EXECUTE ON SC_CREDIT.SP_SYNC_DAY_TYPES TO USRPURPOSEWS;
  GRANT EXECUTE ON SC_CREDIT.SP_SYNC_DAY_TYPES TO USRNCPCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_BTC_EXE_NO_PAYMENT
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_BTC_EXE_NO_PAYMENT (
   PTAB_STATUS_DETAIL           IN SC_CREDIT.TYP_TAB_BTC_STATUS_DETAIL
   ,PTAB_STATUS                 IN SC_CREDIT.TYP_TAB_BTC_STATUS
   ,PTAB_LOANS                  IN SC_CREDIT.TYP_TAB_BTC_LOAN
   ,PTAB_OPERATIONS             IN SC_CREDIT.TYP_TAB_BTC_OPERATION
   ,PTAB_OPERATIONS_DETAIL      IN SC_CREDIT.TYP_TAB_BTC_DETAIL
   ,PTAB_BALANCES               IN SC_CREDIT.TYP_TAB_BTC_BALANCE
   ,PTAB_BALANCES_DETAIL        IN SC_CREDIT.TYP_TAB_BTC_DETAIL
   ,PA_DEVICE                   IN SC_CREDIT.TA_LOAN_OPERATION.FC_DEVICE%TYPE
   ,PA_GPS_LATITUDE             IN SC_CREDIT.TA_LOAN_OPERATION.FC_GPS_LATITUDE%TYPE
   ,PA_GPS_LONGITUDE            IN SC_CREDIT.TA_LOAN_OPERATION.FC_GPS_LONGITUDE%TYPE
   ,PA_STATUS_CODE              OUT NUMBER
   ,PA_STATUS_MSG               OUT VARCHAR2
   ,PA_RECORDS_READ             OUT NUMBER
   ,PA_RECORDS_SUCCESS          OUT NUMBER
   ,PA_RECORDS_ERROR            OUT NUMBER
   ,PTAB_ERROR_RECORDS          OUT SC_CREDIT.TYP_TAB_BTC_ERROR
)
IS

   /* **************************************************************
   * PROJECT: LOAN LIFE CYCLE
   * DESCRIPTION: V3 - PROCESS TO APPLY NO PAYMENT
   * CREATED DATE: 12/11/2024
   * CREATOR: CRISTHIAN MORALES
   * MODIFICATION DATE: 29/11/2024
   * PERFORMANCE MODIFICATIONS - LUIS RAMIREZ
   ************************************************************** */

   --CONSTANTS
   CSL_0                              CONSTANT SIMPLE_INTEGER := 0;
   CSL_1                              CONSTANT SIMPLE_INTEGER := 1;
   CSL_SP                             CONSTANT SIMPLE_INTEGER := 1;

   --CONSTANTS SUCCESS
   CSL_SUCCESS_CODE                   CONSTANT SIMPLE_INTEGER := 0;
   CSL_SUCCESS_MSG                    CONSTANT VARCHAR2(7) := 'SUCCESS';
   CSL_CODE_ERROR                     CONSTANT SIMPLE_INTEGER := -20012;
   CSL_SPACE                          CONSTANT VARCHAR2(2) := ' ';
   CSL_SUCCESS_ERROR                  CONSTANT VARCHAR2(28) := 'SUCCESS, WITH ERRORS RECORDS';
   CSL_TYPE_NULL                      CONSTANT VARCHAR2(15) := 'TYPE FEES NULL';
   CSL_ARROW                          CONSTANT VARCHAR2(5) := ' -> ';
   CSL_COMMA                          CONSTANT VARCHAR2(5) := ' , ';
   CSL_INS_STATUS_DETAIL              CONSTANT VARCHAR2(30) := 'SP_BTC_INS_LOAN_STATUS_DETAIL';
   CSL_EXE_OPERATION                  CONSTANT VARCHAR2(30) := 'SP_BTC_EXE_OPERATION_BALANCE';
   CSL_INS_LOAN_STATUS                CONSTANT VARCHAR2(25) := 'SP_BTC_INS_LOAN_STATUS';
   VL_I                               NUMBER(10,0) := 0;
   VL_STATUS_CODE                     NUMBER(10,0) := 0;
   VL_STATUS_MSG                      VARCHAR2(1000);

   --VARIABLES INTERNAL TYPES ASSIGNMENT
   VLTAB_ERRORS                       SC_CREDIT.TYP_TAB_BTC_ERROR;
   VLTAB_LOANS                        SC_CREDIT.TYP_TAB_BTC_LOAN;
   VLTAB_OPERATIONS                   SC_CREDIT.TYP_TAB_BTC_OPERATION;
   VLTAB_OPERATIONS_DETAIL            SC_CREDIT.TYP_TAB_BTC_DETAIL;
   VLTAB_BALANCES                     SC_CREDIT.TYP_TAB_BTC_BALANCE;
   VLTAB_BALANCES_DETAIL              SC_CREDIT.TYP_TAB_BTC_DETAIL;

   --VARIABLES OF ITERATION BY LOAN
   VLREC_LOAN                        SC_CREDIT.TYP_REC_BTC_LOAN;
   VLTAB_OPERATIONS_BY_LOAN          SC_CREDIT.TYP_TAB_BTC_OPERATION;
   VLTAB_OPERATIONS_DET_BY_LOAN      SC_CREDIT.TYP_TAB_BTC_DETAIL;
   VLTAB_BALANCES_BY_LOAN            SC_CREDIT.TYP_TAB_BTC_BALANCE;
   VLTAB_BALANCES_DET_BY_LOAN        SC_CREDIT.TYP_TAB_BTC_DETAIL;

   VL_DESC_COUNT                     VARCHAR2(500);

BEGIN
   PA_STATUS_CODE := CSL_SUCCESS_CODE;
   PA_STATUS_MSG := CSL_SUCCESS_MSG;
   PA_RECORDS_SUCCESS := CSL_0;
   PA_RECORDS_ERROR := CSL_0;
   PA_RECORDS_READ := CSL_0;
   VLTAB_ERRORS := SC_CREDIT.TYP_TAB_BTC_ERROR();

   IF PTAB_STATUS_DETAIL IS NULL OR PTAB_STATUS IS NULL OR PTAB_LOANS IS NULL
         OR PTAB_OPERATIONS IS NULL OR PTAB_OPERATIONS_DETAIL IS NULL
         OR PTAB_BALANCES IS NULL OR PTAB_BALANCES_DETAIL IS NULL THEN
      RAISE_APPLICATION_ERROR(CSL_CODE_ERROR, CSL_TYPE_NULL);
   END IF;

   VL_I := PTAB_STATUS_DETAIL.FIRST;
   PA_RECORDS_READ := PTAB_STATUS_DETAIL.COUNT;

   --INTERNAL TYPES ASSIGNMENT
   VLTAB_LOANS             := PTAB_LOANS;
   VLTAB_OPERATIONS        := PTAB_OPERATIONS;
   VLTAB_OPERATIONS_DETAIL := PTAB_OPERATIONS_DETAIL;
   VLTAB_BALANCES          := PTAB_BALANCES;
   VLTAB_BALANCES_DETAIL   := PTAB_BALANCES_DETAIL;

   WHILE (VL_I IS NOT NULL) LOOP
      BEGIN
         VLTAB_OPERATIONS_BY_LOAN := SC_CREDIT.TYP_TAB_BTC_OPERATION();
         VLTAB_OPERATIONS_DET_BY_LOAN := SC_CREDIT.TYP_TAB_BTC_DETAIL();
         VLTAB_BALANCES_BY_LOAN := SC_CREDIT.TYP_TAB_BTC_BALANCE();
         VLTAB_BALANCES_DET_BY_LOAN := SC_CREDIT.TYP_TAB_BTC_DETAIL();

         --TAB BY LOAN ASSIGNMENT
         <<loopLoanAssignment>>
         WHILE VLTAB_LOANS.COUNT > CSL_0 AND PTAB_STATUS_DETAIL.EXISTS(VL_I) LOOP
            IF VLTAB_LOANS(VLTAB_LOANS.FIRST).FI_LOAN_ID = PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID
               AND VLTAB_LOANS(VLTAB_LOANS.FIRST).FI_ADMIN_CENTER_ID = PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID THEN

               VLREC_LOAN := VLTAB_LOANS(VLTAB_LOANS.FIRST);
               VLTAB_LOANS.DELETE(VLTAB_LOANS.FIRST);
            ELSE
               EXIT loopLoanAssignment;
            END IF;
         END LOOP loopLoanAssignment;

         --TAB BY OPERATION ASSIGNMENT
         <<loopOperationAssignment>>
         WHILE VLTAB_OPERATIONS.COUNT > CSL_0 AND PTAB_STATUS_DETAIL.EXISTS(VL_I) LOOP
            IF VLTAB_OPERATIONS(VLTAB_OPERATIONS.FIRST).FI_LOAN_ID = PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID
               AND VLTAB_OPERATIONS(VLTAB_OPERATIONS.FIRST).FI_ADMIN_CENTER_ID = PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID THEN

               VLTAB_OPERATIONS_BY_LOAN.EXTEND;
               VLTAB_OPERATIONS_BY_LOAN(VLTAB_OPERATIONS_BY_LOAN.LAST) := VLTAB_OPERATIONS(VLTAB_OPERATIONS.FIRST);
               VLTAB_OPERATIONS.DELETE(VLTAB_OPERATIONS.FIRST);
            ELSE
               EXIT loopOperationAssignment;
            END IF;
         END LOOP loopOperationAssignment;

         --TAB BY OPERATION DET ASSIGNMENT
         <<loopOperationDetAssignment>>
         WHILE VLTAB_OPERATIONS_DETAIL.COUNT > CSL_0 AND PTAB_STATUS_DETAIL.EXISTS(VL_I) LOOP
            IF VLTAB_OPERATIONS_DETAIL(VLTAB_OPERATIONS_DETAIL.FIRST).FI_LOAN_ID = PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID
               AND VLTAB_OPERATIONS_DETAIL(VLTAB_OPERATIONS_DETAIL.FIRST).FI_ADMIN_CENTER_ID = PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID THEN

               VLTAB_OPERATIONS_DET_BY_LOAN.EXTEND;
               VLTAB_OPERATIONS_DET_BY_LOAN(VLTAB_OPERATIONS_DET_BY_LOAN.LAST) := VLTAB_OPERATIONS_DETAIL(VLTAB_OPERATIONS_DETAIL.FIRST);
               VLTAB_OPERATIONS_DETAIL.DELETE(VLTAB_OPERATIONS_DETAIL.FIRST);
            ELSE
               EXIT loopOperationDetAssignment;
            END IF;
         END LOOP loopOperationDetAssignment;

         --TAB BY BALANCES ASSIGNMENT
         <<loopBalanceAssignment>>
         WHILE VLTAB_BALANCES.COUNT > CSL_0 AND PTAB_STATUS_DETAIL.EXISTS(VL_I) LOOP
            IF VLTAB_BALANCES(VLTAB_BALANCES.FIRST).FI_LOAN_ID = PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID
               AND VLTAB_BALANCES(VLTAB_BALANCES.FIRST).FI_ADMIN_CENTER_ID = PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID THEN

               VLTAB_BALANCES_BY_LOAN.EXTEND;
               VLTAB_BALANCES_BY_LOAN(VLTAB_BALANCES_BY_LOAN.LAST) := VLTAB_BALANCES(VLTAB_BALANCES.FIRST);
               VLTAB_BALANCES.DELETE(VLTAB_BALANCES.FIRST);
            ELSE
               EXIT loopBalanceAssignment;
            END IF;
         END LOOP loopBalanceAssignment;

         --TAB BY BALANCES DET ASSIGNMENT
         <<loopBalanceDetAssignment>>
         WHILE VLTAB_BALANCES_DETAIL.COUNT > CSL_0 AND PTAB_STATUS_DETAIL.EXISTS(VL_I) LOOP
            IF VLTAB_BALANCES_DETAIL(VLTAB_BALANCES_DETAIL.FIRST).FI_LOAN_ID = PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID
               AND VLTAB_BALANCES_DETAIL(VLTAB_BALANCES_DETAIL.FIRST).FI_ADMIN_CENTER_ID = PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID THEN

               VLTAB_BALANCES_DET_BY_LOAN.EXTEND;
               VLTAB_BALANCES_DET_BY_LOAN(VLTAB_BALANCES_DET_BY_LOAN.LAST) := VLTAB_BALANCES_DETAIL(VLTAB_BALANCES_DETAIL.FIRST);
               VLTAB_BALANCES_DETAIL.DELETE(VLTAB_BALANCES_DETAIL.FIRST);
            ELSE
               EXIT loopBalanceDetAssignment;
            END IF;
         END LOOP loopBalanceDetAssignment;

      --Se cambio del inicio a aqui
          SC_CREDIT.SP_BTC_INS_LOAN_STATUS_DETAIL
            (PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID
            ,PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID
            ,PTAB_STATUS_DETAIL(VL_I).FI_LOAN_STATUS_ID
            ,PTAB_STATUS_DETAIL(VL_I).FI_ACTION_DETAIL_ID
            ,PTAB_STATUS_DETAIL(VL_I).FI_COUNTER_DAY
            ,PTAB_STATUS_DETAIL(VL_I).FD_INITIAL_DATE
            ,PTAB_STATUS_DETAIL(VL_I).FI_PAYMENT_NUMBER_ID
            ,PTAB_STATUS_DETAIL(VL_I).FD_FINAL_DATE
            ,PTAB_STATUS_DETAIL(VL_I).FI_ON_OFF
            ,CSL_0
            ,VL_STATUS_CODE
            ,VL_STATUS_MSG);

         IF(VL_STATUS_CODE != CSL_0)THEN
            RAISE_APPLICATION_ERROR( CSL_CODE_ERROR, CSL_INS_STATUS_DETAIL || CSL_SPACE || VL_STATUS_MSG);
         END IF;


         --EXECUTE PROCESS TO AFFECT LOAN, OPERATIONS AND BALANCES
         SC_CREDIT.SP_BTC_EXE_OPERATION_BALANCE(
            VLREC_LOAN
            ,VLTAB_OPERATIONS_BY_LOAN
            ,VLTAB_OPERATIONS_DET_BY_LOAN
            ,VLTAB_BALANCES_BY_LOAN
            ,VLTAB_BALANCES_DET_BY_LOAN
            ,PA_DEVICE
            ,PA_GPS_LATITUDE
            ,PA_GPS_LONGITUDE
            ,CSL_0
            ,VL_STATUS_CODE
            ,VL_STATUS_MSG);

         IF(VL_STATUS_CODE != CSL_0)THEN
            RAISE_APPLICATION_ERROR( CSL_CODE_ERROR, CSL_EXE_OPERATION || CSL_SPACE || VL_STATUS_MSG);
         END IF;

         SC_CREDIT.SP_BTC_INS_LOAN_STATUS(
             PTAB_STATUS(VL_I).FI_LOAN_ID
            ,PTAB_STATUS(VL_I).FI_ADMIN_CENTER_ID
            ,PTAB_STATUS(VL_I).FI_LOAN_OPERATION_ID
            ,PTAB_STATUS(VL_I).FI_LOAN_STATUS_ID
            ,PTAB_STATUS(VL_I).FI_LOAN_STATUS_OLD_ID
            ,PTAB_STATUS(VL_I).FI_TRIGGER_ID
            ,PTAB_STATUS(VL_I).FD_LOAN_STATUS_DATE
            ,CSL_0
            ,CSL_0
            ,VL_STATUS_CODE
            ,VL_STATUS_MSG
            );

         IF(VL_STATUS_CODE != CSL_0)THEN
         RAISE_APPLICATION_ERROR( CSL_CODE_ERROR, CSL_INS_LOAN_STATUS || CSL_SPACE || VL_STATUS_MSG);
         END IF;

         --DELETE TYPES BY LOAN (CYCLE)
         VLTAB_OPERATIONS_BY_LOAN.DELETE;
         VLTAB_OPERATIONS_DET_BY_LOAN.DELETE;
         VLTAB_BALANCES_BY_LOAN.DELETE;
         VLTAB_BALANCES_DET_BY_LOAN.DELETE;

         PA_RECORDS_SUCCESS := PA_RECORDS_SUCCESS + CSL_1;

      EXCEPTION
         WHEN OTHERS THEN
            ROLLBACK;
            SC_CREDIT.SP_BATCH_ERROR_LOG(
               UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP)
               ,SQLCODE
               ,SQLERRM
               ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
               ,CSL_0
               ,PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID
                  ||CSL_COMMA
                  ||PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID);
            PA_RECORDS_ERROR :=PA_RECORDS_ERROR + CSL_1;

            VLTAB_ERRORS.EXTEND;
            VLTAB_ERRORS(VLTAB_ERRORS.LAST) :=
               SC_CREDIT.TYP_REC_BTC_ERROR(
                  PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID
                  ,PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID
                  , UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP)
                  ,SQLCODE
                  ,SQLERRM
                  ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                  ,SYSDATE
                  ,CSL_0
                  ,NULL);
      END;
      VL_I := PTAB_STATUS_DETAIL.NEXT(VL_I);
      COMMIT;
   END LOOP;

   COMMIT;

   VL_DESC_COUNT := ' TAB_LOANS '||VLTAB_LOANS.COUNT
                  ||' TAB_OPERATIONS '||VLTAB_OPERATIONS.COUNT
                  ||' TAB_OPERATIONS_DETAIL '||VLTAB_OPERATIONS_DETAIL.COUNT
                  ||' TAB_BALANCES '||VLTAB_BALANCES.COUNT
                  ||' TAB_BALANCES_DETAIL '||VLTAB_BALANCES_DETAIL.COUNT;
   --DELETE TYPES INTERNS
   VLTAB_LOANS.DELETE;
   VLTAB_OPERATIONS.DELETE;
   VLTAB_OPERATIONS_DETAIL.DELETE;
   VLTAB_BALANCES.DELETE;
   VLTAB_BALANCES_DETAIL.DELETE;

   PTAB_ERROR_RECORDS := VLTAB_ERRORS;
   IF(PA_RECORDS_ERROR > CSL_0)THEN
      PA_STATUS_CODE := CSL_1;
      PA_STATUS_MSG := CSL_SUCCESS_ERROR;
   END IF;
   PA_STATUS_MSG := PA_STATUS_MSG
      || ' ' || VL_DESC_COUNT;

EXCEPTION
   WHEN OTHERS THEN
      PA_STATUS_CODE := SQLCODE;
      PA_STATUS_MSG := SQLERRM || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;

      SC_CREDIT.SP_BATCH_ERROR_LOG(
         UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP)
         ,SQLCODE
         ,SQLERRM
         ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
         ,CSL_0
         ,NULL);
END SP_BTC_EXE_NO_PAYMENT;

/

  GRANT EXECUTE ON SC_CREDIT.SP_BTC_EXE_NO_PAYMENT TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_BTC_EXE_NO_PAYMENT TO USRBTCCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_BTC_EXE_APPLY_FEES
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_BTC_EXE_APPLY_FEES (
   PTAB_STATUS_DETAIL           IN SC_CREDIT.TYP_TAB_BTC_STATUS_DETAIL
   ,PTAB_LOANS                  IN SC_CREDIT.TYP_TAB_BTC_LOAN
   ,PTAB_OPERATIONS             IN SC_CREDIT.TYP_TAB_BTC_OPERATION
   ,PTAB_OPERATIONS_DETAIL      IN SC_CREDIT.TYP_TAB_BTC_DETAIL
   ,PTAB_BALANCES               IN SC_CREDIT.TYP_TAB_BTC_BALANCE
   ,PTAB_BALANCES_DETAIL        IN SC_CREDIT.TYP_TAB_BTC_DETAIL
   ,PA_DEVICE                   IN SC_CREDIT.TA_LOAN_OPERATION.FC_DEVICE%TYPE
   ,PA_GPS_LATITUDE             IN SC_CREDIT.TA_LOAN_OPERATION.FC_GPS_LATITUDE%TYPE
   ,PA_GPS_LONGITUDE            IN SC_CREDIT.TA_LOAN_OPERATION.FC_GPS_LONGITUDE%TYPE
   ,PA_STATUS_CODE              OUT NUMBER
   ,PA_STATUS_MSG               OUT VARCHAR2
   ,PA_RECORDS_READ             OUT NUMBER
   ,PA_RECORDS_SUCCESS          OUT NUMBER
   ,PA_RECORDS_ERROR            OUT NUMBER
   ,PTAB_ERROR_RECORDS          OUT SC_CREDIT.TYP_TAB_BTC_ERROR
)
IS

   /* **************************************************************
   * PROYECT: LOAN LIFE CYCLE
   * DESCRIPTION: PROCESS TO INSERT IN TABLE LOAN_STATUS_DETAIL,LOAN,STATUS,LOAN_OPERATION
   * LOAN_BALANCE
   * CREATED DATE: 21/11/2024
   * CREATOR: CRISTHIAN MORALES AND ITZEL TRINIDAD RAMOS
   * MODIFICATION DATE: 29/11/2024
   * PERFORMANCE MODIFICATIONS - LUIS RAMIREZ
   ************************************************************** */

   --CONSTANTS
   CSL_0                              CONSTANT SIMPLE_INTEGER := 0;
   CSL_1                              CONSTANT SIMPLE_INTEGER := 1;
   CSL_SP                            CONSTANT SIMPLE_INTEGER := 1;

   --CONSTANTS SUCCESS
   CSL_SUCCESS_CODE                   CONSTANT SIMPLE_INTEGER := 0;
   CSL_SUCCESS_MSG                    CONSTANT VARCHAR2(7) := 'SUCCESS';
   CSL_CODE_ERROR                     CONSTANT SIMPLE_INTEGER := -20012;
   CSL_SPACE                          CONSTANT VARCHAR2(2) := ' ';
   CSL_SUCCESS_ERROR                  CONSTANT VARCHAR2(28) := 'SUCCESS, WITH ERRORS RECORDS';
   CSL_TYPE_NULL                      CONSTANT VARCHAR2(14) := 'TYPE FEES NULL';
   CSL_ARROW                          CONSTANT VARCHAR2(5) := ' -> ';
   CSL_COMMA                          CONSTANT VARCHAR2(5) := ' , ';
   CSL_INS_STATUS_DETAIL              CONSTANT VARCHAR2(30) := 'SP_BTC_INS_LOAN_STATUS_DETAIL';
   CSL_UPD_STATUS_DETAIL              CONSTANT VARCHAR2(30) := 'SP_BTC_UPD_LOAN_STATUS_DETAIL';
   CSL_GEN_OPERATION                  CONSTANT VARCHAR2(30) := 'SP_BTC_GEN_OPERATION_BALANCE';

   VL_I                               NUMBER(10,0) := 0;
   VL_STATUS_CODE                     NUMBER(10,0) := 0;
   VL_STATUS_MSG                      VARCHAR2(1000);
   --VARIABLES INTERNAL TYPES ASSIGNMENT
   VLTAB_ERRORS                       SC_CREDIT.TYP_TAB_BTC_ERROR;
   VLTAB_LOANS                        SC_CREDIT.TYP_TAB_BTC_LOAN;
   VLTAB_OPERATIONS                   SC_CREDIT.TYP_TAB_BTC_OPERATION;
   VLTAB_OPERATIONS_DETAIL            SC_CREDIT.TYP_TAB_BTC_DETAIL;
   VLTAB_BALANCES                     SC_CREDIT.TYP_TAB_BTC_BALANCE;
   VLTAB_BALANCES_DETAIL              SC_CREDIT.TYP_TAB_BTC_DETAIL;

   --VARIABLES OF ITERATION BY LOAN
   VLREC_LOAN                        SC_CREDIT.TYP_REC_BTC_LOAN;
   VLTAB_OPERATIONS_BY_LOAN          SC_CREDIT.TYP_TAB_BTC_OPERATION;
   VLTAB_OPERATIONS_DET_BY_LOAN      SC_CREDIT.TYP_TAB_BTC_DETAIL;
   VLTAB_BALANCES_BY_LOAN            SC_CREDIT.TYP_TAB_BTC_BALANCE;
   VLTAB_BALANCES_DET_BY_LOAN        SC_CREDIT.TYP_TAB_BTC_DETAIL;

   t1 timestamp;
   t2 timestamp;
   VL_DESC_COUNT                     VARCHAR2(500);


BEGIN
   PA_STATUS_CODE := CSL_SUCCESS_CODE;
   PA_STATUS_MSG := CSL_SUCCESS_MSG;
   PA_RECORDS_SUCCESS := CSL_0;
   PA_RECORDS_ERROR := CSL_0;
   PA_RECORDS_READ := CSL_0;
   VLTAB_ERRORS := SC_CREDIT.TYP_TAB_BTC_ERROR();
   t1 := systimestamp;

   IF PTAB_STATUS_DETAIL IS NULL OR PTAB_LOANS IS NULL
         OR PTAB_OPERATIONS IS NULL OR PTAB_OPERATIONS_DETAIL IS NULL
         OR PTAB_BALANCES IS NULL OR PTAB_BALANCES_DETAIL IS NULL THEN
      RAISE_APPLICATION_ERROR(CSL_CODE_ERROR, CSL_TYPE_NULL);
   END IF;

   VL_I := PTAB_STATUS_DETAIL.FIRST;
   PA_RECORDS_READ := PTAB_STATUS_DETAIL.COUNT;

   --INTERNAL TYPES ASSIGNMENT
   VLTAB_LOANS             := SC_CREDIT.TYP_TAB_BTC_LOAN();
   VLTAB_LOANS             := PTAB_LOANS;
   VLTAB_OPERATIONS        := SC_CREDIT.TYP_TAB_BTC_OPERATION();
   VLTAB_OPERATIONS        := PTAB_OPERATIONS;
   VLTAB_OPERATIONS_DETAIL := SC_CREDIT.TYP_TAB_BTC_DETAIL();
   VLTAB_OPERATIONS_DETAIL := PTAB_OPERATIONS_DETAIL;
   VLTAB_BALANCES          := SC_CREDIT.TYP_TAB_BTC_BALANCE();
   VLTAB_BALANCES          := PTAB_BALANCES;
   VLTAB_BALANCES_DETAIL   := SC_CREDIT.TYP_TAB_BTC_DETAIL();
   VLTAB_BALANCES_DETAIL   := PTAB_BALANCES_DETAIL;

   WHILE (VL_I IS NOT NULL) LOOP
      BEGIN
         VLTAB_OPERATIONS_BY_LOAN := SC_CREDIT.TYP_TAB_BTC_OPERATION();
         VLTAB_OPERATIONS_DET_BY_LOAN := SC_CREDIT.TYP_TAB_BTC_DETAIL();
         VLTAB_BALANCES_BY_LOAN := SC_CREDIT.TYP_TAB_BTC_BALANCE();
         VLTAB_BALANCES_DET_BY_LOAN := SC_CREDIT.TYP_TAB_BTC_DETAIL();

         SC_CREDIT.SP_BTC_UPD_LOAN_STATUS_DETAIL
            (PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID
            ,PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID
            ,PTAB_STATUS_DETAIL(VL_I).FI_PAYMENT_NUMBER_ID
            ,NULL
            ,CSL_0
            ,NULL
            ,CSL_1
            ,CSL_0
            ,VL_STATUS_CODE
            ,VL_STATUS_MSG);

         IF(VL_STATUS_CODE != CSL_0)THEN
            RAISE_APPLICATION_ERROR( CSL_CODE_ERROR, CSL_UPD_STATUS_DETAIL || CSL_SPACE || VL_STATUS_MSG);
         END IF;

         SC_CREDIT.SP_BTC_INS_LOAN_STATUS_DETAIL
            (PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID
            ,PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID
            ,PTAB_STATUS_DETAIL(VL_I).FI_LOAN_STATUS_ID
            ,PTAB_STATUS_DETAIL(VL_I).FI_ACTION_DETAIL_ID
            ,PTAB_STATUS_DETAIL(VL_I).FI_COUNTER_DAY
            ,PTAB_STATUS_DETAIL(VL_I).FD_INITIAL_DATE
            ,PTAB_STATUS_DETAIL(VL_I).FI_PAYMENT_NUMBER_ID
            ,PTAB_STATUS_DETAIL(VL_I).FD_FINAL_DATE
            ,PTAB_STATUS_DETAIL(VL_I).FI_ON_OFF
            ,CSL_0
            ,VL_STATUS_CODE
            ,VL_STATUS_MSG);

         IF(VL_STATUS_CODE != CSL_0)THEN
            RAISE_APPLICATION_ERROR( CSL_CODE_ERROR, CSL_INS_STATUS_DETAIL || CSL_SPACE || VL_STATUS_MSG);
         END IF;

         --TAB BY LOAN ASSIGNMENT
         <<loopLoanAssignment>>
         WHILE VLTAB_LOANS.COUNT > CSL_0 AND PTAB_STATUS_DETAIL.EXISTS(VL_I) LOOP
            IF VLTAB_LOANS(VLTAB_LOANS.FIRST).FI_LOAN_ID = PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID THEN

               VLREC_LOAN := VLTAB_LOANS(VLTAB_LOANS.FIRST);
               VLTAB_LOANS.DELETE(VLTAB_LOANS.FIRST);
            ELSE
               EXIT loopLoanAssignment;
            END IF;
         END LOOP loopLoanAssignment;

         --TAB BY OPERATION ASSIGNMENT
         <<loopOperationAssignment>>
         WHILE VLTAB_OPERATIONS.COUNT > CSL_0 AND PTAB_STATUS_DETAIL.EXISTS(VL_I) LOOP
            IF VLTAB_OPERATIONS(VLTAB_OPERATIONS.FIRST).FI_LOAN_ID = PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID
               AND VLTAB_OPERATIONS(VLTAB_OPERATIONS.FIRST).FI_ADMIN_CENTER_ID = PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID THEN

               VLTAB_OPERATIONS_BY_LOAN.EXTEND;
               VLTAB_OPERATIONS_BY_LOAN(VLTAB_OPERATIONS_BY_LOAN.LAST) := VLTAB_OPERATIONS(VLTAB_OPERATIONS.FIRST);
               VLTAB_OPERATIONS.DELETE(VLTAB_OPERATIONS.FIRST);
            ELSE
               EXIT loopOperationAssignment;
            END IF;
         END LOOP loopOperationAssignment;

         --TAB BY OPERATION DET ASSIGNMENT
         <<loopOperationDetAssignment>>
         WHILE VLTAB_OPERATIONS_DETAIL.COUNT > CSL_0 AND PTAB_STATUS_DETAIL.EXISTS(VL_I) LOOP
            IF VLTAB_OPERATIONS_DETAIL(VLTAB_OPERATIONS_DETAIL.FIRST).FI_LOAN_ID = PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID
               AND VLTAB_OPERATIONS_DETAIL(VLTAB_OPERATIONS_DETAIL.FIRST).FI_ADMIN_CENTER_ID = PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID THEN

               VLTAB_OPERATIONS_DET_BY_LOAN.EXTEND;
               VLTAB_OPERATIONS_DET_BY_LOAN(VLTAB_OPERATIONS_DET_BY_LOAN.LAST) := VLTAB_OPERATIONS_DETAIL(VLTAB_OPERATIONS_DETAIL.FIRST);
               VLTAB_OPERATIONS_DETAIL.DELETE(VLTAB_OPERATIONS_DETAIL.FIRST);
            ELSE
               EXIT loopOperationDetAssignment;
            END IF;
         END LOOP loopOperationDetAssignment;

         --TAB BY BALANCES ASSIGNMENT
         <<loopBalanceAssignment>>
         WHILE VLTAB_BALANCES.COUNT > CSL_0 AND PTAB_STATUS_DETAIL.EXISTS(VL_I) LOOP
            IF VLTAB_BALANCES(VLTAB_BALANCES.FIRST).FI_LOAN_ID = PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID
               AND VLTAB_BALANCES(VLTAB_BALANCES.FIRST).FI_ADMIN_CENTER_ID = PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID THEN

               VLTAB_BALANCES_BY_LOAN.EXTEND;
               VLTAB_BALANCES_BY_LOAN(VLTAB_BALANCES_BY_LOAN.LAST) := VLTAB_BALANCES(VLTAB_BALANCES.FIRST);
               VLTAB_BALANCES.DELETE(VLTAB_BALANCES.FIRST);
            ELSE
               EXIT loopBalanceAssignment;
            END IF;
         END LOOP loopBalanceAssignment;

         --TAB BY BALANCES DET ASSIGNMENT
         <<loopBalanceDetAssignment>>
         WHILE VLTAB_BALANCES_DETAIL.COUNT > CSL_0 AND PTAB_STATUS_DETAIL.EXISTS(VL_I) LOOP
            IF VLTAB_BALANCES_DETAIL(VLTAB_BALANCES_DETAIL.FIRST).FI_LOAN_ID = PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID
               AND VLTAB_BALANCES_DETAIL(VLTAB_BALANCES_DETAIL.FIRST).FI_ADMIN_CENTER_ID = PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID THEN

               VLTAB_BALANCES_DET_BY_LOAN.EXTEND;
               VLTAB_BALANCES_DET_BY_LOAN(VLTAB_BALANCES_DET_BY_LOAN.LAST) := VLTAB_BALANCES_DETAIL(VLTAB_BALANCES_DETAIL.FIRST);
               VLTAB_BALANCES_DETAIL.DELETE(VLTAB_BALANCES_DETAIL.FIRST);
            ELSE
               EXIT loopBalanceDetAssignment;
            END IF;
         END LOOP loopBalanceDetAssignment;

         --EXECUTE PROCESS TO AFFECT LOAN, OPERATIONS AND BALANCES
         SC_CREDIT.SP_BTC_EXE_OPERATION_BALANCE(
            VLREC_LOAN
            ,VLTAB_OPERATIONS_BY_LOAN
            ,VLTAB_OPERATIONS_DET_BY_LOAN
            ,VLTAB_BALANCES_BY_LOAN
            ,VLTAB_BALANCES_DET_BY_LOAN
            ,PA_DEVICE
            ,PA_GPS_LATITUDE
            ,PA_GPS_LONGITUDE
            ,CSL_0
            ,VL_STATUS_CODE
            ,VL_STATUS_MSG);

         IF(VL_STATUS_CODE != CSL_0)THEN
            RAISE_APPLICATION_ERROR( CSL_CODE_ERROR, CSL_GEN_OPERATION || CSL_SPACE || VL_STATUS_MSG);
         END IF;

         --DELETE TYPES BY LOAN (CYCLE)
         VLTAB_OPERATIONS_BY_LOAN.DELETE;
         VLTAB_OPERATIONS_DET_BY_LOAN.DELETE;
         VLTAB_BALANCES_BY_LOAN.DELETE;
         VLTAB_BALANCES_DET_BY_LOAN.DELETE;

         PA_RECORDS_SUCCESS := PA_RECORDS_SUCCESS + CSL_1;

      EXCEPTION
         WHEN OTHERS THEN
            ROLLBACK;
            SC_CREDIT.SP_BATCH_ERROR_LOG(
               UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP)
               ,SQLCODE
               ,SQLERRM
               ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
               ,CSL_0
               ,PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID
                  ||CSL_COMMA
                  ||PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID);
            PA_RECORDS_ERROR :=PA_RECORDS_ERROR + CSL_1;

            VLTAB_ERRORS.EXTEND;
            VLTAB_ERRORS(VLTAB_ERRORS.LAST) :=
               SC_CREDIT.TYP_REC_BTC_ERROR(PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID
                                          ,PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID
                                          , UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP)
                                          ,SQLCODE
                                          ,SQLERRM
                                          ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                          ,SYSDATE
                                          ,CSL_0
                                          ,NULL);
      END;
      VL_I := PTAB_STATUS_DETAIL.NEXT(VL_I);
      COMMIT;
   END LOOP;

   COMMIT;

   VL_DESC_COUNT := ' TAB_LOANS '||VLTAB_LOANS.COUNT
                  ||' TAB_OPERATIONS '||VLTAB_OPERATIONS.COUNT
                  ||' TAB_OPERATIONS_DETAIL '||VLTAB_OPERATIONS_DETAIL.COUNT
                  ||' TAB_BALANCES '||VLTAB_BALANCES.COUNT
                  ||' TAB_BALANCES_DETAIL '||VLTAB_BALANCES_DETAIL.COUNT;
   --DELETE TYPES INTERNS
   VLTAB_LOANS.DELETE;
   VLTAB_OPERATIONS.DELETE;
   VLTAB_OPERATIONS_DETAIL.DELETE;
   VLTAB_BALANCES.DELETE;
   VLTAB_BALANCES_DETAIL.DELETE;

   PTAB_ERROR_RECORDS := VLTAB_ERRORS;
   IF(PA_RECORDS_ERROR > CSL_0)THEN
      PA_STATUS_CODE := CSL_1;
      PA_STATUS_MSG := CSL_SUCCESS_ERROR;
   END IF;
   t2 := systimestamp;
   PA_STATUS_MSG := PA_STATUS_MSG
      || ' ' || 'Elapsed Seconds: '||TO_CHAR(t2-t1, 'SSSS.FF')
      || ' ' || VL_DESC_COUNT;

EXCEPTION
   WHEN OTHERS THEN
      PA_STATUS_CODE := SQLCODE;
      PA_STATUS_MSG := SQLERRM || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;

      SC_CREDIT.SP_BATCH_ERROR_LOG(UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP)
                                  ,SQLCODE
                                  ,SQLERRM
                                  ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                  ,CSL_0
                                  ,NULL);
END SP_BTC_EXE_APPLY_FEES;

/

  GRANT EXECUTE ON SC_CREDIT.SP_BTC_EXE_APPLY_FEES TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_BTC_EXE_APPLY_FEES TO USRBTCCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_BTC_SEL_LATE_FEE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_BTC_SEL_LATE_FEE 
    (PA_FIRST_CENTER_ID        IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE
    ,PA_END_CENTER_ID          IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE
    ,PA_OPERATION_DATE         IN VARCHAR2
    ,PA_STATUS_CODE            OUT NUMBER
    ,PA_STATUS_MSG             OUT VARCHAR2
    ,PA_CUR_FEES               OUT SC_CREDIT.PA_TYPES.TYP_CURSOR)
IS
    /* **************************************************************
    * PROJECT: LOAN LIFE CYCLE
    * DESCRIPTION: SELECT LOAN TO APPLY LATE FEE
    * CREATED DATE: 12/12/2024
    * CREATOR: LUIS RAMIREZ
    ************************************************************** */
    CSL_DATE         CONSTANT VARCHAR2(22)   := 'MM/DD/YYYY HH24:MI:SS';
    CSL_0            CONSTANT SIMPLE_INTEGER := 0;
    CSL_1            CONSTANT SIMPLE_INTEGER := 1;
    CSL_3            CONSTANT SIMPLE_INTEGER := 3;
    CSL_MSG_SUCCESS  CONSTANT VARCHAR2(7)   := 'SUCCESS';
    CSL_SP           CONSTANT SIMPLE_INTEGER := 2;
    CSL_ARROW        CONSTANT VARCHAR2(2)    := '->';
    CSL_COMMA        CONSTANT VARCHAR2(3) := ' , ';
BEGIN
    PA_STATUS_CODE := CSL_0;
    PA_STATUS_MSG  := CSL_MSG_SUCCESS;
    PA_CUR_FEES :=  NULL;

    OPEN PA_CUR_FEES FOR
      WITH TAB_PED AS (
         SELECT TL.FI_LOAN_ID                         AS FI_LOAN_ID
               ,TL.FI_ADMIN_CENTER_ID                 AS FI_ADMIN_CENTER_ID
               ,TL.FC_CUSTOMER_ID                     AS FC_CUSTOMER_ID
               ,TL.FI_COUNTRY_ID                      AS FI_COUNTRY_ID
               ,TL.FI_COMPANY_ID                      AS FI_COMPANY_ID
               ,TL.FI_BUSINESS_UNIT_ID                AS FI_BUSINESS_UNIT_ID
               ,TL.FN_PRINCIPAL_BALANCE               AS FN_PRINCIPAL_BALANCE
               ,TL.FN_ADDITIONAL_CHARGE_BALANCE       AS FN_ADDITIONAL_CHARGE_BALANCE
               ,TL.FN_FINANCE_CHARGE_BALANCE          AS FN_FINANCE_CHARGE_BALANCE
               ,TL.FI_PRODUCT_ID                      AS FI_PRODUCT_ID
               ,TL.FI_RULE_ID                         AS FI_RULE_ID
               ,TL.FI_CURRENT_BALANCE_SEQ             AS FI_CURRENT_BALANCE_SEQ
               ,TL.FI_LOAN_STATUS_ID                  AS FI_LOAN_STATUS_ID
               ,TLSD.FI_REGISTRATION_NUMBER           AS FI_REGISTRATION_NUMBER
               ,TLSD.FI_COUNTER_DAY                   AS FI_COUNTER_DAY
               ,TLSD.FI_PAYMENT_NUMBER_ID             AS FI_PAYMENT_NUMBER_ID
               ,TLSD.FI_ACTION_DETAIL_ID              AS FI_ACTION_DETAIL_ID
               ,SC_CREDIT.FN_SEL_LOAN_BALANCE_DET_JSON
                  (TL.FI_LOAN_ID
                  , TL.FI_ADMIN_CENTER_ID
                  , TL.FI_CURRENT_BALANCE_SEQ
                  , NULL)            AS BALANCE_DET_JSON
               ,(SELECT PS.FN_PAYMENT_BALANCE
                FROM SC_CREDIT.TA_PAYMENT_SCHEDULE PS
                WHERE PS.FI_PAYMENT_SCHEDULE_ID > CSL_0
                  AND PS.FI_LOAN_ID = TL.FI_LOAN_ID
                  AND PS.FI_ADMIN_CENTER_ID = TL.FI_ADMIN_CENTER_ID
                  AND PS.FI_PAYMENT_NUMBER_ID = TLSD.FI_PAYMENT_NUMBER_ID
                  AND PS.FI_STATUS = CSL_1)              AS FN_PAYMENT_BALANCE
               ,(SELECT COUNT(0) AS FLAG_LATE_FEE
                FROM SC_CREDIT.TA_LOAN_STATUS_DETAIL SDFEE
                WHERE SDFEE.FI_LOAN_ID = TLSD.FI_LOAN_ID
                  AND SDFEE.FI_ADMIN_CENTER_ID = TLSD.FI_ADMIN_CENTER_ID
                  AND SDFEE.FI_PAYMENT_NUMBER_ID = TLSD.FI_PAYMENT_NUMBER_ID
                  AND SDFEE.FI_ON_OFF = CSL_0
                  AND SDFEE.FI_ACTION_DETAIL_ID = CSL_1) AS FLAG_LATE_FEE
               ,TO_CHAR(TLSD.FD_INITIAL_DATE,CSL_DATE) AS FD_INITIAL_DATE
         FROM SC_CREDIT.TA_LOAN TL
            INNER JOIN SC_CREDIT.TA_LOAN_STATUS_DETAIL TLSD
                    ON TLSD.FI_LOAN_ID = TL.FI_LOAN_ID
                    AND TLSD.FI_ADMIN_CENTER_ID = TL.FI_ADMIN_CENTER_ID
                    AND TLSD.FI_PAYMENT_NUMBER_ID > CSL_0
                    AND TLSD.FI_ON_OFF = CSL_1
                    AND TLSD.FI_ACTION_DETAIL_ID = CSL_3
         WHERE TL.FI_LOAN_STATUS_ID = CSL_3
           AND TL.FI_ADMIN_CENTER_ID BETWEEN PA_FIRST_CENTER_ID AND PA_END_CENTER_ID)
      SELECT FI_LOAN_ID
            ,FI_ADMIN_CENTER_ID
            ,FC_CUSTOMER_ID
            ,FI_COUNTRY_ID
            ,FI_COMPANY_ID
            ,FI_BUSINESS_UNIT_ID
            ,FN_PRINCIPAL_BALANCE
            ,FN_ADDITIONAL_CHARGE_BALANCE
            ,FN_FINANCE_CHARGE_BALANCE
            ,FI_PRODUCT_ID
            ,FI_RULE_ID
            ,FI_CURRENT_BALANCE_SEQ
            ,FI_LOAN_STATUS_ID
            ,FI_REGISTRATION_NUMBER
            ,FI_COUNTER_DAY
            ,FI_PAYMENT_NUMBER_ID
            ,FI_ACTION_DETAIL_ID
            ,BALANCE_DET_JSON
            ,FN_PAYMENT_BALANCE
            ,FD_INITIAL_DATE
      FROM TAB_PED
      WHERE FLAG_LATE_FEE = CSL_0;
EXCEPTION
WHEN OTHERS THEN
    PA_STATUS_CODE := SQLCODE;
    PA_STATUS_MSG := SQLERRM || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;

    SC_CREDIT.SP_BATCH_ERROR_LOG (
       UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP)
       ,SQLCODE
       ,SQLERRM
       ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
       ,CSL_0
       ,PA_FIRST_CENTER_ID || CSL_COMMA || PA_END_CENTER_ID || CSL_COMMA || PA_OPERATION_DATE
       );
END SP_BTC_SEL_LATE_FEE;

/

  GRANT EXECUTE ON SC_CREDIT.SP_BTC_SEL_LATE_FEE TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_BTC_SEL_LATE_FEE TO USRBTCCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_BTC_SEL_DAILY_INTEREST
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_BTC_SEL_DAILY_INTEREST 
  (
    PA_FIRST_CENTER_ID     IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE
   ,PA_END_CENTER_ID       IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE
   ,PA_TODAY               IN VARCHAR2
   ,PA_STATUS_CODE         OUT NUMBER
   ,PA_STATUS_MSG          OUT VARCHAR2
   ,PA_CUR_SELECT          OUT SC_CREDIT.PA_TYPES.TYP_CURSOR
   )
   IS
   /* **************************************************************
   * PROYECT: LOAN LIFE CYCLE
   * DESCRIPTION: INTEREST CALCULATION PROCEDURE
   * CREATED DATE: 30/10/2024
   * CREATOR: CRISTHIAN MORALES
   * MODIFIED DATE: 16/01/2025
   * [NCPADC-4480-1V2]
   ************************************************************** */

     --CONSTANT
   CSL_ARROW        CONSTANT VARCHAR2(2)   := '->';
   CSL_DATE         CONSTANT VARCHAR2(10)   := 'MM/DD/YYYY';
   CSL_0            CONSTANT SIMPLE_INTEGER := 0;
   CSL_1            CONSTANT SIMPLE_INTEGER := 1;
   CSL_2            CONSTANT SIMPLE_INTEGER := 2;
   CSL_3            CONSTANT SIMPLE_INTEGER := 3;
   CSL_MSG_SUCCESS  CONSTANT VARCHAR2(7)   := 'SUCCESS';
   CSL_PKG          CONSTANT SIMPLE_INTEGER := 1;

   BEGIN

    PA_STATUS_CODE:=CSL_0;
    PA_STATUS_MSG:=CSL_MSG_SUCCESS;
    PA_CUR_SELECT:=NULL;

           --CONSULT LOAN,INTEREST,SCHEDULE
      OPEN PA_CUR_SELECT FOR
        WITH TABPED AS
           (SELECT LO.FI_LOAN_ID                                AS FI_LOAN_ID
                  ,LO.FI_ADMIN_CENTER_ID                        AS FI_ADMIN_CENTER_ID
                  ,LO.FI_COUNTRY_ID								AS FI_COUNTRY_ID
                  ,LO.FI_COMPANY_ID								AS FI_COMPANY_ID
                  ,LO.FI_BUSINESS_UNIT_ID						AS FI_BUSINESS_UNIT_ID
                  ,LO.FC_CUSTOMER_ID							AS FC_CUSTOMER_ID
                  ,LO.FN_FINANCE_CHARGE_AMOUNT                  AS FN_FINANCE_CHARGE_AMOUNT
                  ,LO.FN_PAID_INTEREST_AMOUNT                   AS FN_PAID_INTEREST_AMOUNT
                  ,LO.FN_PRINCIPAL_BALANCE                      AS FN_PRINCIPAL_BALANCE
                  ,LO.FN_FINANCE_CHARGE_BALANCE                 AS FN_FINANCE_CHARGE_BALANCE
				  ,LO.FN_ADDITIONAL_CHARGE_BALANCE              AS FN_ADDITIONAL_CHARGE_BALANCE
                  ,TO_CHAR(LO.FD_ORIGINATION_DATE, CSL_DATE)    AS FD_ORIGINATION_DATE
                  ,TO_CHAR(LO.FD_FIRST_PAYMENT, CSL_DATE)       AS FD_FIRST_PAYMENT
                  ,LO.FI_CURRENT_BALANCE_SEQ                    AS FI_CURRENT_BALANCE_SEQ
                  ,LO.FN_INTEREST_RATE                          AS FN_INTEREST_RATE
                  ,LO.FI_NUMBER_OF_PAYMENTS                     AS FI_NUMBER_OF_PAYMENTS
                  ,LO.FI_TERM_TYPE                              AS FI_TERM_TYPE
                  ,LO.FI_LOAN_STATUS_ID							AS FI_LOAN_STATUS_ID
                  ,LO.FI_PRODUCT_ID                             AS FI_PRODUCT_ID
                  ,LO.FI_RULE_ID                                AS FI_RULE_ID
                  ,TO_CHAR(LO.FD_LOAN_EFFECTIVE_DATE, CSL_DATE) AS FD_LOAN_EFFECTIVE_DATE
                  ,LI.FI_PAYMENT_NUMBER_ID                      AS FI_PAYMENT_NUMBER_ID
                  ,LI.FI_DAYS_ACUM_BY_TERM                      AS FI_DAYS_ACUM_BY_TERM
                  ,LI.FN_DAILY_INTEREST                         AS FN_DAILY_INTEREST
                  ,LI.FN_ACCRUED_INTEREST_BALANCE               AS FN_ACCRUED_INTEREST_BALANCE
                  ,LI.FN_ACCRUED_INTEREST_LOAN					AS FN_ACCRUED_INTEREST_LOAN
                  ,LI.FN_PAYMENT_INTEREST						AS FN_PAYMENT_INTEREST
                  ,LI.FC_CONDITION_INTEREST                     AS FC_CONDITION_INTEREST
                  ,TO_CHAR(LI.FD_APPLICATION_DATE, CSL_DATE)    AS FD_APPLICATION_DATE
                  ,ROW_NUMBER() OVER (PARTITION BY LO.FI_LOAN_ID ORDER BY LI.FI_PAYMENT_NUMBER_ID DESC,LI.FI_DAYS_ACUM_BY_TERM DESC) AS ORDEN
              FROM SC_CREDIT.TA_LOAN LO
              LEFT JOIN SC_CREDIT.TA_LOAN_INTEREST LI
                ON LI.FI_LOAN_ID = LO.FI_LOAN_ID
               AND LI.FI_ADMIN_CENTER_ID = LO.FI_ADMIN_CENTER_ID
             WHERE LO.FI_LOAN_ID > CSL_0
               AND LO.FI_ADMIN_CENTER_ID BETWEEN PA_FIRST_CENTER_ID AND PA_END_CENTER_ID
               AND LO.FI_LOAN_STATUS_ID IN (CSL_2, CSL_3)
            )
            SELECT A.FI_LOAN_ID                                 AS FI_LOAN_ID
                  ,A.FI_ADMIN_CENTER_ID                         AS FI_ADMIN_CENTER_ID
                  ,A.FI_COUNTRY_ID								AS FI_COUNTRY_ID
                  ,A.FI_COMPANY_ID								AS FI_COMPANY_ID
                  ,A.FI_BUSINESS_UNIT_ID						AS FI_BUSINESS_UNIT_ID
                  ,A.FC_CUSTOMER_ID							    AS FC_CUSTOMER_ID
                  ,A.FN_FINANCE_CHARGE_AMOUNT                  AS FN_FINANCE_CHARGE_AMOUNT
                  ,A.FN_PAID_INTEREST_AMOUNT                   AS FN_PAID_INTEREST_AMOUNT
                  ,A.FN_PRINCIPAL_BALANCE                       AS FN_PRINCIPAL_BALANCE
                  ,A.FN_FINANCE_CHARGE_BALANCE                  AS FN_FINANCE_CHARGE_BALANCE
                  ,A.FN_ADDITIONAL_CHARGE_BALANCE              AS FN_ADDITIONAL_CHARGE_BALANCE
                  ,A.FD_ORIGINATION_DATE                        AS FD_ORIGINATION_DATE
                  ,A.FD_FIRST_PAYMENT                           AS FD_FIRST_PAYMENT
                  ,A.FI_CURRENT_BALANCE_SEQ                     AS FI_CURRENT_BALANCE_SEQ
                  ,A.FN_INTEREST_RATE                           AS FN_INTEREST_RATE
                  ,A.FI_NUMBER_OF_PAYMENTS                      AS FI_NUMBER_OF_PAYMENTS
                  ,A.FI_TERM_TYPE                               AS FI_TERM_TYPE
                  ,A.FI_LOAN_STATUS_ID							AS FI_LOAN_STATUS_ID
                  ,A.FI_PRODUCT_ID                             AS FI_PRODUCT_ID
                  ,A.FI_RULE_ID                                 AS FI_RULE_ID
                  ,A.FD_LOAN_EFFECTIVE_DATE                     AS FD_LOAN_EFFECTIVE_DATE
                  ,A.FI_PAYMENT_NUMBER_ID                       AS FI_PAYMENT_NUMBER_ID
                  ,A.FI_DAYS_ACUM_BY_TERM                       AS FI_DAYS_ACUM_BY_TERM
                  ,A.FN_DAILY_INTEREST                          AS FN_DAILY_INTEREST
                  ,A.FN_ACCRUED_INTEREST_BALANCE                AS FN_ACCRUED_INTEREST_BALANCE
                  ,A.FN_ACCRUED_INTEREST_LOAN					AS FN_ACCRUED_INTEREST_LOAN
                  ,A.FN_PAYMENT_INTEREST						AS FN_PAYMENT_INTEREST
                  ,A.FC_CONDITION_INTEREST                      AS FC_CONDITION_INTEREST
                  ,A.FD_APPLICATION_DATE                        AS FD_APPLICATION_DATE
                  ,(SELECT TO_CHAR(PSA.FD_DUE_DATE, CSL_DATE)   AS FD_DUE_DATE_ANT
                      FROM SC_CREDIT.TA_PAYMENT_SCHEDULE PSA
                     WHERE PSA.FI_LOAN_ID = A.FI_LOAN_ID
                       AND PSA.FI_ADMIN_CENTER_ID = A.FI_ADMIN_CENTER_ID
                       AND PSA.FD_DUE_DATE < TO_DATE(PA_TODAY, CSL_DATE)
                       ORDER BY PSA.FI_PAYMENT_NUMBER_ID DESC
                       FETCH FIRST 1 ROW ONLY)                      AS FD_DUE_DATE_BEFORE
                  ,(SELECT PSA.FI_PAYMENT_NUMBER_ID             AS FI_PAYMENT
                      FROM SC_CREDIT.TA_PAYMENT_SCHEDULE PSA
                     WHERE PSA.FI_LOAN_ID = A.FI_LOAN_ID
                       AND PSA.FI_ADMIN_CENTER_ID = A.FI_ADMIN_CENTER_ID
                       AND PSA.FD_DUE_DATE >= TO_DATE(PA_TODAY, CSL_DATE)
                       AND ROWNUM = CSL_1)                      AS FI_PAYMENT_ID
                  ,(SELECT TO_CHAR(PSA.FD_DUE_DATE, CSL_DATE)   AS FD_DUE_DATE_NEXT
                      FROM SC_CREDIT.TA_PAYMENT_SCHEDULE PSA
                     WHERE PSA.FI_LOAN_ID = A.FI_LOAN_ID
                       AND PSA.FI_ADMIN_CENTER_ID = A.FI_ADMIN_CENTER_ID
                       AND PSA.FD_DUE_DATE >= TO_DATE(PA_TODAY, CSL_DATE)
                       AND ROWNUM = CSL_1)                      AS FD_DUE_DATE_AFTER
                  ,SC_CREDIT.FN_SEL_LOAN_BALANCE_DET_JSON
                     (A.FI_LOAN_ID
                     ,A.FI_ADMIN_CENTER_ID
                     ,A.FI_CURRENT_BALANCE_SEQ
                     ,NULL) AS BALANCE_DET_JSON
              FROM TABPED A
             WHERE A.ORDEN = CSL_1
           		;

      EXCEPTION
      WHEN OTHERS THEN
         PA_STATUS_CODE := SQLCODE;
         PA_STATUS_MSG := SQLERRM || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;

         SC_CREDIT.SP_BATCH_ERROR_LOG(UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_PKG)
                                     ,SQLCODE
                                     ,SQLERRM
                                     ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                     ,CSL_0
                                     ,PA_FIRST_CENTER_ID || PA_END_CENTER_ID
                                     );
END SP_BTC_SEL_DAILY_INTEREST;

/

  GRANT EXECUTE ON SC_CREDIT.SP_BTC_SEL_DAILY_INTEREST TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_BTC_SEL_DAILY_INTEREST TO USRBTCCREDIT1;

--------------------------------------------------------
--  DDL for Procedure SP_SYNC_PAYMENT_SCHEDULE_STATUS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_SYNC_PAYMENT_SCHEDULE_STATUS (
    PA_SYNC_JSON   		  CLOB,
    PA_UPDATED_ROWS	OUT NUMBER,
    PA_STATUS_CODE	OUT NUMBER,
    PA_STATUS_MSG 	OUT VARCHAR2
  ) IS
/* **************************************************************
* PROJECT: NCP
* DESCRIPTION: CATALOG SYNCHONIZATION TC_PAYMENT_SCHEDULE_STATUS
* CREATED DATE: 2025/01/09
* CREATOR: CESAR CORTES
* MODIFICATION DATE: 2025/01/09
************************************************************** */
  BEGIN
    PA_STATUS_CODE := 0;
    PA_STATUS_MSG  := 'OK';

  MERGE INTO SC_CREDIT.TC_PAYMENT_SCHEDULE_STATUS A
  USING (
    SELECT
      *
    FROM
      JSON_TABLE ( PA_SYNC_JSON, '$.paymentScheduleStatus[*]'
        COLUMNS (
          ID NUMBER PATH '$.id',
          DESCRIPTION VARCHAR2 ( 80 ) PATH '$.description',
          STATUS NUMBER PATH '$.status',
          USER_TYPE VARCHAR2 ( 30 )  PATH '$.user',
          CREATED_DATE TIMESTAMP PATH '$.createdDate',
          MODIFICATION_DATE TIMESTAMP PATH '$.modificationDate'
        )
      )
  ) B ON ( A.FI_PMT_SCHEDULE_STATUS_ID = B.ID )
  WHEN MATCHED THEN UPDATE
  SET A.FI_PMT_SCHEDULE_STATUS_DESC = B.DESCRIPTION,
      A.FI_STATUS = B.STATUS,
      A.FC_USER = B.USER_TYPE,
      A.FD_MODIFICATION_DATE = CAST(B.MODIFICATION_DATE AS DATE)
  WHEN NOT MATCHED THEN
  INSERT (
    FI_PMT_SCHEDULE_STATUS_ID,
    FI_PMT_SCHEDULE_STATUS_DESC,
    FI_STATUS,
    FC_USER,
    FD_CREATED_DATE,
    FD_MODIFICATION_DATE)
  VALUES
    ( B.ID,
      B.DESCRIPTION,
      B.STATUS,
      B.USER_TYPE,
      B.CREATED_DATE,
      B.MODIFICATION_DATE);

      PA_UPDATED_ROWS := SQL%ROWCOUNT;

  COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      PA_STATUS_CODE := SQLCODE;
      PA_STATUS_MSG  := SQLERRM;
      SC_CREDIT.SP_ERROR_LOG('SP_SYNC_PAYMENT_SCHEDULE_STATUS', SQLCODE, SQLERRM, DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, NULL,'');

  END SP_SYNC_PAYMENT_SCHEDULE_STATUS;

/

  GRANT EXECUTE ON SC_CREDIT.SP_SYNC_PAYMENT_SCHEDULE_STATUS TO USRPURPOSEWS;
  GRANT EXECUTE ON SC_CREDIT.SP_SYNC_PAYMENT_SCHEDULE_STATUS TO USRNCPCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_TMP_BTC_SEL_UNPAID
--------------------------------------------------------
set define off;

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

  GRANT EXECUTE ON SC_CREDIT.SP_TMP_BTC_SEL_UNPAID TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_TMP_BTC_SEL_UNPAID TO USRBTCCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_TMP_BTC_SEL_UNPAID TO USRCREDIT02;
--------------------------------------------------------
--  DDL for Procedure SP_SYNC_LOAN_STATUS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_SYNC_LOAN_STATUS (
    PA_SYNC_JSON   		CLOB,
    PA_UPDATED_ROWS	OUT NUMBER,
    PA_STATUS_CODE	OUT NUMBER,
    PA_STATUS_MSG	OUT VARCHAR2
  ) IS
/* **************************************************************
* PROJECT: NCP
* DESCRIPTION: CATALOG SYNCHONIZATION TC_LOAN_STATUS
* CREATED DATE: 2025/01/09
* CREATOR: CESAR CORTES
* MODIFICATION DATE: 2025/01/09
************************************************************** */
  CSL_0 CONSTANT SIMPLE_INTEGER := 0;
BEGIN
  PA_STATUS_CODE := CSL_0;
  PA_STATUS_MSG := 'OK';
  PA_UPDATED_ROWS  := CSL_0;

  MERGE INTO SC_CREDIT.TC_LOAN_STATUS A
  USING (
    SELECT
      *
    FROM
      JSON_TABLE ( PA_SYNC_JSON, '$.loanStatus[*]'
        COLUMNS (
          ID NUMBER PATH '$.id',
          DESCRIPTION VARCHAR2 ( 50 ) PATH '$.description',
          ACRONYM VARCHAR2 ( 50 ) PATH '$.acronym',
          STATUS NUMBER PATH '$.status',
          USER_NAME VARCHAR2 ( 50 ) PATH '$.user',
          CREATED_DATE TIMESTAMP PATH '$.createdDate',
          MODIFICATION_DATE TIMESTAMP PATH '$.modificationDate'
        )
      )
  ) B ON ( A.FI_LOAN_STATUS_ID = B.ID )
  WHEN MATCHED THEN UPDATE
  SET A.FC_LOAN_STATUS_DESC = B.DESCRIPTION,
	  A.FC_ACRONYM 	 =B.ACRONYM,
      A.FI_STATUS = B.STATUS,
      A.FC_USER = B.USER_NAME,
      A.FD_MODIFICATION_DATE = CAST(B.MODIFICATION_DATE AS DATE)
  WHEN NOT MATCHED THEN
  INSERT (
    FI_LOAN_STATUS_ID,
    FC_LOAN_STATUS_DESC,
    FC_ACRONYM,
    FI_STATUS,
    FC_USER,
    FD_CREATED_DATE,
    FD_MODIFICATION_DATE )
  VALUES
    ( B.ID,
      B.DESCRIPTION,
      B.ACRONYM,
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
      SC_CREDIT.SP_ERROR_LOG('SP_SYNC_LOAN_STATUS', SQLCODE, SQLERRM, DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, NULL,'');

  END SP_SYNC_LOAN_STATUS;

/

  GRANT EXECUTE ON SC_CREDIT.SP_SYNC_LOAN_STATUS TO USRPURPOSEWS;
  GRANT EXECUTE ON SC_CREDIT.SP_SYNC_LOAN_STATUS TO USRNCPCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_SEL_LAST_TENDER_TYP_PAYMENT
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_SEL_LAST_TENDER_TYP_PAYMENT (
    PA_LOAN_ID         IN  SC_CREDIT.TA_LOAN.FI_LOAN_ID%TYPE,
    PA_ADMIN_CENTER_ID IN  SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE,
    PA_TENDER_TYPE_ID  IN  SC_CREDIT.TC_TENDER_TYPE.FI_TENDER_TYPE_ID%TYPE,
    PA_JSON_OBJECT     OUT CLOB,
    PA_STATUS_CODE     OUT NUMBER,
    PA_STATUS_MSG      OUT VARCHAR2
)
IS
 /***************************************************************************
 * PROJECT: CORE LOAN
 * DESCRIPTION: PROCEDURE FOR GETTING LOAN_OPERATION_ID CORRESPONDING TO
 *              PAYMENT OF THE SPECIFIED TENDER, TO REVERSE SUCH PAYMENT.
 * PRECONDITIONS: PRE-EXISTING LOANS AND OPERATIONS
 * CREATED DATE: 03/12/2024
 * CREATOR: GILBERTO CHAVEZ MUNOZ
 ****************************************************************************/
  CSL_0                CONSTANT SIMPLE_INTEGER := 0;
  CSL_1                CONSTANT SIMPLE_INTEGER := 1;
  CSL_2                CONSTANT SIMPLE_INTEGER := 2;
  CSL_3                CONSTANT SIMPLE_INTEGER := 3;
  CSL_204              CONSTANT SIMPLE_INTEGER := 204;
  CSL_422              CONSTANT SIMPLE_INTEGER := 422;
  CSL_SP               CONSTANT SIMPLE_INTEGER := 1;
  CSL_PAYMENT          CONSTANT SIMPLE_INTEGER := 2;
  CSL_CASH             CONSTANT SIMPLE_INTEGER := 3;
  CSL_ARROW            CONSTANT VARCHAR2(5) := '->';
  CSL_JSON             CONSTANT VARCHAR2(5) := NULL;
  CSL_SUCCESS          CONSTANT VARCHAR2(8) := 'SUCCESS';
  CSL_BAD_ENTITY       CONSTANT VARCHAR2(22) := 'Unprocessable Entity';
  CSL_BAD_OPERATION    CONSTANT VARCHAR2(40) := 'Operation with more than one Tender';
  CSL_NO_DATA_FOUND    CONSTANT VARCHAR2(55) := 'NO DATA FOUND (IN OPERATION OR OPERATION_DETAIL)';
  CSL_MANY_TENDERS     CONSTANT VARCHAR2(55) := 'OPERATION WITH MORE THAN 1 TENDER';
  VL_LOAN_STATUS_ID    SC_CREDIT.TA_LOAN.FI_LOAN_STATUS_ID%TYPE;
  VL_OPERATION_ID      SC_CREDIT.TA_LOAN_OPERATION.FI_LOAN_OPERATION_ID%TYPE;
  VL_OP_TYPE_ID        SC_CREDIT.TA_LOAN_OPERATION.FI_OPERATION_TYPE_ID%TYPE;
  VL_OP_AMOUNT         SC_CREDIT.TA_LOAN_OPERATION.FN_OPERATION_AMOUNT%TYPE;
  VL_APLICATION_DATE   SC_CREDIT.TA_LOAN_OPERATION.FD_APPLICATION_DATE%TYPE;
  VL_MODIFICATION_DATE SC_CREDIT.TA_LOAN_OPERATION.FD_MODIFICATION_DATE%TYPE;
  VL_PAYMENT_ID        SC_CREDIT.TA_PAYMENT_SCHEDULE.FI_SCHEDULE_TYPE_ID%TYPE;
  VL_COUNT_TENDERS     NUMBER;
  VL_TOT_TENDERS       NUMBER;

  TYPE REC_OP_DET_VALUE IS RECORD
     (
        FI_LOAN_CONCEPT_ID   SC_CREDIT.TA_LOAN_OPERATION_DETAIL.FI_LOAN_CONCEPT_ID%TYPE,
        FC_LOAN_CONCEPT_DESC SC_CREDIT.TC_LOAN_CONCEPT.FC_LOAN_CONCEPT_DESC%TYPE,
        FN_ITEM_AMOUNT       SC_CREDIT.TA_LOAN_OPERATION_DETAIL.FN_ITEM_AMOUNT%TYPE
     );

  TYPE TAB_OP_DET_VALUE IS TABLE OF REC_OP_DET_VALUE;
  VL_TABVALUE_OD TAB_OP_DET_VALUE;

  VG_LOANS         CLOB;
  VG_JA_LOANS      JSON_ARRAY_T := JSON_ARRAY_T();
  VG_JO_OPERATIONS JSON_OBJECT_T := JSON_OBJECT_T();
  VG_JA_OPERATIONS JSON_ARRAY_T := JSON_ARRAY_T();
  VG_JO_OP_DETAILS JSON_OBJECT_T := JSON_OBJECT_T();
  VG_JA_OP_DETAILS JSON_ARRAY_T := JSON_ARRAY_T();
  VG_JO_LOAN_DATA  JSON_OBJECT_T := JSON_OBJECT_T();

BEGIN
  PA_STATUS_CODE := CSL_0;
  PA_STATUS_MSG  := CSL_SUCCESS;

SELECT COUNT(DISTINCT(LNOT.FI_TENDER_TYPE_ID))
INTO   VL_COUNT_TENDERS
FROM   SC_CREDIT.TA_LOAN LN Inner Join SC_CREDIT.TA_LOAN_OPERATION LNOP
                                       ON LN.FI_LOAN_ID = LNOP.FI_LOAN_ID AND LN.FI_ADMIN_CENTER_ID = LNOP.FI_ADMIN_CENTER_ID
                            INNER JOIN SC_CREDIT.TA_LOAN_OPERATION_TENDER LNOT
                                       ON LNOP.FI_LOAN_OPERATION_ID = LNOT.FI_LOAN_OPERATION_ID AND LNOP.FI_ADMIN_CENTER_ID = LNOT.FI_ADMIN_CENTER_ID
                                           AND LNOP.FI_LOAN_ID = LNOT.FI_LOAN_ID
WHERE  LN.FI_LOAN_ID = PA_LOAN_ID
  AND    LN.FI_ADMIN_CENTER_ID = PA_ADMIN_CENTER_ID
  AND    LNOP.FI_OPERATION_TYPE_ID = CSL_PAYMENT
  AND    LNOT.FI_TENDER_TYPE_ID = PA_TENDER_TYPE_ID
  And    LN.FI_LOAN_STATUS_ID = CSL_2
;

IF VL_COUNT_TENDERS > CSL_1 THEN
     PA_STATUS_CODE := CSL_422;
     PA_STATUS_MSG  := CSL_BAD_OPERATION;
ELSE
SELECT LN.FI_LOAN_STATUS_ID, MAX(LNOP.FI_LOAN_OPERATION_ID)
INTO   VL_LOAN_STATUS_ID, VL_OPERATION_ID
FROM   SC_CREDIT.TA_LOAN LN Inner Join SC_CREDIT.TA_LOAN_OPERATION LNOP
                                       ON LN.FI_LOAN_ID = LNOP.FI_LOAN_ID AND LN.FI_ADMIN_CENTER_ID = LNOP.FI_ADMIN_CENTER_ID
                            INNER JOIN SC_CREDIT.TA_LOAN_OPERATION_TENDER LNOT
                                       ON LNOP.FI_LOAN_OPERATION_ID = LNOT.FI_LOAN_OPERATION_ID AND LNOP.FI_ADMIN_CENTER_ID = LNOT.FI_ADMIN_CENTER_ID
                                           AND LNOP.FI_LOAN_ID = LNOT.FI_LOAN_ID
WHERE  LN.FI_LOAN_ID = PA_LOAN_ID
  AND    LN.FI_ADMIN_CENTER_ID = PA_ADMIN_CENTER_ID
  AND    LNOP.FI_OPERATION_TYPE_ID = CSL_PAYMENT
  AND    LNOT.FI_TENDER_TYPE_ID = PA_TENDER_TYPE_ID
  And    LN.FI_LOAN_STATUS_ID = CSL_2
  AND    LNOP.FI_LOAN_OPERATION_ID NOT IN( SELECT LOV.FI_OPERATION_REVERSED_ID
                                           FROM   SC_CREDIT.TA_LOAN_OPERATION_VOID LOV
                                           WHERE  LOV.FI_LOAN_ID = PA_LOAN_ID
                                             AND    LOV.FI_ADMIN_CENTER_ID = PA_ADMIN_CENTER_ID
                                             AND    LOV.FI_OPERATION_TYPE_ID = CSL_PAYMENT
)
GROUP BY LN.FI_LOAN_STATUS_ID
;

SELECT FI_PAYMENT_NUMBER_ID
INTO   VL_PAYMENT_ID
FROM   SC_CREDIT.TA_LOAN LN
           INNER  JOIN SC_CREDIT.TA_PAYMENT_SCHEDULE PS
                       ON LN.FI_LOAN_ID = PS.FI_LOAN_ID And LN.FI_ADMIN_CENTER_ID = PS.FI_ADMIN_CENTER_ID
WHERE  LN.FI_LOAN_ID = PA_LOAN_ID
  AND    LN.FI_ADMIN_CENTER_ID = PA_ADMIN_CENTER_ID
  AND    FI_PAYMENT_NUMBER_ID = (SELECT MAX(FI_PAYMENT_NUMBER_ID)
                                 FROM   SC_CREDIT.TA_PAYMENT_SCHEDULE TPS
                                 WHERE  TPS.FI_LOAN_ID = LN.FI_LOAN_ID
                                   AND    TPS.FI_ADMIN_CENTER_ID = LN.FI_ADMIN_CENTER_ID
                                   AND    TPS.FN_PAYMENT_BALANCE <> TPS.FN_PAYMENT_AMOUNT
)
;

SELECT LNOP.FI_OPERATION_TYPE_ID, LNOP.FN_OPERATION_AMOUNT, LNOP.FD_APPLICATION_DATE, LNOP.FD_MODIFICATION_DATE
INTO   VL_OP_TYPE_ID, VL_OP_AMOUNT, VL_APLICATION_DATE, VL_MODIFICATION_DATE
FROM   SC_CREDIT.TA_LOAN LN Inner Join SC_CREDIT.TA_LOAN_OPERATION LNOP
                                       ON LN.FI_LOAN_ID = LNOP.FI_LOAN_ID AND LN.FI_ADMIN_CENTER_ID = LNOP.FI_ADMIN_CENTER_ID
WHERE  LN.FI_LOAN_ID = PA_LOAN_ID
  AND    LN.FI_ADMIN_CENTER_ID = PA_ADMIN_CENTER_ID
  AND    LNOP.FI_OPERATION_TYPE_ID = CSL_PAYMENT
  AND    FI_LOAN_OPERATION_ID = VL_OPERATION_ID
;

SELECT OD.FI_LOAN_CONCEPT_ID, LC.FC_LOAN_CONCEPT_DESC, OD.FN_ITEM_AMOUNT
    BULK COLLECT INTO VL_TABVALUE_OD
FROM   SC_CREDIT.TA_LOAN_OPERATION_DETAIL OD
           INNER JOIN SC_CREDIT.TC_LOAN_CONCEPT LC
                      ON OD.FI_LOAN_CONCEPT_ID = LC.FI_LOAN_CONCEPT_ID
WHERE  FI_LOAN_OPERATION_ID = VL_OPERATION_ID
;

FOR i IN 1..VL_TABVALUE_OD.COUNT LOOP
        VG_JO_OP_DETAILS.put('conceptId', VL_TABVALUE_OD(i).FI_LOAN_CONCEPT_ID);
        VG_JO_OP_DETAILS.put('amount', VL_TABVALUE_OD(i).FN_ITEM_AMOUNT);
        VG_JA_OP_DETAILS.append(VG_JO_OP_DETAILS);
END LOOP;

    VG_JO_OPERATIONS.put('typeId', VL_OP_TYPE_ID);
    VG_JO_OPERATIONS.put('amount', VL_OP_AMOUNT);
    VG_JO_OPERATIONS.put('applicationDate', VL_APLICATION_DATE);
    VG_JO_OPERATIONS.put('date', SYSDATE);
    VG_JO_OPERATIONS.put('details', VG_JA_OP_DETAILS);
    VG_JA_OPERATIONS.append(VG_JO_OPERATIONS);

    VG_JO_LOAN_DATA.put('id', PA_LOAN_ID);
    VG_JO_LOAN_DATA.put('adminCenterId', PA_ADMIN_CENTER_ID);
    VG_JO_LOAN_DATA.put('operations', VG_JA_OPERATIONS);

    VG_JA_LOANS.append(VG_JO_LOAN_DATA);
    VG_LOANS := VG_JA_LOANS.to_string;

SELECT json_object(
               'updateBalancesRequestDto' VALUE
                 json_object('loans' VALUE VG_LOANS  FORMAT JSON
                            ),
               'statusId'   VALUE VL_LOAN_STATUS_ID,
               'lastUpdate' VALUE VL_MODIFICATION_DATE,
               'paymentID'  VALUE VL_PAYMENT_ID,
               'operationId' VALUE VL_OPERATION_ID
       )
INTO   PA_JSON_OBJECT
FROM DUAL;
END IF;

EXCEPTION
  WHEN NO_DATA_FOUND THEN
    PA_STATUS_CODE := CSL_204;
    PA_STATUS_MSG := CSL_NO_DATA_FOUND;
WHEN OTHERS THEN
    PA_STATUS_CODE := SQLCODE;
    PA_STATUS_MSG := SQLERRM || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    SC_CREDIT.SP_ERROR_LOG(
       UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP)
       ,SQLCODE
       ,SQLERRM
       ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
       ,NULL
       ,CSL_JSON
       );
END SP_SEL_LAST_TENDER_TYP_PAYMENT;

/

  GRANT EXECUTE ON SC_CREDIT.SP_SEL_LAST_TENDER_TYP_PAYMENT TO USRNCPCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_STATUS_PAYMENT_SCHEDULE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_STATUS_PAYMENT_SCHEDULE (
    PA_PAYMENT_SCHEDULES IN CLOB,
    PA_STATUS_MSG        OUT VARCHAR2,
    PA_STATUS_CODE       OUT NUMBER
) IS

  VG_COUNT_UPDATE    NUMBER(3,0);
  CSL_0              CONSTANT SIMPLE_INTEGER := 0;
  CSL_404            CONSTANT SIMPLE_INTEGER := 404;
  CSL_SUCCESS        CONSTANT VARCHAR2(8) := 'OK';
  CSL_JSON           CONSTANT VARCHAR2(5) := NULL;
  CSL_UUID_TRACKING  CONSTANT VARCHAR2(5) := NULL;
BEGIN
  PA_STATUS_CODE := CSL_0;


FOR rec IN (
    SELECT VG_LOAN_ID,
           VG_ADMIN_CENTER_ID,
           VG_PAYMENT_NUMBER_ID,
           VG_PAYMENT_STATUS_ID,
           VG_PAYMENT_BALANCE,
           TO_DATE(VG_PAYMENT_DATE, 'dd/MM/yy') AS VG_PAYMENT_DATE
    FROM JSON_TABLE(
           PA_PAYMENT_SCHEDULES,
           '$.paymentSchedules[*]'
           COLUMNS (
             VG_LOAN_ID          NUMBER PATH '$.loanId',
             VG_ADMIN_CENTER_ID   NUMBER PATH '$.adminCenterId',
             VG_PAYMENT_NUMBER_ID NUMBER PATH '$.paymentNumberId',
             VG_PAYMENT_STATUS_ID NUMBER PATH '$.paymentStatusId',
             VG_PAYMENT_BALANCE  NUMBER PATH '$.paymentBalance',
             VG_PAYMENT_DATE     VARCHAR2(50) PATH '$.paymentDate'
           )
         )
  ) LOOP

UPDATE SC_CREDIT.TA_PAYMENT_SCHEDULE
SET FI_PMT_SCHEDULE_STATUS_ID = rec.VG_PAYMENT_STATUS_ID,
    FD_MODIFICATION_DATE = SYSDATE,
    FN_PAYMENT_BALANCE = rec.VG_PAYMENT_BALANCE,
    FD_PAYMENT_DATE = rec.VG_PAYMENT_DATE
WHERE FI_LOAN_ID = rec.VG_LOAN_ID
  AND FI_ADMIN_CENTER_ID = rec.VG_ADMIN_CENTER_ID
  AND FI_PAYMENT_NUMBER_ID = rec.VG_PAYMENT_NUMBER_ID;

VG_COUNT_UPDATE := SQL%ROWCOUNT;

    IF VG_COUNT_UPDATE > CSL_0 THEN
      PA_STATUS_MSG := CSL_SUCCESS;
ELSE
      PA_STATUS_MSG := 'NOT UPDATED';
      PA_STATUS_CODE := CSL_404;
END IF;
END LOOP;

COMMIT;

EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    PA_STATUS_CODE := SQLCODE;
    PA_STATUS_MSG := SQLERRM || '->' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    SC_CREDIT.SP_ERROR_LOG(
       UTL_CALL_STACK.SUBPROGRAM(1)(1),
       SQLCODE,
       SQLERRM,
       DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
       ,CSL_UUID_TRACKING
       ,CSL_JSON

    );
END SP_STATUS_PAYMENT_SCHEDULE;

/

  GRANT EXECUTE ON SC_CREDIT.SP_STATUS_PAYMENT_SCHEDULE TO USRNCPCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_SYNC_TENDER_CATEGORY_TYPE
--------------------------------------------------------
set define off;

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

  GRANT EXECUTE ON SC_CREDIT.SP_SYNC_TENDER_CATEGORY_TYPE TO USRPURPOSEWS;
  GRANT EXECUTE ON SC_CREDIT.SP_SYNC_TENDER_CATEGORY_TYPE TO USRNCPCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_BTC_INS_ERROR_LOG
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_BTC_INS_ERROR_LOG (
    PTAB_ERRORS                IN SC_CREDIT.TYP_TAB_BTC_ERROR
   ,PA_STATUS_CODE            OUT NUMBER
   ,PA_STATUS_MSG             OUT VARCHAR2
   ,PA_RECORDS_READ           OUT NUMBER
   ,PA_RECORDS_SUCCESS        OUT NUMBER
   ,PA_RECORDS_ERROR          OUT NUMBER)
IS
   /* **************************************************************
   * PROJECT: PURPOSE CORE LLC
   * DESCRIPTION: INSERT ERRORS OCCURRED IN BATCH
   * CREATED DATE: 05/11/2024
   * CREATOR: LUIS RAMIREZ
   ************************************************************** */

   CSL_0                    CONSTANT SIMPLE_INTEGER := 0;
   CSL_1                    CONSTANT SIMPLE_INTEGER := 1;
   CSL_80                   CONSTANT SIMPLE_INTEGER := 80;
   CSL_500                  CONSTANT SIMPLE_INTEGER := 500;
   CSL_600                  CONSTANT SIMPLE_INTEGER := 600;
   CSL_1000                 CONSTANT SIMPLE_INTEGER := 1000;
   CSL_SP                   CONSTANT SIMPLE_INTEGER := 1;

   --CONSTANTS SUCCESS
   CSL_SUCCESS_CODE         CONSTANT SIMPLE_INTEGER := 0;
   CSL_SUCCESS_ERROR_CODE   CONSTANT SIMPLE_INTEGER := 1;
   CSL_SUCCESS_MSG          CONSTANT VARCHAR2(10) := 'SUCCESS';
   CSL_SUCCESS_ERROR_MSG    CONSTANT VARCHAR2(35) := 'SUCCESS, WITH ERRORS RECORDS';
   CSL_ARROW                CONSTANT VARCHAR2(5) := '->';
   CSL_DATE_FORMAT          CONSTANT VARCHAR2(40) := 'MM/DD/YYYY hh24:mi:ss';

   EXC_BULK_ERRORS EXCEPTION;
   PRAGMA EXCEPTION_INIT(EXC_BULK_ERRORS, -24381); -- CODE ERROR FOR SAVE EXCEPTIONS

BEGIN
   --INITIALIZATION
   PA_STATUS_CODE := CSL_SUCCESS_CODE;
   PA_STATUS_MSG := CSL_SUCCESS_MSG;
   PA_RECORDS_SUCCESS := CSL_0;
   PA_RECORDS_ERROR := CSL_0;

   PA_RECORDS_READ := PTAB_ERRORS.COUNT;

   BEGIN
      FORALL VL_I IN INDICES OF PTAB_ERRORS SAVE EXCEPTIONS
         INSERT INTO SC_CREDIT.TA_BATCH_ERROR_LOG(
               FI_LOG_ID
               , FD_ERROR
               , FC_PROCESS
               , FI_SQL_CODE
               , FC_SQL_ERRM
               , FC_BACKTRACE
               , FI_TRANSACTION
               , FC_ADITIONAL
               , FC_USER
               , FD_CREATED_DATE
               , FD_MODIFICATION_DATE
            )VALUES (
                     SC_CREDIT.SE_BATCH_ERROR_LOG.NEXTVAL
                     ,TO_DATE(PTAB_ERRORS(VL_I).FD_ERROR,CSL_DATE_FORMAT)
                     ,SUBSTR(PTAB_ERRORS(VL_I).FC_PROCESS, CSL_1, CSL_80)
                     ,PTAB_ERRORS(VL_I).FI_SQL_CODE
                     ,SUBSTR(PTAB_ERRORS(VL_I).FC_SQL_ERRM, CSL_1, CSL_1000)
                     ,SUBSTR(PTAB_ERRORS(VL_I).FC_BACKTRACE, CSL_1, CSL_600)
                     ,PTAB_ERRORS(VL_I).FI_TRANSACTION
                     ,SUBSTR(PTAB_ERRORS(VL_I).FC_ADITIONAL, CSL_1, CSL_500)
                     ,USER
                     ,SYSDATE
                     ,SYSDATE
                     );
         COMMIT;

   EXCEPTION
      WHEN EXC_BULK_ERRORS THEN
         PA_RECORDS_ERROR := PA_RECORDS_ERROR + SQL%BULK_EXCEPTIONS.COUNT;
   END;
   COMMIT;

   IF PA_RECORDS_ERROR > CSL_0 THEN
      PA_STATUS_CODE := CSL_SUCCESS_ERROR_CODE;
      PA_STATUS_MSG := CSL_SUCCESS_ERROR_MSG;
   END IF;

EXCEPTION
   WHEN OTHERS THEN
      PA_STATUS_CODE := SQLCODE;
      PA_STATUS_MSG := SQLERRM || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;

      SC_CREDIT.SP_BATCH_ERROR_LOG(
         UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP)
         ,SQLCODE
         ,SQLERRM
         ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
         ,0
         ,NULL
         );
END SP_BTC_INS_ERROR_LOG;

/

  GRANT EXECUTE ON SC_CREDIT.SP_BTC_INS_ERROR_LOG TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_BTC_INS_ERROR_LOG TO USRBTCCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_BTC_SEL_UNPAID
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_BTC_SEL_UNPAID 
   (PA_FIRST_CENTER_ID        IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE
   ,PA_END_CENTER_ID          IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE
   ,PA_TODAY                  IN VARCHAR2
   ,PA_STATUS_CODE            OUT NUMBER
   ,PA_STATUS_MSG             OUT VARCHAR2
   ,PA_CUR_SELECT             OUT SC_CREDIT.PA_TYPES.TYP_CURSOR)

   IS
      ----------------------------------------------------------------------
          -- PROJECT: LOAN LIFE CYCLE
      -- CREATOR: Eduardo Cervantes Hernandez
      -- CREATED DATE:   18/12/2024
      -- DESCRIPTION: Select loan unpaid
      -- APPLICATION:  Process Batch of Purpose
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
              FROM SC_CREDIT.TA_LOAN LO
        INNER JOIN SC_CREDIT.TA_PAYMENT_SCHEDULE PS
                ON LO.FI_LOAN_ID = PS.FI_LOAN_ID
               AND LO.FI_ADMIN_CENTER_ID = PS.FI_ADMIN_CENTER_ID
               AND PS.FI_PMT_SCHEDULE_STATUS_ID = CSL_1
               AND PS.FD_DUE_DATE <= TO_DATE(PA_TODAY, CSL_DATE)
             WHERE LO.FI_LOAN_STATUS_ID = CSL_3
               AND LO.FI_ADMIN_CENTER_ID BETWEEN PA_FIRST_CENTER_ID AND PA_END_CENTER_ID
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
END SP_BTC_SEL_UNPAID;

/

  GRANT EXECUTE ON SC_CREDIT.SP_BTC_SEL_UNPAID TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_BTC_SEL_UNPAID TO USRBTCCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_SYNC_COUNTRY
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_SYNC_COUNTRY (
    PA_SYNC_JSON   		  CLOB,
    PA_UPDATED_ROWS	OUT NUMBER,
    PA_STATUS_CODE	OUT NUMBER,
    PA_STATUS_MSG	  OUT VARCHAR2
  ) IS
/* **************************************************************
* PROJECT: NCP
* DESCRIPTION: CATALOG SYNCHONIZATION TC_COUNTRY
* CREATED DATE: 2025/01/09
* CREATOR: CESAR CORTES
* MODIFICATION DATE: 2025/01/09
************************************************************** */
  CSL_0 CONSTANT SIMPLE_INTEGER := 0;
BEGIN
  PA_STATUS_CODE := CSL_0;
  PA_STATUS_MSG := 'OK';
  PA_UPDATED_ROWS  := CSL_0;

  MERGE INTO SC_CREDIT.TC_COUNTRY A
  USING (
    SELECT
      *
    FROM
      JSON_TABLE ( PA_SYNC_JSON, '$.country[*]'
        COLUMNS (
          ID NUMBER PATH '$.id',
          name VARCHAR2 ( 50 ) PATH '$.name',
          code VARCHAR2 ( 50 ) PATH '$.code',
          STATUS NUMBER PATH '$.status',
          USER_NAME VARCHAR2 ( 50 ) PATH '$.user',
          CREATED_DATE TIMESTAMP PATH '$.createdDate',
          MODIFICATION_DATE TIMESTAMP PATH '$.modificationDate'
        )
      )
  ) B ON ( A.FI_COUNTRY_ID = B.ID )
  WHEN MATCHED THEN UPDATE
  SET A.FC_COUNTRY_NAME = B.name,
	  A.FC_COUNTRY_CODE 	 =B.code,
      A.FI_STATUS = B.STATUS,
      A.FC_USER = B.USER_NAME,
      A.FD_MODIFICATION_DATE = CAST(B.MODIFICATION_DATE AS DATE)
  WHEN NOT MATCHED THEN
  INSERT (
    FI_COUNTRY_ID,
    FC_COUNTRY_NAME,
    FC_COUNTRY_CODE,
    FI_STATUS,
    FC_USER,
    FD_CREATED_DATE,
    FD_MODIFICATION_DATE )
  VALUES
    ( B.ID,
      B.name,
      B.code,
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
      SC_CREDIT.SP_ERROR_LOG('SP_SYNC_COUNTRY', SQLCODE, SQLERRM, DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, NULL,'');

END SP_SYNC_COUNTRY;

/

  GRANT EXECUTE ON SC_CREDIT.SP_SYNC_COUNTRY TO USRPURPOSEWS;
  GRANT EXECUTE ON SC_CREDIT.SP_SYNC_COUNTRY TO USRNCPCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_LOAN_DATA_LAST_DETAIL
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_LOAN_DATA_LAST_DETAIL (
    PA_LOAN_ID	SC_CREDIT.TA_LOAN.FI_LOAN_ID%TYPE,
    PA_ADMIN_CENTER_ID	SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE,
    PA_LOAN OUT CLOB,
    PA_STATUS_MSG OUT VARCHAR2,
    PA_STATUS_CODE OUT NUMBER
)
IS
   /* **************************************************************
   * PROYECT: CORE LOAN
   * DESCRIPTION: Obtain the last payment and its details
   * CREATED DATE: 06/12/2024
   * CREATOR: VICTOR DANIEL GUTIERREZ RODRIGUEZ
   * MODIFIED DATE: 06/12/2024
   ************************************************************** */

    CSL_0              CONSTANT SIMPLE_INTEGER := 0;
    CSL_1              CONSTANT SIMPLE_INTEGER := 1;
    CSL_2              CONSTANT SIMPLE_INTEGER := 2;
    CSL_7              CONSTANT SIMPLE_INTEGER := 7;
    CSL_8              CONSTANT SIMPLE_INTEGER := 8;
    CSL_204            CONSTANT SIMPLE_INTEGER := 204;
    CSL_422            CONSTANT SIMPLE_INTEGER := 422;
    CSL_ARROW          CONSTANT VARCHAR2(2)  := '->';
    CSL_DATE           CONSTANT VARCHAR2(10) := 'MM/DD/YYYY';
    CSL_MSG_SUCCESS    CONSTANT VARCHAR2(7)  := 'SUCCESS';
    CSL_MSG_NO_CONTENT CONSTANT VARCHAR2(10) := 'NO CONTENT';
    CSL_MSG_NO_LOAN    CONSTANT VARCHAR2(40) := 'LOAN IN WAITING, SOLD OR CLOSED STATUS';

    VG_LOAN_BALANCE_ID    NUMBER;
    VG_ADMIN_CENTER_COUNT NUMBER;
    VG_STATUS             NUMBER;

     TYPE REC_BALANCE IS RECORD
     (
        FI_LOAN_BALANCE_ID              SC_CREDIT.TA_LOAN_BALANCE.FI_LOAN_BALANCE_ID%TYPE,
        FI_LOAN_OPERATION_ID            SC_CREDIT.TA_LOAN_BALANCE.FI_LOAN_OPERATION_ID%TYPE,
        FN_PRINCIPAL_BALANCE            SC_CREDIT.TA_LOAN_BALANCE.FN_PRINCIPAL_BALANCE%TYPE,
        FN_FINANCE_CHARGE_BALANCE       SC_CREDIT.TA_LOAN_BALANCE.FN_FINANCE_CHARGE_BALANCE%TYPE,
        FN_ADDITIONAL_CHARGE_BALANCE    SC_CREDIT.TA_LOAN_BALANCE.FN_ADDITIONAL_CHARGE_BALANCE%TYPE
     );

     TYPE TAB_VALUE_BALANCE IS TABLE OF REC_BALANCE;
     VL_TABVALUE_BALANCE TAB_VALUE_BALANCE;

     TYPE REC_DETAIL IS RECORD
     (
        FI_LOAN_CONCEPT_ID SC_CREDIT.TA_LOAN_BALANCE_DETAIL.FI_LOAN_CONCEPT_ID%TYPE,
        FN_ITEM_AMOUNT SC_CREDIT.TA_LOAN_BALANCE_DETAIL.FN_ITEM_AMOUNT%TYPE
     );

     TYPE TAB_VALUE_DETAIL IS TABLE OF REC_DETAIL;
     VL_TABVALUE_DETAIL TAB_VALUE_DETAIL;

     VG_JO_BALANCE_TYPE JSON_OBJECT_T := JSON_OBJECT_T();
     VG_JA_BALANCE_TYPE      JSON_ARRAY_T  := JSON_ARRAY_T();
     VG_BALANCE_TYPE CLOB;

     VG_JO_DETAIL_TYPE JSON_OBJECT_T := JSON_OBJECT_T();
     VG_JA_DETAIL_TYPE      JSON_ARRAY_T  := JSON_ARRAY_T();
     VG_DETAIL_TYPE CLOB;

     VG_OPERATION_TYPE CLOB;
     VG_LOAN_CONCEPT   CLOB;
     VG_PAY_SCHEDULE   CLOB;
     VG_PAY_SCH_STATUS CLOB;
     VG_BLANCE_DETAIL CLOB;

     VG_JO_OPERATION_TYPE      JSON_OBJECT_T := JSON_OBJECT_T();
     VG_JA_OPERATION_TYPE      JSON_ARRAY_T  := JSON_ARRAY_T();
     VG_JO_LOAN_CONCEPT        JSON_OBJECT_T := JSON_OBJECT_T();
     VG_JA_LOAN_CONCEPT        JSON_ARRAY_T  := JSON_ARRAY_T();
     VG_JO_PAY_SCHEDULE        JSON_OBJECT_T := JSON_OBJECT_T();
     VG_JA_PAY_SCHEDULE        JSON_ARRAY_T := JSON_ARRAY_T();
     VG_JO_PAY_SCHEDULE_STATUS JSON_OBJECT_T := JSON_OBJECT_T();

    TYPE REC_VALUE IS RECORD
     (
        fi_operation_type_id   SC_CREDIT.TC_OPERATION_TYPE.FI_OPERATION_TYPE_ID%TYPE,
        fc_operation_type_desc SC_CREDIT.TC_OPERATION_TYPE.FC_OPERATION_TYPE_DESC%TYPE
     );
    TYPE TAB_VALUE IS TABLE OF REC_VALUE;
    VL_TABVALUE_LOD TAB_VALUE;


    TYPE REC_LOAN_CONCEPT IS RECORD
     (
        FI_LOAN_CONCEPT_ID      SC_CREDIT.TC_LOAN_CONCEPT.FI_LOAN_CONCEPT_ID%TYPE,
        FC_LOAN_CONCEPT_DESC    SC_CREDIT.TC_LOAN_CONCEPT.FC_LOAN_CONCEPT_DESC%TYPE,
        FI_LOAN_CONCEP_TYPE_ID  SC_CREDIT.TC_LOAN_CONCEPT.FI_LOAN_CONCEPT_TYPE_ID%TYPE
     );
    TYPE TAB_LOAN_CONCEPT IS TABLE OF REC_LOAN_CONCEPT;
    VL_TAB_LOAN_CONCEPT TAB_LOAN_CONCEPT;


    TYPE REC_PAY_SCHEDULE IS RECORD
     (
       FI_PAYMENT_NUMBER_ID SC_CREDIT.TA_PAYMENT_SCHEDULE.FI_PAYMENT_NUMBER_ID%TYPE,
       FN_PAYMENT_BALANCE   SC_CREDIT.TA_PAYMENT_SCHEDULE.FN_PAYMENT_BALANCE%TYPE
     );
    TYPE TAB_PAY_SCHEDULE IS TABLE OF REC_PAY_SCHEDULE;
    VL_TAB_PAY_SCHEDULE TAB_PAY_SCHEDULE;

    TYPE REC_PAY_SCHEDULE_STATUS IS RECORD
     (
       FI_PMT_SCHEDULE_STATUS_DESC SC_CREDIT.TC_PAYMENT_SCHEDULE_STATUS.FI_PMT_SCHEDULE_STATUS_DESC%TYPE,
       FI_PMT_SCHEDULE_STATUS_ID   SC_CREDIT.TC_PAYMENT_SCHEDULE_STATUS.FI_PMT_SCHEDULE_STATUS_ID%TYPE
     );
    TYPE TAB_PAY_SCHEDULE_STATUS IS TABLE OF REC_PAY_SCHEDULE_STATUS;
    VL_TAB_PAY_SCHEDULE_STATUS TAB_PAY_SCHEDULE_STATUS;

BEGIN
    PA_STATUS_CODE:=0;
    VG_LOAN_BALANCE_ID:=0;
    VG_ADMIN_CENTER_COUNT:=0;

SELECT COUNT(FI_ADMIN_CENTER_ID) INTO VG_ADMIN_CENTER_COUNT FROM SC_CREDIT.TA_LOAN WHERE FI_LOAN_ID= PA_LOAN_ID AND FI_ADMIN_CENTER_ID=PA_ADMIN_CENTER_ID ;
BEGIN
SELECT FI_LOAN_STATUS_ID INTO VG_STATUS FROM SC_CREDIT.TA_LOAN WHERE FI_LOAN_ID= PA_LOAN_ID AND FI_ADMIN_CENTER_ID=PA_ADMIN_CENTER_ID ;
EXCEPTION
            WHEN OTHERS THEN
                VG_STATUS:=0;
END;

        IF VG_ADMIN_CENTER_COUNT > CSL_0 AND VG_STATUS >  CSL_1 AND VG_STATUS <  CSL_7 THEN

SELECT MAX(FI_LOAN_BALANCE_ID) INTO VG_LOAN_BALANCE_ID FROM SC_CREDIT.TA_LOAN_BALANCE WHERE FI_LOAN_ID=PA_LOAN_ID AND FI_ADMIN_CENTER_ID=PA_ADMIN_CENTER_ID;
IF VG_LOAN_BALANCE_ID > 0 THEN

SELECT FI_LOAN_CONCEPT_ID,FN_ITEM_AMOUNT
    BULK COLLECT INTO VL_TABVALUE_DETAIL
FROM SC_CREDIT.TA_LOAN_BALANCE_DETAIL
WHERE FI_LOAN_BALANCE_ID=VG_LOAN_BALANCE_ID AND FI_ADMIN_CENTER_ID=PA_ADMIN_CENTER_ID
ORDER BY FD_CREATED_DATE;

VG_JO_DETAIL_TYPE := JSON_OBJECT_T();
                VG_JA_DETAIL_TYPE := JSON_ARRAY_T();

FOR i IN 1.. VL_TABVALUE_DETAIL.COUNT LOOP

                       VG_JO_DETAIL_TYPE.put('conceptId',VL_TABVALUE_DETAIL(i).FI_LOAN_CONCEPT_ID );
                       VG_JO_DETAIL_TYPE.put('itemAmount',VL_TABVALUE_DETAIL(i).FN_ITEM_AMOUNT );

                       VG_JA_DETAIL_TYPE.append(VG_JO_DETAIL_TYPE);

END LOOP;

SELECT FI_LOAN_BALANCE_ID, FI_LOAN_OPERATION_ID, FN_PRINCIPAL_BALANCE, FN_FINANCE_CHARGE_BALANCE, FN_ADDITIONAL_CHARGE_BALANCE
    BULK COLLECT INTO VL_TABVALUE_BALANCE
FROM SC_CREDIT.TA_LOAN_BALANCE
WHERE FI_LOAN_BALANCE_ID=VG_LOAN_BALANCE_ID AND FI_ADMIN_CENTER_ID=PA_ADMIN_CENTER_ID;

VG_JO_BALANCE_TYPE := JSON_OBJECT_T();
                VG_JA_BALANCE_TYPE := JSON_ARRAY_T();

FOR i IN 1.. VL_TABVALUE_BALANCE.COUNT LOOP

                       VG_JO_BALANCE_TYPE.put('id',VL_TABVALUE_BALANCE(i).FI_LOAN_BALANCE_ID );
                       VG_JO_BALANCE_TYPE.put('loanOperationId',VL_TABVALUE_BALANCE(i).FI_LOAN_OPERATION_ID );
                       VG_JO_BALANCE_TYPE.put('principalBalance',VL_TABVALUE_BALANCE(i).FN_PRINCIPAL_BALANCE );
                       VG_JO_BALANCE_TYPE.put('financeChargeBalance',VL_TABVALUE_BALANCE(i).FN_FINANCE_CHARGE_BALANCE );
                       VG_JO_BALANCE_TYPE.put('additionalChargeBalance',VL_TABVALUE_BALANCE(i).FN_ADDITIONAL_CHARGE_BALANCE );
                       VG_JO_BALANCE_TYPE.put('TA_BALANCE_DETAIL',VG_JA_DETAIL_TYPE);
END LOOP;

                VG_JA_BALANCE_TYPE.append(VG_JO_BALANCE_TYPE);
                VG_BLANCE_DETAIL:=VG_JO_BALANCE_TYPE.to_string;

SELECT fi_operation_type_id, fc_operation_type_desc
    BULK COLLECT INTO VL_TABVALUE_LOD
FROM   SC_CREDIT.tc_operation_type
WHERE  fi_status = 1;
VG_JO_OPERATION_TYPE := JSON_OBJECT_T();
FOR i IN 1.. VL_TABVALUE_LOD.COUNT LOOP
                      VG_JO_OPERATION_TYPE.put(VL_TABVALUE_LOD(i).FC_OPERATION_TYPE_DESC, VL_TABVALUE_LOD(i).FI_OPERATION_TYPE_ID);
END LOOP;
                VG_JA_OPERATION_TYPE := JSON_ARRAY_T();
                VG_JA_OPERATION_TYPE.append(VG_JO_OPERATION_TYPE);
                VG_OPERATION_TYPE := VG_JO_OPERATION_TYPE.to_string;

SELECT FI_LOAN_CONCEPT_ID, FC_LOAN_CONCEPT_DESC, FI_LOAN_CONCEPT_TYPE_ID
    BULK COLLECT INTO VL_TAB_LOAN_CONCEPT
FROM   SC_CREDIT.TC_LOAN_CONCEPT
WHERE  FI_STATUS = 1;
VG_JO_LOAN_CONCEPT := JSON_OBJECT_T();
FOR i IN 1..VL_TAB_LOAN_CONCEPT.COUNT LOOP
                       VG_JO_LOAN_CONCEPT.put(VL_TAB_LOAN_CONCEPT(i).FC_LOAN_CONCEPT_DESC, VL_TAB_LOAN_CONCEPT(i).FI_LOAN_CONCEPT_ID);
END LOOP;
                VG_JA_LOAN_CONCEPT := JSON_ARRAY_T();
                VG_JA_LOAN_CONCEPT.append( VG_JO_LOAN_CONCEPT );
                VG_LOAN_CONCEPT := VG_JO_LOAN_CONCEPT.to_string;

SELECT FI_PAYMENT_NUMBER_ID, FN_PAYMENT_BALANCE
    BULK COLLECT INTO VL_TAB_PAY_SCHEDULE
FROM   SC_CREDIT.TA_PAYMENT_SCHEDULE
WHERE  FI_LOAN_ID = PA_LOAN_ID
  AND    FI_ADMIN_CENTER_ID = PA_ADMIN_CENTER_ID
  AND    FI_PMT_SCHEDULE_STATUS_ID = CSL_1
UNION
SELECT FI_PAYMENT_NUMBER_ID, FN_PAYMENT_BALANCE
FROM   SC_CREDIT.TA_PAYMENT_SCHEDULE
WHERE  FI_LOAN_ID = PA_LOAN_ID
  AND    FI_ADMIN_CENTER_ID = PA_ADMIN_CENTER_ID
  AND    FI_PMT_SCHEDULE_STATUS_ID = CSL_2
  AND    FI_PAYMENT_NUMBER_ID = (SELECT MAX(PS2.FI_PAYMENT_NUMBER_ID)
                                 FROM   SC_CREDIT.TA_PAYMENT_SCHEDULE PS2
                                 WHERE  PS2.FI_LOAN_ID = PA_LOAN_ID
                                   AND    PS2.FI_ADMIN_CENTER_ID = PA_ADMIN_CENTER_ID
                                   AND    PS2.FI_PMT_SCHEDULE_STATUS_ID = CSL_2)
ORDER BY FI_PAYMENT_NUMBER_ID;

VG_JO_PAY_SCHEDULE := JSON_OBJECT_T();

FOR i IN 1..VL_TAB_PAY_SCHEDULE.COUNT LOOP
                   VG_JO_PAY_SCHEDULE.put(
                       TO_CHAR(VL_TAB_PAY_SCHEDULE(i).FI_PAYMENT_NUMBER_ID),
                       TO_NUMBER(VL_TAB_PAY_SCHEDULE(i).FN_PAYMENT_BALANCE)
                   );
END LOOP;

                VG_PAY_SCHEDULE := VG_JO_PAY_SCHEDULE.to_string;

SELECT FI_PMT_SCHEDULE_STATUS_DESC, FI_PMT_SCHEDULE_STATUS_ID
    BULK COLLECT INTO VL_TAB_PAY_SCHEDULE_STATUS
FROM   SC_CREDIT.TC_PAYMENT_SCHEDULE_STATUS
WHERE  FI_STATUS = 1;
VG_JO_PAY_SCHEDULE_STATUS := JSON_OBJECT_T();
FOR i IN 1..VL_TAB_PAY_SCHEDULE_STATUS.COUNT LOOP
                   VG_JO_PAY_SCHEDULE_STATUS.put(VL_TAB_PAY_SCHEDULE_STATUS(i).FI_PMT_SCHEDULE_STATUS_DESC, VL_TAB_PAY_SCHEDULE_STATUS(i).FI_PMT_SCHEDULE_STATUS_ID);
END LOOP;
                VG_PAY_SCH_STATUS := VG_JO_PAY_SCHEDULE_STATUS.to_string;

SELECT json_object('TA_LOAN' VALUE json_object(
                                                                'PAYMENT_AMOUNT'                VALUE LN.FN_PRINCIPAL_AMOUNT,
                                                                'FINANCE_CHARGE_AMOUNT'         VALUE LN.FN_FINANCE_CHARGE_AMOUNT,
                                                                'PRINCIPAL_BALANCE'             VALUE LN.FN_PRINCIPAL_BALANCE,
                                                                'FINANCE_CHARGE_BALANCE'        VALUE LN.FN_FINANCE_CHARGE_BALANCE,
                                                                'ADDITIONAL_CHARGE_BALANCE'     VALUE LN.FN_ADDITIONAL_CHARGE_BALANCE,
                                                                'TRANSACTION'                   VALUE LN.FI_TRANSACTION,
                                                                'STATUS'                        VALUE LN.FI_LOAN_STATUS_ID,
                                                                'CUSTOMER_ID'                   VALUE LN.FC_CUSTOMER_ID,
                                                                'ORIGINATION_CENTER_ID'         VALUE LN.FI_ORIGINATION_CENTER_ID,
                                                                'ADMIN_CENTER_ID'               VALUE LN.FI_ADMIN_CENTER_ID,
                                                                'ADDITIONAL_STATUS'             VALUE LN.FI_ADDITIONAL_STATUS,
                                                                'STATUS_UPDATE'                 VALUE TO_CHAR(LN.FD_LOAN_STATUS_DATE,'YYYY-MM-DD')
                                                                ),
                   'OPERATION_TYPE'                VALUE VG_OPERATION_TYPE FORMAT JSON,
                   'LOAN_CONCEPT'                  VALUE VG_LOAN_CONCEPT   FORMAT JSON,
                   'PAYMENT_SCHEDULE'              VALUE VG_PAY_SCHEDULE   FORMAT JSON,
                   'PAYMENT_SCHEDULE_STATUS'       VALUE VG_PAY_SCH_STATUS FORMAT JSON,
                   'TA_BALANCE'                    VALUE VG_BLANCE_DETAIL FORMAT JSON
       )
INTO   PA_LOAN
FROM   SC_CREDIT.TA_LOAN LN
WHERE  FI_LOAN_ID= PA_LOAN_ID AND FI_ADMIN_CENTER_ID=PA_ADMIN_CENTER_ID  ;

PA_STATUS_CODE := CSL_0;
                    PA_STATUS_MSG:=CSL_MSG_SUCCESS;
ELSE
                    PA_STATUS_CODE := CSL_204;
                    PA_STATUS_MSG:=CSL_MSG_NO_CONTENT;
END IF;
       ELSIF VG_STATUS = CSL_1 OR VG_STATUS = CSL_7 OR VG_STATUS = CSL_8 THEN
             PA_STATUS_CODE := CSL_422;
            PA_STATUS_MSG:=CSL_MSG_NO_LOAN;
ELSE
            PA_STATUS_CODE := CSL_204;
            PA_STATUS_MSG:=CSL_MSG_NO_CONTENT;
END IF;
EXCEPTION
      WHEN OTHERS THEN
        PA_STATUS_CODE := SQLCODE;
        PA_STATUS_MSG:= SQLERRM || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
        SC_CREDIT.SP_ERROR_LOG(UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_1)
                                    ,SQLCODE
                                    ,SQLERRM
                                    ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                    ,PA_LOAN_ID
                                    ,NULL);

END SP_LOAN_DATA_LAST_DETAIL;

/

  GRANT EXECUTE ON SC_CREDIT.SP_LOAN_DATA_LAST_DETAIL TO USRNCPCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_BTC_SEL_CHANGE_DEFAULT
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_BTC_SEL_CHANGE_DEFAULT 
    (PA_FIRST_CENTER_ID        IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE
   ,PA_END_CENTER_ID          IN SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE
   ,PA_STATUS_CODE            OUT NUMBER
   ,PA_STATUS_MSG             OUT VARCHAR2
   ,PA_CUR_SELECT             OUT SC_CREDIT.PA_TYPES.TYP_CURSOR)

   IS
      ----------------------------------------------------------------------
          -- PROJECT: LOAN LIFE CYCLE
      -- CREATOR: Eduardo Cervantes Hernandez
      -- CREATED DATE:   18/12/2024
      -- DESCRIPTION: Select CHANGE DEFAULT
      -- APPLICATION:  Process Batch of Purpose
      --MODIFIED DATE: 30/12/2024
      -----------------------------------------------------------------------

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
          SELECT FI_LOAN_ID
               ,FI_ADMIN_CENTER_ID
               ,FC_CUSTOMER_ID
               ,FI_COUNTRY_ID
               ,FI_COMPANY_ID
               ,FI_BUSINESS_UNIT_ID
               ,FN_PRINCIPAL_BALANCE
               ,FN_FINANCE_CHARGE_BALANCE
               ,FN_ADDITIONAL_CHARGE_BALANCE
               ,FI_PRODUCT_ID
               ,FI_RULE_ID
               ,FI_LOAN_STATUS_ID
               ,FI_CURRENT_BALANCE_SEQ
               ,FC_PLATFORM_ID
               ,FC_SUB_PLATFORM_ID
               ,FI_REGISTRATION_NUMBER
               ,FI_COUNTER_DAY
               ,FI_PAYMENT_NUMBER_ID
               ,FI_ACTION_DETAIL_ID
               ,FD_INITIAL_DATE
               ,BALANCE_DET_JSON
               ,INTEREST_JSON
           FROM(
         SELECT LO.FI_LOAN_ID
               ,LO.FI_ADMIN_CENTER_ID
               ,LO.FC_CUSTOMER_ID
               ,LO.FI_COUNTRY_ID
               ,LO.FI_COMPANY_ID
               ,LO.FI_BUSINESS_UNIT_ID
               ,LO.FN_PRINCIPAL_BALANCE
               ,LO.FN_FINANCE_CHARGE_BALANCE
               ,LO.FN_ADDITIONAL_CHARGE_BALANCE
               ,LO.FI_PRODUCT_ID
               ,LO.FI_RULE_ID
               ,LO.FI_LOAN_STATUS_ID
               ,LO.FI_CURRENT_BALANCE_SEQ
               ,LO.FC_PLATFORM_ID
               ,LO.FC_SUB_PLATFORM_ID
               ,SD.FI_REGISTRATION_NUMBER
               ,SD.FI_COUNTER_DAY
               ,SD.FI_PAYMENT_NUMBER_ID
               ,SD.FI_ACTION_DETAIL_ID
               ,TO_CHAR(SD.FD_INITIAL_DATE,CSL_DATE) AS FD_INITIAL_DATE
               ,SC_CREDIT.FN_SEL_LOAN_BALANCE_DET_JSON
                  (LO.FI_LOAN_ID
                  ,LO.FI_ADMIN_CENTER_ID
                  ,LO.FI_CURRENT_BALANCE_SEQ
                  ,LO.FC_UUID_TRACKING) AS BALANCE_DET_JSON
               ,SC_CREDIT.FN_SEL_LOAN_INTEREST_JSON
                  (LO.FI_LOAN_ID
                  ,LO.FI_ADMIN_CENTER_ID
                  ,LO.FC_UUID_TRACKING) AS INTEREST_JSON
               ,ROW_NUMBER() OVER (PARTITION BY LO.FI_LOAN_ID ORDER BY SD.FI_PAYMENT_NUMBER_ID) TAB_LOAN
           FROM SC_CREDIT.TA_LOAN LO
     INNER JOIN SC_CREDIT.TA_LOAN_STATUS_DETAIL SD
             ON SD.FI_LOAN_ID = LO.FI_LOAN_ID
            AND SD.FI_ADMIN_CENTER_ID = LO.FI_ADMIN_CENTER_ID
            AND FI_REGISTRATION_NUMBER  >  CSL_0
            AND SD.FI_ON_OFF = CSL_1
            AND SD.FI_ACTION_DETAIL_ID = CSL_3
          WHERE LO.FI_LOAN_STATUS_ID = CSL_3
            AND LO.FI_ADMIN_CENTER_ID BETWEEN PA_FIRST_CENTER_ID AND PA_END_CENTER_ID
       )
        WHERE TAB_LOAN=1;

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
END SP_BTC_SEL_CHANGE_DEFAULT;

/

  GRANT EXECUTE ON SC_CREDIT.SP_BTC_SEL_CHANGE_DEFAULT TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_BTC_SEL_CHANGE_DEFAULT TO USRBTCCREDIT1;

--------------------------------------------------------
--  DDL for Procedure SP_SYNC_OPERATION_TYPE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_SYNC_OPERATION_TYPE (
    PA_SYNC_JSON   		  CLOB,
    PA_UPDATED_ROWS	OUT NUMBER,
    PA_STATUS_CODE	OUT NUMBER,
    PA_STATUS_MSG  	OUT VARCHAR2
  ) IS
/* **************************************************************
* PROJECT: NCP
* DESCRIPTION: CATALOG SYNCHONIZATION TC_OPERATION_TYPE
* CREATED DATE: 2025/01/09
* CREATOR: CESAR CORTES
* MODIFICATION DATE: 2025/01/09
************************************************************** */
  CSL_0 CONSTANT SIMPLE_INTEGER := 0;
BEGIN
  PA_STATUS_CODE := CSL_0;
  PA_STATUS_MSG := 'OK';
  PA_UPDATED_ROWS  := CSL_0;

  MERGE INTO SC_CREDIT.TC_OPERATION_TYPE A
  USING (
    SELECT
      *
    FROM
      JSON_TABLE ( PA_SYNC_JSON, '$.operationType[*]'
        COLUMNS (
          ID NUMBER PATH '$.id',
          DESCRIPTION VARCHAR2 ( 80 ) PATH '$.description',
          LOAN_STATUS NUMBER PATH '$.loandStatus',
          OPERATION_SIGN NUMBER PATH '$.operationSign',
          CONCEPT_VOID NUMBER PATH '$.conceptVoid',
          STATUS NUMBER  PATH '$.status',
          USER_NAME VARCHAR2 ( 30 ) PATH '$.user',
          CREATEDDATE TIMESTAMP PATH '$.createdDate',
          MODIFICATIONDATE TIMESTAMP PATH '$.modificationDate'
        )
      )
  ) B ON ( A.FI_OPERATION_TYPE_ID = B.ID )
  WHEN MATCHED THEN UPDATE
  SET
	A.FC_OPERATION_TYPE_DESC  = B.DESCRIPTION,
	A.FI_LOAN_STATUS_ID  = B.LOAN_STATUS,
	A.FI_OPERATION_SIGN_ID = nvl(B.OPERATION_SIGN,A.FI_OPERATION_SIGN_ID),
	A.FI_CONCEPT_VOID     = nvl(B.CONCEPT_VOID, A.FI_CONCEPT_VOID),
	A.FI_STATUS           = B.STATUS,
  A.FC_USER                      = B.USER_NAME,
	A.FD_CREATED_DATE  = B.CREATEDDATE,
  A.FD_MODIFICATION_DATE = CAST(B.MODIFICATIONDATE AS DATE)
  WHEN NOT MATCHED THEN
  INSERT (
    FI_OPERATION_TYPE_ID,
    FC_OPERATION_TYPE_DESC,
    FI_LOAN_STATUS_ID,
    FI_OPERATION_SIGN_ID,
    FI_CONCEPT_VOID,
    FI_STATUS,
    FC_USER,
    FD_CREATED_DATE,
    FD_MODIFICATION_DATE)
  VALUES
    ( B.ID,
      B.DESCRIPTION,
      B.LOAN_STATUS,
      B.OPERATION_SIGN,
      B.CONCEPT_VOID,
      B.STATUS,
      B.USER_NAME,
      B.CREATEDDATE,
      B.MODIFICATIONDATE);

      PA_UPDATED_ROWS := SQL%ROWCOUNT;

  COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      PA_STATUS_CODE := SQLCODE;
    PA_STATUS_MSG  := SQLERRM;
      SC_CREDIT.SP_ERROR_LOG('SP_SYNC_OPERATION_TYPE', SQLCODE, SQLERRM, DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, NULL,'');

  END SP_SYNC_OPERATION_TYPE;

/

  GRANT EXECUTE ON SC_CREDIT.SP_SYNC_OPERATION_TYPE TO USRPURPOSEWS;
  GRANT EXECUTE ON SC_CREDIT.SP_SYNC_OPERATION_TYPE TO USRNCPCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_EXE_AMORTIZATION
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_EXE_AMORTIZATION (
    PA_LOAN_ID          IN SC_CREDIT.TA_PAYMENT_SCHEDULE.FI_LOAN_ID%TYPE,
    PA_CUR_PAYMENT      OUT SYS_REFCURSOR,
    PA_CUR_PAYMENT_FEE  OUT SYS_REFCURSOR,
    PA_STATUS_CODE      OUT NUMBER,
    PA_STATUS_MSG       OUT VARCHAR2)
  AS
  --CONSTANTS
    CSG_1                          CONSTANT SIMPLE_INTEGER := 1;
    CSG_0                          CONSTANT SIMPLE_INTEGER := 0;
    CSG_X                          CONSTANT VARCHAR(1) := 'X';
    CSG_ARROW                      CONSTANT VARCHAR2(5) := ' -> ';
    CSG_SUCCESS_CODE               CONSTANT SIMPLE_INTEGER := 0;
    CSG_SUCCESS_MSG                CONSTANT VARCHAR2(10) := 'SUCCESS';
    CSG_NOT_DATA_FOUND             CONSTANT SIMPLE_INTEGER := -20204;
    CSG_NOT_DATA_FOUND_MSG         CONSTANT VARCHAR2(80) := 'NO DATA FOUND';
    CSL_EXE_AMORTIZACION_RECALC    CONSTANT VARCHAR2(50) := 'SC_CREDIT.SP_EXE_AMORTIZATION_RECALC';
    CSG_FORMAT_DATE                CONSTANT VARCHAR2(50) := 'YYYY-MM-DDTHH24:MI:SSTZH:TZM';
/*************************************************************
* PROJECT:              NCP-OUTSTANDING BALANCE
* DESCRIPTION:          STORED PROCEDURE TO SELECT AMORTIZATION
* CREATOR:              JOSE DE JESUS BRAVO AGUILAR/RICARDO HAZAEL GOMEZ ALVAREZ.
* CREATED DATE:         SEP-05-2024
* MODIFICATION DATE:    JAN-22-2025
* [NCPACS-4804 V1]
*************************************************************/
  -- VARIABLES
    VL_VAL_LOAN_ID SC_CREDIT.TA_PAYMENT_SCHEDULE.FI_LOAN_ID%TYPE;
  -- EXCEPTION
    EXC_NO_DATA_FOUND         EXCEPTION;
    PRAGMA EXCEPTION_INIT (EXC_NO_DATA_FOUND, CSG_NOT_DATA_FOUND);

  BEGIN

    SELECT COUNT(*)
      INTO VL_VAL_LOAN_ID
      FROM SC_CREDIT.TA_LOAN
      WHERE FI_LOAN_ID = PA_LOAN_ID;

    IF VL_VAL_LOAN_ID = CSG_0 THEN
    RAISE EXC_NO_DATA_FOUND;
    END IF;

    PA_STATUS_CODE := CSG_SUCCESS_CODE;
    PA_STATUS_MSG := CSG_SUCCESS_MSG;

    OPEN PA_CUR_PAYMENT FOR
    SELECT DISTINCT
        PS.FI_PAYMENT_NUMBER_ID
        ,PS.FN_PAYMENT_AMOUNT
        ,TO_CHAR(CAST(PS.FD_DUE_DATE AS TIMESTAMP WITH TIME ZONE),CSG_FORMAT_DATE) AS FD_DUE_DATE
        ,PS.FI_PERIOD_DAYS
        ,PS.FN_INTEREST_AMOUNT
        ,PS.FN_PRINCIPAL_PAYMENT_AMOUNT
        ,PS.FN_OUTSTANDING_BALANCE
        ,PS.FI_PMT_SCHEDULE_STATUS_ID
    FROM
        SC_CREDIT.TA_PAYMENT_SCHEDULE PS
    WHERE
        PS.FI_LOAN_ID = PA_LOAN_ID
    ORDER BY FI_PAYMENT_NUMBER_ID ASC;

    OPEN PA_CUR_PAYMENT_FEE FOR
    SELECT DISTINCT
        PSF.FI_PAYMENT_SCHEDULE_ID
        ,PSF.FI_FEE_SEQ
        ,PSF.FI_LOAN_CONCEPT_ID
        ,LC.FC_LOAN_CONCEPT_DESC
        ,PSF.FN_FEE_AMOUNT
        ,PSF.FN_FEE_PAYMENT_BALANCE
    FROM
        SC_CREDIT.TA_PAYMENT_SCHEDULE_FEE PSF
    LEFT OUTER JOIN
        SC_CREDIT.TC_LOAN_CONCEPT LC
    ON
        PSF.FI_LOAN_CONCEPT_ID = LC.FI_LOAN_CONCEPT_ID
    WHERE
        PSF.FI_LOAN_ID = PA_LOAN_ID
    ORDER BY FI_PAYMENT_SCHEDULE_ID ASC;

  EXCEPTION
    WHEN EXC_NO_DATA_FOUND THEN
      PA_STATUS_CODE := CSG_NOT_DATA_FOUND;
      PA_STATUS_MSG := CSG_NOT_DATA_FOUND_MSG;
      SC_CREDIT.SP_ERROR_LOG(CSL_EXE_AMORTIZACION_RECALC, SQLCODE, SQLERRM,
        DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, CSG_X);
    WHEN OTHERS THEN
      PA_STATUS_CODE := SQLCODE;
      PA_STATUS_MSG := SQLERRM || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
      SC_CREDIT.SP_ERROR_LOG(CSL_EXE_AMORTIZACION_RECALC, SQLCODE, SQLERRM,
        DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, CSG_X);
  END SP_EXE_AMORTIZATION;

/

  GRANT EXECUTE ON SC_CREDIT.SP_EXE_AMORTIZATION TO USRPURPOSEWS;
  GRANT EXECUTE ON SC_CREDIT.SP_EXE_AMORTIZATION TO USRNCPCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_SEL_LOAN_STATUS_CLOSE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_SEL_LOAN_STATUS_CLOSE 
   (PA_LOAN_ID                IN      SC_CREDIT.TA_LOAN_STATUS.FI_LOAN_ID%TYPE
   ,PA_LOAN_ADMIN_CENTER_ID   IN      SC_CREDIT.TA_LOAN_STATUS.FI_ADMIN_CENTER_ID%TYPE
   ,PA_CUR_RESULTS            OUT     SC_CREDIT.PA_TYPES.TYP_CURSOR
   ,PA_STATUS_CODE            OUT     NUMBER
   ,PA_STATUS_MSG             OUT     VARCHAR2)
IS
/************************************************************************************************************************************************************************************************************************************************
PROJECT:            PURPOSE_LIFE_LOAN_CYCLE
DESCRIPTION:        THIS STORE PROCEDURE EXECUTES A CONSULT ON TA_LOAN TABLE AND RETURNS A CURSOR WITH THE INFORMATION OF THE LOAN
PRECONDITIONS:      IT MUST RECEIVE A LOAN_ID AND A LOAN_ADMIN_CENTER_ID
CREATOR:            AIXA SARMIENTO
CREATED DATE:       02/01/2025
MODIFICATION DATE:
USER MODIFICATION:
*************************************************************************************************************************************************************************************************************************************************/
   CSL_ARROW        CONSTANT VARCHAR2(5)    := '-->';                                   -- AESTHETICS SIGN
   CSL_0            CONSTANT SIMPLE_INTEGER := 0;
   CSL_1            CONSTANT SIMPLE_INTEGER := 1;
   CSL_SUCCESS_MSG  CONSTANT VARCHAR2(10)   := 'SUCCESS';                               -- MESSAGE SUCCESS
   CSL_PKG          CONSTANT SIMPLE_INTEGER := 1;                                               -- CONSTANT WITH VALUE EQUAL TO 1, FOR THE SP_BATCH_ERROR_LOG
   CSL_DATE_FORMAT  CONSTANT VARCHAR2(25)   := 'MM/DD/YYYY hh24:mi:ss'; -- CONSTANT FOR DATE CONVERSION WITH HOURS
   CSL_SPACE        CONSTANT VARCHAR2(3)    := ' ';
   VL_DATA_OUT      VARCHAR2(30)            :=  PA_LOAN_ID || CSL_SPACE || PA_LOAN_ADMIN_CENTER_ID;

BEGIN

   OPEN PA_CUR_RESULTS FOR
      SELECT
             A.FI_LOAN_ID,
                         A.FI_ADMIN_CENTER_ID,
                         A.FI_PRODUCT_ID,
             A.FI_RULE_ID,
             A.FC_CUSTOMER_ID,
             A.FI_COUNTRY_ID,
             A.FI_BUSINESS_UNIT_ID,
             A.FI_COMPANY_ID,
             A.FI_LOAN_STATUS_ID,
             TO_CHAR(A.FD_LOAN_STATUS_DATE,CSL_DATE_FORMAT) AS FD_LOAN_STATUS_DATE,
             A.FN_PRINCIPAL_BALANCE,
             A.FN_FINANCE_CHARGE_BALANCE,
             A.FN_ADDITIONAL_CHARGE_BALANCE
        FROM SC_CREDIT.TA_LOAN A
       WHERE A.FI_LOAN_ID = PA_LOAN_ID
         AND A.FI_ADMIN_CENTER_ID = PA_LOAN_ADMIN_CENTER_ID;

   PA_STATUS_CODE   := CSL_0;
   PA_STATUS_MSG    := CSL_SUCCESS_MSG;

EXCEPTION
    WHEN OTHERS THEN
       PA_STATUS_CODE := SQLCODE;
       PA_STATUS_MSG  := SQLERRM || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;

       SC_CREDIT.SP_ERROR_LOG('SP_SEL_LOAN_STATUS_CLOSE', SQLCODE, SQLERRM,
         DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, PA_LOAN_ID, VL_DATA_OUT);

END SP_SEL_LOAN_STATUS_CLOSE;

/

  GRANT EXECUTE ON SC_CREDIT.SP_SEL_LOAN_STATUS_CLOSE TO USRNCPCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_SYNC_TERM_TYPE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_SYNC_TERM_TYPE 
 (
    PA_SYNC_JSON        CLOB,
    PA_UPDATED_ROWS OUT NUMBER,
    PA_STATUS_CODE  OUT NUMBER,
    PA_STATUS_MSG   OUT VARCHAR2
  ) IS
/* **************************************************************
* PROJECT: NCP
* DESCRIPTION: CATALOG SYNCHONIZATION TC_TERM_TYPE
* CREATED DATE: 2025/01/09
* CREATOR: CESAR CORTES
* MODIFICATION DATE: 2025/01/09
************************************************************** */
  BEGIN
    PA_STATUS_CODE := 0;
    PA_STATUS_MSG  := 'OK';

     MERGE INTO SC_CREDIT.TC_TERM_TYPE A
  USING (
    SELECT
      *
    FROM
      JSON_TABLE ( PA_SYNC_JSON, '$.termType[*]'
        COLUMNS (
          ID NUMBER PATH '$.id',
          description VARCHAR2 ( 50 ) PATH '$.description',
          DAYS NUMBER PATH '$.typeDays',
          ADAPTER_ID NUMBER PATH '$.adapterId',
          STATUS NUMBER PATH '$.status',
          USER_NAME VARCHAR2 ( 50 ) PATH '$.user',
          CREATED_DATE TIMESTAMP PATH '$.createdDate',
          MODIFICATION_DATE TIMESTAMP PATH '$.modificationDate'
        )
      )
  ) B ON ( A.FI_TERM_TYPE_ID = B.ID )
  WHEN MATCHED THEN UPDATE
  SET A.FC_TERM_TYPE_DESC = B.description,
      A.FI_TERM_TYPE_DAYS = B.DAYS,
      A.FC_ADAPTER_ID = B.ADAPTER_ID,
	    A.FI_STATUS=B.STATUS,
      A.FC_USER = B.USER_NAME,
      A.FD_CREATED_DATE = B.CREATED_DATE,
      A.FD_MODIFICATION_DATE = CAST(B.MODIFICATION_DATE AS DATE)
  WHEN NOT MATCHED THEN
  INSERT (
    FI_TERM_TYPE_ID,
    FC_TERM_TYPE_DESC,
    FI_TERM_TYPE_DAYS,
    FC_ADAPTER_ID,
    FI_STATUS,
    FC_USER,
    FD_CREATED_DATE,
    FD_MODIFICATION_DATE )
  VALUES
    ( B.ID,
      B.description,
      B.DAYS,
      B.ADAPTER_ID,
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
      SC_CREDIT.SP_ERROR_LOG('SP_SYNC_TERM_TYPE', SQLCODE, SQLERRM, DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, NULL,'');

  END SP_SYNC_TERM_TYPE;

/

  GRANT EXECUTE ON SC_CREDIT.SP_SYNC_TERM_TYPE TO USRPURPOSEWS;
  GRANT EXECUTE ON SC_CREDIT.SP_SYNC_TERM_TYPE TO USRNCPCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_EXE_AMORTIZATION_RECALC
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_EXE_AMORTIZATION_RECALC (
    PA_LOAN_ID          IN SC_CREDIT.TA_PAYMENT_SCHEDULE.FI_LOAN_ID%TYPE,
    PA_CUR_LOAN_INFO    OUT SYS_REFCURSOR,
    PA_CUR_PAYMENT      OUT SYS_REFCURSOR,
    PA_CUR_FEES         OUT SYS_REFCURSOR,
    PA_STATUS_CODE      OUT NUMBER,
    PA_STATUS_MSG       OUT VARCHAR2)
  AS
  --CONSTANTS
    CSG_1                          CONSTANT SIMPLE_INTEGER := 1;
    CSG_0                          CONSTANT SIMPLE_INTEGER := 0;
    CSG_X                          CONSTANT VARCHAR(1) := 'X';
    CSG_ARROW                      CONSTANT VARCHAR2(5) := ' -> ';
    CSG_SUCCESS_CODE               CONSTANT SIMPLE_INTEGER := 0;
    CSG_SUCCESS_MSG                CONSTANT VARCHAR2(10) := 'SUCCESS';
    CSG_NOT_DATA_FOUND             CONSTANT SIMPLE_INTEGER := -20204;
    CSG_NOT_DATA_FOUND_MSG         CONSTANT VARCHAR2(80) := 'NO DATA FOUND';
    CSL_EXE_AMORTIZACION_RECALC    CONSTANT VARCHAR2(50) := 'SC_CREDIT.SP_EXE_AMORTIZATION_RECALC';
    CSG_FORMAT_DATE                CONSTANT VARCHAR2(50) := 'YYYY-MM-DDTHH24:MI:SSTZH:TZM';
/*************************************************************
  PROJECT    :  NCP-OUTSTANDING BALANCE
  DESCRIPTION:  STORED PROCEDURE TO SELECT AMORTIZATION
  CREATOR:      JOSE DE JESUS BRAVO AGUILAR/RICARDO HAZAEL GOMEZ ALVAREZ.
  CREATED DATE: SEP-05-2024
  MODIFICATION DATE: JAN-03-2025
*************************************************************/
  -- VARIABLES
    VL_VAL_LOAN_ID SC_CREDIT.TA_PAYMENT_SCHEDULE.FI_LOAN_ID%TYPE;
  -- EXCEPTION
    EXC_NO_DATA_FOUND         EXCEPTION;
    PRAGMA EXCEPTION_INIT (EXC_NO_DATA_FOUND, CSG_NOT_DATA_FOUND);

  BEGIN

    SELECT COUNT(*)
      INTO VL_VAL_LOAN_ID
      FROM SC_CREDIT.TA_LOAN
      WHERE FI_LOAN_ID = PA_LOAN_ID;

    IF VL_VAL_LOAN_ID = CSG_0 THEN
    RAISE EXC_NO_DATA_FOUND;
    END IF;

    PA_STATUS_CODE := CSG_SUCCESS_CODE;
    PA_STATUS_MSG := CSG_SUCCESS_MSG;

    OPEN PA_CUR_LOAN_INFO FOR
    SELECT DISTINCT
        L.FI_LOAN_ID,
        L.FI_ADMIN_CENTER_ID,
        L.FI_COUNTRY_ID,
        L.FI_COMPANY_ID,
        L.FI_BUSINESS_UNIT_ID,
        L.FI_PRODUCT_ID,
        L.FI_RULE_ID,
        L.FN_PRINCIPAL_AMOUNT,
        TO_CHAR(CAST(L.FD_ORIGINATION_DATE AS TIMESTAMP WITH TIME ZONE), CSG_FORMAT_DATE) AS FD_ORIGINATION_DATE,
        L.FN_INTEREST_RATE,
        L.FI_TERM_TYPE,
        TO_CHAR(CAST(L.FD_LOAN_EFFECTIVE_DATE AS TIMESTAMP WITH TIME ZONE),CSG_FORMAT_DATE) AS FD_LOAN_EFFECTIVE_DATE

    FROM
        SC_CREDIT.TA_LOAN L
    WHERE
        L.FI_LOAN_ID = PA_LOAN_ID;

    OPEN PA_CUR_PAYMENT FOR
    SELECT DISTINCT
        PS.FI_PAYMENT_NUMBER_ID
        ,PS.FN_PAYMENT_AMOUNT
        ,TO_CHAR(CAST(PS.FD_DUE_DATE AS TIMESTAMP WITH TIME ZONE),CSG_FORMAT_DATE) AS FD_DUE_DATE
        ,PS.FI_PMT_SCHEDULE_STATUS_ID
        ,TO_CHAR(CAST(PS.FD_PAYMENT_DATE AS TIMESTAMP WITH TIME ZONE),CSG_FORMAT_DATE) AS FD_PAYMENT_DATE
    FROM
        SC_CREDIT.TA_PAYMENT_SCHEDULE PS
    WHERE
        PS.FI_LOAN_ID = PA_LOAN_ID
    ORDER BY FI_PAYMENT_NUMBER_ID ASC;

      OPEN PA_CUR_FEES FOR
    SELECT
        LO.FI_LOAN_OPERATION_ID,
        LBD.FI_LOAN_BALANCE_ID,
        LBD.FI_LOAN_CONCEPT_ID,
        LBD.FN_ITEM_AMOUNT,
        LC.FC_LOAN_CONCEPT_DESC
    FROM
    SC_CREDIT.TA_LOAN_OPERATION LO
    LEFT OUTER JOIN SC_CREDIT.TA_LOAN_BALANCE LB
    ON LO.FI_LOAN_OPERATION_ID = LB.FI_LOAN_OPERATION_ID
    LEFT OUTER JOIN SC_CREDIT.TA_LOAN_BALANCE_DETAIL LBD
    ON LB.FI_LOAN_BALANCE_ID = LBD.FI_LOAN_BALANCE_ID
    LEFT OUTER JOIN SC_CREDIT.TC_LOAN_CONCEPT LC
    ON LBD.FI_LOAN_CONCEPT_ID = LC.FI_LOAN_CONCEPT_ID
    WHERE LO.FI_LOAN_ID = PA_LOAN_ID
        AND LBD.FI_LOAN_BALANCE_ID = (
      SELECT MIN(FI_LOAN_BALANCE_ID)
      FROM SC_CREDIT.TA_LOAN_BALANCE
      WHERE FI_LOAN_ID = PA_LOAN_ID);

  EXCEPTION
    WHEN EXC_NO_DATA_FOUND THEN
      PA_STATUS_CODE := CSG_NOT_DATA_FOUND;
      PA_STATUS_MSG := CSG_NOT_DATA_FOUND_MSG;
      SC_CREDIT.SP_ERROR_LOG(CSL_EXE_AMORTIZACION_RECALC, SQLCODE, SQLERRM,
        DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, CSG_X);
    WHEN OTHERS THEN
      PA_STATUS_CODE := SQLCODE;
      PA_STATUS_MSG := SQLERRM || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
      SC_CREDIT.SP_ERROR_LOG(CSL_EXE_AMORTIZACION_RECALC, SQLCODE, SQLERRM,
        DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, CSG_X);

  END SP_EXE_AMORTIZATION_RECALC;

/

  GRANT EXECUTE ON SC_CREDIT.SP_EXE_AMORTIZATION_RECALC TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_EXE_AMORTIZATION_RECALC TO USRPURPOSEWS;
--------------------------------------------------------
--  DDL for Procedure SP_SEL_OPERATION_REF_PAYMENT
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_SEL_OPERATION_REF_PAYMENT (
    PA_REFERENCE_ID     IN  SC_CREDIT.TA_LOAN_OPERATION_REF_PAYMENT.FC_REFERENCE_ID%TYPE,
    PA_OPERATION_REF_ID OUT SC_CREDIT.TA_LOAN_OPERATION_REF_PAYMENT.FI_OPERATION_REF_ID%TYPE,
    PA_STATUS_CODE      OUT NUMBER,
    PA_STATUS_MSG       OUT VARCHAR2
)
IS
 /* *************************************************************************
 * PROJECT: CORE LOAN
 * DESCRIPTION: PROCEDURE FOR GETTING PA_OPERATION_REF_ID OF AN
 *              OPERATION TO BE CLEARED.
 * PRECONDITIONS: PRE-EXISTING LOANS AND OPERATIONS
 * CREATED DATE: 07/01/2025
 * CREATOR: GILBERTO CHAVEZ MUNOZ
 ****************************************************************************/
  CSL_0             CONSTANT SIMPLE_INTEGER := 0;
  CSL_1             CONSTANT SIMPLE_INTEGER := 1;
  CSL_204           CONSTANT SIMPLE_INTEGER := 204;
  CSL_SP            CONSTANT SIMPLE_INTEGER := 1;
  CSL_ARROW         CONSTANT VARCHAR2(5)  := '->';
  CSL_JSON          CONSTANT VARCHAR2(5)  := NULL;
  CSL_SUCCESS       CONSTANT VARCHAR2(8)  := 'SUCCESS';
  CSL_NO_DATA_FOUND CONSTANT VARCHAR2(20) := 'NO REFERENCE FOUND';
BEGIN
  PA_STATUS_CODE := CSL_0;
  PA_STATUS_MSG  := CSL_SUCCESS;

SELECT ORP.FI_OPERATION_REF_ID
INTO   PA_OPERATION_REF_ID
FROM   SC_CREDIT.TA_LOAN_OPERATION_REF_PAYMENT ORP
WHERE  ORP.FC_REFERENCE_ID = PA_REFERENCE_ID;
EXCEPTION
  WHEN NO_DATA_FOUND THEN
    PA_STATUS_CODE := CSL_204;
    PA_STATUS_MSG  := CSL_NO_DATA_FOUND;
WHEN OTHERS THEN
    PA_STATUS_CODE := SQLCODE;
    PA_STATUS_MSG  := SQLERRM || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    SC_CREDIT.SP_ERROR_LOG(
       UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP)
       ,SQLCODE
       ,SQLERRM
       ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
       ,NULL
       ,CSL_JSON
       );
END SP_SEL_OPERATION_REF_PAYMENT;

/

  GRANT EXECUTE ON SC_CREDIT.SP_SEL_OPERATION_REF_PAYMENT TO USRNCPCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_SEL_PAYMENT_SCHEDULE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_SEL_PAYMENT_SCHEDULE (
  PA_LOAN_ID                   SC_CREDIT.TA_PAYMENT_SCHEDULE.FI_LOAN_ID%TYPE
  ,PA_PMT_SCHEDULE_STATUS_ID   SC_CREDIT.TA_PAYMENT_SCHEDULE.FI_PMT_SCHEDULE_STATUS_ID%TYPE DEFAULT NULL
  ,PA_CUR_RESULT               OUT SYS_REFCURSOR
  ,PA_STATUS_CODE              OUT NUMBER
  ,PA_STATUS_MSG               OUT VARCHAR2)

  AS
  -- GLOBAL CONSTANTS
  CSG_0                         CONSTANT SIMPLE_INTEGER := 0;
  CSG_1                         CONSTANT SIMPLE_INTEGER := 1;
  CSG_ARROW                     CONSTANT VARCHAR2(5) := ' -> ';
  CSG_X                         CONSTANT VARCHAR2(5) := 'X';
  CSG_SUCCESS_CODE              CONSTANT SIMPLE_INTEGER := 0;
  CSG_SUCCESS_MSG               CONSTANT VARCHAR2(10) := 'SUCCESS';
  CSG_NO_DATA_FOUND_CODE        CONSTANT SIMPLE_INTEGER := -20204;
  CSG_NO_DATA_FOUND_MSG         CONSTANT VARCHAR2(50) := 'THE DATA DOES NOT EXIST';
  CSG_SP_SEL_PAYMENT_SCHEDULE   CONSTANT VARCHAR2(50) := 'SP_SEL_PAYMENT_SCHEDULE';
  CSG_FORMAT_DATE               CONSTANT VARCHAR2(50) := 'YYYY-MM-DDTHH24:MI:SSTZH:TZM';
/*************************************************************
  PROJECT    :  NCP-OUTSTANDING BALANCE
  DESCRIPTION:  STORED PROCEDURE TO SELECT A PAYMENT SCHEDULE
  CREATOR:      JOS��� DE JES���S BRAVO AGUILAR/RICARDO HAZAEL GOMEZ ALVAREZ.
  CREATED DATE: AGO-02-2024
  MODIFICATION DATE: JAN-03-2025
*************************************************************/
  -- VARIABLES LOCALES
  VL_LOAN_ID                SC_CREDIT.TA_PAYMENT_SCHEDULE.FI_LOAN_ID%TYPE;

  EXC_NO_DATA_FOUND         EXCEPTION;
  PRAGMA EXCEPTION_INIT (EXC_NO_DATA_FOUND, CSG_NO_DATA_FOUND_CODE);

  BEGIN
    PA_STATUS_CODE := CSG_SUCCESS_CODE;
    PA_STATUS_MSG  := CSG_SUCCESS_MSG;

  SELECT COUNT(*)
  INTO VL_LOAN_ID
  FROM SC_CREDIT.TA_PAYMENT_SCHEDULE
  WHERE FI_LOAN_ID = PA_LOAN_ID;
  IF VL_LOAN_ID = CSG_0 THEN
    RAISE EXC_NO_DATA_FOUND;
  END IF;

  OPEN PA_CUR_RESULT FOR
    SELECT
      FI_LOAN_ID
      ,FI_PAYMENT_NUMBER_ID
      ,FN_PAYMENT_AMOUNT
      ,TO_CHAR(CAST(FD_DUE_DATE AS TIMESTAMP WITH TIME ZONE), CSG_FORMAT_DATE) AS FD_DUE_DATE
      ,FI_PMT_SCHEDULE_STATUS_ID
    FROM SC_CREDIT.TA_PAYMENT_SCHEDULE
    WHERE FI_LOAN_ID = PA_LOAN_ID
    AND (PA_PMT_SCHEDULE_STATUS_ID IS NULL OR
        FI_PMT_SCHEDULE_STATUS_ID = PA_PMT_SCHEDULE_STATUS_ID);

  EXCEPTION
    WHEN EXC_NO_DATA_FOUND THEN
    PA_STATUS_CODE := CSG_NO_DATA_FOUND_CODE;
    PA_STATUS_MSG := CSG_NO_DATA_FOUND_MSG || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    OPEN PA_CUR_RESULT FOR
      SELECT
      NULL FI_LOAN_ID
      ,NULL FI_PAYMENT_NUMBER_ID
      ,NULL FN_PAYMENT_AMOUNT
      ,NULL FD_DUE_DATE
      ,NULL FI_PMT_SCHEDULE_STATUS_ID
      FROM DUAL WHERE CSG_1 = CSG_0;

    WHEN OTHERS THEN
      PA_STATUS_CODE := SQLCODE;
      PA_STATUS_MSG := SQLERRM || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
      IF PA_CUR_RESULT%ISOPEN THEN
        CLOSE PA_CUR_RESULT;
      END IF;
      SC_CREDIT.SP_ERROR_LOG(CSG_SP_SEL_PAYMENT_SCHEDULE, SQLCODE, SQLERRM,
        DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, CSG_X);

  END SP_SEL_PAYMENT_SCHEDULE;

/

  GRANT EXECUTE ON SC_CREDIT.SP_SEL_PAYMENT_SCHEDULE TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_SEL_PAYMENT_SCHEDULE TO USRPURPOSEWS;
--------------------------------------------------------
--  DDL for Procedure SP_BTC_SEL_WAITING_CLOSED
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_BTC_SEL_WAITING_CLOSED 
   (PA_FIRST_CENTER_ID        IN          SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE
   ,PA_END_CENTER_ID          IN          SC_CREDIT.TA_LOAN.FI_ADMIN_CENTER_ID%TYPE
   ,PA_LOAN_STATUS_DATE       IN          VARCHAR2
   ,PA_STATUS_ID              IN          SC_CREDIT.TA_LOAN.FI_LOAN_STATUS_ID%TYPE
   ,PA_STATUS_CODE            OUT         NUMBER
   ,PA_STATUS_MSG             OUT         VARCHAR2
   ,PA_CUR_RESULTS            OUT         SC_CREDIT.PA_TYPES.TYP_CURSOR)
IS

/****************************************************************************************************
*PROJECT:            PURPOSE_LIFE_LOAN_CYCLE
*DESCRIPTION:        THIS STORE PROCEDURE EXECUTES A CONSULT ON TA_LOAN TABLE AND RETURNS A CURSOR
                     WITH THE INFORMATION OF THE LOANS WITH WAITING STATUS
*PRECONDITIONS:      IT MUST RECEIVE A RANGE OF ADMIN CENTERS, THE DESIRED LOAN STATUS DATE AND
                     THE STATUS ID
*CREATOR:            CESAR MEDINA
*CREATED DATE:       13/11/2024
*MODIFICATION DATE:  09/01/2025
*USER MODIFICATION:  AIXA SARMIENTO
*****************************************************************************************************/
   CSL_ARROW                      CONSTANT VARCHAR2(5)    := '-->';
   CSL_DATE_FORMAT                CONSTANT VARCHAR2(12)   := 'MM/DD/YYYY';
   CSL_0                          CONSTANT SIMPLE_INTEGER := 0;
   CSL_1                          CONSTANT SIMPLE_INTEGER := 1;
   CSL_SUCCESS_MESSAGE            CONSTANT VARCHAR2(10)   := 'SUCCESS';
   CSL_PKG                        CONSTANT SIMPLE_INTEGER := 1;
   VL_DATE_STATUS                 DATE;

BEGIN
   PA_CUR_RESULTS      := NULL;
   PA_STATUS_CODE      := CSL_0;
   PA_STATUS_MSG       := CSL_SUCCESS_MESSAGE;
   VL_DATE_STATUS      := TO_DATE(PA_LOAN_STATUS_DATE,CSL_DATE_FORMAT);

   OPEN PA_CUR_RESULTS FOR
      SELECT A.FI_LOAN_ID
            ,A.FI_ADMIN_CENTER_ID
            ,A.FI_COUNTRY_ID
            ,A.FI_COMPANY_ID
            ,A.FI_BUSINESS_UNIT_ID
            ,A.FI_PRODUCT_ID
            ,A.FC_CUSTOMER_ID
            ,A.FN_PRINCIPAL_AMOUNT
            ,A.FN_FINANCE_CHARGE_AMOUNT
            ,A.FN_PRINCIPAL_BALANCE
            ,A.FN_FINANCE_CHARGE_BALANCE
            ,A.FN_ADDITIONAL_CHARGE_BALANCE
            ,A.FI_CURRENT_BALANCE_SEQ
            ,A.FI_LOAN_STATUS_ID
            ,TO_CHAR(A.FD_LOAN_STATUS_DATE,CSL_DATE_FORMAT) AS FD_LOAN_STATUS_DATE
            ,A.FI_RULE_ID
            ,NVL(SC_CREDIT.FN_SEL_LOAN_BALANCE_DET_JSON(A.FI_LOAN_ID
                                                       ,A.FI_ADMIN_CENTER_ID
                                                       ,A.FI_CURRENT_BALANCE_SEQ
                                                       ,NULL),'[]')  AS FJ_BALANCE_DETAIL
        FROM SC_CREDIT.TA_LOAN A
       WHERE A.FI_LOAN_STATUS_ID = PA_STATUS_ID
         AND A.FI_ADMIN_CENTER_ID BETWEEN PA_FIRST_CENTER_ID AND PA_END_CENTER_ID
         AND A.FD_LOAN_STATUS_DATE  <= VL_DATE_STATUS;

EXCEPTION
    WHEN OTHERS THEN
        PA_STATUS_CODE      := SQLCODE;
        PA_STATUS_MSG       := SQLERRM || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;

        SC_CREDIT.SP_BATCH_ERROR_LOG (UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_PKG)
                                     ,SQLCODE
                                     ,SQLERRM
                                     ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                     ,CSL_0
                                     ,PA_FIRST_CENTER_ID || PA_END_CENTER_ID || PA_LOAN_STATUS_DATE || PA_STATUS_ID
                                     );
END SP_BTC_SEL_WAITING_CLOSED;

/

  GRANT EXECUTE ON SC_CREDIT.SP_BTC_SEL_WAITING_CLOSED TO USRBTCCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_BTC_SEL_WAITING_CLOSED TO USRNCPCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_BTC_APPLY_FEES
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_BTC_APPLY_FEES 
    (PTAB_STATUS_DETAIL          IN SC_CREDIT.TYP_TAB_BTC_STATUS_DETAIL
    ,PTAB_LOANS                  IN SC_CREDIT.TYP_TAB_BTC_LOAN
    ,PTAB_OPERATIONS             IN SC_CREDIT.TYP_TAB_BTC_OPERATION
    ,PTAB_OPERATIONS_DETAIL      IN SC_CREDIT.TYP_TAB_BTC_DETAIL
    ,PTAB_BALANCES               IN SC_CREDIT.TYP_TAB_BTC_BALANCE
    ,PTAB_BALANCES_DETAIL        IN SC_CREDIT.TYP_TAB_BTC_DETAIL
    ,PA_USER                     IN VARCHAR2
    ,PA_DEVICE                   IN VARCHAR2
    ,PA_GPS_LATITUDE             IN VARCHAR2
    ,PA_GPS_LONGITUDE            IN VARCHAR2
    ,PA_STATUS_CODE              OUT NUMBER
    ,PA_STATUS_MSG               OUT VARCHAR2
    ,PA_RECORDS_READ             OUT NUMBER
    ,PA_RECORDS_SUCCESS          OUT NUMBER
    ,PA_RECORDS_ERROR            OUT NUMBER
    ,PTAB_ERROR_RECORDS          OUT SC_CREDIT.TYP_TAB_BTC_ERROR)
IS
   /* **************************************************************
   * DESCRIPTION: PROCESS TO INSERT IN TABLE LOAN_STATUS_DETAIL,LOAN,STATUS,LOAN_OPERATION
   * LOAN_BALANCE
   * CREATED DATE: 21/11/2024
   * CREATOR: CRISTHIAN MORALES AND ITZEL TRINIDAD RAMOS
   ************************************************************** */
    CSL_0                      CONSTANT SIMPLE_INTEGER := 0;
    CSL_1                      CONSTANT SIMPLE_INTEGER := 1;
    CSL_NUMERROR               CONSTANT SIMPLE_INTEGER := -20012;
    CSL_MSG_SUCCESS            CONSTANT VARCHAR2(20)   := 'SUCCESS';
    CSL_TYPE_NULL              CONSTANT VARCHAR2(50) := 'TYPE IS NULL';
    CSL_SUCCESS_ERROR          CONSTANT VARCHAR2(50) := 'SUCCESS, WITH ERRORS RECORDS';
    CSL_PKG                    CONSTANT SIMPLE_INTEGER := 1;
    CSL_ARROW                  CONSTANT VARCHAR2(10)    := '->';
    CSL_SPACE                  CONSTANT VARCHAR2(40) := ' ';

    VL_I                       NUMBER(10,0) := 0;
    VL_TAB_ERRORS              SC_CREDIT.TYP_TAB_BTC_ERROR;
    VL_STATUS_CODE             NUMBER(10,0) := 0;
    VL_STATUS_MSG              VARCHAR2(1000);
    VL_PROCESS_DESC            VARCHAR2(150);
    VL_TAB_LOANS               SC_CREDIT.TYP_TAB_BTC_LOAN;
BEGIN
    PA_STATUS_CODE := CSL_0;
    PA_STATUS_MSG  := CSL_MSG_SUCCESS;
    PA_RECORDS_SUCCESS := 0;
    PA_RECORDS_ERROR := 0;
    PA_RECORDS_READ := 0;
    VL_TAB_ERRORS := SC_CREDIT.TYP_TAB_BTC_ERROR();

    IF PTAB_STATUS_DETAIL IS NULL OR PTAB_LOANS IS NULL OR
       PTAB_OPERATIONS IS NULL OR PTAB_OPERATIONS_DETAIL IS NULL OR
       PTAB_BALANCES IS NULL OR PTAB_BALANCES_DETAIL IS NULL THEN
       RAISE_APPLICATION_ERROR(CSL_NUMERROR, CSL_TYPE_NULL);
    END IF;

    VL_I := PTAB_STATUS_DETAIL.FIRST;
    PA_RECORDS_READ := PTAB_STATUS_DETAIL.COUNT;
    VL_TAB_LOANS := SC_CREDIT.TYP_TAB_BTC_LOAN();

    WHILE (VL_I IS NOT NULL) LOOP
    BEGIN

       VL_PROCESS_DESC := 'SP_BTC_UPD_LOAN_STATUS_DETAIL';
       SC_CREDIT.SP_BTC_UPD_LOAN_STATUS_DETAIL
                (PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID
                ,PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID
                ,PTAB_STATUS_DETAIL(VL_I).FI_PAYMENT_NUMBER_ID
                ,NULL
                ,CSL_0
                ,NULL
                ,CSL_1
                ,CSL_0
                ,VL_STATUS_CODE
                ,VL_STATUS_MSG);

       IF(VL_STATUS_CODE != CSL_0)THEN
         RAISE_APPLICATION_ERROR( CSL_NUMERROR, VL_PROCESS_DESC || CSL_SPACE || VL_STATUS_MSG);
       END IF;

       VL_PROCESS_DESC := 'SP_BTC_INS_LOAN_STATUS_DETAIL';
       SC_CREDIT.SP_BTC_INS_LOAN_STATUS_DETAIL
                (PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID
                ,PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID
                ,PTAB_STATUS_DETAIL(VL_I).FI_LOAN_STATUS_ID
                ,PTAB_STATUS_DETAIL(VL_I).FI_ACTION_DETAIL_ID
                ,PTAB_STATUS_DETAIL(VL_I).FI_COUNTER_DAY
                ,PTAB_STATUS_DETAIL(VL_I).FD_INITIAL_DATE
                ,PTAB_STATUS_DETAIL(VL_I).FI_PAYMENT_NUMBER_ID
                ,PTAB_STATUS_DETAIL(VL_I).FD_FINAL_DATE
                ,PTAB_STATUS_DETAIL(VL_I).FI_ON_OFF
                ,CSL_0
                ,VL_STATUS_CODE
                ,VL_STATUS_MSG);

          IF(VL_STATUS_CODE != CSL_0)THEN
          RAISE_APPLICATION_ERROR( CSL_NUMERROR, VL_PROCESS_DESC || CSL_SPACE || VL_STATUS_MSG);
       END IF;

       SELECT SC_CREDIT.TYP_REC_BTC_LOAN(
                   LO.FI_ADMIN_CENTER_ID
                  ,LO.FI_LOAN_ID
                  ,LO.FN_PRINCIPAL_BALANCE
                  ,LO.FN_FINANCE_CHARGE_BALANCE
                  ,LO.FN_ADDITIONAL_CHARGE_BALANCE
                  ,LO.FI_ADDITIONAL_STATUS
                  ,LO.FI_CURRENT_BALANCE_SEQ
                  ,LO.FI_LOAN_STATUS_ID
                  ,LO.FC_LOAN_STATUS_DATE
                  ,LO.FI_TRANSACTION)             AS REC_BTC_LOAN
       BULK COLLECT INTO VL_TAB_LOANS
            FROM TABLE (PTAB_LOANS) LO
       WHERE LO.FI_ADMIN_CENTER_ID = PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID
            AND LO.FI_LOAN_ID = PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID;


       VL_PROCESS_DESC := 'SP_BTC_GEN_OPERATION_BALANCE';
       SC_CREDIT.SP_BTC_GEN_OPERATION_BALANCE
                  (VL_TAB_LOANS
                  ,PTAB_OPERATIONS
                  ,PTAB_OPERATIONS_DETAIL
                  ,PTAB_BALANCES
                  ,PTAB_BALANCES_DETAIL
                  ,PA_USER
                  ,PA_DEVICE
                  ,PA_GPS_LATITUDE
                  ,PA_GPS_LONGITUDE
                  ,CSL_0
                  ,VL_STATUS_CODE
                  ,VL_STATUS_MSG);

       IF(VL_STATUS_CODE != CSL_0)THEN
         RAISE_APPLICATION_ERROR( CSL_NUMERROR, VL_PROCESS_DESC || CSL_SPACE || VL_STATUS_MSG);
       END IF;

      PA_RECORDS_SUCCESS := PA_RECORDS_SUCCESS + CSL_1;
         COMMIT;
      EXCEPTION
      WHEN OTHERS THEN
           ROLLBACK;
           SC_CREDIT.SP_BATCH_ERROR_LOG(UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_PKG)
                                             ,SQLCODE
                                             ,SQLERRM
                                             ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                             ,CSL_0
                                             ,PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID || ',' || PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID);
           PA_RECORDS_ERROR :=PA_RECORDS_ERROR + CSL_1;

           VL_TAB_ERRORS.EXTEND;
           VL_TAB_ERRORS(VL_TAB_ERRORS.LAST) :=
           SC_CREDIT.TYP_REC_BTC_ERROR(PTAB_STATUS_DETAIL(VL_I).FI_ADMIN_CENTER_ID
                                            ,PTAB_STATUS_DETAIL(VL_I).FI_LOAN_ID
                                            ,UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_PKG)
                                            ,SQLCODE
                                            ,SQLERRM
                                            ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                            ,SYSDATE
                                            ,CSL_0
                                            ,NULL);
         END;
         VL_I := PTAB_STATUS_DETAIL.NEXT(VL_I);

       END LOOP;
       PTAB_ERROR_RECORDS := VL_TAB_ERRORS;
       IF(PA_RECORDS_ERROR > CSL_0)THEN
         PA_STATUS_CODE := CSL_1;
         PA_STATUS_MSG := CSL_SUCCESS_ERROR;
       END IF;
       COMMIT;

       EXCEPTION
       WHEN OTHERS THEN
         PA_STATUS_CODE := SQLCODE;
         PA_STATUS_MSG := SQLERRM || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;

         SC_CREDIT.SP_BATCH_ERROR_LOG(UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_PKG)
                                     ,SQLCODE
                                     ,SQLERRM
                                     ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                     ,CSL_0
                                     ,NULL);

    END SP_BTC_APPLY_FEES;

/

  GRANT EXECUTE ON SC_CREDIT.SP_BTC_APPLY_FEES TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_BTC_APPLY_FEES TO USRBTCCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_SYNC_LOAN_CONCEPT_TYPE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_SYNC_LOAN_CONCEPT_TYPE 
 (
    PA_SYNC_JSON        CLOB,
    PA_UPDATED_ROWS OUT NUMBER,
    PA_STATUS_CODE  OUT NUMBER,
    PA_STATUS_MSG   OUT VARCHAR2
  ) IS
/* **************************************************************
* PROJECT: NCP
* DESCRIPTION: CATALOG SYNCHONIZATION TC_LOAN_CONCEPT_TYPE
* CREATED DATE: 2025/01/09
* CREATOR: CESAR CORTES
* MODIFICATION DATE: 2025/01/09
************************************************************** */
    CSL_0 CONSTANT SIMPLE_INTEGER := 0;
BEGIN
  PA_STATUS_CODE := CSL_0;
  PA_STATUS_MSG := 'OK';
  PA_UPDATED_ROWS  := CSL_0;

     MERGE INTO SC_CREDIT.TC_LOAN_CONCEPT_TYPE A
  USING (
    SELECT
      *
    FROM
      JSON_TABLE ( PA_SYNC_JSON, '$.loanConceptType[*]'
        COLUMNS (
          ID NUMBER PATH '$.id',
          description VARCHAR2 ( 50 ) PATH '$.description',
          STATUS NUMBER PATH '$.status',
          USER_NAME VARCHAR2 ( 50 ) PATH '$.user',
          CREATED_DATE TIMESTAMP PATH '$.createdDate',
          MODIFICATION_DATE TIMESTAMP PATH '$.modificationDate'
        )
      )
  ) B ON ( A.FI_LOAN_CONCEPT_TYPE_ID = B.ID )
  WHEN MATCHED THEN UPDATE
  SET A.FC_LOAN_CONCEPT_TYPE_DESC = B.description,
	    A.FI_STATUS=B.STATUS,
      A.FC_USER = B.USER_NAME,
      A.FD_CREATED_DATE = B.CREATED_DATE,
      A.FD_MODIFICATION_DATE = CAST(B.MODIFICATION_DATE AS DATE)
  WHEN NOT MATCHED THEN
  INSERT (
    FI_LOAN_CONCEPT_TYPE_ID,
    FC_LOAN_CONCEPT_TYPE_DESC,
    FI_STATUS,
    FC_USER,
    FD_CREATED_DATE,
    FD_MODIFICATION_DATE )
  VALUES
    ( B.ID,
      B.description,
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
      SC_CREDIT.SP_ERROR_LOG('SP_SYNC_LOAN_CONCEPT_TYPE', SQLCODE, SQLERRM, DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, NULL,'');

END SP_SYNC_LOAN_CONCEPT_TYPE;

/

  GRANT EXECUTE ON SC_CREDIT.SP_SYNC_LOAN_CONCEPT_TYPE TO USRPURPOSEWS;
  GRANT EXECUTE ON SC_CREDIT.SP_SYNC_LOAN_CONCEPT_TYPE TO USRNCPCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_BTC_EXE_CHANGE_DEFAULT
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_BTC_EXE_CHANGE_DEFAULT 
   (PTAB_STATUS_DETAIL          IN SC_CREDIT.TYP_TAB_BTC_STATUS_DETAIL
   ,PTAB_STATUS                 IN SC_CREDIT.TYP_TAB_BTC_STATUS
   ,PTAB_LOANS                  IN SC_CREDIT.TYP_TAB_BTC_LOAN
   ,PTAB_OPERATIONS             IN SC_CREDIT.TYP_TAB_BTC_OPERATION
   ,PTAB_OPERATIONS_DETAIL      IN SC_CREDIT.TYP_TAB_BTC_DETAIL
   ,PTAB_BALANCES               IN SC_CREDIT.TYP_TAB_BTC_BALANCE
   ,PTAB_BALANCES_DETAIL        IN SC_CREDIT.TYP_TAB_BTC_DETAIL
   ,PA_DEVICE                   IN SC_CREDIT.TA_LOAN_OPERATION.FC_DEVICE%TYPE
   ,PA_GPS_LATITUDE             IN SC_CREDIT.TA_LOAN_OPERATION.FC_GPS_LATITUDE%TYPE
   ,PA_GPS_LONGITUDE            IN SC_CREDIT.TA_LOAN_OPERATION.FC_GPS_LONGITUDE%TYPE
   ,PA_STATUS_CODE              OUT NUMBER
   ,PA_STATUS_MSG               OUT VARCHAR2
   ,PA_RECORDS_READ             OUT NUMBER
   ,PA_RECORDS_SUCCESS          OUT NUMBER
   ,PA_RECORDS_ERROR            OUT NUMBER
   ,PTAB_ERROR_RECORDS          OUT SC_CREDIT.TYP_TAB_BTC_ERROR
)
IS

   /* **************************************************************
   * PROJECT: LOAN LIFE CYCLE
   * DESCRIPTION: V3 - PROCESS TO APPLY NO PAYMENT
   * CREATED DATE: 17/12/2024
   * CREATOR: EDUARDO CERVANTES
   * MODIFICATION DATE: 15/01/2025
   * PERFORMANCE MODIFICATIONS - IVAN LOPEZ
   * [NCPADC-4480-1V2]
   ************************************************************** */

   --CONSTANTS
   CSL_0                              CONSTANT SIMPLE_INTEGER := 0;
   CSL_1                              CONSTANT SIMPLE_INTEGER := 1;
   CSL_3                              CONSTANT SIMPLE_INTEGER := 3;
   CSL_SP                             CONSTANT SIMPLE_INTEGER := 1;

   --CONSTANTS SUCCESS
   CSL_SUCCESS_CODE                   CONSTANT SIMPLE_INTEGER := 0;
   CSL_SUCCESS_MSG                    CONSTANT VARCHAR2(7) := 'SUCCESS';
   CSL_CODE_ERROR                     CONSTANT SIMPLE_INTEGER := -20012;
   CSL_SPACE                          CONSTANT VARCHAR2(2) := ' ';
   CSL_SUCCESS_ERROR                  CONSTANT VARCHAR2(28) := 'SUCCESS, WITH ERRORS RECORDS';
   CSL_TYPE_NULL                      CONSTANT VARCHAR2(15) := 'TYPE FEES NULL';
   CSL_ARROW                          CONSTANT VARCHAR2(5) := ' -> ';
   CSL_COMMA                          CONSTANT VARCHAR2(5) := ' , ';
   CSL_INS_STATUS_DETAIL              CONSTANT VARCHAR2(50) := 'NO INSERT TA_LOAN_STATUS_DETAIL';
   CSL_EXE_OPERATION                  CONSTANT VARCHAR2(30) := 'SP_BTC_EXE_OPERATION_BALANCE';
   CSL_INS_LOAN_STATUS                CONSTANT VARCHAR2(30) := 'SP_BTC_INS_LOAN_STATUS';
   CSL_UPD_LOAN_STATUS_DETAIL         CONSTANT VARCHAR2(30) := 'SP_BTC_UPD_LOAN_STATUS_DETAIL';
   VL_I                               NUMBER(10,0) := 0;
   VL_STATUS_CODE                     NUMBER(10,0) := 0;
   VL_STATUS_MSG                      VARCHAR2(1000);
   CSL_DATE_FORMAT                    CONSTANT VARCHAR2(40)   := 'MM/DD/YYYY hh24:mi:ss';

   --VARIABLES INTERNAL TYPES ASSIGNMENT
   VLTAB_ERRORS                       SC_CREDIT.TYP_TAB_BTC_ERROR;
   VLTAB_STATUS_DETAIL                SC_CREDIT.TYP_TAB_BTC_STATUS_DETAIL;
   VLTAB_LOANS                        SC_CREDIT.TYP_TAB_BTC_LOAN;
   VLTAB_OPERATIONS                   SC_CREDIT.TYP_TAB_BTC_OPERATION;
   VLTAB_OPERATIONS_DETAIL            SC_CREDIT.TYP_TAB_BTC_DETAIL;
   VLTAB_BALANCES                     SC_CREDIT.TYP_TAB_BTC_BALANCE;
   VLTAB_BALANCES_DETAIL              SC_CREDIT.TYP_TAB_BTC_DETAIL;

   --VARIABLES OF ITERATION BY LOAN
   VLTAB_STATUS_DET_BY_LOAN          SC_CREDIT.TYP_TAB_BTC_STATUS_DETAIL;
   VLREC_LOAN                        SC_CREDIT.TYP_REC_BTC_LOAN;
   VLTAB_OPERATIONS_BY_LOAN          SC_CREDIT.TYP_TAB_BTC_OPERATION;
   VLTAB_OPERATIONS_DET_BY_LOAN      SC_CREDIT.TYP_TAB_BTC_DETAIL;
   VLTAB_BALANCES_BY_LOAN            SC_CREDIT.TYP_TAB_BTC_BALANCE;
   VLTAB_BALANCES_DET_BY_LOAN        SC_CREDIT.TYP_TAB_BTC_DETAIL;

   VL_T1 timestamp;--TODO QUITAR
   VL_T2 timestamp;--TODO QUITAR
   VL_DESC_COUNT                     VARCHAR2(500);

BEGIN
   PA_STATUS_CODE := CSL_SUCCESS_CODE;
   PA_STATUS_MSG := CSL_SUCCESS_MSG;
   PA_RECORDS_SUCCESS := CSL_0;
   PA_RECORDS_ERROR := CSL_0;
   PA_RECORDS_READ := CSL_0;
   VLTAB_ERRORS := SC_CREDIT.TYP_TAB_BTC_ERROR();
   VL_T1 := systimestamp;--TODO QUITAR

   IF PTAB_STATUS_DETAIL IS NULL OR PTAB_STATUS IS NULL OR PTAB_LOANS IS NULL
         OR PTAB_OPERATIONS IS NULL OR PTAB_OPERATIONS_DETAIL IS NULL
         OR PTAB_BALANCES IS NULL OR PTAB_BALANCES_DETAIL IS NULL THEN
      RAISE_APPLICATION_ERROR(CSL_CODE_ERROR, CSL_TYPE_NULL);
   END IF;

   VL_I := PTAB_LOANS.FIRST;
   PA_RECORDS_READ := PTAB_LOANS.COUNT;

   --INTERNAL TYPES ASSIGNMENT
   VLTAB_STATUS_DETAIL     := PTAB_STATUS_DETAIL;
   VLTAB_LOANS             := PTAB_LOANS;
   VLTAB_OPERATIONS        := PTAB_OPERATIONS;
   VLTAB_OPERATIONS_DETAIL := PTAB_OPERATIONS_DETAIL;
   VLTAB_BALANCES          := PTAB_BALANCES;
   VLTAB_BALANCES_DETAIL   := PTAB_BALANCES_DETAIL;


   WHILE (VL_I IS NOT NULL) LOOP
      BEGIN
         VLTAB_STATUS_DET_BY_LOAN := SC_CREDIT.TYP_TAB_BTC_STATUS_DETAIL();
         VLTAB_OPERATIONS_BY_LOAN := SC_CREDIT.TYP_TAB_BTC_OPERATION();
         VLTAB_OPERATIONS_DET_BY_LOAN := SC_CREDIT.TYP_TAB_BTC_DETAIL();
         VLTAB_BALANCES_BY_LOAN := SC_CREDIT.TYP_TAB_BTC_BALANCE();
         VLTAB_BALANCES_DET_BY_LOAN := SC_CREDIT.TYP_TAB_BTC_DETAIL();


         --TAB BY STATUS DETAIL
         <<loopStatusDetailAssignment>>
         WHILE VLTAB_STATUS_DETAIL.COUNT > CSL_0 AND PTAB_LOANS.EXISTS(VL_I) LOOP
            IF VLTAB_STATUS_DETAIL(VLTAB_STATUS_DETAIL.FIRST).FI_LOAN_ID = PTAB_LOANS(VL_I).FI_LOAN_ID
               AND VLTAB_STATUS_DETAIL(VLTAB_STATUS_DETAIL.FIRST).FI_ADMIN_CENTER_ID = PTAB_LOANS(VL_I).FI_ADMIN_CENTER_ID THEN

               VLTAB_STATUS_DET_BY_LOAN.EXTEND;
               VLTAB_STATUS_DET_BY_LOAN(VLTAB_STATUS_DET_BY_LOAN.LAST) := VLTAB_STATUS_DETAIL(VLTAB_STATUS_DETAIL.FIRST);
               VLTAB_STATUS_DETAIL.DELETE(VLTAB_STATUS_DETAIL.FIRST);
            ELSE
               EXIT loopStatusDetailAssignment;
            END IF;
         END LOOP loopStatusDetailAssignment;

         --TAB BY LOAN ASSIGNMENT
         <<loopLoanAssignment>>
         WHILE VLTAB_LOANS.COUNT > CSL_0 AND PTAB_LOANS.EXISTS(VL_I) LOOP
            IF VLTAB_LOANS(VLTAB_LOANS.FIRST).FI_LOAN_ID = PTAB_LOANS(VL_I).FI_LOAN_ID
               AND VLTAB_LOANS(VLTAB_LOANS.FIRST).FI_ADMIN_CENTER_ID = PTAB_LOANS(VL_I).FI_ADMIN_CENTER_ID THEN

               VLREC_LOAN := VLTAB_LOANS(VLTAB_LOANS.FIRST);
               VLTAB_LOANS.DELETE(VLTAB_LOANS.FIRST);
            ELSE
               EXIT loopLoanAssignment;
            END IF;
         END LOOP loopLoanAssignment;

         --TAB BY OPERATION ASSIGNMENT
         <<loopOperationAssignment>>
         WHILE VLTAB_OPERATIONS.COUNT > CSL_0 AND PTAB_LOANS.EXISTS(VL_I) LOOP
            IF VLTAB_OPERATIONS(VLTAB_OPERATIONS.FIRST).FI_LOAN_ID = PTAB_LOANS(VL_I).FI_LOAN_ID
               AND VLTAB_OPERATIONS(VLTAB_OPERATIONS.FIRST).FI_ADMIN_CENTER_ID = PTAB_LOANS(VL_I).FI_ADMIN_CENTER_ID THEN

               VLTAB_OPERATIONS_BY_LOAN.EXTEND;
               VLTAB_OPERATIONS_BY_LOAN(VLTAB_OPERATIONS_BY_LOAN.LAST) := VLTAB_OPERATIONS(VLTAB_OPERATIONS.FIRST);
               VLTAB_OPERATIONS.DELETE(VLTAB_OPERATIONS.FIRST);
            ELSE
               EXIT loopOperationAssignment;
            END IF;
         END LOOP loopOperationAssignment;

         --TAB BY OPERATION DET ASSIGNMENT
         <<loopOperationDetAssignment>>
         WHILE VLTAB_OPERATIONS_DETAIL.COUNT > CSL_0 AND PTAB_LOANS.EXISTS(VL_I) LOOP
            IF VLTAB_OPERATIONS_DETAIL(VLTAB_OPERATIONS_DETAIL.FIRST).FI_LOAN_ID = PTAB_LOANS(VL_I).FI_LOAN_ID
               AND VLTAB_OPERATIONS_DETAIL(VLTAB_OPERATIONS_DETAIL.FIRST).FI_ADMIN_CENTER_ID = PTAB_LOANS(VL_I).FI_ADMIN_CENTER_ID THEN

               VLTAB_OPERATIONS_DET_BY_LOAN.EXTEND;
               VLTAB_OPERATIONS_DET_BY_LOAN(VLTAB_OPERATIONS_DET_BY_LOAN.LAST) := VLTAB_OPERATIONS_DETAIL(VLTAB_OPERATIONS_DETAIL.FIRST);
               VLTAB_OPERATIONS_DETAIL.DELETE(VLTAB_OPERATIONS_DETAIL.FIRST);
            ELSE
               EXIT loopOperationDetAssignment;
            END IF;
         END LOOP loopOperationDetAssignment;

         --TAB BY BALANCES ASSIGNMENT
         <<loopBalanceAssignment>>
         WHILE VLTAB_BALANCES.COUNT > CSL_0 AND PTAB_LOANS.EXISTS(VL_I) LOOP
            IF VLTAB_BALANCES(VLTAB_BALANCES.FIRST).FI_LOAN_ID = PTAB_LOANS(VL_I).FI_LOAN_ID
               AND VLTAB_BALANCES(VLTAB_BALANCES.FIRST).FI_ADMIN_CENTER_ID = PTAB_LOANS(VL_I).FI_ADMIN_CENTER_ID THEN

               VLTAB_BALANCES_BY_LOAN.EXTEND;
               VLTAB_BALANCES_BY_LOAN(VLTAB_BALANCES_BY_LOAN.LAST) := VLTAB_BALANCES(VLTAB_BALANCES.FIRST);
               VLTAB_BALANCES.DELETE(VLTAB_BALANCES.FIRST);
            ELSE
               EXIT loopBalanceAssignment;
            END IF;
         END LOOP loopBalanceAssignment;

         --TAB BY BALANCES DET ASSIGNMENT
         <<loopBalanceDetAssignment>>
         WHILE VLTAB_BALANCES_DETAIL.COUNT > CSL_0 AND PTAB_LOANS.EXISTS(VL_I) LOOP
            IF VLTAB_BALANCES_DETAIL(VLTAB_BALANCES_DETAIL.FIRST).FI_LOAN_ID = PTAB_LOANS(VL_I).FI_LOAN_ID
               AND VLTAB_BALANCES_DETAIL(VLTAB_BALANCES_DETAIL.FIRST).FI_ADMIN_CENTER_ID = PTAB_LOANS(VL_I).FI_ADMIN_CENTER_ID THEN

               VLTAB_BALANCES_DET_BY_LOAN.EXTEND;
               VLTAB_BALANCES_DET_BY_LOAN(VLTAB_BALANCES_DET_BY_LOAN.LAST) := VLTAB_BALANCES_DETAIL(VLTAB_BALANCES_DETAIL.FIRST);
               VLTAB_BALANCES_DETAIL.DELETE(VLTAB_BALANCES_DETAIL.FIRST);
            ELSE
               EXIT loopBalanceDetAssignment;
            END IF;
         END LOOP loopBalanceDetAssignment;


           --Se apagan los registros ya que pasara a default
         SC_CREDIT.SP_BTC_UPD_LOAN_STATUS_DETAIL
            (PTAB_LOANS(VL_I).FI_LOAN_ID
            ,PTAB_LOANS(VL_I).FI_ADMIN_CENTER_ID
            ,NULL
            ,NULL
            ,CSL_0
            ,PTAB_LOANS(VL_I).FI_TRANSACTION
            ,CSL_0
            ,CSL_0
            ,VL_STATUS_CODE
            ,VL_STATUS_MSG);

         IF(VL_STATUS_CODE != CSL_0)THEN
            RAISE_APPLICATION_ERROR( CSL_CODE_ERROR, CSL_UPD_LOAN_STATUS_DETAIL || CSL_SPACE || VL_STATUS_MSG);
         END IF;

           --se hace el insert en status_detail para pasar a default
         FORALL SD IN VLTAB_STATUS_DET_BY_LOAN.FIRST .. VLTAB_STATUS_DET_BY_LOAN.LAST SAVE EXCEPTIONS
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
          VALUES (VLTAB_STATUS_DET_BY_LOAN(SD).FI_LOAN_ID
                 ,VLTAB_STATUS_DET_BY_LOAN(SD).FI_ADMIN_CENTER_ID
                 ,SC_CREDIT.SE_LOAN_STATUS_DETAIL.NEXTVAL
                 ,VLTAB_STATUS_DET_BY_LOAN(SD).FI_LOAN_STATUS_ID
                 ,VLTAB_STATUS_DET_BY_LOAN(SD).FI_ACTION_DETAIL_ID
                 ,VLTAB_STATUS_DET_BY_LOAN(SD).FI_COUNTER_DAY
                 ,TO_DATE(VLTAB_STATUS_DET_BY_LOAN(SD).FD_INITIAL_DATE,CSL_DATE_FORMAT)
                 ,VLTAB_STATUS_DET_BY_LOAN(SD).FI_PAYMENT_NUMBER_ID
                 ,TO_DATE(VLTAB_STATUS_DET_BY_LOAN(SD).FD_FINAL_DATE,CSL_DATE_FORMAT)
                 ,VLTAB_STATUS_DET_BY_LOAN(SD).FI_ON_OFF
                 ,USER
                 ,SYSDATE
                 ,SYSDATE
                 );

         IF(SQL%ROWCOUNT = CSL_0)THEN
            RAISE_APPLICATION_ERROR( CSL_CODE_ERROR, CSL_INS_STATUS_DETAIL || CSL_SPACE || VL_STATUS_MSG);
         END IF;

         --EXECUTE PROCESS TO AFFECT LOAN, OPERATIONS AND BALANCES
         SC_CREDIT.SP_BTC_EXE_OPERATION_BALANCE(
            VLREC_LOAN
            ,VLTAB_OPERATIONS_BY_LOAN
            ,VLTAB_OPERATIONS_DET_BY_LOAN
            ,VLTAB_BALANCES_BY_LOAN
            ,VLTAB_BALANCES_DET_BY_LOAN
            ,PA_DEVICE
            ,PA_GPS_LATITUDE
            ,PA_GPS_LONGITUDE
            ,CSL_0
            ,VL_STATUS_CODE
            ,VL_STATUS_MSG);

         IF(VL_STATUS_CODE != CSL_0)THEN
            RAISE_APPLICATION_ERROR( CSL_CODE_ERROR, CSL_EXE_OPERATION || CSL_SPACE || VL_STATUS_MSG);
         END IF;

         SC_CREDIT.SP_BTC_INS_LOAN_STATUS(
             PTAB_STATUS(VL_I).FI_LOAN_ID
            ,PTAB_STATUS(VL_I).FI_ADMIN_CENTER_ID
            ,PTAB_STATUS(VL_I).FI_LOAN_OPERATION_ID
            ,PTAB_STATUS(VL_I).FI_LOAN_STATUS_ID
            ,PTAB_STATUS(VL_I).FI_LOAN_STATUS_OLD_ID
            ,PTAB_STATUS(VL_I).FI_TRIGGER_ID
            ,PTAB_STATUS(VL_I).FD_LOAN_STATUS_DATE
            ,CSL_0
            ,CSL_0
            ,VL_STATUS_CODE
            ,VL_STATUS_MSG
            );

         IF(VL_STATUS_CODE != CSL_0)THEN
         RAISE_APPLICATION_ERROR( CSL_CODE_ERROR, CSL_INS_LOAN_STATUS || CSL_SPACE || VL_STATUS_MSG);
         END IF;

         --DELETE TYPES BY LOAN (CYCLE)
         VLTAB_STATUS_DET_BY_LOAN.DELETE;
         VLTAB_OPERATIONS_BY_LOAN.DELETE;
         VLTAB_OPERATIONS_DET_BY_LOAN.DELETE;
         VLTAB_BALANCES_BY_LOAN.DELETE;
         VLTAB_BALANCES_DET_BY_LOAN.DELETE;

         PA_RECORDS_SUCCESS := PA_RECORDS_SUCCESS + CSL_1;

      EXCEPTION
         WHEN OTHERS THEN
            ROLLBACK;
            SC_CREDIT.SP_BATCH_ERROR_LOG(
               UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP)
               ,SQLCODE
               ,SQLERRM
               ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
               ,CSL_0
               ,PTAB_LOANS(VL_I).FI_ADMIN_CENTER_ID
                  ||CSL_COMMA
                  ||PTAB_LOANS(VL_I).FI_LOAN_ID
                  );
            PA_RECORDS_ERROR :=PA_RECORDS_ERROR + CSL_1;

            VLTAB_ERRORS.EXTEND;
            VLTAB_ERRORS(VLTAB_ERRORS.LAST) :=
               SC_CREDIT.TYP_REC_BTC_ERROR(PTAB_LOANS(VL_I).FI_ADMIN_CENTER_ID
                                          ,PTAB_LOANS(VL_I).FI_LOAN_ID
                                          , UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP)
                                          ,SQLCODE
                                          ,SQLERRM
                                          ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                          ,SYSDATE
                                          ,CSL_0
                                          ,NULL);
      END;
      VL_I := PTAB_LOANS.NEXT(VL_I);
      COMMIT;
   END LOOP;

   COMMIT;

   VL_DESC_COUNT := ' TAB_LOANS '||VLTAB_LOANS.COUNT
                  ||' TAB_OPERATIONS '||VLTAB_OPERATIONS.COUNT
                  ||' TAB_OPERATIONS_DETAIL '||VLTAB_OPERATIONS_DETAIL.COUNT
                  ||' TAB_BALANCES '||VLTAB_BALANCES.COUNT
                  ||' TAB_BALANCES_DETAIL '||VLTAB_BALANCES_DETAIL.COUNT;
   --DELETE TYPES INTERNS
   VLTAB_STATUS_DETAIL.DELETE;
   VLTAB_LOANS.DELETE;
   VLTAB_OPERATIONS.DELETE;
   VLTAB_OPERATIONS_DETAIL.DELETE;
   VLTAB_BALANCES.DELETE;
   VLTAB_BALANCES_DETAIL.DELETE;

   PTAB_ERROR_RECORDS := VLTAB_ERRORS;
   IF(PA_RECORDS_ERROR > CSL_0)THEN
      PA_STATUS_CODE := CSL_1;
      PA_STATUS_MSG := CSL_SUCCESS_ERROR;
   END IF;
   VL_T2 := systimestamp;--TODO QUITAR
   PA_STATUS_MSG := PA_STATUS_MSG
      || ' ' || 'Elapsed Seconds: '||TO_CHAR(VL_T2-VL_T1, 'SSSS.FF')
      || ' ' || VL_DESC_COUNT;--TODO QUITAR

EXCEPTION
   WHEN OTHERS THEN
      PA_STATUS_CODE := SQLCODE;
      PA_STATUS_MSG := SQLERRM || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;

      SC_CREDIT.SP_BATCH_ERROR_LOG(UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP)
                                  ,SQLCODE
                                  ,SQLERRM
                                  ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                  ,CSL_0
                                  ,NULL);
END SP_BTC_EXE_CHANGE_DEFAULT;

/

  GRANT EXECUTE ON SC_CREDIT.SP_BTC_EXE_CHANGE_DEFAULT TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.SP_BTC_EXE_CHANGE_DEFAULT TO USRBTCCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_INS_LOAN_OP_TENDER
--------------------------------------------------------
set define off;

  CREATE OR REPLACE  PROCEDURE SC_CREDIT.SP_INS_LOAN_OP_TENDER (
  PA_LOAN_ID           IN SC_CREDIT.TA_LOAN_OPERATION_TENDER.FI_LOAN_ID%TYPE,
  PA_LOAN_OPERATION_ID IN SC_CREDIT.TA_LOAN_OPERATION_TENDER.FI_LOAN_OPERATION_ID%TYPE,
  PA_ADMIN_CENTER_ID   IN SC_CREDIT.TA_LOAN_OPERATION_TENDER.FI_ADMIN_CENTER_ID%TYPE,
  PA_TENDERS_AMOUNTS   IN VARCHAR2,
  PA_FC_USER           IN SC_CREDIT.TA_LOAN_OPERATION_TENDER.FC_USER%TYPE,
  PA_STATUS_MSG        OUT VARCHAR2,
  PA_STATUS_CODE       OUT NUMBER
)
/* **********************************************************************
 * PROJECT: CORE LOAN
 * DESCRIPTION: PROCEDURE FOR GETTING LOAN_OPERATION_ID CORRESPONDING
 *              TO PAYMENT IN CASH TO BE REVERSED.
 * PRECONDITIONS: PRE-EXISTING LOANS AND OPERATIONS
 * CREATED DATE: 09/12/2024
 * CREATOR: GILBERTO CHAVEZ MUNOZ
 * MODIFICATION: 2025-01-15 CESAR SANCHEZ HERNANDEZ
 * [NCPRDC-5152 V2.0.0]
 ***********************************************************************/
IS
  CSL_0            CONSTANT SIMPLE_INTEGER := 0;
  CSL_1            CONSTANT SIMPLE_INTEGER := 1;
  CSL_204          CONSTANT SIMPLE_INTEGER := 204;
  CSL_409          CONSTANT SIMPLE_INTEGER := 409;
  CSL_ARROW        CONSTANT VARCHAR2(5) := '->';
  CSL_JSON         CONSTANT VARCHAR2(5) := NULL;
  CSL_SUCCESS      CONSTANT VARCHAR2(8) := 'SUCCESS';
  CSL_NO_INSERTION CONSTANT VARCHAR2(20) := 'NO DATA INSERTED';
  CSL_DUPLICATE    CONSTANT VARCHAR2(50) := 'RECORD ALREADY EXISTS';
  CSL_SP           CONSTANT SIMPLE_INTEGER := 1;
  VL_TENDER_SEQ    NUMBER := 1;
  VL_EXISTE        NUMBER := 0;

BEGIN
  PA_STATUS_CODE := CSL_0;
  PA_STATUS_MSG  := CSL_SUCCESS;

FOR rec IN (
    SELECT VG_TENDER_TYPE_ID,
           VG_OPERATION_AMOUNT
    FROM JSON_TABLE(
      PA_TENDERS_AMOUNTS,
      '$[*]' COLUMNS (
        VG_TENDER_TYPE_ID    NUMBER PATH '$.id',
        VG_OPERATION_AMOUNT  NUMBER PATH '$.amount'
      )
    )
  ) LOOP


SELECT COUNT(1)
INTO VL_EXISTE
FROM SC_CREDIT.TA_LOAN_OPERATION_TENDER
WHERE FI_LOAN_ID = PA_LOAN_ID
  AND FI_ADMIN_CENTER_ID = PA_ADMIN_CENTER_ID
  AND FI_LOAN_OPERATION_ID = PA_LOAN_OPERATION_ID
  AND FI_OPERATION_TENDER_SEQ = VL_TENDER_SEQ
  AND FI_TENDER_TYPE_ID = rec.VG_TENDER_TYPE_ID;

IF VL_EXISTE > 0 THEN
        PA_STATUS_CODE := CSL_409;
        PA_STATUS_MSG := CSL_DUPLICATE;
ROLLBACK;
RETURN;
END IF;


INSERT INTO SC_CREDIT.TA_LOAN_OPERATION_TENDER
(FI_LOAN_OPERATION_ID, FI_ADMIN_CENTER_ID, FI_TENDER_TYPE_ID, FI_OPERATION_TENDER_SEQ, FN_OPERATION_AMOUNT, FI_STATUS, FC_USER, FD_CREATED_DATE, FD_MODIFICATION_DATE, FI_LOAN_ID)
VALUES
    (PA_LOAN_OPERATION_ID, PA_ADMIN_CENTER_ID, rec.VG_TENDER_TYPE_ID, VL_TENDER_SEQ, rec.VG_OPERATION_AMOUNT, CSL_1, PA_FC_USER, SYSDATE, SYSDATE, PA_LOAN_ID);

VL_TENDER_SEQ := VL_TENDER_SEQ + 1;
END LOOP;

COMMIT;

EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    PA_STATUS_CODE := CSL_204;
    PA_STATUS_MSG := SQLERRM || CSL_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;

    SC_CREDIT.SP_ERROR_LOG(
       UTL_CALL_STACK.SUBPROGRAM(CSL_1)(CSL_SP),
       SQLCODE,
       SQLERRM,
       DBMS_UTILITY.FORMAT_ERROR_BACKTRACE,
       NULL,
       CSL_JSON
    );
END SP_INS_LOAN_OP_TENDER;

/

  GRANT EXECUTE ON SC_CREDIT.SP_INS_LOAN_OP_TENDER TO USRNCPCREDIT1;
--------------------------------------------------------
--  DDL for Procedure SP_SYNC_PAYMENT_TYPE
--------------------------------------------------------
set define off;

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

  GRANT EXECUTE ON SC_CREDIT.SP_SYNC_PAYMENT_TYPE TO USRPURPOSEWS;
  GRANT EXECUTE ON SC_CREDIT.SP_SYNC_PAYMENT_TYPE TO USRNCPCREDIT1;

--------------------------------------------------------
--  DDL for Package PA_EXE_LOAN_DISBURSEMENT
--------------------------------------------------------

  CREATE OR REPLACE  PACKAGE SC_CREDIT.PA_EXE_LOAN_DISBURSEMENT 
  AS
  -- GLOBAL CONSTANTS
    CSG_ZERO                 CONSTANT SIMPLE_INTEGER := 0;
    CSG_ONE                  CONSTANT SIMPLE_INTEGER := 1;
    CSG_X                    CONSTANT VARCHAR2(3)    := 'X';
    CSG_ARROW                CONSTANT VARCHAR2(5)    := ' -> ';
    CSG_COLON                CONSTANT VARCHAR2(5)    := ' : ';
    CSG_FORMAT_DATE          CONSTANT VARCHAR2(30)   := 'YYYY-MM-DDTHH24:MI:SSTZH:TZM';
    CSG_CURRENT_DATE         CONSTANT DATE           := SYSDATE;
    CSG_CURRENT_USER         CONSTANT VARCHAR2(50)   := USER;
    CSG_SUCCESS_CODE         CONSTANT SIMPLE_INTEGER := 0;
    CSG_SUCCESS_MSG          CONSTANT VARCHAR2(50)   := 'SUCCESS';
    CSG_FOREING_KEY_CODE     CONSTANT SIMPLE_INTEGER := -2291;
    CSG_FOREING_KEY_MSG      CONSTANT VARCHAR2(50)   := 'THE FOREING KEY VIOLATED';
    CSG_PRIMARY_KEY_CODE     CONSTANT SIMPLE_INTEGER := -00001;
    CSG_PRIMARY_KEY_MSG      CONSTANT VARCHAR2(50)   := 'DATA DUPLICATED';
    CSG_DATA_NOT_SAVED_CODE  CONSTANT SIMPLE_INTEGER := -20400;
    CSG_DATA_NOT_SAVED_MSG   CONSTANT VARCHAR2(50)   := 'DATA NOT SAVED';

  -- PROCEDURE THAT ORQUESTS THE DISBURSEMENT
  PROCEDURE SP_EXE_LOAN_DISBURSEMENT (
    PA_DATA_LOAN_OPERATION           IN CLOB
    ,PA_DATA_LOAN_OPERATION_DETAIL   IN CLOB
    ,PA_DATA_LOAN_BALANCE            IN CLOB
    ,PA_DATA_LOAN_BALANCE_DETAIL     IN CLOB
    ,PA_DATA_LOAN_TENDER             IN CLOB
    ,PA_DATA_LOAN_STATUS             IN CLOB
    ,PA_DATA_LOAN                    IN CLOB
    ,PA_FINANCE_CHARGE_BALANCE       OUT NUMBER
    ,PA_SEQUENCE                     OUT NUMBER
    ,PA_LOAN_OPERATION_ID_OUT        OUT NUMBER
    ,PA_OPERATION_DATE_OUT           OUT VARCHAR2
    ,PA_STATUS_CODE                  OUT NUMBER
    ,PA_STATUS_MSG                   OUT VARCHAR2);

  -- PROCEDURE TO INSERT A LOAN OPERATION
  PROCEDURE SP_INS_LOAN_OPERATION(
    PA_DATA_LOAN_OPERATION    IN CLOB
    ,PA_LOAN_OPERATION_ID     IN NUMBER
    ,PA_STATUS_CODE           OUT NUMBER
    ,PA_STATUS_MSG            OUT VARCHAR2);

  -- PROCEDURE TO INSERT THE DETAILS OF A LOAN OPERATION
  PROCEDURE SP_INS_LOAN_OPERATION_DETAIL (
    PA_DATA_LOAN_OPERATION_DETAIL   IN CLOB
    ,PA_LOAN_OPERATION_ID           IN NUMBER
    ,PA_STATUS_CODE                 OUT NUMBER
    ,PA_STATUS_MSG                  OUT VARCHAR2);

  -- PROCEDURE TO INSERT A LOAN BALANCE
  PROCEDURE SP_INS_LOAN_BALANCE (
    PA_DATA_LOAN_BALANCE    IN CLOB
    ,PA_LOAN_BALANCE_ID     IN NUMBER
    ,PA_LOAN_OPERATION_ID   IN NUMBER
    ,PA_BALANCE_SEQ         OUT NUMBER
    ,PA_OLD_BALANCE_ID      OUT NUMBER
    ,PA_STATUS_CODE         OUT NUMBER
    ,PA_STATUS_MSG          OUT VARCHAR2);

  -- PROCEDURE TO INSERT THE DETAILS OF A LOAN BALANCE
  PROCEDURE SP_INS_LOAN_BALANCE_DETAIL (
    PA_DATA_LOAN_BALANCE_DETAIL IN CLOB
    ,PA_LOAN_BALANCE_ID         IN NUMBER
    ,PA_OLD_BALANCE_ID          IN NUMBER
    ,PA_STATUS_CODE             OUT NUMBER
    ,PA_STATUS_MSG              OUT VARCHAR2);

  -- PROCEDURE TO INSERT THE TENDERS OF THE OPERATION
  PROCEDURE SP_INS_LOAN_TENDER (
    PA_DATA_LOAN_TENDER   IN CLOB
    ,PA_LOAN_OPERATION_ID IN NUMBER
    ,PA_STATUS_CODE       OUT NUMBER
    ,PA_STATUS_MSG        OUT VARCHAR2);

  -- PROCEDURE TO INSERT THE LOAN STATUS
    PROCEDURE SP_INS_LOAN_STATUS (
    PA_DATA_LOAN_STATUS   IN CLOB
    ,PA_LOAN_OPERATION_ID IN NUMBER
    ,PA_STATUS_CODE       OUT NUMBER
    ,PA_STATUS_MSG        OUT VARCHAR2);

  -- PROCEDURE TO UPDATE THE LOAN STATUS AND PRINCIPAL BALANCE OF THE LOAN
  PROCEDURE SP_UPD_LOAN (
    PA_DATA_LOAN    IN CLOB
    ,PA_STATUS_CODE OUT NUMBER
    ,PA_STATUS_MSG  OUT VARCHAR2);

  END PA_EXE_LOAN_DISBURSEMENT;

/

  GRANT EXECUTE ON SC_CREDIT.PA_EXE_LOAN_DISBURSEMENT TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.PA_EXE_LOAN_DISBURSEMENT TO USRPURPOSEWS;
--------------------------------------------------------
--  DDL for Package PA_EXE_LOAN_ORIGINATION
--------------------------------------------------------

  CREATE OR REPLACE  PACKAGE SC_CREDIT.PA_EXE_LOAN_ORIGINATION 
  AS
    --CONSTANTS GLOB
    CSG_ZERO                 CONSTANT SIMPLE_INTEGER := 0;
    CSG_X                    CONSTANT VARCHAR2(1) := 'X';
    CSG_ARROW                CONSTANT VARCHAR2(5) := ' -> ';
    CSG_HYPHEN               CONSTANT VARCHAR2(5) := ' - ';
    CSG_COLON                CONSTANT VARCHAR2(5) := ' : ';
    CSG_SUCCESS_CODE         CONSTANT SIMPLE_INTEGER := 0;
    CSG_SUCCESS_MSG          CONSTANT VARCHAR2(10) := 'SUCCESS';
    CSG_ONE                  CONSTANT SIMPLE_INTEGER := 1;
    CSG_CURRENT_DATE         CONSTANT DATE := SYSDATE;
    CSG_CURRENT_USER         CONSTANT VARCHAR2(30) := USER;
    CSG_DATA_NOT_SAVED_CODE  CONSTANT SIMPLE_INTEGER := -20400;
    CSG_DATA_NOT_SAVED_MSG   CONSTANT VARCHAR2(50) := 'THE DATA NOT SAVED';
    CSG_FOREING_KEY_CODE     CONSTANT SIMPLE_INTEGER := -2291;
    CSG_FOREING_KEY_MSG      CONSTANT VARCHAR2(50) := 'THE FOREING KEY VIOLATED';
    CSG_PRIMARY_KEY_CODE     CONSTANT SIMPLE_INTEGER := -00001;
    CSG_PRIMARY_KEY_MSG      CONSTANT VARCHAR2(50) := 'DATA DUPLICATED';
    CSG_DUPLICATE_DATA_CODE  CONSTANT SIMPLE_INTEGER := -20409;
    CSG_DUPLICATE_DATA_MSG   CONSTANT VARCHAR2(50) := 'THE DATA ALREADY EXISTS';
    CSG_FORMAT_DATE          CONSTANT VARCHAR2(50) := 'YYYY-MM-DDTHH24:MI:SSTZH:TZM';

    --PROCEDURE TO ORQUEST THE ORIGINATION OF A LOAN
    PROCEDURE SP_EXE_LOAN_ORIGINATION(
    PA_LOAN_DATA                    IN CLOB,
    PA_PAYMENT_SCHEDULE_DATA        IN CLOB,
    PA_DATA_LOAN_OPERATION          IN CLOB,
    PA_DATA_LOAN_OPERATION_DETAIL   IN CLOB,
    PA_DATA_LOAN_BALANCE            IN CLOB,
    PA_DATA_LOAN_BALANCE_DETAIL     IN CLOB,
    PA_PAYMENT_SCHEDULE_FEE_DATA    IN CLOB,
    PA_STATUS_CODE                  OUT NUMBER,
    PA_STATUS_MSG                   OUT VARCHAR2);

     --PROCEDURE TO INSERT LOAN
    PROCEDURE SP_INS_LOAN (
    PA_LOAN_DATA                IN CLOB,
    PA_FINANCE_CHARGE_BALANCE   OUT NUMBER,
    PA_STATUS_CODE              OUT NUMBER,
    PA_STATUS_MSG               OUT VARCHAR2);

    --PROCEDURE TO INSERT PAYMENT SCHEDULE
    PROCEDURE SP_INS_PAYMENT_SCHEDULE (
    PA_PAYMENT_SCHEDULE_DATA    IN CLOB,
    PA_STATUS_CODE              OUT NUMBER,
    PA_STATUS_MSG               OUT VARCHAR2);

    --PROCEDURE TO INSERT OPERATION
    PROCEDURE SP_INS_LOAN_OPERATION(
    PA_DATA_LOAN_OPERATION    IN CLOB
    ,PA_LOAN_OPERATION_ID     IN NUMBER
    ,PA_STATUS_CODE           OUT NUMBER
    ,PA_STATUS_MSG            OUT VARCHAR2);

    --PROCEDURE TO INSERT OPERATION DETAIL
    PROCEDURE SP_INS_LOAN_OPERATION_DETAIL (
    PA_DATA_LOAN_OPERATION_DETAIL IN CLOB
    ,PA_LOAN_OPERATION_ID         IN NUMBER
    ,PA_STATUS_CODE               OUT NUMBER
    ,PA_STATUS_MSG                OUT VARCHAR2);

  --PROCEDURE TO INSERT BALANCE
  PROCEDURE SP_INS_LOAN_BALANCE (
    PA_DATA_LOAN_BALANCE    IN CLOB
    ,PA_LOAN_BALANCE_ID     IN NUMBER
    ,PA_LOAN_OPERATION_ID   IN NUMBER
    ,PA_STATUS_CODE         OUT NUMBER
    ,PA_STATUS_MSG          OUT VARCHAR2);

   --PROCEDURE TO INSERT BALANCE DETAIL
  PROCEDURE SP_INS_LOAN_BALANCE_DETAIL (
    PA_DATA_LOAN_BALANCE_DETAIL IN CLOB
    ,PA_LOAN_BALANCE_ID         IN NUMBER
    ,PA_STATUS_CODE             OUT NUMBER
    ,PA_STATUS_MSG              OUT VARCHAR2);

    --PROCEDURE TO INSERT PAYMENT SCHEDULE FEE
    PROCEDURE SP_INS_PAYMENT_SCHEDULE_FEE (
    PA_PAYMENT_SCHEDULE_FEE_DATA    IN CLOB,
    PA_STATUS_CODE                  OUT NUMBER,
    PA_STATUS_MSG                   OUT VARCHAR2);
  END PA_EXE_LOAN_ORIGINATION;

/

  GRANT EXECUTE ON SC_CREDIT.PA_EXE_LOAN_ORIGINATION TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.PA_EXE_LOAN_ORIGINATION TO USRPURPOSEWS;
--------------------------------------------------------
--  DDL for Package PA_EXE_OUTSTANDING_BALANCE
--------------------------------------------------------

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

  GRANT EXECUTE ON SC_CREDIT.PA_EXE_OUTSTANDING_BALANCE TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.PA_EXE_OUTSTANDING_BALANCE TO USRPURPOSEWS;
  GRANT EXECUTE ON SC_CREDIT.PA_EXE_OUTSTANDING_BALANCE TO USRCREDIT02;
--------------------------------------------------------
--  DDL for Package PA_LOAN_BALANCE
--------------------------------------------------------

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

  GRANT EXECUTE ON SC_CREDIT.PA_LOAN_BALANCE TO USRNCPCREDIT1;
--------------------------------------------------------
--  DDL for Package PA_LOAN_CANCEL
--------------------------------------------------------

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

  GRANT EXECUTE ON SC_CREDIT.PA_LOAN_CANCEL TO USRNCPCREDIT1;
--------------------------------------------------------
--  DDL for Package PA_MISCELLANEOUS_OPERATIONS
--------------------------------------------------------

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

  GRANT EXECUTE ON SC_CREDIT.PA_MISCELLANEOUS_OPERATIONS TO USRNCPCREDIT1;
--------------------------------------------------------
--  DDL for Package PA_TMP_LOAN_PROCESS
--------------------------------------------------------

  CREATE OR REPLACE  PACKAGE SC_CREDIT.PA_TMP_LOAN_PROCESS 
   AS

   /*
   *DESCRIPCION: PROCEDIMIENTO QUE REALIZA LAS OPERACIONES CRUD DE LA TABLA     TA_TMP_LOAN_PROCESS
   * PROJECT: LOAN LIFE CYCLE
   *CREADOR: IVAN LOPEZ
   *FECHA DE CREACION: 19/12/24
   */

   PROCEDURE SPTA_TMP_LOAN_PROCESS
      (
      PA_ACCION                IN NUMBER,
      PA_FI_LOAN_ID            IN     SC_CREDIT.TA_TMP_LOAN_PROCESS.FI_LOAN_ID%TYPE,
      PA_FI_ADMIN_CENTER_ID    IN     SC_CREDIT.TA_TMP_LOAN_PROCESS.FI_ADMIN_CENTER_ID%TYPE,
      PA_FI_PROCESS            IN     SC_CREDIT.TA_TMP_LOAN_PROCESS.FI_PROCESS%TYPE,
      PA_FC_USER               IN VARCHAR2,
      PA_FD_CREATED_DATE       IN VARCHAR2,
      PA_FD_MODIFICATION_DATE  IN VARCHAR2,
      PA_FI_TRACK              IN     SC_CREDIT.TA_TMP_LOAN_PROCESS.FI_TRACK%TYPE,
      PA_STATUS_CODE                OUT NUMBER,
      PA_STATUS_MSG           OUT VARCHAR2,
      PA_CURSOR                OUT   SC_CREDIT.PA_TYPES.TYP_CURSOR);

END PA_TMP_LOAN_PROCESS;

/

  GRANT EXECUTE ON SC_CREDIT.PA_TMP_LOAN_PROCESS TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.PA_TMP_LOAN_PROCESS TO USRCREDIT02;
--------------------------------------------------------
--  DDL for Package Body PA_EXE_LOAN_DISBURSEMENT
--------------------------------------------------------

  CREATE OR REPLACE  PACKAGE BODY SC_CREDIT.PA_EXE_LOAN_DISBURSEMENT 
  AS
  PROCEDURE SP_EXE_LOAN_DISBURSEMENT (
    PA_DATA_LOAN_OPERATION           IN CLOB
    ,PA_DATA_LOAN_OPERATION_DETAIL   IN CLOB
    ,PA_DATA_LOAN_BALANCE            IN CLOB
    ,PA_DATA_LOAN_BALANCE_DETAIL     IN CLOB
    ,PA_DATA_LOAN_TENDER             IN CLOB
    ,PA_DATA_LOAN_STATUS             IN CLOB
    ,PA_DATA_LOAN                    IN CLOB
    ,PA_FINANCE_CHARGE_BALANCE       OUT NUMBER
    ,PA_SEQUENCE                     OUT NUMBER
    ,PA_LOAN_OPERATION_ID_OUT        OUT NUMBER
    ,PA_OPERATION_DATE_OUT           OUT VARCHAR2
    ,PA_STATUS_CODE                  OUT NUMBER
    ,PA_STATUS_MSG                   OUT VARCHAR2)
  AS
/* **************************************************************
* PROJECT: NCP-OUTSTANDING BALANCE
* DESCRIPTION: PROCEDURE THAT ORQUEST THE PROCESS OF DISBURSEMENT
* CREATED DATE: 22/08/2024
* CREATOR: JOSE DE JESUS BRAVO AGUILAR / RICARDO HAZAEL GOMEZ ALVAREZ
* MODIFICATION DATE: 23/12/2024
************************************************************** */
  -- LOCAL CONSTANTS
    CSL_ISSUE_OPERATION_CODE        CONSTANT SIMPLE_INTEGER := -20050;
    CSL_ISSUE_OPERATION_MSG         CONSTANT VARCHAR2(50)   := 'ISSUE IN SP_INS_LOAN_OPERATION: ';
    CSL_ISSUE_OPERATION_DETAIL_CODE CONSTANT SIMPLE_INTEGER := -20060;
    CSL_ISSUE_OPERATION_DETAIL_MSG  CONSTANT VARCHAR2(50)   := 'ISSUE IN SP_INS_LOAN_OPERATION_DETAIL:';
    CSL_ISSUE_BALANCE_CODE          CONSTANT SIMPLE_INTEGER := -20070;
    CSL_ISSUE_BALANCE_MSG           CONSTANT VARCHAR2(50)   := 'ISSUE IN SP_INS_LOAN_BALANCE: ';
    CSL_ISSUE_BALANCE_DETAIL_CODE   CONSTANT SIMPLE_INTEGER := -20080;
    CSL_ISSUE_BALANCE_DETAIL_MSG    CONSTANT VARCHAR2(50)   := 'ISSUE IN SP_INS_LOAN_BALANCE_DETAIL: ';
    CSL_ISSUE_UPDATE_CODE           CONSTANT SIMPLE_INTEGER := -20090;
    CSL_ISSUE_UPDATE_MSG            CONSTANT VARCHAR2(50)   := 'ISSUE IN SP_UPD_LOAN: ';
    CSL_ISSUE_LOAN_TENDER_CODE      CONSTANT SIMPLE_INTEGER := -20100;
    CSL_ISSUE_LOAN_TENDER_MSG       CONSTANT VARCHAR2(50)   := 'ISSUE IN SP_INS_LOAN_TENDER: ';
    CSL_ISSUE_LOAN_STATUS_CODE      CONSTANT SIMPLE_INTEGER := -20110;
    CSL_ISSUE_LOAN_STATUS_MSG       CONSTANT VARCHAR2(50)   := 'ISSUE IN SP_INS_LOAN_STATUS: ';
    CSL_EXE_DISBURSEMENT            CONSTANT VARCHAR2(50)   := 'SP_EXE_LOAN_DISBURSEMENT';
  -- EXCEPTIONS
  EXC_ISSUE_OPERATION           EXCEPTION;
  PRAGMA EXCEPTION_INIT (EXC_ISSUE_OPERATION, CSL_ISSUE_OPERATION_CODE);
  EXC_ISSUE_OPERATION_DETAIL    EXCEPTION;
  PRAGMA EXCEPTION_INIT (EXC_ISSUE_OPERATION_DETAIL, CSL_ISSUE_OPERATION_DETAIL_CODE);
  EXC_ISSUE_BALANCE             EXCEPTION;
  PRAGMA EXCEPTION_INIT (EXC_ISSUE_BALANCE, CSL_ISSUE_BALANCE_CODE);
  EXC_ISSUE_BALANCE_DETAIL      EXCEPTION;
  PRAGMA EXCEPTION_INIT (EXC_ISSUE_BALANCE_DETAIL, CSL_ISSUE_BALANCE_DETAIL_CODE);
  EXC_ISSUE_LOAN_TENDER         EXCEPTION;
  PRAGMA EXCEPTION_INIT (EXC_ISSUE_LOAN_TENDER, CSL_ISSUE_LOAN_TENDER_CODE);
  EXC_ISSUE_LOAN_STATUS         EXCEPTION;
  PRAGMA EXCEPTION_INIT (EXC_ISSUE_LOAN_STATUS, CSL_ISSUE_LOAN_STATUS_CODE);
  EXC_ISSUE_UPDATE_LOAN         EXCEPTION;
  PRAGMA EXCEPTION_INIT (EXC_ISSUE_UPDATE_LOAN, CSL_ISSUE_UPDATE_CODE);
  -- VARIABLES
    VL_LOAN_OPERATION_ID        NUMBER(15);
    PA_LOAN_OPERATION_ID        NUMBER(15);
    VL_LOAN_BALANCE_ID          NUMBER(15);
    PA_LOAN_BALANCE_ID          NUMBER(15);
    PA_OLD_BALANCE_ID           NUMBER(15);
    VL_OPERATION_DATE           DATE;
    PA_BALANCE_SEQ              NUMBER(5);

  BEGIN
    VL_LOAN_OPERATION_ID    := SC_CREDIT.FN_GET_NEXT_LOAN_OPERATION_ID;
    VL_LOAN_BALANCE_ID      := SC_CREDIT.FN_GET_NEXT_LOAN_BALANCE_ID;
    PA_LOAN_OPERATION_ID    := VL_LOAN_OPERATION_ID;
    PA_LOAN_BALANCE_ID      := VL_LOAN_BALANCE_ID;

  BEGIN
    SP_INS_LOAN_OPERATION (
      PA_DATA_LOAN_OPERATION
      ,PA_LOAN_OPERATION_ID
      ,PA_STATUS_CODE
      ,PA_STATUS_MSG);
    IF PA_STATUS_CODE <> CSG_ZERO THEN
      RAISE EXC_ISSUE_OPERATION;
    END IF;

    SP_INS_LOAN_OPERATION_DETAIL (
      PA_DATA_LOAN_OPERATION_DETAIL
      ,PA_LOAN_OPERATION_ID
      ,PA_STATUS_CODE
      ,PA_STATUS_MSG);
    IF PA_STATUS_CODE <> CSG_ZERO THEN
      RAISE EXC_ISSUE_OPERATION_DETAIL;
    END IF;

    SP_INS_LOAN_BALANCE (
      PA_DATA_LOAN_BALANCE
      ,PA_LOAN_BALANCE_ID
      ,PA_LOAN_OPERATION_ID
      ,PA_BALANCE_SEQ
      ,PA_OLD_BALANCE_ID
      ,PA_STATUS_CODE
      ,PA_STATUS_MSG);
    IF PA_STATUS_CODE <> CSG_ZERO THEN
      RAISE EXC_ISSUE_BALANCE;
    END IF;

    SP_INS_LOAN_BALANCE_DETAIL (
      PA_DATA_LOAN_BALANCE_DETAIL
      ,PA_LOAN_BALANCE_ID
      ,PA_OLD_BALANCE_ID
      ,PA_STATUS_CODE
      ,PA_STATUS_MSG);
    IF PA_STATUS_CODE <> CSG_ZERO THEN
      RAISE EXC_ISSUE_BALANCE_DETAIL;
    END IF;

    SP_INS_LOAN_TENDER (
      PA_DATA_LOAN_TENDER
      ,PA_LOAN_OPERATION_ID
      ,PA_STATUS_CODE
      ,PA_STATUS_MSG);
    IF PA_STATUS_CODE <> CSG_ZERO THEN
      RAISE EXC_ISSUE_LOAN_TENDER;
    END IF;

    SP_INS_LOAN_STATUS (
      PA_DATA_LOAN_STATUS
      ,PA_LOAN_OPERATION_ID
      ,PA_STATUS_CODE
      ,PA_STATUS_MSG);
    IF PA_STATUS_CODE <> CSG_ZERO THEN
      RAISE EXC_ISSUE_LOAN_STATUS;
    END IF;

    SP_UPD_LOAN (
      PA_DATA_LOAN
      ,PA_STATUS_CODE
      ,PA_STATUS_MSG);
    IF PA_STATUS_CODE <> CSG_ZERO THEN
      RAISE EXC_ISSUE_UPDATE_LOAN;
    END IF;

  COMMIT;

    PA_SEQUENCE := PA_BALANCE_SEQ;

    SELECT FN_FINANCE_CHARGE_BALANCE INTO PA_FINANCE_CHARGE_BALANCE FROM SC_CREDIT.TA_LOAN_BALANCE
      WHERE FI_LOAN_OPERATION_ID = PA_LOAN_OPERATION_ID;

    SELECT FD_OPERATION_DATE INTO VL_OPERATION_DATE FROM SC_CREDIT.TA_LOAN_OPERATION
      WHERE FI_LOAN_OPERATION_ID = PA_LOAN_OPERATION_ID;

    PA_OPERATION_DATE_OUT := TO_CHAR(CAST(VL_OPERATION_DATE AS TIMESTAMP WITH TIME ZONE), CSG_FORMAT_DATE);
    PA_LOAN_OPERATION_ID_OUT := PA_LOAN_OPERATION_ID;

    PA_STATUS_CODE := CSG_SUCCESS_CODE;
    PA_STATUS_MSG  := CSG_SUCCESS_MSG;
  EXCEPTION
    WHEN EXC_ISSUE_OPERATION THEN
    ROLLBACK;
        PA_STATUS_MSG := CSL_ISSUE_OPERATION_MSG || CSL_ISSUE_OPERATION_CODE || CSG_ARROW || PA_STATUS_CODE ||
          CSG_COLON || PA_STATUS_MSG || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
        PA_STATUS_CODE := CSL_ISSUE_OPERATION_CODE;
        SC_CREDIT.SP_ERROR_LOG(CSL_EXE_DISBURSEMENT, SQLCODE, SQLERRM,
          DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, CSG_X);
    WHEN EXC_ISSUE_OPERATION_DETAIL THEN
    ROLLBACK;
      PA_STATUS_MSG := CSL_ISSUE_OPERATION_DETAIL_MSG || CSL_ISSUE_OPERATION_DETAIL_CODE  || CSG_ARROW ||
        PA_STATUS_CODE || CSG_COLON || PA_STATUS_MSG || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
      PA_STATUS_CODE := CSL_ISSUE_OPERATION_DETAIL_CODE;
      SC_CREDIT.SP_ERROR_LOG(CSL_EXE_DISBURSEMENT, SQLCODE, SQLERRM,
        DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, CSG_X);
    WHEN EXC_ISSUE_BALANCE THEN
    ROLLBACK;
      PA_STATUS_MSG := CSL_ISSUE_BALANCE_MSG || CSL_ISSUE_BALANCE_CODE || CSG_ARROW || PA_STATUS_CODE ||
        CSG_COLON || PA_STATUS_MSG || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
      PA_STATUS_CODE := CSL_ISSUE_BALANCE_CODE;
      SC_CREDIT.SP_ERROR_LOG(CSL_EXE_DISBURSEMENT, SQLCODE, SQLERRM,
        DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, CSG_X);
    WHEN EXC_ISSUE_BALANCE_DETAIL THEN
    ROLLBACK;
      PA_STATUS_MSG := CSL_ISSUE_BALANCE_DETAIL_MSG || CSL_ISSUE_BALANCE_DETAIL_CODE || CSG_ARROW ||
        PA_STATUS_CODE || CSG_COLON || PA_STATUS_MSG || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
      PA_STATUS_CODE := CSL_ISSUE_BALANCE_DETAIL_CODE;
      SC_CREDIT.SP_ERROR_LOG(CSL_EXE_DISBURSEMENT, SQLCODE, SQLERRM,
        DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, CSG_X);
    WHEN EXC_ISSUE_LOAN_TENDER THEN
    ROLLBACK;
      PA_STATUS_MSG := CSL_ISSUE_LOAN_TENDER_MSG || CSL_ISSUE_LOAN_TENDER_CODE  || CSG_ARROW ||
        PA_STATUS_CODE || CSG_COLON || PA_STATUS_MSG || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
      PA_STATUS_CODE := CSL_ISSUE_LOAN_TENDER_CODE;
      SC_CREDIT.SP_ERROR_LOG(CSL_EXE_DISBURSEMENT, SQLCODE, SQLERRM,
        DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, CSG_X);
    WHEN EXC_ISSUE_LOAN_STATUS THEN
    ROLLBACK;
      PA_STATUS_MSG := CSL_ISSUE_LOAN_STATUS_MSG || CSL_ISSUE_LOAN_STATUS_CODE  || CSG_ARROW ||
        PA_STATUS_CODE || CSG_COLON || PA_STATUS_MSG || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
      PA_STATUS_CODE := CSL_ISSUE_LOAN_STATUS_CODE;
      SC_CREDIT.SP_ERROR_LOG(CSL_EXE_DISBURSEMENT, SQLCODE, SQLERRM,
        DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, CSG_X);
    WHEN EXC_ISSUE_UPDATE_LOAN THEN
    ROLLBACK;
      PA_STATUS_MSG := CSL_ISSUE_UPDATE_MSG || CSL_ISSUE_UPDATE_CODE || CSG_ARROW || PA_STATUS_CODE ||
        CSG_COLON || PA_STATUS_MSG || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
      PA_STATUS_CODE := CSL_ISSUE_UPDATE_CODE;
      SC_CREDIT.SP_ERROR_LOG(CSL_EXE_DISBURSEMENT, SQLCODE, SQLERRM,
        DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, CSG_X);
    WHEN OTHERS THEN
    ROLLBACK;
      PA_STATUS_CODE := SQLCODE;
        PA_STATUS_MSG := SQLERRM || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
      SC_CREDIT.SP_ERROR_LOG(CSL_EXE_DISBURSEMENT, SQLCODE, SQLERRM,
        DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, CSG_X);
    END;
  END SP_EXE_LOAN_DISBURSEMENT;

  PROCEDURE SP_INS_LOAN_OPERATION (
    PA_DATA_LOAN_OPERATION    IN CLOB
    ,PA_LOAN_OPERATION_ID     IN NUMBER
    ,PA_STATUS_CODE           OUT NUMBER
    ,PA_STATUS_MSG            OUT VARCHAR2)
IS
/* **************************************************************
* PROJECT: NCP-OUTSTANDING BALANCE
* DESCRIPTION: PROCEDURE THAT INSERT THE LOAN OPERATION OF DISBURSEMENT
************************************************************** */
  -- CONSTANTS
  CSL_INSERT_OPERATION     CONSTANT VARCHAR2(50) := 'CSL_INSERT_OPERATION';
  -- VARIABLES
  VL_COUNTRY_ID             NUMBER(3);
  VL_COMPANY_ID             NUMBER(3);
  VL_BUSINESS_UNIT_ID       NUMBER(5);
  VL_LOAN_ID                NUMBER(15);
  VL_ADMIN_CENTER_ID        NUMBER(8);
  VL_OPERATION_TYPE_ID      NUMBER(5);
  VL_TRANSACTION            NUMBER(33);
  VL_PLATFORM_ID            VARCHAR2(6);
  VL_SUB_PLATFORM_ID        VARCHAR2(6);
  VL_OPERATION_AMOUNT       NUMBER(12,2);
  VL_APPLICATION_CHAR       VARCHAR2(30);
  VL_APPLICATION_DATE       DATE;
  VL_END_USER               VARCHAR2(10);
  VL_UUID_TRACKING          VARCHAR2(36);
  VL_GPS_LATITUDE           VARCHAR2(15);
  VL_GPS_LONGITUDE          VARCHAR2(15);
  VL_IP_ADDRESS             VARCHAR2(39);
  VL_DEVICE                 VARCHAR2(50);
  -- EXCEPTIONS
  EXC_FOREING_KEY_VIOLATION EXCEPTION;
  PRAGMA EXCEPTION_INIT(EXC_FOREING_KEY_VIOLATION, CSG_FOREING_KEY_CODE);
  EX_DUPLICATE_PK           EXCEPTION;
  PRAGMA EXCEPTION_INIT(EX_DUPLICATE_PK, CSG_PRIMARY_KEY_CODE);

  BEGIN
    PA_STATUS_CODE := CSG_SUCCESS_CODE;
    PA_STATUS_MSG  := CSG_SUCCESS_MSG;
    VL_COUNTRY_ID           := JSON_VALUE(PA_DATA_LOAN_OPERATION, '$.countryId');
    VL_COMPANY_ID           := JSON_VALUE(PA_DATA_LOAN_OPERATION, '$.companyId');
    VL_BUSINESS_UNIT_ID     := JSON_VALUE(PA_DATA_LOAN_OPERATION, '$.businessUnitId');
    VL_LOAN_ID              := JSON_VALUE(PA_DATA_LOAN_OPERATION, '$.loanId');
    VL_ADMIN_CENTER_ID      := JSON_VALUE(PA_DATA_LOAN_OPERATION, '$.adminCenterId');
    VL_OPERATION_TYPE_ID    := JSON_VALUE(PA_DATA_LOAN_OPERATION, '$.operationTypeId');
    VL_TRANSACTION          := JSON_VALUE(PA_DATA_LOAN_OPERATION, '$.transaction');
    VL_PLATFORM_ID          := JSON_VALUE(PA_DATA_LOAN_OPERATION, '$.platformId');
    VL_SUB_PLATFORM_ID      := JSON_VALUE(PA_DATA_LOAN_OPERATION, '$.subPlatformId');
    VL_OPERATION_AMOUNT     := JSON_VALUE(PA_DATA_LOAN_OPERATION, '$.operationAmount');
    VL_APPLICATION_CHAR     := JSON_VALUE(PA_DATA_LOAN_OPERATION, '$.applicationDate');
    VL_END_USER             := JSON_VALUE(PA_DATA_LOAN_OPERATION, '$.endUser');
    VL_UUID_TRACKING        := JSON_VALUE(PA_DATA_LOAN_OPERATION, '$.uuidTracking');
    VL_GPS_LATITUDE         := JSON_VALUE(PA_DATA_LOAN_OPERATION, '$.gpsLatitude');
    VL_GPS_LONGITUDE        := JSON_VALUE(PA_DATA_LOAN_OPERATION, '$.gpsLongitude');
    VL_IP_ADDRESS           := JSON_VALUE(PA_DATA_LOAN_OPERATION, '$.ipAddress');
    VL_DEVICE               := JSON_VALUE(PA_DATA_LOAN_OPERATION, '$.device');
    VL_APPLICATION_DATE     := CAST(TO_TIMESTAMP_TZ(VL_APPLICATION_CHAR, CSG_FORMAT_DATE) AS DATE);

      INSERT INTO SC_CREDIT.TA_LOAN_OPERATION (
        FI_LOAN_OPERATION_ID
        ,FI_COUNTRY_ID
        ,FI_COMPANY_ID
        ,FI_BUSINESS_UNIT_ID
        ,FI_LOAN_ID
        ,FI_ADMIN_CENTER_ID
        ,FI_OPERATION_TYPE_ID
        ,FI_TRANSACTION
        ,FC_PLATFORM_ID
        ,FC_SUB_PLATFORM_ID
        ,FN_OPERATION_AMOUNT
        ,FD_APPLICATION_DATE
        ,FD_OPERATION_DATE
        ,FI_STATUS
        ,FC_END_USER
        ,FC_UUID_TRACKING
        ,FC_GPS_LATITUDE
        ,FC_GPS_LONGITUDE
        ,FC_IP_ADDRESS
        ,FC_DEVICE
        ,FC_USER
        ,FD_CREATED_DATE
        ,FD_MODIFICATION_DATE)
      VALUES (
        PA_LOAN_OPERATION_ID
        ,VL_COUNTRY_ID
        ,VL_COMPANY_ID
        ,VL_BUSINESS_UNIT_ID
        ,VL_LOAN_ID
        ,VL_ADMIN_CENTER_ID
        ,VL_OPERATION_TYPE_ID
        ,VL_TRANSACTION
        ,VL_PLATFORM_ID
        ,VL_SUB_PLATFORM_ID
        ,VL_OPERATION_AMOUNT
        ,VL_APPLICATION_DATE
        ,CSG_CURRENT_DATE
        ,CSG_ONE
        ,VL_END_USER
        ,VL_UUID_TRACKING
        ,VL_GPS_LATITUDE
        ,VL_GPS_LONGITUDE
        ,VL_IP_ADDRESS
        ,VL_DEVICE
        ,CSG_CURRENT_USER
        ,CSG_CURRENT_DATE
        ,CSG_CURRENT_DATE);

  EXCEPTION
    WHEN EXC_FOREING_KEY_VIOLATION THEN
    ROLLBACK;
    PA_STATUS_CODE := CSG_FOREING_KEY_CODE;
    PA_STATUS_MSG := CSG_FOREING_KEY_MSG || CSG_ARROW ||SQLERRM|| DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_OPERATION, SQLCODE, SQLERRM,
      DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, VL_UUID_TRACKING, PA_DATA_LOAN_OPERATION);
    WHEN EX_DUPLICATE_PK THEN
    ROLLBACK;
    PA_STATUS_CODE := CSG_PRIMARY_KEY_CODE;
    PA_STATUS_MSG := CSG_PRIMARY_KEY_MSG || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_OPERATION, SQLCODE, SQLERRM,
      DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, VL_UUID_TRACKING, PA_DATA_LOAN_OPERATION);
    WHEN OTHERS THEN
    ROLLBACK;
      PA_STATUS_CODE := SQLCODE;
      PA_STATUS_MSG := SQLERRM || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
      SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_OPERATION, SQLCODE, SQLERRM,
        DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, VL_UUID_TRACKING, PA_DATA_LOAN_OPERATION);
  END SP_INS_LOAN_OPERATION;

  PROCEDURE SP_INS_LOAN_OPERATION_DETAIL (
    PA_DATA_LOAN_OPERATION_DETAIL               IN CLOB
    ,PA_LOAN_OPERATION_ID IN NUMBER
    ,PA_STATUS_CODE       OUT NUMBER
    ,PA_STATUS_MSG        OUT VARCHAR2)
  IS
/* **************************************************************
* PROJECT: NCP-OUTSTANDING BALANCE
* DESCRIPTION: PROCEDURE THAT INSERT THE DETAILS OF THE LOAN OPERATION OF DISBURSEMENT
************************************************************** */
  -- LOCAL CONSTANTS
  CSL_INSERT_OPERATION_DETAIL CONSTANT VARCHAR2(50) := 'SP_INS_LOAN_OPERATION_DETAIL';
  -- EXCEPTIONS
  EXC_DATA_NOT_SAVED        EXCEPTION;
  PRAGMA EXCEPTION_INIT(EXC_DATA_NOT_SAVED, CSG_DATA_NOT_SAVED_CODE);
  EXC_FOREING_KEY_VIOLATION EXCEPTION;
  PRAGMA EXCEPTION_INIT(EXC_FOREING_KEY_VIOLATION, CSG_FOREING_KEY_CODE);
  EX_DUPLICATE_PK           EXCEPTION;
  PRAGMA EXCEPTION_INIT(EX_DUPLICATE_PK, CSG_PRIMARY_KEY_CODE);
  -- VARIABLES
    VL_INSERT_COUNT         NUMBER(1) := CSG_ZERO;

  BEGIN
    FOR JSON_REC IN (
      SELECT
        FI_LOAN_ID
        ,FI_ADMIN_CENTER_ID
        ,FI_LOAN_CONCEPT_ID
        ,FN_ITEM_AMOUNT
      FROM JSON_TABLE(
        PA_DATA_LOAN_OPERATION_DETAIL
        ,'$[*]'
      COLUMNS (
        FI_LOAN_ID          NUMBER(15) PATH '$.loanId'
        ,FI_ADMIN_CENTER_ID NUMBER(8) PATH '$.adminCenterId'
        ,FI_LOAN_CONCEPT_ID NUMBER(5) PATH '$.loanConceptId'
        ,FN_ITEM_AMOUNT     NUMBER(12,2) PATH '$.itemAmount')))
      LOOP
      INSERT INTO SC_CREDIT.TA_LOAN_OPERATION_DETAIL (
        FI_LOAN_ID
        ,FI_LOAN_OPERATION_ID
        ,FI_ADMIN_CENTER_ID
        ,FI_LOAN_CONCEPT_ID
        ,FN_ITEM_AMOUNT
        ,FC_USER
        ,FD_CREATED_DATE
        ,FD_MODIFICATION_DATE)
      VALUES (
        JSON_REC.FI_LOAN_ID
        ,PA_LOAN_OPERATION_ID
        ,JSON_REC.FI_ADMIN_CENTER_ID
        ,JSON_REC.FI_LOAN_CONCEPT_ID
        ,JSON_REC.FN_ITEM_AMOUNT
        ,CSG_CURRENT_USER
        ,CSG_CURRENT_DATE
       ,CSG_CURRENT_DATE);

        VL_INSERT_COUNT := VL_INSERT_COUNT + CSG_ONE;
      END LOOP;

      IF VL_INSERT_COUNT = CSG_ZERO THEN
        RAISE EXC_DATA_NOT_SAVED;
      END IF;

    PA_STATUS_CODE := CSG_SUCCESS_CODE;
    PA_STATUS_MSG := CSG_SUCCESS_MSG;
  EXCEPTION
    WHEN EXC_FOREING_KEY_VIOLATION THEN
    ROLLBACK;
    PA_STATUS_CODE := CSG_FOREING_KEY_CODE;
    PA_STATUS_MSG := CSG_FOREING_KEY_MSG || CSG_ARROW ||SQLERRM|| DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_OPERATION_DETAIL, SQLCODE, SQLERRM,
      DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, PA_DATA_LOAN_OPERATION_DETAIL);
    WHEN EX_DUPLICATE_PK THEN
    ROLLBACK;
    PA_STATUS_CODE := CSG_PRIMARY_KEY_CODE;
    PA_STATUS_MSG := CSG_PRIMARY_KEY_MSG || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_OPERATION_DETAIL, SQLCODE, SQLERRM,
      DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, PA_DATA_LOAN_OPERATION_DETAIL);
    WHEN EXC_DATA_NOT_SAVED THEN
    ROLLBACK;
      PA_STATUS_CODE := CSG_DATA_NOT_SAVED_CODE;
      PA_STATUS_MSG := CSG_DATA_NOT_SAVED_MSG || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
      SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_OPERATION_DETAIL, SQLCODE, SQLERRM,
        DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, PA_DATA_LOAN_OPERATION_DETAIL);
    WHEN OTHERS THEN
    ROLLBACK;
      PA_STATUS_CODE := SQLCODE;
      PA_STATUS_MSG := SQLERRM || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
      SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_OPERATION_DETAIL, SQLCODE, SQLERRM,
        DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, PA_DATA_LOAN_OPERATION_DETAIL);
  END SP_INS_LOAN_OPERATION_DETAIL;

   PROCEDURE SP_INS_LOAN_BALANCE (
    PA_DATA_LOAN_BALANCE   IN CLOB
    ,PA_LOAN_BALANCE_ID    IN NUMBER
    ,PA_LOAN_OPERATION_ID  IN NUMBER
    ,PA_BALANCE_SEQ        OUT NUMBER
    ,PA_OLD_BALANCE_ID     OUT NUMBER
    ,PA_STATUS_CODE        OUT NUMBER
    ,PA_STATUS_MSG         OUT VARCHAR2)
  IS
/* **************************************************************
* PROJECT: NCP-OUTSTANDING BALANCE
* DESCRIPTION: PROCEDURE THAT INSERT THE LOAN BALANCE OF THE DISBURSEMENT
************************************************************** */
  -- CONSTANTS
  CSL_INSERT_BALANCE            CONSTANT VARCHAR2(50) := 'SP_INS_LOAN_BALANCE';
  -- VARIABLES
  VL_ADMIN_CENTER_ID            NUMBER(8);
  VL_LOAN_ID                    NUMBER(15);
  VL_PRINCIPAL_BALANCE          NUMBER(12,2);
  VL_ADDITIONAL_CHARGE_BALANCE  NUMBER(12,2);
  VL_BALANCE_SEQ                NUMBER(5);
  VL_ROW_BALANCE                NUMBER(3);
  VL_FINANCE_CHARGE_BALANCE     NUMBER(12,2);
  VL_OLD_BALANCE_ID             NUMBER(15);
  -- EXCEPTIONS
  EXC_FOREING_KEY_VIOLATION EXCEPTION;
  PRAGMA EXCEPTION_INIT(EXC_FOREING_KEY_VIOLATION, CSG_FOREING_KEY_CODE);
  EX_DUPLICATE_PK           EXCEPTION;
  PRAGMA EXCEPTION_INIT(EX_DUPLICATE_PK, CSG_PRIMARY_KEY_CODE);

  BEGIN
    PA_STATUS_CODE := CSG_SUCCESS_CODE;
    PA_STATUS_MSG  := CSG_SUCCESS_MSG;
    VL_ADMIN_CENTER_ID              := JSON_VALUE(PA_DATA_LOAN_BALANCE, '$.adminCenterId');
    VL_LOAN_ID                      := JSON_VALUE(PA_DATA_LOAN_BALANCE, '$.loanId');
    VL_PRINCIPAL_BALANCE            := JSON_VALUE(PA_DATA_LOAN_BALANCE, '$.principalBalance');
    VL_ADDITIONAL_CHARGE_BALANCE    := JSON_VALUE(PA_DATA_LOAN_BALANCE, '$.additionalChargeBalance');

    SELECT NVL(MAX(FI_BALANCE_SEQ), CSG_ZERO) + CSG_ONE
      INTO VL_BALANCE_SEQ
      FROM SC_CREDIT.TA_LOAN_BALANCE
      WHERE FI_LOAN_ID = VL_LOAN_ID
      AND FI_ADMIN_CENTER_ID = VL_ADMIN_CENTER_ID;

      PA_BALANCE_SEQ := VL_BALANCE_SEQ;

    SELECT COUNT (*) INTO VL_ROW_BALANCE
      FROM SC_CREDIT.TA_LOAN_BALANCE
      WHERE FI_LOAN_ID = VL_LOAN_ID
      AND FI_ADMIN_CENTER_ID = VL_ADMIN_CENTER_ID;
      IF VL_ROW_BALANCE > CSG_ZERO THEN

        SELECT FN_FINANCE_CHARGE_BALANCE, FI_LOAN_BALANCE_ID
            INTO VL_FINANCE_CHARGE_BALANCE, VL_OLD_BALANCE_ID
            FROM SC_CREDIT.TA_LOAN_BALANCE
            WHERE FI_LOAN_ID = VL_LOAN_ID
            AND FI_ADMIN_CENTER_ID = VL_ADMIN_CENTER_ID
            AND ROWNUM = CSG_ONE;

            PA_OLD_BALANCE_ID := VL_OLD_BALANCE_ID;

      ELSE
        VL_FINANCE_CHARGE_BALANCE := CSG_ZERO;
        VL_OLD_BALANCE_ID := CSG_ZERO;
        PA_OLD_BALANCE_ID := VL_OLD_BALANCE_ID;
      END IF;

      INSERT INTO SC_CREDIT.TA_LOAN_BALANCE (
        FI_LOAN_BALANCE_ID
        ,FI_ADMIN_CENTER_ID
        ,FI_LOAN_ID
        ,FI_LOAN_OPERATION_ID
        ,FI_BALANCE_SEQ
        ,FN_PRINCIPAL_BALANCE
        ,FN_FINANCE_CHARGE_BALANCE
        ,FN_ADDITIONAL_CHARGE_BALANCE
        ,FC_USER
        ,FD_CREATED_DATE
        ,FD_MODIFICATION_DATE)
      VALUES (
        PA_LOAN_BALANCE_ID
        ,VL_ADMIN_CENTER_ID
        ,VL_LOAN_ID
        ,PA_LOAN_OPERATION_ID
        ,VL_BALANCE_SEQ
        ,VL_PRINCIPAL_BALANCE
        ,VL_FINANCE_CHARGE_BALANCE
        ,VL_ADDITIONAL_CHARGE_BALANCE
        ,CSG_CURRENT_USER
        ,CSG_CURRENT_DATE
        ,CSG_CURRENT_DATE);

  EXCEPTION
    WHEN EXC_FOREING_KEY_VIOLATION THEN
    ROLLBACK;
    PA_STATUS_CODE := CSG_FOREING_KEY_CODE;
    PA_STATUS_MSG := CSG_FOREING_KEY_MSG || CSG_ARROW ||SQLERRM|| DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_BALANCE, SQLCODE, SQLERRM,
      DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, PA_DATA_LOAN_BALANCE);
    WHEN EX_DUPLICATE_PK THEN
    ROLLBACK;
    PA_STATUS_CODE := CSG_PRIMARY_KEY_CODE;
    PA_STATUS_MSG := CSG_PRIMARY_KEY_MSG || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_BALANCE, SQLCODE, SQLERRM,
      DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, PA_DATA_LOAN_BALANCE);
    WHEN OTHERS THEN
    ROLLBACK;
      PA_STATUS_CODE := SQLCODE;
      PA_STATUS_MSG := SQLERRM || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
      SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_BALANCE, SQLCODE, SQLERRM,
        DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, PA_DATA_LOAN_BALANCE);
  END SP_INS_LOAN_BALANCE;

  PROCEDURE SP_INS_LOAN_BALANCE_DETAIL (
    PA_DATA_LOAN_BALANCE_DETAIL   IN CLOB,
    PA_LOAN_BALANCE_ID            IN NUMBER,
    PA_OLD_BALANCE_ID             IN NUMBER,
    PA_STATUS_CODE                OUT NUMBER,
    PA_STATUS_MSG                 OUT VARCHAR2)
  IS
/* **************************************************************
* PROJECT: NCP-OUTSTANDING BALANCE
* DESCRIPTION: PROCEDURE THAT INSERT THE DETAILS OF THE LOAN BALANCE OF DISBURSEMENT
************************************************************** */
  -- CONSTANTS
  CSL_INSERT_BALANCE_DETAIL CONSTANT VARCHAR2(50) := 'SP_INS_LOAN_BALANCE_DETAIL';
  -- EXCEPTIONS
  EXC_DATA_NOT_SAVED        EXCEPTION;
  PRAGMA EXCEPTION_INIT(EXC_DATA_NOT_SAVED, CSG_DATA_NOT_SAVED_CODE);
  EXC_FOREIGN_KEY_VIOLATION EXCEPTION;
  PRAGMA EXCEPTION_INIT(EXC_FOREIGN_KEY_VIOLATION, CSG_FOREING_KEY_CODE);
  EX_DUPLICATE_PK           EXCEPTION;
  PRAGMA EXCEPTION_INIT(EX_DUPLICATE_PK, CSG_PRIMARY_KEY_CODE);
  -- VARIABLES
  VL_INSERT_COUNT           NUMBER(3) := CSG_ZERO;

  BEGIN
  FOR JSON_REC IN (
    SELECT
      FI_LOAN_ID
      ,FI_ADMIN_CENTER_ID
      ,FI_LOAN_CONCEPT_ID
      ,FN_ITEM_AMOUNT
    FROM JSON_TABLE(
      PA_DATA_LOAN_BALANCE_DETAIL,
      '$[*]'
      COLUMNS (
        FI_LOAN_ID           NUMBER(15) PATH '$.loanId'
        ,FI_ADMIN_CENTER_ID  NUMBER(8) PATH '$.adminCenterId'
        ,FI_LOAN_CONCEPT_ID  NUMBER(5) PATH '$.loanConceptId'
        ,FN_ITEM_AMOUNT      NUMBER(12,2) PATH '$.itemAmount')))
      LOOP
    INSERT INTO SC_CREDIT.TA_LOAN_BALANCE_DETAIL (
      FI_LOAN_ID
      ,FI_LOAN_BALANCE_ID
      ,FI_ADMIN_CENTER_ID
      ,FI_LOAN_CONCEPT_ID
      ,FN_ITEM_AMOUNT
      ,FC_USER
      ,FD_CREATED_DATE
      ,FD_MODIFICATION_DATE)
    VALUES (
      JSON_REC.FI_LOAN_ID
      ,PA_LOAN_BALANCE_ID
      ,JSON_REC.FI_ADMIN_CENTER_ID
      ,JSON_REC.FI_LOAN_CONCEPT_ID
      ,JSON_REC.FN_ITEM_AMOUNT
      ,CSG_CURRENT_USER
      ,CSG_CURRENT_DATE
      ,CSG_CURRENT_DATE);

    VL_INSERT_COUNT := VL_INSERT_COUNT + CSG_ONE;
  END LOOP;

  IF VL_INSERT_COUNT = CSG_ZERO THEN
    RAISE EXC_DATA_NOT_SAVED;
  END IF;

  FOR LBD_REC IN (
    SELECT
        LBD.FI_LOAN_ID
        ,LBD.FI_ADMIN_CENTER_ID
        ,LBD.FI_LOAN_CONCEPT_ID
        ,LBD.FN_ITEM_AMOUNT
    FROM SC_CREDIT.TA_LOAN_BALANCE_DETAIL LBD
    WHERE LBD.FI_LOAN_BALANCE_ID = PA_OLD_BALANCE_ID)
      LOOP
    INSERT INTO SC_CREDIT.TA_LOAN_BALANCE_DETAIL (
        FI_LOAN_ID
        ,FI_LOAN_BALANCE_ID
        ,FI_ADMIN_CENTER_ID
        ,FI_LOAN_CONCEPT_ID
        ,FN_ITEM_AMOUNT
        ,FC_USER
        ,FD_CREATED_DATE
        ,FD_MODIFICATION_DATE)
      VALUES (
        LBD_REC.FI_LOAN_ID
        ,PA_LOAN_BALANCE_ID
        ,LBD_REC.FI_ADMIN_CENTER_ID
        ,LBD_REC.FI_LOAN_CONCEPT_ID
        ,LBD_REC.FN_ITEM_AMOUNT
        ,CSG_CURRENT_USER
        ,CSG_CURRENT_DATE
        ,CSG_CURRENT_DATE);
  END LOOP;

  PA_STATUS_CODE := CSG_SUCCESS_CODE;
  PA_STATUS_MSG := CSG_SUCCESS_MSG;
  EXCEPTION
  WHEN EXC_FOREIGN_KEY_VIOLATION THEN
    ROLLBACK;
    PA_STATUS_CODE := CSG_FOREING_KEY_CODE;
    PA_STATUS_MSG := CSG_FOREING_KEY_MSG || ' -> ' || SQLERRM || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_BALANCE_DETAIL, SQLCODE, SQLERRM,
      DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, PA_DATA_LOAN_BALANCE_DETAIL);
  WHEN EX_DUPLICATE_PK THEN
    ROLLBACK;
    PA_STATUS_CODE := CSG_PRIMARY_KEY_CODE;
    PA_STATUS_MSG := CSG_PRIMARY_KEY_MSG || ' -> ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_BALANCE_DETAIL, SQLCODE, SQLERRM,
      DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, PA_DATA_LOAN_BALANCE_DETAIL);
  WHEN EXC_DATA_NOT_SAVED THEN
    ROLLBACK;
    PA_STATUS_CODE := CSG_DATA_NOT_SAVED_CODE;
    PA_STATUS_MSG := CSG_DATA_NOT_SAVED_MSG || ' -> ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_BALANCE_DETAIL, SQLCODE, SQLERRM,
      DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, PA_DATA_LOAN_BALANCE_DETAIL);
  WHEN OTHERS THEN
    ROLLBACK;
    PA_STATUS_CODE := SQLCODE;
    PA_STATUS_MSG := SQLERRM || ' -> ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_BALANCE_DETAIL, SQLCODE, SQLERRM,
      DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, PA_DATA_LOAN_BALANCE_DETAIL);
  END SP_INS_LOAN_BALANCE_DETAIL;

  PROCEDURE SP_INS_LOAN_TENDER (
    PA_DATA_LOAN_TENDER   IN CLOB
    ,PA_LOAN_OPERATION_ID IN NUMBER
    ,PA_STATUS_CODE       OUT NUMBER
    ,PA_STATUS_MSG        OUT VARCHAR2)
  IS
/* **************************************************************
* PROJECT: NCP-OUTSTANDING BALANCE
* DESCRIPTION: PROCEDURE THAT INSERT THE TENDERS OF THE DISBURSEMENT
************************************************************** */
  -- CONSTANTS
  CSL_INSERT_LOAN_TENDER    CONSTANT VARCHAR2(50) := 'SP_INS_LOAN_OPERATION_TENDER';
  -- EXCEPTIONS
  EXC_DATA_NOT_SAVED        EXCEPTION;
  PRAGMA EXCEPTION_INIT(EXC_DATA_NOT_SAVED, CSG_DATA_NOT_SAVED_CODE);
  EXC_FOREING_KEY_VIOLATION EXCEPTION;
  PRAGMA EXCEPTION_INIT(EXC_FOREING_KEY_VIOLATION, CSG_FOREING_KEY_CODE);
  EX_DUPLICATE_PK           EXCEPTION;
  PRAGMA EXCEPTION_INIT(EX_DUPLICATE_PK, CSG_PRIMARY_KEY_CODE);
  -- VARIABLES
  VL_INSERT_COUNT           NUMBER(3) := CSG_ZERO;
  VL_OPERATION_TENDER_SEQ   NUMBER(5);

  BEGIN
    FOR JSON_REC IN (
      SELECT
        FI_LOAN_ID
        ,FI_ADMIN_CENTER_ID
        ,FI_TENDER_TYPE_ID
        ,FN_OPERATION_AMOUNT
        ,FI_STATUS
      FROM JSON_TABLE(
        PA_DATA_LOAN_TENDER
        ,'$[*]'
      COLUMNS (
        FI_LOAN_ID              NUMBER(15) PATH '$.loanId'
        ,FI_ADMIN_CENTER_ID     NUMBER(8) PATH '$.adminCenterId'
        ,FI_TENDER_TYPE_ID      NUMBER (4) PATH '$.tenderTypeId'
        ,FN_OPERATION_AMOUNT    NUMBER(12,2) PATH '$.operationAmount'
        ,FI_STATUS              NUMBER (2) PATH '$.status')))
      LOOP

      SELECT NVL(MAX(FI_OPERATION_TENDER_SEQ), CSG_ZERO) + CSG_ONE
        INTO VL_OPERATION_TENDER_SEQ
        FROM SC_CREDIT.TA_LOAN_OPERATION_TENDER
        WHERE FI_LOAN_OPERATION_ID = PA_LOAN_OPERATION_ID
        AND FI_ADMIN_CENTER_ID = JSON_REC.FI_ADMIN_CENTER_ID
        AND FI_LOAN_ID = JSON_REC.FI_LOAN_ID;

      INSERT INTO SC_CREDIT.TA_LOAN_OPERATION_TENDER (
        FI_LOAN_ID
        ,FI_LOAN_OPERATION_ID
        ,FI_ADMIN_CENTER_ID
        ,FI_TENDER_TYPE_ID
        ,FI_OPERATION_TENDER_SEQ
        ,FN_OPERATION_AMOUNT
        ,FI_STATUS
        ,FC_USER
        ,FD_CREATED_DATE
        ,FD_MODIFICATION_DATE)
      VALUES (
        JSON_REC.FI_LOAN_ID
        ,PA_LOAN_OPERATION_ID
        ,JSON_REC.FI_ADMIN_CENTER_ID
        ,JSON_REC.FI_TENDER_TYPE_ID
        ,VL_OPERATION_TENDER_SEQ
        ,JSON_REC.FN_OPERATION_AMOUNT
        ,JSON_REC.FI_STATUS
        ,CSG_CURRENT_USER
        ,CSG_CURRENT_DATE
        ,CSG_CURRENT_DATE);

        VL_INSERT_COUNT := VL_INSERT_COUNT + CSG_ONE;
      END LOOP;

      IF VL_INSERT_COUNT = CSG_ZERO THEN
        RAISE EXC_DATA_NOT_SAVED;
      END IF;

    PA_STATUS_CODE := CSG_SUCCESS_CODE;
    PA_STATUS_MSG := CSG_SUCCESS_MSG;
  EXCEPTION
    WHEN EXC_FOREING_KEY_VIOLATION THEN
    ROLLBACK;
    PA_STATUS_CODE := CSG_FOREING_KEY_CODE;
    PA_STATUS_MSG := CSG_FOREING_KEY_MSG || CSG_ARROW ||SQLERRM|| DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_LOAN_TENDER, SQLCODE, SQLERRM,
      DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, PA_DATA_LOAN_TENDER);
    WHEN EX_DUPLICATE_PK THEN
    ROLLBACK;
    PA_STATUS_CODE := CSG_PRIMARY_KEY_CODE;
    PA_STATUS_MSG := CSG_PRIMARY_KEY_MSG || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_LOAN_TENDER, SQLCODE, SQLERRM,
      DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, PA_DATA_LOAN_TENDER);
    WHEN EXC_DATA_NOT_SAVED THEN
    ROLLBACK;
      PA_STATUS_CODE := CSG_DATA_NOT_SAVED_CODE;
      PA_STATUS_MSG := CSG_DATA_NOT_SAVED_MSG || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
      SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_LOAN_TENDER, SQLCODE, SQLERRM,
        DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, PA_DATA_LOAN_TENDER);
    WHEN OTHERS THEN
    ROLLBACK;
      PA_STATUS_CODE := SQLCODE;
      PA_STATUS_MSG := SQLERRM || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
      SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_LOAN_TENDER, SQLCODE, SQLERRM,
        DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, PA_DATA_LOAN_TENDER);
  END SP_INS_LOAN_TENDER;

  PROCEDURE SP_INS_LOAN_STATUS (
    PA_DATA_LOAN_STATUS   IN CLOB
    ,PA_LOAN_OPERATION_ID IN NUMBER
    ,PA_STATUS_CODE       OUT NUMBER
    ,PA_STATUS_MSG        OUT VARCHAR2)
  IS
/* **************************************************************
* PROJECT: NCP-OUTSTANDING BALANCE
* DESCRIPTION: PROCEDURE THAT INSERT THE LOAN STATUS OF DISBURSEMENT
************************************************************** */
  -- CONSTANTS
  CSL_INSERT_LOAN_STATUS   CONSTANT VARCHAR2(50) := 'SP_INS_LOAN_STATUS';
  -- VARIABLES
  VL_LOAN_ID                NUMBER(15);
  VL_ADMIN_CENTER_ID        NUMBER(8);
  VL_LOAN_STATUS_ID         NUMBER(5);
  VL_LOAN_STATUS_OLD_ID     NUMBER(5);
  VL_TRIGGER_ID             NUMBER(3);
  -- EXCEPTIONS
  EXC_FOREING_KEY_VIOLATION EXCEPTION;
  PRAGMA EXCEPTION_INIT(EXC_FOREING_KEY_VIOLATION, CSG_FOREING_KEY_CODE);
  EX_DUPLICATE_PK           EXCEPTION;
  PRAGMA EXCEPTION_INIT(EX_DUPLICATE_PK, CSG_PRIMARY_KEY_CODE);

  BEGIN
    PA_STATUS_CODE := CSG_SUCCESS_CODE;
    PA_STATUS_MSG  := CSG_SUCCESS_MSG;
    VL_LOAN_ID            := JSON_VALUE(PA_DATA_LOAN_STATUS, '$.loanId');
    VL_ADMIN_CENTER_ID    := JSON_VALUE(PA_DATA_LOAN_STATUS, '$.adminCenterId');
    VL_LOAN_STATUS_ID     := JSON_VALUE(PA_DATA_LOAN_STATUS, '$.loanStatusId');
    VL_LOAN_STATUS_OLD_ID := JSON_VALUE(PA_DATA_LOAN_STATUS, '$.loanStatusOldId');
    VL_TRIGGER_ID         := JSON_VALUE(PA_DATA_LOAN_STATUS, '$.triggerId');

      INSERT INTO SC_CREDIT.TA_LOAN_STATUS (
        FI_LOAN_ID
        ,FI_ADMIN_CENTER_ID
        ,FI_LOAN_OPERATION_ID
        ,FI_LOAN_STATUS_ID
        ,FI_LOAN_STATUS_OLD_ID
        ,FI_TRIGGER_ID
        ,FD_LOAN_STATUS_DATE
        ,FC_USER
        ,FD_CREATED_DATE
        ,FD_MODIFICATION_DATE)
      VALUES (
        VL_LOAN_ID
        ,VL_ADMIN_CENTER_ID
        ,PA_LOAN_OPERATION_ID
        ,VL_LOAN_STATUS_ID
        ,VL_LOAN_STATUS_OLD_ID
        ,VL_TRIGGER_ID
        ,CSG_CURRENT_DATE
        ,CSG_CURRENT_USER
        ,CSG_CURRENT_DATE
        ,CSG_CURRENT_DATE);

  EXCEPTION
    WHEN EXC_FOREING_KEY_VIOLATION THEN
    ROLLBACK;
    PA_STATUS_CODE := CSG_FOREING_KEY_CODE;
    PA_STATUS_MSG := CSG_FOREING_KEY_MSG || CSG_ARROW ||SQLERRM|| DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_LOAN_STATUS, SQLCODE, SQLERRM,
      DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, PA_DATA_LOAN_STATUS);
    WHEN EX_DUPLICATE_PK THEN
    ROLLBACK;
    PA_STATUS_CODE := CSG_PRIMARY_KEY_CODE;
    PA_STATUS_MSG := CSG_PRIMARY_KEY_MSG || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_LOAN_STATUS, SQLCODE, SQLERRM,
      DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, PA_DATA_LOAN_STATUS);
    WHEN OTHERS THEN
    ROLLBACK;
      PA_STATUS_CODE := SQLCODE;
      PA_STATUS_MSG := SQLERRM || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
      SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_LOAN_STATUS, SQLCODE, SQLERRM,
        DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, PA_DATA_LOAN_STATUS);
  END SP_INS_LOAN_STATUS;

  PROCEDURE SP_UPD_LOAN (
    PA_DATA_LOAN    IN CLOB
    ,PA_STATUS_CODE OUT NUMBER
    ,PA_STATUS_MSG  OUT VARCHAR2)
  IS
/* **************************************************************
* PROJECT: NCP-OUTSTANDING BALANCE
* DESCRIPTION: PROCEDURE THAT UPDATE THE LOAN STATUS, SEQUENCE AND PRINCIPAL BALANCE OF THE LOAN.
************************************************************** */
  -- CONSTANTS
  CSL_UPDATE_FAILED_CODE    CONSTANT SIMPLE_INTEGER := -20304;
  CSL_UPDATE_FAILED_MSG     CONSTANT VARCHAR2(50) := 'FAILED TO UPDATE';
  CSL_UPDATE_LOAN           CONSTANT VARCHAR2(50) := 'SP_UPD_LOAN';
  -- EXCEPTIONS
  EXC_UPDATE_FAILED         EXCEPTION;
  PRAGMA EXCEPTION_INIT(EXC_UPDATE_FAILED, CSL_UPDATE_FAILED_CODE);
  -- VARIABLES
  VL_NEW_BALANCE_SEQ        NUMBER(5);

  BEGIN
    FOR JSON_REC IN (
      SELECT
        FI_LOAN_ID
        ,FI_ADMIN_CENTER_ID
        ,FI_LOAN_STATUS_ID
        ,FN_PRINCIPAL_BALANCE
      FROM JSON_TABLE (
        PA_DATA_LOAN
        ,'$[*]'
      COLUMNS (
        FI_LOAN_ID            NUMBER(15) PATH '$.loanId'
        ,FI_ADMIN_CENTER_ID   NUMBER(5) PATH '$.adminCenterId'
        ,FI_LOAN_STATUS_ID    NUMBER(5) PATH '$.loanStatusId'
        ,FN_PRINCIPAL_BALANCE NUMBER(12,2) PATH '$.principalBalance')))
      LOOP
      SELECT NVL(MAX(FI_CURRENT_BALANCE_SEQ), CSG_ZERO) + CSG_ONE
        INTO VL_NEW_BALANCE_SEQ
        FROM SC_CREDIT.TA_LOAN
        WHERE FI_LOAN_ID = JSON_REC.FI_LOAN_ID
        AND FI_ADMIN_CENTER_ID = JSON_REC.FI_ADMIN_CENTER_ID;
      UPDATE SC_CREDIT.TA_LOAN
        SET FI_LOAN_STATUS_ID = JSON_REC.FI_LOAN_STATUS_ID
        ,FI_CURRENT_BALANCE_SEQ = VL_NEW_BALANCE_SEQ
        ,FD_LOAN_STATUS_DATE = CSG_CURRENT_DATE
        ,FN_PRINCIPAL_BALANCE = JSON_REC.FN_PRINCIPAL_BALANCE
        ,FD_MODIFICATION_DATE = CSG_CURRENT_DATE
        WHERE FI_LOAN_ID = JSON_REC.FI_LOAN_ID
        AND FI_ADMIN_CENTER_ID = JSON_REC.FI_ADMIN_CENTER_ID;

      IF SQL%ROWCOUNT = CSG_ZERO THEN
        RAISE EXC_UPDATE_FAILED;
      END IF;
      END LOOP;

    PA_STATUS_CODE := CSG_SUCCESS_CODE;
    PA_STATUS_MSG := CSG_SUCCESS_MSG;
  EXCEPTION
    WHEN EXC_UPDATE_FAILED THEN
    ROLLBACK;
      PA_STATUS_CODE := CSL_UPDATE_FAILED_CODE;
      PA_STATUS_MSG := CSL_UPDATE_FAILED_MSG || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
      SC_CREDIT.SP_ERROR_LOG(CSL_UPDATE_LOAN, SQLCODE, SQLERRM,
        DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, PA_DATA_LOAN);
    WHEN OTHERS THEN
    ROLLBACK;
      PA_STATUS_CODE := SQLCODE;
      PA_STATUS_MSG := SQLERRM || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
      SC_CREDIT.SP_ERROR_LOG(CSL_UPDATE_LOAN, SQLCODE, SQLERRM,
        DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, PA_DATA_LOAN);
  END SP_UPD_LOAN;
  END PA_EXE_LOAN_DISBURSEMENT;

/

  GRANT EXECUTE ON SC_CREDIT.PA_EXE_LOAN_DISBURSEMENT TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.PA_EXE_LOAN_DISBURSEMENT TO USRPURPOSEWS;
--------------------------------------------------------
--  DDL for Package Body PA_EXE_LOAN_ORIGINATION
--------------------------------------------------------

  CREATE OR REPLACE  PACKAGE BODY SC_CREDIT.PA_EXE_LOAN_ORIGINATION 
  AS
  PROCEDURE SP_EXE_LOAN_ORIGINATION(
    PA_LOAN_DATA                    IN CLOB,
    PA_PAYMENT_SCHEDULE_DATA        IN CLOB,
    PA_DATA_LOAN_OPERATION          IN CLOB,
    PA_DATA_LOAN_OPERATION_DETAIL   IN CLOB,
    PA_DATA_LOAN_BALANCE            IN CLOB,
    PA_DATA_LOAN_BALANCE_DETAIL     IN CLOB,
    PA_PAYMENT_SCHEDULE_FEE_DATA    IN CLOB,
    PA_STATUS_CODE                  OUT NUMBER,
    PA_STATUS_MSG                   OUT VARCHAR2)
  AS
/*****************************************************************
* PROJECT:              NCP-OUTSTANDING BALANCE
* DESCRIPTION:          PACKAGE FOR ORIGINATION TO REGISTER A NEW LOAN.
* CREATOR:              RICARDO HAZAEL GOMEZ ALVAREZ/JOSE DE JESUS BRAVO AGUILAR
* CREATED DATE:         OCT-21-2024
* MODIFICATED DATE:     JAN-22-2024
* [NCPACS-4804 V1]
*****************************************************************/
  -- CONSTANTS LOCAL
    CSL_EXE_LOAN                        CONSTANT VARCHAR2(50) := 'SP_EXE_LOAN_ORIGINATION ';
    CSL_ISSUE_NULL_DATA_LOAN_CODE       CONSTANT SIMPLE_INTEGER := -20020;
    CSL_ISSUE_NULL_DATA_LOAN_MSG        CONSTANT VARCHAR2(80) := 'ISSUE IN SP_INS_LOAN: ';
    CSL_ISSUE_NULL_DATA_PAY_CODE        CONSTANT SIMPLE_INTEGER := -20030;
    CSL_ISSUE_NULL_DATA_PAY_MSG         CONSTANT VARCHAR2(80) := 'ISSUE IN SP_INS_PAYMENT_SCHEDULE: ';
    CSL_ISSUE_OPERATION_ID_CODE         CONSTANT SIMPLE_INTEGER := -20040;
    CSL_ISSUE_OPERATION_ID_MSG          CONSTANT VARCHAR2(100) := 'FAILED TO RETRIEVE A VALID ID FOR THE OPERATION OR BALANCE.';
    CSL_ISSUE_OPERATION_CODE            CONSTANT SIMPLE_INTEGER := -20050;
    CSL_ISSUE_OPERATION_MSG             CONSTANT VARCHAR2(50) := 'ISSUE IN SP_INS_LOAN_OPERATION: ';
    CSL_ISSUE_OPERATION_DETAIL_CODE     CONSTANT SIMPLE_INTEGER := -20060;
    CSL_ISSUE_OPERATION_DETAIL_MSG      CONSTANT VARCHAR2(50) := 'ISSUE IN SP_INS_LOAN_OPERATION_DETAIL: ';
    CSL_ISSUE_BALANCE_CODE              CONSTANT SIMPLE_INTEGER := -20070;
    CSL_ISSUE_BALANCE_MSG               CONSTANT VARCHAR2(50) := 'ISSUE IN SP_INS_LOAN_BALANCE: ';
    CSL_ISSUE_BALANCE_DETAIL_CODE       CONSTANT SIMPLE_INTEGER := -20080;
    CSL_ISSUE_BALANCE_DETAIL_MSG        CONSTANT VARCHAR2(50) := 'ISSUE IN SP_INS_LOAN_BALANCE_DETAIL: ';
    CSL_ISSUE_PMT_SCHEDULE_FEE_CODE     CONSTANT SIMPLE_INTEGER := -20120;
    CSL_ISSUE_PMT_SCHEDULE_FEE_MSG      CONSTANT VARCHAR2(50) := 'ISSUE IN SP_INS_PAYMENT_SCHEDULE_FEE: ';
    -- EXCEPTIONS
    EXC_NULL_DATA_LOAN EXCEPTION;
    PRAGMA EXCEPTION_INIT (EXC_NULL_DATA_LOAN, CSL_ISSUE_NULL_DATA_LOAN_CODE);
    EXC_NULL_DATA_PAYMENT EXCEPTION;
    PRAGMA EXCEPTION_INIT (EXC_NULL_DATA_PAYMENT, CSL_ISSUE_NULL_DATA_PAY_CODE);
    EXC_ISSUE_OPERATION_ID EXCEPTION;
    PRAGMA EXCEPTION_INIT (EXC_ISSUE_OPERATION_ID, CSL_ISSUE_OPERATION_ID_CODE);
    EXC_ISSUE_OPERATION EXCEPTION;
    PRAGMA EXCEPTION_INIT (EXC_ISSUE_OPERATION, CSL_ISSUE_OPERATION_CODE);
    EXC_ISSUE_OPERATION_DETAIL EXCEPTION;
    PRAGMA EXCEPTION_INIT (EXC_ISSUE_OPERATION_DETAIL, CSL_ISSUE_OPERATION_DETAIL_CODE);
    EXC_ISSUE_BALANCE EXCEPTION;
    PRAGMA EXCEPTION_INIT (EXC_ISSUE_BALANCE, CSL_ISSUE_BALANCE_CODE);
    EXC_ISSUE_BALANCE_DETAIL EXCEPTION;
    PRAGMA EXCEPTION_INIT (EXC_ISSUE_BALANCE_DETAIL, CSL_ISSUE_BALANCE_DETAIL_CODE);
    EXC_ISSUE_PAYMENT_SCHEDULE_FEE EXCEPTION;
    PRAGMA EXCEPTION_INIT (EXC_ISSUE_PAYMENT_SCHEDULE_FEE, CSL_ISSUE_PMT_SCHEDULE_FEE_CODE);
    --VARIABLES
    VL_LOAN_OPERATION_ID            NUMBER(15);
    PA_LOAN_OPERATION_ID            NUMBER(15);
    VL_LOAN_BALANCE_ID              NUMBER(15);
    PA_LOAN_BALANCE_ID              NUMBER(15);
    PA_FINANCE_CHARGE_BALANCE       NUMBER(12,2);

    BEGIN
    SP_INS_LOAN (
    --PROCEDURE TO INSERT LOAN
      PA_LOAN_DATA,
      PA_FINANCE_CHARGE_BALANCE,
      PA_STATUS_CODE,
      PA_STATUS_MSG);
    IF PA_STATUS_CODE <> CSG_ZERO THEN
      ROLLBACK;
      RAISE EXC_NULL_DATA_LOAN;
    END IF;

    SP_INS_PAYMENT_SCHEDULE (
    --PROCEDURE TO INSERT PAYMENT SCHEDULE
      PA_PAYMENT_SCHEDULE_DATA,
      PA_STATUS_CODE,
      PA_STATUS_MSG);
    IF PA_STATUS_CODE <> CSG_ZERO THEN
      ROLLBACK;
      RAISE EXC_NULL_DATA_PAYMENT;
    END IF;

    IF PA_FINANCE_CHARGE_BALANCE > CSG_ZERO THEN
        BEGIN
    VL_LOAN_OPERATION_ID := SC_CREDIT.FN_GET_NEXT_LOAN_OPERATION_ID;
    VL_LOAN_BALANCE_ID := SC_CREDIT.FN_GET_NEXT_LOAN_BALANCE_ID;

    IF VL_LOAN_OPERATION_ID IS NULL OR VL_LOAN_BALANCE_ID IS NULL THEN
        PA_STATUS_CODE := CSL_ISSUE_OPERATION_ID_CODE;
        PA_STATUS_MSG := CSL_ISSUE_OPERATION_ID_MSG;
        RAISE EXC_ISSUE_OPERATION_ID;
     END IF;

    PA_LOAN_OPERATION_ID := VL_LOAN_OPERATION_ID;
    PA_LOAN_BALANCE_ID := VL_LOAN_BALANCE_ID;

            SP_INS_LOAN_OPERATION (
            --PROCEDURE TO INSERT OPERATION
                PA_DATA_LOAN_OPERATION
                ,PA_LOAN_OPERATION_ID
                ,PA_STATUS_CODE
                ,PA_STATUS_MSG);
            IF PA_STATUS_CODE <> CSG_ZERO THEN
            RAISE EXC_ISSUE_OPERATION;
            END IF;

            SP_INS_LOAN_OPERATION_DETAIL (
            --PROCEDURE TO INSERT OPERATION DETAIL
                PA_DATA_LOAN_OPERATION_DETAIL
                ,PA_LOAN_OPERATION_ID
                ,PA_STATUS_CODE
                ,PA_STATUS_MSG);
            IF PA_STATUS_CODE <> CSG_ZERO THEN
            RAISE EXC_ISSUE_OPERATION_DETAIL;
            END IF;

            SP_INS_LOAN_BALANCE (
            --PROCEDURE TO INSERT BALANCE
                PA_DATA_LOAN_BALANCE
                ,PA_LOAN_BALANCE_ID
                ,PA_LOAN_OPERATION_ID
                ,PA_STATUS_CODE
                ,PA_STATUS_MSG);
            IF PA_STATUS_CODE <> CSG_ZERO THEN
            RAISE EXC_ISSUE_BALANCE;
            END IF;

            SP_INS_LOAN_BALANCE_DETAIL (
            --PROCEDURE TO INSERT BALANCE DETAIL
                PA_DATA_LOAN_BALANCE_DETAIL
                ,PA_LOAN_BALANCE_ID
                ,PA_STATUS_CODE
                ,PA_STATUS_MSG);
            IF PA_STATUS_CODE <> CSG_ZERO THEN
            RAISE EXC_ISSUE_BALANCE_DETAIL;
            END IF;

            SP_INS_PAYMENT_SCHEDULE_FEE (
            --PROCEDURE TO INSERT PAYMENT SCHEDULE FEE
                PA_PAYMENT_SCHEDULE_FEE_DATA
                ,PA_STATUS_CODE
                ,PA_STATUS_MSG);
            IF PA_STATUS_CODE <> CSG_ZERO THEN
            RAISE EXC_ISSUE_PAYMENT_SCHEDULE_FEE;
            END IF;
            END;
        END IF;
  COMMIT;

    PA_STATUS_CODE := CSG_SUCCESS_CODE;
    PA_STATUS_MSG := CSG_SUCCESS_MSG;

  EXCEPTION
      WHEN EXC_NULL_DATA_LOAN THEN
      ROLLBACK;
      PA_STATUS_MSG := CSL_ISSUE_NULL_DATA_LOAN_MSG ||CSL_ISSUE_NULL_DATA_LOAN_CODE|| CSG_ARROW ||PA_STATUS_CODE||
      CSG_COLON || PA_STATUS_MSG || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
      PA_STATUS_CODE := CSL_ISSUE_NULL_DATA_LOAN_CODE;
      SC_CREDIT.SP_ERROR_LOG(CSL_EXE_LOAN, SQLCODE, SQLERRM, DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, CSG_X);
      WHEN EXC_NULL_DATA_PAYMENT THEN
      ROLLBACK;
      PA_STATUS_MSG := CSL_ISSUE_NULL_DATA_PAY_MSG ||CSL_ISSUE_NULL_DATA_PAY_CODE|| CSG_ARROW ||PA_STATUS_CODE||
      CSG_COLON || PA_STATUS_MSG || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
      PA_STATUS_CODE := CSL_ISSUE_NULL_DATA_PAY_CODE;
      SC_CREDIT.SP_ERROR_LOG(CSL_EXE_LOAN, SQLCODE, SQLERRM,
      DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, CSG_X);
      WHEN EXC_ISSUE_OPERATION THEN
      ROLLBACK;
      PA_STATUS_MSG := CSL_ISSUE_OPERATION_MSG || CSL_ISSUE_OPERATION_CODE || CSG_ARROW || PA_STATUS_CODE ||
      CSG_COLON || PA_STATUS_MSG || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
      PA_STATUS_CODE := CSL_ISSUE_OPERATION_CODE;
      SC_CREDIT.SP_ERROR_LOG(CSL_EXE_LOAN, SQLCODE, SQLERRM, DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, CSG_X);
      WHEN EXC_ISSUE_OPERATION_DETAIL THEN
      ROLLBACK;
      PA_STATUS_MSG := CSL_ISSUE_OPERATION_DETAIL_MSG || CSL_ISSUE_OPERATION_DETAIL_CODE  || CSG_ARROW || PA_STATUS_CODE ||
      CSG_COLON || PA_STATUS_MSG || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
      PA_STATUS_CODE := CSL_ISSUE_OPERATION_DETAIL_CODE;
      SC_CREDIT.SP_ERROR_LOG(CSL_EXE_LOAN, SQLCODE, SQLERRM,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, CSG_X);
      WHEN EXC_ISSUE_BALANCE THEN
      ROLLBACK;
      PA_STATUS_MSG := CSL_ISSUE_BALANCE_MSG || CSL_ISSUE_BALANCE_CODE || CSG_ARROW || PA_STATUS_CODE ||
      CSG_COLON || PA_STATUS_MSG || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
      PA_STATUS_CODE := CSL_ISSUE_BALANCE_CODE;
      SC_CREDIT.SP_ERROR_LOG(CSL_EXE_LOAN, SQLCODE, SQLERRM, DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, CSG_X);
      WHEN EXC_ISSUE_BALANCE_DETAIL THEN
      ROLLBACK;
      PA_STATUS_MSG := CSL_ISSUE_BALANCE_DETAIL_MSG || CSL_ISSUE_BALANCE_DETAIL_CODE || CSG_ARROW || PA_STATUS_CODE ||
      CSG_COLON || PA_STATUS_MSG || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
      PA_STATUS_CODE := CSL_ISSUE_BALANCE_DETAIL_CODE;
      SC_CREDIT.SP_ERROR_LOG(CSL_EXE_LOAN, SQLCODE, SQLERRM, DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, CSG_X);
      WHEN EXC_ISSUE_PAYMENT_SCHEDULE_FEE THEN
      ROLLBACK;
      PA_STATUS_MSG := CSL_ISSUE_PMT_SCHEDULE_FEE_MSG || CSL_ISSUE_PMT_SCHEDULE_FEE_CODE || CSG_ARROW || PA_STATUS_CODE ||
      CSG_COLON || PA_STATUS_MSG || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
      PA_STATUS_CODE := CSL_ISSUE_PMT_SCHEDULE_FEE_CODE;
      SC_CREDIT.SP_ERROR_LOG(CSL_EXE_LOAN, SQLCODE, SQLERRM, DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, CSG_X);
      WHEN OTHERS THEN
      PA_STATUS_CODE := SQLCODE;
      PA_STATUS_MSG := SQLERRM || CSG_ARROW || PA_STATUS_MSG || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
      SC_CREDIT.SP_ERROR_LOG(CSL_EXE_LOAN, SQLCODE, SQLERRM, DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, CSG_X);
        ROLLBACK;

    IF PA_STATUS_CODE = CSG_ZERO THEN
      PA_STATUS_CODE := -1;
    END IF;
  END SP_EXE_LOAN_ORIGINATION;

  PROCEDURE SP_INS_LOAN (
    PA_LOAN_DATA                IN CLOB,
    PA_FINANCE_CHARGE_BALANCE   OUT NUMBER,
    PA_STATUS_CODE              OUT NUMBER,
    PA_STATUS_MSG               OUT VARCHAR2)
  AS
/* **************************************************************
* PROJECT: NCP-OUTSTANDING BALANCE
* DESCRIPTION: PROCEDURE TO INSERT LOAN
************************************************************** */
  -- LOCAL CONSTANS
  CSL_INSERT_LOAN                 CONSTANT VARCHAR2(50) := 'SP_INS_LOAN';
  CSL_LOAN_ALREADY_EXISTS_CODE    CONSTANT SIMPLE_INTEGER := -20021;
  CSL_LOAN_ALREADY_EXISTS_MSG    CONSTANT VARCHAR2(80) := 'LOAN ID ALREADY EXISTS';
  --VARIABLES
  VL_LOAN_ID                      NUMBER(15);
  VL_COUNTRY_ID                   NUMBER(3);
  VL_COMPANY_ID                   NUMBER(3);
  VL_BUSINESS_UNIT_ID             NUMBER(5);
  VL_ADMIN_CENTER_ID              NUMBER(8);
  VL_ORIGINATION_CENTER_ID        NUMBER(8);
  VL_PLATFORM_ID                  VARCHAR2(6);
  VL_SUB_PLATFORM_ID              VARCHAR2(6);
  VL_CUSTOMER_ID                  VARCHAR2(36);
  VL_PRODUCT_ID                   NUMBER(10);
  VL_PRINCIPAL_AMOUNT             NUMBER(12,2);
  VL_FINANCE_CHARGE_AMOUNT        NUMBER(12,2);
  VL_PRINCIPAL_BALANCE            NUMBER(12,2);
  VL_FINANCE_CHARGE_BALANCE       NUMBER(12,2);
  VL_ADDITIONAL_CHARGE_BALANCE    NUMBER(12,2);
  VL_ORIGINATION_DATE_CHAR        VARCHAR2(30);
  VL_ORIGINATION_DATE             DATE;
  VL_FIRST_PAYMENT_CHAR           VARCHAR2(30);
  VL_FIRST_PAYMENT                DATE;
  VL_DUE_DATE_CHAR                VARCHAR2(30);
  VL_DUE_DATE                     DATE;
  VL_APR                          NUMBER(12,6);
  VL_INTEREST_RATE                NUMBER(12,2);
  VL_NUMBER_OF_PAYMENTS           NUMBER(5);
  VL_TERM_TYPE                    NUMBER(5);
  VL_LOAN_STATUS_ID               NUMBER(5);
  VL_ACCRUED_TYPE_ID              NUMBER(5);
  VL_RULE_ID                      NUMBER(5);
  VL_END_USER                     VARCHAR2(10);
  VL_OPERATION_DATE_CHAR          VARCHAR2(30);
  VL_OPERATION_DATE               DATE;
  VL_UUID_TRACKING                VARCHAR2(36);
  VL_IP_ADDRESS                   VARCHAR2(39);
  VL_DEVICE                       VARCHAR2(50);
  VL_TRANSACTION                  NUMBER(33);
  VL_LOAN_EFFECTIVE_DATE_CHAR     VARCHAR2(30);
  VL_LOAN_EFFECTIVE_DATE          DATE;
  VL_INSERT_COUNT                 NUMBER(3);
  VL_EXISTING_LOAN_COUNT          NUMBER(1);
  VL_SUBTERM_TYPE_ID              NUMBER(5);

  -- EXCEPTIONS
  EXC_FOREING_KEY_VIOLATION   EXCEPTION;
  PRAGMA EXCEPTION_INIT(EXC_FOREING_KEY_VIOLATION, CSG_FOREING_KEY_CODE);
  EXC_DATA_NOT_SAVED          EXCEPTION;
  PRAGMA EXCEPTION_INIT (EXC_DATA_NOT_SAVED, CSG_DATA_NOT_SAVED_CODE);
  EX_DUPLICATE_PK             EXCEPTION;
  PRAGMA EXCEPTION_INIT(EX_DUPLICATE_PK, CSG_PRIMARY_KEY_CODE);
  EXC_LOAN_ALREADY_EXISTS     EXCEPTION;
  PRAGMA EXCEPTION_INIT(EXC_LOAN_ALREADY_EXISTS, CSL_LOAN_ALREADY_EXISTS_CODE);

  BEGIN
    VL_LOAN_ID                          := JSON_VALUE(PA_LOAN_DATA, '$.loanId');
    VL_COUNTRY_ID                       := JSON_VALUE(PA_LOAN_DATA, '$.countryId');
    VL_COMPANY_ID                       := JSON_VALUE(PA_LOAN_DATA, '$.companyId');
    VL_BUSINESS_UNIT_ID                 := JSON_VALUE(PA_LOAN_DATA, '$.businessUnitId');
    VL_ADMIN_CENTER_ID                  := JSON_VALUE(PA_LOAN_DATA, '$.adminCenterId');
    VL_ORIGINATION_CENTER_ID            := JSON_VALUE(PA_LOAN_DATA, '$.originationCenterId');
    VL_PLATFORM_ID                      := JSON_VALUE(PA_LOAN_DATA, '$.platformId');
    VL_SUB_PLATFORM_ID                  := JSON_VALUE(PA_LOAN_DATA, '$.subPlatformId');
    VL_CUSTOMER_ID                      := JSON_VALUE(PA_LOAN_DATA, '$.customerId');
    VL_PRODUCT_ID                       := JSON_VALUE(PA_LOAN_DATA, '$.productId');
    VL_PRINCIPAL_AMOUNT                 := JSON_VALUE(PA_LOAN_DATA, '$.principalAmount');
    VL_FINANCE_CHARGE_AMOUNT            := JSON_VALUE(PA_LOAN_DATA, '$.financeChargeAmount');
    VL_PRINCIPAL_BALANCE                := JSON_VALUE(PA_LOAN_DATA, '$.principalBalance');
    VL_FINANCE_CHARGE_BALANCE           := JSON_VALUE(PA_LOAN_DATA, '$.financeChargeBalance');
    VL_ADDITIONAL_CHARGE_BALANCE        := JSON_VALUE(PA_LOAN_DATA, '$.additionalChargeBalance');
    VL_ORIGINATION_DATE_CHAR            := JSON_VALUE(PA_LOAN_DATA, '$.originationDate');
    VL_FIRST_PAYMENT_CHAR               := JSON_VALUE(PA_LOAN_DATA, '$.firstPayment');
    VL_DUE_DATE_CHAR                    := JSON_VALUE(PA_LOAN_DATA, '$.dueDate');
    VL_APR                              := JSON_VALUE(PA_LOAN_DATA, '$.apr');
    VL_INTEREST_RATE                    := JSON_VALUE(PA_LOAN_DATA, '$.interestRate');
    VL_NUMBER_OF_PAYMENTS               := JSON_VALUE(PA_LOAN_DATA, '$.numberOfPayments');
    VL_TERM_TYPE                        := JSON_VALUE(PA_LOAN_DATA, '$.termType');
    VL_LOAN_STATUS_ID                   := JSON_VALUE(PA_LOAN_DATA, '$.loanStatusId');
    VL_ACCRUED_TYPE_ID                  := JSON_VALUE(PA_LOAN_DATA, '$.accruedTypeId');
    VL_RULE_ID                          := JSON_VALUE(PA_LOAN_DATA, '$.ruleId');
    VL_END_USER                         := JSON_VALUE(PA_LOAN_DATA, '$.endUser');
    VL_OPERATION_DATE_CHAR              := JSON_VALUE(PA_LOAN_DATA, '$.operationDate');
    VL_UUID_TRACKING                    := JSON_VALUE(PA_LOAN_DATA, '$.uuidTracking');
    VL_IP_ADDRESS                       := JSON_VALUE(PA_LOAN_DATA, '$.ipAddress');
    VL_DEVICE                           := JSON_VALUE(PA_LOAN_DATA, '$.device');
    VL_TRANSACTION                      := JSON_VALUE(PA_LOAN_DATA, '$.transaction');
    VL_LOAN_EFFECTIVE_DATE_CHAR         := JSON_VALUE(PA_LOAN_DATA, '$.loanEffectiveDate');
    VL_ORIGINATION_DATE                 := CAST(TO_TIMESTAMP_TZ(VL_ORIGINATION_DATE_CHAR, CSG_FORMAT_DATE) AS DATE);
    VL_FIRST_PAYMENT                    := CAST(TO_TIMESTAMP_TZ(VL_FIRST_PAYMENT_CHAR, CSG_FORMAT_DATE) AS DATE);
    VL_DUE_DATE                         := CAST(TO_TIMESTAMP_TZ(VL_DUE_DATE_CHAR, CSG_FORMAT_DATE) AS DATE);
    VL_OPERATION_DATE                   := CAST(TO_TIMESTAMP_TZ(VL_OPERATION_DATE_CHAR, CSG_FORMAT_DATE) AS DATE);
    VL_LOAN_EFFECTIVE_DATE              := CAST(TO_TIMESTAMP_TZ(VL_LOAN_EFFECTIVE_DATE_CHAR, CSG_FORMAT_DATE) AS DATE);
    VL_SUBTERM_TYPE_ID                  := JSON_VALUE(PA_LOAN_DATA, '$.subTermTypeId');

  SELECT COUNT(*)
    INTO VL_EXISTING_LOAN_COUNT
    FROM SC_CREDIT.TA_LOAN
    WHERE FI_LOAN_ID = VL_LOAN_ID;

  IF VL_EXISTING_LOAN_COUNT > CSG_ZERO THEN
    RAISE EXC_LOAN_ALREADY_EXISTS;
  END IF;

  INSERT INTO SC_CREDIT.TA_LOAN (
    FI_COUNTRY_ID,
    FI_COMPANY_ID,
    FI_BUSINESS_UNIT_ID,
    FI_ADMIN_CENTER_ID,
    FI_LOAN_ID,
    FI_ORIGINATION_CENTER_ID,
    FC_PLATFORM_ID,
    FC_SUB_PLATFORM_ID,
    FC_CUSTOMER_ID,
    FI_PRODUCT_ID,
    FN_PRINCIPAL_AMOUNT,
    FN_FINANCE_CHARGE_AMOUNT,
    FN_PRINCIPAL_BALANCE,
    FN_FINANCE_CHARGE_BALANCE,
    FN_ADDITIONAL_CHARGE_BALANCE,
    FD_ORIGINATION_DATE,
    FD_FIRST_PAYMENT,
    FD_DUE_DATE,
    FN_APR,
    FI_ADDITIONAL_STATUS,
    FI_CURRENT_BALANCE_SEQ,
    FN_INTEREST_RATE,
    FI_NUMBER_OF_PAYMENTS,
    FI_TERM_TYPE,
    FI_LOAN_STATUS_ID,
    FI_ACCRUED_TYPE_ID,
    FI_RULE_ID,
    FC_END_USER,
    FD_OPERATION_DATE,
    FC_UUID_TRACKING,
    FC_IP_ADDRESS,
    FC_DEVICE,
    FC_USER,
    FD_CREATED_DATE,
    FD_MODIFICATION_DATE,
    FI_TRANSACTION,
    FD_LOAN_STATUS_DATE,
    FD_LOAN_EFFECTIVE_DATE,
    FI_SUBTERM_TYPE_ID)
  VALUES (
    VL_COUNTRY_ID,
    VL_COMPANY_ID,
    VL_BUSINESS_UNIT_ID,
    VL_ADMIN_CENTER_ID,
    VL_LOAN_ID,
    VL_ORIGINATION_CENTER_ID,
    VL_PLATFORM_ID,
    VL_SUB_PLATFORM_ID,
    VL_CUSTOMER_ID,
    VL_PRODUCT_ID,
    VL_PRINCIPAL_AMOUNT,
    VL_FINANCE_CHARGE_AMOUNT,
    VL_PRINCIPAL_BALANCE,
    VL_FINANCE_CHARGE_BALANCE,
    VL_ADDITIONAL_CHARGE_BALANCE,
    VL_ORIGINATION_DATE,
    VL_FIRST_PAYMENT,
    VL_DUE_DATE,
    VL_APR,
    CSG_ZERO,
    CSG_ZERO,
    VL_INTEREST_RATE,
    VL_NUMBER_OF_PAYMENTS,
    VL_TERM_TYPE,
    VL_LOAN_STATUS_ID,
    VL_ACCRUED_TYPE_ID,
    VL_RULE_ID,
    VL_END_USER,
    VL_OPERATION_DATE,
    VL_UUID_TRACKING,
    VL_IP_ADDRESS,
    VL_DEVICE,
    CSG_CURRENT_USER,
    CSG_CURRENT_DATE,
    CSG_CURRENT_DATE,
    VL_TRANSACTION,
    CSG_CURRENT_DATE,
    VL_LOAN_EFFECTIVE_DATE,
    VL_SUBTERM_TYPE_ID);

  VL_INSERT_COUNT := SQL%ROWCOUNT;

    IF VL_INSERT_COUNT = CSG_ZERO THEN
    RAISE EXC_DATA_NOT_SAVED;
    END IF;

    PA_STATUS_CODE := CSG_SUCCESS_CODE;
    PA_STATUS_MSG := CSG_SUCCESS_MSG;
    PA_FINANCE_CHARGE_BALANCE := VL_FINANCE_CHARGE_BALANCE;

  EXCEPTION
    WHEN EXC_FOREING_KEY_VIOLATION THEN
    ROLLBACK;
    PA_STATUS_CODE := CSG_FOREING_KEY_CODE;
    PA_STATUS_MSG := CSG_FOREING_KEY_MSG || CSG_ARROW || SQLERRM || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_LOAN, SQLCODE, SQLERRM,
    DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, VL_UUID_TRACKING, PA_LOAN_DATA);
    WHEN EX_DUPLICATE_PK THEN
    ROLLBACK;
    PA_STATUS_CODE := CSG_PRIMARY_KEY_CODE;
    PA_STATUS_MSG := CSG_PRIMARY_KEY_MSG || CSG_ARROW || SQLERRM || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_LOAN, SQLCODE, SQLERRM,
    DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, VL_UUID_TRACKING, PA_LOAN_DATA);
    WHEN EXC_DATA_NOT_SAVED THEN
    ROLLBACK;
    PA_STATUS_CODE := CSG_DATA_NOT_SAVED_CODE;
    PA_STATUS_MSG := CSG_DATA_NOT_SAVED_MSG || CSG_ARROW ||  SQLERRM || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_LOAN, SQLCODE, SQLERRM,
      DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, VL_UUID_TRACKING, CSG_X );
    WHEN EXC_LOAN_ALREADY_EXISTS THEN
    ROLLBACK;
    PA_STATUS_CODE := CSL_LOAN_ALREADY_EXISTS_CODE;
    PA_STATUS_MSG := CSL_LOAN_ALREADY_EXISTS_MSG || CSG_ARROW || SQLERRM || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_LOAN, SQLCODE, SQLERRM,
      DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, VL_UUID_TRACKING, PA_LOAN_DATA);
  WHEN OTHERS THEN
    PA_STATUS_CODE := SQLCODE;
    PA_STATUS_MSG := SQLERRM || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_LOAN, SQLCODE, SQLERRM,
      DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, VL_UUID_TRACKING, PA_LOAN_DATA);
  END SP_INS_LOAN;

  PROCEDURE SP_INS_PAYMENT_SCHEDULE (
    PA_PAYMENT_SCHEDULE_DATA IN CLOB,
    PA_STATUS_CODE OUT NUMBER,
    PA_STATUS_MSG OUT VARCHAR2)
  AS
/* **************************************************************
* PROJECT: NCP-OUTSTANDING BALANCE
* DESCRIPTION: PROCEDURE TO INSERT PAYMENT SCHEDULE
************************************************************** */
  --LOCAL CONSTANS
    CSL_INSERT_PAYMENT       CONSTANT VARCHAR2(50) := 'SP_INS_PAYMENT_SCHEDULE';
  -- EXCEPTIONS
    EXC_DATA_NOT_SAVED           EXCEPTION;
    PRAGMA EXCEPTION_INIT (EXC_DATA_NOT_SAVED, CSG_DATA_NOT_SAVED_CODE);
    EXC_FOREING_KEY_VIOLATION EXCEPTION;
    PRAGMA EXCEPTION_INIT(EXC_FOREING_KEY_VIOLATION, CSG_FOREING_KEY_CODE);
    EX_DUPLICATE_PK EXCEPTION;
    PRAGMA EXCEPTION_INIT(EX_DUPLICATE_PK, CSG_PRIMARY_KEY_CODE);
    EXC_DUPLICATE_DATA EXCEPTION;
    PRAGMA EXCEPTION_INIT (EXC_DUPLICATE_DATA, CSG_DUPLICATE_DATA_CODE);

  TYPE REC_PAYMENT_SCHEDULE_TYPE IS RECORD (
    FI_LOAN_ID                      SC_CREDIT.TA_PAYMENT_SCHEDULE.FI_LOAN_ID%TYPE,
    FI_PAYMENT_NUMBER_ID            SC_CREDIT.TA_PAYMENT_SCHEDULE.FI_PAYMENT_NUMBER_ID%TYPE,
    FI_ADMIN_CENTER_ID              SC_CREDIT.TA_PAYMENT_SCHEDULE.FI_ADMIN_CENTER_ID%TYPE,
    FN_PAYMENT_AMOUNT               SC_CREDIT.TA_PAYMENT_SCHEDULE.FN_PAYMENT_AMOUNT%TYPE,
    FD_DUE_DATE_CHAR                VARCHAR2(30),
    FI_PMT_SCHEDULE_STATUS_ID       SC_CREDIT.TA_PAYMENT_SCHEDULE.FI_PMT_SCHEDULE_STATUS_ID%TYPE,
    FI_PERIOD_DAYS                  SC_CREDIT.TA_PAYMENT_SCHEDULE.FI_PERIOD_DAYS%TYPE,
    FN_INTEREST_AMOUNT              SC_CREDIT.TA_PAYMENT_SCHEDULE.FN_INTEREST_AMOUNT%TYPE,
    FN_PRINCIPAL_PAYMENT_AMOUNT     SC_CREDIT.TA_PAYMENT_SCHEDULE.FN_PRINCIPAL_PAYMENT_AMOUNT%TYPE,
    FN_OUTSTANDING_BALANCE          SC_CREDIT.TA_PAYMENT_SCHEDULE.FN_OUTSTANDING_BALANCE%TYPE);

    VL_PAYMENT_SCHEDULE_DATA        REC_PAYMENT_SCHEDULE_TYPE;
    VL_INSERT_COUNT                 NUMBER(3) := CSG_ZERO;
    VL_NUMBER_OF_PAYMMENTS          SC_CREDIT.TA_LOAN.FI_NUMBER_OF_PAYMENTS%TYPE;
    VL_COUNT_NUMBER_OF_PAYMENTS     NUMBER(3);
    VL_DUE_DATE                     DATE;
  BEGIN
     FOR JSON_REC IN (
    SELECT
      PS.FI_LOAN_ID,
      PS.FI_PAYMENT_NUMBER_ID,
      PS.FI_ADMIN_CENTER_ID,
      PS.FN_PAYMENT_AMOUNT,
      PS.FD_DUE_DATE_CHAR,
      PS.FI_PMT_SCHEDULE_STATUS_ID,
      PS.FI_PERIOD_DAYS,
      PS.FN_INTEREST_AMOUNT,
      PS.FN_PRINCIPAL_PAYMENT_AMOUNT,
      PS.FN_OUTSTANDING_BALANCE
    FROM JSON_TABLE (
      PA_PAYMENT_SCHEDULE_DATA,
      '$[*]'
    COLUMNS (
      FI_LOAN_ID                    NUMBER(15) PATH '$.loanId',
      FI_PAYMENT_NUMBER_ID          NUMBER(3) PATH '$.paymentNumberId',
      FI_ADMIN_CENTER_ID            NUMBER(8) PATH '$.adminCenterId',
      FN_PAYMENT_AMOUNT             NUMBER(12,2) PATH '$.paymentAmount',
      FD_DUE_DATE_CHAR              VARCHAR2(30) PATH '$.dueDate',
      FI_PMT_SCHEDULE_STATUS_ID     NUMBER(3) PATH '$.pmtScheduleStatusId',
      FI_PERIOD_DAYS                NUMBER(3) PATH '$.periodDays',
      FN_INTEREST_AMOUNT            NUMBER(12,2) PATH '$.interestAmount',
      FN_PRINCIPAL_PAYMENT_AMOUNT   NUMBER(12,2) PATH '$.principalPaymentAmount',
      FN_OUTSTANDING_BALANCE        NUMBER(12,2) PATH '$.outstandingBalance'))
    PS)
  LOOP
    VL_PAYMENT_SCHEDULE_DATA.FI_LOAN_ID                     := JSON_REC.FI_LOAN_ID;
    VL_PAYMENT_SCHEDULE_DATA.FI_PAYMENT_NUMBER_ID           := JSON_REC.FI_PAYMENT_NUMBER_ID;
    VL_PAYMENT_SCHEDULE_DATA.FI_ADMIN_CENTER_ID             := JSON_REC.FI_ADMIN_CENTER_ID;
    VL_PAYMENT_SCHEDULE_DATA.FN_PAYMENT_AMOUNT              := JSON_REC.FN_PAYMENT_AMOUNT;
    VL_PAYMENT_SCHEDULE_DATA.FD_DUE_DATE_CHAR               := JSON_REC.FD_DUE_DATE_CHAR;
    VL_PAYMENT_SCHEDULE_DATA.FI_PMT_SCHEDULE_STATUS_ID      := JSON_REC.FI_PMT_SCHEDULE_STATUS_ID;
    VL_DUE_DATE                                             := CAST(TO_TIMESTAMP_TZ(VL_PAYMENT_SCHEDULE_DATA.FD_DUE_DATE_CHAR, CSG_FORMAT_DATE) AS DATE);
    VL_PAYMENT_SCHEDULE_DATA.FI_PERIOD_DAYS                 := JSON_REC.FI_PERIOD_DAYS;
    VL_PAYMENT_SCHEDULE_DATA.FN_INTEREST_AMOUNT             := JSON_REC.FN_INTEREST_AMOUNT;
    VL_PAYMENT_SCHEDULE_DATA.FN_PRINCIPAL_PAYMENT_AMOUNT    := JSON_REC.FN_PRINCIPAL_PAYMENT_AMOUNT;
    VL_PAYMENT_SCHEDULE_DATA.FN_OUTSTANDING_BALANCE         := JSON_REC.FN_OUTSTANDING_BALANCE;

  INSERT INTO SC_CREDIT.TA_PAYMENT_SCHEDULE (
    FI_PAYMENT_SCHEDULE_ID
    ,FI_LOAN_ID
    ,FI_ADMIN_CENTER_ID
    ,FI_PAYMENT_NUMBER_ID
    ,FN_PAYMENT_AMOUNT
    ,FN_PAYMENT_BALANCE
    ,FI_SCHEDULE_TYPE_ID
    ,FD_DUE_DATE
    ,FI_PMT_SCHEDULE_STATUS_ID
    ,FI_STATUS
    ,FC_USER
    ,FD_CREATED_DATE
    ,FD_MODIFICATION_DATE
    ,FI_PERIOD_DAYS
    ,FN_INTEREST_AMOUNT
    ,FN_PRINCIPAL_PAYMENT_AMOUNT
    ,FN_OUTSTANDING_BALANCE)

  VALUES (
    VL_PAYMENT_SCHEDULE_DATA.FI_PAYMENT_NUMBER_ID
    ,VL_PAYMENT_SCHEDULE_DATA.FI_LOAN_ID
    ,VL_PAYMENT_SCHEDULE_DATA.FI_ADMIN_CENTER_ID
    ,VL_PAYMENT_SCHEDULE_DATA.FI_PAYMENT_NUMBER_ID
    ,VL_PAYMENT_SCHEDULE_DATA.FN_PAYMENT_AMOUNT
    ,VL_PAYMENT_SCHEDULE_DATA.FN_PAYMENT_AMOUNT
    ,CSG_ONE
    ,VL_DUE_DATE
    ,VL_PAYMENT_SCHEDULE_DATA.FI_PMT_SCHEDULE_STATUS_ID
    ,CSG_ONE
    ,CSG_CURRENT_USER
    ,CSG_CURRENT_DATE
    ,CSG_CURRENT_DATE
    ,VL_PAYMENT_SCHEDULE_DATA.FI_PERIOD_DAYS
    ,VL_PAYMENT_SCHEDULE_DATA.FN_INTEREST_AMOUNT
    ,VL_PAYMENT_SCHEDULE_DATA.FN_PRINCIPAL_PAYMENT_AMOUNT
    ,VL_PAYMENT_SCHEDULE_DATA.FN_OUTSTANDING_BALANCE);

    VL_INSERT_COUNT := VL_INSERT_COUNT + CSG_ONE;
  END LOOP;

    SELECT COUNT(CSG_ONE)
    INTO VL_COUNT_NUMBER_OF_PAYMENTS FROM SC_CREDIT.TA_LOAN WHERE FI_LOAN_ID = VL_PAYMENT_SCHEDULE_DATA.FI_LOAN_ID
    AND FI_ADMIN_CENTER_ID = VL_PAYMENT_SCHEDULE_DATA.FI_ADMIN_CENTER_ID;

    IF VL_COUNT_NUMBER_OF_PAYMENTS > CSG_ONE THEN
        RAISE EXC_DUPLICATE_DATA;
    END IF;

    SELECT  FI_NUMBER_OF_PAYMENTS
    INTO VL_NUMBER_OF_PAYMMENTS FROM SC_CREDIT.TA_LOAN WHERE FI_LOAN_ID = VL_PAYMENT_SCHEDULE_DATA.FI_LOAN_ID
    AND FI_ADMIN_CENTER_ID = VL_PAYMENT_SCHEDULE_DATA.FI_ADMIN_CENTER_ID;

  IF VL_INSERT_COUNT <> VL_NUMBER_OF_PAYMMENTS THEN
    RAISE EXC_DATA_NOT_SAVED;
  END IF;

    PA_STATUS_CODE := CSG_SUCCESS_CODE;
    PA_STATUS_MSG := CSG_SUCCESS_MSG;

  EXCEPTION
    WHEN EXC_FOREING_KEY_VIOLATION THEN
    ROLLBACK;
    PA_STATUS_CODE := CSG_FOREING_KEY_CODE;
    PA_STATUS_MSG := CSG_FOREING_KEY_MSG || CSG_ARROW || SQLERRM || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_PAYMENT, SQLCODE, SQLERRM,
    DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, PA_PAYMENT_SCHEDULE_DATA);
    WHEN EX_DUPLICATE_PK THEN
    ROLLBACK;
    PA_STATUS_CODE := CSG_PRIMARY_KEY_CODE;
    PA_STATUS_MSG := CSG_PRIMARY_KEY_MSG || CSG_ARROW || SQLERRM || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_PAYMENT, SQLCODE, SQLERRM,
    DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, PA_PAYMENT_SCHEDULE_DATA);
    WHEN EXC_DATA_NOT_SAVED THEN
    ROLLBACK;
    PA_STATUS_CODE := CSG_DATA_NOT_SAVED_CODE;
    PA_STATUS_MSG := CSG_DATA_NOT_SAVED_MSG || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_PAYMENT, SQLCODE, SQLERRM,
      DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, PA_PAYMENT_SCHEDULE_DATA );
    WHEN EXC_DUPLICATE_DATA THEN
    ROLLBACK;
    PA_STATUS_CODE := CSG_DUPLICATE_DATA_CODE;
    PA_STATUS_MSG := CSG_DUPLICATE_DATA_MSG || CSG_ARROW || SQLERRM ||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_PAYMENT, SQLCODE, SQLERRM,
      DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, PA_PAYMENT_SCHEDULE_DATA);
    WHEN OTHERS THEN
    PA_STATUS_CODE := SQLCODE;
    PA_STATUS_MSG := SQLERRM || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_PAYMENT, SQLCODE, SQLERRM,
      DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, PA_PAYMENT_SCHEDULE_DATA);
    ROLLBACK;
  END SP_INS_PAYMENT_SCHEDULE;

  PROCEDURE SP_INS_LOAN_OPERATION(
    PA_DATA_LOAN_OPERATION    IN CLOB
    ,PA_LOAN_OPERATION_ID     IN NUMBER
    ,PA_STATUS_CODE           OUT NUMBER
    ,PA_STATUS_MSG            OUT VARCHAR2)
  IS
/* **************************************************************
* PROJECT: NCP-OUTSTANDING BALANCE
* DESCRIPTION: PROCEDURE TO INSERT LOAN OPERATION
************************************************************** */
    --LOCAL CONSTANS
    CSL_INSERT_OPERATION     CONSTANT VARCHAR2(50) := 'SP_INS_LOAN_OPERATION';
      -- VARIABLES
  VL_COUNTRY_ID             NUMBER(3);
  VL_COMPANY_ID             NUMBER(3);
  VL_BUSINESS_UNIT_ID       NUMBER(5);
  VL_LOAN_ID                NUMBER(15);
  VL_ADMIN_CENTER_ID        NUMBER(8);
  VL_OPERATION_TYPE_ID      NUMBER(5);
  VL_TRANSACTION            NUMBER(33);
  VL_PLATFORM_ID            VARCHAR2(6);
  VL_SUB_PLATFORM_ID        VARCHAR2(6);
  VL_OPERATION_AMOUNT       NUMBER(12,2);
  VL_APPLICATION_DATE_CHAR  VARCHAR2(30);
  VL_APPLICATION_DATE       DATE;
  VL_END_USER               VARCHAR2(10);
  VL_UUID_TRACKING          VARCHAR2(36);
  VL_GPS_LATITUDE           VARCHAR2(15);
  VL_GPS_LONGITUDE          VARCHAR2(15);
  VL_IP_ADDRESS             VARCHAR2(39);
  VL_DEVICE                 VARCHAR2(50);
   -- EXCEPTIONS
  EXC_FOREING_KEY_VIOLATION EXCEPTION;
  PRAGMA EXCEPTION_INIT(EXC_FOREING_KEY_VIOLATION, CSG_FOREING_KEY_CODE);
  EX_DUPLICATE_PK EXCEPTION;
  PRAGMA EXCEPTION_INIT(EX_DUPLICATE_PK, CSG_PRIMARY_KEY_CODE);

  BEGIN
    VL_COUNTRY_ID               := JSON_VALUE(PA_DATA_LOAN_OPERATION, '$.countryId');
    VL_COMPANY_ID               := JSON_VALUE(PA_DATA_LOAN_OPERATION, '$.companyId');
    VL_BUSINESS_UNIT_ID         := JSON_VALUE(PA_DATA_LOAN_OPERATION, '$.businessUnitId');
    VL_LOAN_ID                  := JSON_VALUE(PA_DATA_LOAN_OPERATION, '$.loanId');
    VL_ADMIN_CENTER_ID          := JSON_VALUE(PA_DATA_LOAN_OPERATION, '$.adminCenterId');
    VL_OPERATION_TYPE_ID        := JSON_VALUE(PA_DATA_LOAN_OPERATION, '$.operationTypeId');
    VL_TRANSACTION              := JSON_VALUE(PA_DATA_LOAN_OPERATION, '$.transaction');
    VL_PLATFORM_ID              := JSON_VALUE(PA_DATA_LOAN_OPERATION, '$.platformId');
    VL_SUB_PLATFORM_ID          := JSON_VALUE(PA_DATA_LOAN_OPERATION, '$.subPlatformId');
    VL_OPERATION_AMOUNT         := JSON_VALUE(PA_DATA_LOAN_OPERATION, '$.operationAmount');
    VL_APPLICATION_DATE_CHAR    := JSON_VALUE(PA_DATA_LOAN_OPERATION, '$.applicationDate');
    VL_END_USER                 := JSON_VALUE(PA_DATA_LOAN_OPERATION, '$.endUser');
    VL_UUID_TRACKING            := JSON_VALUE(PA_DATA_LOAN_OPERATION, '$.uuidTracking');
    VL_GPS_LATITUDE             := JSON_VALUE(PA_DATA_LOAN_OPERATION, '$.gpsLatitude');
    VL_GPS_LONGITUDE            := JSON_VALUE(PA_DATA_LOAN_OPERATION, '$.gpsLongitude');
    VL_IP_ADDRESS               := JSON_VALUE(PA_DATA_LOAN_OPERATION, '$.ipAddress');
    VL_DEVICE                   := JSON_VALUE(PA_DATA_LOAN_OPERATION, '$.device');
    VL_APPLICATION_DATE         := CAST(TO_TIMESTAMP_TZ(VL_APPLICATION_DATE_CHAR, CSG_FORMAT_DATE) AS DATE);

      INSERT INTO SC_CREDIT.TA_LOAN_OPERATION (
        FI_LOAN_OPERATION_ID,
        FI_COUNTRY_ID,
        FI_COMPANY_ID,
        FI_BUSINESS_UNIT_ID,
        FI_LOAN_ID,
        FI_ADMIN_CENTER_ID,
        FI_OPERATION_TYPE_ID,
        FI_TRANSACTION,
        FC_PLATFORM_ID,
        FC_SUB_PLATFORM_ID,
        FN_OPERATION_AMOUNT,
        FD_APPLICATION_DATE,
        FD_OPERATION_DATE,
        FI_STATUS,
        FC_END_USER,
        FC_UUID_TRACKING,
        FC_GPS_LATITUDE,
        FC_GPS_LONGITUDE,
        FC_IP_ADDRESS,
        FC_DEVICE,
        FC_USER,
        FD_CREATED_DATE,
        FD_MODIFICATION_DATE)
      VALUES (
        PA_LOAN_OPERATION_ID
        ,VL_COUNTRY_ID
        ,VL_COMPANY_ID
        ,VL_BUSINESS_UNIT_ID
        ,VL_LOAN_ID
        ,VL_ADMIN_CENTER_ID
        ,VL_OPERATION_TYPE_ID
        ,VL_TRANSACTION
        ,VL_PLATFORM_ID
        ,VL_SUB_PLATFORM_ID
        ,VL_OPERATION_AMOUNT
        ,VL_APPLICATION_DATE
        ,CSG_CURRENT_DATE
        ,CSG_ONE
        ,VL_END_USER
        ,VL_UUID_TRACKING
        ,VL_GPS_LATITUDE
        ,VL_GPS_LONGITUDE
        ,VL_IP_ADDRESS
        ,VL_DEVICE
        ,CSG_CURRENT_USER
        ,CSG_CURRENT_DATE
        ,CSG_CURRENT_DATE);

    PA_STATUS_CODE := CSG_SUCCESS_CODE;
    PA_STATUS_MSG := CSG_SUCCESS_MSG;

  EXCEPTION
    WHEN EXC_FOREING_KEY_VIOLATION THEN
    ROLLBACK;
    PA_STATUS_CODE := CSG_FOREING_KEY_CODE;
    PA_STATUS_MSG := CSG_FOREING_KEY_MSG || CSG_ARROW ||SQLERRM|| DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_OPERATION, SQLCODE, SQLERRM,
      DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, VL_UUID_TRACKING, PA_DATA_LOAN_OPERATION);
    WHEN EX_DUPLICATE_PK THEN
    ROLLBACK;
    PA_STATUS_CODE := CSG_PRIMARY_KEY_CODE;
    PA_STATUS_MSG := CSG_PRIMARY_KEY_MSG || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_OPERATION, SQLCODE, SQLERRM,
      DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, VL_UUID_TRACKING, PA_DATA_LOAN_OPERATION);
    WHEN OTHERS THEN
    ROLLBACK;
      PA_STATUS_CODE := SQLCODE;
      PA_STATUS_MSG := SQLERRM || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
      SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_OPERATION, SQLCODE, SQLERRM,
        DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, VL_UUID_TRACKING, PA_DATA_LOAN_OPERATION);
  END SP_INS_LOAN_OPERATION;

  PROCEDURE SP_INS_LOAN_OPERATION_DETAIL (
    PA_DATA_LOAN_OPERATION_DETAIL               IN CLOB
    ,PA_LOAN_OPERATION_ID IN NUMBER
    ,PA_STATUS_CODE       OUT NUMBER
    ,PA_STATUS_MSG        OUT VARCHAR2)
  IS
/* **************************************************************
* PROJECT: NCP-OUTSTANDING BALANCE
* DESCRIPTION: PROCEDURE TO INSERT OPERATION DETAIL
************************************************************** */
    -- LOCAL CONSTANTS
    CSL_INSERT_OPERATION_DETAIL CONSTANT VARCHAR2(50) := 'SP_INS_LOAN_OPERATION_DETAIL';
    -- EXCEPTIONS
  EXC_DATA_NOT_SAVED EXCEPTION;
  PRAGMA EXCEPTION_INIT(EXC_DATA_NOT_SAVED, CSG_DATA_NOT_SAVED_CODE);
  EXC_FOREING_KEY_VIOLATION EXCEPTION;
  PRAGMA EXCEPTION_INIT(EXC_FOREING_KEY_VIOLATION, CSG_FOREING_KEY_CODE);
  EX_DUPLICATE_PK EXCEPTION;
  PRAGMA EXCEPTION_INIT(EX_DUPLICATE_PK, CSG_PRIMARY_KEY_CODE);

  TYPE REC_LOAN_OPERATION_DETAIL_TYPE IS RECORD (
    FI_LOAN_ID  SC_CREDIT.TA_LOAN.FI_LOAN_ID%TYPE
    ,FI_ADMIN_CENTER_ID  SC_CREDIT.TA_LOAN_OPERATION_DETAIL.FI_ADMIN_CENTER_ID%TYPE
    ,FI_LOAN_CONCEPT_ID SC_CREDIT.TA_LOAN_OPERATION_DETAIL.FI_LOAN_CONCEPT_ID%TYPE
    ,FN_ITEM_AMOUNT     SC_CREDIT.TA_LOAN_OPERATION_DETAIL.FN_ITEM_AMOUNT%TYPE);

    VL_LOAN_OPERATION_DETAIL_DATA REC_LOAN_OPERATION_DETAIL_TYPE;
    VL_INSERT_COUNT               NUMBER(3) := CSG_ZERO;

  BEGIN
    FOR JSON_REC IN (
      SELECT
        OD.FI_LOAN_ID
        ,OD.FI_ADMIN_CENTER_ID
        ,OD.FI_LOAN_CONCEPT_ID
        ,OD.FN_ITEM_AMOUNT
      FROM JSON_TABLE(
        PA_DATA_LOAN_OPERATION_DETAIL
        ,'$[*]'
      COLUMNS (
        FI_LOAN_ID NUMBER (15) PATH '$.loanId'
        ,FI_ADMIN_CENTER_ID NUMBER(8) PATH '$.adminCenterId'
        ,FI_LOAN_CONCEPT_ID  NUMBER(5) PATH '$.loanConceptId'
        ,FN_ITEM_AMOUNT     NUMBER(12,2) PATH '$.itemAmount'))
        OD)
      LOOP
        VL_LOAN_OPERATION_DETAIL_DATA.FI_LOAN_ID           :=   JSON_REC.FI_LOAN_ID;
        VL_LOAN_OPERATION_DETAIL_DATA.FI_ADMIN_CENTER_ID   := JSON_REC.FI_ADMIN_CENTER_ID;
        VL_LOAN_OPERATION_DETAIL_DATA.FI_LOAN_CONCEPT_ID  := JSON_REC.FI_LOAN_CONCEPT_ID;
        VL_LOAN_OPERATION_DETAIL_DATA.FN_ITEM_AMOUNT      := JSON_REC.FN_ITEM_AMOUNT;

      INSERT INTO SC_CREDIT.TA_LOAN_OPERATION_DETAIL (
        FI_LOAN_ID
        ,FI_LOAN_OPERATION_ID
        ,FI_ADMIN_CENTER_ID
        ,FI_LOAN_CONCEPT_ID
        ,FN_ITEM_AMOUNT
        ,FC_USER
        ,FD_CREATED_DATE
        ,FD_MODIFICATION_DATE)
      VALUES (
        VL_LOAN_OPERATION_DETAIL_DATA.FI_LOAN_ID
        ,PA_LOAN_OPERATION_ID
        ,VL_LOAN_OPERATION_DETAIL_DATA.FI_ADMIN_CENTER_ID
        ,VL_LOAN_OPERATION_DETAIL_DATA.FI_LOAN_CONCEPT_ID
        ,VL_LOAN_OPERATION_DETAIL_DATA.FN_ITEM_AMOUNT
        ,CSG_CURRENT_USER
        ,CSG_CURRENT_DATE
       ,CSG_CURRENT_DATE);

        VL_INSERT_COUNT := VL_INSERT_COUNT + CSG_ONE;
      END LOOP;

      IF VL_INSERT_COUNT = CSG_ZERO THEN
        RAISE EXC_DATA_NOT_SAVED;
      END IF;

    PA_STATUS_CODE := CSG_SUCCESS_CODE;
    PA_STATUS_MSG := CSG_SUCCESS_MSG;

  EXCEPTION
    WHEN EXC_FOREING_KEY_VIOLATION THEN
    ROLLBACK;
    PA_STATUS_CODE := CSG_FOREING_KEY_CODE;
    PA_STATUS_MSG := CSG_FOREING_KEY_MSG || CSG_ARROW ||SQLERRM|| DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_OPERATION_DETAIL, SQLCODE, SQLERRM, DBMS_UTILITY.FORMAT_ERROR_BACKTRACE,
      CSG_X, PA_DATA_LOAN_OPERATION_DETAIL);
    WHEN EX_DUPLICATE_PK THEN
    ROLLBACK;
    PA_STATUS_CODE := CSG_PRIMARY_KEY_CODE;
    PA_STATUS_MSG := CSG_PRIMARY_KEY_MSG || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_OPERATION_DETAIL, SQLCODE, SQLERRM, DBMS_UTILITY.FORMAT_ERROR_BACKTRACE,
      CSG_X, PA_DATA_LOAN_OPERATION_DETAIL);
    WHEN EXC_DATA_NOT_SAVED THEN
    ROLLBACK;
      PA_STATUS_CODE := CSG_DATA_NOT_SAVED_CODE;
      PA_STATUS_MSG := CSG_DATA_NOT_SAVED_MSG || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
      SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_OPERATION_DETAIL, SQLCODE, SQLERRM, DBMS_UTILITY.FORMAT_ERROR_BACKTRACE,
        CSG_X, PA_DATA_LOAN_OPERATION_DETAIL);
    WHEN OTHERS THEN
    ROLLBACK;
      PA_STATUS_CODE := SQLCODE;
      PA_STATUS_MSG := SQLERRM || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
      SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_OPERATION_DETAIL, SQLCODE, SQLERRM,
        DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, PA_DATA_LOAN_OPERATION_DETAIL);

  END SP_INS_LOAN_OPERATION_DETAIL;

  PROCEDURE SP_INS_LOAN_BALANCE (
    PA_DATA_LOAN_BALANCE   IN CLOB
    ,PA_LOAN_BALANCE_ID    IN NUMBER
    ,PA_LOAN_OPERATION_ID  IN NUMBER
    ,PA_STATUS_CODE        OUT NUMBER
    ,PA_STATUS_MSG         OUT VARCHAR2)
  IS
/* **************************************************************
* PROJECT: NCP-OUTSTANDING BALANCE
* DESCRIPTION: PROCEDURE TO INSERT LOAN BALANCE
************************************************************** */
    -- LOCAL CONSTANTS
    CSL_INSERT_BALANCE       CONSTANT VARCHAR2(50) := 'SP_INS_LOAN_BALANCE';
    -- VARIABLES
    VL_ADMIN_CENTER_ID            NUMBER(8);
    VL_LOAN_ID                    NUMBER(15);
    VL_PRINCIPAL_BALANCE          NUMBER(12,2);
    VL_FINANCE_CHARGE_BALANCE     NUMBER(12,2);
    VL_ADDITIONAL_CHARGE_BALANCE  NUMBER(12,2);
    VL_INSERT_ROWS                NUMBER(3);
  --EXCEPCION
  EXC_DATA_NOT_SAVED EXCEPTION;
  PRAGMA EXCEPTION_INIT(EXC_DATA_NOT_SAVED, CSG_DATA_NOT_SAVED_CODE);
  EXC_FOREING_KEY_VIOLATION EXCEPTION;
  PRAGMA EXCEPTION_INIT(EXC_FOREING_KEY_VIOLATION, CSG_FOREING_KEY_CODE);
  EX_DUPLICATE_PK EXCEPTION;
  PRAGMA EXCEPTION_INIT(EX_DUPLICATE_PK, CSG_PRIMARY_KEY_CODE);

  BEGIN
        VL_ADMIN_CENTER_ID              := JSON_VALUE(PA_DATA_LOAN_BALANCE, '$.adminCenterId');
        VL_LOAN_ID                      := JSON_VALUE(PA_DATA_LOAN_BALANCE, '$.loanId');
        VL_PRINCIPAL_BALANCE            := JSON_VALUE(PA_DATA_LOAN_BALANCE, '$.principalBalance');
        VL_FINANCE_CHARGE_BALANCE       := JSON_VALUE(PA_DATA_LOAN_BALANCE, '$.financeChargeBalance');
        VL_ADDITIONAL_CHARGE_BALANCE    := JSON_VALUE(PA_DATA_LOAN_BALANCE, '$.additionalChargeBalance');

      INSERT INTO SC_CREDIT.TA_LOAN_BALANCE (
        FI_LOAN_BALANCE_ID
        ,FI_ADMIN_CENTER_ID
        ,FI_LOAN_ID
        ,FI_LOAN_OPERATION_ID
        ,FI_BALANCE_SEQ
        ,FN_PRINCIPAL_BALANCE
        ,FN_FINANCE_CHARGE_BALANCE
        ,FN_ADDITIONAL_CHARGE_BALANCE
        ,FC_USER
        ,FD_CREATED_DATE
        ,FD_MODIFICATION_DATE)
      VALUES (
        PA_LOAN_BALANCE_ID
        ,VL_ADMIN_CENTER_ID
        ,VL_LOAN_ID
        ,PA_LOAN_OPERATION_ID
        ,CSG_ONE
        ,VL_PRINCIPAL_BALANCE
        ,VL_FINANCE_CHARGE_BALANCE
        ,VL_ADDITIONAL_CHARGE_BALANCE
        ,CSG_CURRENT_USER
        ,CSG_CURRENT_DATE
        ,CSG_CURRENT_DATE);

       VL_INSERT_ROWS := SQL%ROWCOUNT;
       IF VL_INSERT_ROWS = CSG_ZERO THEN
        RAISE EXC_DATA_NOT_SAVED;
       END IF;

    UPDATE SC_CREDIT.TA_LOAN SET FI_CURRENT_BALANCE_SEQ = CSG_ONE WHERE FI_LOAN_ID = VL_LOAN_ID
    AND FI_ADMIN_CENTER_ID = VL_ADMIN_CENTER_ID;

    PA_STATUS_CODE := CSG_SUCCESS_CODE;
    PA_STATUS_MSG := CSG_SUCCESS_MSG;

  EXCEPTION
    WHEN EXC_FOREING_KEY_VIOLATION THEN
    ROLLBACK;
    PA_STATUS_CODE := CSG_FOREING_KEY_CODE;
    PA_STATUS_MSG := CSG_FOREING_KEY_MSG || CSG_ARROW ||SQLERRM|| DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_BALANCE, SQLCODE, SQLERRM,
      DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, PA_DATA_LOAN_BALANCE);
    WHEN EX_DUPLICATE_PK THEN
    ROLLBACK;
    PA_STATUS_CODE := CSG_PRIMARY_KEY_CODE;
    PA_STATUS_MSG := CSG_PRIMARY_KEY_MSG || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_BALANCE, SQLCODE, SQLERRM,
      DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, PA_DATA_LOAN_BALANCE);
    WHEN EXC_DATA_NOT_SAVED THEN
    ROLLBACK;
      PA_STATUS_CODE := CSG_DATA_NOT_SAVED_CODE;
      PA_STATUS_MSG := CSG_DATA_NOT_SAVED_MSG || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
      SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_BALANCE, SQLCODE, SQLERRM,
        DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, PA_DATA_LOAN_BALANCE);
    WHEN OTHERS THEN
    ROLLBACK;
      PA_STATUS_CODE := SQLCODE;
      PA_STATUS_MSG := SQLERRM || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
      SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_BALANCE, SQLCODE, SQLERRM,
        DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, PA_DATA_LOAN_BALANCE);
  END SP_INS_LOAN_BALANCE;

  PROCEDURE SP_INS_LOAN_BALANCE_DETAIL (
    PA_DATA_LOAN_BALANCE_DETAIL               IN CLOB
    ,PA_LOAN_BALANCE_ID   IN NUMBER
    ,PA_STATUS_CODE       OUT NUMBER
    ,PA_STATUS_MSG        OUT VARCHAR2)
  IS
/* **************************************************************
* PROJECT: NCP-OUTSTANDING BALANCE
* DESCRIPTION: PROCEDURE TO INSERT LOAN BALANCE DETAIL
************************************************************** */
    -- LOCAL CONSTANTS
    CSL_INSERT_BALANCE_DETAIL   CONSTANT VARCHAR2(50) := 'SP_INS_LOAN_BALANCE_DETAIL';
    -- EXCEPTIONS
  EXC_DATA_NOT_SAVED EXCEPTION;
  PRAGMA EXCEPTION_INIT(EXC_DATA_NOT_SAVED, CSG_DATA_NOT_SAVED_CODE);
  EXC_FOREING_KEY_VIOLATION EXCEPTION;
  PRAGMA EXCEPTION_INIT(EXC_FOREING_KEY_VIOLATION, CSG_FOREING_KEY_CODE);
  EX_DUPLICATE_PK EXCEPTION;
  PRAGMA EXCEPTION_INIT(EX_DUPLICATE_PK, CSG_PRIMARY_KEY_CODE);
    -- TYPES
  TYPE REC_LOAN_BALANCE_DETAIL_TYPE IS RECORD (
    FI_LOAN_ID  SC_CREDIT.TA_LOAN.FI_LOAN_ID%TYPE
    ,FI_ADMIN_CENTER_ID    SC_CREDIT.TA_LOAN_BALANCE_DETAIL.FI_ADMIN_CENTER_ID%TYPE
    ,FI_LOAN_CONCEPT_ID   SC_CREDIT.TA_LOAN_BALANCE_DETAIL.FI_LOAN_CONCEPT_ID%TYPE
    ,FN_ITEM_AMOUNT       SC_CREDIT.TA_LOAN_BALANCE_DETAIL.FN_ITEM_AMOUNT%TYPE);

    VL_LOAN_BALANCE_DETAIL_DATA REC_LOAN_BALANCE_DETAIL_TYPE;
    VL_INSERT_COUNT      NUMBER(3) := CSG_ZERO;

  BEGIN
    FOR JSON_REC IN (
      SELECT
        BD.FI_LOAN_ID
        ,BD.FI_ADMIN_CENTER_ID
        ,BD.FI_LOAN_CONCEPT_ID
        ,BD.FN_ITEM_AMOUNT
      FROM JSON_TABLE(
        PA_DATA_LOAN_BALANCE_DETAIL
        ,'$[*]'
      COLUMNS (
        FI_LOAN_ID             NUMBER(15) PATH '$.loanId'
        ,FI_ADMIN_CENTER_ID    NUMBER(8) PATH '$.adminCenterId'
        ,FI_LOAN_CONCEPT_ID   NUMBER(5) PATH '$.loanConceptId'
        ,FN_ITEM_AMOUNT       NUMBER(12,2) PATH '$.itemAmount'))
        BD)
      LOOP
      VL_LOAN_BALANCE_DETAIL_DATA.FI_LOAN_ID            := JSON_REC.FI_LOAN_ID;
        VL_LOAN_BALANCE_DETAIL_DATA.FI_ADMIN_CENTER_ID  := JSON_REC.FI_ADMIN_CENTER_ID;
        VL_LOAN_BALANCE_DETAIL_DATA.FI_LOAN_CONCEPT_ID  := JSON_REC.FI_LOAN_CONCEPT_ID;
        VL_LOAN_BALANCE_DETAIL_DATA.FN_ITEM_AMOUNT      := JSON_REC.FN_ITEM_AMOUNT;

      INSERT INTO SC_CREDIT.TA_LOAN_BALANCE_DETAIL (
        FI_LOAN_ID
        ,FI_LOAN_BALANCE_ID
        ,FI_ADMIN_CENTER_ID
        ,FI_LOAN_CONCEPT_ID
        ,FN_ITEM_AMOUNT
        ,FC_USER
        ,FD_CREATED_DATE
        ,FD_MODIFICATION_DATE)
      VALUES (
        VL_LOAN_BALANCE_DETAIL_DATA.FI_LOAN_ID
        ,PA_LOAN_BALANCE_ID
        ,VL_LOAN_BALANCE_DETAIL_DATA.FI_ADMIN_CENTER_ID
        ,VL_LOAN_BALANCE_DETAIL_DATA.FI_LOAN_CONCEPT_ID
        ,VL_LOAN_BALANCE_DETAIL_DATA.FN_ITEM_AMOUNT
        ,CSG_CURRENT_USER
        ,CSG_CURRENT_DATE
        ,CSG_CURRENT_DATE);

        VL_INSERT_COUNT := VL_INSERT_COUNT + CSG_ONE;
      END LOOP;

      IF VL_INSERT_COUNT = CSG_ZERO THEN
        RAISE EXC_DATA_NOT_SAVED;
      END IF;

    PA_STATUS_CODE := CSG_SUCCESS_CODE;
    PA_STATUS_MSG := CSG_SUCCESS_MSG;

  EXCEPTION
    WHEN EXC_FOREING_KEY_VIOLATION THEN
    ROLLBACK;
    PA_STATUS_CODE := CSG_FOREING_KEY_CODE;
    PA_STATUS_MSG := CSG_FOREING_KEY_MSG || CSG_ARROW ||SQLERRM|| DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_BALANCE_DETAIL, SQLCODE, SQLERRM,
      DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, PA_DATA_LOAN_BALANCE_DETAIL);
    WHEN EX_DUPLICATE_PK THEN
    ROLLBACK;
    PA_STATUS_CODE := CSG_PRIMARY_KEY_CODE;
    PA_STATUS_MSG := CSG_PRIMARY_KEY_MSG || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_BALANCE_DETAIL, SQLCODE, SQLERRM,
      DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, PA_DATA_LOAN_BALANCE_DETAIL);
    WHEN EXC_DATA_NOT_SAVED THEN
    ROLLBACK;
      PA_STATUS_CODE := CSG_DATA_NOT_SAVED_CODE;
      PA_STATUS_MSG := CSG_DATA_NOT_SAVED_MSG || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
      SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_BALANCE_DETAIL, SQLCODE, SQLERRM,
        DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, PA_DATA_LOAN_BALANCE_DETAIL);
    WHEN OTHERS THEN
    ROLLBACK;
      PA_STATUS_CODE := SQLCODE;
      PA_STATUS_MSG := SQLERRM || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
      SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_BALANCE_DETAIL, SQLCODE, SQLERRM,
        DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, PA_DATA_LOAN_BALANCE_DETAIL);
  END SP_INS_LOAN_BALANCE_DETAIL;

  PROCEDURE SP_INS_PAYMENT_SCHEDULE_FEE (
    PA_PAYMENT_SCHEDULE_FEE_DATA    IN CLOB,
    PA_STATUS_CODE                  OUT NUMBER,
    PA_STATUS_MSG                   OUT VARCHAR2)
  AS
/* **************************************************************
* PROJECT: NCP-OUTSTANDING BALANCE
* DESCRIPTION: PROCEDURE TO INSERT PAYMENT SCHEDULE FEE
************************************************************** */
  --LOCAL CONSTANS
    CSL_INSERT_PAYMENT_FEE       CONSTANT VARCHAR2(50) := 'SP_INS_PAYMENT_SCHEDULE_FEE';
  -- EXCEPTIONS
    EXC_DATA_NOT_SAVED           EXCEPTION;
    PRAGMA EXCEPTION_INIT (EXC_DATA_NOT_SAVED, CSG_DATA_NOT_SAVED_CODE);

  TYPE REC_PAYMENT_SCHEDULE_FEE_TYPE IS RECORD (
    FI_PAYMENT_SCHEDULE_ID          SC_CREDIT.TA_PAYMENT_SCHEDULE_FEE.FI_PAYMENT_SCHEDULE_ID%TYPE,
    FI_LOAN_ID                      SC_CREDIT.TA_PAYMENT_SCHEDULE_FEE.FI_LOAN_ID%TYPE,
    FI_ADMIN_CENTER_ID              SC_CREDIT.TA_PAYMENT_SCHEDULE_FEE.FI_ADMIN_CENTER_ID%TYPE,
    FI_LOAN_CONCEPT_ID              SC_CREDIT.TA_PAYMENT_SCHEDULE_FEE.FI_LOAN_CONCEPT_ID%TYPE,
    FN_FEE_AMOUNT                   SC_CREDIT.TA_PAYMENT_SCHEDULE_FEE.FN_FEE_AMOUNT%TYPE,
    FN_FEE_PAYMENT_BALANCE          SC_CREDIT.TA_PAYMENT_SCHEDULE_FEE.FN_FEE_PAYMENT_BALANCE%TYPE);

    VL_PAYMENT_SCHEDULE_FEE_DATA    REC_PAYMENT_SCHEDULE_FEE_TYPE;
    VL_FEE_SEQ                      NUMBER(3);

  BEGIN
     FOR JSON_REC IN (
    SELECT
      PSF.FI_PAYMENT_SCHEDULE_ID,
      PSF.FI_LOAN_ID,
      PSF.FI_ADMIN_CENTER_ID,
      PSF.FI_LOAN_CONCEPT_ID,
      PSF.FN_FEE_AMOUNT,
      PSF.FN_FEE_PAYMENT_BALANCE
    FROM JSON_TABLE (
      PA_PAYMENT_SCHEDULE_FEE_DATA,
      '$[*]'
    COLUMNS (
      FI_PAYMENT_SCHEDULE_ID    NUMBER(3) PATH '$.paymentScheduleId',
      FI_LOAN_ID                NUMBER(15) PATH '$.loanId',
      FI_ADMIN_CENTER_ID        NUMBER(8) PATH '$.adminCenterId',
      FI_LOAN_CONCEPT_ID        NUMBER(3) PATH '$.loanConceptId',
      FN_FEE_AMOUNT             NUMBER(12,2) PATH '$.feeAmount',
      FN_FEE_PAYMENT_BALANCE    NUMBER(12,2) PATH '$.feePaymentBalance'))
    PSF)
  LOOP
    VL_PAYMENT_SCHEDULE_FEE_DATA.FI_PAYMENT_SCHEDULE_ID     := JSON_REC.FI_PAYMENT_SCHEDULE_ID;
    VL_PAYMENT_SCHEDULE_FEE_DATA.FI_LOAN_ID                 := JSON_REC.FI_LOAN_ID;
    VL_PAYMENT_SCHEDULE_FEE_DATA.FI_ADMIN_CENTER_ID         := JSON_REC.FI_ADMIN_CENTER_ID;
    VL_PAYMENT_SCHEDULE_FEE_DATA.FI_LOAN_CONCEPT_ID         := JSON_REC.FI_LOAN_CONCEPT_ID;
    VL_PAYMENT_SCHEDULE_FEE_DATA.FN_FEE_AMOUNT              := JSON_REC.FN_FEE_AMOUNT;
    VL_PAYMENT_SCHEDULE_FEE_DATA.FN_FEE_PAYMENT_BALANCE     := JSON_REC.FN_FEE_PAYMENT_BALANCE;

  SELECT NVL(MAX(FI_FEE_SEQ), CSG_ZERO) + CSG_ONE
    INTO VL_FEE_SEQ
    FROM SC_CREDIT.TA_PAYMENT_SCHEDULE_FEE
    WHERE FI_PAYMENT_SCHEDULE_ID = JSON_REC.FI_PAYMENT_SCHEDULE_ID
    AND FI_LOAN_ID = JSON_REC.FI_LOAN_ID
    AND FI_ADMIN_CENTER_ID = JSON_REC.FI_ADMIN_CENTER_ID;

  INSERT INTO SC_CREDIT.TA_PAYMENT_SCHEDULE_FEE (
    FI_PAYMENT_SCHEDULE_ID
    ,FI_FEE_SEQ
    ,FI_LOAN_ID
    ,FI_ADMIN_CENTER_ID
    ,FI_LOAN_CONCEPT_ID
    ,FN_FEE_AMOUNT
    ,FN_FEE_PAYMENT_BALANCE
    ,FC_USER
    ,FD_CREATED_DATE
    ,FD_MODIFICATION_DATE)

  VALUES (
    VL_PAYMENT_SCHEDULE_FEE_DATA.FI_PAYMENT_SCHEDULE_ID
    ,VL_FEE_SEQ
    ,VL_PAYMENT_SCHEDULE_FEE_DATA.FI_LOAN_ID
    ,VL_PAYMENT_SCHEDULE_FEE_DATA.FI_ADMIN_CENTER_ID
    ,VL_PAYMENT_SCHEDULE_FEE_DATA.FI_LOAN_CONCEPT_ID
    ,VL_PAYMENT_SCHEDULE_FEE_DATA.FN_FEE_AMOUNT
    ,VL_PAYMENT_SCHEDULE_FEE_DATA.FN_FEE_PAYMENT_BALANCE
    ,CSG_CURRENT_USER
    ,CSG_CURRENT_DATE
    ,CSG_CURRENT_DATE);
  END LOOP;

    PA_STATUS_CODE := CSG_SUCCESS_CODE;
    PA_STATUS_MSG := CSG_SUCCESS_MSG;

  EXCEPTION
    WHEN OTHERS THEN
    PA_STATUS_CODE := SQLCODE;
    PA_STATUS_MSG := SQLERRM || CSG_ARROW || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    SC_CREDIT.SP_ERROR_LOG(CSL_INSERT_PAYMENT_FEE, SQLCODE, SQLERRM,
      DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, CSG_X, PA_PAYMENT_SCHEDULE_FEE_DATA);
    ROLLBACK;
  END SP_INS_PAYMENT_SCHEDULE_FEE;
END PA_EXE_LOAN_ORIGINATION;

/

  GRANT EXECUTE ON SC_CREDIT.PA_EXE_LOAN_ORIGINATION TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.PA_EXE_LOAN_ORIGINATION TO USRPURPOSEWS;
--------------------------------------------------------
--  DDL for Package Body PA_EXE_OUTSTANDING_BALANCE
--------------------------------------------------------

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

  GRANT EXECUTE ON SC_CREDIT.PA_EXE_OUTSTANDING_BALANCE TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.PA_EXE_OUTSTANDING_BALANCE TO USRPURPOSEWS;
  GRANT EXECUTE ON SC_CREDIT.PA_EXE_OUTSTANDING_BALANCE TO USRCREDIT02;
--------------------------------------------------------
--  DDL for Package Body PA_LOAN_BALANCE
--------------------------------------------------------

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

  GRANT EXECUTE ON SC_CREDIT.PA_LOAN_BALANCE TO USRNCPCREDIT1;
--------------------------------------------------------
--  DDL for Package Body PA_LOAN_CANCEL
--------------------------------------------------------

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

  GRANT EXECUTE ON SC_CREDIT.PA_LOAN_CANCEL TO USRNCPCREDIT1;
--------------------------------------------------------
--  DDL for Package Body PA_MISCELLANEOUS_OPERATIONS
--------------------------------------------------------

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

  GRANT EXECUTE ON SC_CREDIT.PA_MISCELLANEOUS_OPERATIONS TO USRNCPCREDIT1;
--------------------------------------------------------
--  DDL for Package Body PA_TMP_LOAN_PROCESS
--------------------------------------------------------

  CREATE OR REPLACE  PACKAGE BODY SC_CREDIT.PA_TMP_LOAN_PROCESS 
   AS
      /*
      *PROYECTO: CORE BANCARIO
      *DESCRIPCION: PROCEDIMIENTOS DE LAS OPERACIONES CRUD
      *OPERACIONES:
      * 1. SELECT
      * 2. INSERT
      * 3. UPDATE
      * 4. DELETE
      */

   --TIPOS DE OPERACIONES
   CSG_OPSELECT      CONSTANT SIMPLE_INTEGER := 1;
   CSG_OPINSERT      CONSTANT SIMPLE_INTEGER := 2;
   CSG_OPUPDATE      CONSTANT SIMPLE_INTEGER := 3;
   CSG_OPDELETE      CONSTANT SIMPLE_INTEGER := 4;

   --CONSTANTES CODIGOS DE RESPUESTA
   CSG_CODERROR404   CONSTANT SIMPLE_INTEGER := 404;
   CSG_CODERROR400   CONSTANT SIMPLE_INTEGER := 400;
   CSG_CODERROR500   CONSTANT SIMPLE_INTEGER := 500;
   CSG_CODOK200      CONSTANT SIMPLE_INTEGER := 200;

   --CONSTANTES GENERICAS
   CSG_0             CONSTANT SIMPLE_INTEGER := 0;
   CSG_1             CONSTANT SIMPLE_INTEGER := 1;
   CSG_FMTFECHA      CONSTANT VARCHAR2(30 CHAR):= 'DD/MM/YYYY hh24:mi:ss';
   CSG_MAXREGSEL     CONSTANT SIMPLE_INTEGER := 200;

   --CONSTANTES MANEJO DE ERRORES
   CSG_FIPKG         CONSTANT SIMPLE_INTEGER := 1;
   CSG_FISP          CONSTANT SIMPLE_INTEGER := 2;
   CSG_ACCIONINVALID CONSTANT SIMPLE_INTEGER := -20003;


   -- Mensajes de Error
   CSG_OPNOVALIDA    CONSTANT VARCHAR2(40 CHAR) := 'Accion no parametrizada-';
   CSG_SINACTUALIZAR CONSTANT VARCHAR2(40 CHAR) := 'No se encontraron datos';
   CSG_EXITOSA       CONSTANT VARCHAR2(40 CHAR) := 'Operacion exitosa';
   CSG_IDLOG         CONSTANT VARCHAR2(40 CHAR) := 'IDLOG (%S), ';
   CSG_SUST          CONSTANT VARCHAR2(40 CHAR) := '%S';
   CSG_PUNTO         CONSTANT VARCHAR2(40 CHAR) := '.';
   VG_CONTEOREG      NUMBER(10);

   /*
   *DESCRIPCION: PROCEDIMIENTO PARA LA ASIGNACION DE VARIABLES DE SALIDA
   *  INPUT PARAMETERS
   *     VG_CONTEOREG
   *     PA_ACCION
   *  OUTPUT PARAMETERS
   *     PA_CODIGO
   *     PA_DESCRIPCION
   */
   PROCEDURE SPGENERARESP(
      VG_CONTEOREG             IN NUMBER,
      PA_ACCION                IN NUMBER,
      PA_CODIGO                OUT NUMBER,
      PA_DESCRIPCION           OUT VARCHAR2)
   AS
   BEGIN
      -- Asigna codigo de salida
      IF VG_CONTEOREG = CSG_0 AND  PA_ACCION != CSG_OPSELECT
      THEN
         PA_CODIGO := CSG_CODERROR404;
         PA_DESCRIPCION := CSG_SINACTUALIZAR;
      ELSE
         -- Se envia codigo 200
         PA_CODIGO := CSG_CODOK200;
         -- Se envia respuesta exitosa del SPTACOMPLEMIVA
         PA_DESCRIPCION := CSG_EXITOSA;
      END IF;
   END;


   /*
   *DESCRIPCION: PROCEDIMIENTO PARA LA ASIGNACION DE VARIABLES DE SALIDA
   *  INPUT PARAMETERS
   *     PA_ERRCODE
   *     PA_FIIDLOG
   *     PA_ERRMSG
   *  OUTPUT PARAMETERS
   *     PA_CODIGO
   *     PA_DESCRIPCION
   */
   PROCEDURE SPGENERARESPERR(
   PA_ERRCODE              IN NUMBER,
   PA_FIIDLOG              IN NUMBER,
   PA_ERRMSG               IN VARCHAR2,
   PA_CODIGO               OUT NUMBER,
   PA_DESCRIPCION          OUT VARCHAR2)
   AS
   BEGIN
      PA_CODIGO := CASE PA_ERRCODE WHEN CSG_ACCIONINVALID THEN CSG_CODERROR400
                                ELSE CSG_CODERROR500
                   END;
      PA_DESCRIPCION :=REPLACE(CSG_IDLOG,CSG_SUST, PA_FIIDLOG) || PA_ERRMSG;
   END;

   /*
   *DESCRIPCION: PROCEDIMIENTO QUE REALIZA LAS OPERACIONES CRUD DE LA TABLA     TA_TMP_LOAN_PROCESS
   */

   PROCEDURE SPTA_TMP_LOAN_PROCESS(
      PA_ACCION                IN NUMBER,
      PA_FI_LOAN_ID            IN     SC_CREDIT.TA_TMP_LOAN_PROCESS.FI_LOAN_ID%TYPE,
      PA_FI_ADMIN_CENTER_ID    IN     SC_CREDIT.TA_TMP_LOAN_PROCESS.FI_ADMIN_CENTER_ID%TYPE,
      PA_FI_PROCESS            IN     SC_CREDIT.TA_TMP_LOAN_PROCESS.FI_PROCESS%TYPE,
      PA_FC_USER               IN VARCHAR2,
      PA_FD_CREATED_DATE       IN VARCHAR2,
      PA_FD_MODIFICATION_DATE  IN VARCHAR2,
      PA_FI_TRACK              IN     SC_CREDIT.TA_TMP_LOAN_PROCESS.FI_TRACK%TYPE,
      PA_STATUS_CODE                OUT NUMBER,
      PA_STATUS_MSG           OUT VARCHAR2,
      PA_CURSOR                OUT   SC_CREDIT.PA_TYPES.TYP_CURSOR)
   IS
      CSL_FCPROCESO     CONSTANT VARCHAR2(80) := 'MTO   TA_TMP_LOAN_PROCESS';

   BEGIN
      /*
       *   LECTURA DE DATOS
       */



      IF PA_ACCION = CSG_OPSELECT THEN
           OPEN PA_CURSOR FOR
         SELECT
               TA.FI_LOAN_ID                                      AS FI_LOAN_ID,
               TA.FI_ADMIN_CENTER_ID                              AS FI_ADMIN_CENTER_ID,
               TA.FI_PROCESS                                      AS FI_PROCESS,
               TA.FC_USER                                         AS FC_USER,
               TO_CHAR(TA.FD_CREATED_DATE,CSG_FMTFECHA)           AS FD_CREATED_DATE,
               TO_CHAR(TA.FD_MODIFICATION_DATE,CSG_FMTFECHA)      AS FD_MODIFICATION_DATE,
               TA.FI_TRACK                                        AS FI_TRACK
            FROM     SC_CREDIT.TA_TMP_LOAN_PROCESS TA
            WHERE  (PA_FI_LOAN_ID=FI_LOAN_ID OR PA_FI_LOAN_ID IS NULL)
               AND (PA_FI_ADMIN_CENTER_ID=FI_ADMIN_CENTER_ID OR PA_FI_ADMIN_CENTER_ID IS NULL)
               AND (PA_FI_PROCESS=FI_PROCESS OR PA_FI_PROCESS IS NULL)
                AND (PA_FI_TRACK=FI_TRACK OR PA_FI_TRACK IS NULL)
               AND ROWNUM<CSG_MAXREGSEL;

      /*
       *   INSERCION DE DATOS
       */
      ELSIF PA_ACCION =  CSG_OPINSERT THEN
         INSERT INTO     SC_CREDIT.TA_TMP_LOAN_PROCESS(
            FI_LOAN_ID,
            FI_ADMIN_CENTER_ID,
            FI_PROCESS,
            FC_USER,
            FD_CREATED_DATE,
            FD_MODIFICATION_DATE,
            FI_TRACK
         )
         VALUES (
            PA_FI_LOAN_ID,
            PA_FI_ADMIN_CENTER_ID,
            PA_FI_PROCESS,
            PA_FC_USER,
            TO_DATE(PA_FD_CREATED_DATE,CSG_FMTFECHA),
            TO_DATE(PA_FD_MODIFICATION_DATE,CSG_FMTFECHA),
            PA_FI_TRACK
         );

      /*
       *   ACTUALIZACION DE DATOS
       */
      ELSIF PA_ACCION = CSG_OPUPDATE THEN
         UPDATE     SC_CREDIT.TA_TMP_LOAN_PROCESS SET
            FC_USER = NVL(PA_FC_USER,FC_USER),
            FD_CREATED_DATE = NVL2(PA_FD_CREATED_DATE,TO_DATE(PA_FD_CREATED_DATE,CSG_FMTFECHA),FD_CREATED_DATE),
            FD_MODIFICATION_DATE = NVL2(PA_FD_MODIFICATION_DATE,TO_DATE(PA_FD_MODIFICATION_DATE,CSG_FMTFECHA),FD_MODIFICATION_DATE),
            FI_TRACK = NVL(PA_FI_TRACK,FI_TRACK)
         WHERE PA_FI_LOAN_ID=FI_LOAN_ID
            AND PA_FI_ADMIN_CENTER_ID=FI_ADMIN_CENTER_ID
            AND PA_FI_PROCESS=FI_PROCESS
            AND PA_FI_TRACK=FI_TRACK
;

      /*
       *   BORRADO DE DATOS
       */
      ELSIF PA_ACCION = CSG_OPDELETE THEN
         DELETE FROM     SC_CREDIT.TA_TMP_LOAN_PROCESS
         WHERE  PA_FI_LOAN_ID=FI_LOAN_ID
            AND PA_FI_ADMIN_CENTER_ID=FI_ADMIN_CENTER_ID
            AND PA_FI_PROCESS=FI_PROCESS
            AND PA_FI_TRACK=FI_TRACK
;
      ELSE
            RAISE_APPLICATION_ERROR( CSG_ACCIONINVALID, CSG_OPNOVALIDA || PA_ACCION );
      END IF;
      SPGENERARESP(SQL%ROWCOUNT,PA_ACCION,PA_STATUS_CODE,PA_STATUS_MSG);
          COMMIT;
   --Bloque de excepciones
   EXCEPTION
      WHEN OTHERS THEN
            ROLLBACK;
        OPEN PA_CURSOR FOR
        SELECT NULL FROM DUAL;
        PA_STATUS_CODE := SQLCODE;
        PA_STATUS_MSG := SQLERRM  || ' -> ' ||  DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
           SC_CREDIT.SP_ERROR_LOG(UTL_CALL_STACK.SUBPROGRAM(CSG_1)(CSG_FIPKG)
                                    ,SQLCODE
                                    ,SQLERRM
                                    ,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                    ,PA_FI_LOAN_ID ||',' ||PA_FI_ADMIN_CENTER_ID||','||PA_FI_PROCESS||','||PA_ACCION
                                    ,NULL);
   END SPTA_TMP_LOAN_PROCESS;

END PA_TMP_LOAN_PROCESS;

/

  GRANT EXECUTE ON SC_CREDIT.PA_TMP_LOAN_PROCESS TO USRNCPCREDIT1;
  GRANT EXECUTE ON SC_CREDIT.PA_TMP_LOAN_PROCESS TO USRCREDIT02;
