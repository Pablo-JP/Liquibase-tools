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

GRANT EXECUTE ON SC_CREDIT.SP_SEL_TRIGGER TO USRNCPCREDIT1
/
