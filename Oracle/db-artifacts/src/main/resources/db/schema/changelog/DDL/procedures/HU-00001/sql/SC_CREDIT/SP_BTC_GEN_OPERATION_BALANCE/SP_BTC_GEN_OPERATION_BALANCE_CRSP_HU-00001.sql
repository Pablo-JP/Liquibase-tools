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

GRANT EXECUTE ON SC_CREDIT.SP_BTC_GEN_OPERATION_BALANCE TO USRNCPCREDIT1
/
GRANT EXECUTE ON SC_CREDIT.SP_BTC_GEN_OPERATION_BALANCE TO USRBTCCREDIT1
/
