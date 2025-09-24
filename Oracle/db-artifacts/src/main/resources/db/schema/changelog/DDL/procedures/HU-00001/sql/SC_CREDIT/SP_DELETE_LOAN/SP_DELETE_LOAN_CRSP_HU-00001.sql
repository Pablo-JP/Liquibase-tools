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

GRANT EXECUTE ON SC_CREDIT.SP_DELETE_LOAN TO USRNCPCREDIT1
/
