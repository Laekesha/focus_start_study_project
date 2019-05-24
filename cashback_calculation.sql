create or replace package fs11_cashback_calcuation as

end fs11_cashback_calcuation;

create or replace package body fs11_cashback_calcuation as

    function calculation (p_client_id varchar2, p_transaction_id varchar2) return number as -- %rowtype like fs11_periods?
    begin
        null;
        /*
         p_transaction_id = fs11_purchases.id / fs11_refunds.purchase_id -- only one table for transactions is better?(
         */
        -- minus, if refund
    end;

    savepoint calculation;

    procedure cash_accrual(p_purchase_id varchar2) as
    begin
        null;
    end;

    rollback to savepoint calculation;

        /*

        */

end fs11_cashback_calcuation;