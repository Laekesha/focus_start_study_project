create or replace package fs11_cashback_calcuation as

end fs11_cashback_calcuation;

create or replace package body fs11_cashback_calcuation as

    function calculation (p_transaction_id varchar2) return number as
    calc_cash number;
    begin
        --null;
        -- minus, if refund

        select CLIENT_ID, SUM(AMOUNT)
        from (select CLIENT_ID, sum(PURCHASE_AMOUNT) "AMOUNT"
              from (select CARD_NUM "CARD", sum(TRANSACTION_AMOUNT) "PURCHASE_AMOUNT"
                    from FS11_PURCHASES
                         --where TRANSACTION_DATE between start_period and end_period
                    group by CARD_NUM)
                       join FS11_CARDS on FS11_CARDS.CARD_NUM = CARD
              group by CLIENT_ID
              union
              select CLIENT_ID, -sum(REFUND_AMOUNT) "AMOUNT"
              from (select CARD_NUM "CARD", sum(TRANSACTION_AMOUNT) "REFUND_AMOUNT"
                    from FS11_REFUNDS
                         --where TRANSACTION_DATE between start_period and end_period
                    group by CARD_NUM)
                       join FS11_CARDS on FS11_CARDS.CARD_NUM = CARD
              group by CLIENT_ID)
        group by CLIENT_ID;

        return calc_cash;
    end;

    savepoint calculation;

    procedure cash_accrual(p_client_id varchar2) as
    begin
        null;
        -- count(fs11_purchases.id) >= 10, 100 <= monthly_cash <= 3000.
        -- system pays sum_cash (sum(calc_cash) from all cards for one client_id and master card), but
        -- max(sum(calc_cash)) = 3000. or if _ > 3000 => = := 3000.
        -- insert into periods
    end;

    rollback to savepoint calculation;

    /*

    */

end fs11_cashback_calcuation;