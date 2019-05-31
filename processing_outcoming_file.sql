create or replace package fs11_processing_outcoming_file as
    procedure process(p_transactions TRANSACTION_TABLE, p_file_date date, p_period_id pls_integer);
end fs11_processing_outcoming_file;
/
create or replace package body fs11_processing_outcoming_file as
    response clob;
    transactions TRANSACTION_TABLE;
    file_date date;
    current_period_id number;

    type card_cashback_calc_record is record
        (card_num varchar2(40), purchases_count number, calc_cashback number);
    type card_cashback_calc_table is table of card_cashback_calc_record index by varchar2 (40);

    type client_cashback_calc_record is record
        (client_id number, transaction_count number, cashback number);
    type clients_cashback_calc_table is table of client_cashback_calc_record index by pls_integer;

    cards card_cashback_calc_table;
    clients clients_cashback_calc_table;

    procedure read_cards_cashback as
        type read_card_table is table of card_cashback_calc_record;
        read_cards read_card_table;
    begin
        select tr.CARD_NUM "CARD",
               (case
                    WHEN sum(PURCHASES_COUNT) IS NULL
                        THEN 0
                    ELSE sum(PURCHASES_COUNT)
                   END)    "PURCHASES",
               (case
                    WHEN sum(CALC_CASHBACK) IS NULL
                        THEN 0
                    ELSE sum(CALC_CASHBACK)
                   END)    "CASHBACK"
               bulk collect into read_cards
        from FS11_CARD_CASHBACKS cash
                 right join (
            select distinct CARD_NUM
            from table (transactions)) tr on
            cash.CARD_NUM = tr.CARD_NUM
        where PERIOD_ID = 201905
           or PERIOD_ID is null
        group by tr.CARD_NUM;

        for i in 1 .. read_cards.count
            loop
                cards(read_cards(i).card_num) := read_cards(i);
            end loop;
    end;

    procedure read_clients_cashback as
        type read_client_table is table of client_cashback_calc_record;
        read_clients read_client_table;
    begin
        select CLIENT_ID,
               (case
                    WHEN sum(PURCHASES_COUNT) IS NULL
                        THEN 0
                    ELSE sum(PURCHASES_COUNT)
                   END) "PURCHASES",
               (case
                    WHEN sum(CALC_CASHBACK) IS NULL
                        THEN 0
                    ELSE sum(CALC_CASHBACK)
                   END) "CASHBACK"
               bulk collect into read_clients
        from FS11_CARD_CASHBACKS cash
                 right join (
            select distinct CARD_NUM
            from table (transactions)) tr on
            tr.CARD_NUM = cash.CARD_NUM
                 join FS11_CARDS cards on tr.CARD_NUM = cards.CARD_NUM
        where PERIOD_ID = 201905
           or PERIOD_ID is null
        group by CLIENT_ID;

        for i in 1 .. read_clients.count
            loop
                clients(read_clients(i).client_id) := read_clients(i);
            end loop;
    end;

    procedure write_cards_cashback as

        type write_table is table of FS11_CARD_CASHBACKS%rowtype;
        write_cards write_table := write_table();
        idx         varchar2(40);
        i           number;
    begin

        idx := cards.first;
        while (idx is not null)
            loop
                write_cards.extend;
                i := write_cards.last;
                write_cards(i).FILE_DATE := file_date;
                write_cards(i).PERIOD_ID := current_period_id;
                write_cards(i).CARD_NUM := cards(idx).card_num;
                write_cards(i).PURCHASES_COUNT := cards(idx).purchases_count;
                write_cards(i).CALC_CASHBACK := cards(idx).calc_cashback;
                idx := cards.next(idx);
            end loop;

        forall i in 1 .. write_cards.count
            insert into FS11_CARD_CASHBACKS
            values write_cards(i);
        --commit;
    end;

    procedure process_transactions as
        cashback          number;
        cashback_response number;
    begin
        read_cards_cashback();
        read_clients_cashback();

        response := 'H;123456789012;' || to_char(sysdate, 'yyyymmddhh24miss') || chr(10);

        for r in (select ID,
                         TRANSACTION_DATE,
                         CLIENT_ID,
                         FS11_CARDS.CARD_NUM  "CARD",
                         TRANSACTION_AMOUNT   "AMOUNT",
                         merch_r.PERCENT_CASH "MERCHANT",
                         mcc_r.PERCENT_CASH   "MCC"
                  from table (transactions) tr
                           join FS11_CARDS on FS11_CARDS.CARD_NUM = tr.CARD_NUM
                           join FS11_MERCHANT_RULES merch_r on
                          merch_r.MERCHANT_ID = tr.MERCHANT_ID and
                          TRANSACTION_DATE between merch_r.START_DATE AND merch_r.END_DATE

                           join FS11_MCC_RULES mcc_r on
                          mcc_r.MCC = COMMON and
                          TRANSACTION_DATE between mcc_r.START_DATE AND mcc_r.END_DATE
                  where TRANSACTION_TYPE = 'P'

                  union

                  select ID,
                         REFUND_DATE          "TRANSACTION_DATE",
                         CLIENT_ID,
                         FS11_CARDS.CARD_NUM  "CARD",
                         -TRANSACTION_AMOUNT  "AMOUNT",
                         merch_r.PERCENT_CASH "MERCHANT",
                         mcc_r.PERCENT_CASH   "MCC"
                  from (select t1.ID,
                               t1.TRANSACTION_TYPE,
                               t1.TRANSACTION_DATE "REFUND_DATE",
                               t2.TRANSACTION_DATE "PURCHASE_DATE",
                               t1.TRANSACTION_AMOUNT,
                               t1.CARD_NUM         "CARD",
                               t2.COMMON           "PURCHASE_MCC",
                               t2.MERCHANT_ID      "PURCHASE_MERCHANT"
                        from table (transactions) t1,
                             table (transactions) t2
                        where t1.TRANSACTION_TYPE = 'R'
                          and t1.COMMON = t2.ID)

                           join FS11_CARDS on FS11_CARDS.CARD_NUM = CARD
                           join FS11_MERCHANT_RULES merch_r on
                          merch_r.MERCHANT_ID = PURCHASE_MERCHANT and
                          PURCHASE_DATE between merch_r.START_DATE AND merch_r.END_DATE
                           join FS11_MCC_RULES mcc_r on
                          mcc_r.MCC = PURCHASE_MCC and
                          PURCHASE_DATE between mcc_r.START_DATE AND mcc_r.END_DATE
                  order by TRANSACTION_DATE
            )
            loop
                if r.MCC = 0 or r.MERCHANT = 0
                then
                    cashback := 0;
                else
                    if r.MERCHANT is not null
                    then
                        cashback := r.AMOUNT * r.MERCHANT;
                    else
                        if r.MCC is not null
                        then
                            cashback := r.AMOUNT * r.MCC;
                        else
                            cashback := r.AMOUNT * 0.01;
                        end if;
                    end if;
                end if;

                cards(r.CARD).calc_cashback := cards(r.CARD).calc_cashback + cashback;
                if cashback > 0 then
                    cards(r.CARD).purchases_count := cards(r.CARD).purchases_count + 1;
                end if;

                clients(r.CLIENT_ID).cashback := clients(r.CLIENT_ID).cashback + cashback;
                if cashback > 0 then
                    clients(r.CLIENT_ID).transaction_count := clients(r.CLIENT_ID).transaction_count + 1;
                end if;

                if clients(r.CLIENT_ID).transaction_count >= 10 and
                   clients(r.CLIENT_ID).cashback >= 100
                then
                    if clients(r.CLIENT_ID).cashback <= 3000
                    then
                        cashback_response := clients(r.CLIENT_ID).cashback;
                    else
                        cashback_response := 3000;
                    end if;
                else
                    cashback_response := 0;
                end if;

                response := response || 'S;' ||
                            r.CARD || ';' ||
                            r.ID || ';' ||
                            cashback || ';' ||
                            cashback_response
                    || '   ' || clients(r.CLIENT_ID).transaction_count
                    || chr(10);
            end loop;

        response := response ||
                    'T;' || transactions.COUNT || ';0' || chr(10);
        write_cards_cashback;
--         print(response);
    end;

     procedure process(p_transactions TRANSACTION_TABLE, p_file_date date, p_period_id pls_integer) as
        file_id varchar2(12);
    begin
        clients.delete;
        cards.delete;
        transactions := p_transactions;
        file_date := p_file_date;
        current_period_id := p_period_id;
        process_transactions;
        file_id := DBMS_RANDOM.STRING('x', 12);
        insert into FS11_FILE_RECORDS values (file_id, sysdate, 'response', 'processed', null);
        insert into FS11_FILE_CONTENT values (file_id, response);
        commit;
    end;

end fs11_processing_outcoming_file;
