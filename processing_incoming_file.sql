create or replace package fs11_processing_incoming_file as
    procedure fs11_proc_file(p_file_id varchar2);
end fs11_processing_incoming_file;

create or replace package body fs11_processing_incoming_file as

    transactions TRANSACTION_TABLE;
    purchase_count number;
    refund_count number;

    type record_fields is table of varchar2(2000);
    array record_fields := record_fields();

    file_length number;
    file_pos number;
    file_content clob;
    file_id varchar2(12);

    current_period_date date;
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

    purchases_integrity exception;
    refunds_integrity exception;
    file_id_exist exception;
    trans_id_exist exception;
--     refund_more_than_purschase exception;
--     pragma exception_init (refund_more_than_purschase, -20000);
--     transaction_wrong_date exception;

    procedure print(p_message varchar2) as
    begin
        dbms_output.put_line(p_message);
    end;

    procedure print(p_message clob) as
    begin
        dbms_output.put_line(p_message);
    end;

    procedure proc_header as -- control of unique ?
    begin
        file_date := to_date(array(3), 'yyyymmddhh24miss');
--         file_id := array(2);
--         insert into FS11_FILE_RECORDS (file_id, file_name, file_date, file_type, file_status)
--         values (file_id, 'not/need', to_date(array(3), 'yyyymmddhh24miss'), 'incoming', 'new');
--         print('HEADER:');
--         fs11_print_array(array);
--     exception
--         when dup_val_on_index then raise file_id_exist;
    end;

    procedure proc_transaction as
    begin
        null;
        transactions.extend;
        transactions(transactions.last) := TRANSACTION_TYPE(
                array(1),
                array(2),
                array(3),
                to_date(array(4), 'yyyymmddhh24miss'),
                to_number(array(5)),
                array(6),
                array(7),
                array(8)
            );
--         if transactions(transactions.last).TRANSACTION_DATE not between current_period_date and file_date then
--             raise transaction_wrong_date;
--         end if;
    end;

    procedure fs11_proc_trailer as
    begin
        select count(*) into purchase_count from table(transactions) where TRANSACTION_TYPE = 'P';
        select count(*) into refund_count from table(transactions) where TRANSACTION_TYPE = 'R';
        if purchase_count <> to_number(array(2)) then
            raise purchases_integrity;
        end if;
        if refund_count <> to_number(array(3)) then
            raise refunds_integrity;
        end if;
    end;

    procedure error_log(p_message varchar2) as
    begin
        update FS11_FILE_RECORDS set error_message = p_message where FS11_FILE_RECORDS.FILE_ID = file_id;
        print(p_message);
    end;

    procedure fs11_parse_record as
        delimiter_pos       number;
        record_end_pos      number;
        record_field_number integer := 0;
    begin
        record_end_pos := INSTR(file_content, chr(10), file_pos);
        array.delete;
        -- if it is last record then we don't have delimiter after last field
        if record_end_pos = file_length then
            loop
                delimiter_pos := INSTR(file_content, ';', file_pos);
                record_field_number := record_field_number + 1;
                array.extend;
                -- resolve upper comment problem here
                if delimiter_pos = 0 then
                    array(record_field_number) := SUBSTR(file_content, file_pos, file_length - file_pos);
                    file_pos := file_length;
                    exit;
                end if;
                if delimiter_pos < record_end_pos then
                    array(record_field_number) := SUBSTR(file_content, file_pos, delimiter_pos - file_pos);
                    file_pos := delimiter_pos + 1;
                else
                    array(record_field_number) := SUBSTR(file_content, file_pos, delimiter_pos - file_pos - 2);
                    file_pos := delimiter_pos - 1;
                    exit;
                end if;
            end loop;
        else
            loop
                delimiter_pos := INSTR(file_content, ';', file_pos);
                record_field_number := record_field_number + 1;
                array.extend;
                if delimiter_pos < record_end_pos then
                    array(record_field_number) := SUBSTR(file_content, file_pos, delimiter_pos - file_pos);
                    file_pos := delimiter_pos + 1;
                else
                    array(record_field_number) := SUBSTR(file_content, file_pos, delimiter_pos - file_pos - 2);
                    file_pos := delimiter_pos - 1;
                    exit;
                end if;
            end loop;
        end if;
    end;

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
        response          clob;
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
                    'T;' || (purchase_count + refund_count) || ';0' || chr(10);
        write_cards_cashback;
--         print(response);
    end;

    procedure manage_periods as
        period_count number;
    begin
        -- init
        select count(*) into period_count from FS11_PERIODS;
        if period_count = 0 then
            insert into FS11_PERIODS
            values (to_number(to_char(sysdate, 'yyyymm')),
                    trunc(sysdate, 'MONTH') + 9,
                    'current');
        end if;
        select period_id, period_date into current_period_id, current_period_date
        from FS11_PERIODS
        where PERIOD_STATUS = 'current';
    end;

    procedure fs11_proc_file(p_file_id varchar2) as
    begin

        transactions := TRANSACTION_TABLE();
        clients.delete;
        cards.delete;
        array := record_fields();

        manage_periods;

        -- todo
        file_pos := 1;
        file_id := p_file_id;
        print('File ID: ' || file_id);
        select file_content into file_content
        from FS11_FILE_CONTENT
        where FS11_FILE_CONTENT.file_id = p_file_id;

        file_length := LENGTH(file_content);

        print('File length: ' || file_length);

        loop
            fs11_parse_record;
            case (array(1))
                when 'H' then
                    proc_header;
                when 'P' then
                    proc_transaction;
                when 'R' then
                    proc_transaction;
                when 'T' then
                    null;
                    fs11_proc_trailer;
                else
                    print(array(1));
                end case;
            if file_pos = file_length then
                exit;
            end if;
        end loop;

        begin

            process_transactions;

--   https://asktom.oracle.com/pls/asktom/f?p=100:11:0::::P11_QUESTION_ID:9539655000346985922

            insert into FS11_PURCHASES
            select card_num,
                   id,
                   transaction_date,
                   transaction_amount,
                   merchant_id,
                   to_number(common, '9999'),
                   comment_purchase
            from table (transactions)
            where transaction_type = 'P';

            insert into FS11_REFUNDS
            select card_num,
                   id,
                   transaction_date,
                   transaction_amount,
                   merchant_id,
                   common,
                   comment_purchase
            from table (transactions)
            where transaction_type = 'R';

            insert into FS11_TRANSACTION_TEMP
            select *
            from table (transactions);

            commit;

        exception
            when dup_val_on_index then raise trans_id_exist;
        end;

        print('Complete.');

    exception
        --         when file_id_exist then error_log('This file identifier was used before.');
        when purchases_integrity then error_log('Error in the trailer. Number of purchases is wrong.');
        when refunds_integrity then error_log('Error in the trailer. Number of refunds is wrong');
        when trans_id_exist then error_log('This transaction identifier was used before.');
--         when refund_more_than_purschase then error_log('refund_more_than_purschase ' || SQLERRM);
--         when transaction_wrong_date then error_log('Transaction date not in current period');

    end;

end fs11_processing_incoming_file;