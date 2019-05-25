create or replace package fs11_processing_incoming_file as
    procedure fs11_proc_file(p_file_id varchar2);
end fs11_processing_incoming_file;

create or replace package body fs11_processing_incoming_file as
    --     subtype merchant is varchar2(30);
    period_date date;
    file_date date;

    type mcc_rules_table is table of FS11_MCC_RULES%rowtype;
    mcc_rules mcc_rules_table := mcc_rules_table();

    type merchant_rules_table is table of FS11_MERCHANT_RULES%rowtype;
    merchant_rules merchant_rules_table := merchant_rules_table();


--     type purchases_records is table of fs11_purchases%rowtype;
    type refunds_records is table of fs11_refunds%rowtype;
--     purchases purchases_records := purchases_records();
--     purchases TRANSACTION_TABLE;
    transactions TRANSACTION_TABLE;
    refunds refunds_records := refunds_records();

    type refund_amount_type is table of number index by VARCHAR2 (12);
    refund_amounts refund_amount_type;

    type record_fields is table of varchar2(2000); -- index by pls_integer;
    array record_fields := record_fields();

    file_length number;
    file_pos number;
    file_content clob;
    file_id varchar2(12);

    purchases_integrity exception;
    refunds_integrity exception;
    file_id_exist exception;
    trans_id_exist exception;
    refund_more_than_purschase exception;
    pragma exception_init (refund_more_than_purschase, -20000);
    transaction_wrong_date exception;

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
        --         load_rules;
--         file_id := array(2);
--         insert into FS11_FILE_RECORDS (file_id, file_name, file_date, file_type, file_status)
--         values (file_id, 'not/need', to_date(array(3), 'yyyymmddhh24miss'), 'incoming', 'new');
--         print('HEADER:');
--         fs11_print_array(array);
--     exception
--         when dup_val_on_index then raise file_id_exist;
    end;

    procedure proc_purchase as
        i number;
    begin
        -- todo rise cast exception
--         purchases.extend;
--         i := purchases.last;
--         purchases(i).card_num := array(2);
--         purchases(i).id := array(3);
--         purchases(i).transaction_date := to_date(array(4), 'yyyymmddhh24miss');
--         purchases(i).transaction_amount := to_number(array(5));
--         purchases(i).merchant_id := array(6);
--         purchases(i).mcc := to_number(array(7), '9999');
--         purchases(i).comment_purchase := array(8);

        null;

        --             transactions.extend;
--             transactions(transactions.last) := TRANSACTION_TYPE(
--                 array(1),
--                 array(2),
--                 array(3),
--                 to_date(array(4), 'yyyymmddhh24miss'),
--                 to_number(array(5)),
--                 array(6),
--                 array(7),
--                 array(8)
--             );

--         if purchases(i).transaction_date not between period_date and file_date then
--             -- TODO Write error records
--             raise transaction_wrong_date;
--         end if;
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

        --         if purchases(i).transaction_date not between period_date and file_date then
        --             -- TODO Write error records
        --             raise transaction_wrong_date;
        --         end if;
    end;


    procedure fs11_proc_refund as
        i number;
    begin
        -- todo rise cast exception
--         refunds.extend;
--         i := refunds.last;
--         refunds(i).card_num := array(2);
--         refunds(i).id := array(3);
--         refunds(i).transaction_date := to_date(array(4), 'yyyymmddhh24miss');
--         refunds(i).transaction_amount := to_number(array(5));
--         refunds(i).merchant_id := array(6);
--         refunds(i).purchase_id := array(7);
--         refunds(i).comment_refund := array(8);

--          if refunds(i).transaction_date not between period_date and file_date then
--             -- TODO Write error records
--             raise transaction_wrong_date;
--         end if;

        if (refund_amounts.exists(refunds(i).purchase_id)) then
            refund_amounts(refunds(i).purchase_id) :=
                        refund_amounts(refunds(i).purchase_id) + refunds(i).transaction_amount;
        else
            refund_amounts(refunds(i).purchase_id) := refunds(i).transaction_amount;
        end if;

    end;

--     procedure fs11_proc_trailer as
--     begin
--         print('Trailer purchases: ' || array(2) || ' purchase_record_collection.count: ' || purchases.count);
--         print('Trailer refunds: ' || array(3));
--         if purchases.count <> to_number(array(2)) then
--             raise purchases_integrity;
--         end if;
--         if refunds.count <> to_number(array(3)) then
--             raise refunds_integrity;
--         end if;
--     end;

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

    procedure load_rules as
    begin
        select * bulk collect into mcc_rules
        from FS11_MCC_RULES
        where START_DATE <= file_date
          and period_date < END_DATE;
        select * bulk collect into merchant_rules
        from FS11_MERCHANT_RULES
        where START_DATE <= file_date
          and period_date < END_DATE;
    end;

    procedure process_records as
        type client_type is record (cashback number, transaction_count number);
        type clients_table is table of client_type index by pls_integer;
        clients      clients_table;
        cashback number;
        response     clob;
    begin

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
                         REFUND_DATE "TRANSACTION_DATE",
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
                               t1.CARD_NUM    "CARD",
                               t2.COMMON      "PURCHASE_MCC",
                               t2.MERCHANT_ID "PURCHASE_MERCHANT"
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

                if (clients.exists(r.CLIENT_ID)) then
                    clients(r.CLIENT_ID).cashback := clients(r.CLIENT_ID).cashback + cashback;
                    clients(r.CLIENT_ID).transaction_count := clients(r.CLIENT_ID).transaction_count + 1;
                else
                    clients(r.CLIENT_ID) := client_type(cashback, 1);
--                     clients(r.CLIENT_ID).cashback := cashback;
                end if;

                response := response || 'S;' ||
                            r.CARD || ';' ||
                            r.ID || ';' ||
                            cashback || ';' ||
                            clients(r.CLIENT_ID).transaction_count || ';' ||
                            clients(r.CLIENT_ID).cashback || chr(10);
            end loop;
            print(response);
    end;


    procedure fs11_proc_file(p_file_id varchar2) as
    begin
        transactions := TRANSACTION_TABLE();

        select * bulk collect into mcc_rules
        from FS11_MCC_RULES
        where START_DATE <= file_date
          and period_date < END_DATE;

        --         select PERIOD_DATE into period_date from FS11_PERIODS where PERIOD_STATUS = 'current';

        -- todo
        file_id := p_file_id;
        print('File ID: ' || file_id);
        select file_content into file_content
        from FS11_FILE_CONTENT
        where FS11_FILE_CONTENT.file_id = p_file_id;


        array := record_fields();
        file_pos := 1;
        file_length := LENGTH(file_content);

        --         purchases.delete;
--         refunds.delete;

        print('File length: ' || file_length);

        loop
            fs11_parse_record;
            case (array(1))
                when 'H' then
                    proc_header;
                when 'P' then
                    proc_transaction;
--                     proc_purchase;
                when 'R' then
                    proc_transaction;
--                     fs11_proc_refund;
                when 'T' then
                    null;
--                     fs11_proc_trailer;
                else
                    print(array(1));
                end case;
            if file_pos = file_length then
                exit;
            end if;
        end loop;

        begin
            --             for indx in 1 .. purchases.COUNT
--                 loop
--                     if refund_amounts.exists(purchases(indx).id) then
--                         if purchases(indx).TRANSACTION_AMOUNT - refund_amounts(purchases(indx).id) < 0 then
--                             raise_application_error(-20000, 'purchase_id = ' || purchases(indx).id);
--                         end if;
--                     end if;
--                 end loop;


--             forall indx in 1..purchases.count
--                 insert into fs11_purchases
--                 values purchases(indx);
--
--             forall indx in 1..purchases.count
--                 insert into fs11_purchases
--                 values (purchases(indx).card_num,
--                         purchases(indx).id,
--                         purchases(indx).transaction_date,
--                         purchases(indx).transaction_amount,
--                         purchases(indx).merchant_id,
--                         to_number(purchases(indx).common, '9999'),
--                         purchases(indx).comment_purchase);

--      https://asktom.oracle.com/pls/asktom/f?p=100:11:0::::P11_QUESTION_ID:9539655000346985922

            process_records;

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
            -- where transaction_type = 'R';

--             forall indx in 1..refunds.count
--                 insert into fs11_refunds
--                 values refunds(indx);

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
        when refund_more_than_purschase then error_log('refund_more_than_purschase ' || SQLERRM);
        when transaction_wrong_date then error_log('transaction_wrong_date');

    end;

end fs11_processing_incoming_file;