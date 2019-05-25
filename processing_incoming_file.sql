-- new excep-s with rollback/savepoint. sql%isopen&close(?). new coll-n for file-status extended array (?)
-- exc: wrong order. unknown card, merch, mcc(?). if file is incorrect (diff.ways) - rollback, no new card/merch/mcc!
-- difference between purchases and their refunds
-- pls_integer for collection ?

create or replace package fs11_processing_incoming_file as
    procedure fs11_proc_file(p_file_id varchar2);
end fs11_processing_incoming_file;

create or replace package body fs11_processing_incoming_file as

    type purchases_records is table of fs11_purchases%rowtype;
    type refunds_records is table of fs11_refunds%rowtype;
    purchases purchases_records := purchases_records();
    refunds refunds_records := refunds_records();

    type refund_amount_type is table of number index by VARCHAR2(12);
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
    pragma exception_init(refund_more_than_purschase, -20000);

    procedure print(p_message varchar2) as
    begin
        dbms_output.put_line(p_message);
    end;

    procedure proc_header as -- control of unique ?
    begin
        null;
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
        purchases.extend;
        i := purchases.last;
        purchases(i).card_num := array(2);
        purchases(i).id := array(3);
        purchases(i).transaction_date := to_date(array(4), 'yyyymmddhh24miss');
        purchases(i).transaction_amount := to_number(array(5));
        purchases(i).merchant_id := array(6);
        purchases(i).mcc := to_number(array(7), '9999');
        purchases(i).comment_purchase := array(8);
    end;

    procedure fs11_proc_refund as
        i number;
    begin
        refunds.extend;
        i := refunds.last;
        refunds(i).card_num := array(2);
        refunds(i).id := array(3);
        refunds(i).transaction_date := to_date(array(4), 'yyyymmddhh24miss');
        refunds(i).transaction_amount := to_number(array(5));
        refunds(i).merchant_id := array(6);
        refunds(i).purchase_id := array(7);
        refunds(i).comment_refund := array(8);
        if (refund_amounts.exists(refunds(i).purchase_id)) then
            refund_amounts(refunds(i).purchase_id) := refund_amounts(refunds(i).purchase_id) + refunds(i).transaction_amount;
        else
            refund_amounts(refunds(i).purchase_id) := refunds(i).transaction_amount;
        end if;

    end;

    procedure fs11_proc_trailer as
    begin
        print('Trailer purchases: ' || array(2) || ' purchase_record_collection.count: ' || purchases.count);
        print('Trailer refunds: ' || array(3));
        if purchases.count <> to_number(array(2)) then
            raise purchases_integrity;
        end if;
        if refunds.count <> to_number(array(3)) then
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

    procedure fs11_proc_file(p_file_id varchar2) as
    begin
        file_id := p_file_id;
        print('File ID: ' || file_id);

        select file_content into file_content from FS11_FILE_CONTENT
        where FS11_FILE_CONTENT.file_id = p_file_id;

        array := record_fields();
        file_pos := 1;
        file_length := LENGTH(file_content);

        purchases.delete;
        refunds.delete;

        print('File length: ' || file_length);

        loop
            fs11_parse_record;
            case (array(1))
                when 'H' then
                    proc_header;
                when 'P' then
                    proc_purchase;
                when 'R' then
                    fs11_proc_refund;
                when 'T' then
                    fs11_proc_trailer;
                else
                    print(array(1));
                end case;
            if file_pos = file_length then
                exit;
            end if;
        end loop;

        begin
            for indx in 1 .. purchases.COUNT
                loop
                    if refund_amounts.exists(purchases(indx).id) then
                        if purchases(indx).TRANSACTION_AMOUNT - refund_amounts(purchases(indx).id) < 0 then
                            raise_application_error(-20000, 'purchase_id = ' || purchases(indx).id);
                        end if;
                    end if;
                end loop;


            forall indx in 1..purchases.count
                insert into fs11_purchases
                values purchases(indx);

            forall indx in 1..refunds.count
                insert into fs11_refunds
                values refunds(indx);

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

    end;

end fs11_processing_incoming_file;