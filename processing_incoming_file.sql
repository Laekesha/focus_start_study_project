create or replace package fs11_processing_incoming_file as
    --     procedure fs11_proc_file(p_file_id varchar2);
    procedure proc_file_table;
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

    purchases_integrity exception;
    refunds_integrity exception;
    file_id_exist exception;
    trans_id_exist exception;
    wrong_file_const exception;
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

    procedure proc_header as
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
        select count(*) into purchase_count from table (transactions) where TRANSACTION_TYPE = 'P';
        select count(*) into refund_count from table (transactions) where TRANSACTION_TYPE = 'R';
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
        array := record_fields();

        manage_periods;

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
                    raise wrong_file_const;
                end case;
            if file_pos = file_length then
                exit;
            end if;
        end loop;

        FS11_PROCESSING_OUTCOMING_FILE.PROCESS(transactions, file_date, current_period_id);

        begin

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
        when wrong_file_const then error_log('wrong_file_const');
        --         when refund_more_than_purschase then error_log('refund_more_than_purschase ' || SQLERRM);
--         when transaction_wrong_date then error_log('Transaction date not in current period');

    end;

    procedure proc_file_table as
    begin
        for ids in
            (select file_id
             from FS11_FILE_CONTENT
                      natural join FS11_FILE_RECORDS
             where FILE_STATUS = 'new'
               and FILE_TYPE = 'incoming')
            loop
                print('Process: ' || ids.FILE_ID);
                fs11_proc_file(ids.FILE_ID);
            end loop;
    end;

end fs11_processing_incoming_file;