-- +for all i in 1 ? (insert), +exceptions with rollback and sql%isopen & close, +new collection for file-status extended ass array
-- id_record, status (new, processed, rejected), error_message
-- + 2 variables for check header an check trailer
-- excep when p after r. excep when new card, merch, mcc (?). if file is incorrect - rollback, no new card/merch/mcc!
-- + inserts

create or replace package fs11_processing_incoming_file as
    procedure fs11_proc_file(p_file_name varchar2, p_file_string varchar2); -- point of entry. public(cause declaring in the specification)
end fs11_processing_incoming_file;

create or replace package body fs11_processing_incoming_file as

    type record_fields is table of varchar2(2000);
    type transactions is table of record_fields;
    purchases transactions := transactions();
    refunds transactions := transactions();
    array record_fields := record_fields();
    purchase_count number;
    refund_count number;
    file_length number;
    file_pos number;
    file_string varchar2(32767);
    purchases_integrity exception;
    refunds_integrity exception;

    procedure fs11_print_array(p_parsed_array record_fields) as
    begin
        for i in 1..p_parsed_array.COUNT -- start from 2 to remove first litter constant field
            loop
                if p_parsed_array(i) is NULL then
                    dbms_output.put_line('NULL');
                else
                    dbms_output.put_line(p_parsed_array(i));
                end if;
            end loop;
    end;

    procedure fs11_proc_header as
    begin
        dbms_output.put_line('HEADER:');
        fs11_print_array(array);
    end;

    procedure fs11_proc_purchase as
    begin
        dbms_output.put_line('PURCHASE:');
        fs11_print_array(array);
        purchases.extend;
        purchases(purchases.LAST) := array;
    end;

    procedure fs11_proc_refund as
    begin
        dbms_output.put_line('REFUND:');
        fs11_print_array(array);
        refunds.extend;
        refunds(refunds.LAST) := array;
    end;

    procedure fs11_proc_trailer as
    begin
        if refunds.count <> to_number(array(2)) then
            raise purchases_integrity;
        end if;
        if purchases.count <> to_number(array(3)) then
            raise refunds_integrity;
        end if;
    end;

    procedure fs11_parse_record as
        delimiter_pos       number;
        record_end_pos      number;
        record_field_number integer := 0;
    begin
        record_end_pos := INSTR(file_string, chr(10), file_pos);
        array.delete;
        -- if it is last record then we don't have delimiter after last field
        if record_end_pos = file_length then
            loop
                delimiter_pos := INSTR(file_string, ';', file_pos);
                record_field_number := record_field_number + 1;
                array.extend;
                -- resolve upper comment problem here
                if delimiter_pos = 0 then
                    array(record_field_number) := SUBSTR(file_string, file_pos, file_length - file_pos);
                    file_pos := file_length;
                    exit;
                end if;
                if delimiter_pos < record_end_pos then
                    array(record_field_number) := SUBSTR(file_string, file_pos, delimiter_pos - file_pos);
                    file_pos := delimiter_pos + 1;
                else
                    array(record_field_number) := SUBSTR(file_string, file_pos, delimiter_pos - file_pos - 2);
                    file_pos := delimiter_pos - 1;
                    exit;
                end if;
            end loop;
        else
            loop
                delimiter_pos := INSTR(file_string, ';', file_pos);
                record_field_number := record_field_number + 1;
                array.extend;
                if delimiter_pos < record_end_pos then
                    array(record_field_number) := SUBSTR(file_string, file_pos, delimiter_pos - file_pos);
                    file_pos := delimiter_pos + 1;
                else
                    array(record_field_number) := SUBSTR(file_string, file_pos, delimiter_pos - file_pos - 2);
                    file_pos := delimiter_pos - 1;
                    exit;
                end if;
            end loop;
        end if;
    end;

    procedure fs11_proc_file(p_file_name varchar2, p_file_string varchar2) as
    begin
        file_string := p_file_string;
        array := record_fields();
        file_pos := 1;

        file_length := LENGTH(file_string);
        dbms_output.put_line('File length: ' || file_length);

        loop
            fs11_parse_record;
            case (array(1))
                when 'H' then
                    fs11_proc_header;
                when 'P' then
                    fs11_proc_purchase;
                when 'R' then
                    fs11_proc_refund;
                when 'T' then
                    fs11_proc_trailer;
                else
                    dbms_output.put_line(array(1));
                end case;
            if file_pos = file_length then
                exit;
            end if;
        end loop;

    exception
        when purchases_integrity
            then dbms_output.put_line('Error in the trailer. Number of purchases is wrong');
        when refunds_integrity
            then dbms_output.put_line('Error in the trailer. Number of refunds is wrong');

    end;

end fs11_processing_incoming_file;