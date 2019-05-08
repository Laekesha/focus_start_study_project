-- +for all i in 1 ?, +exceptions with rollback and sql%isopen & close, +new collection for file-status extended ass array
-- id_record, status (new, processed, rejected), error_message
-- + 2 variables for check header an check trailer
-- excep when p after r. excep when new card, merch, mcc (?). if file is incorrect - no new card/merch/mcc!
-- + inserts

create or replace package fs11_processing_incoming_file as
    procedure fs11_proc_file(p_file_name varchar2, p_file_string varchar2); -- point of entry. public(cause the declare in the specification)
end fs11_processing_incoming_file;

-- two variables for header and trailer isnt public
    create or replace package body fs11_processing_incoming_file as

    type fields_tab is table of varchar2(2000);
    array fields_tab := fields_tab();
    purchase_count number;
    refund_count number;
    file_length number;
    file_pos number;
    file_string varchar2(32767);

    procedure fs11_proc_header as
    begin
        null;
    end;

    procedure fs11_proc_purchase as
    begin
        null;
    end;

    procedure fs11_proc_refund as
    begin
        null;
    end;

    procedure fs11_proc_trailer as
    begin
        null;
    end;

    procedure fs11_parse as
        delimiter_pos  number;
        record_end_pos number;
        c              integer := 0;
    begin
        select INSTR(file_string, chr(10), file_pos) into record_end_pos from DUAL;
        array.delete;
        -- if it is last record then we don't have delimiter after last field
        if record_end_pos = file_length then
            loop
                select INSTR(file_string, ';', file_pos) into delimiter_pos from DUAL;
                c := c + 1;
                array.extend;
                -- resolve upper comment probem here
                if delimiter_pos = 0 then
                    select SUBSTR(file_string, file_pos, file_length - file_pos) into array(c) from DUAL;
                    file_pos := file_length;
                    exit;
                end if;
                if delimiter_pos < record_end_pos then
                    select SUBSTR(file_string, file_pos, delimiter_pos - file_pos) into array(c) from DUAL;
                    file_pos := delimiter_pos + 1;
                else
                    select SUBSTR(file_string, file_pos, delimiter_pos - file_pos - 2) into array(c) from DUAL;
                    file_pos := delimiter_pos - 1;
                    exit;
                end if;
            end loop;
        else
            loop
                select INSTR(file_string, ';', file_pos) into delimiter_pos from DUAL;
                c := c + 1;
                array.extend;
                if delimiter_pos < record_end_pos then
                    select SUBSTR(file_string, file_pos, delimiter_pos - file_pos) into array(c) from DUAL;
                    file_pos := delimiter_pos + 1;
                else
                    select SUBSTR(file_string, file_pos, delimiter_pos - file_pos - 2) into array(c) from DUAL;
                    file_pos := delimiter_pos - 1;
                    exit;
                end if;
            end loop;
        end if;
    end;

    procedure PRINT_ARRAY(p_parsed_array fields_tab) as
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

    procedure fs11_proc_file(p_file_name varchar2, p_file_string varchar2) as
    begin
        file_string := p_file_string;
        array := fields_tab();
        purchase_count := 0;
        refund_count := 0;
        file_pos := 1;

        select LENGTH(file_string) into file_length from DUAL;
        dbms_output.put_line('File length: ' || file_length);

        loop
            fs11_parse;
            case (array(1))
                when 'H' then
                    -- can process header here
                    dbms_output.put_line('HEADER:');
                    PRINT_ARRAY(array);
                when 'P' then
                    -- can process purchase here
                    dbms_output.put_line('PURCHASE:');
                    purchase_count := purchase_count + 1;
                    PRINT_ARRAY(array);
                when 'R' then
                    -- can process refund here
                    dbms_output.put_line('REFUND:');
                    refund_count := refund_count + 1;
                    PRINT_ARRAY(array);
                when 'T' then
                    -- can process trailer here
                    dbms_output.put_line('TRAILER:');
                    PRINT_ARRAY(array);
                else
                    dbms_output.put_line(array(1));
                end case;
            if file_pos = file_length then
                exit;
            end if;
        end loop;
        -- here we can check our counters with trailer values
        dbms_output.put_line('Purchases: ' || purchase_count);
        dbms_output.put_line('Refunds: ' || refund_count);
    end;

end fs11_processing_incoming_file;