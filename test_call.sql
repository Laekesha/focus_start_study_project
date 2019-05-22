declare
    function generate_file_content(file_id varchar2, transaction_count number, refunds_count number) return clob as
        content clob;
        transaction varchar2(32767);
    begin
        content := 'H;' || file_id || ';';
        content := content || to_char(sysdate, 'yyyymmddhh24miss') || chr(10);
        for void in 1 .. transaction_count - refunds_count
        loop
            transaction := 'P;' || DBMS_RANDOM.STRING('x', 40) || ';';
            transaction := transaction || DBMS_RANDOM.STRING('x', 12) || ';';
            transaction := transaction || to_char(sysdate, 'yyyymmddhh24miss') || ';';
            transaction := transaction || trunc(DBMS_RANDOM.VALUE * 9999999999) || ';';
            transaction := transaction || DBMS_RANDOM.STRING('x', 30) || ';';
            transaction := transaction || trunc(DBMS_RANDOM.VALUE * 8999 + 1000)  || ';';
            transaction := transaction || 'comment' /* comment here */ || chr(10);
            content := content || transaction;
        end loop;
        for void in 1 .. refunds_count
        loop
            transaction := 'R;' || DBMS_RANDOM.STRING('x', 40) || ';';
            transaction := transaction || DBMS_RANDOM.STRING('x', 12) || ';';
            transaction := transaction || to_char(sysdate, 'yyyymmddhh24miss') || ';';
            transaction := transaction || trunc(DBMS_RANDOM.VALUE * 9999999999) || ';';
            transaction := transaction || DBMS_RANDOM.STRING('x', 30) || ';';
            transaction := transaction || DBMS_RANDOM.STRING('x', 12) ||  ';';
            transaction := transaction || 'comment' /* comment here */ || chr(10);
            content := content || transaction;
        end loop;
        content := content || 'T;' || to_char(transaction_count - refunds_count) || ';' || to_char(refunds_count) || chr(10);
        return content;

    end;

    procedure add_file as
        file_content clob;
        file_id     varchar2(12);
    begin
        file_id := DBMS_RANDOM.STRING('x', 12);
        file_content := generate_file_content(file_id, 10000, 0);
        insert into FS11_FILE_RECORDS values (file_id, 'file', sysdate, 'incoming', 'new', null);
        insert into FS11_FILE_CONTENT values (file_id, file_content);
        commit;
    end;

begin
--     add_file;
    FOR ids IN
        (select file_id FROM FS11_FILE_CONTENT
         natural join FS11_FILE_RECORDS where FILE_STATUS = 'new' and FILE_TYPE = 'incoming')
        loop
            DBMS_OUTPUT.put_line('Process: ' || ids.FILE_ID);
            FS11_PROCESSING_INCOMING_FILE.FS11_PROC_FILE(ids.FILE_ID);
        end loop;
--
end;