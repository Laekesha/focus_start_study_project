declare
    file_string clob;
    function generate_file_content(transaction_count number, refunds_count number) return clob as
        content clob;
        transaction varchar2(32767);
    begin
        content := 'H;' || trunc(DBMS_RANDOM.VALUE * 999999999999)/*DBMS_RANDOM.STRING('x', 12)*/ || ';';
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

begin
    file_string := generate_file_content(10000, 0);
--     dbms_output.put(file_string);
    FS11_PROCESSING_INCOMING_FILE.FS11_PROC_FILE(file_string);
end;