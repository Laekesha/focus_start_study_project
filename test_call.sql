declare
    function generate_file_content(transaction_count number, refunds_count number) return varchar2 as
        content varchar2(32767);
        transaction varchar2(32767);
    begin
        content := 'H;' || DBMS_RANDOM.STRING('x', 12) || ';';
        content := content || to_char(sysdate, 'yyyymmddhh24miss') || chr(10);
        for void in 1 .. transaction_count - refunds_count
        loop
            transaction := 'P;' || DBMS_RANDOM.STRING('x', 40) || ';';
            transaction := transaction || DBMS_RANDOM.STRING('x', 12) || ';';
            transaction := transaction || to_char(sysdate, 'yyyymmddhh24miss') || ';';
            transaction := transaction || trunc(DBMS_RANDOM.VALUE * 999999999999) || ';';
            transaction := transaction || DBMS_RANDOM.STRING('x', 30) || ';';
            transaction := transaction || trunc(DBMS_RANDOM.VALUE * 8999 + 1000)  || ';';
            transaction := transaction || ';' || chr(10);
            content := content || transaction;
        end loop;
        for void in 1 .. refunds_count
        loop
            transaction := 'R;' || DBMS_RANDOM.STRING('x', 40) || ';';
            transaction := transaction || DBMS_RANDOM.STRING('x', 12) || ';';
            transaction := transaction || to_char(sysdate, 'yyyymmddhh24miss') || ';';
            transaction := transaction || trunc(DBMS_RANDOM.VALUE * 999999999999) || ';';
            transaction := transaction || DBMS_RANDOM.STRING('x', 30) || ';';
            transaction := transaction || DBMS_RANDOM.STRING('x', 12) ||  ';';
            transaction := transaction || ';' || chr(10);
            content := content || transaction;
        end loop;
        content := content || 'T;' || to_char(transaction_count - refunds_count) || ';' || to_char(refunds_count) || chr(10);
        return content;

    end;

begin
    FS11_PROCESSING_INCOMING_FILE.FS11_PROC_FILE('stub', generate_file_content(100, 10));
end;