declare
    procedure print(p_message varchar2) as
    begin
        dbms_output.put_line(p_message);
    end;

    function random_string(lenght number) return varchar2
        as 
        begin 
            return dbms_random.string('x', lenght);
        end;

     function random_number(upper_bound number) return number
        as
        begin
            return trunc(dbms_random.value * upper_bound);
        end;
    
    function generate_file_content(file_id varchar2, transaction_count number, refunds_count number) return clob as
        content clob;
        transaction varchar2(32767);
    begin
        content := 'H;' || file_id || ';';
        content := content || to_char(sysdate, 'yyyymmddhh24miss') || chr(10);
        for void in 1 .. transaction_count - refunds_count
        loop
            transaction := 'P;' || random_string(40) || ';';
            transaction := transaction || random_string(12) || ';';
            transaction := transaction || to_char(sysdate, 'yyyymmddhh24miss') || ';';
            transaction := transaction || random_number(9999999999) || ';';
            transaction := transaction || random_string(30) || ';';
            transaction := transaction || random_number(8999) + 1000  || ';';
            transaction := transaction || random_string(random_number(2000)) || chr(10);
            content := content || transaction;
        end loop;
        for void in 1 .. refunds_count
        loop
            transaction := 'R;' || random_string(40) || ';';
            transaction := transaction || random_string(12) || ';';
            transaction := transaction || to_char(sysdate, 'yyyymmddhh24miss') || ';';
            transaction := transaction || random_number(9999999999) || ';';
            transaction := transaction || random_string(30) || ';';
            transaction := transaction || random_string(12) ||  ';';
            transaction := transaction || random_string(random_number(2000))|| chr(10);
            content := content || transaction;
        end loop;
        content := content || 'T;' || to_char(transaction_count - refunds_count) || ';' || to_char(refunds_count) || chr(10);
        return content;

    end;

    procedure trunc_file_tables as
    begin
        execute immediate 'alter table FS11_FILE_CONTENT disable constraint FK_FILE_CONTENT_TO_FILE_RECORDS';
        execute immediate 'truncate table FS11_FILE_RECORDS';
        execute immediate 'alter table FS11_FILE_CONTENT enable constraint FK_FILE_CONTENT_TO_FILE_RECORDS';
        execute immediate 'truncate table FS11_FILE_CONTENT';
        print('FS11_FILE_CONTENT and FS11_FILE_RECORDS truncated');
    end;

    procedure add_file as
        file_content clob;
        file_id     varchar2(12);
    begin
        file_id := random_string(12);
        file_content := generate_file_content(file_id, 10000, 0);
        insert into FS11_FILE_RECORDS values (file_id, 'file', sysdate, 'incoming', 'new', null);
        insert into FS11_FILE_CONTENT values (file_id, file_content);
        commit;
    end;

    procedure proc_file_table as
    begin
        FOR ids IN
            (select file_id
             FROM FS11_FILE_CONTENT
                      natural join FS11_FILE_RECORDS
             where FILE_STATUS = 'new'
               and FILE_TYPE = 'incoming')
            loop
                print('Process: ' || ids.FILE_ID);
                FS11_PROCESSING_INCOMING_FILE.FS11_PROC_FILE(ids.FILE_ID);
            end loop;
    end;

begin
--     add_file;
    trunc_file_tables;
end;