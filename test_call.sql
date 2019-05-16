declare
    file_string varchar2(32767) := 'H;11;20191103003100' ||
                                   chr(10) ||
                                   'P;1234567891abcdefghij1234567890abcdefghij;3;20191103003100;0;0;1234;comment' ||
                                   chr(10) ||
                                   'R;1234567892abcdefghij1234567890abcdefghij;5;20191103003100;0;0;1d;' ||
                                   chr(10) ||
                                   'T;1;1' ||
                                   chr(10);
begin
    FS11_PROCESSING_INCOMING_FILE.FS11_PROC_FILE('test.csv', file_string);
end;