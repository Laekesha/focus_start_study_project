create or replace package fs11_file_exchange as
    procedure fs11_file_recording (p_file_id varchar2, p_file_content clob);
end fs11_file_exchange;
/
create or replace package body fs11_file_exchange as

    procedure fs11_file_recording (p_file_id varchar2, p_file_content clob) as
    begin
        insert into fs11_file_content (file_id, file_content) values (p_file_id, p_file_content);
    end;

end fs11_file_exchange;