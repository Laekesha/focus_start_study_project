declare
    type client_records is table of FS11_CLIENTS%rowtype;
    type card_records is table of FS11_CARDS%rowtype;
    clients      client_records := client_records();
    cards        card_records   := card_records();
    file_content clob;
    file_id      varchar2(200);

    procedure print(p_message varchar2) as
    begin
        dbms_output.put_line(p_message);
    end;

    procedure print(p_message clob) as
    begin
        dbms_output.put_line(p_message);
    end;

    function random_string(length number) return varchar2
    as
    begin
        return dbms_random.string('x', length);
    end;

    function random_number(upper_bound number) return number
    as
    begin
        return trunc(dbms_random.value * upper_bound); -- upper bound EXCLUSIVE
    end;

    function random_date return date
    as
        first_day date := trunc(sysdate, 'MONTH');
    begin
        return first_day + random_number(last_day(sysdate) - first_day + 1);
    end;

    procedure generate_file_content(file_id varchar2, per_card_purchases number) as
        type parts is table of number;
        refund_parts    parts  := parts();
        transaction     varchar2(32767);
        purchases_count number := 0;
        refunds_count   number := 0;
        purchase_id     varchar2(12);
        merchant_id     varchar2(30);
        purchase_amount varchar2(30);
        purcahses_date  date;
        refund_amount   number;
    begin
        file_content := 'H;' || file_id || ';';
        file_content := file_content || to_char(sysdate, 'yyyymmddhh24miss') || chr(10);
        for card_indx in 1 .. cards.count
            loop
                for void in 1 .. random_number(per_card_purchases)
                    loop
                        purchase_id := random_string(12);
                        merchant_id := random_string(30);
                        purchase_amount := random_number(1000) * 1000 + 1000;
                        purcahses_date := random_date;
                        transaction := 'P;' || cards(card_indx).CARD_NUM || ';';
                        transaction := transaction || purchase_id || ';';
                        transaction := transaction || to_char(purcahses_date, 'yyyymmddhh24miss') || ';';
                        transaction := transaction || purchase_amount || ';';
                        transaction := transaction || merchant_id || ';';
                        transaction := transaction || to_char(random_number(8999) + 1000) || ';';
                        transaction := transaction || random_string(random_number(200)) || chr(10); -- 2000 !!!!!!
                        file_content := file_content || transaction;
                        purchases_count := purchases_count + 1;
                        if DBMS_RANDOM.value < 0.1 then -- refund probability
                            refund_parts.delete;
                            if DBMS_RANDOM.value < 0.9 then -- full refund probability
                                refund_parts.extend;
                                refund_parts(refund_parts.last) := purchase_amount;
                            else
                                refund_amount := purchase_amount;
                                for void IN 1 .. random_number(3) + 1 -- refunds parts
                                    loop
--                                         if refund_amount <= 0 then
--                                             exit;
--                                         end if;
                                        refund_parts.extend;
                                        refund_parts(refund_parts.last) := refund_amount * DBMS_RANDOM.value;
--                                         refund_amount := refund_amount - refund_parts(refund_parts.last);
                                    end loop;
                            end if;
                            for i IN refund_parts.first .. refund_parts.last
                                loop
                                    transaction := 'R;' || cards(card_indx).CARD_NUM || ';';
                                    transaction := transaction || random_string(12) || ';';
                                    transaction := transaction ||
                                                   to_char(purcahses_date + random_number(7), 'yyyymmddhh24miss') ||
                                                   ';';
                                    transaction := transaction || refund_parts(i) || ';';
                                    transaction := transaction || merchant_id || ';';
                                    transaction := transaction || purchase_id || ';';
                                    transaction := transaction || random_string(random_number(2000)) || chr(10);
                                    file_content := file_content || transaction;
                                end loop;
                            refunds_count := refunds_count + refund_parts.count;
                        end if;

                    end loop;
            end loop;
        file_content := file_content || 'T;' || to_char(purchases_count) || ';' || to_char(refunds_count) ||
                        chr(10);
    end;

    procedure trunc_transactions as
    begin
        execute immediate 'truncate table FS11_REFUNDS';
        execute immediate 'alter table FS11_REFUNDS disable constraint FK_REFUNDS_TO_PURCHASES';
        execute immediate 'truncate table FS11_PURCHASES';
        execute immediate 'alter table FS11_REFUNDS enable constraint FK_REFUNDS_TO_PURCHASES';
        print('FS11_PURCHASES and FS11_REFUNDS truncated');
    end;

    -- TODO:
    procedure trunc_clients as
    begin
        null;
    end;

    -- TODO:
    procedure trunc_cards as
    begin
        null;
    end;

    procedure trunc_file_tables as
    begin
        execute immediate 'truncate table FS11_FILE_CONTENT';
        execute immediate 'alter table FS11_FILE_CONTENT disable constraint FK_FILE_CONTENT_TO_FILE_RECORDS';
        execute immediate 'truncate table FS11_FILE_RECORDS';
        execute immediate 'alter table FS11_FILE_CONTENT enable constraint FK_FILE_CONTENT_TO_FILE_RECORDS';
        print('FS11_FILE_CONTENT and FS11_FILE_RECORDS truncated');
    end;

    procedure insert_file_into_tables as
    begin
        insert into FS11_FILE_RECORDS values (file_id, sysdate, 'incoming', 'new', null);
        insert into FS11_FILE_CONTENT values (file_id, file_content);
        commit;
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
                FS11_PROCESSING_INCOMING_FILE.FS11_PROC_FILE(ids.FILE_ID);
            end loop;
    end;

    procedure generate_clients(client_count number) as
        client FS11_CLIENTS%rowtype;
    begin
        for void in 1 .. client_count
            loop
                client.CLIENT_ID := random_number(9999999);
                client.FIRST_NAME := random_string(10);
                client.LAST_NAME := random_string(10);
                client.PHONE := to_char(random_number(9999999999) + 70000000000);
                client.EMAIL := random_string(10) || '@' || random_string(5) || '.ru';
                clients.extend;
                clients(clients.last) := client;
            end loop;
    end;

    procedure generate_cards(per_client_card_count number) as
        card        FS11_CARDS%rowtype;
        max_card_id number := 9999999 * per_client_card_count;
    begin
        for client_indx in 1 .. clients.count
            loop
                for card_indx in 1 .. random_number(per_client_card_count)
                    loop
                        card.CARD_ID := random_number(max_card_id);
                        card.CARD_NUM := random_string(40);
                        card.CLIENT_ID := clients(client_indx).CLIENT_ID;
                        card.START_DATE := random_date;
                        card.END_DATE := card.START_DATE + 365;
                        card.STATUS := 'active';
                        if card_indx = 1 then
                            card.CARD_ROLE := 'master';
                        else
                            card.CARD_ROLE := 'slave';
                        end if;
                        cards.extend;
                        cards(cards.last) := card;
                    end loop;
            end loop;
    end;

    procedure insert_clients_and_cards_into_tables as
    begin
        forall indx in 1 .. clients.count
            insert into FS11_CLIENTS
            values clients(indx);
        forall indx in 1..cards.count
            insert into FS11_CARDS
            values cards(indx);
        commit;
    end;


begin
    trunc_transactions;
    trunc_file_tables;
    file_id := random_string(12);
    generate_clients(20);
    generate_cards(per_client_card_count => 3);
    insert_clients_and_cards_into_tables;
    generate_file_content(file_id, per_card_purchases => 100);
    insert_file_into_tables;
    proc_file_table;
--     print(file_content);
end;

-- https://asktom.oracle.com/pls/apex/f?p=100:11:0::::P11_QUESTION_ID:774225935270