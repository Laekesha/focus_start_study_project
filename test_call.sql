declare
    type mcc_rules_table is table of FS11_MCC_RULES%rowtype;
    mcc_rules mcc_rules_table := mcc_rules_table();

    type merchant_rules_table is table of FS11_MERCHANT_RULES%rowtype;
    merchant_rules merchant_rules_table :=  merchant_rules_table();


    type client_records is table of FS11_CLIENTS%rowtype;
    type card_records is table of FS11_CARDS%rowtype;
    clients      client_records   := client_records();
    cards        card_records     := card_records();
    file_content clob;
    file_id      varchar2(200);
    type merchant_records is table of FS11_MERCHANTS%rowtype;
    merchants    merchant_records := merchant_records();
    type mcc_records is table of FS11_MCC%rowtype;
    mcc          mcc_records      := mcc_records();

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
        refund_amount   number;
        transaction_date date;
    begin
        transaction_date := trunc(sysdate) - 10;
        file_content := 'H;' || file_id || ';';
        file_content := file_content || to_char(sysdate, 'yyyymmddhh24miss') || chr(10);
        for card_indx in 1 .. cards.count
            loop
                for void in 1 .. random_number(per_card_purchases)
                    loop
                        transaction_date := transaction_date + 1/24;
                        purchase_id := random_string(12);
                        merchant_id := merchants(random_number(merchants.COUNT) + 1).MERCHANT_ID;
                        purchase_amount := random_number(1000) * 10 + 10;
                        transaction := 'P;' || cards(card_indx).CARD_NUM || ';';
                        transaction := transaction || purchase_id || ';';
                        transaction := transaction || to_char(transaction_date, 'yyyymmddhh24miss') || ';';
                        transaction := transaction || purchase_amount || ';';
                        transaction := transaction || merchant_id || ';';
                        transaction := transaction || mcc(random_number(mcc.COUNT) + 1).MCC || ';';
                        transaction := transaction || random_string(random_number(200)) || chr(10); -- 2000 !!!!!!
                        file_content := file_content || transaction;
                        purchases_count := purchases_count + 1;
                        if DBMS_RANDOM.value < 0.1 then -- refund probability
                            transaction_date := transaction_date + 1;
                            refund_parts.delete;
                            if DBMS_RANDOM.value < 0.9 then -- full refund probability
                                refund_parts.extend;
                                refund_parts(refund_parts.last) := purchase_amount;
                            else
                                refund_amount := purchase_amount;
                                for void IN 1 .. random_number(3) + 1 -- refunds parts
                                    loop
                                        if refund_amount <= 0 then
                                            exit;
                                        end if;
                                        refund_parts.extend;
                                        refund_parts(refund_parts.last) := refund_amount * DBMS_RANDOM.value;
                                        refund_amount := refund_amount - refund_parts(refund_parts.last); -- comment for refunds error
                                    end loop;
                            end if;
                            for i IN refund_parts.first .. refund_parts.last
                                loop
                                    transaction := 'R;' || cards(card_indx).CARD_NUM || ';';
                                    transaction := transaction || random_string(12) || ';';
                                    transaction := transaction ||
                                                   to_char(transaction_date, 'yyyymmddhh24miss') ||
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
        card FS11_CARDS%rowtype;
    begin
        for client_indx in 1 .. clients.count
            loop
                for card_indx in 1 .. random_number(per_client_card_count) + 1
                    loop
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

    procedure insert_into_tables as
    begin
        forall indx in 1 .. clients.count
            insert into FS11_CLIENTS
            values clients(indx);
        forall indx in 1..cards.count
            insert into FS11_CARDS
            values cards(indx);
        forall indx in 1..merchants.count
            insert into FS11_MERCHANTS
            values merchants(indx);
        forall indx in 1..mcc.count
            insert into FS11_MCC
            values mcc(indx);
        forall indx in 1..mcc_rules.count
            insert into FS11_MCC_RULES
            values mcc_rules(indx);
        forall indx in 1..merchant_rules.count
            insert into FS11_MERCHANT_RULES
            values merchant_rules(indx);
        commit;
    end;

    procedure generate_merchants(merch_count number) as
    begin
        for i in 1 .. merch_count
            loop
                merchants.extend;
                merchants(merchants.LAST).MERCHANT_ID := random_string(30);
            end loop;
    end;

    procedure generate_MCC(MCC_count number) as
    begin
        for i in 1 .. MCC_count
            loop
                mcc.extend;
                mcc(mcc.LAST).MCC := 1000 + i;
            end loop;
    end;

    procedure trunc_tables as
    begin
        execute immediate 'truncate table FS11_TRANSACTION_TEMP';
        for tc in (select constraint_name, table_name
                   from user_constraints
                   where table_name like 'FS11_%'
                     and constraint_type = 'R')
            loop
                --             print(tc.table_name || ' ' || tc.constraint_name);
                execute immediate 'alter table ' || tc.table_name || ' disable constraint ' || tc.constraint_name;
            end loop;

        for tc in (select table_name
                   from user_constraints
                   where table_name like 'FS11_%')
            loop
                execute immediate 'truncate table ' || tc.table_name;
            end loop;

        for tc in (select constraint_name, table_name
                   from user_constraints
                   where table_name like 'FS11_%'
                     and constraint_type = 'R')
            loop
                execute immediate 'alter table ' || tc.table_name || ' enable constraint ' || tc.constraint_name;
            end loop;
        print('All tables truncated');
    end;

    procedure generate_rules as
    begin
        for i in 1 .. MCC.COUNT
            loop
                mcc_rules.extend;
                mcc_rules(i).MCC := MCC(i).MCC;
                mcc_rules(i).START_DATE := sysdate - 30;
                mcc_rules(i).END_DATE := sysdate + 30;
                mcc_rules(i).PERCENT_CASH := random_number(12) / 100;
                if mcc_rules(i).PERCENT_CASH = 0.11 then
                    mcc_rules(i).PERCENT_CASH := NULL;
                end if;
            end loop;
        for i in 1 .. merchants.COUNT
            loop
                merchant_rules.extend;
                merchant_rules(i).MERCHANT_ID := merchants(i).MERCHANT_ID;
                merchant_rules(i).START_DATE := sysdate - 30;
                merchant_rules(i).END_DATE := sysdate + 30;
                merchant_rules(i).PERCENT_CASH := random_number(12) / 100;
                if merchant_rules(i).PERCENT_CASH = 0.11 then
                    merchant_rules(i).PERCENT_CASH := NULL;
                end if;
            end loop;
    end;


begin
    trunc_tables;

    file_id := random_string(12);
    generate_MCC(500);
    generate_merchants(10);
    generate_rules;
    generate_clients(10);
    generate_cards(per_client_card_count => 3);
    insert_into_tables;
    generate_file_content(file_id, per_card_purchases => 10);
    insert_file_into_tables;
    proc_file_table;
end;