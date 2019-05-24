declare
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
                        merchant_id := merchants(random_number(merchants.COUNT) + 1).MERCHANT_ID;
                        purchase_amount := random_number(1000) * 1000 + 1000;
                        purcahses_date := random_date;
                        transaction := 'P;' || cards(card_indx).CARD_NUM || ';';
                        transaction := transaction || purchase_id || ';';
                        transaction := transaction || to_char(purcahses_date, 'yyyymmddhh24miss') || ';';
                        transaction := transaction || purchase_amount || ';';
                        transaction := transaction || merchant_id || ';';
                        transaction := transaction || mcc(random_number(mcc.COUNT) + 1).MCC || ';';
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
                                        if refund_amount <= 0 then -- comment for refunds error
                                            exit; -- comment for refunds error
                                        end if; -- comment for refunds error
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
        commit;
    end;


    function generate_current_cashback_file return clob as
        type card_cashback_table is table of integer index by varchar2 (40);
        card_cashback          card_cashback_table;
        type process_record is record (ID varchar2(12), CARD varchar2(40), AMOUNT number, MERCHANT number, MCC number);
        type process_table is table of process_record;
        processed_transactions process_table;
        cashback               number;
        response               clob;
    begin
        response := 'H;' || random_string(12) || ';' || to_char(sysdate, 'yyyymmddhh24miss') || chr(10);

        select ID,
               CARD,
               AMOUNT,
               MERCHANT,
               MCC
               bulk collect into processed_transactions
        from (select ID, CARD_NUM "CARD", TRANSACTION_AMOUNT "AMOUNT", mr.PERCENT_CASH "MERCHANT", mc.PERCENT_CASH "MCC"
              from FS11_PURCHASES
                       join FS11_MERCHANTS mr on FS11_PURCHASES.MERCHANT_ID = mr.MERCHANT_ID
                       join FS11_MCC mc on FS11_PURCHASES.MCC = mc.MCC
              union
              select ID, CARD_NUM "CARD", TRANSACTION_AMOUNT "AMOUNT", PERCENT_CASH "MERCHANT", null "MCC"
              from FS11_REFUNDS
                       join FS11_MERCHANTS on FS11_REFUNDS.MERCHANT_ID = FS11_MERCHANTS.MERCHANT_ID);

        for i in 1 .. processed_transactions.COUNT
            loop
                if processed_transactions(i).MCC = 0 or processed_transactions(i).MERCHANT = 0
                then
                    cashback := 0;
                else
                    if processed_transactions(i).MERCHANT is not null
                    then
                        cashback := processed_transactions(i).AMOUNT * processed_transactions(i).MERCHANT;
                    else
                        if processed_transactions(i).MCC is not null
                        then
                            cashback := processed_transactions(i).AMOUNT * processed_transactions(i).MCC;
                        else
                            cashback := processed_transactions(i).AMOUNT * 0.01;
                        end if;
                    end if;
                end if;
                if (card_cashback.exists(processed_transactions(i).CARD)) then
                    card_cashback(processed_transactions(i).CARD) :=
                            card_cashback(processed_transactions(i).CARD) + cashback;
                else
                    card_cashback(processed_transactions(i).CARD) := cashback;
                end if;

                response := response || 'S;' ||
                            processed_transactions(i).ID || ';' ||
                            processed_transactions(i).CARD || ';' ||
                            cashback || ';' ||
                            card_cashback(processed_transactions(i).CARD) || chr(10);
            end loop;
        response := response ||
                    'T;' || processed_transactions.COUNT || ';0' || chr(10);

        return response;

    end;


    function generate_total_cashback_file return clob as
        type client_cashback_table is table of integer index by pls_integer;
        client_cashback        client_cashback_table;
        type process_record is record (CLIENT_ID number, ID varchar2(12), CARD varchar2(40), AMOUNT number, MERCHANT number, MCC number);
        type process_table is table of process_record;
        processed_transactions process_table;
        cashback               number;
        response               clob;
        client_id              number;
        type card_record is record (CLIENT_ID number, MASTER_CARD varchar2(40));
        type card_table is table of card_record;
        master_cards           card_table;
    begin
        response := 'H;' || random_string(12) || ';' || to_char(sysdate, 'yyyymmddhh24miss') || ';' ||
                    '201905' || chr(10); -- TODO Replace placeholder by date

        select CLIENT_ID,
               ID,
               CARD,
               AMOUNT,
               MERCHANT,
               MCC
               bulk collect into processed_transactions
        from (select ID, CARD_NUM "CARD", TRANSACTION_AMOUNT "AMOUNT", mr.PERCENT_CASH "MERCHANT", mc.PERCENT_CASH "MCC"
              from FS11_PURCHASES
                       join FS11_MERCHANTS mr on FS11_PURCHASES.MERCHANT_ID = mr.MERCHANT_ID
                       join FS11_MCC mc on FS11_PURCHASES.MCC = mc.MCC
              union
              select ID, CARD_NUM "CARD", TRANSACTION_AMOUNT "AMOUNT", PERCENT_CASH "MERCHANT", null "MCC"
              from FS11_REFUNDS
                       join FS11_MERCHANTS on FS11_REFUNDS.MERCHANT_ID = FS11_MERCHANTS.MERCHANT_ID)
                 join FS11_CARDS on CARD = FS11_CARDS.CARD_NUM;

        for i in 1 .. processed_transactions.COUNT
            loop
                if processed_transactions(i).MCC = 0 or processed_transactions(i).MERCHANT = 0
                then
                    cashback := 0;
                else
                    if processed_transactions(i).MERCHANT is not null
                    then
                        cashback := processed_transactions(i).AMOUNT * processed_transactions(i).MERCHANT;
                    else
                        if processed_transactions(i).MCC is not null
                        then
                            cashback := processed_transactions(i).AMOUNT * processed_transactions(i).MCC;
                        else
                            cashback := processed_transactions(i).AMOUNT * 0.01;
                        end if;
                    end if;
                end if;
                if (client_cashback.exists(processed_transactions(i).CLIENT_ID)) then
                    client_cashback(processed_transactions(i).CLIENT_ID) :=
                            client_cashback(processed_transactions(i).CLIENT_ID) + cashback;
                else
                    client_cashback(processed_transactions(i).CLIENT_ID) := cashback;
                end if;
            end loop;

        select CLIENT_ID,
               CARD_NUM "MASTER_CARD"
               bulk collect into master_cards
        from FS11_CARDS
        where CARD_ROLE = 'master';


        for i in 1 .. master_cards.COUNT
            loop
                if (client_cashback.exists(master_cards(i).CLIENT_ID)) then
                    response := response || 'C;' ||
                                master_cards(i).MASTER_CARD || ';' ||
                                client_cashback(master_cards(i).CLIENT_ID) || chr(10);
                end if;
            end loop;

        response := response ||
                    'T;' || client_cashback.COUNT || chr(10);

        return response;

    end;

    procedure generate_merchants(merch_count number) as
    begin
        for i in 1 .. merch_count
            loop
                merchants.extend;
                merchants(merchants.LAST).MERCHANT_ID := random_string(30);
                merchants(merchants.LAST).PERCENT_CASH := random_number(12) / 100;
                if merchants(merchants.LAST).PERCENT_CASH = 0.11 then
                    merchants(merchants.LAST).PERCENT_CASH := NULL;
                end if;
            end loop;
    end;

    procedure generate_MCC(MCC_count number) as
    begin
        for i in 1 .. MCC_count
            loop
                mcc.extend;
                mcc(mcc.LAST).MCC := 1000 + i;
                mcc(mcc.LAST).PERCENT_CASH := random_number(12) / 100;
                if mcc(mcc.LAST).PERCENT_CASH = 0.11 then
                    mcc(mcc.LAST).PERCENT_CASH := NULL;
                end if;
            end loop;
    end;

    procedure trunc_tables as
    begin
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

begin
    print(generate_current_cashback_file);
    print(generate_total_cashback_file);
    return;

    trunc_tables;

    file_id := random_string(12);
    generate_MCC(500);
    generate_merchants(100);
    generate_clients(10);
    generate_cards(per_client_card_count => 3);
    insert_into_tables;
    generate_file_content(file_id, per_card_purchases => 10);
    insert_file_into_tables;
    proc_file_table;
end;