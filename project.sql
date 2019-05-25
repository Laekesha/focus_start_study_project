-- drop table fs11_file_content;
-- drop table fs11_file_records;

-- drop table fs11_refunds;
-- drop table fs11_purchases;
--
-- drop table fs11_cards;
-- drop table fs11_clients;
--
-- drop table fs11_mcc_rules;
-- drop table fs11_mcc;
-- drop table fs11_merchant_rules;
-- drop table fs11_merchants;
--
-- drop table fs11_periods;

-- logging?

-- drop table FS11_TRANSACTION_TEMP;
create table FS11_TRANSACTION_TEMP (
    transaction_type   varchar2(1),
    card_num           varchar2(40),
    id                 varchar2(12),
    transaction_date   date ,
    transaction_amount number(10),
    merchant_id        varchar2(30),
    common              varchar2(12),
    comment_purchase   varchar2(2000)
);


-- drop type transaction_table;
-- drop type transaction_type;

create type transaction_type is object (
    transaction_type   varchar2(1),
    card_num           varchar2(40),
    id                 varchar2(12),
    transaction_date   date ,
    transaction_amount number(10),
    merchant_id        varchar2(30),
    common              varchar2(12),
    comment_purchase   varchar2(2000)
);

create type transaction_table is table of transaction_type;

create table fs11_file_records (
    file_id       varchar2(12)  not null primary key, -- internal or supernumerary?
    file_date     date          not null,
    file_type     varchar2(8)   not null
        constraint check_file_type check (file_type in ('incoming', 'response', 'report')),
    file_status   varchar2(100) not null
        constraint check_file_status check (file_status in ('new', 'processed', 'rejected')),
    error_message varchar2(1000) default null
);

create table fs11_file_content (
    file_id      varchar2(12)        not null primary key, -- supernumerary
        constraint fk_1 foreign key (file_id) references fs11_file_records (file_id),
    file_content clob not null -- that's what we parse
);
/
create table fs11_clients (
    client_id  number        not null primary key,
    first_name varchar2(200) not null,
    last_name  varchar2(200) not null,
    phone      varchar2(11)  not null,
        /*
        constraint check_phone check (phone like '7%' / '8%'), -- ?
         */
    email      varchar2(100) not null
        /*
        constraint check_email check (email like '%@%.%')
         */
);

create table fs11_cards (
    card_num   varchar2(200) primary key,
    client_id  number not null,
        constraint fk_cards_to_clients foreign key (client_id) references fs11_clients (client_id),
    start_date date          not null,
    end_date   date          not null,
    status     varchar2(7)   not null
        constraint check_card_status check (status in ('active', 'blocked')), -- ? constraint name from pl/sql developer
    card_role  varchar2(6)   not null
        constraint check_card_role check (card_role in ('master', 'slave'))
);
/
-- calc_rules (in order): exception list 0%, special list 0-10%, default list 1%
-- add attribute period_id?

-- not realized
create table fs11_mcc (
       mcc number(4) primary key
       --mcc_name varchar(200)
       );

create table fs11_mcc_rules (
       mcc number(4) primary key,
        constraint fk_mcc_rules_to_mcc foreign key (mcc) references fs11_mcc (mcc),
       --period_id (?)
       --percent_cash number default null (?)
       start_date date default null,
       end_date date default null,
       percent_cash number default null
       );

create table fs11_merchants (
       merchant_id varchar2(30) primary key
       --merchant_name varchar2(200)
       );

create table fs11_merchant_rules (
       merchant_id varchar2(30) primary key,
        constraint fk_2 foreign key (merchant_id) references fs11_merchants (merchant_id),
       --period_id (?)
       --percent_cash number default null (?)
       start_date date default null,
       end_date date default null,
       percent_cash number default null
       );
/
-- sections for dates!
create table fs11_purchases (
    card_num           varchar2(40)  not null,
        constraint fk_purchases_to_cards foreign key (card_num) references fs11_cards (card_num),
    id                 varchar2(12)  not null primary key,-- Unique purchase identifier in the merchant accounting system.
    transaction_date   date          not null,            -- yyyymmddhh24miss
    transaction_amount number(10)    not null,
    merchant_id        varchar2(30)  not null,
        constraint fk_purchases_to_merchants foreign key (merchant_id) references fs11_merchants (merchant_id),
    mcc                number(4),
        constraint fk_purchases_to_mcc  foreign key (mcc) references fs11_mcc (mcc),
    comment_purchase   varchar2(2000) default null
);
-- sections for dates!
create table fs11_refunds (
    card_num           varchar2(40)   not null,
         constraint fk_refunds_to_cards foreign key (card_num) references fs11_cards (card_num),
    id                 varchar2(12)   not null primary key,
    transaction_date   date           not null,
    transaction_amount number(10)     not null,
    merchant_id        varchar2(30)   not null,
        constraint fk_refunds_to_merchants foreign key (merchant_id) references fs11_merchants (merchant_id),
    purchase_id        varchar2(12)   not null,
        constraint fk_refunds_to_purchases  foreign key (purchase_id) references fs11_purchases (id),
    comment_refund     varchar2(2000) default null
);

/
-- percent_cash default null fk with merch & mcc (?)

create table fs11_periods (
    period_id     number    primary key,
    period_date   date,
    period_status varchar2(7) not null
        constraint check_period_status check (period_status in ('current', 'report'))
);

create table fs11_card_cashbacks (
    -- timestamp    timestamp,
    period_id         number,
    card_num          varchar2(12),
        constraint fk_3 foreign key (card_num) references fs11_cards (card_num),
    purchases_count   number, -- <= 10
    calc_cashback     number  -- <= 100
);

/*
    /*
    client_id not null,
        constraint fk_periods_to_clients foreign key (client_id) references fs11_clients (client_id), */
    /*
period_form varchar2() not null
        constraint check_period_form check (period_form in ('unloaded', '')), --check current or report
! catalogue for own codes of errors

 */

create or replace package fs11_processing_incoming_file as
    procedure fs11_proc_file(p_file_id varchar2);
end fs11_processing_incoming_file;

create or replace package body fs11_processing_incoming_file as
--     subtype merchant is varchar2(30);
    period_date date;
    file_date date;

    type mcc_rules_table is table of FS11_MCC_RULES%rowtype;
    mcc_rules mcc_rules_table := mcc_rules_table();

    type merchant_rules_table is table of FS11_MERCHANT_RULES%rowtype;
    merchant_rules merchant_rules_table :=  merchant_rules_table();


--     type purchases_records is table of fs11_purchases%rowtype;
    type refunds_records is table of fs11_refunds%rowtype;
--     purchases purchases_records := purchases_records();
--     purchases TRANSACTION_TABLE;
    transactions TRANSACTION_TABLE;
    refunds refunds_records := refunds_records();

    type refund_amount_type is table of number index by VARCHAR2(12);
    refund_amounts refund_amount_type;

    type record_fields is table of varchar2(2000); -- index by pls_integer;
    array record_fields := record_fields();

    file_length number;
    file_pos number;
    file_content clob;
    file_id varchar2(12);

    purchases_integrity exception;
    refunds_integrity exception;
    file_id_exist exception;
    trans_id_exist exception;
    refund_more_than_purschase exception;
    pragma exception_init(refund_more_than_purschase, -20000);
    transaction_wrong_date exception;

    procedure print(p_message varchar2) as
    begin
        dbms_output.put_line(p_message);
    end;

    procedure proc_header as -- control of unique ?
    begin
        file_date := to_date(array(3), 'yyyymmddhh24miss');
--         load_rules;
--         file_id := array(2);
--         insert into FS11_FILE_RECORDS (file_id, file_name, file_date, file_type, file_status)
--         values (file_id, 'not/need', to_date(array(3), 'yyyymmddhh24miss'), 'incoming', 'new');
--         print('HEADER:');
--         fs11_print_array(array);
--     exception
--         when dup_val_on_index then raise file_id_exist;
    end;

    procedure proc_purchase as
        i number;
    begin
        -- todo rise cast exception
--         purchases.extend;
--         i := purchases.last;
--         purchases(i).card_num := array(2);
--         purchases(i).id := array(3);
--         purchases(i).transaction_date := to_date(array(4), 'yyyymmddhh24miss');
--         purchases(i).transaction_amount := to_number(array(5));
--         purchases(i).merchant_id := array(6);
--         purchases(i).mcc := to_number(array(7), '9999');
--         purchases(i).comment_purchase := array(8);

            null;

--             transactions.extend;
--             transactions(transactions.last) := TRANSACTION_TYPE(
--                 array(1),
--                 array(2),
--                 array(3),
--                 to_date(array(4), 'yyyymmddhh24miss'),
--                 to_number(array(5)),
--                 array(6),
--                 array(7),
--                 array(8)
--             );

--         if purchases(i).transaction_date not between period_date and file_date then
--             -- TODO Write error records
--             raise transaction_wrong_date;
--         end if;
    end;

    procedure proc_transaction as
    begin
        null;
        transactions.extend;
        transactions(transactions.last) := TRANSACTION_TYPE(
                array(1),
                array(2),
                array(3),
                to_date(array(4), 'yyyymmddhh24miss'),
                to_number(array(5)),
                array(6),
                array(7),
                array(8)
            );

        --         if purchases(i).transaction_date not between period_date and file_date then
    --             -- TODO Write error records
    --             raise transaction_wrong_date;
    --         end if;
    end;


    procedure fs11_proc_refund as
        i number;
    begin
        -- todo rise cast exception
--         refunds.extend;
--         i := refunds.last;
--         refunds(i).card_num := array(2);
--         refunds(i).id := array(3);
--         refunds(i).transaction_date := to_date(array(4), 'yyyymmddhh24miss');
--         refunds(i).transaction_amount := to_number(array(5));
--         refunds(i).merchant_id := array(6);
--         refunds(i).purchase_id := array(7);
--         refunds(i).comment_refund := array(8);

--          if refunds(i).transaction_date not between period_date and file_date then
--             -- TODO Write error records
--             raise transaction_wrong_date;
--         end if;

        if (refund_amounts.exists(refunds(i).purchase_id)) then
            refund_amounts(refunds(i).purchase_id) := refund_amounts(refunds(i).purchase_id) + refunds(i).transaction_amount;
        else
            refund_amounts(refunds(i).purchase_id) := refunds(i).transaction_amount;
        end if;

    end;

--     procedure fs11_proc_trailer as
--     begin
--         print('Trailer purchases: ' || array(2) || ' purchase_record_collection.count: ' || purchases.count);
--         print('Trailer refunds: ' || array(3));
--         if purchases.count <> to_number(array(2)) then
--             raise purchases_integrity;
--         end if;
--         if refunds.count <> to_number(array(3)) then
--             raise refunds_integrity;
--         end if;
--     end;

    procedure error_log(p_message varchar2) as
    begin
        update FS11_FILE_RECORDS set error_message = p_message where FS11_FILE_RECORDS.FILE_ID = file_id;
        print(p_message);
    end;

    procedure fs11_parse_record as
        delimiter_pos       number;
        record_end_pos      number;
        record_field_number integer := 0;
    begin
        record_end_pos := INSTR(file_content, chr(10), file_pos);
        array.delete;
        -- if it is last record then we don't have delimiter after last field
        if record_end_pos = file_length then
            loop
                delimiter_pos := INSTR(file_content, ';', file_pos);
                record_field_number := record_field_number + 1;
                array.extend;
                -- resolve upper comment problem here
                if delimiter_pos = 0 then
                    array(record_field_number) := SUBSTR(file_content, file_pos, file_length - file_pos);
                    file_pos := file_length;
                    exit;
                end if;
                if delimiter_pos < record_end_pos then
                    array(record_field_number) := SUBSTR(file_content, file_pos, delimiter_pos - file_pos);
                    file_pos := delimiter_pos + 1;
                else
                    array(record_field_number) := SUBSTR(file_content, file_pos, delimiter_pos - file_pos - 2);
                    file_pos := delimiter_pos - 1;
                    exit;
                end if;
            end loop;
        else
            loop
                delimiter_pos := INSTR(file_content, ';', file_pos);
                record_field_number := record_field_number + 1;
                array.extend;
                if delimiter_pos < record_end_pos then
                    array(record_field_number) := SUBSTR(file_content, file_pos, delimiter_pos - file_pos);
                    file_pos := delimiter_pos + 1;
                else
                    array(record_field_number) := SUBSTR(file_content, file_pos, delimiter_pos - file_pos - 2);
                    file_pos := delimiter_pos - 1;
                    exit;
                end if;
            end loop;
        end if;
    end;

    procedure load_rules as
        begin
            select * bulk collect into mcc_rules
            from FS11_MCC_RULES
            where START_DATE <= file_date and period_date < END_DATE;
            select * bulk collect into merchant_rules
            from FS11_MERCHANT_RULES
            where START_DATE <= file_date and period_date < END_DATE;
        end;

    procedure process_records as
        subtype card_type is varchar2(40);
        type cards_clients_table is table of integer index by card_type;
        card_2_client cards_clients_table;
    begin
        null;
    end;


    procedure fs11_proc_file(p_file_id varchar2) as
    begin
        transactions := TRANSACTION_TABLE();

        select * bulk collect into mcc_rules
            from FS11_MCC_RULES
            where START_DATE <= file_date and period_date < END_DATE;

--         select PERIOD_DATE into period_date from FS11_PERIODS where PERIOD_STATUS = 'current';

        -- todo
        file_id := p_file_id;
        print('File ID: ' || file_id);
        select file_content into file_content from FS11_FILE_CONTENT
        where FS11_FILE_CONTENT.file_id = p_file_id;


        array := record_fields();
        file_pos := 1;
        file_length := LENGTH(file_content);

--         purchases.delete;
--         refunds.delete;

        print('File length: ' || file_length);

        loop
            fs11_parse_record;
            case (array(1))
                when 'H' then
                    proc_header;
                when 'P' then
                proc_transaction;
--                     proc_purchase;
                when 'R' then
                proc_transaction;
--                     fs11_proc_refund;
                when 'T' then
                    null;
--                     fs11_proc_trailer;
                else
                    print(array(1));
                end case;
            if file_pos = file_length then
                exit;
            end if;
        end loop;

        begin
--             for indx in 1 .. purchases.COUNT
--                 loop
--                     if refund_amounts.exists(purchases(indx).id) then
--                         if purchases(indx).TRANSACTION_AMOUNT - refund_amounts(purchases(indx).id) < 0 then
--                             raise_application_error(-20000, 'purchase_id = ' || purchases(indx).id);
--                         end if;
--                     end if;
--                 end loop;


--             forall indx in 1..purchases.count
--                 insert into fs11_purchases
--                 values purchases(indx);
--
--             forall indx in 1..purchases.count
--                 insert into fs11_purchases
--                 values (purchases(indx).card_num,
--                         purchases(indx).id,
--                         purchases(indx).transaction_date,
--                         purchases(indx).transaction_amount,
--                         purchases(indx).merchant_id,
--                         to_number(purchases(indx).common, '9999'),
--                         purchases(indx).comment_purchase);

--      https://asktom.oracle.com/pls/asktom/f?p=100:11:0::::P11_QUESTION_ID:9539655000346985922

            insert into FS11_PURCHASES
                select card_num,
                        id,
                        transaction_date,
                        transaction_amount,
                        merchant_id,
                        to_number(common, '9999'),
                        comment_purchase
                            from table(transactions)
                where transaction_type = 'P';

             insert into FS11_REFUNDS
                select  card_num,
                        id,
                        transaction_date,
                        transaction_amount,
                        merchant_id,
                        common,
                        comment_purchase
                            from table(transactions)
                where transaction_type = 'R';

            insert into FS11_TRANSACTION_TEMP
                select  *
                            from table(transactions);
               -- where transaction_type = 'R';

--             forall indx in 1..refunds.count
--                 insert into fs11_refunds
--                 values refunds(indx);

            commit;

        exception
            when dup_val_on_index then raise trans_id_exist;
        end;

    print('Complete.');

    exception
--         when file_id_exist then error_log('This file identifier was used before.');
        when purchases_integrity then error_log('Error in the trailer. Number of purchases is wrong.');
        when refunds_integrity then error_log('Error in the trailer. Number of refunds is wrong');
        when trans_id_exist then error_log('This transaction identifier was used before.');
        when refund_more_than_purschase then error_log('refund_more_than_purschase ' || SQLERRM);
        when transaction_wrong_date then error_log('transaction_wrong_date');

    end;

end fs11_processing_incoming_file;

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
        purcahses_date  date;
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
                        purchase_amount := random_number(1000) * 1000 + 1000;
                        purcahses_date := transaction_date;
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
                                                   to_char(purcahses_date + 1, 'yyyymmddhh24miss') ||
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

   /* function generate_current_cashback_file return clob as
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
*/
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
--     print(generate_current_cashback_file);
--     print(generate_total_cashback_file);
--     return;

   trunc_tables;

    file_id := random_string(12);
    generate_MCC(500);
    generate_merchants(100);
    generate_rules;
    generate_clients(10);
    generate_cards(per_client_card_count => 3);
    insert_into_tables;
    generate_file_content(file_id, per_card_purchases => 10);
    insert_file_into_tables;
    proc_file_table;
end;