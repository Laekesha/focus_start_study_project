drop table fs11_file_content;
drop table fs11_file_records;
drop table fs11_refunds;
drop table fs11_purchases;
drop table fs11_cards;
drop table fs11_clients;

drop table fs11_mcc;
drop table fs11_merchants;
drop table fs11_periods;

-- logging?

create table fs11_file_records (
    file_id       varchar2(12)  not null primary key,
    file_date     date          not null,
    file_type     varchar2(8)   not null
        constraint check_file_type
            check (file_type in ('incoming', 'response', 'report')),
    file_status   varchar2(100) not null
        constraint check_file_status
            check (file_status in ('new', 'processed', 'rejected')),
    error_message varchar2(1000) default null
);

create table fs11_file_content (
    file_id      varchar2(12)        not null primary key,
        constraint fk_file_content_to_file_records foreign key (file_id) references fs11_file_records (file_id),
    file_content clob not null
);
/
create table fs11_clients (
    client_id  number        not null primary key,
    first_name varchar2(200) not null,
    last_name  varchar2(200) not null,
    phone      varchar2(11)  not null,
        /*
        constraint check_phone
            check (phone like '7%' / '8%'), -- ?
         */
    email      varchar2(100) not null
        /*
        constraint check_email
            check (email like '%@%.%')
         */
);

create table fs11_cards (
    card_id    number        not null primary key,
    card_num   varchar2(200) not null unique,
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
create table fs11_purchases (
    card_num           varchar2(40)  not null,
        constraint fk_purchases_to_cards foreign key (card_num) references fs11_cards (card_num),
    id                 varchar2(12)  not null primary key,-- Unique purchase identifier in the merchant accounting system.
    transaction_date   date          not null,            -- yyyymmddhh24miss
    transaction_amount number(10)    not null,
    merchant_id        varchar2(30)  not null,
    mcc                number(4),
    comment_purchase   varchar2(2000) default null
);

create table fs11_refunds (
    card_num           varchar2(40)   not null,
         constraint fk_refunds_to_cards foreign key (card_num) references fs11_cards (card_num),
    id                 varchar2(12)   not null primary key,
    transaction_date   date           not null,
    transaction_amount number(10)     not null,
    merchant_id        varchar2(30)   not null,
    purchase_id        varchar2(12)   not null,
        constraint fk_refunds_to_purchases  foreign key (purchase_id) references fs11_purchases (id), -- = fs11_purchases.id
    comment_refund     varchar2(2000) default null
);


/
-- attributes?
create table fs11_mcc (
       id_mcc number not null, -- pk
       mcc number(4) not null
       -- percent_cash
       );

create table fs11_merchants (
       id_merch number not null, -- pk
       merch_name varchar2(200) not null,
       address varchar2(200) not null,
       phone varchar2(100) -- not null? check
       -- term_num number
       );

-- calc_rules (in mcc and merch?)
create table fs11_periods (
       period_id,
       status_period varchar2(50),  -- name? check current or report
       --procent_cash
       calc_cashback number not null
       );