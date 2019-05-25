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
        constraint fk_file_content_to_file_records foreign key (file_id) references fs11_file_records (file_id),
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
create table fs11_mcc (
       mcc number(4) not null primary key,
       percent_cash number default null
       );

create table fs11_merchants (
       merchant_id varchar2(30) primary key,
       -- merchant_name varchar2(200) not null,
       percent_cash number default null
       );
/
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
    period_id     number, -- primary key,
    period_date   timestamp,
    period_status varchar2(7) not null
        constraint check_period_status check (period_status in ('current', 'report')),
    calc_cashback number  --
);

create table fs11_card_cashbacks (
    period_id         number, -- primary key,
    period_status     varchar2(7) not null
        constraint check_period_status check (period_status in ('current', 'report')),
    card_num          varchar2(12),
    constraint fk_card_cashbacks_to_cards foreign key (card_num) references fs11_cards (card_num),
    transaction_count number, -- <= 10
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