drop table fs11_file_content;
drop table fs11_file_records;
drop table fs11_refunds;
drop table fs11_purchases;
drop table fs11_clients;
drop table fs11_cards;
drop table fs11_mcc;
drop table fs11_merchants;
drop table fs11_periods;

--logging?
--indexes for the fs11_transactions
-- to_char('yyyy-mm-dd hh24:mi:ss')
-- (id;pan;dt;amount;merchant;mcc;description)

create table fs11_file_records (
    file_id number not null primary key, -- or just id?
    file_name varchar2(1000) not null,
    file_date date not null,
    file_type varchar2(100)  not null
        constraint check_file_type
            check (file_type in ('incoming', 'response', 'report')),
    file_status varchar2(100)  not null
        constraint check_file_status
            check (file_status in ('new', 'processed', 'rejected')),
    error_message varchar2(1000)
);

create table fs11_file_content (
    file_id number not null primary key
        constraint fk_file_id
            references fs11_file_records,
    file_content varchar2(200) not null
);
/
create table fs11_purchases (
	card_num varchar2(40) not null,
	id varchar2(12) not null primary key,-- Unique purchase identifier in the merchant accounting system.
	transaction_date date not null, -- yyyymmddhh24miss
	transaction_amount number(10) not null,
	merchant_id varchar2(30) not null,
	mcc number(4),
	comment_purchase varchar2(200) not null
);

create table fs11_refunds (
	card_num varchar2(40) not null,
	id varchar2(12) not null primary key,
	transaction_date date not null,
	transaction_amount number(10) not null,
	merchant_id varchar2(30) not null,
	purchase_id varchar2(12) not null
		constraint fs11_refunds_fs11_purchases_id_fk -- = fs11_purchases.id
			references fs11_purchases (id),
	comment_refund varchar2(2000) not null
);
/

create table fs11_clients (
       client_id number not null, -- pk
       first_name varchar2(200) not null,
       last_name varchar2(200) not null,
       phone varchar2(100) not null, -- need check constraint to the special format
       email varchar2(100) -- check consrtaint
             /*constraint pk_id_period primary key (id_period)
              constraint fk_ foreign key ()
                         reference ()
             */
       );

create table fs11_cards (
       card_id number not null, --pk
       card_num varchar2(200), -- cause cipher --fk
       client_id number not null, -- fk
       start_date date not null,
       end_date not null,
       status varchar2(50), -- logic type? 'active' and 'blocked'
       card_role varchar2(50) -- lt?
              /*constraint pk_id_period primary key (id_period)
              constraint fk_ foreign key ()
                         reference ()
              */
       );

/*
create table fs11_transactions (
       id_trans number not null, -- pk
       card_num varchar2(200) -- cause cipher --fk
       date_trans date not null, -- varchar2?
       amount_trans number(?) --not null? whole or fractional?
       --term_num number(3) ?
       id_merch number not null, -- or merch_name? fk
       mcc number(4) not null, -- fk
       type_trans varchar2(50) not null, -- logic type?
              constraint pk_id_period primary key (id_period),
              constraint fk_ foreign key ()
                         reference ()
       );
--indexes for parsing
create unique index id_trans_idx on fs11_transactions(id_trans);
create index card_num_idx on fs11_transactions(card_num);
create index date_trans_idx on fs11_transactions(date_trans);
create index amount_trans_idx on fs11_transactions(amount_trans);
-- create index term_num_idx on 6013_transactions(term_num);
create index id_merch_idx on fs11_transactions(id_merch);
create index mcc_idx on fs11_transactions(mcc);
create index type_trans_idx on fs11_transactions(type_trans);
*/


--new attributes?
create table fs11_mcc (
       id_mcc number not null,
       mcc number(4) not null
       mcc_percent_cash
           /*constraint pk_id_period primary key (id_period)
              constraint fk_ foreign key ()
                         reference ()
              */
       );

create table fs11_merchants (
       id_merch number not null, -- pk, fk from 6013_transactions
       merch_name varchar2(200) not null,
       address varchar2(200) not null,
       phone varchar2(100) -- not null? check
       -- term_num number
              /*constraint pk_id_period primary key (id_period)
              constraint fk_ foreign key ()
                         reference ()
              */
       );

-- attributes?? and calc.rules (in mcc and merch?)
create table fs11_periods (
       id_period,
       status_period varchar2(50),  -- name? current or report, only two means - logic type?


       calc_cashback number not null
              /*constraint pk_id_period primary key (id_period)
              constraint fk_ foreign key ()
                         reference ()
              */
       );