--project
-- 1. structure

/*
drop table 6013_content_file;
drop table 6013_reg_files;
drop table 6013_clients;
drop table 6013_cards;
drop table 6013_transactions;
drop table 6013_mcc;
drop table 6013_merchants;
drop table 6013_periods;
*/

--constraints! pk, fk, check, unique
--logging!
--indexes for the 6013_transactions
-- to_char('yyyy-mm-dd hh24:mi:ss')
-- (id;pan;dt;amount;merchant;mcc;description)

-- file exchange csv, common table #1, constraints?
create table /*6013_*/content_file (
       id_file number not null, -- pk. fk?
       content_file varchar2(200) not null
       );

-- file exchange csv, common table #2, constraints?
create table /*6013_*/reg_files (
       id_file number not null, --pk
       -- file_name varchar2(100) not null,
       file_type varchar2() --'incoming' = 'i', 'report' = 'r', 'out_' = 'o'
       --file_date date not null, -- date of what?? creation, get, ...
       file_status varchar2(50) -- 'get', ...
       );

--done
create table 6013_clients (
       id_client number not null, -- pk
       first_name varchar2(200) not null,
       last_name varchar2(200) not null,
       phone varchar2(100) not null, -- need check constraint to the special format
       email varchar2(100) -- check consrtaint
             /*constraint pk_id_period primary key (id_period)
              constraint fk_ foreign key ()
                         reference ()
             */
       );

create table 6013_cards (
       id_card number not null, --pk
       card_num varchar2(200) -- cause cipher --fk
       id_client number not null, -- fk
       start_date date not null,
       end_date not null,
       status varchar2(50) -- logic type? 'active' and 'blocked'
       role_card varchar2(50)-- lt?
              /*constraint pk_id_period primary key (id_period)
              constraint fk_ foreign key ()
                         reference ()
              */
       );

--
create table 6013_transactions (
       id_trans number not null, -- pk
       card_num varchar2(200) -- cause cipher --fk
       date_trans date not null, -- varchar2?
       amount_trans number(?) --not null? whole or fractional?
       --term_num number(3) ?
       id_merch number not null, -- or merch_name? fk
       mcc number(4) not null, -- fk
       type_trans varchar2(50) not null -- logic type?
              /*constraint pk_id_period primary key (id_period)
              constraint fk_ foreign key ()
                         reference ()
              */
       );
--indexes for parsing
create unique index id_trans_idx on 6013_transactions(id_trans);
create index card_num_idx on 6013_transactions(card_num);
create index date_trans_idx on 6013_transactions(date_trans);
create index amount_trans_idx on 6013_transactions(amount_trans);
-- create index term_num_idx on 6013_transactions(term_num);
create index id_merch_idx on 6013_transactions(id_merch);
create index mcc_idx on 6013_transactions(mcc);
create index type_trans_idx on 6013_transactions(type_trans);



--new attributes?
create table 6013_mcc (
       id_mcc number not null,
       mcc number(4) not null
       mcc_percent_cash
           /*constraint pk_id_period primary key (id_period)
              constraint fk_ foreign key ()
                         reference ()
              */
       );

create table 6013_merchants (
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
create table 6013_periods (
       id_period,
       status_period varchar2(50),  -- name? current or report, only two means - logic type?


       calc_cashback number not null
              /*constraint pk_id_period primary key (id_period)
              constraint fk_ foreign key ()
                         reference ()
              */
       );
------------------------------------------------------------------------------------------------------------------------

-- 2. get transaction information
--parse

---- to_date('yyyy-mm-dd hh24:mi:ss')
-- file: 'id;pan;dt;amount;merchant;mcc;description', merchant = merch_name (?)

create or replace function 6013_parsing(p_content_file in out varchar2) --, index_count number)
return varchar2;
as

content_file varchar2(200); --reference?
--index_count number;

begin
  -- id_table := ; --dynamic?
  -- index columnl

  -- for i in 1..index_count
      instr(content_file, 1, ';', ) from 6013_content_file; -- before ';'
      -- ltrim(content_file, '')


end
/
create or replace procedure 6013_processing_trans (column_count _?_ number) --or index_count
as
-- reference to the columns of the 6013_transactions?
--column1 number; ...
-- cursor on the 'i' or indexes?
begin
  for i in 1..column_count
    loop
      column(i) := 6013_parsing(content_file, i, );
    end loop;

  -- := 6013_parsing(content_file, ); --1..n fields, syntax?
end
/