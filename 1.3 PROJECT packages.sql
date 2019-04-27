-- to_date('yyyy-mm-dd hh24:mi:ss')
-- view of file: 'id;pan(card);dt;amount;merchant;mcc;description'

create or replace package 6013_processing_incoming_file 
  as
       -- two variables for header and trailer
       type fields_tab is table of varchar2(2000);
       function 6013_parse (incoming_string varchar2);
       function 6013_procFiles (); --public. to record files in the special table?
                                   -- is there checking exists and other checks?
       function 6013_procHead (ptFields in Fields_tab); -- is private and next others too
       function 6013_procPurchase ();
       function 6013_procRefund ();
       function 6013_procTrailer ();
  end;
/
-- our collection type is nested table
     
-- parsing with collections. first option. fuflo
create or replace type 6013_parsed_array as table of varchar2(200);

create or replace function 6013_parse (string varchar2)
return 6013_parsed_array
as
6013_parsed_array;
begin
  array(1):= string ||'1';
  array(2):= string ||'2';
  dbms_output.put_line(array(1));
  return array;
end;

declare
 begin
  dbms_output.put_line(6013_parse('varchar'));
  t := 6013_parse('varchar');
 end; 
/
-- parsing with collections. second option. ne to
declare
--;
 begin
  vtFields := fields_tab ('P','xxx','gjeghe', '325364869', 10000, 'pyatorocka', 0513)
  for i in 1..ptFields.count 
             loop
              dbms_output.put_line(i||': '||vtFields(i)); 
             end loop;
 end;             
/
-- parsing with collections. fird option. so cute
create or replace type parsed_array as table of varchar2(200); -- done

-- vtField Fields_tab := Fields_tab();

create or replace function /*6013_*/parseString (string varchar2)
 return parsed_array
 as 
 cursor pFields is
        select * from 
               (select regexp_substr(str, '[^:]', 1, level) as substr from
                       (select string as str from dual)
                        connect by level <= lenght(regexp_replace(str, '[^;]+')+1);
 array parsed_array := parsed_array;
 counter integer := 0;
 /*
 begin
  regexp_count(str, '[^;]+');
  lenght(regexp_replace((str, '[^;]+')));
 end;
 */
begin
  for n in pFields 
    loop
      counter := counter + 1;
      array.extend;
      array(counter) := n.substr;
      dbms_output.put_line(Field||'('||counter||')'||' :'||array(counter));
      
    return array;      
    end loop;
end;                                                                                        
/
create or replace package body 6013_processing_incoming_file 
  as
   
   parsed_array as table of varchar2(200);

   -- vtField Fields_tab := Fields_tab(); ?

   function /*6013_*/parseString (string varchar2)
    return parsed_array
     as 
    cursor pFields is
        select * from 
               (select regexp_substr(str, '[^:]', 1, level) as substr from
                       (select string as str from dual)
                        connect by level <= lenght(regexp_replace(str, '[^;]+')+1);
    array parsed_array := parsed_array;
    counter integer := 0;
    /*
    begin
     regexp_count(str, '[^;]+');
     lenght(regexp_replace((str, '[^;]+')));
    end;
    */
   begin
    for n in pFields 
     loop
      counter := counter + 1;
      array.extend;
      array(counter) := n.substr;
      dbms_output.put_line(Field||'('||counter||')'||' :'||array(counter));
     end loop; 
    
    return array;      
end;
    
    function 6013_procFiles () 
      as
      begin
        
      end;
    
    function 6013_procHead (ptFields in Fields_tab); 
      as
      vtFields fields_tab;
      begin
       for i in 1..ptFields.count 
        loop
               
        end loop;
      end;
      
    function 6013_procPurchase () 
      as
      begin
        
      end;
      
    function 6013_procRefund () 
      as
      begin
        
      end;
      
    function 6013_procTrailer () 
      as
      begin
        
      end;
      
  end body 6013_processing_incoming_file; 
