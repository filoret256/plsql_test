create or replace package test is

  -- Author  : Fedor Levinchuk
  -- Purpose : Test

  test_table           varchar2(100) := 'test_data'; --название таблицы с основными данными
  test_seq             varchar2(100) := 'test_seq'; --название сиквенса для таблицы

  test_event_table     varchar2(100) := 'test_data_event'; --название таблицы с эвентами для сновной таблицы содержатся данные по изменению поля some_value
  test_event_table_seq varchar2(100) := 'test_data_event_seq'; --название сиквенса для таблицы
  
  test_trigger         varchar2(100) := 'test_check_defore_insert'; -- название тригера отрабатывает до добавления записи в test_table, закрывает все прошлые записи по client_id

  procedure gen_data(in_number in number); --генерит случайные данные
  procedure gen_event(event_number in number); --генерит случайные эвенты
  procedure run_test; --содержит в себе обе процедуры gen_data и gen_event
end test;
/
create or replace package body test is

  ------------------------------------
  --проверка наличия объектов в схеме 
  function check_tbl_exts(in_obj_name in varchar, in_type in varchar2)
    return boolean is
  
    v_n number;
  begin
  
    select count(1)
      into v_n
      from user_objects t
     where t.OBJECT_TYPE = in_type
       and upper(t.OBJECT_NAME) = upper(in_obj_name);
  
    if v_n = 1 then
    
      dbms_output.put_line(in_type || ' ' || in_obj_name || ' существует');
      return true;
    
    end if;
  
    dbms_output.put_line(in_type || ' ' || in_obj_name || ' НЕ существует');
    return false;
  
  end;

  ------------------------------------
  --процедура создания контента для таблицы test_data
  procedure gen_data(in_number in number) is
  
    v_client_id   number;
    v_client_name varchar2(100);
    v_some_data   number;
    sql_string    varchar2(3000);
    v_incr        number;
  
  begin
  
  --проверка наличия таблицы, в случае отсутствия создает
    if check_tbl_exts(test_table, 'TABLE') != true then
    
      dbms_output.put_line('создаем ' || test_table);
    
      execute immediate 'create table ' || test_table || '(
      id number,
      client_id number,
      client_name varchar2(100),
      date_from timestamp,
      date_to  timestamp,
      some_value number
    )';
    
    end if;

  --проверка наличия сиквенса, в случае отсутствия создает
    if check_tbl_exts(test_seq, 'SEQUENCE') != true then
    
      dbms_output.put_line('создаем ' || test_seq);
    
      execute immediate 'create sequence ' || test_seq || '
      minvalue 1
      maxvalue 999999999999999999999999999
      start with 1
      increment by 1';
    
    end if;

  --проверка наличия триггера, в случае отсутствия создает
    if check_tbl_exts(test_trigger, 'TRIGGER') != true then
    
      dbms_output.put_line('создаем ' || test_trigger);
    
      execute immediate '
    create or replace trigger ' || test_trigger || '
  before insert on ' || test_table || ' 
  for each row 
declare
  v_n number;
begin
  
  select count(1) into v_n 
  from ' || test_table || ' where client_id = :new.client_id and date_to is null;
    
  if v_n >0 then

  update ' || test_table || ' t
  set date_to = systimestamp
  where t.client_id = :new.client_id
  and date_to is null;

  end if;  
  
end ' || test_trigger || ';';
  --тригер проверяет открытые записи по clien_id , если есть закрывает текущим таймстемпом
    end if;
  
    dbms_output.put_line('генерим какой то контент');
  
  --создает контент в таблицу, число зависит от входного параметра
    for x in 1 .. in_number loop
    
      v_client_id   := round(dbms_random.value(1, 50));
      v_client_name := 'client_' || dbms_random.string('l', 1);
      v_some_data   := round(dbms_random.value(1, 1000));
      v_incr        := round(dbms_random.value(1, 5));
    
      sql_string := 'insert into ' || test_table || '
        (id,client_id,client_name,date_from,some_value)
    values
        (' || test_seq || '.nextval , ' || v_client_id ||
                    ' , ' || '''' || v_client_name || '''' ||
                    ',systimestamp,' || v_some_data || '    )';
    
      execute immediate sql_string;
    
    end loop;
  
    commit;
  
  --проверка на ошибки, в случае если будет как нибудь ошибка выведет в dbms_output
  exception
    when others then
      begin
        dbms_output.put_line('ошибка ' || SQLERRM);
        rollback;
      end;
  end;

--процедура создания контента для таблицы test_data_event
  procedure gen_event(event_number in number) is
  
    TYPE t_table_clients_col IS TABLE OF number;
    v_clients_id t_table_clients_col;
    v_max_colect number;
    v_client_id  number;
    v_some_data  number;
    v_old_val    number;
    sql_string   varchar2(3000);
  begin
  
  --проверяет наличие таблицы test_data_event, в случае отсутствия создает
    if check_tbl_exts(test_event_table, 'TABLE') != true then
    
      dbms_output.put_line('создаем ' || test_event_table);
    
      execute immediate 'create table ' || test_event_table || '(
      id number,
      oper_date timestamp,
      client_id number,
      old_val number,
      new_val number
    )';
    
    end if;
  --проверяет наличие сиквенса, в случае отсутствия создает
    if check_tbl_exts(test_event_table_seq, 'SEQUENCE') != true then
    
      dbms_output.put_line('создаем ' || test_event_table_seq);
    
      execute immediate 'create sequence ' || test_event_table_seq || '
      minvalue 1
      maxvalue 999999999999999999999999999
      start with 1
      increment by 1';
    
    end if;
  
    --заносим все активные ( не закрытые ) данные в коллекацию
    execute immediate 'select client_id from ' || test_table ||
                      ' where systimestamp between date_from and nvl(date_to,systimestamp+interval''1''minute) ' BULK
                      COLLECT
      INTO v_clients_id;
    
    --присваеваем переменно общее количество открытыз записей, сделано для большей наглядности
    v_max_colect := v_clients_id.last;
  
    dbms_output.put_line('генерируем эвенты');
    
    --генерим какой то контент в зависимости от входных данных
    for x in 1 .. event_number loop
    
      v_client_id := round(dbms_random.value(1, v_max_colect));
      v_some_data := round(dbms_random.value(1, 1000));

      --находим старое значнеие поля some_value и созхраняем в переменную v_old_val, пригодится дял заполнения таблицы test_event_table
      sql_string := ' select some_value from ' || test_table ||
                    ' where client_id = ' ||
                    to_char(v_clients_id(v_client_id)) ||
                    ' and systimestamp between date_from and nvl(date_to,systimestamp+interval''1''minute)';
    
      execute immediate sql_string
        into v_old_val;
      -- обновляем значнеие поля some_value, сделал поиск по client_id дял наглядности, быстрее было бы по rowid
      sql_string := ' update ' || test_table ||
                    ' set some_value = some_value + ' || v_some_data ||
                    ' where client_id = ' ||
                    to_char(v_clients_id(v_client_id)) ||
                    ' and systimestamp between date_from and nvl(date_to,systimestamp+interval''1''minute) ';
    
      execute immediate sql_string;

      --вставляем полученые данные в test_event_table
      sql_string := 'insert into ' || test_event_table ||
                    '( id,oper_date,client_id,old_val,new_val) 
                     values (' || test_event_table_seq ||
                    '.nextval, systimestamp,' ||
                    to_char(v_clients_id(v_client_id)) || ',' || v_old_val || ',' ||
                    to_char(v_old_val + v_some_data) || ')';
    
      execute immediate sql_string;
    
    end loop;
  
    commit;
    dbms_output.put_line('эвенты созданы, всего ' || event_number || ' шт');
  
  --обработчик ошибок, точнее покажет ошибки если такие будут
  exception
    when others then
      begin
        dbms_output.put_line('ошибка ' || SQLERRM);
        rollback;
      end;
    
  end;

  --общая процедура выполнения тестового задания
  procedure run_test is
  begin
  
    gen_data(10000);
    gen_event(1000);
  end;

begin
  null;
end test;
/
