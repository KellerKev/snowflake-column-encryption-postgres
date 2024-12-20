SET my.snowflake_connection_url = 'snowflake://SNOWFLAKE_USER:SNOWFLAKE_PASSWORD-OR-TOKEN-OR-KEY:postgres_role:postgres_encrypt_wh:SNOWFLAKE_ACCOUNT@postgresschema/postgresdb';
select snowflake_employee_insert_aes('emp1001','kevin','malone','Smith Str. 6','34534','5455455' );
select snowflake_employee_insert_aes('emp1002','mark','dorsay','Brazlian ave. 7','75434','78443555' );
select snowflake_employee_insert_aes('emp1003','william','stone','Brazlian ave. 155','85434','999993555' );

select snowflake_employee_insert_aes_deterministic('emp1004','jessica','parker','Maple St. 42','65789','123456789');
select snowflake_employee_insert_aes_deterministic('emp1005','oliver','reed','Ocean Blvd. 8','45321','987654321');
select snowflake_employee_insert_aes_deterministic('emp1006','emily','clark','Sunrise Rd. 22','87654','234567890');
select snowflake_employee_insert_aes_deterministic('emp1007','ethan','wright','Lakeview Dr. 13','56432','345678901');


select snowflake_employee_insert_aes_deterministic('emp1008','sophia','johnson','Hilltop Ave. 99','74321','456789012');
select snowflake_employee_insert_aes_deterministic('emp1008','sophia','johnson','Hilltop Ave. 99','74321','456789012');


select * from query_snowflake_employee_plain()   ;


select * from snowflake_employee;


select postalcode from select_aes_snowflake_employee_iv('{"emp_id":"emp1005"}') ;



select sum(cast(postalcode as integer)) from (select * from select_aes_snowflake_employee_iv('{"emp_id":"emp1002"}') union all 
select * from select_aes_snowflake_employee_iv('{"emp_id":"emp1005"}')) ;



select emp_id, lastname, firstname from select_aes_snowflake_employee_iv('{"lastname":"reed","firstname":"oliver"}') ;

select snowflake_employee_update_aes('emp1005','{"firstname":"christian","lastname":"grannysmith"}');


 
select * from snowflake_employee;



select emp_id from snowflake_employee where firstname='oliver';



select emp_id from snowflake_employee where firstname='christian';


select snowflake_employee_update_aes_deterministic('emp1005','{"firstname":"peter","lastname":"petersmith"}');

 
select * from snowflake_employee;



select emp_id from snowflake_employee where firstname='christian';



select emp_id from snowflake_employee where firstname='peter';


select snowflake_employee_aes_delete('emp1005');



 
select * from snowflake_employee;



select emp_id from snowflake_employee where firstname='peter';

