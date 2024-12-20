use role accountadmin;

CREATE OR REPLACE WAREHOUSE postgres_encrypt_wh WITH WAREHOUSE_SIZE='X-SMALL';

create role postgres_role;

grant usage on WAREHOUSE postgres_encrypt_wh  to role postgres_role;
grant operate on  WAREHOUSE postgres_encrypt_wh to role postgres_role;


GRANT CREATE TAG ON SCHEMA POSTGRESDB.POSTGRESSCHEMA TO ROLE postgres_role;
GRANT APPLY TAG ON ACCOUNT TO ROLE postgres_role;

GRANT APPLY MASKING POLICY ON ACCOUNT TO ROLE postgres_role;


grant create database on account to role postgres_role;


grant role postgres_role to user xxxx;

use role postgres_role;


create database postgresdb;
create schema postgresschema;


CREATE or replace TABLE employee (emp_id varchar, firstname varchar,lastname varchar, address varchar, postalcode varchar, phone varchar);


select * from employee;

--Let-s decrypt here:


CREATE OR REPLACE FUNCTION decrypt_cbc(cipher VARCHAR, key VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
HEX_DECODE_STRING(
    DECRYPT_RAW(
        TO_BINARY(REPLACE(cipher, SUBSTR(cipher, -24), ''), 'BASE64'),
        BASE64_DECODE_BINARY(key),
        TO_BINARY(SUBSTR(cipher, -24), 'BASE64'),
        NULL,
        'AES-CBC'
    )::STRING
)
$$;




set aes_cbc_key ='qg0q8m+kwmjcIIXkhZF2P1krwi+h/ry3CXJhqiZJT6M=';

--set iv_var='92wwrVOOtcv1SwIV';

create or replace masking policy decrypt_pg
   as (val string) returns string ->
   case

      when is_role_in_session('postgres_role')
           then decrypt_cbc(val,$aes_cbc_key)
      else '** masked **'
   end;



ALTER TABLE IF EXISTS employee MODIFY COLUMN emp_id SET MASKING POLICY decrypt_pg;

select * from employee;

ALTER TABLE IF EXISTS employee MODIFY COLUMN emp_id UNSET MASKING POLICY  ;



create tag DECRYPTME;

alter table employee set tag
  DECRYPTME = 'YUP';

  alter tag DECRYPTME set masking policy decrypt_pg;
  alter tag DECRYPTME unset masking policy decrypt_pg;






--- ENCRYPTION IN SNOWFLAKE

CREATE or replace TABLE employee_plain (emp_id varchar, firstname varchar,lastname varchar, address varchar, postalcode varchar, phone varchar);


INSERT INTO employee_plain VALUES ('emp1007', 'John', 'Smith', '18 Maple St, Chicago', '77001', '+1-555-234-5678');
INSERT INTO employee_plain VALUES ('emp1008', 'Bob', 'Brown', '702 Maple St, Los Angeles', '60601', '+1-555-456-7890');
INSERT INTO employee_plain VALUES ('emp1009', 'Alice', 'Johnson', '161 Pine St, New York', '90001', '+1-555-345-6789');
INSERT INTO employee_plain VALUES ('emp1010', 'Michael', 'Jones', '818 Main St, Houston', '10001', '+1-555-234-5678');
INSERT INTO employee_plain VALUES ('emp1011', 'Sarah', 'Taylor', '510 Elm St, Phoenix', '85001', '+1-555-123-4567');


create tag ENCRYPTME;

alter table employee_plain set tag
 ENCRYPTME = 'YUP';


 set aes_cbc_key ='qg0q8m+kwmjcIIXkhZF2P1krwi+h/ry3CXJhqiZJT6M=';
--set iv_var='92wwrVOOtcv1SwIV';

CREATE OR REPLACE MASKING POLICY encrypt_pg
AS (val STRING) RETURNS STRING ->
CASE
    WHEN IS_ROLE_IN_SESSION('postgres_role') THEN BASE64_ENCODE(
        TO_BINARY(
            (
                SELECT CONCAT(
                    PARSE_JSON(ENCRYPT_RAW(
                        TO_BINARY(val, 'UTF-8'),
                        BASE64_DECODE_BINARY($aes_cbc_key),
                        TO_BINARY(LEFT(SHA2(val, 256), 32), 'HEX'),
                        NULL,
                        'AES-CBC'
                    )):ciphertext::STRING,
                    PARSE_JSON(ENCRYPT_RAW(
                        TO_BINARY(val, 'UTF-8'),
                        BASE64_DECODE_BINARY($aes_cbc_key),
                        TO_BINARY(LEFT(SHA2(val, 256), 32), 'HEX'),
                        NULL,
                        'AES-CBC'
                    )):iv::STRING
                )
            ),
            'HEX'
        )
    )
    ELSE '** masked **'
END;


  alter tag ENCRYPTME set masking policy encrypt_pg;



select * from employee_plain;

 -- alter tag ENCRYPTME unset masking policy encrypt_pg;


select 
BASE64_ENCODE(
TO_BINARY(
            (
                SELECT CONCAT(
                    PARSE_JSON(ENCRYPT_RAW(
                        TO_BINARY('Sarah', 'UTF-8'),
                        BASE64_DECODE_BINARY($aes_cbc_key),
                        TO_BINARY(LEFT(SHA2('Sarah', 256), 32), 'HEX'),
                        NULL,
                        'AES-CBC'
                    )):ciphertext::STRING,
                    PARSE_JSON(ENCRYPT_RAW(
                        TO_BINARY('Sarah', 'UTF-8'),
                        BASE64_DECODE_BINARY($aes_cbc_key),
                        TO_BINARY(LEFT(SHA2('Sarah', 256), 32), 'HEX'),
                        NULL,
                        'AES-CBC'
                    )):iv::STRING
                )
            ),
            'HEX'
        ))
    ;




