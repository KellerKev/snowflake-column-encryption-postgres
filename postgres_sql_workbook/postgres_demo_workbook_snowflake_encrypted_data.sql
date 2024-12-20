create extension pgcrypto;
--create extension plpython3u;

--SET my.snowflake_connection_url = 'snowflake://SNOWFLAKE_USER:SNOWFLAKE_PASSWORD-OR-TOKEN-OR-KEY:SNOWFLAKE_ROLE:SNOWFLAKE_WAREHOUSE:SNOWFLAKE_ACCOUNT@SNOWFLAKE_SCHEMA/SNOWFLAKE_DATABASE';

SET my.snowflake_connection_url = 'snowflake://SNOWFLAKE_USER:SNOWFLAKE_PASSWORD-OR-TOKEN-OR-KEY:postgres_role:postgres_encrypt_wh:SNOWFLAKE_ACCOUNT@postgresschema/postgresdb';

CREATE OR REPLACE FUNCTION aesencrypt_iv(aesstring text) 
RETURNS table (response text) as
$$

import base64
import random
import string

def encode_base64(message):
	message_bytes = message.encode('ascii')
	base64_bytes = base64.b64encode(message_bytes)
	base64_message = base64_bytes.decode('ascii')
	return base64_message


def get_random_string(length):
    # choose from all lowercase letter
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str
    
results = []
iv=encode_base64('92wwrVOOtcv1SwIV')

if aesstring!=None and aesstring!='':
	query1 = """select encode(encrypt_iv('""" + aesstring + """',decode('qg0q8m+kwmjcIIXkhZF2P1krwi+h/ry3CXJhqiZJT6M=', 'base64'), decode('"""+iv+"""','base64'), 'aes-cbc'),'base64')::varchar"""
	syn_enc_q = plpy.execute(query1)
	#plpy.notice(syn_enc_q.__str__())
	syn_enc=syn_enc_q[0]['encode']
	results.append(syn_enc+iv)
	return results
else:
	results.append('')
	return results

$$ LANGUAGE 'plpython3u';


CREATE OR REPLACE FUNCTION aesencrypt(aesstring text) 
RETURNS table (response text) as
$$

import base64
import random
import string

def encode_base64(message):
	message_bytes = message.encode('ascii')
	base64_bytes = base64.b64encode(message_bytes)
	base64_message = base64_bytes.decode('ascii')
	return base64_message


def get_random_string(length):
    # choose from all lowercase letter
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str
    
results = []
iv=encode_base64(get_random_string(16))
if aesstring!=None and aesstring!='':
	query1 = """select encode(encrypt_iv('""" + aesstring + """',decode('qg0q8m+kwmjcIIXkhZF2P1krwi+h/ry3CXJhqiZJT6M=', 'base64'), decode('"""+iv+"""','base64'), 'aes-cbc'),'base64')::varchar"""
	#plpy.notice(query1)
	syn_enc_q = plpy.execute(query1)
	#plpy.notice(syn_enc_q.__str__())
	syn_enc=syn_enc_q[0]['encode']
	results.append(syn_enc+iv)
	return results
else:
	results.append('')
	return results
	
$$ LANGUAGE 'plpython3u';


CREATE OR REPLACE FUNCTION snowflake_employee_aes_delete(emp_id TEXT, firstname TEXT, lastname TEXT, address TEXT, postalcode TEXT, phone TEXT) 
RETURNS TABLE (response TEXT) AS
$$
from pydal import DAL, Field

try:
	# Retrieve the connection URL from the custom PostgreSQL variable
	connection_url_result = plpy.execute("SHOW my.snowflake_connection_url")
	if not connection_url_result:
		raise Exception("Connection URL not set in my.postgres_connection_url")
	connection_url = connection_url_result[0]['my.snowflake_connection_url']
	
	# Initialize the DAL with the retrieved connection URL
	db = DAL(connection_url, folder='db', fake_migrate=True, migrate=False)

	# Define the employee table
	db.define_table(
		'employee',
		Field('emp_id'),
		Field('firstname'),
		Field('lastname'),
		Field('address'),
		Field('postalcode'),
		Field('phone'),
		primarykey=['lastname']
	)
	
	results = []
	
	# Encrypt the fields
	query1 = """SELECT aesencrypt_iv('{}') AS aesencrypt_iv""".format(emp_id)
	query2 = """SELECT aesencrypt('{}') AS aesencrypt""".format(firstname)
	query3 = """SELECT aesencrypt('{}') AS aesencrypt""".format(lastname)
	query4 = """SELECT aesencrypt('{}') AS aesencrypt""".format(address)
	query5 = """SELECT aesencrypt('{}') AS aesencrypt""".format(postalcode)
	query6 = """SELECT aesencrypt('{}') AS aesencrypt""".format(phone)
	
	emp_id_enc_q = plpy.execute(query1)
	emp_id_enc = emp_id_enc_q[0]['aesencrypt_iv']
	
	firstname_enc_q = plpy.execute(query2)
	firstname_enc = firstname_enc_q[0]['aesencrypt']
	
	lastname_enc_q = plpy.execute(query3)
	lastname_enc = lastname_enc_q[0]['aesencrypt']
	
	address_enc_q = plpy.execute(query4)
	address_enc = address_enc_q[0]['aesencrypt']
	
	postalcode_enc_q = plpy.execute(query5)
	postalcode_enc = postalcode_enc_q[0]['aesencrypt']
	
	phone_enc_q = plpy.execute(query6)
	phone_enc = phone_enc_q[0]['aesencrypt']
	
	# Insert the encrypted employee record
	db.employee.insert(
		emp_id=emp_id_enc,
		firstname=firstname_enc,
		lastname=lastname_enc,
		address=address_enc,
		postalcode=postalcode_enc,
		phone=phone_enc
	)
	
	results.append(["ok"])
	
finally:
	if db:
		db.close()

return results
$$ LANGUAGE plpython3u;



-- Insert  rows into Snowfake encrypted non-deterministically

select snowflake_employee_insert_aes('emp1001','kevin','malone','Smith Str. 6','34534','5455455' );
select snowflake_employee_insert_aes('emp1002','mark','dorsay','Brazlian ave. 7','75434','78443555' );
select snowflake_employee_insert_aes('emp1003','william','stone','Brazlian ave. 155','85434','999993555' );





CREATE OR REPLACE FUNCTION snowflake_employee_insert_aes_deterministic(emp_id text, firstname text, lastname text, address text, postalcode text, phone text) 
RETURNS table (response text) as
$$
from pydal import DAL, Field
try:
	# Retrieve the connection URL from the custom PostgreSQL variable
	connection_url_result = plpy.execute("SHOW my.snowflake_connection_url")
	if not connection_url_result:
		raise Exception("Connection URL not set in my.postgres_connection_url")
	connection_url = connection_url_result[0]['my.snowflake_connection_url']
	db = DAL(connection_url, folder='db', fake_migrate=True, migrate=False)
	db.define_table('employee', Field('emp_id'), Field('firstname'), Field('lastname'),Field('address'),Field('postalcode'),Field('phone'), primarykey = ['lastname'])
	results = []
	
	
	query1 = """select aesencrypt_iv('""" + emp_id + """')"""
	query2=  """select aesencrypt_iv('""" + firstname + """')"""
	query3=  """select aesencrypt_iv('""" + lastname + """')"""
	query4=  """select aesencrypt_iv('""" + address + """')"""
	query5=  """select aesencrypt_iv('""" + postalcode + """')"""
	query6=  """select aesencrypt_iv('""" + phone + """')"""
	
	emp_id_enc_q = plpy.execute(query1)
	#plpy.notice(emp_id_enc_q.__str__())
	emp_id_enc=emp_id_enc_q[0]['aesencrypt_iv']
	
	
	firstname_enc_q = plpy.execute(query2)
	firstname_enc=firstname_enc_q[0]['aesencrypt_iv']
	
	
	lastname_enc_q = plpy.execute(query3)
	lastname_enc=lastname_enc_q[0]['aesencrypt_iv']
	
	
	
	address_enc_q = plpy.execute(query4)
	address_enc=address_enc_q[0]['aesencrypt_iv']
	
	postalcode_enc_q = plpy.execute(query5)
	postalcode_enc=postalcode_enc_q[0]['aesencrypt_iv']
	
	phone_enc_q = plpy.execute(query6)
	phone_enc=phone_enc_q[0]['aesencrypt_iv']

	
	
	db.employee.insert(emp_id = emp_id_enc, firstname = firstname_enc, lastname = lastname_enc, address=address_enc,postalcode=postalcode_enc, phone=phone_enc)
	results.append(["ok"])
finally:
	if db:
		db.close()
return results

$$ LANGUAGE 'plpython3u';


-- Insert rows into Snowfake encrypted determinsitically
select snowflake_employee_insert_aes_deterministic('emp1004','jessica','parker','Maple St. 42','65789','123456789');
select snowflake_employee_insert_aes_deterministic('emp1005','oliver','reed','Ocean Blvd. 8','45321','987654321');
select snowflake_employee_insert_aes_deterministic('emp1006','emily','clark','Sunrise Rd. 22','87654','234567890');
select snowflake_employee_insert_aes_deterministic('emp1007','ethan','wright','Lakeview Dr. 13','56432','345678901');

-- lets add this 2 times so its good to see in Snowflake that the encrypted values look the same
select snowflake_employee_insert_aes_deterministic('emp1008','sophia','johnson','Hilltop Ave. 99','74321','456789012');
select snowflake_employee_insert_aes_deterministic('emp1008','sophia','johnson','Hilltop Ave. 99','74321','456789012');

CREATE OR REPLACE FUNCTION query_snowflake_employee_plain() 
RETURNS TABLE (
	emp_id TEXT, 
	firstname TEXT, 
	lastname TEXT, 
	address TEXT, 
	postalcode TEXT, 
	phone TEXT
) AS
$$
from pydal import DAL, Field

try:
	# Retrieve the connection URL from the custom PostgreSQL variable
	connection_url_result = plpy.execute("SHOW my.snowflake_connection_url")
	if not connection_url_result:
		raise Exception("Connection URL not set in my.snowflake_connection_url")
	connection_url = connection_url_result[0]['my.snowflake_connection_url']
	
	# Initialize the DAL with the retrieved connection URL
	db = DAL(connection_url, folder='db', fake_migrate=True, migrate=False)
	
	# Define the employee table
	db.define_table(
		'employee',
		Field('emp_id'),
		Field('firstname'),
		Field('lastname'),
		Field('address'),  
		Field('postalcode'),
		Field('phone'),  
		primarykey=['lastname']
	)
	
	results = []
	rows = db().select(db.employee.ALL)
	for row in rows:
		results.append([
			row['emp_id'],
			row['firstname'],
			row['lastname'],
			row['address'],
			row['postalcode'],
			row['phone']
		])
finally:
	if db:
		db.close()

return results

$$ LANGUAGE 'plpython3u';


-- Query Snowflake table without on-premise decryption

select * from query_snowflake_employee_plain()   ;


--Let's download data from Snowflake and decrpt within Postgres

CREATE OR REPLACE FUNCTION aesdecrypt(aesstring text) 
RETURNS table (response text) as
$$

    
results = []


#plpy.notice(cipher.__str__())
#plpy.notice(iv.__str__())
if aesstring!=None and aesstring!='':
	iv=aesstring[-24:]
	cipher=aesstring.replace(iv,'')

		
	query1 = """select decrypt_iv(decode('""" + cipher + """','base64'),decode('qg0q8m+kwmjcIIXkhZF2P1krwi+h/ry3CXJhqiZJT6M=', 'base64'), decode('"""+iv+"""','base64'), 'aes-cbc')"""
	#plpy.notice(query1.__str__())
	syn_enc_q = plpy.execute(query1)
	#plpy.notice(syn_enc_q.__str__())
	syn_enc=syn_enc_q[0]['decrypt_iv']
	results.append(syn_enc.decode())
	return results
	
else:
	results.append('')
	return results


$$ LANGUAGE 'plpython3u';

CREATE VIEW snowflake_employee AS
  select aesdecrypt(emp_id) as emp_id , aesdecrypt(firstname) as firstname, aesdecrypt(lastname) as lastname,aesdecrypt(address) as address,aesdecrypt(postalcode) as postalcode, aesdecrypt(phone) as phone from query_snowflake_employee_plain();

 
 -- Data will queried and dowloaded from Snowflaek encrypted and only decrypted within our Postgres
 
select * from snowflake_employee;

-- Let's get a specific row

select emp_id from snowflake_employee where firstname='oliver';


--Let's get the postal code of a specific employee
CREATE OR REPLACE FUNCTION select_aes_snowflake_employee_iv(jsoninput TEXT) 
RETURNS TABLE (
	emp_id TEXT, 
	firstname TEXT, 
	lastname TEXT, 
	address TEXT, 
	postalcode TEXT, 
	phone TEXT
) AS
$$
from pydal import DAL, Field
import json

try:
	# Retrieve the connection URL from the custom PostgreSQL variable
	connection_url_result = plpy.execute("SHOW my.snowflake_connection_url")
	if not connection_url_result:
		raise Exception("Connection URL not set in my.snowflake_connection_url")
	connection_url = connection_url_result[0]['my.snowflake_connection_url']
	
	# Initialize the DAL with the retrieved connection URL
	db = DAL(connection_url, folder='db', fake_migrate=True, migrate=False)
	
	# Define the employee table
	db.define_table(
		'employee',
		Field('emp_id'),
		Field('firstname'),
		Field('lastname'),
		Field('address'),  
		Field('postalcode'),
		Field('phone'),  
		primarykey=['lastname']
	)
	
	results = []
	testdict = json.loads(jsoninput)
	
	select_query = "SELECT * FROM employee WHERE "
	
	# Construct the WHERE clause with encrypted values
	for k, v in testdict.items():
		# Encrypt the value using aesencrypt_iv
		query_enc = f"SELECT aesencrypt_iv('{v}') AS aesencrypt_iv"
		enc_q = plpy.execute(query_enc)
		if not enc_q:
			raise Exception(f"Encryption failed for value: {v}")
		enc = enc_q[0]['aesencrypt_iv']
		
		# Append to the SELECT query
		select_query += f"{k} = '{enc}' AND "
	
	# Remove the trailing ' AND '
	select_query = select_query.rstrip(" AND ")
	
	# Execute the constructed SELECT query
	data = db.executesql(select_query)
	
	# Decrypt the retrieved data
	for row in data:
		dec_empid_query = f"SELECT aesdecrypt('{row[0]}') AS aesdecrypt"
		dec_empid_q = plpy.execute(dec_empid_query)
		dec_empid = dec_empid_q[0]['aesdecrypt']
		
		dec_firstname_query = f"SELECT aesdecrypt('{row[1]}') AS aesdecrypt"
		dec_firstname_q = plpy.execute(dec_firstname_query)
		dec_firstname = dec_firstname_q[0]['aesdecrypt']
		
		dec_lastname_query = f"SELECT aesdecrypt('{row[2]}') AS aesdecrypt"
		dec_lastname_q = plpy.execute(dec_lastname_query)
		dec_lastname = dec_lastname_q[0]['aesdecrypt']
		
		dec_address_query = f"SELECT aesdecrypt('{row[3]}') AS aesdecrypt"
		dec_address_q = plpy.execute(dec_address_query)
		dec_address = dec_address_q[0]['aesdecrypt']
		
		dec_postalcode_query = f"SELECT aesdecrypt('{row[4]}') AS aesdecrypt"
		dec_postalcode_q = plpy.execute(dec_postalcode_query)
		dec_postalcode = dec_postalcode_q[0]['aesdecrypt']
		
		dec_phone_query = f"SELECT aesdecrypt('{row[5]}') AS aesdecrypt"
		dec_phone_q = plpy.execute(dec_phone_query)
		dec_phone = dec_phone_q[0]['aesdecrypt']
		
		results.append([
			dec_empid,
			dec_firstname,
			dec_lastname,
			dec_address,
			dec_postalcode,
			dec_phone
		])
	
finally:
	if db:
		db.close()

return results

$$ LANGUAGE plpython3u;


--Let's get the postal code of a specific employee
select postalcode from select_aes_snowflake_employee_iv('{"emp_id":"emp1005"}') ;

-- You can also do type conversions and aggregates. Let's cast the postalcode field to integer and do sum of those 2 records:

select sum(cast(postalcode as integer)) from (select * from select_aes_snowflake_employee_iv('{"emp_id":"emp1002"}') union all 
select * from select_aes_snowflake_employee_iv('{"emp_id":"emp1005"}')) ;

--Let'specific columns of a specifc user.

select emp_id, lastname, firstname from select_aes_snowflake_employee_iv('{"lastname":"reed","firstname":"oliver"}') ;

-- Let's update an employee in Snowflake


CREATE OR REPLACE FUNCTION snowflake_employee_update_aes(emp_id text, updatestring text) 
RETURNS table (response text) as
$$
from pydal import DAL, Field
import json
try:
	# Retrieve the connection URL from the custom PostgreSQL variable
	connection_url_result = plpy.execute("SHOW my.snowflake_connection_url")
	if not connection_url_result:
		raise Exception("Connection URL not set in my.snowflake_connection_url")
	connection_url = connection_url_result[0]['my.snowflake_connection_url']
	
	# Initialize the DAL with the retrieved connection URL
	db = DAL(connection_url, folder='db', fake_migrate=True, migrate=False)
	db.define_table('employee', Field('emp_id'), Field('firstname'), Field('lastname'),Field('address'),Field('postalcode'),Field('phone'), primarykey = ['lastname'])
	results = []
	query1 = """select aesencrypt_iv('""" + emp_id + """')"""
	emp_id_enc_q = plpy.execute(query1)
	emp_id_enc=emp_id_enc_q[0]['aesencrypt_iv']
	enc_dict={}
	testdict=json.loads(updatestring)
	for k, v in testdict.items():
		query = """select aesencrypt('""" + v + """')"""
		enc_v_q = plpy.execute(query)
		enc_v=enc_v_q [0]['aesencrypt']    
		enc_dict[k]=str(enc_v)
		
	db(db.employee.emp_id==emp_id_enc).update(**enc_dict)
	results.append(["updated"])
finally:
	if db:
		db.close()
return results
$$ LANGUAGE 'plpython3u';

-- Let's update an employee in Snowflake

select snowflake_employee_update_aes('emp1005','{"firstname":"christian","lastname":"grannysmith"}');

-- Let's check results
 
select * from snowflake_employee;

-- Let's get a specific row -- oliver is gone

select emp_id from snowflake_employee where firstname='oliver';


-- Let's get a specific row --  christian has the employee id emp1005 now

select emp_id from snowflake_employee where firstname='christian';

-- We can also updat the employee and switch to deterministic encryption if we want

CREATE OR REPLACE FUNCTION snowflake_employee_update_aes_deterministic(emp_id text, updatestring text) 
RETURNS table (response text) as
$$
from pydal import DAL, Field
import json
try:
	# Retrieve the connection URL from the custom PostgreSQL variable
	connection_url_result = plpy.execute("SHOW my.snowflake_connection_url")
	if not connection_url_result:
		raise Exception("Connection URL not set in my.snowflake_connection_url")
	connection_url = connection_url_result[0]['my.snowflake_connection_url']
	
	# Initialize the DAL with the retrieved connection URL
	db = DAL(connection_url, folder='db', fake_migrate=True, migrate=False)
	db.define_table('employee', Field('emp_id'), Field('firstname'), Field('lastname'),Field('address'),Field('postalcode'),Field('phone'), primarykey = ['lastname'])
	results = []
	query1 = """select aesencrypt_iv('""" + emp_id + """')"""
	emp_id_enc_q = plpy.execute(query1)
	emp_id_enc=emp_id_enc_q[0]['aesencrypt_iv']
	enc_dict={}
	testdict=json.loads(updatestring)
	for k, v in testdict.items():
		query = """select aesencrypt_iv('""" + v + """')"""
		enc_v_q = plpy.execute(query)
		enc_v=enc_v_q [0]['aesencrypt_iv']    
		enc_dict[k]=str(enc_v)
		
	db(db.employee.emp_id==emp_id_enc).update(**enc_dict)
	results.append(["updated"])
finally:
	if db:
		db.close()
return results
$$ LANGUAGE 'plpython3u';


select snowflake_employee_update_aes_deterministic('emp1005','{"firstname":"peter","lastname":"petersmith"}');

-- Let's check results
 
select * from snowflake_employee;

-- Let's get a specific row -- christian is gone

select emp_id from snowflake_employee where firstname='christian';


-- Let's get a specific row --  peter has the employee id emp1005 now

select emp_id from snowflake_employee where firstname='peter';


-- Let's delete a specific employee now


CREATE OR REPLACE FUNCTION snowflake_employee_aes_delete(emp_id text) 
RETURNS table (response text) as
$$
from pydal import DAL, Field
import json
try:
	# Retrieve the connection URL from the custom PostgreSQL variable
	connection_url_result = plpy.execute("SHOW my.snowflake_connection_url")
	if not connection_url_result:
		raise Exception("Connection URL not set in my.snowflake_connection_url")
	connection_url = connection_url_result[0]['my.snowflake_connection_url']
	
	# Initialize the DAL with the retrieved connection URL
	db = DAL(connection_url, folder='db', fake_migrate=True, migrate=False)
	db.define_table('employee', Field('emp_id'), Field('firstname'), Field('lastname'),Field('address'),Field('postalcode'),Field('phone'), primarykey = ['lastname'])
	results = []
	query1 = """select aesencrypt_iv('""" + emp_id + """')"""
	emp_id_enc_q = plpy.execute(query1)
	emp_id_enc=emp_id_enc_q[0]['aesencrypt_iv']
	enc_dict={}

		
	db(db.employee.emp_id==emp_id_enc).delete()
	results.append(["deleted"])
finally:
	if db:
		db.close()
return results
$$ LANGUAGE 'plpython3u';


select snowflake_employee_aes_delete('emp1005');


-- Let's check results -- peter is gone
 
select * from snowflake_employee;

-- Let's get a specific row -- peter is gone

select emp_id from snowflake_employee where firstname='peter';


-- Want to have bulk inserts e.g. from CSV files? Let'us Postgres trigger function: 


-- We create a local dummy table employees

CREATE TABLE employee (emp_id text, firstname text,lastname text, address text, postalcode text, phone text);

-- Set a trigger to insert into Snowflake instead of local table. In local tabke we just insert OK if Snowflake insert was good.
-- Dummy table can be dropped afterwards

CREATE OR REPLACE FUNCTION encrypt_values_snow() RETURNS trigger AS $$
BEGIN
IF tg_op = 'INSERT' then

NEW.firstname = snowflake_employee_insert_aes(NEW.emp_id, NEW.firstname,NEW.lastname, NEW.address, NEW.postalcode, NEW.phone); 
NEW.emp_id = 'OK'; 
NEW.lastname= 'OK';
NEW.address='OK';
NEW.postalcode='OK';
NEW.phone='OK';
RETURN NEW;
END IF;
END;
$$ LANGUAGE plpgsql;

create or Replace TRIGGER encrypt_values_insert_snow
BEFORE INSERT  ON employee
FOR EACH ROW EXECUTE PROCEDURE encrypt_values_snow();

-- Let's test
insert into employee values ('emp0020','andy','meyersmith','holunderweg 8','25545','5454154455');

select * from employee;

drop table employee;

-- Should only show OK values. Real values are inserted into Snowflake encrypted. 
-- You can not import a CSV or other files in bulk in the table and they will be bulk uploaded into Snowflake encrypted. 

-- This is a insert by insert implementation, so really slow and expensive on the Snowflake side. 
-- We can populate the local temporary table with the encrypted values instead in Postgres which is fast and does not incure cloud processing costs per se. 
-- Afterwards you export from Postgres the CSV file with encrypted value into Snowflake a stage. 

CREATE TABLE employee (emp_id text, firstname text,lastname text, address text, postalcode text, phone text);

-- We set a local trigger. If you like you could also set an UPDATE trigger optonally here as well. 

CREATE OR REPLACE FUNCTION encrypt_values() RETURNS trigger AS $$
BEGIN
IF tg_op = 'INSERT'  THEN
NEW.emp_id = aesencrypt_iv(NEW.emp_id) ;
NEW.firstname =aesencrypt_iv(NEW.firstname) ;
NEW.lastname =aesencrypt_iv(NEW.lastname) ;
NEW.address =aesencrypt_iv(NEW.address) ;
NEW.postalcode =aesencrypt_iv(NEW.postalcode) ;
NEW.phone =aesencrypt_iv(NEW.phone) ;
RETURN NEW;
END IF;
END;
$$ LANGUAGE plpgsql;

create or Replace TRIGGER encrypt_values_insert
BEFORE INSERT ON employee
FOR EACH ROW EXECUTE PROCEDURE encrypt_values();

insert into employee values ('emp0021','sarah','wagenknecht','schlesiengweg 18','65643','87455455');

-- Check result
select * from employee;

-- Now you can also import a CSV or bulk import from yoru favorite ETL tool, which does not need to worry about encryption.
-- Let Postgres do the encryption and when done export as CSV, upload to Snowflake stage and import the data into the table encrypted

-- Once done you can drop the table again
drop table employee;
