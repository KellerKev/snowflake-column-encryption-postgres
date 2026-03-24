-- =============================================================================
-- AES-CBC Encryption & Decryption Demo for Snowflake
-- =============================================================================
-- This worksheet demonstrates:
-- 1. Role & warehouse setup for the demo
-- 2. Generating fake employee data with Python Faker
-- 3. Encrypting data using AES-CBC with ENCRYPT_RAW (random IV per row)
-- 4. Encrypting data using AES-CBC with a fixed IV (cross-system testing)
-- 5. Decrypting data using DECRYPT_RAW
-- 6. Splitting ciphertext into IV and payload components
-- 7. Applying encryption/decryption via masking policies & tags
-- 8. Staging and loading encrypted data
-- 9. Postgres pgcrypto-compatible format: BASE64(iv_raw || ciphertext_raw)
-- =============================================================================

-- =============================================================================
-- STEP 1: Role, Warehouse & Schema Setup
-- =============================================================================

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE WAREHOUSE postgres_encrypt_wh WITH WAREHOUSE_SIZE='X-SMALL';

CREATE ROLE IF NOT EXISTS postgres_role;

GRANT USAGE ON WAREHOUSE postgres_encrypt_wh TO ROLE postgres_role;
GRANT OPERATE ON WAREHOUSE postgres_encrypt_wh TO ROLE postgres_role;

GRANT CREATE TAG ON SCHEMA POSTGRESDB.POSTGRESSCHEMA TO ROLE postgres_role;
GRANT APPLY TAG ON ACCOUNT TO ROLE postgres_role;
GRANT APPLY MASKING POLICY ON ACCOUNT TO ROLE postgres_role;

GRANT CREATE DATABASE ON ACCOUNT TO ROLE postgres_role;

-- Replace 'xxxx' with your Snowflake username
GRANT ROLE postgres_role TO USER xxxx;

USE ROLE postgres_role;

CREATE DATABASE IF NOT EXISTS postgresdb;
CREATE SCHEMA IF NOT EXISTS postgresschema;
USE SCHEMA postgresdb.postgresschema;

-- =============================================================================
-- STEP 2: Session Configuration
-- =============================================================================

SET aes_cbc_key = 'qg0q8m+kwmjcIIXkhZF2P1krwi+h/ry3CXJhqiZJT6M=';

-- =============================================================================
-- STEP 3: Create Tables
-- =============================================================================

CREATE OR REPLACE TABLE employee (
    emp_id     VARCHAR,
    firstname  VARCHAR,
    lastname   VARCHAR,
    address    VARCHAR,
    postalcode VARCHAR,
    phone      VARCHAR
);

CREATE OR REPLACE TABLE employee_fake2 (
    emp_id     VARCHAR,
    firstname  VARCHAR,
    lastname   VARCHAR,
    address    VARCHAR,
    postalcode VARCHAR,
    phone      VARCHAR
);

-- =============================================================================
-- STEP 4: Fake Data Generator (Python UDF using Faker)
-- =============================================================================

CREATE OR REPLACE FUNCTION FAKE(locale VARCHAR, provider VARCHAR, parameters VARIANT)
RETURNS VARIANT
LANGUAGE PYTHON
VOLATILE
RUNTIME_VERSION = '3.11'
PACKAGES = ('faker', 'simplejson')
HANDLER = 'fake'
AS $$
import simplejson as json
from faker import Faker

def fake(locale, provider, parameters):
    if type(parameters).__name__ == 'sqlNullWrapper':
        parameters = {}
    fake = Faker(locale=locale)
    return json.loads(json.dumps(fake.format(formatter=provider, **parameters), default=str))
$$;

CREATE OR REPLACE VIEW fake_data AS
SELECT
    FAKE('en_US', 'ean', {'length': 8})::VARCHAR         AS emp_id,
    FAKE('en_US', 'first_name', NULL)::VARCHAR            AS firstname,
    FAKE('en_US', 'last_name', NULL)::VARCHAR             AS lastname,
    FAKE('en_US', 'street_address', NULL)::VARCHAR        AS address,
    FAKE('en_US', 'postalcode', NULL)::VARCHAR            AS postalcode,
    FAKE('en_US', 'phone_number', NULL)::VARCHAR          AS phone
FROM TABLE(GENERATOR(ROWCOUNT => 200));

-- Preview fake data
SELECT * FROM fake_data LIMIT 10;

-- Load fake data into table
INSERT INTO employee_fake2 SELECT * FROM fake_data;
SELECT * FROM employee_fake2 LIMIT 5;

-- =============================================================================
-- STEP 5: Encryption Function – Random IV (Recommended for Production)
-- =============================================================================
-- Encrypts plaintext with a random IV each call.
-- Output format: BASE64(iv_raw_16_bytes || ciphertext_raw)
-- Compatible with Postgres pgcrypto format.
-- =============================================================================

CREATE OR REPLACE FUNCTION encrypt_cbc_random_iv(inputtext VARCHAR, key VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS $$
    SELECT BASE64_ENCODE(
        AS_BINARY(GET(enc, 'iv')) || AS_BINARY(GET(enc, 'ciphertext'))
    )
    FROM (
        SELECT ENCRYPT_RAW(
            TO_BINARY(BASE64_ENCODE(inputtext), 'BASE64'),
            BASE64_DECODE_BINARY(key),
            NULL, NULL,
            'AES-CBC'
        ) AS enc
    )
$$;

-- Test: each call produces a unique ciphertext (random IV)
SELECT encrypt_cbc_random_iv('test', $aes_cbc_key);
SELECT encrypt_cbc_random_iv('test', $aes_cbc_key);

-- =============================================================================
-- STEP 6: Encryption Function – Fixed IV (For Testing / Cross-System Compat)
-- =============================================================================
-- Use encrypt_cbc_iv when you need a deterministic IV (e.g., for testing or
-- cross-system compatibility with a known IV). For production, prefer
-- encrypt_cbc_random_iv above.
-- =============================================================================

SET iv_var = '92wwrVOOtcv1SwIV';

CREATE OR REPLACE FUNCTION encrypt_cbc_iv(inputtext VARCHAR, key VARCHAR, iv_in VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS $$
    BASE64_ENCODE(
        TO_BINARY(BASE64_ENCODE(iv_in), 'BASE64')
        || AS_BINARY(GET(
            ENCRYPT_RAW(
                TO_BINARY(BASE64_ENCODE(inputtext), 'BASE64'),
                BASE64_DECODE_BINARY(key),
                TO_BINARY(BASE64_ENCODE(iv_in), 'BASE64'),
                NULL,
                'AES-CBC'
            ),
            'ciphertext'
        ))
    )
$$;

-- Test fixed IV encryption (same input = same output)
SELECT encrypt_cbc_iv('test', $aes_cbc_key, $iv_var);
SELECT encrypt_cbc_iv('+1-860-881-7959x65550', $aes_cbc_key, $iv_var);

-- =============================================================================
-- STEP 7: Decryption Function
-- =============================================================================
-- The encrypted string is: BASE64(iv_raw_16_bytes || ciphertext_raw)
-- First 16 bytes of decoded binary = IV, remainder = ciphertext.
-- Uses TO_VARCHAR(..., 'UTF-8') so it works regardless of BINARY_OUTPUT_FORMAT.
-- =============================================================================

CREATE OR REPLACE FUNCTION decrypt_cbc(cipher VARCHAR, key VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS $$
    TO_VARCHAR(
        DECRYPT_RAW(
            TO_BINARY(SUBSTR(HEX_ENCODE(TO_BINARY(cipher, 'BASE64')), 33), 'HEX'),
            BASE64_DECODE_BINARY(key),
            TO_BINARY(LEFT(HEX_ENCODE(TO_BINARY(cipher, 'BASE64')), 32), 'HEX'),
            NULL,
            'AES-CBC'
        ),
        'UTF-8'
    )
$$;

-- Test round-trip with random IV
SELECT decrypt_cbc(encrypt_cbc_random_iv('test', $aes_cbc_key), $aes_cbc_key);
SELECT decrypt_cbc(encrypt_cbc_random_iv('Bonjour', $aes_cbc_key), $aes_cbc_key);

-- Test round-trip with fixed IV
SELECT decrypt_cbc(encrypt_cbc_iv('test', $aes_cbc_key, $iv_var), $aes_cbc_key);
SELECT decrypt_cbc(encrypt_cbc_iv('Bonjour', $aes_cbc_key, $iv_var), $aes_cbc_key);

-- Pre-computed Postgres-format ciphertext examples (fixed IV):
-- 'test'    => 'OTJ3d3JWT090Y3YxU3dJVvzaiodnHealhiJFBV6kodw='
-- 'Bonjour' => 'OTJ3d3JWT090Y3YxU3dJVhOgnulA8jRpNPbWT/d29pE='
SELECT decrypt_cbc('OTJ3d3JWT090Y3YxU3dJVvzaiodnHealhiJFBV6kodw=', $aes_cbc_key);
SELECT decrypt_cbc('OTJ3d3JWT090Y3YxU3dJVhOgnulA8jRpNPbWT/d29pE=', $aes_cbc_key);

-- =============================================================================
-- STEP 8: Helper – Split Cipher into Components (Table Function)
-- =============================================================================

CREATE OR REPLACE FUNCTION return_cipher_iv(cipher VARCHAR)
RETURNS TABLE(ciphertext VARCHAR, iv VARCHAR)
AS $$
    SELECT
        BASE64_ENCODE(TO_BINARY(SUBSTR(h, 33), 'HEX'))  AS ciphertext,
        BASE64_ENCODE(TO_BINARY(LEFT(h, 32), 'HEX'))    AS iv
    FROM (SELECT HEX_ENCODE(TO_BINARY(cipher, 'BASE64')) AS h)
$$;

-- Test the helper
SELECT * FROM TABLE(return_cipher_iv(encrypt_cbc_random_iv('test', $aes_cbc_key)));

-- =============================================================================
-- STEP 9: Masking Policy – Encrypt on Read (Tag-Based)
-- =============================================================================
-- When postgres_role queries the data, it is encrypted via random IV.
-- Other roles see '** masked **'.
-- =============================================================================

CREATE OR REPLACE MASKING POLICY encrypt_pg
AS (val STRING) RETURNS STRING ->
    CASE
        WHEN IS_ROLE_IN_SESSION('postgres_role')
            THEN encrypt_cbc_random_iv(val, 'qg0q8m+kwmjcIIXkhZF2P1krwi+h/ry3CXJhqiZJT6M=')
        ELSE '** masked **'
    END;

-- Apply via tag
CREATE OR REPLACE TAG ENCRYPTME2;
ALTER TAG ENCRYPTME2 SET MASKING POLICY encrypt_pg;
ALTER TABLE employee_fake2 SET TAG ENCRYPTME2 = 'YUP';

-- Verify – query should return encrypted values (or masked)
SELECT emp_id FROM employee_fake2 LIMIT 1;

-- Cleanup: remove policy from tag
ALTER TAG ENCRYPTME2 UNSET MASKING POLICY encrypt_pg;

-- =============================================================================
-- STEP 10: Masking Policy – Encrypt Column Directly
-- =============================================================================

ALTER TABLE employee_fake2 MODIFY COLUMN firstname SET MASKING POLICY encrypt_pg;
SELECT firstname FROM employee_fake2 LIMIT 5;

-- Remove policy
ALTER TABLE employee_fake2 MODIFY COLUMN firstname UNSET MASKING POLICY;

-- =============================================================================
-- STEP 11: Stage & Export Encrypted Data
-- =============================================================================

CREATE OR REPLACE FILE FORMAT my_csv_format
    TYPE = CSV
    FIELD_DELIMITER = ','
    NULL_IF = ('NULL', 'null')
    SKIP_HEADER = 1
    EMPTY_FIELD_AS_NULL = TRUE
    COMPRESSION = GZIP;

CREATE OR REPLACE STAGE my_unload_stage FILE_FORMAT = my_csv_format;

-- Export encrypted data to stage
COPY INTO @my_unload_stage FROM employee_fake2 HEADER = TRUE;
LIST @my_unload_stage;

-- Reload into employee table
TRUNCATE employee;
COPY INTO employee FROM @my_unload_stage/employee_fake2.csv.gz;
SELECT * FROM employee LIMIT 10;

-- =============================================================================
-- STEP 12: Decryption View
-- =============================================================================
-- Creates a view that transparently decrypts all columns using the session key.
-- =============================================================================

CREATE OR REPLACE VIEW employee_decrypt AS
SELECT
    decrypt_cbc(emp_id,     $aes_cbc_key) AS emp_id,
    decrypt_cbc(firstname,  $aes_cbc_key) AS firstname,
    decrypt_cbc(lastname,   $aes_cbc_key) AS lastname,
    decrypt_cbc(address,    $aes_cbc_key) AS address,
    decrypt_cbc(postalcode, $aes_cbc_key) AS postalcode,
    decrypt_cbc(phone,      $aes_cbc_key) AS phone
FROM employee;

SELECT * FROM employee_decrypt LIMIT 10;

-- =============================================================================
-- STEP 13: Decryption Masking Policy (Decrypt on Read)
-- =============================================================================
-- An alternative approach: store data encrypted, and use a masking policy
-- to decrypt transparently for authorized roles.
-- =============================================================================

CREATE OR REPLACE MASKING POLICY decrypt_pg
AS (val STRING) RETURNS STRING ->
    CASE
        WHEN IS_ROLE_IN_SESSION('postgres_role')
            THEN decrypt_cbc(val, 'qg0q8m+kwmjcIIXkhZF2P1krwi+h/ry3CXJhqiZJT6M=')
        ELSE '** masked **'
    END;

-- Apply decrypt policy to the employee table
ALTER TABLE employee MODIFY COLUMN emp_id SET MASKING POLICY decrypt_pg;
SELECT * FROM employee LIMIT 5;

-- Remove policy
ALTER TABLE employee MODIFY COLUMN emp_id UNSET MASKING POLICY;

-- =============================================================================
-- STEP 14: Quick Verification Queries
-- =============================================================================

SELECT COUNT(*) FROM employee_fake2;
SELECT COUNT(*) FROM employee;

-- =============================================================================
-- STEP 15: Cleanup (Optional)
-- =============================================================================

-- DROP TABLE employee;
-- DROP TABLE employee_fake2;
-- DROP VIEW employee_decrypt;
-- DROP VIEW fake_data;
-- DROP FUNCTION encrypt_cbc_random_iv(VARCHAR, VARCHAR);
-- DROP FUNCTION encrypt_cbc_iv(VARCHAR, VARCHAR, VARCHAR);
-- DROP FUNCTION decrypt_cbc(VARCHAR, VARCHAR);
-- DROP FUNCTION return_cipher_iv(VARCHAR);
-- DROP FUNCTION FAKE(VARCHAR, VARCHAR, VARIANT);
-- DROP MASKING POLICY encrypt_pg;
-- DROP MASKING POLICY decrypt_pg;
-- DROP TAG ENCRYPTME2;
-- DROP STAGE my_unload_stage;
-- DROP FILE FORMAT my_csv_format;
-- DROP WAREHOUSE postgres_encrypt_wh;
-- DROP ROLE postgres_role;
