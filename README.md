# snowflake-column-encryption-postgres
This repo hosts the SQL workbooks and the code you need to setup a Postgres 17 database as your Snowflake data proxy in order to transparently encrypt and decrypt data on the column level with our own on-premise AES key. Data in Snowflake will alway be encrypted on the column level and even during queries.
