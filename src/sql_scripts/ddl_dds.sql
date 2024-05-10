-- DDS --
DROP TABLE IF EXISTS stv2024031256__DWH.operations_rej;
DROP TABLE IF EXISTS stv2024031256__DWH.currencies_rej;
DROP TABLE IF EXISTS stv2024031256__DWH.operations CASCADE;
DROP TABLE IF EXISTS stv2024031256__DWH.currencies CASCADE;

CREATE TABLE stv2024031256__DWH.operations
(
    operation_id uuid NOT NULL,
    account_number_from int NOT NULL,
    account_number_to int NOT NULL,
    currency_code smallint NOT NULL,
    country varchar(30) NOT NULL,
    status varchar(30) NOT NULL,
    transaction_type varchar(30) NOT NULL,
    amount int NOT NULL,
    transaction_dt timestamp(3) NOT NULL,
    CONSTRAINT pk PRIMARY KEY (operation_id, transaction_dt) ENABLED

)
ORDER BY transaction_dt
SEGMENTED BY hash(operation_id, transaction_dt) all nodes
PARTITION BY transaction_dt::date
GROUP BY calendar_hierarchy_day(transaction_dt::date, 3, 2);


CREATE PROJECTION stv2024031256__DWH.operations_b2
(
 operation_id,
 account_number_from,
 account_number_to,
 currency_code,
 country,
 status,
 transaction_type,
 amount,
 transaction_dt
)
AS
 SELECT operations.operation_id,
        operations.account_number_from,
        operations.account_number_to,
        operations.currency_code,
        operations.country,
        operations.status,
        operations.transaction_type,
        operations.amount,
        operations.transaction_dt
 FROM stv2024031256__DWH.operations
 ORDER BY operations.transaction_dt
SEGMENTED BY hash(operations.operation_id, operations.transaction_dt) ALL NODES KSAFE 1;


CREATE TABLE stv2024031256__DWH.currencies 
(
    id int, 
    date_update timestamp(0) NOT NULL,
    currency_code smallint NOT NULL,
    currency_code_with smallint NOT NULL,
    currency_with_div numeric(5, 3) NOT NULL,
    CONSTRAINT pk PRIMARY KEY (currency_code, currency_code_with, date_update) ENABLED
)
ORDER BY id
SEGMENTED BY HASH(id) ALL NODES
PARTITION BY date_update::date
GROUP BY calendar_hierarchy_day(date_update::date, 3, 2);

CREATE PROJECTION stv2024031256__DWH.currencies_b2 (
	id,
	date_update,
	currency_code,
	currency_code_with,
	currency_with_div
	)
AS
 SELECT currencies.id,
		currencies.date_update,
		currencies.currency_code,
		currencies.currency_code_with,
		currencies.currency_with_div
 FROM stv2024031256__DWH.currencies
 ORDER BY currencies.id
SEGMENTED BY hash(currencies.id) ALL NODES KSAFE 1;