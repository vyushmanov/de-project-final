

table_list = [{'table': 'currencies', 'dt_condition_column': 'date_update'}
              ,{'table': 'transactions', 'dt_condition_column': 'transaction_dt'}]


for t in table_list:

    print(t['table'])