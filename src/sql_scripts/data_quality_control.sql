SELECT
	COUNT(1) row_count
	,COUNT(distinct date_update) days
FROM public.currencies


SELECT
	COUNT(1) row_count
	,SUM(CASE WHEN account_number_from < 0 THEN 1 ELSE 0 END) test_count
	,100.0 * SUM(CASE WHEN account_number_from < 0 THEN 1 ELSE 0 END) / COUNT(1) share_
FROM public.transactions


SELECT transaction_type
	,COUNT(1)
FROM public.transactions
GROUP BY transaction_type
ORDER BY 2 desc