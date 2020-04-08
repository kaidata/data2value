```sql
SELECT DISTINCT t.Num AS ConsecutiveNums
FROM (
	SELECT Num, lag(Num, 1, NULL) OVER (ORDER BY Id) AS pre
		, lead(Num, 1, NULL) OVER (ORDER BY Id) AS aft
	FROM Logs
) t
WHERE t.Num = t.pre
	AND t.Num = t.aft
```

