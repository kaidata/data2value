```sql
SELECT CASE 
		WHEN id = (
			SELECT MAX(id)
			FROM seat
		)
		AND mod(id, 2) = 1 THEN id
		WHEN id < (
			SELECT MAX(id)
			FROM seat
		)
		AND mod(id, 2) = 1 THEN id + 1
		WHEN mod(id, 2) = 0 THEN id - 1
	END AS id, student
FROM seat
ORDER BY id
```

