[sql format tool](https://tool.lu/sql/)

- mysql

``` sql
-- desc Trips
SELECT c.Request_at AS Day, CAST(SUM(if(c.Status = 'completed', 0, 1)) / COUNT(1) AS decimal(3, 2)) AS "Cancellation Rate"
FROM (
	SELECT t.Request_at, t.Status
	FROM Trips t
		INNER JOIN Users u1
		ON (t.Client_Id = u1.Users_id
			AND u1.Banned = 'No'
			AND u1.Role != 'partner'
			AND str_to_date(t.Request_at, '%Y-%m-%d') BETWEEN str_to_date('2013-10-1', '%Y-%m-%d') AND str_to_date('2013-10-3', '%Y-%m-%d'))
		INNER JOIN Users u2
		ON (t.Driver_Id = u2.Users_id
			AND u2.Banned = 'No'
			AND u2.Role != 'partner'
			AND str_to_date(t.Request_at, '%Y-%m-%d') BETWEEN str_to_date('2013-10-1', '%Y-%m-%d') AND str_to_date('2013-10-3', '%Y-%m-%d'))
) c
GROUP BY c.Request_at
```