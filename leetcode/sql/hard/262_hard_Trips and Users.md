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



The `Trips` table holds all taxi trips. Each trip has a unique Id, while Client_Id and Driver_Id are both foreign keys to the Users_Id at the `Users` table. Status is an ENUM type of (‘completed’, ‘cancelled_by_driver’, ‘cancelled_by_client’).

```
+----+-----------+-----------+---------+--------------------+----------+
| Id | Client_Id | Driver_Id | City_Id |        Status      |Request_at|
+----+-----------+-----------+---------+--------------------+----------+
| 1  |     1     |    10     |    1    |     completed      |2013-10-01|
| 2  |     2     |    11     |    1    | cancelled_by_driver|2013-10-01|
| 3  |     3     |    12     |    6    |     completed      |2013-10-01|
| 4  |     4     |    13     |    6    | cancelled_by_client|2013-10-01|
| 5  |     1     |    10     |    1    |     completed      |2013-10-02|
| 6  |     2     |    11     |    6    |     completed      |2013-10-02|
| 7  |     3     |    12     |    6    |     completed      |2013-10-02|
| 8  |     2     |    12     |    12   |     completed      |2013-10-03|
| 9  |     3     |    10     |    12   |     completed      |2013-10-03| 
| 10 |     4     |    13     |    12   | cancelled_by_driver|2013-10-03|
+----+-----------+-----------+---------+--------------------+----------+
```

The `Users` table holds all users. Each user has an unique Users_Id, and Role is an ENUM type of (‘client’, ‘driver’, ‘partner’).

```
+----------+--------+--------+
| Users_Id | Banned |  Role  |
+----------+--------+--------+
|    1     |   No   | client |
|    2     |   Yes  | client |
|    3     |   No   | client |
|    4     |   No   | client |
|    10    |   No   | driver |
|    11    |   No   | driver |
|    12    |   No   | driver |
|    13    |   No   | driver |
+----------+--------+--------+
```

Write a SQL query to find the cancellation rate of requests made by unbanned users (both client and driver must be unbanned) between **Oct 1, 2013** and **Oct 3, 2013**. The cancellation rate is computed by dividing the number of canceled (by client or driver) requests made by unbanned users by the total number of requests made by unbanned users.

For the above tables, your SQL query should return the following rows with the cancellation rate being rounded to *two* decimal places.

```
+------------+-------------------+
|     Day    | Cancellation Rate |
+------------+-------------------+
| 2013-10-01 |       0.33        |
| 2013-10-02 |       0.00        |
| 2013-10-03 |       0.50        |
+------------+-------------------+
```