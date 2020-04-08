```sql
SELECT a.name AS Department, a.employee AS Employee, a.salary AS Salary
FROM (
	SELECT e.Name AS employee, e.Salary AS salary, d.Name AS name, dense_rank() OVER (PARTITION BY DepartmentId ORDER BY Salary DESC) AS rank
	FROM Employee e
		INNER JOIN Department d ON e.DepartmentId = d.Id
) a
WHERE a.rank = 1;
```

