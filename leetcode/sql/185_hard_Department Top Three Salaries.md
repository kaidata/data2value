[Oracle中排名排序函数，ROW_NUMBER、RANK、DENSE_RANK、NTILE、的简介](https://blog.csdn.net/a1150499208/article/details/91039772?depth_1-utm_source=distribute.pc_relevant.none-task&utm_source=distribute.pc_relevant.none-task)



``` sql
SELECT d.Name AS "Department", c.Name AS "Employee", c.Salary AS "Salary"
FROM (
	SELECT Name, Salary, DepartmentId, DENSE_RANK() OVER (PARTITION BY DepartmentId ORDER BY Salary DESC) AS rank
	FROM Employee
) c
	INNER JOIN Department d
	ON d.Id = c.DepartmentId
		AND c.rank <= 3
```