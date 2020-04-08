``` sql
CREATE FUNCTION getNthHighestSalary(N IN NUMBER) RETURN NUMBER IS
result NUMBER;
BEGIN
    /* Write your PL/SQL query statement below */
    
    SELECT d.salary
    INTO result
    FROM (
        SELECT c.Salary AS salary
        FROM (
            SELECT Salary, dense_RANK() OVER (ORDER BY Salary DESC) AS rank
            FROM Employee
        ) c
        WHERE c.rank = N
    ) d
    WHERE rownum = 1;
    
    RETURN result;
END;
```

