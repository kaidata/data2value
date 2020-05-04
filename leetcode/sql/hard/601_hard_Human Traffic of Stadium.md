- by id

  ``` sql
  SELECT a.id AS "id", to_char(a.visit_date, 'yyyy-mm-dd') AS "visit_date", a.people AS "people"
  FROM (
  	SELECT DISTINCT d.id AS id, d.visit_date AS visit_date, d.people AS people
  	FROM stadium d
  		INNER JOIN (
  			SELECT id, aft_id
  			FROM (
  				SELECT id, lead(s.id, 2, NULL) OVER (ORDER BY s.id) AS aft_id
  				FROM stadium s
  				WHERE s.people >= 100
  			) c
  			WHERE c.aft_id - c.id = 2
  		) b
  		ON d.id - b.id IN (0, 1, 2)
  ) a
  ORDER BY a.id
  ```

  

- by visit_date

  ``` sql
  SELECT a.id AS "id", to_char(a.visit_date, 'yyyy-mm-dd') AS "visit_date", a.people AS "people"
  FROM (
  	SELECT DISTINCT d.id AS id, d.visit_date AS visit_date, d.people AS people
  	FROM stadium d
  		INNER JOIN (
  			SELECT visit_date, aft_date
  			FROM (
  				SELECT visit_date, lead(s.visit_date, 2, NULL) OVER (ORDER BY s.visit_date) AS aft_date
  				FROM stadium s
  				WHERE s.people >= 100
  			) c
  			WHERE ROUND(TO_NUMBER(c.aft_date - c.visit_date)) = 2
  		) b
  		ON ROUND(TO_NUMBER(d.visit_date - b.visit_date)) IN (0, 1, 2)
  ) a
  ORDER BY a.visit_date
  ```




X city built a new stadium, each day many people visit it and the stats are saved as these columns: **id**, **visit_date**, **people**

Please write a query to display the records which have 3 or more consecutive rows and the amount of people more than 100(inclusive).

For example, the table `stadium`:

```
+------+------------+-----------+
| id   | visit_date | people    |
+------+------------+-----------+
| 1    | 2017-01-01 | 10        |
| 2    | 2017-01-02 | 109       |
| 3    | 2017-01-03 | 150       |
| 4    | 2017-01-04 | 99        |
| 5    | 2017-01-05 | 145       |
| 6    | 2017-01-06 | 1455      |
| 7    | 2017-01-07 | 199       |
| 8    | 2017-01-08 | 188       |
+------+------------+-----------+
```

For the sample data above, the output is:

```
+------+------------+-----------+
| id   | visit_date | people    |
+------+------------+-----------+
| 5    | 2017-01-05 | 145       |
| 6    | 2017-01-06 | 1455      |
| 7    | 2017-01-07 | 199       |
| 8    | 2017-01-08 | 188       |
+------+------------+-----------+
```