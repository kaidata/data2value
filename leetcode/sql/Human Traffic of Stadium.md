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

  