- group by month, department_id; avg_dep_salary
- group by month; avg_com_salary
- join on month, case when then else



'data bank salary'

```scala
package com.chaosdata.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AverageSalaryDepartmentsCompany {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("having window lag")
      .master("local")
      .getOrCreate()

    val df = spark.read.option("header", true).csv("E:\\workspace\\SwordOffer-master\\src\\main\\resources\\employee.csv")

    val depAvg = df.groupBy("year", "department_code").agg(avg("salaries").alias("dep_avg_salary"))

    val comAvg = df.groupBy("year").agg(avg("salaries").alias("com_avg_salary"))

    val res = depAvg.join(comAvg, "year").selectExpr("year", "department_code", "dep_avg_salary", "com_avg_salary",
      "(case " +
        "when dep_avg_salary > com_avg_salary then 'higher' " +
        "when dep_avg_salary < com_avg_salary then 'lower' " +
        "else 'same' " +
        "end) comparison").orderBy("year", "department_code")

    res.show()

    spark.stop()
  }

}
```





Given two tables as below, write a query to display the comparison result (higher/lower/same) of the average salary of employees in a department to the company’s average salary.
Table: salary

```
| id | employee_id | amount | pay_date   |
|----|-------------|--------|------------|
| 1  | 1           | 9000   | 2017-03-31 |
| 2  | 2           | 6000   | 2017-03-31 |
| 3  | 3           | 10000  | 2017-03-31 |
| 4  | 1           | 7000   | 2017-02-28 |
| 5  | 2           | 6000   | 2017-02-28 |
| 6  | 3           | 8000   | 2017-02-28 |
```

The employee_id column refers to the employee_id in the following table employee.

```
| employee_id | department_id |
|-------------|---------------|
| 1           | 1             |
| 2           | 2             |
| 3           | 2             |
```

So for the sample data above, the result is:

```
| pay_month | department_id | comparison  |
|-----------|---------------|-------------|
| 2017-03   | 1             | higher      |
| 2017-03   | 2             | lower       |
| 2017-02   | 1             | same        |
| 2017-02   | 2             | same        |
```

