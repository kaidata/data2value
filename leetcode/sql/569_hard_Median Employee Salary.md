- row_num over partition by Company order by Salary as rownum
- count(1) over group by Company as total_count
- rn >= ceil(max_rownum / 2) and rn <= (max_rownum / 2) + 1

```scala
package com.chaosdata.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object MedianEmployeeSalary {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("having window lag")
      .master("local")
      .getOrCreate()

    val df = spark.read.option("header", true).csv("E:\\workspace\\SwordOffer-master\\src\\main\\resources\\employee.csv")
      .select("department_code", "salaries").withColumn("id", monotonically_increasing_id())

    val rowNumDF = df.withColumn("rn", row_number().over(Window.partitionBy("department_code").orderBy("salaries")))

    val depEmployeeCount = rowNumDF.groupBy("department_code").agg(max("rn").alias("max_rownum"))
    depEmployeeCount.show()

    val res = rowNumDF.join(depEmployeeCount, "department_code").filter("rn >= ceil(max_rownum / 2) and rn <= (max_rownum / 2) + 1")

    res.show()

    spark.stop()
  }

}

```



The `Employee` table holds all employees. The employee table has three columns: Employee Id, Company Name, and Salary.

```
+-----+------------+--------+
|Id   | Company    | Salary |
+-----+------------+--------+
|1    | A          | 2341   |
|2    | A          | 341    |
|3    | A          | 15     |
|4    | A          | 15314  |
|5    | A          | 451    |
|6    | A          | 513    |
|7    | B          | 15     |
|8    | B          | 13     |
|9    | B          | 1154   |
|10   | B          | 1345   |
|11   | B          | 1221   |
|12   | B          | 234    |
|13   | C          | 2345   |
|14   | C          | 2645   |
|15   | C          | 2645   |
|16   | C          | 2652   |
|17   | C          | 65     |
+-----+------------+--------+
```

Write a SQL query to find the median salary of each company. Bonus points if you can solve it without using any built-in SQL functions.

```
+-----+------------+--------+
|Id   | Company    | Salary |
+-----+------------+--------+
|5    | A          | 451    |
|6    | A          | 513    |
|12   | B          | 234    |
|9    | B          | 1154   |
|14   | C          | 2645   |
+-----+------------+--------+
```

