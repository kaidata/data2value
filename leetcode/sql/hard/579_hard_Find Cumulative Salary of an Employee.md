- 

```scala
package com.chaosdata.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object FindCumulativeSalary {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("having window lag")
      .master("local")
      .getOrCreate()

    import spark.implicits._
    val df = "1\t1\t20\n2\t1\t20\n1\t2\t30\n2\t2\t30\n3\t2\t40\n1\t3\t40\n3\t3\t60\n1\t4\t60\n3\t4\t70"
      .split("\n").map(line => {
      val arr = line.split("\t")

      (arr(0).toInt, arr(1).toInt, arr(2).toInt)
    }).toSeq.toDF("id", "month", "salary")

    df.show()

    val res = df.withColumn("rn", row_number().over(Window.partitionBy("id").orderBy(desc("month"))))
      .filter("rn>1 and rn<5")

    val res_ = res.withColumnRenamed("id", "id_")
      .withColumnRenamed("month", "month_")
      .withColumnRenamed("salary", "salary_")

    val resultDF = res.join(res_, $"id" === $"id_" and $"month" >= $"month_", "outer")
      .groupBy("id", "month").agg(sum("salary_").alias("cumulativeSalary"))
      .orderBy(asc("id"), desc("month"))

    resultDF.show()

    spark.stop()
  }
}

```





The Employee table holds the salary information in a year. Write a SQL to get the cumulative sum of an employee’s salary over a period of 3 months but exclude the most recent month.

The result should be displayed by ‘Id’ ascending, and then by ‘Month’ descending.


Input

```
| Id | Month | Salary |
|----|-------|--------|
| 1  | 1     | 20     |
| 2  | 1     | 20     |
| 1  | 2     | 30     |
| 2  | 2     | 30     |
| 3  | 2     | 40     |
| 1  | 3     | 40     |
| 3  | 3     | 60     |
| 1  | 4     | 60     |
| 3  | 4     | 70     |
```

Output

```
| Id | Month | Salary |
|----|-------|--------|
| 1  | 3     | 90     |
| 1  | 2     | 50     |
| 1  | 1     | 20     |
| 2  | 1     | 20     |
| 3  | 3     | 100    |
| 3  | 2     | 40     |
```

