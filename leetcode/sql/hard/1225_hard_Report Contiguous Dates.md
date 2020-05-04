- contiguous
  - lag
  - lead
- group
  - rownum, rn_1
  - filter uncondition record && rownum, rn_2
  - group_id = rn_1 - rn_2



- note(not recommend solution)
  - join (day > day_)
  - group by day, agg(set)
    - set process



```scala
package com.chaosdata.spark

import java.sql.Date

import com.twitter.util.TimeFormat
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object ReportContiguousDates {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("having window lag")
      .master("local")
      .getOrCreate()

    val parseDateUDF = spark.udf.register("parseDate", (str: String) => {
      new Date(new TimeFormat("yyyy/MM/dd").parse(str).inMilliseconds)
    })

    val parseBoolUDF = spark.udf.register("parseBool", (str: String) => {
      StringUtils.equals(str, "TRUE")
    })

    val df = spark.read.option("header", true).csv("E:\\workspace\\SwordOffer-master\\src\\main\\resources\\taskStatus.csv")
      .selectExpr("parseDate(date) as day", "parseBool(status) as success")
    // rownum
    val all = df.withColumn("rn_1", row_number().over(Window.orderBy(desc("day"))))

    /**
     * 求前面、后面的状态
     */
    val tmp = all
      .withColumn("pre", lag("success", 1)
        .over(Window.orderBy(desc("day"))))
      .withColumn("aft", lead("success", 1)
        .over(Window.orderBy(desc("day"))))
      .select("day", "pre", "success", "aft", "rn_1")
    tmp.show()

    /**
     * 过滤掉不连续的record, 并重新rownum
     */
    import spark.implicits._
    val onlyContiguousDF = tmp.filter($"success" === $"pre" or $"success" === $"aft")
      .withColumn("rn_2", row_number().over(Window.orderBy(desc("day"))))
      .withColumn("group_id", $"rn_1" - $"rn_2")
    onlyContiguousDF.show()

    val result = onlyContiguousDF.groupBy("group_id", "success")
      .agg(min("day").alias("start"), max("day").alias("end"))
    result.show()

    spark.stop()
  }
}
```



Table: `Failed`

```
+--------------+---------+
| Column Name  | Type    |
+--------------+---------+
| fail_date    | date    |
+--------------+---------+
Primary key for this table is fail_date.
Failed table contains the days of failed tasks.
```

Table: `Succeeded`

```
+--------------+---------+
| Column Name  | Type    |
+--------------+---------+
| success_date | date    |
+--------------+---------+
Primary key for this table is success_date.
Succeeded table contains the days of succeeded tasks.
```

 

A system is running one task **every day**. Every task is independent of the previous tasks. The tasks can fail or succeed.

Write an SQL query to generate a report of `period_state` for each continuous interval of days in the period from **2019-01-01** to **2019-12-31**.

```
period_state` is *'failed'* if tasks in this interval failed or *'succeeded'* if tasks in this interval succeeded. Interval of days are retrieved as `start_date` and `end_date.
```

Order result by `start_date`.

The query result format is in the following example:

```
Failed table:
+-------------------+
| fail_date         |
+-------------------+
| 2018-12-28        |
| 2018-12-29        |
| 2019-01-04        |
| 2019-01-05        |
+-------------------+

Succeeded table:
+-------------------+
| success_date      |
+-------------------+
| 2018-12-30        |
| 2018-12-31        |
| 2019-01-01        |
| 2019-01-02        |
| 2019-01-03        |
| 2019-01-06        |
+-------------------+


Result table:
+--------------+--------------+--------------+
| period_state | start_date   | end_date     |
+--------------+--------------+--------------+
| succeeded    | 2019-01-01   | 2019-01-03   |
| failed       | 2019-01-04   | 2019-01-05   |
| succeeded    | 2019-01-06   | 2019-01-06   |
+--------------+--------------+--------------+

The report ignored the system state in 2018 as we care about the system in the period 2019-01-01 to 2019-12-31.
From 2019-01-01 to 2019-01-03 all tasks succeeded and the system state was "succeeded".
From 2019-01-04 to 2019-01-05 all tasks failed and system state was "failed".
From 2019-01-06 to 2019-01-06 all tasks succeeded and system state was "succeeded".
```

