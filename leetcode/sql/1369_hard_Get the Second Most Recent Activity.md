- 1 result
- \>= 2 rank over



```scala
package com.chaosdata.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object GetSecondMostRecentActivity {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("having window lag")
      .master("local")
      .getOrCreate()

    import spark.implicits._
    val userActivityDF = "Alice\tTravel\t2020-02-12\t2020-02-20\nAlice\tDancing\t2020-02-21\t2020-02-23\nAlice\tTravel\t2020-02-24\t2020-02-28\nBob\tTravel\t2020-02-11\t2020-02-18"
      .split("\n").map(line => {
      val arr = line.split("\t")
      (arr(0), arr(1), arr(2), arr(3))
    }).toSeq.toDF("username", "activity", "startDate", "endDate")

    val tmp = userActivityDF.withColumn("rn", row_number().over(Window.partitionBy("username").orderBy(desc("startDate"))))
    
    val justOne = tmp.groupBy("username").agg(count("activity").alias("activity_count")).filter("activity_count=1")

    val res = tmp.join(justOne, "username")
      .select("username", "activity", "startDate", "endDate")
      .union(
        tmp.filter("rn=2").select("username", "activity", "startDate", "endDate"))

    res.show()

    spark.stop()
  }
}

```





Table: `UserActivity`

```
+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| username      | varchar |
| activity      | varchar |
| startDate     | Date    |
| endDate       | Date    |
+---------------+---------+
This table does not contain primary key.
This table contain information about the activity performed of each user in a period of time.
A person with username performed a activity from startDate to endDate.
```

Write an SQL query to show the **second most recent activity** of each user.

If the user only has one activity, return that one. 

A user can't perform more than one activity at the same time. Return the result table in **any** order.

The query result format is in the following example:

```
UserActivity table:
+------------+--------------+-------------+-------------+
| username   | activity     | startDate   | endDate     |
+------------+--------------+-------------+-------------+
| Alice      | Travel       | 2020-02-12  | 2020-02-20  |
| Alice      | Dancing      | 2020-02-21  | 2020-02-23  |
| Alice      | Travel       | 2020-02-24  | 2020-02-28  |
| Bob        | Travel       | 2020-02-11  | 2020-02-18  |
+------------+--------------+-------------+-------------+

Result table:
+------------+--------------+-------------+-------------+
| username   | activity     | startDate   | endDate     |
+------------+--------------+-------------+-------------+
| Alice      | Dancing      | 2020-02-21  | 2020-02-23  |
| Bob        | Travel       | 2020-02-11  | 2020-02-18  |
+------------+--------------+-------------+-------------+

The most recent activity of Alice is Travel from 2020-02-24 to 2020-02-28, before that she was dancing from 2020-02-21 to 2020-02-23.
Bob only has one record, we just take that one.
```

