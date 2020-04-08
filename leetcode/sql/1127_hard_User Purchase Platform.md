- group by spend_date, platform
- group by spend_date
  - filter count(platform) == 2
  - (case when boolExpr then value else value end) aliasName
- union



```scala
package com.chaosdata.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

import org.apache.spark.sql.functions._

object SpendingAnalysis {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("having window lag")
      .master("local")
      .getOrCreate()

    val df = toDF(spark)

    val twoLevelRes = df.groupBy("spend_date", "platform").agg(sum("amount").alias("total_amount"), countDistinct("user_id").alias("total_users"))
      .select("spend_date", "platform", "total_amount", "total_users")
    twoLevelRes.show()

    val oneLevelRes = df.groupBy("spend_date").agg(sum("amount").alias("total_amount"), countDistinct("user_id").alias("total_users"), countDistinct("platform").alias("total_platform"))
      .withColumn("platform", lit("both"))
//      .filter("total_platform >= 2")
      .selectExpr("spend_date", "platform", "(case when total_platform=2 then total_amount else 0 end) total_amount", "(case when total_platform=2 then total_users else 0 end) total_users")
    oneLevelRes.show()

    oneLevelRes.union(twoLevelRes).orderBy("spend_date", "platform").show(100)

    spark.stop()
  }

  def toDF(spark: SparkSession): DataFrame = {
    import spark.implicits._
    "1\t2019/7/1\tmobile\t100\n1\t2019/7/2\tdesktop\t100\n1\t2019/7/3\tmobile\t100\n2\t2019/7/4\tdesktop\t100\n2\t2019/7/1\tmobile\t100\n3\t2019/7/2\tdesktop\t100\n3\t2019/7/3\tmobile\t100\n4\t2019/7/4\tdesktop\t100\n1\t2019/7/1\tmobile\t100\n1\t2019/7/2\tdesktop\t100\n1\t2019/7/3\tmobile\t100\n2\t2019/7/4\tdesktop\t100\n2\t2019/7/1\tmobile\t100\n3\t2019/7/2\tmobile\t100\n3\t2019/7/3\tmobile\t100\n4\t2019/7/4\tmobile\t100\n1\t2019/7/1\tmobile\t100\n1\t2019/7/2\tmobile\t100\n1\t2019/7/3\tmobile\t100"
      .split("\n").map(line => {
      val arr = line.split("\t")

      Spending(arr(0), arr(1), arr(2), arr(3))
    }).toSeq.toDF()
  }
}

case class Spending(user_id: String, spend_date: String, platform: String, amount: String)

```





Table: `Spending`

```
+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| user_id     | int     |
| spend_date  | date    |
| platform    | enum    | 
| amount      | int     |
+-------------+---------+
The table logs the spendings history of users that make purchases from an online shopping website which has a desktop and a mobile application.
(user_id, spend_date, platform) is the primary key of this table.
The platform column is an ENUM type of ('desktop', 'mobile').
```

Write an SQL query to find the total number of users and the total amount spent using mobile **only**, desktop **only** and **both** mobile and desktop together for each date.

The query result format is in the following example:

```
Spending table:
+---------+------------+----------+--------+
| user_id | spend_date | platform | amount |
+---------+------------+----------+--------+
| 1       | 2019-07-01 | mobile   | 100    |
| 1       | 2019-07-01 | desktop  | 100    |
| 2       | 2019-07-01 | mobile   | 100    |
| 2       | 2019-07-02 | mobile   | 100    |
| 3       | 2019-07-01 | desktop  | 100    |
| 3       | 2019-07-02 | desktop  | 100    |
+---------+------------+----------+--------+

Result table:
+------------+----------+--------------+-------------+
| spend_date | platform | total_amount | total_users |
+------------+----------+--------------+-------------+
| 2019-07-01 | desktop  | 100          | 1           |
| 2019-07-01 | mobile   | 100          | 1           |
| 2019-07-01 | both     | 200          | 1           |
| 2019-07-02 | desktop  | 100          | 1           |
| 2019-07-02 | mobile   | 100          | 1           |
| 2019-07-02 | both     | 0            | 0           |
+------------+----------+--------------+-------------+ 
On 2019-07-01, user 1 purchased using both desktop and mobile, user 2 purchased using mobile only and user 3 purchased using desktop only.
On 2019-07-02, user 2 purchased using mobile only, user 3 purchased using desktop only and no one purchased using both platforms.
```

