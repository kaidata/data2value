- Day1_retention; lead(event_date, 1, null) over partition by player_id order by event_date asc next_event_date
  - ROUND(TO_NUMBER(next_event_date - event_date)) = 1
- install; first over partition by player_id order by event_date asc
- inner join
- installs; select player_id, device_id, first() over(partition by device_id order by event_date asc) as install_date from Activity
- day1_retention; select  player_id, event_date, lead(event_date, 1, null) over(partition by player_id order by event_date asc ) as next_date from Activity where ROUND(TO_NUMBER(next_event_date - event_date)) = 1

```scala
package com.chaosdata.spark

import java.sql.Date

import com.twitter.util.TimeFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object GamePlayAnalysis {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("having window lag")
      .master("local")
      .getOrCreate()

    val parseDateUDF = spark.udf.register("parseDate", (str: String) => {
      new Date(new TimeFormat("yyyy-MM-dd").parse(str).inMilliseconds)
    })

    import spark.implicits._
    val activityDF = "1\t2\t2016-03-01\t5\n1\t2\t2016-03-02\t6\n2\t3\t2017-06-25\t1\n3\t1\t2016-03-01\t0\n3\t4\t2016-07-03\t5"
      .split("\n").map(line => {
      val arr = line.split("\t")
      (arr(0), arr(1), arr(2), arr(3))
    }).toSeq.toDF("player_id", "device_id", "event_date_", "games_played")
      .withColumn("event_date", parseDateUDF(col("event_date_")))
      .drop("event_date_")

    val tmp = activityDF.withColumn("rn", row_number().over(Window.partitionBy("player_id").orderBy(asc("event_date"))))
      .withColumn("aft", lead("event_date", 1).over(Window.partitionBy("player_id").orderBy(asc("event_date"))))
      .withColumn("diff", datediff(col("aft"), col("event_date")))
      .filter("rn=1")
      .selectExpr("event_date as install_dt", "case when diff=1 then 1 else 0 end as day1_retention")
      .groupBy("install_dt").agg(
      count("*").alias("install_count")
      , sum("day1_retention").alias("day1_retention_count")
    ).selectExpr("install_dt", "install_count", "day1_retention_count", "cast((day1_retention_count/install_count) as decimal(3,2)) as Day1_retention")
    
    tmp.show()

    spark.stop()
  }

}
```





Table: `Activity`

```
+--------------+---------+
| Column Name  | Type    |
+--------------+---------+
| player_id    | int     |
| device_id    | int     |
| event_date   | date    |
| games_played | int     |
+--------------+---------+
(player_id, event_date) is the primary key of this table.
This table shows the activity of players of some game.
Each row is a record of a player who logged in and played a number of games (possibly 0) before logging out on some day using so
me device.
```

We define the *install date* of a player to be the first login day of that player.

We also define *day 1 retention* of some date `X` to be the number of players whose install date is `X` and they logged back in on the day right after `X`, divided by the number of players whose install date is `X`, **rounded to 2 decimal places**.

Write an SQL query that reports for each **install date**, the **number of players** that installed the game on that day and the **day 1 retention**.

The query result format is in the following example:

```
Activity table:
+-----------+-----------+------------+--------------+
| player_id | device_id | event_date | games_played |
+-----------+-----------+------------+--------------+
| 1         | 2         | 2016-03-01 | 5            |
| 1         | 2         | 2016-03-02 | 6            |
| 2         | 3         | 2017-06-25 | 1            |
| 3         | 1         | 2016-03-01 | 0            |
| 3         | 4         | 2016-07-03 | 5            |
+-----------+-----------+------------+--------------+

Result table:
+------------+----------+----------------+
| install_dt | installs | Day1_retention |
+------------+----------+----------------+
| 2016-03-01 | 2        | 0.50           |
| 2017-06-25 | 1        | 0.00           |
+------------+----------+----------------+
Player 1 and 3 installed the game on 2016-03-01 but only player 1 logged back in on 2016-03-02 so the day 1 retention of 2016-03-01 is 1 / 2 = 0.50
Player 2 installed the game on 2017-06-25 but didn't log back in on 2017-06-26 so the day 1 retention of 2017-06-25 is 0 / 1 = 0.00
```

