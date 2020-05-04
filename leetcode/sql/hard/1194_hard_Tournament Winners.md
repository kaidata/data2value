

```scala
package com.chaosdata.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object TournamentWinners {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("having window lag")
      .master("local")
      .getOrCreate()

    import spark.implicits._
    val playersDF = "15\t1\n25\t1\n30\t1\n45\t1\n10\t2\n35\t2\n50\t2\n20\t3\n40\t3"
      .split("\n").map(line => {
      val arr = line.split("\t")
      (arr(0), arr(1))
    }).toSeq.toDF("player_id", "group_id")
    val matchesDF = "1\t15\t45\t3\t0\n2\t30\t25\t1\t2\n3\t30\t15\t2\t0\n4\t40\t20\t5\t2\n5\t35\t50\t1\t1"
      .split("\n").map(line => {
      val arr = line.split("\t")
      (arr(0), arr(1), arr(2), arr(3), arr(4))
    }).toSeq.toDF("match_id", "first_player", "second_player", "first_score", "second_score")

    val tmp = playersDF.join(matchesDF, $"first_player" === $"player_id" or $"second_player" === $"player_id")

    val simple = tmp.selectExpr("player_id", "group_id", "case when player_id=first_player then first_score else second_score end as score")

    val res = simple.groupBy("group_id", "player_id").agg(sum("score").alias("sum_score"))
      .withColumn("rn", row_number().over(Window.partitionBy("group_id").orderBy(desc("sum_score"), asc("player_id"))))
      .filter("rn = 1")
      .select("group_id", "player_id")
      .orderBy(asc("group_id"))

    res.show()

    spark.stop()
  }
}
```



Table: `Players`

```
+-------------+-------+
| Column Name | Type  |
+-------------+-------+
| player_id   | int   |
| group_id    | int   |
+-------------+-------+
player_id is the primary key of this table.
Each row of this table indicates the group of each player.
```

Table: `Matches`

```
+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| match_id      | int     |
| first_player  | int     |
| second_player | int     | 
| first_score   | int     |
| second_score  | int     |
+---------------+---------+
match_id is the primary key of this table.
Each row is a record of a match, first_player and second_player contain the player_id of each match.
first_score and second_score contain the number of points of the first_player and second_player respectively.
You may assume that, in each match, players belongs to the same group.
```

 

The winner in each group is the player who scored the maximum total points within the group. In the case of a tie, the **lowest** player_id wins.

Write an SQL query to find the winner in each group.

The query result format is in the following example:

```
Players table:
+-----------+------------+
| player_id | group_id   |
+-----------+------------+
| 15        | 1          |
| 25        | 1          |
| 30        | 1          |
| 45        | 1          |
| 10        | 2          |
| 35        | 2          |
| 50        | 2          |
| 20        | 3          |
| 40        | 3          |
+-----------+------------+

Matches table:
+------------+--------------+---------------+-------------+--------------+
| match_id   | first_player | second_player | first_score | second_score |
+------------+--------------+---------------+-------------+--------------+
| 1          | 15           | 45            | 3           | 0            |
| 2          | 30           | 25            | 1           | 2            |
| 3          | 30           | 15            | 2           | 0            |
| 4          | 40           | 20            | 5           | 2            |
| 5          | 35           | 50            | 1           | 1            |
+------------+--------------+---------------+-------------+--------------+

Result table:
+-----------+------------+
| group_id  | player_id  |
+-----------+------------+ 
| 1         | 15         |
| 2         | 35         |
| 3         | 40         |
+-----------+------------+
```



- each match query group
- match one 2 two
- group by group, 