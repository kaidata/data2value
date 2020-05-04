

```scala
package com.chaosdata.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{asc, desc, row_number, sum}

object MarketAnalysisII {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("having window lag")
      .master("local")
      .getOrCreate()

    import spark.implicits._
    val usersDF = "1\t2019-01-01\tLenovo\n2\t2019-02-09\tSamsung\n3\t2019-01-19\tLG\n4\t2019-05-21\tHP   "
      .split("\n").map(line => {
      val arr = line.split("\t")
      (arr(0), arr(1), arr(2))
    }).toSeq.toDF("user_id", "join_date", "favorite_brand")
    val ordersDF = "1\t2019-08-01\t4\t1\t2\n2\t2019-08-02\t2\t1\t3\n3\t2019-08-03\t3\t2\t3\n4\t2019-08-04\t1\t4\t2\n5\t2019-08-04\t1\t3\t4\n6\t2019-08-05\t2\t2\t4"
      .split("\n").map(line => {
      val arr = line.split("\t")
      (arr(0), arr(1), arr(2), arr(3), arr(4))
    }).toSeq.toDF("order_id", "order_date", "item_id", "buyer_id", "seller_id")

    val itemsDF = "1\tSamsung\n2\tLenovo\n3\tLG\n4\tHP"
      .split("\n").map(line => {
      val arr = line.split("\t")
      (arr(0), arr(1))
    }).toSeq.toDF("item_id", "item_brand")


    val tmp = ordersDF.withColumn("rn",
      row_number().over(Window.partitionBy("seller_id").orderBy(asc("order_date"))))
      .filter("rn=2")
      .join(itemsDF, "item_id")


    val res = usersDF.join(tmp, $"user_id" === $"seller_id", "outer")
      .selectExpr("user_id as seller_id", "case when (item_brand=favorite_brand and rn=2) then 'yes' else 'no' end as 2nd_item_fav_brand")
      .orderBy("seller_id")

    res.show()

    spark.stop()
  }

}
```





Table: `Users`

```
+----------------+---------+
| Column Name    | Type    |
+----------------+---------+
| user_id        | int     |
| join_date      | date    |
| favorite_brand | varchar |
+----------------+---------+
user_id is the primary key of this table.
This table has the info of the users of an online shopping website where users can sell and buy items.
```

Table: `Orders`

```
+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| order_id      | int     |
| order_date    | date    |
| item_id       | int     |
| buyer_id      | int     |
| seller_id     | int     |
+---------------+---------+
order_id is the primary key of this table.
item_id is a foreign key to the Items table.
buyer_id and seller_id are foreign keys to the Users table.
```

Table: `Items`

```
+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| item_id       | int     |
| item_brand    | varchar |
+---------------+---------+
item_id is the primary key of this table.
```

 

Write an SQL query to find for each user, whether the brand of the second item (by date) they sold is their favorite brand. If a user sold less than two items, report the answer for that user as no.

It is guaranteed that no seller sold more than one item on a day.

The query result format is in the following example:

```
Users table:
+---------+------------+----------------+
| user_id | join_date  | favorite_brand |
+---------+------------+----------------+
| 1       | 2019-01-01 | Lenovo         |
| 2       | 2019-02-09 | Samsung        |
| 3       | 2019-01-19 | LG             |
| 4       | 2019-05-21 | HP             |
+---------+------------+----------------+

Orders table:
+----------+------------+---------+----------+-----------+
| order_id | order_date | item_id | buyer_id | seller_id |
+----------+------------+---------+----------+-----------+
| 1        | 2019-08-01 | 4       | 1        | 2         |
| 2        | 2019-08-02 | 2       | 1        | 3         |
| 3        | 2019-08-03 | 3       | 2        | 3         |
| 4        | 2019-08-04 | 1       | 4        | 2         |
| 5        | 2019-08-04 | 1       | 3        | 4         |
| 6        | 2019-08-05 | 2       | 2        | 4         |
+----------+------------+---------+----------+-----------+

Items table:
+---------+------------+
| item_id | item_brand |
+---------+------------+
| 1       | Samsung    |
| 2       | Lenovo     |
| 3       | LG         |
| 4       | HP         |
+---------+------------+

Result table:
+-----------+--------------------+
| seller_id | 2nd_item_fav_brand |
+-----------+--------------------+
| 1         | no                 |
| 2         | yes                |
| 3         | yes                |
| 4         | no                 |
+-----------+--------------------+

The answer for the user with id 1 is no because they sold nothing.
The answer for the users with id 2 and 3 is yes because the brands of their second sold items are their favorite brands.
The answer for the user with id 4 is no because the brand of their second sold item is not their favorite brand.
```

