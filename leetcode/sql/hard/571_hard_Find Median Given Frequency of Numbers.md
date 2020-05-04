- inner join 总序号

```scala
package com.chaosdata.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.util.Random

object FindMedianGivenFrequency {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("having window lag")
      .master("local")
      .getOrCreate()

    import spark.implicits._
    val df = (1 to 100).map(id => (id, Random.nextInt(19) + 1)).toDF("id", "freq")

    val all_count = df.agg(sum("freq").alias("all_count")).head().getLong(0)

    val df_ = df.withColumnRenamed("id", "id_").withColumnRenamed("freq", "freq_")

    val indexDF = df.join(df_, $"id" >= $"id_", "outer")
      .groupBy("id").agg(sum("freq_").alias("index_count"))

    val tmp = df.join(indexDF, "id").orderBy(asc("id"))

    println(all_count)
    val filterExpr = if (all_count % 2 == 0)
      s"(index_count - freq) < ${(all_count / 2).toInt} and (index_count) >= ${(all_count / 2).toInt} or " +
        s"(index_count - freq) < ${(all_count / 2).toInt + 1} and (index_count) >= ${(all_count / 2).toInt + 1}"
    else
      s"(index_count - freq) < ${(all_count / 2).toInt + 1} and (index_count) >= ${(all_count / 2).toInt + 1}"
    println(filterExpr)

    val result = tmp.filter(filterExpr)

    result.show()

    spark.stop()
  }

}
```



The `Numbers` table keeps the value of number and its frequency.

```
+----------+-------------+
|  Number  |  Frequency  |
+----------+-------------|
|  0       |  7          |
|  1       |  1          |
|  2       |  3          |
|  3       |  1          |
+----------+-------------+
```

In this table, the numbers are `0, 0, 0, 0, 0, 0, 0, 1, 2, 2, 2, 3`, so the median is `(0 + 0) / 2 = 0`.

Write a query to find the median of all numbers and name the result as `median`.