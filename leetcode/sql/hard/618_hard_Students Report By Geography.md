- group by name
- pivot



```scala
package com.chaosdata.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object StudentsReportByGeography {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getName).master("local").getOrCreate()

    import spark.implicits._
    val df = Seq(("Jack", "America"), ("Pascal", "Europe"), ("Xi", "Asia"), ("Jane", "America")).toDF("name", "continent")

    val pivotDF = df.groupBy("name").pivot("continent", Seq("America", "Asia", "Europe")).agg(first("name")).drop("name")
    pivotDF.show()

    val df1 = pivotDF.select("America").filter(!isnull(col("America"))).withColumn("rn1", row_number().over(Window.orderBy("America")))
    val df2 = pivotDF.select("Asia").filter(!isnull(col("Asia"))).withColumn("rn2", row_number().over(Window.orderBy("Asia")))
    val df3 = pivotDF.select("Europe").filter(!isnull(col("Europe"))).withColumn("rn3", row_number().over(Window.orderBy("Europe")))

    val df4 = df1.join(df2, df1.col("rn1") === df2.col("rn2"), "full")

    df4.show()

    val res = df4.join(df3, (df4.col("rn1") === df3.col("rn3")) or (df4.col("rn2") === (df3.col("rn3"))), "full")
      .drop("rn1").drop("rn2").drop("rn3")

    res.show()

    spark.stop()
  }

}
```





A U.S graduate school has students from Asia, Europe and America. The students' location information are stored in table `student` as below.

```
| name   | continent |
|--------|-----------|
| Jack   | America   |
| Pascal | Europe    |
| Xi     | Asia      |
| Jane   | America   |
```

[Pivot](https://en.wikipedia.org/wiki/Pivot_table) the continent column in this table so that each name is sorted alphabetically and displayed underneath its corresponding continent. The output headers should be America, Asia and Europe respectively. It is guaranteed that the student number from America is no less than either Asia or Europe.

For the sample input, the output is:

```
| America | Asia | Europe |
|---------|------|--------|
| Jack    | Xi   | Pascal |
| Jane    |      |        |
```