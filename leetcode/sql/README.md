- 185_hard_Department Top Three Salaries
  
  - done
  - **DENSE_RANK()** OVER (PARTITION BY DepartmentId ORDER BY Salary DESC) AS rank
    - rank <= 3
  
- 262_hard_Trips and Users
  
  - done
  - driver inner join\client inner join
  
- 569_hard_Median Employee Salary

  - done
  - row_num over partition by Company order by Salary as rownum
  - count(1) over group by Company as total_count
  - rn >= ceil(max_rownum / 2) and rn <= (max_rownum / 2) + 1

- 571_hard_Find Median Given Frequency of Numbers

  - done

  - max_serial_id as all_count

  - ```scala
    if (all_count % 2 == 0)
          s"(index_count - freq) < ${(all_count / 2).toInt} and (index_count) >= ${(all_count / 2).toInt} or " +
            s"(index_count - freq) < ${(all_count / 2).toInt + 1} and (index_count) >= ${(all_count / 2).toInt + 1}"
        else
          s"(index_count - freq) < ${(all_count / 2).toInt + 1} and (index_count) >= ${(all_count / 2).toInt + 1}"
    ```

- 579_hard_Find Cumulative[累积的] Salary of an Employee

  - done
  - over a period of 3 months but exclude the most recent month.
    - df.withColumn("rn", row_number().over(Window.partitionBy("id").orderBy(desc("month"))))
            .filter("rn>1 and rn<5")
  - cumulative sum of an employee’s salary 
    - join(res_, $"id" === $"id_" and $"month" >= $"month_", "outer")

- 601_hard_Human Traffic of Stadium
  
  - done
  - 筛选连续3天的记录
    - lead(s.visit_date, 2, NULL) OVER (ORDER BY s.visit_date) AS aft_date
    - WHERE ROUND(TO_NUMBER(c.aft_date - c.visit_date)) = 2
  - 根据符合的记录衍生后面2天的记录
    - ROUND(TO_NUMBER(d.visit_date - b.visit_date)) IN (0, 1, 2)
  - 去重
    - DISTINCT d.id AS id
  
- 615_hard_Average Salary Departments VS Company

  - done
  - group by month, department_id; avg_dep_salary
  - group by month; avg_com_salary
  - join on month, case when then else

- 618_hard_Students Report By Geography

  - done
  - groupBy("name").pivot("continent", Seq("America", "Asia", "Europe")).agg(first("name"))
  - 按column分拆表; select("country_name"), 并衍生rownum
  - 根据rownum合并记录

- 1097_hard_Game Play Analysis V

  - done

- 1127_hard_User Purchase Platform

  - done
  - group by spend_date, platform
  - group by spend_date
    - filter count(platform) == 2
    - (case when boolExpr then value else value end) aliasName
  - union

- 1159_hard_Market Analysis II

  - done
  - 找出第二购买品牌
    - withColumn("rn",      row_number().over(Window.partitionBy("seller_id").orderBy(asc("order_date"))))
            .filter("rn=2")
  - 第二购买品牌与喜爱品牌相同
    - usersDF.join(tmp, $"user_id" === $"seller_id", "outer")
            .selectExpr("user_id as seller_id", "case when (item_brand=favorite_brand and rn=2) then 'yes' else 'no' end as 2nd_item_fav_brand")

- 1194_hard_Tournament Winners

  - done
  - 关联group
    - playersDF.join(matchesDF, $"first_player" === $"player_id" or $"second_player" === $"player_id")
  - 分数分组汇总并排序，筛选组内排名第一
    - groupBy("group_id", "player_id").agg(sum("score").alias("sum_score"))
            .withColumn("rn", row_number().over(Window.partitionBy("group_id").orderBy(desc("sum_score"), asc("player_id"))))
            .filter("rn = 1")

- 1225_hard_Report Contiguous Dates

  - done
  - contiguous
    - lag
    - lead
    - .withColumn("pre", lag("success", 1)
              .over(Window.orderBy(desc("day"))))
            .withColumn("aft", lead("success", 1)
              .over(Window.orderBy(desc("day"))))
  - group
    - rownum, rn_1
      - withColumn("rn_1", row_number().over(Window.orderBy(desc("day"))))
    - filter uncondition record && rownum, rn_2
      - filter($"success" === $"pre" or $"success" === $"aft")
              .withColumn("rn_2", row_number().over(Window.orderBy(desc("day"))))
              .withColumn("group_id", $"rn_1" - $"rn_2")
    - group_id = rn_1 - rn_2
      - groupBy("group_id", "success")
              .agg(min("day").alias("start"), max("day").alias("end"))

- 1336_hard_Number of Transactions per Visit

  - done [20200411]
  - groupBy("transaction_date", "user_id").agg(count("*").alias("trans_count"))
  - 序列生成

- 1369_hard_Get the Second Most Recent Activity

  - done
  - withColumn("rn", row_number().over(Window.partitionBy("username").orderBy(desc("startDate"))))
  - filter("rn=2")
  - groupBy("username").agg(count("activity").alias("activity_count")).filter("activity_count=1")
  - union

- 1384_hard_Total Sales Amount by Year

  - done

  - 开始、结束时间按年切割衍生

    - withColumn("timeRanges", timeRangeSplitUDF(col("period_start"), col("period_end")))

  - 多值变行

    - withColumn("timeRange", explode($"timeRanges"))

  - 求年度天数并乘每天平均销售额

    - withColumn("days", datediff($"timeRange._2", $"timeRange._1").+(1))
            .withColumn("total_amount", $"days".*($"average_daily_sales"))

    

