```sql
SELECT Score, dense_rank() OVER (ORDER BY Score DESC) AS Rank
FROM Scores
```

