## Retviews Data Engineering Pipeline

This is a demonstration of how a data engineering pipeline can be implemented in order to build a Data Lake on Amazon.

___

### Tasks
1. Begin pipeline.
2. Check and verify source file encoding (UTF-8)
3. Push the file to RAW layer in S3.
4. Use Glue to curate the file and move to CURATED layer.
5. Use Glue to aggregate data and store in AGGREGATED layer in PARQUET.
6. Send a notification to Slack for job completion.
7. End pipeline.

![](https://i.imgur.com/8rOjaeB.png)