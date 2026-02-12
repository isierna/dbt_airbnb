Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt source freshness
- dbt test (
    Python scripts that parses the run results json files in target:
     - run 'python scripts/load_test_results.py' => It will generate table with test results for BI tool.
     - run 'python scripts/load_source_freshness.py' => It will generate 'FRESHNESS_FROM_JSON' table from sources.json run results
    Models created on top of Elementary test results:
     - run 'dbt run -s test_results_report' => it will generate table in DEV_REPORT schema
     - run 'dbt run -s source_freshness' => it will generate table in DEV schema
     - run 'dbt run -s data_quality_score_daily'
    )


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
