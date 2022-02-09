#  Welcome To Lending Club Loan Data
Project is a Scala console application that is retrieving data using Hive. The dataset is from the Lending club platform,
which is a platform that allows individuals to lend to other individuals.This data set represents thousands of loans made through the Lending Club platform.
The application allows users to login either as an admin or basic user.
### MVP:
- ALL user interaction comes purely from the console application

-Hive scrape data from datasets from a csv file
- 2 types of users: BASIC and ADMIN: ADMIN user implements all CRUD in the backend and BASIC user only updates and run queries. 
- implemented bucketing by loan amount, and partitioning by state
### Technologies
- Hadoop MapReduce
- YARN(by default) 
- HDFS
- Scala 2.11 (or 2.12)
- Hive
- Git + GitHub
- VSCode
