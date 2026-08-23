## (Pre Class) Airflow Setup

### Setup Steps

1. navigate to project folder
```cmd
cd 04-automated-data-pipelines/
```
Ref: [Project folder](https://github.com/earthern573/data-engineering-bootcamp/tree/main/04-automated-data-pipelines)

2. Run cmd
```sh
mkdir -p ./config ./dags ./logs ./plugins ./tests ./pyspark ./spark-events
```
Ref: [Create folders required for set-up](https://github.com/earthern573/data-engineering-bootcamp/blob/main/04-automated-data-pipelines/README.md?plain=1#L23)

3. Run cmd
```sh
echo -e "AIRFLOW_UID=$(id -u)" > .env
```
Ref: [set up variable](https://github.com/earthern573/data-engineering-bootcamp/blob/main/04-automated-data-pipelines/README.md?plain=1#L30)

4. Run
```docker
docker compose up
```
Wait until app is build, and port 8080 is ready to access, may take a while over 10 minutes.

5. Access Airflow after Step 4 completed.
URL from step 4 will navigate to login page, the default username, password are `airflow`, `airflow`, respectively.

6. After complete an exercise in step 5, Run
```docker
docker compose down
```