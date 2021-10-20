# Module 1 - Initialize Airflow

## Setting up Airflow for local environment

We'll setup Airflow using the `docker-compose.yaml` file from Airflow.
https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html#initializing-environment

1. Run the following command to setup a user
    ```
    docker-compose up airflow-init
    ```
2. After the user has been created, we can now launch the other Airflow components
    ```
    docker-compose up -d
    ```
3. You should be seeing healthy containers running when doing `docker ps -a`

![airflow_containers](./airflow/images/airflow_containers.png)



To learn more about the architecture, here's the [documentation](https://airflow.apache.org/docs/apache-airflow/stable/concepts/overview.html).
