# Airflow Demo 
- Change `CeleryExecutor` to `LocalExecutor` 
- Remove Celery Worker, Flower and Redis (all used with Celery)
    - Task Management : Celery
    - Monitoring Task : Flower
    - Message Broker : Redis
- 