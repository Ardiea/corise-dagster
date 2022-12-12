from typing import List

from dagster import (
    In,
    Nothing,
    Out,
    ResourceDefinition,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    SkipReason,
    String,
    graph,
    op,
    schedule,
    sensor,
    static_partitioned_config,
)
from workspaces.project.sensors import get_s3_keys
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock



@op(
    config_schema={"s3_key": String}, 
    required_resource_keys={"s3"},
    out=Out(is_required=False, dagster_type=List[Stock])
)
def get_s3_data(context) -> List[Stock]:
    s3_key = context.op_config["s3_key"]
    result = []
    for stock_list in context.resources.s3.get_data(s3_key):
        result.append(Stock.from_list(stock_list))
    return result


@op(
    ins=
    {
        "data": In(dagster_type=List[Stock], 
         description="List of stock metrics at given times.")
    },
    out=Out(dagster_type=Aggregation),
)
def process_data(data: List[Stock]) -> Aggregation:
    highest = data[0]
    for element in data:
        if element.high > highest.high:
            highest = element
    return Aggregation(date=highest.date, high=highest.high)


@op(
    required_resource_keys={"redis"}
)
def put_redis_data(context, highest_value: Aggregation):
    context.resources.redis.put_data(name=str(highest_value.date), value=str(highest_value.high))


@op(
    required_resource_keys={"s3"}
)
def put_s3_data(context, highest_value: Aggregation):
    context.resources.s3.put_data(key_name=str(highest_value.date), data=highest_value)


@graph
def week_3_pipeline():
    data = process_data(get_s3_data())
    put_redis_data(data)
    put_s3_data(data)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


docker = {
    "resources": {
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://localstack:4566",
            }
        },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379,
            }
        },
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}

@static_partitioned_config(
    partition_keys=[str(i) for i in range(1,11)]
)
def docker_config(partition_key: int):
    part_config = docker.copy()
    part_config["ops"] = {"get_s3_data": {"config": {"s3_key": f"prefix/stock_{partition_key}.csv"}}}
    return part_config


week_3_pipeline_local = week_3_pipeline.to_job(
    name="week_3_pipeline_local",
    config=local,
    resource_defs={"s3": mock_s3_resource, "redis": ResourceDefinition.mock_resource()}
)

week_3_pipeline_docker = week_3_pipeline.to_job(
    name="week_3_pipeline_docker",
    config=docker,
    resource_defs={"s3": s3_resource, "redis": redis_resource},
    op_retry_policy=RetryPolicy(max_retries=10,delay=1)
)

week_3_schedule_local = ScheduleDefinition(
    job=week_3_pipeline_local,
    cron_schedule="*/5 * * * *"
)

@schedule(cron_schedule="0/5 * * * *", job=week_3_pipeline_docker)
def week_3_schedule_docker():
    for file in stocks:
        request = week_3_pipeline_docker.run_request_for_partition(partition_key=file, run_key=file)
        yield request


@sensor(job=week_3_pipeline_docker)
def week_3_sensor_docker(context):
    new_files = get_s3_keys(
        bucket="dagster",
        prefix="prefix",
        endpoint_url="http://localstack:4566"
    )
    sensor_docker_config = docker.copy()
    if not new_files:
        yield SkipReason("No new s3 files found in bucket.")
        return
    for new_file in new_files:
        sensor_docker_config["ops"] = {"get_s3_data": {"config": {"s3_key": new_file}}}
        yield RunRequest(
            run_key=new_file,
            run_config=sensor_docker_config
        )
