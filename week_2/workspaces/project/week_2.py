from typing import List

from dagster import In, Nothing, Out, ResourceDefinition, String, graph, op
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
def process_data(context, data: List[Stock]) -> Aggregation:
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
def week_2_pipeline():
    data = process_data(get_s3_data())
    put_redis_data(data)
    put_s3_data(data)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
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
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}

week_2_pipeline_local = week_2_pipeline.to_job(
    name="week_2_pipeline_local",
    config=local,
    resource_defs={"s3": mock_s3_resource, "redis": ResourceDefinition.mock_resource()}
)

week_2_pipeline_docker = week_2_pipeline.to_job(
    name="week_2_pipeline_docker",
    config=docker,
    resource_defs={"s3": s3_resource, "redis": redis_resource}
)
