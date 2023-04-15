from datetime import datetime
from typing import List

from dagster import (
    In,
    Nothing,
    OpExecutionContext,
    Out,
    ResourceDefinition,
    String,
    graph,
    op,
)
from workspaces.config import REDIS, S3, S3_FILE
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(
    config_schema={"s3_key": String},
    out={"stocks": Out(dagster_type=List)},
    required_resource_keys={"s3"},
    tags={"kind": "s3"},
)
def get_s3_data(context: OpExecutionContext) -> List[Stock]:
    key_name = context.op_config["s3_key"]
    s3_data = context.resources.s3.get_data(key_name)
    stocks: List[Stock] = []
    for s in s3_data:
        stocks.append(Stock.from_list(s))
    return stocks


@op(
    ins={"stocks": In(dagster_type=List[Stock])},
    out={"aggregation": Out(dagster_type=Aggregation)},
    tags={"kind": "s3"},
)
def process_data(context: OpExecutionContext, stocks: List[Stock]) -> Aggregation:
    highest_val_stock = stocks[0]
    for stock in stocks:
        if highest_val_stock.high <= stock.high:
            highest_val_stock = stock
    return Aggregation(date=highest_val_stock.date, high=highest_val_stock.high)


@op(
    required_resource_keys={"redis"},
    ins={"aggregation": In(dagster_type=Aggregation)},
    tags={"kind": "redis"},
)
def put_redis_data(context: OpExecutionContext, aggregation: Aggregation):
    context.resources.redis.put_data(name=str(aggregation.date), value=str(aggregation.high))


@op(
    required_resource_keys={"s3"},
    ins={"aggregation": In(dagster_type=Aggregation)},
    tags={"kind": "s3"},
)
def put_s3_data(context: OpExecutionContext, aggregation: Aggregation):
    context.resources.s3.put_data(key_name="key_name", data=aggregation)


@graph
def machine_learning_graph():
    data = get_s3_data()
    processed_data = process_data(data)
    put_s3_data(processed_data)
    put_redis_data(processed_data)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": S3_FILE}}},
}

docker = {
    "resources": {
        "s3": {"config": S3},
        "redis": {"config": REDIS},
    },
    "ops": {"get_s3_data": {"config": {"s3_key": S3_FILE}}},
}

machine_learning_job_local = machine_learning_graph.to_job(
    name="machine_learning_job_local",
    config=local,
    resource_defs={"s3": mock_s3_resource, "redis": ResourceDefinition.mock_resource()},
)

machine_learning_job_docker = machine_learning_graph.to_job(
    name="machine_learning_job_docker", config=docker, resource_defs={"s3": s3_resource, "redis": redis_resource}
)
