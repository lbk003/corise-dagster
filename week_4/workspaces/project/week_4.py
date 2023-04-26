from datetime import datetime
from typing import List

from dagster import (
    In,
    AssetSelection,
    Nothing,
    OpExecutionContext,
    ScheduleDefinition,
    String,
    ResourceDefinition,
    asset,
    define_asset_job,
    load_assets_from_current_module,
    static_partitioned_config,
)
from workspaces.types import Aggregation, Stock
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.config import REDIS, S3


@asset(
    config_schema={"s3_key": String},
    required_resource_keys={"s3"},
    op_tags={"kind": "s3"},
)
def get_s3_data(context: OpExecutionContext) -> List[Stock]:
    key_name = context.op_config["s3_key"]
    s3_data = context.resources.s3.get_data(key_name)
    stocks: List[Stock] = []
    for s in s3_data:
        stocks.append(Stock.from_list(s))
    return stocks


@asset(
    op_tags={"kind": "s3"},
)
def process_data(context: OpExecutionContext, get_s3_data):
    highest_val_stock = get_s3_data[0]
    for stock in get_s3_data:
        if highest_val_stock.high <= stock.high:
            highest_val_stock = stock
    return Aggregation(date=highest_val_stock.date, high=highest_val_stock.high)


@asset(
    required_resource_keys={"redis"},
    op_tags={"kind": "redis"},
)
def put_redis_data(context: OpExecutionContext, process_data):
    context.resources.redis.put_data(name=str(process_data.date), value=str(process_data.high))


@asset(
    required_resource_keys={"s3"},
    op_tags={"kind": "s3"},
)
def put_s3_data(context: OpExecutionContext, process_data):
    d = datetime.utcnow().strftime("%Y-%m-%d")
    key_name = f"/aggregation_{d}"
    context.resources.s3.put_data(key_name=key_name, data=process_data)


project_assets = load_assets_from_current_module()

docker = {
    "resources": {
        "s3": {"config": S3},
        "redis": {"config": REDIS},
    },
}

@static_partitioned_config(partition_keys=[str(n) for n in range(1, 11)])
def docker_config(partition_key: str):
    return {
        **docker,
        "ops": {"get_s3_data": {"config": {"s3_key": f"prefix/stock_{partition_key}.csv"}}},
    }

machine_learning_asset_job = define_asset_job(
    name="machine_learning_asset_job",
    selection=project_assets,
    config=docker_config,
)

machine_learning_schedule = ScheduleDefinition(
    job=machine_learning_asset_job, 
    cron_schedule="*/15 * * * *")


# etl_asset_job = define_asset_job(
#     name="etl_asset_job",
#     selection=AssetSelection.groups("etl"),
#     config={"ops": {"create_table": {"config": {"table_name": "fake_table"}}}},
# )
