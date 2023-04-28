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
    StaticPartitionsDefinition
)
from workspaces.types import Aggregation, Stock
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.config import REDIS, S3


csv_partitions = StaticPartitionsDefinition(
    [str(n) for n in range(1, 11)]
)

@asset(
    required_resource_keys={"s3"},
    description="Get a list of stocks from an S3 file",
    op_tags={"kind": "s3"},
    partitions_def=csv_partitions
)
def get_s3_data(context: OpExecutionContext) -> List[Stock]: # Is OpExecutionContext type correct?
    # key_name = context.op_config["s3_key"]
    key_name = context.asset_partition_key_for_output() # If I add this, then the test fails at line 63 
    # with build_op_context(op_config={"s3_key": "data/stock.csv"}, resources={"s3": s3_mock}) as context:
    #.  get_s3_data(context)
    # 
    context.log.info(f"Key Name = {key_name}")

    s3_data = context.resources.s3.get_data(key_name)
    context.log.info(f"S3 Data {s3_data}")

    stocks: List[Stock] = []
    for s in s3_data:
        stocks.append(Stock.from_list(s))
    
    context.log.info(f"Stocks = {stocks}")
    return stocks


@asset(
    op_tags={"kind": "s3"},
    description="Given a list of stocks return the Aggregation",
)
def process_data(context: OpExecutionContext, get_s3_data: List[Stock]) -> Aggregation:
    highest_val_stock = get_s3_data[0]
    for stock in get_s3_data:
        if highest_val_stock.high <= stock.high:
            highest_val_stock = stock
    return Aggregation(date=highest_val_stock.date, high=highest_val_stock.high)


@asset(
    required_resource_keys={"redis"},
    description="Upload an Aggregation to Redis",
    op_tags={"kind": "redis"},
)
def put_redis_data(context: OpExecutionContext, process_data: Aggregation):
    context.resources.redis.put_data(name=str(process_data.date), value=str(process_data.high))


@asset(
    required_resource_keys={"s3"},
    description="Upload an aggregation to S3",
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



def docker_config():
        return {
        **docker,
        "ops": {"get_s3_data": {"config": {"s3_key": f"prefix/stock_8.csv"}}},
    }


# # @static_partitioned_config(partition_keys=[str(n) for n in range(1, 11)])
# def docker_config(partition_key: str):
#     return {
#         **docker,
#         "ops": {"get_s3_data": {"config": {"s3_key": f"prefix/stock_{partition_key}.csv"}}},
#     }

machine_learning_asset_job = define_asset_job(
    name="machine_learning_asset_job",
    selection=project_assets,
    config=docker_config,
)

machine_learning_schedule = ScheduleDefinition(
    job=machine_learning_asset_job, 
    cron_schedule="*/1 * * * *"
)
    