import csv
from datetime import datetime
from typing import Iterator, List

from dagster import In, Nothing, Out, String, job, op, usable_as_dagster_type, Output
from pydantic import BaseModel


@usable_as_dagster_type(description="Stock data")
class Stock(BaseModel):
    date: datetime
    close: float
    volume: int
    open: float
    high: float
    low: float

    @classmethod
    def from_list(cls, input_list: List[str]):
        """Do not worry about this class method for now"""
        return cls(
            date=datetime.strptime(input_list[0], "%Y/%m/%d"),
            close=float(input_list[1]),
            volume=int(float(input_list[2])),
            open=float(input_list[3]),
            high=float(input_list[4]),
            low=float(input_list[5]),
        )


@usable_as_dagster_type(description="Aggregation of stock data")
class Aggregation(BaseModel):
    date: datetime
    high: float


def csv_helper(file_name: str) -> Iterator[Stock]:
    with open(file_name) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            yield Stock.from_list(row)


@op(
    config_schema={"s3_key": String}, 
    out=Out(is_required=False, dagster_type=List[Stock])
)
def get_s3_data(context) -> List[Stock]:
    temp_list = []
    for element in csv_helper(context.op_config["s3_key"]):
        temp_list.append(element)
    return temp_list


@op(
    ins={"data": In(dagster_type=List[Stock], description="List of stock metrics at given times.")},
    out=Out(is_required=True, dagster_type=Aggregation),
)
def process_data(context, data: List[Stock]) -> Aggregation:
    highest = data[0]
    for element in data:
        if element.high > highest.high:
            highest = element
    return Aggregation(date=highest.date, high=highest.high)

@op(
    ins={"highest_value": In(dagster_type=Aggregation, description="The datetime the highest value was seen and the value itself.")}
)
def put_redis_data(context, highest_value: Aggregation):
    pass

@job
def week_1_pipeline():
    data_from_s3 = get_s3_data()
    highest_value = process_data(data_from_s3)
    put_redis_data(highest_value)
    
