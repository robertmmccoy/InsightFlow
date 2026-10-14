import pandas as pd

from pyathena import connect

from utils.config import (
    AWS_REGION,
    ATHENA_DATABASE,
    ATHENA_WORKGROUP
)

def run_query(sql: str) -> pd.DataFrame:

    conn = connect(
    region_name=AWS_REGION,
    schema_name=ATHENA_DATABASE,
    work_group=ATHENA_WORKGROUP
    )

    return pd.read_sql(sql, conn)