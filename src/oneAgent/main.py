import pyspark.sql.functions as F
from pyspark.sql import DataFrame

def sample_transform(df: DataFrame) -> DataFrame:
    return (df
            .groupBy("name")
            .agg(F.countDistinct("name").alias("cnt"))
            .select("name",
                    "cnt")
            )
