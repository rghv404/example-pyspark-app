def get_df_from_file(spark_session, path, format='parquet', option=None):
    if option:
        return spark_session.read.option(option, True).format(format).load(path)
    return spark_session.read.format(format).load(path)