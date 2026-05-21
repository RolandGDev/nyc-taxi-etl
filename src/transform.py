import  pyspark.sql.functions as F

def transform_func(df):
    df_filtrado = df.filter(
        (df["trip_distance"] > 0) &
        (df["fare_amount"] > 0) &
        (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime") > 0)
    )
    df_com_duracao = df_filtrado.withColumn("duracao ", F.round((F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime"))/ 60, 2))
    df_velocidade = df_com_duracao.withColumn(
        "velocidade_media",
        F.round(
            F.col("trip_distance") / (
                    (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 3600
            ), 2
        )
    )
    return df_velocidade