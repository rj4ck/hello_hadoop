from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, countDistinct

spark = SparkSession.builder.appName("UbicacionesPorDia").getOrCreate()

# Leer CSV desde HDFS
df = spark.read.option("header", "true").csv("hdfs:///user/alumno/datos/vehiculos_gps.csv")

# Crear columna "ubicacion" combinando latitud y longitud
df = df.withColumn("ubicacion", concat_ws(",", df["latitud"], df["longitud"]))

# Agrupar por vehículo y fecha, contar ubicaciones únicas
resumen = df.groupBy("vehiculo_id", "fecha").agg(
    countDistinct("ubicacion").alias("ubicaciones")
)

# Guardar en Parquet
resumen.write.mode("overwrite").parquet("hdfs:///user/alumno/salida/ubicaciones_por_dia")
