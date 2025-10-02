from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("ResumenUbicaciones").getOrCreate()

# Leer Parquet
df = spark.read.parquet("hdfs:///user/alumno/salida/ubicaciones_por_dia")

# Calcular estadísticas por fecha
resumen = df.groupBy("fecha").agg(
    F.avg("ubicaciones").alias("media"),
    F.stddev("ubicaciones").alias("desviacion"),
    F.min("ubicaciones").alias("minimo"),
    F.max("ubicaciones").alias("maximo")
)

# Guardar en CSV con encabezado
resumen.write.mode("overwrite").option("header", "true").csv("hdfs:///user/alumno/salida/resumen/")



