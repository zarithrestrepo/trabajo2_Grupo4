"""Glue job rama 2: cuanto consume y cuanto cuesta cada bloque por mes, y cambio interanual."""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark import SparkContext
from pyspark.sql import Window
from pyspark.sql import functions as F

COSTO_KWH = 512.0   # supuesto de la propuesta: COP/kWh de energia comprada a la red

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'input_path', 'output_path'])
glue = GlueContext(SparkContext.getOrCreate())
spark = glue.spark_session
job = Job(glue)
job.init(args['JOB_NAME'], args)

silver = spark.read.parquet(args['input_path'])

mensual = (silver.groupBy('bloque_origen', 'anio', 'mes')
                 .agg(F.sum('kwh').alias('kwh_total'),
                      F.countDistinct('fecha').alias('dias_con_datos'))
                 .withColumn('kwh_promedio_dia', F.col('kwh_total') / F.col('dias_con_datos'))
                 .withColumn('costo_estimado_cop', F.col('kwh_total') * F.lit(COSTO_KWH)))

# Oct-2021 y oct-2026 estan incompletos: la variacion se calcula sobre el kWh promedio por dia.
# Mismo mes del anio anterior = fila previa de la ventana (por bloque y mes, ordenada por anio)
w = Window.partitionBy('bloque_origen', 'mes').orderBy('anio')
res = (mensual.withColumn('kwh_promedio_dia_anio_ant', F.lag('kwh_promedio_dia').over(w))
              .withColumn('variacion_interanual_pct',
                          F.round((F.col('kwh_promedio_dia') / F.col('kwh_promedio_dia_anio_ant') - 1) * 100, 2))
              .withColumn('kwh_total', F.round('kwh_total', 2))
              .withColumn('kwh_promedio_dia', F.round('kwh_promedio_dia', 2))
              .withColumn('kwh_promedio_dia_anio_ant', F.round('kwh_promedio_dia_anio_ant', 2))
              .withColumn('costo_estimado_cop', F.round('costo_estimado_cop', 0))
              .orderBy('bloque_origen', 'anio', 'mes'))
print(f'[RAMA2] Filas de la agrupacion: {res.count()}')      # 9 bloques x 61 meses = 549

res.coalesce(1).write.mode('overwrite').parquet(args['output_path'])

job.commit()
