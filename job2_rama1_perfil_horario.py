"""Glue job rama 1: en que horas y dias de la semana se concentra la demanda de cada bloque."""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'input_path', 'output_path'])
glue = GlueContext(SparkContext.getOrCreate())
spark = glue.spark_session
job = Job(glue)
job.init(args['JOB_NAME'], args)

silver = spark.read.parquet(args['input_path'])

# kWh promedio por bloque, dia de la semana (1 = lunes ... 7 = domingo) y hora
perfil = (silver.groupBy('bloque_origen', 'dia_semana', 'hora')
                .agg(F.round(F.avg('kwh'), 2).alias('kwh_promedio'),
                     F.count('*').alias('n_registros'))
                .orderBy('bloque_origen', 'dia_semana', 'hora'))
print(f'[RAMA1] Filas de la agrupacion: {perfil.count()}')   # 9 bloques x 7 x 24 = 1512

perfil.coalesce(1).write.mode('overwrite').parquet(args['output_path'])

job.commit()
