"""Glue job inicial: limpia el consumo, lo une con medidores y escribe Parquet particionado por anio."""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark import SparkContext
from pyspark.sql import functions as F

# Esquemas explícitos: se asignan por posición, así el BOM y las tildes del encabezado no estorban
SCHEMA_CONSUMO = 'medidor string, fecha date, hora int, kwh double'
SCHEMA_MEDIDORES = ('medidor string, subestacion_origen string, bloque_origen string, '
                    'capacidad_kva_origen int, nivel_tension_kv string, bloques_atendidos string, '
                    'escala double, intensidad_pico double, tendencia_anual double')

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'consumo_path', 'medidores_path', 'output_path'])
glue = GlueContext(SparkContext.getOrCreate())
spark = glue.spark_session
job = Job(glue)
job.init(args['JOB_NAME'], args)

# ---------- 1. Lectura (CSV en S3) ----------
consumo = spark.read.csv(args['consumo_path'], header=True, schema=SCHEMA_CONSUMO)
medidores = spark.read.csv(args['medidores_path'], header=True, schema=SCHEMA_MEDIDORES)
n_leidos = consumo.count()
print(f'[LIMPIEZA] Filas leidas: {n_leidos}')

# ---------- 2. Limpieza ----------
# a) Vacios: sin medidor, fecha, hora o kWh no sirve la fila
c1 = consumo.dropna(subset=['medidor', 'fecha', 'hora', 'kwh'])
# b) Negativos: una energia activa no puede ser negativa (error de lectura)
c2 = c1.where(F.col('kwh') >= 0)
# c) Duplicados: una sola lectura por medidor, fecha y hora
c3 = c2.dropDuplicates(['medidor', 'fecha', 'hora'])
# d) Atipicos: mas de 3 veces el percentil 99 del propio medidor (picos absurdos)
p99 = c3.groupBy('medidor').agg(F.percentile_approx('kwh', 0.99).alias('p99'))
c4 = (c3.join(F.broadcast(p99), 'medidor')
        .where(F.col('kwh') <= 3 * F.col('p99'))
        .drop('p99')
        .cache())

n1, n2, n3, n4 = c1.count(), c2.count(), c3.count(), c4.count()
print(f'[LIMPIEZA] Vacios eliminados: {n_leidos - n1}')
print(f'[LIMPIEZA] Negativos eliminados: {n1 - n2}')
print(f'[LIMPIEZA] Duplicados eliminados: {n2 - n3}')
print(f'[LIMPIEZA] Atipicos eliminados: {n3 - n4}')
print(f'[LIMPIEZA] Filas limpias: {n4}')

# ---------- 3. Join con medidores (columna en comun: medidor) ----------
enriquecido = (c4.join(F.broadcast(medidores), 'medidor', 'inner')
                 .withColumn('anio', F.year('fecha'))
                 .withColumn('mes', F.month('fecha'))
                 # 1 = lunes ... 7 = domingo
                 .withColumn('dia_semana', ((F.dayofweek('fecha') + 5) % 7 + 1).cast('int')))
print(f'[JOIN] Filas tras el join: {enriquecido.count()}')

# ---------- 4. Escritura en Parquet particionado por anio ----------
(enriquecido.repartition('anio')
            .write.mode('overwrite')
            .partitionBy('anio')
            .parquet(args['output_path']))

job.commit()
