-- =====================================================================
-- Athena (Query editor): ejecutar UNA consulta por vez, en este orden.
-- Cambiar TU-BUCKET por el nombre real del bucket.
-- Antes: Settings > Manage > Query result location = s3://TU-BUCKET/athena-resultados/
-- =====================================================================

-- 1) Base de datos (luego elegir "trabajo2" en el panel izquierdo)
CREATE DATABASE IF NOT EXISTS trabajo2;

-- 2) Datos transformados (Silver), particionados por anio
CREATE EXTERNAL TABLE IF NOT EXISTS trabajo2.consumo_enriquecido (
  medidor string, fecha date, hora int, kwh double,
  subestacion_origen string, bloque_origen string, capacidad_kva_origen int,
  nivel_tension_kv string, bloques_atendidos string,
  escala double, intensidad_pico double, tendencia_anual double,
  mes int, dia_semana int
)
PARTITIONED BY (anio int)
STORED AS PARQUET
LOCATION 's3://TU-BUCKET/silver/consumo_enriquecido/';

-- 3) Registrar las particiones anio=2021..2026 (sin esto la tabla sale vacia)
MSCK REPAIR TABLE trabajo2.consumo_enriquecido;

-- 4) Comprobar volumen y particion. Esperado: 6 filas que suman 5.230.220
SELECT anio, count(*) AS filas
FROM trabajo2.consumo_enriquecido
GROUP BY anio ORDER BY anio;

-- ---------- RAMA 1: perfil horario ----------
-- 5) Tabla
CREATE EXTERNAL TABLE IF NOT EXISTS trabajo2.perfil_horario_bloque (
  bloque_origen string, dia_semana int, hora int, kwh_promedio double, n_registros bigint
)
STORED AS PARQUET
LOCATION 's3://TU-BUCKET/gold/perfil_horario_bloque/';

-- 6) PREGUNTA 1: en que hora y dia de la semana (1=lunes) se concentra la demanda de cada bloque?
SELECT bloque_origen, dia_semana, hora, kwh_promedio
FROM (
  SELECT *, row_number() OVER (PARTITION BY bloque_origen ORDER BY kwh_promedio DESC) AS rk
  FROM trabajo2.perfil_horario_bloque
)
WHERE rk = 1
ORDER BY bloque_origen;

-- 7) Complemento: perfil de 24 horas de un miercoles (3) para un bloque
SELECT hora, kwh_promedio
FROM trabajo2.perfil_horario_bloque
WHERE bloque_origen = '20' AND dia_semana = 3
ORDER BY hora;

-- ---------- RAMA 2: consumo y costo mensual ----------
-- 8) Tabla
CREATE EXTERNAL TABLE IF NOT EXISTS trabajo2.consumo_costo_mensual (
  bloque_origen string, anio int, mes int, kwh_total double, dias_con_datos bigint,
  kwh_promedio_dia double, costo_estimado_cop double,
  kwh_promedio_dia_anio_ant double, variacion_interanual_pct double
)
STORED AS PARQUET
LOCATION 's3://TU-BUCKET/gold/consumo_costo_mensual/';

-- 9) PREGUNTA 2: cuanto consume y cuesta cada bloque por anio? (solo anios completos 2022-2025)
--    Esperado: 36 filas (9 bloques x 4 anios)
SELECT bloque_origen, anio,
       round(sum(kwh_total))          AS kwh_anual,
       round(sum(costo_estimado_cop)) AS costo_cop_anual
FROM trabajo2.consumo_costo_mensual
WHERE anio BETWEEN 2022 AND 2025
GROUP BY bloque_origen, anio
ORDER BY bloque_origen, anio;

-- 10) Detalle mensual con variacion interanual de un bloque (61 filas)
SELECT anio, mes, kwh_total, costo_estimado_cop, variacion_interanual_pct
FROM trabajo2.consumo_costo_mensual
WHERE bloque_origen = '20'
ORDER BY anio, mes;

-- 11) Hallazgo: el bloque 18 baja ~11 % en 2025 (ver guia)
SELECT anio, mes, kwh_promedio_dia, variacion_interanual_pct
FROM trabajo2.consumo_costo_mensual
WHERE bloque_origen = '18'
ORDER BY anio, mes;
