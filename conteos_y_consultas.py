"""
Cuadre de conteos y consulta a DynamoDB (Parte B).

  python3 conteos_y_consultas.py --bucket MI-BUCKET --resumen resumen_productor.json

1) Cuadre:  publicados (productor) = unicos + repetidos + invalidos (tablas de DynamoDB)
            y, como segunda comprobacion, = lineas guardadas por el consumidor crudo en S3.
2) Pregunta del proyecto (tabla ventanas_resultados):
            ¿que bloque tuvo el mayor consumo promedio por evento y cual fue su ventana pico?
"""
import argparse
import json
from collections import defaultdict
from decimal import Decimal

import boto3


def scan_todo(tabla):
    items, kw = [], {}
    while True:
        r = tabla.scan(**kw)
        items += r["Items"]
        if "LastEvaluatedKey" not in r:
            return items
        kw["ExclusiveStartKey"] = r["LastEvaluatedKey"]


def contar(tabla):
    """Cuenta los items de una tabla (Scan con Select=COUNT, paginado)."""
    total, kw = 0, {"Select": "COUNT"}
    while True:
        r = tabla.scan(**kw)
        total += r["Count"]
        if "LastEvaluatedKey" not in r:
            return total
        kw["ExclusiveStartKey"] = r["LastEvaluatedKey"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--resumen", default="resumen_productor.json")
    ap.add_argument("--publicados", type=int, default=None, help="si no se usa --resumen")
    ap.add_argument("--region", default=None)
    a = ap.parse_args()

    ddb = boto3.resource("dynamodb", region_name=a.region)
    s3 = boto3.client("s3", region_name=a.region)

    publicados = a.publicados
    if publicados is None:
        publicados = json.load(open(a.resumen))["publicados"]

    unicos = contar(ddb.Table("eventos_vistos"))
    repetidos = contar(ddb.Table("eventos_repetidos"))
    invalidos = contar(ddb.Table("eventos_invalidos"))
    ventanas = scan_todo(ddb.Table("ventanas_resultados"))
    suma_ventanas = sum(int(v["conteo"]) for v in ventanas)

    crudos = 0
    for pag in s3.get_paginator("list_objects_v2").paginate(Bucket=a.bucket, Prefix="crudos/"):
        for o in pag.get("Contents", []):
            cuerpo = s3.get_object(Bucket=a.bucket, Key=o["Key"])["Body"].read().decode("utf-8")
            crudos += sum(1 for l in cuerpo.splitlines() if l.strip())

    total = unicos + repetidos + invalidos
    print("\n=== CUADRE DE CONTEOS ===")
    print(f"Publicados (productor)                 : {publicados}")
    print(f"Unicos procesados (eventos_vistos)     : {unicos}")
    print(f"  suma de 'conteo' en las ventanas     : {suma_ventanas}")
    print(f"Repetidos descartados (eventos_repetidos): {repetidos}")
    print(f"Invalidos descartados (eventos_invalidos): {invalidos}")
    print(f"Unicos + repetidos + invalidos         : {total}")
    print(f"Eventos crudos en S3 (consumidor 2)    : {crudos}")
    ok = publicados == total == crudos and suma_ventanas == unicos
    print("RESULTADO:", "CUADRA" if ok else "NO CUADRA (revisar)")

    print("\n=== PREGUNTA: ¿que bloque consume mas por evento y cuando fue su ventana pico? ===")
    por_bloque = defaultdict(lambda: [0, Decimal("0")])
    for v in ventanas:
        por_bloque[v["categoria"]][0] += int(v["conteo"])
        por_bloque[v["categoria"]][1] += v["suma"]
    print(f"{'bloque':8}{'eventos':>9}{'kWh prom.':>11}   ventana pico (promedio del minuto)")
    for b, (n, s) in sorted(por_bloque.items(), key=lambda x: -(x[1][1] / x[1][0])):
        pico = max((v for v in ventanas if v["categoria"] == b), key=lambda v: v["promedio"])
        print(f"{b:8}{n:>9}{float(s / n):>11.2f}   {pico['ventana_inicio']}  ({pico['promedio']} kWh, {pico['conteo']} eventos)")


if __name__ == "__main__":
    main()
