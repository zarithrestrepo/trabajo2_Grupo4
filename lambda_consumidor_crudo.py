"""
Consumidor 2 (Lambda): guarda en S3 TODOS los eventos del stream tal como llegan.

Escribe un archivo JSON-lines por lote:  crudos/aaaa/mm/dd/hh/<uuid>.jsonl
Variable de entorno: BUCKET (nombre del bucket, sin s3://)
"""
import base64
import os
import uuid
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3")
BUCKET = os.environ["BUCKET"]


def handler(event, context):
    lineas = [base64.b64decode(r["kinesis"]["data"]).decode("utf-8", errors="replace")
              for r in event["Records"]]
    ahora = datetime.now(timezone.utc)
    clave = f"crudos/{ahora:%Y/%m/%d/%H}/{uuid.uuid4()}.jsonl"
    s3.put_object(Bucket=BUCKET, Key=clave, Body=("\n".join(lineas) + "\n").encode("utf-8"))
    print(f"{len(lineas)} eventos -> s3://{BUCKET}/{clave}")
    return {"eventos": len(lineas)}
