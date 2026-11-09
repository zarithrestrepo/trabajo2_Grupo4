"""
Consumidor 1 (Lambda): lee el stream, valida, descarta repetidos y calcula ventanas de 1 minuto.

Por cada evento del lote:
  - INVALIDO  -> se guarda en la tabla eventos_invalidos con el motivo
  - REPETIDO  -> se descarta (se reconoce por event_id: put condicional en eventos_vistos)
  - UNICO     -> se suma a su ventana (categoria = bloque, ventana = minuto del campo ts, UTC)
Al final del lote se actualiza ventanas_resultados (conteo, suma, promedio).

Los conteos para cuadrar salen de contar las tablas (no de contadores sueltos), y cada tabla
tiene una clave que hace la escritura idempotente: si Lambda reintenta un lote, nada se cuenta dos veces.
  eventos_vistos     (clave event_id)  -> unicos
  eventos_repetidos  (clave seq)       -> repetidos descartados
  eventos_invalidos  (clave seq)       -> invalidos descartados, con su motivo

Variables de entorno (opcionales; valores por defecto abajo):
  T_VISTOS, T_VENTANAS, T_INVALIDOS, T_REPETIDOS, KWH_MIN, KWH_MAX
"""
import base64
import json
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError

ddb = boto3.resource("dynamodb")
T_VISTOS = ddb.Table(os.environ.get("T_VISTOS", "eventos_vistos"))
T_VENTANAS = ddb.Table(os.environ.get("T_VENTANAS", "ventanas_resultados"))
T_INVALIDOS = ddb.Table(os.environ.get("T_INVALIDOS", "eventos_invalidos"))
T_REPETIDOS = ddb.Table(os.environ.get("T_REPETIDOS", "eventos_repetidos"))
KWH_MIN = float(os.environ.get("KWH_MIN", "0"))
KWH_MAX = float(os.environ.get("KWH_MAX", "1500"))
CAMPOS = ("event_id", "bloque", "kwh", "ts")


def validar(texto):
    """Devuelve (evento, None) si es valido o (None, motivo)."""
    try:
        e = json.loads(texto)
    except ValueError:
        return None, "json_invalido"
    if not isinstance(e, dict):
        return None, "json_invalido"
    for c in CAMPOS:
        if e.get(c) is None or e.get(c) == "":
            return None, f"campo_faltante:{c}"
    if isinstance(e["kwh"], bool) or not isinstance(e["kwh"], (int, float)):
        return None, "kwh_no_numerico"
    if not (KWH_MIN <= e["kwh"] <= KWH_MAX):
        return None, f"kwh_fuera_de_rango[{KWH_MIN:g},{KWH_MAX:g}]"
    try:
        ts = datetime.strptime(e["ts"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None, "ts_invalido"
    e["_ventana"] = ts.replace(second=0).strftime("%Y-%m-%dT%H:%M:00Z")
    return e, None


def handler(event, context):
    agg = defaultdict(lambda: [0, Decimal("0")])       # (bloque, ventana) -> [conteo, suma]
    unicos = repetidos = invalidos = 0

    for rec in event["Records"]:
        seq = rec["kinesis"]["sequenceNumber"]
        texto = base64.b64decode(rec["kinesis"]["data"]).decode("utf-8", errors="replace")
        e, motivo = validar(texto)

        if motivo:                                      # --- invalido
            T_INVALIDOS.put_item(Item={"seq": seq, "motivo": motivo, "evento": texto,
                                       "recibido": datetime.now(timezone.utc).isoformat()})
            invalidos += 1
            continue

        try:                                            # --- repetido o unico
            T_VISTOS.put_item(
                Item={"event_id": e["event_id"], "seq": seq,
                      "ttl": int(time.time()) + 24 * 3600},
                ConditionExpression="attribute_not_exists(event_id)",
                ReturnValuesOnConditionCheckFailure="ALL_OLD")
        except ClientError as err:
            if err.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise
            previo = err.response.get("Item", {}).get("seq", {})
            previo = previo.get("S") if isinstance(previo, dict) else previo
            if previo == seq:
                continue                                # reintento de Lambda del mismo registro: ya contado
            T_REPETIDOS.put_item(Item={"seq": seq, "event_id": e["event_id"], "seq_original": previo,
                                       "recibido": datetime.now(timezone.utc).isoformat()})
            repetidos += 1
            continue

        unicos += 1
        k = (e["bloque"], e["_ventana"])
        agg[k][0] += 1
        agg[k][1] += Decimal(str(e["kwh"]))

    for (bloque, ventana), (n, suma) in agg.items():    # --- ventanas
        r = T_VENTANAS.update_item(
            Key={"categoria": bloque, "ventana_inicio": ventana},
            UpdateExpression="ADD conteo :n, suma :s",
            ExpressionAttributeValues={":n": n, ":s": suma},
            ReturnValues="ALL_NEW")["Attributes"]
        prom = (r["suma"] / r["conteo"]).quantize(Decimal("0.01"))
        T_VENTANAS.update_item(
            Key={"categoria": bloque, "ventana_inicio": ventana},
            UpdateExpression="SET promedio = :p",
            ExpressionAttributeValues={":p": prom})

    print(f"lote: unicos={unicos} repetidos={repetidos} invalidos={invalidos} ventanas={len(agg)}")
    return {"unicos": unicos, "repetidos": repetidos, "invalidos": invalidos}
