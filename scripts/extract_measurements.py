import sys
import json
import logging
import requests
import time
from datetime import datetime, timezone, timedelta
from google.cloud import storage
from google.auth import default

# Configuración de Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constantes
MAX_RETRIES = 3

def get_gcs_client():
    creds, _ = default()
    return storage.Client(credentials=creds)

def get_sensor_ids_from_locations_json(bucket_name, input_prefix):
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    
    blobs = list(bucket.list_blobs(prefix=input_prefix))
    
    if not blobs:
        logger.warning(f"No se encontraron archivos de entrada en: gs://{bucket_name}/{input_prefix}")
        return []

    sensor_ids = set()
    
    logger.info(f"Procesando {len(blobs)} archivos de localizaciones...")
    
    for blob in blobs:
        if not blob.name.endswith(".ndjson"):
            continue
            
        content = blob.download_as_text()
        for line in content.splitlines():
            if not line.strip(): continue
            try:
                record = json.loads(line)
                location_data = record.get("data", {})
                sensors = location_data.get("sensors", [])
                for s in sensors:
                    if "id" in s:
                        sensor_ids.add(s["id"])
            except json.JSONDecodeError:
                continue

    logger.info(f"Total de Sensores únicos encontrados: {len(sensor_ids)}")
    return list(sensor_ids)

def upload_chunk_to_gcs(bucket_name, object_path, data_list):
    if not data_list:
        return
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_path)
    ndjson_content = "\n".join([json.dumps(record, ensure_ascii=False) for record in data_list])
    blob.upload_from_string(ndjson_content, content_type="application/x-ndjson")
    logger.info(f"Chunk guardado: gs://{bucket_name}/{object_path} ({len(data_list)} registros)")

def fetch_measurements(api_url, api_key, sensor_id, date_from, date_to):
    url = f"{api_url}/sensors/{sensor_id}/measurements"
    headers = {"X-API-Key": api_key}
    
    params = {
        "limit": 1000, 
        "page": 1,
        "datetime_from": date_from,
        "datetime_to": date_to
    }
    
    all_measurements = []
    # Set para evitar duplicados exactos si la API falla en la paginación
    seen_measurement_ids = set()
    
    while True:
        try:
            response = requests.get(url, headers=headers, params=params, timeout=15)
            
            if response.status_code == 404:
                return [] 
            
            response.raise_for_status()
            data = response.json()
            results = data.get("results", [])
            
            # Filtrado de duplicados de API en caliente
            new_results = []
            for res in results:
                # Usamos una combinación única. En v3 suele haber 'period.datetime.utc'
                try:
                    # Intentamos crear una clave única temporal
                    unique_key = f"{res.get('period', {}).get('datetime', {}).get('utc')}-{res.get('value')}"
                    if unique_key not in seen_measurement_ids:
                        seen_measurement_ids.add(unique_key)
                        new_results.append(res)
                except:
                    # Si falla la estructura, lo añadimos por seguridad
                    new_results.append(res)

            all_measurements.extend(new_results)
            
            if len(results) < params["limit"]:
                break
                
            params["page"] += 1
            time.sleep(0.1) 
            
        except Exception as e:
            logger.error(f"Error extrayendo sensor {sensor_id}: {e}")
            break
            
    return all_measurements

def main(run_id, logical_date, bucket_name, input_base_dir, output_base_dir, api_url, api_key):
    # 1. Ventana de Tiempo
    dt_run = datetime.strptime(logical_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    date_to = dt_run.isoformat()
    date_from = (dt_run - timedelta(days=1)).isoformat()
    
    # Formato para la estructura de carpetas: YYYY/MM/DD
    path_date_struct = f"{dt_run.year}/{dt_run.month:02d}/{dt_run.day:02d}"
    
    # Timestamp del lote
    batch_extraction_ts = datetime.now(timezone.utc).isoformat()

    # 2. Leer IDs
    input_prefix = f"{input_base_dir}/{path_date_struct}/"
    sensor_ids = get_sensor_ids_from_locations_json(bucket_name, input_prefix)
    
    if not sensor_ids:
        logger.warning("No se encontraron sensores. Finalizando.")
        return

    # 3. Procesar
    buffer = []
    chunk_size = 2000 
    file_counter = 0
    total_processed = 0
    
    for sensor_id in sensor_ids:
        measurements = fetch_measurements(api_url, api_key, sensor_id, date_from, date_to)
        
        if measurements:
            for m in measurements:
                record = {
                    "data": m,
                    "_audit_run_id": run_id,
                    "_audit_sensor_id": sensor_id,
                    "_audit_logical_date": logical_date, # <--- ¡RECUPERADO! Vital para Partition
                    "_audit_extracted_at": batch_extraction_ts
                }
                buffer.append(record)
            
            total_processed += len(measurements)

        if len(buffer) >= chunk_size:
            file_name = f"measurements_{run_id}_part{file_counter}.ndjson"
            output_path = f"{output_base_dir}/{path_date_struct}/{file_name}"
            
            for r in buffer:
                r["_audit_gcs_filename"] = f"gs://{bucket_name}/{output_path}"
                
            upload_chunk_to_gcs(bucket_name, output_path, buffer)
            buffer = []
            file_counter += 1
        
        time.sleep(0.05) 

    # 4. Volcar final
    if buffer:
        file_name = f"measurements_{run_id}_part{file_counter}.ndjson"
        output_path = f"{output_base_dir}/{path_date_struct}/{file_name}"
        
        for r in buffer:
            r["_audit_gcs_filename"] = f"gs://{bucket_name}/{output_path}"
            
        upload_chunk_to_gcs(bucket_name, output_path, buffer)

    if total_processed == 0:
        logger.warning("No se encontraron medidas. Generando archivo vacío.")
        empty_filename = f"measurements_{run_id}_EMPTY.ndjson"
        empty_path = f"{output_base_dir}/{path_date_struct}/{empty_filename}"
        client = get_gcs_client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(empty_path)
        blob.upload_from_string("", content_type="application/x-ndjson")

    logger.info(f"Proceso finalizado. Total: {total_processed}")

if __name__ == "__main__":
    if len(sys.argv) < 8:
        sys.exit(1)
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7])