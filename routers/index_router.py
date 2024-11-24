from fastapi import APIRouter, UploadFile, File
import tempfile
import os
from pathlib import Path
from threading import Thread
from queue import Queue

from models.file import load_file
import storage.zincsearch as zincsearch

MEGABYTE_SIZE = 1024 * 1024
BUFFER_SIZE = 10000

router = APIRouter(
    prefix="/api",
    tags=["indexing"]
)

@router.post("/index")
async def create_index(
    file: UploadFile = File(...),
):
    try:
        temp_dir = Path(tempfile.gettempdir()) / "adn_index_data"
        temp_dir.mkdir(exist_ok=True)

        original_extension = Path(file.filename).suffix
        with tempfile.NamedTemporaryFile(suffix=original_extension, dir=temp_dir) as temp_file:
            temp_file_path = Path(temp_file.name)

            while chunk := await file.read(MEGABYTE_SIZE*10):
                temp_file.write(chunk)

            file_index = load_file(temp_file_path)
            index_name = "vcf_index"

            mapping_data = zincsearch.create_index_mapping_from_headers(index_name, list(file_index.by_name.keys()))

            zincsearch.create_or_update_mapping(mapping_data)

            buffer_records = []
            upload_queue = Queue()

            def upload_worker():
                while True:
                    records_batch = upload_queue.get()
                    if records_batch is None:
                        break
                    try:
                        zincsearch.bulk_insert(index_name, records_batch)
                    except Exception as e:
                        print(f"Error uploading batch: {str(e)}")
                    upload_queue.task_done()

            upload_thread = Thread(target=upload_worker, daemon=True)
            upload_thread.start()

            for variant in file_index._readable:
                variant_slice = str(variant).split('\t')
                record = {file_index.by_index[col]: value for col, value in enumerate(variant_slice)}
                buffer_records.append(record)

                if len(buffer_records) >= BUFFER_SIZE:
                    upload_queue.put(buffer_records.copy())
                    buffer_records.clear()

            if buffer_records:
                upload_queue.put(buffer_records.copy())

            upload_queue.put(None)
            upload_thread.join()
            upload_queue.join()

        return {
            "original_filename": file.filename,
            "temp_path": str(temp_file_path),
            "headers": file_index.by_name,
            "status": "saved"
        }
    except Exception as e:
        return {"error": str(e)}

@router.post("/query")
async def query_index():
    """
    Endpoint to query the indexed file.
    To be implemented later.
    """
    pass