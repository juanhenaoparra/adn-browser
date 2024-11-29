from fastapi import APIRouter, UploadFile, File
import tempfile
from pathlib import Path
from datetime import datetime
from models.file import load_file
import storage.zincsearch as zincsearch
from controllers.index_file import AsyncBatchProcessor
import asyncio

MEGABYTE_SIZE = 1024 * 1024
BUFFER_SIZE = 1000

router = APIRouter(
    prefix="/api",
    tags=["indexing"]
)

@router.post("/index")
async def index_file(
    file: UploadFile = File(...),
):
    now = datetime.now()
    try:
        temp_dir = Path(tempfile.gettempdir()) / "adn_index_data"
        temp_dir.mkdir(exist_ok=True)

        original_extension = Path(file.filename).suffix
        with tempfile.NamedTemporaryFile(suffix=original_extension, dir=temp_dir) as temp_file:
            temp_file_path = Path(temp_file.name)

            # Save the file to the temporary directory
            while chunk := await file.read(MEGABYTE_SIZE*10):
                temp_file.write(chunk)

            file_index = load_file(temp_file_path)
            index_name = "vcf_index"

            mapping_data = zincsearch.create_index_mapping_from_headers(index_name, list(file_index.by_name.keys()))

            zincsearch.create_or_update_mapping(mapping_data)

            count = 0
            processor = AsyncBatchProcessor(batch_size=BUFFER_SIZE, timeout=60*120, index_name=index_name, num_workers=20)

            for variant in file_index._readable:
                count += 1
                variant_slice = str(variant).strip().split('\t')
                record = {file_index.by_index[col]: value for col, value in enumerate(variant_slice)}
                await processor.add_record(record)
                print(f"Processed {count} records", end="\r")

            print("\nWaiting for batch processor to finish")
            await processor.stop()
            print("Batch processor finished")

            return {
                "original_filename": file.filename,
                "temp_path": str(temp_file_path),
                "headers": file_index.by_name,
                "status": "completed",
                "records_processed": count
            }
    except Exception as e:
        return {"error": str(e)}
    finally:
        print(f"Total time: {datetime.now() - now}")

@router.post("/query")
async def query_index():
    """
    Endpoint to query the indexed file.
    To be implemented later.
    """
    pass