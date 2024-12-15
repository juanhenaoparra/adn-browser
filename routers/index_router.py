from fastapi import APIRouter, UploadFile, File, Query, HTTPException
from typing import Optional
import tempfile
from pathlib import Path
from datetime import datetime
from models.file import load_file
import storage.zincsearch as zincsearch
from controllers.index_file import AsyncBatchProcessor
from controllers.email import Publisher
import random
from models.email import EmailRequest

MEGABYTE_SIZE = 1024 * 1024
BUFFER_SIZE = 1000

router = APIRouter(prefix="/api", tags=["indexing"])


@router.post("/index")
async def index_file(
    file: UploadFile = File(...),
):
    now = datetime.now()
    try:
        temp_dir = Path(tempfile.gettempdir()) / "adn_index_data"
        temp_dir.mkdir(exist_ok=True)

        original_extension = Path(file.filename).suffix
        with tempfile.NamedTemporaryFile(
            suffix=original_extension, dir=temp_dir
        ) as temp_file:
            temp_file_path = Path("samples/cabernetSauvignon-001.vcf")

            # Save the file to the temporary directory
            while chunk := await file.read(MEGABYTE_SIZE * 10):
                temp_file.write(chunk)

            file_index = load_file(temp_file_path)
            index_name = "vcf_index_delete_me"

            mapping_data = zincsearch.create_index_mapping_from_headers(
                index_name, list(file_index.by_name.keys())
            )

            zincsearch.create_or_update_mapping(mapping_data)

            count = 0
            processor = AsyncBatchProcessor(
                batch_size=BUFFER_SIZE,
                timeout=60 * 120,
                index_name=index_name,
                num_workers=5,
            )

            for variant in file_index._readable:
                count += 1
                variant_slice = str(variant).strip().split("\t")
                record = {
                    file_index.by_index[col]: value
                    for col, value in enumerate(variant_slice)
                }
                record["filename"] = file.filename
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
                "records_processed": count,
            }
    except Exception as e:
        return {"error": str(e)}
    finally:
        print(f"Total time: {datetime.now() - now}")


@router.post("/query")
async def query_index(
    filename: Optional[str] = Query(None, description="Filter results by filename"),
    search: Optional[str] = Query(
        None, description="Search term to match across indexed columns"
    ),
    page: int = Query(1, ge=1, description="Page number"),
    size: int = Query(10, ge=1, le=100, description="Number of records per page"),
):
    """
    Query the indexed VCF records with pagination, filename filter, and search across indexed columns.

    Args:
        filename: Optional filename to filter results
        search: Optional search term to match across indexed columns (CHROM, FILTER, INFO, FORMAT)
        page: Page number (starts from 1)
        size: Number of records per page (1-100)

    Returns:
        Dict containing search results and pagination metadata
    """
    try:
        index_name = "vcf_index"

        results = zincsearch.search_records(
            index_name=index_name,
            filename=filename,
            search_term=search,
            page=page,
            size=size,
        )

        return {
            "total": results.get("hits", {}).get("total", {}).get("value", 0),
            "page": page,
            "size": size,
            "records": results.get("hits", {}).get("hits", []),
            "took_ms": results.get("took", 0),
        }

    except Exception as e:
        return {"error": str(e)}


@router.post("/email")
async def generate_otp(email_request: EmailRequest):
    try:
        otp = "".join([str(random.randint(0, 9)) for _ in range(6)])

        message = {"email": email_request.email, "otp": otp}

        publisher = Publisher()
        publisher.publish(message)

        return {
            "message": "OTP generated and email queued successfully",
            "email": email_request.email,
            "otp": otp,
            "expires_in": 600,
        }
    except Exception as e:
        print(f"Error publishing message: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
