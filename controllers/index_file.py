import asyncio
import threading
import time
from typing import Dict, List, Set
from concurrent.futures import ThreadPoolExecutor

import storage.zincsearch as zincsearch

BUSY_WAIT_SECONDS = 1

class AsyncBatchProcessor:
    def __init__(self, batch_size: int, timeout: float, index_name: str, num_workers: int = 4):
        self.batch_size = batch_size
        self.timeout = timeout
        self.index_name = index_name
        self.num_workers = num_workers
        self.thread_pool = ThreadPoolExecutor(max_workers=num_workers)

        # Dictionary to store batches: key -> list of records
        self.batches: Dict[int, List[dict]] = {}
        self.current_index = 0  # Points to current batch being filled
        self.raw_pending_keys: Set[int] = set()  # Pool of keys ready to be processed
        self.pending_keys: List[Set[int]] = list()  # Pool of keys ready to be processed

        self.running = True
        self.stop_event = asyncio.Event()
        self.timeout_event = asyncio.Event()
        self.starting_time = time.time()

    async def add_record(self, record: dict, is_last: bool = False):
        if self.current_index not in self.batches:
            self.batches[self.current_index] = []

        # Add record to current batch
        self.batches[self.current_index].append(record)

        # If current batch is full or it's the last record, add to pending keys and move to next index
        if len(self.batches[self.current_index]) >= self.batch_size or is_last:
            self.raw_pending_keys.add(self.current_index)
            self.current_index += 1

            if is_last:
                self.stop_event.set()

    def process_batch_threaded(self, worker_id: int, batch_index: int):
        """Thread-based batch processing"""
        start_time = time.time()
        try:
            batch = self.batches[batch_index]
            zincsearch.bulk_insert(self.index_name, batch)

            self.pending_keys[worker_id].discard(batch_index)
            del self.batches[batch_index]
        except Exception as e:
            print(f"Error processing batch {batch_index}: {str(e)}")
            raise
        finally:
            print(f"Batch {batch_index} inserted in {time.time() - start_time} seconds by worker {worker_id}")

    async def worker(self, worker_id: int):
        """Async worker that delegates batch processing to threads"""
        loop = asyncio.get_running_loop()

        while self.running:
            if self.timeout_event.is_set():
                print(f"Timeout reached, stopping worker {worker_id}")
                break

            if self.stop_event.is_set() and len(self.pending_keys)-1 < worker_id:
                break

            if self.stop_event.is_set() and not self.pending_keys[worker_id]:
                break

            start_time = time.time()

            if not self.pending_keys[worker_id]:
                await asyncio.sleep(BUSY_WAIT_SECONDS)
                continue

            batch_index = self.pending_keys[worker_id].pop()

            # Execute batch processing in thread pool
            await loop.run_in_executor(
                self.thread_pool,
                self.process_batch_threaded,
                worker_id,
                batch_index
            )

            print(f"Worker {worker_id} finished in {time.time() - start_time} seconds")

    async def start(self):
        print("original size of raw pending keys", len(self.raw_pending_keys))

        print("worker groups size", self.num_workers)

        for i in range(len(self.raw_pending_keys)):
            worker_index = i % self.num_workers

            if worker_index+1 > len(self.pending_keys):
                self.pending_keys.append(set())

            self.pending_keys[worker_index].add(self.raw_pending_keys.pop())

        if len(self.raw_pending_keys) > 0:
            self.pending_keys[self.num_workers-1].update(self.raw_pending_keys)

        print("size of raw pending keys", len(self.raw_pending_keys))
        print("final size of pending keys", len(self.pending_keys))

        workers = [
            asyncio.create_task(self.worker(i))
            for i in range(self.num_workers)
        ]

        timeout_checker = asyncio.create_task(self.check_timeout())

        await self.stop_event.wait()

        while self.batches:
            await asyncio.sleep(BUSY_WAIT_SECONDS)

        self.running = False
        await asyncio.gather(*workers)
        timeout_checker.cancel()

    async def check_timeout(self):
        print("Starting timeout checker")
        while self.running:
            current_time = time.time()

            if (current_time - self.starting_time) >= self.timeout:
                print(f"Timeout reached, stopping after {current_time - self.starting_time} seconds")
                self.timeout_event.set()
                break

            print(f"Waiting for {len(self.batches)} batches to finish", end="\r")
            await asyncio.sleep(20)

    async def stop(self):
        print("Stopping batch processor...")
        self.stop_event.set()
        await self.start()  # This will process remaining batches
        self.thread_pool.shutdown(wait=True)
        print("Batch processor stopped.")

async def insert_batch(index_name, records_batch):
    try:
        zincsearch.bulk_insert(index_name, records_batch)
    except Exception as e:
        print(f"Error uploading batch: {str(e)}")
        raise

