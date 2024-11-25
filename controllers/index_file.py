from queue import Queue
import time
import threading

import storage.zincsearch as zincsearch

class BatchProcessor:
    def __init__(self, batch_size, timeout, index_name):
        self.queue = Queue()
        self.batch_size = batch_size
        self.timeout = timeout
        self.lock = threading.Lock()
        self.current_batch = []
        self.last_process_time = time.time()
        self.running = True
        self.index_name = index_name
        self.stop_event = threading.Event()

        self.worker_thread = threading.Thread(target=self.process_batches, daemon=True)
        self.worker_thread.start()

    def add_record(self, record):
        with self.lock:
            self.current_batch.append(record)

    def process_batch(self):
        self.last_process_time = time.time()

        if len(self.current_batch) == 0:
            return

        batch_to_process = []

        if len(self.current_batch) >= self.batch_size:
            batch_to_process = self.current_batch[:self.batch_size]
            self.current_batch = self.current_batch[self.batch_size:]
        else:
            batch_to_process = self.current_batch
            self.current_batch = []

        insert_batch(self.index_name, batch_to_process)

    def print_remaining_batch(self):
        def print_message():
            while self.running:
                message = f"Remaining batch [{len(self.current_batch)}]"
                print(message, end="\r")
                time.sleep(5)

        threading.Thread(target=print_message, daemon=True).start()

    def process_batches(self):
        while self.running:
            with self.lock:
                if self.stop_event.is_set():
                    print(f"Processing remaining batch before shutdown [{len(self.current_batch)}]")

                    for _ in range(0, len(self.current_batch), self.batch_size):
                        self.process_batch()
                        time.sleep(0.1)  # Avoid busy waiting

                    break

                if len(self.current_batch) >= self.batch_size or ((time.time() - self.last_process_time) >= self.timeout):
                    self.process_batch()

            time.sleep(0.1)  # Avoid busy waiting

    def stop(self):
        print("Stopping batch processor...")
        self.stop_event.set()
        self.print_remaining_batch()
        self.worker_thread.join()
        print("Batch processor stopped.")


def insert_batch(index_name, records_batch):
    try:
        zincsearch.bulk_insert(index_name, records_batch)
    except Exception as e:
        print(f"Error uploading batch: {str(e)}")
        raise

