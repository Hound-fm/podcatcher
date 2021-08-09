from utils import unix_time_millis, save_json_cache, load_json_cache

DEFAULT_STATUS = {
    "sync": False,
    "updated": None,
    "chunk_index": -1,
}


class Status:
    def __init__(self):
        self.sync_status()

    def sync_status(self):
        status = load_json_cache("status")
        self.status = status if status else DEFAULT_STATUS

    # Methods
    def update_status(self, success=False, index=0):
        # New status
        self.status = {
            "sync": success,
            "updated": unix_time_millis(),
            "chunk_index": index,
        }
        # Save cache data
        save_json_cache(self.status, "status")

    # Actions


main_status = Status()
