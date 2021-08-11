from utils import save_json_cache, load_json_cache

DEFAULT_STATUS = {
    "sync": False,
    "updated": 0,
    "init_sync": False,
    "chunk_index": -1,
}


class Status:
    def __init__(self):
        self.sync_status()

    def sync_status(self):
        status = load_json_cache("status")
        self.status = status or DEFAULT_STATUS

    def reset_status(self):
        self.status = DEFAULT_STATUS
        save_json_cache("status", self.status)

    def update_status(self, newStatus={}):
        self.sync_status()
        self.status = {
            **self.status,
            **newStatus,
        }
        save_json_cache("status", self.status)


main_status = Status()
