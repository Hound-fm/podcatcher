from utils import unix_time_millis, save_json_cache


def update_status(success=False, index=0):
    # New status
    status = {
        "sync": success,
        "updated": unix_time_millis(),
        "chunk_index": index,
    }
    # Save cache data
    save_json_cache(status, "status")
