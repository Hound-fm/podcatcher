import json
import time
import datetime
import numpy as np
import pandas as pd
from os import path, listdir, remove as removeFile
from config import config
from itertools import islice
from datetime import datetime
from pygments import highlight
from pygments.lexers import JsonLexer
from pygments.formatters import TerminalFormatter


def print_json(data):
    json_str = json.dumps(data, indent=4, sort_keys=True)
    print(highlight(json_str, JsonLexer(), TerminalFormatter()))


def unique_array(x):
    return list(dict.fromkeys(x))


def empty_assign_list(s):
    return [[[]] * s.sum()]


def unique_clean_list(x):
    return [x for x in set(x) if pd.notna(x)]


def safe_date(date_value):
    return (
        pd.to_datetime(date_value)
        if not pd.isna(date_value)
        else datetime(1970, 1, 1, 0, 0)
    )


def unix_time_millis():
    return int(datetime.now().timestamp() * 1000)


# Normalized "today" timestamp
def now_timestamp():
    return pd.Timestamp.utcnow().normalize()


def get_current_time():
    return datetime.now().astimezone().replace(microsecond=0).isoformat()


def load_df_cache(file_name):
    try:
        data = pd.read_json(f"{config['CACHE_DIR']}/df_{file_name}.json")
        return data
    except:
        pass
    return pd.DataFrame()


def load_json_cache(file_name):
    file_path = path.join(config["CACHE_DIR"], f"{file_name}.json")
    try:
        with open(file_path, "r") as f:
            data = json.load(f)
            return data
    except:
        pass


def remove_cache():
    try:
        # iterate over files in cache directory
        for filename in listdir(config["CACHE_DIR"]):
            f = path.join(config["CACHE_DIR"], filename)
            # Check for files to remove.
            # Ignore .gitkeep
            if path.isfile(f) and filename != ".gitkeep":
                removeFile(f)
    except:
        pass


def save_df_cache(name, df):
    df.to_json(f"{config['CACHE_DIR']}/df_{name}.json", orient="records")


def save_json_cache(file_name, json_data):
    file_path = path.join(config["CACHE_DIR"], f"{file_name}.json")
    with open(file_path, "wt") as f:
        json.dump(json_data, f, sort_keys=True, ensure_ascii=True)


def increase_delay_time(delay=0, index=1, delta=0.64):
    return delay * np.power(index, 2) * delta


def get_outpoints(df):
    return df["transaction_hash_id"] + ":" + df["vout"].astype(str)


def truncate_string(text, max=32):
    truncated = str(text)
    if len(truncated) > max:
        truncated = truncated[0 : max - 1]
        truncated += "..."
    return truncated


def get_streams_urls(df):
    df_claims = df.copy()
    df_claims = df_claims[["stream_id", "name"]]
    df_claims["permanent_url"] = (
        df_claims["name"].astype(str) + ":" + df_claims["stream_id"]
    )
    df_claims["permanent_url"] = df_claims["permanent_url"].astype(str)
    return df_claims["permanent_url"]
