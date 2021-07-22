import json
import datetime
import numpy as np
import pandas as pd
import os.path as path
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


def get_current_time():
    return datetime.now().astimezone().replace(microsecond=0).isoformat()


def load_df_cache(name):
    data = pd.read_csv(f"data/cache/{name}.csv")
    return data


def load_json_cache(json_data, file_name):
    file_path = path.join("data/cache", f"{file_name}.json")
    with open(file_path, "r") as f:
        data = json.load(f)
        return data


def save_df_cache(df, name):
    df.to_csv(f"data/cache/{name}.csv", index=False)


def save_json_cache(json_data, file_name):
    file_path = path.join("data/cache", f"{file_name}.json")
    with open(file_path, "wt") as f:
        json.dump(json_data, f, sort_keys=True, indent=2, ensure_ascii=True)


def increase_delay_time(delay=0, index=1, delta=0.64):
    return delay * np.power(index, 2) * delta


def get_streams_cannonical_url(df):
    df_claims = df.copy()
    df_claims = df_claims[["claim_id", "name", "channel_name", "channel_id"]]
    df_claims["claim_char"] = df_claims["claim_id"].str[0]
    df_claims["channel_char"] = df_claims["channel_id"].str.slice(0, 2)
    df_claims["cannonical_url"] = (
        +df_claims["channel_name"].astype(str)
        + "#"
        + df_claims["channel_char"].astype(str)
        + "/"
        + df_claims["name"].astype(str)
        + "#"
        + df_claims["claim_char"].astype(str)
    )
    df_claims["cannonical_url"] = df_claims["cannonical_url"].astype(str)
    return df_claims["cannonical_url"]


def get_channels_cannonical_url(df):
    df_claims = df.copy()
    df_claims = df_claims[["channel_name", "channel_id"]]
    df_claims["channel_char"] = df_claims["channel_id"].str.slice(0, 2)
    df_claims["cannonical_url"] = (
        +df_claims["channel_name"].astype(str)
        + "#"
        + df_claims["channel_char"].astype(str)
    )
    df_claims["cannonical_url"] = df_claims["cannonical_url"].astype(str)
    return df_claims["cannonical_url"]
