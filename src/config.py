import os
from dotenv import dotenv_values

config = {
    # Load shared development variables
    **dotenv_values(".env.default"),
    # Load sensitive variables
    **dotenv_values(".env"),
    # Override loaded values with environment variables
    **os.environ,
}

# Convert env vars to correct type
if "CHUNK_SIZE" in config:
    config["CHUNK_SIZE"] = int(config["CHUNK_SIZE"])

if "TIMEOUT_DELAY" in config:
    config["TIMEOUT_DELAY"] = int(config["TIMEOUT_DELAY"])


if "MAX_TIMEOUT_RETRY" in config:
    config["MAX_TIMEOUT_RETRY"] = int(config["MAX_TIMEOUT_RETRY"])
