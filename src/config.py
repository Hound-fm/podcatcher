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
if "DEFAULT_CHUNK_SIZE" in config:
    config["DEFAULT_CHUNK_SIZE"] = int(config["DEFAULT_CHUNK_SIZE"])

if "DEFAULT_TIMEOUT_DELAY" in config:
    config["DEFAULT_TIMEOUT_DELAY"] = int(config["DEFAULT_TIMEOUT_DELAY"])
