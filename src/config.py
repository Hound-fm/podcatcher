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
