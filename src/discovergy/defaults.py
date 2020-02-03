# -*- coding: utf-8 -*-

"""

Discovergy default values

"""

PASSWORD_OBFUSCATION = "not saved to config file"

DEFAULT_CONFIG = """
[discovergy_account]
email: none
password: none
save_password: False

[auth_token]
token : none

[file_location]
data_dir: "~/discovergy/data/"
log_dir: "~/discovergy/log/"

[poll]
default = 60
try_sleep = 1200

[open_weather_map]
id: none
"""
