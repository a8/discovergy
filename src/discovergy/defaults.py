# -*- coding: utf-8 -*-

"""

Discovergy default values

"""

APP_NAME = "discoverpy"
API_HOST = "https://api.discovergy.com"
API_URL = "/public/v1"

PASSWORD_OBFUSCATION = "not saved to config file"

DEFAULT_CONFIG = """
[discovergy_account]
email: none
password: none
save_password: False

[file_location]
data_dir: "~/discovergy/data/"
log_dir: "~/discovergy/log/"

[poll]
# all values in seconds
discovergy: 43200
weather: 7200
awattar: 43200

[open_weather_map]
id: none
latitude: none
longitude: none
"""
