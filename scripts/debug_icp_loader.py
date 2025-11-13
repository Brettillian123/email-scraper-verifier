import pprint

from src.config import load_icp_config

cfg = load_icp_config()
print("Type:", type(cfg).__name__)
pprint.pp(cfg)
