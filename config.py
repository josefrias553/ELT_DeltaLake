from configparser import ConfigParser

config = ConfigParser()
config.read("pipeline.conf")

token = config["api_credentials"]["token"]

url = "https://api.github.com"
endpoint_issues = "repos/torvalds/linux/issues"
endpoint_commits = "repos/torvalds/linux/commits"
headers = {
    "Authorization": f"Bearer {token}",
    "Accept": "application/vnd.github+json",
    "User-Agent": "data-engineer-test"
}
params_issues = {
    "state": "all",
    "per_page": 100
}

DELTA_ISSUES_PATH = "data_lake/bronze/github/issues"
DELTA_COMMITS_PATH = "data_lake/bronze/github/commits"

DELTA_ISSUES_SILVER_PATH = DELTA_ISSUES_PATH.replace("bronze", "silver")
DELTA_COMMITS_SILVER_PATH = DELTA_COMMITS_PATH.replace("bronze", "silver")

partition_options_bronze = [
    ["created_year", "state"],
    ["ingestion_date"]
]

partition_options_silver = [
    None,
    None
]