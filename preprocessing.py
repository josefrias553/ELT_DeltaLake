from deltalake import DeltaTable
import pandas as pd
import os
from config import DELTA_ISSUES_PATH, DELTA_COMMITS_PATH

def process_commits(path):
    if not os.path.exists(DELTA_COMMITS_PATH) or not os.listdir(DELTA_COMMITS_PATH):
        print(f"[INFO] No existe la tabla Delta en {path}. Se omite el procesamiento de commits.\n")
        return None

    print("Processing commits...")
    try:
        dt_commits = DeltaTable(path)
        df_commits = dt_commits.to_pandas()

        df_commits["sha"] = df_commits["sha"].astype("string")
        df_commits["author_name"] = df_commits["author_name"].astype("string")
        df_commits["author_email"] = df_commits["author_email"].astype("string")
        df_commits["committer.login"] = df_commits["committer.login"].astype("string")
        df_commits["message"] = df_commits["message"].astype("string")

        df_commits["commit.comment_count"] = df_commits["commit.comment_count"].astype("int16")
        df_commits["commit.verification.verified"] = df_commits["commit.verification.verified"].astype("boolean")

        df_commits["ingestion_date"] = pd.to_datetime(df_commits["ingestion_date"], utc=True)
        df_commits["created_date"] = pd.to_datetime(df_commits["created_date"], utc=True)

        df_commits.rename(columns={
            "sha": "commit_sha",
            "committer.login": "committer_login",
            "message": "commit_message",
            "commit.comment_count": "comment_count",
            "commit.verification.verified": "is_verified",
            "ingestion_date": "ingestion_timestamp",
            "created_date": "commit_created_at"
        }, inplace=True)

        commits_por_autor = df_commits.groupby("author_name").size()
        df_commits["commits_por_autor"] = df_commits["author_name"].map(commits_por_autor)

        df_commits["mes"] = df_commits["commit_created_at"].dt.tz_localize(None).dt.to_period("M")
        verificados_por_mes = df_commits.groupby("mes")["is_verified"].mean() * 100
        df_commits["ratio_verificados_mes"] = df_commits["mes"].map(verificados_por_mes)

        print("Finished processing commits...\n")
        return df_commits

    except Exception as e:
        print(f"[ERROR] Falló al leer la tabla Delta en {path}.\n{str(e)}")
        return None

def process_issues(path):
    if not os.path.exists(DELTA_ISSUES_PATH) or not os.listdir(DELTA_ISSUES_PATH):
        print(f"[INFO] No existe la tabla Delta en {path}. Se omite el procesamiento de commits.")
        return None

    print("Processing issues...")
    try:
        dt_issues = DeltaTable(path)
        df_issues = dt_issues.to_pandas()

        df_issues["id"] = df_issues["id"].astype("int32")
        df_issues["number"] = df_issues["number"].astype("int32")
        df_issues["comments"] = df_issues["comments"].astype("int16")

        df_issues["title"] = df_issues["title"].astype("string")
        df_issues["body"] = df_issues["body"].astype("string")
        df_issues["state"] = df_issues["state"].astype("category")
        df_issues["author_association"] = df_issues["author_association"].astype("category")
        df_issues["user.login"] = df_issues["user.login"].astype("string")
        df_issues["timeline_url"] = df_issues["timeline_url"].astype("string")

        df_issues["created_at"] = pd.to_datetime(df_issues["created_at"], utc=True)
        df_issues["updated_at"] = pd.to_datetime(df_issues["updated_at"], utc=True)
        df_issues["closed_at"] = pd.to_datetime(df_issues["closed_at"], utc=True)

        df_issues["created_year"] = df_issues["created_year"].astype("int16")

        df_issues["reactions.total_count"] = df_issues["reactions.total_count"].astype("int16")
        df_issues["reactions.+1"] = df_issues["reactions.+1"].astype("int16")
        df_issues["reactions.heart"] = df_issues["reactions.heart"].astype("int16")

        df_issues.rename(columns={
            "id": "issue_id",
            "number": "issue_number",
            "title": "issue_title",
            "body": "issue_body",
            "state": "issue_state",
            "author_association": "author_role",
            "user.login": "user_login",
            "comments": "comment_count",
            "reactions.total_count": "reactions_total",
            "reactions.+1": "reactions_plus1",
            "reactions.heart": "reactions_heart"
        }, inplace=True)

        df_issues["resolucion_dias"] = (df_issues["closed_at"] - df_issues["created_at"]).dt.days
        df_issues["resolucion_dias"] = df_issues["resolucion_dias"].fillna(-1)

        X = 10
        df_issues["es_popular"] = df_issues["reactions_total"] > X

        print("Finished processing issues...\n")
        return df_issues

    except Exception as e:
        print(f"[ERROR] Falló al leer la tabla Delta en {path}.\n{str(e)}")
        return None