import os
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DATABASE_URL: str = os.getenv("DATABASE_URL", os.getenv("POSTGRES_URL", ""))
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    POLL_SECONDS: int = int(os.getenv("POLL_SECONDS", "20"))
    SUBREDDITS: str | None = os.getenv("SUBREDDITS")
    FOURCHAN_BOARDS: str = os.getenv("FOURCHAN_BOARDS", "biz")
    SERVICE_NAME: str = os.getenv("SERVICE_NAME", "service")

    # Reddit API (script-type app)
    REDDIT_CLIENT_ID: str | None = os.getenv("REDDIT_CLIENT_ID", "yIqlOj0uUWIZAUT4iYMRXQ")
    REDDIT_CLIENT_SECRET: str | None = os.getenv("REDDIT_CLIENT_SECRET", "")
    REDDIT_REFRESH_TOKEN: str | None = os.getenv("REDDIT_REFRESH_TOKEN", "172404832418064-LfzMzmif1AJvMQIXTDFMxaTlTKL6Sg")
    REDDIT_ACCESS_TOKEN: str | None = os.getenv("REDDIT_ACCESS_TOKEN", "eyJhbGciOiJSUzI1NiIsImtpZCI6IlNIQTI1NjpzS3dsMnlsV0VtMjVmcXhwTU40cWY4MXE2OWFFdWFyMnpLMUdhVGxjdWNZIiwidHlwIjoiSldUIn0.eyJzdWIiOiJ1c2VyIiwiZXhwIjoxNzU1MDU0OTg0Ljc1ODg3MywiaWF0IjoxNzU0OTY4NTg0Ljc1ODg3MywianRpIjoiNXhRY2NUZFJMVFdYTXcxMnJvOGFoczItVjNFX3hBIiwiY2lkIjoieUlxbE9qMHVVV0laQVVUNGlZTVJYUSIsImxpZCI6InQyXzFwNDFvcHV6bzAiLCJhaWQiOiJ0Ml8xcDQxb3B1em8wIiwibGNhIjoxNzQ2OTIzNzk3MjYwLCJzY3AiOiJlSnhFamtGdXhUQUlST19DT2plcXVzQUc1NlBhSVFLY0tyZXZxUFBiM1dnR1p0NEhWR01pQ1ljTmhsTFZJMHpLRExYSEdTajlMMnV5d3dZLWkxZVR3cW5EWm8xcFRCNTM1M3k2TkRMNWxpOWhrc2puMjJmNTNfRlp4dktWdXU1TG5PcHZpTlpSTE9fdzR1Vm92UGdYeUJnSk5qaE5MZ3dlN0k0N3ItQlV5MDRoUGtMaWhnMjZYRHp3d0QxYnNGYWR4N01haHExSmZTaWYwamVzVXBwTE9mY0dHN3pFUXkwN0Y5dm5Ud0FBQVBfX09TQnNodyJ9.Ru6jCziujzcwxsnF-KW3TeowNfRhEAxMuNEMEbmw0e8-IQtluwWjrdGMrbkku9fgh1yVJgpLzh8ZhhsthHRUdLcB_ve5q3QgStKNR8hr6-CvITC3EAy4VRM-LXjPe7UILfMWzIDjtusM-tNwwAKDuGHH9fTAnGuQ4s6fhI0d99ztTzARG5gObrnlPaITjmA3Sxoi1oJgdxYc5GWXOONUgMV651a9AQzGKSNVXH_680WESLOvp9Glq_pGnau2zQoqLvJ1jJ14q1u6lTgo7AUj209iSG_HnORr5NeGpEwqCNbC-jKW7x5tbG0MeWMNk_3FGX-EyW5nNCYQ_VNALzBAkA")
    REDDIT_USER_AGENT: str = os.getenv("REDDIT_USER_AGENT", "anthill-scraper/0.1")

    # Path to YAML file with a list of subreddits for the service
    SUBREDDITS_CONFIG_PATH: str = os.getenv("SUBREDDITS_CONFIG_PATH", "config/reddit1.subreddits.yml")

    # OpenAI
    OPENAI_API_KEY: str | None = os.getenv("OPENAI_API_KEY", "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")
    OPENAI_MODEL: str = os.getenv("OPENAI_MODEL", "gpt-3.5-turbo")

    class Config:
        env_file = "anthill.env"

