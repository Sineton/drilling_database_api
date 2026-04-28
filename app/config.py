"""
Configuration settings for the API
"""
from pydantic_settings import BaseSettings
from functools import lru_cache
from typing import Optional


class Settings(BaseSettings):
    """Application settings"""
    
    # Database
    database_url: str = "postgresql://postgres:postgres@localhost:5432/postgres"
    
    # API
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_debug: bool = True
    api_title: str = "ИС ПОАС API"
    api_version: str = "1.0.0"
    api_prefix: str = "/api/v1"
    
    # Import settings
    import_batch_size: int = 10000
    import_max_parallel_jobs: int = 4
    
    # Data directory
    data_dir: str = "./data/temp_extract"
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance"""
    return Settings()


settings = get_settings()
