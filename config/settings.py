from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # .env 文件路径和编码
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8')

    # Database Config
    DATABASE_URL: str = "sqlite:///./persistence/tushare_data.db"
    DB_PATH: str = "./persistence/tushare_data.db"

    # Tushare Config
    TUSHARE_TOKEN: str = "default_token"
    TUSHARE_ENABLED: bool = True  # 启用Tushare作为默认接口
    
    # Tinyshare Config (备用数据源)
    TINYSHARE_TOKEN: str = "default_token"

    # News / Tinyshare-Pro (独立授权码)
    NEWS_TOKEN: str = "default_token"
    NEWS_ENABLED: bool = True

    # DeepSeek Config
    DEEPSEEK_API_KEY: str = "default_key"
    DEEPSEEK_BASE_URL: str = "https://api.deepseek.com"
    DEEPSEEK_ENABLED: bool = True  # 临时启用以便测试
    
    # OpenAI Config (可以保留作为备用)
    OPENAI_API_KEY: str = "default_key"
    OPENAI_BASE_URL: str | None = None
    OPENAI_ENABLED: bool = False

# 创建一个全局可用的配置实例
settings = Settings()