from langchain_openai import ChatOpenAI
from config.settings import settings

def get_llm():
    """
    根据 .env 配置加载并返回启用的 LLM 实例。
    优先使用 DeepSeek。
    """
    if settings.DEEPSEEK_ENABLED:
        print("--- LLM Provider: DeepSeek ---")
        return ChatOpenAI(
            model="deepseek-chat",
            api_key=settings.DEEPSEEK_API_KEY,
            base_url=settings.DEEPSEEK_BASE_URL,
            temperature=0,
            max_tokens=4096,
        )
    elif settings.OPENAI_ENABLED:
        print("--- LLM Provider: OpenAI ---")
        return ChatOpenAI(
            model="gpt-4o",
            api_key=settings.OPENAI_API_KEY,
            base_url=settings.OPENAI_BASE_URL,
            temperature=0,
        )
    else:
        raise ValueError("No LLM is enabled in the .env file. Please set DEEPSEEK_ENABLED or OPENAI_ENABLED to true.")