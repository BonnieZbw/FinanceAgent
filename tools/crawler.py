import logging
import asyncio
from typing import Optional, List
from datetime import datetime, timedelta
from urllib.parse import quote_plus
from crawl4ai import AsyncWebCrawler
from config.logging_config import get_logger
from core.data_processor import DataProcessor

import re
import json
from urllib.parse import urlparse

# --- Config loading imports ---
import os
from typing import Dict
try:
    import yaml  # optional; used for external config overrides
except Exception:  # pragma: no cover
    yaml = None


logger = get_logger(__name__)

# --- Helpers: search parsing / url filter / relative time fallback ---
_link_line_re = re.compile(r"^[#\-\*\s]*\[([^\]]{3,200})\]\((https?://[^\)\s]+)\)")
_tail_src_re = re.compile(r"\|\s*([\u4e00-\u9fa5A-Za-z0-9_.·\-]{2,20})\s*(?:\||$)")

def _is_valid_url(url: str) -> bool:
    if not url:
        return False
    if url.startswith("javascript:"):
        return False
    bad_parts = [
        "bing.com/rebates", "bing.com/copilotsearch", "bing.com/maps", "bing.com/shop",
        "bing.com/travel", "bing.com/videos", "bing.com/images", "/rebates/", "/payouts",
        "form=PTFTNR",
    ]
    if any(bp in url for bp in bad_parts):
        return False
    return url.startswith("http")


def _parse_search_markdown(md: str) -> List[Dict[str, str]]:
    """从搜索页 markdown 提取若干条 (title, url, snippet, source_raw)。
    兼容 Bing News / 百度新闻的常见格式：形如 `- [标题](URL)` 或 `## [标题](URL)`。
    """
    out: List[Dict[str, str]] = []
    if not md:
        return out
    lines = [ln.strip() for ln in md.splitlines() if ln.strip()]
    for i, ln in enumerate(lines):
        m = _link_line_re.search(ln)
        if not m:
            continue
        title = (m.group(1) or "").strip()
        url = (m.group(2) or "").strip()
        if not _is_valid_url(url):
            continue
        # 摘要：取下一行的纯文本
        snippet = ""
        if i + 1 < len(lines):
            nxt = lines[i + 1]
            if ("http" not in nxt) and len(nxt) > 10:
                snippet = nxt[:240]
        # 来源：尝试从行尾 `| 媒体 | 时间` 抓一个媒体名
        src_raw = ""
        tail = ln[m.end():]
        mt = _tail_src_re.search(tail)
        if mt:
            src_raw = mt.group(1).strip()
        out.append({
            "title": title,
            "url": url,
            "snippet": snippet,
            "source_raw": src_raw,
        })
    return out

_rel_min_re = re.compile(r"(\d+)\s*分钟[前]?")
_rel_hour_re = re.compile(r"(\d+)\s*小时[前]?")
_rel_day_re = re.compile(r"(\d+)\s*天[前]?")
_rel_month_re = re.compile(r"(\d+)\s*个月[前]?")

def _infer_relative_time(text: str, ref_dt: datetime) -> Optional[str]:
    """从如‘2 小时前/28 天’等相对时间片段推断绝对时间（北京时间）。"""
    if not text:
        return None
    try:
        base = ref_dt.astimezone(CHINA_TZ)
    except Exception:
        base = datetime.now(tz=CHINA_TZ)
    m = _rel_min_re.search(text)
    if m and m.group(1):
        dt = base - timedelta(minutes=int(m.group(1)))
        return dt.strftime("%Y-%m-%d %H:%M")
    m = _rel_hour_re.search(text)
    if m and m.group(1):
        dt = base - timedelta(hours=int(m.group(1)))
        return dt.strftime("%Y-%m-%d %H:%M")
    m = _rel_day_re.search(text)
    if m and m.group(1):
        dt = base - timedelta(days=int(m.group(1)))
        return dt.strftime("%Y-%m-%d %H:%M")
    m = _rel_month_re.search(text)
    if m and m.group(1):
        # 月份按30天近似
        dt = base - timedelta(days=30*int(m.group(1)))
        return dt.strftime("%Y-%m-%d %H:%M")
    return None

_url_dt_dash_re = re.compile(r"/(20\d{2})[\-/](\d{1,2})[\-/](\d{1,2})(?:/|\b)")
_url_dt_compact_re = re.compile(r"/(20\d{2})(\d{2})(\d{2})(?:/|\b)")

def _parse_dt_from_url(url: str) -> Optional[str]:
    """从URL中提取日期（如 /2025/08/12/ 或 /20250812/），返回 'YYYY-MM-DD 00:00'（北京时间）。"""
    if not url:
        return None
    try:
        m = _url_dt_dash_re.search(url)
        if m:
            y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
            return datetime(y, mo, d, 0, 0, tzinfo=CHINA_TZ).strftime("%Y-%m-%d %H:%M")
        m = _url_dt_compact_re.search(url)
        if m:
            y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
            return datetime(y, mo, d, 0, 0, tzinfo=CHINA_TZ).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return None
    return None

# --- URL normalization for problematic article domains ---
_dy163_re = re.compile(r"^https?://dy\.163\.com/article/([A-Za-z0-9]+)\.html")

def _normalize_article_url(u: str) -> str:
    """对已知易出错的文章域做URL重写。当前支持：dy.163.com → www.163.com。
    例如：https://dy.163.com/article/G1PJLCNG051986N4.html
      →   https://www.163.com/dy/article/G1PJLCNG051986N4.html
    """
    if not u:
        return u
    try:
        m = _dy163_re.match(u)
        if m:
            return f"https://www.163.com/dy/article/{m.group(1)}.html"
    except Exception:
        pass
    return u

# ----------------- Custom helpers for cleaning and filtering Chinese text -----------------
_cjk_re = re.compile(r"[\u4e00-\u9fff]")

def _clean_page_text(md_or_text: str) -> str:
    """仅保留正文：去图片与无用链接；保留锚文本。
    处理顺序：去HTML标签→去markdown图片→将markdown链接替换为纯文本→去裸URL→压缩空白。
    """
    if not md_or_text:
        return ""
    t = md_or_text
    # 去HTML标签（保险起见，再清一次）
    t = re.sub(r"<[^>]+>", " ", t)
    # 去markdown图片 ![alt](url)
    t = re.sub(r"!\[[^\]]*\]\([^\)]+\)", " ", t)
    # markdown 链接 [text](url) → text
    t = re.sub(r"\[([^\]]+)\]\((?:https?://[^\)]+)\)", r"\1", t)
    # 去裸URL
    t = re.sub(r"https?://\S+", " ", t)
    # 去多余空白
    t = re.sub(r"\s{2,}", " ", t).strip()
    return t

def _has_enough_chinese(text: str) -> bool:
    if not text:
        return False
    total = len(text)
    cjk = len(_cjk_re.findall(text))
    if cjk < 30:
        return False
    return (cjk / max(total, 1)) >= 0.05

# -----------------------------
# Config (default) + hot reload
# -----------------------------
DEFAULT_NEWS_CFG: Dict = {
    "news_window_days": 3,
    "news_topk": 10,
    "source_weights": {
        "上海证券报": 1.2, "证券时报": 1.2, "中国证券报": 1.2,
        "上证报": 1.2, "中国证监会": 1.3, "交易所": 1.25,
        "深圳证券交易所": 1.25, "上海证券交易所": 1.25,
        "财联社": 1.15, "券商中国": 1.1, "同花顺": 1.05, "东方财富": 1.05,
    },
    "domain_weights": {
        "cs.com.cn": 1.2, "cnstock.com": 1.2, "csrc.gov.cn": 1.3,
        "sse.com.cn": 1.25, "szse.cn": 1.25, "cls.cn": 1.15,
        "10jqka.com.cn": 1.05, "eastmoney.com": 1.05,
    },
    "source_aliases": {
        "上证报": "上海证券报", "上海证券报": "上海证券报", "中国证券网": "上海证券报",
        "证券时报": "证券时报", "证券时报网": "证券时报",
        "中国证券报": "中国证券报", "中证网": "中国证券报",
        "东方财富": "东方财富", "东方财富网": "东方财富",
        "同花顺": "同花顺", "同花顺财经": "同花顺",
        "财联社": "财联社", "CLS": "财联社", "券商中国": "券商中国",
        "证券日报": "证券日报",
        "上交所": "上海证券交易所", "上海证券交易所": "上海证券交易所",
        "深交所": "深圳证券交易所", "深圳证券交易所": "深圳证券交易所",
        "证监会": "中国证监会", "中国证监会": "中国证监会",
    },
    "domain_aliases": {
        "cnstock.com": "上海证券报", "cs.com.cn": "证券时报",
        "csrc.gov.cn": "中国证监会", "sse.com.cn": "上海证券交易所",
        "szse.cn": "深圳证券交易所", "eastmoney.com": "东方财富",
        "10jqka.com.cn": "同花顺", "cls.cn": "财联社",
        "people.cn": "人民网", "xinhuanet.com": "新华社",
    },
    "pos_words": [
        "增持", "回购", "超预期", "上调", "利好", "签约", "中标", "获批", "突破", "增长",
        "创新高", "涨停", "提价", "盈利改善", "产能扩张", "政策支持", "订单充足"
    ],
    "neg_words": [
        "减持", "限售解禁", "下调", "利空", "亏损", "违规", "问询函", "处罚", "被调查",
        "下滑", "爆雷", "停牌", "诉讼", "资产减值", "延期", "产线停工", "业绩预亏"
    ],
    "neu_words": ["发布", "公告", "披露", "召开", "回复", "说明", "说明会"],
    "priority_keywords": [
        "公告", "停复牌", "停牌", "复牌", "问询函", "回购", "减持", "增持", "限售解禁",
        "监管", "处罚", "核查", "业绩预告", "业绩快报", "中报", "年报", "分红", "配股", "定增",
        "并购", "重组"
    ],
}

_CFG_CACHE = {
    "path": None,  # resolved once
    "mtime": None,
    "cfg": DEFAULT_NEWS_CFG,
    "priority_re": re.compile("|".join(map(re.escape, DEFAULT_NEWS_CFG["priority_keywords"]))),
}


def _resolve_cfg_path() -> str:
    # Prefer env NEWS_CFG_PATH; fallback to ../config/news_config.yml
    env_path = os.getenv("NEWS_CFG_PATH")
    if env_path and os.path.exists(env_path):
        return env_path
    base = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "config"))
    return os.path.join(base, "news_config.yml")


def _deep_update(dst: Dict, src: Dict) -> Dict:
    for k, v in (src or {}).items():
        if isinstance(v, dict) and isinstance(dst.get(k), dict):
            _deep_update(dst[k], v)
        else:
            dst[k] = v
    return dst


def _load_yaml(path: str) -> Dict:
    if not yaml or not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
            if not isinstance(data, dict):
                return {}
            return data
    except Exception:
        return {}


def get_news_config() -> Dict:
    """Load config with hot reload. External YAML (if present) overrides defaults."""
    path = _CFG_CACHE["path"] or _resolve_cfg_path()
    _CFG_CACHE["path"] = path
    try:
        mtime = os.path.getmtime(path)
    except Exception:
        mtime = None
    if _CFG_CACHE["mtime"] != mtime:
        # reload
        cfg = DEFAULT_NEWS_CFG.copy()
        cfg = _deep_update(cfg, _load_yaml(path))
        # rebuild regex
        priority = cfg.get("priority_keywords", [])
        _CFG_CACHE["priority_re"] = re.compile("|".join(map(re.escape, priority))) if priority else re.compile("$")
        _CFG_CACHE["cfg"] = cfg
        _CFG_CACHE["mtime"] = mtime
    return _CFG_CACHE["cfg"]


def get_priority_regex() -> re.Pattern:
    get_news_config()  # ensure fresh
    return _CFG_CACHE["priority_re"]


# --- Industry/macro helpers ---
def expand_industry_keywords(raw: List[str]) -> List[str]:
    """Expand industry keywords.
    1) 优先读取 YAML 的 industry_upper_map
    2) 若未命中且允许，则通过 LLM 生成上位词（可在 YAML 打开 industry_upper_llm_enabled）
    """
    cfg = get_news_config()
    upper_map = cfg.get("industry_upper_map", {}) or {}
    use_llm = bool(cfg.get("industry_upper_llm_enabled", False))
    out: List[str] = []
    seen = set()

    def _emit(term: str):
        if term and term not in seen:
            out.append(term)
            seen.add(term)

    for k in raw or []:
        if not k:
            continue
        _emit(k)
        uppers = upper_map.get(k)
        if not uppers and use_llm:
            uppers = _llm_expand_industry_terms(k, cfg)
        for u in (uppers or []):
            _emit(u)
    return out

# --- LLM 上位词兜底 ---
def _llm_expand_industry_terms(term: str, cfg: Dict) -> List[str]:
    dp = get_data_processor()
    return dp.propose_upper_industry_terms(term, cfg.get("industry_upper_llm_prompt"))

_title_strip_re = re.compile(r"[\s\-_|【】\[\]（）()：:，,。.!！?？]+")
_digits_re = re.compile(r"\d{2,}")

def _canonical_event_key(title: str) -> str:
    """Normalize title to a coarse event key: lowercase, strip punctuation/spaces, prune long digits.
    This groups multi-source coverage of the same event.
    """
    if not title:
        return ""
    t = title.strip().lower()
    t = _digits_re.sub("", t)
    t = _title_strip_re.sub(" ", t)
    t = re.sub(r"\s+", " ", t)
    # Trim generic suffix words that often vary per outlet
    t = t.replace("快讯", "").replace("最新", "").strip()
    return t

def _contains_keywords(text: str, words: List[str]) -> bool:
    t = text or ""
    return any(w for w in (words or []) if w and w in t)


# --- Functions using config ---
def _simple_cn_sentiment(text: str) -> str:
    cfg = get_news_config()
    pos_words = cfg.get("pos_words", [])
    neg_words = cfg.get("neg_words", [])
    t = text or ""
    pos = any(w in t for w in pos_words)
    neg = any(w in t for w in neg_words)
    if pos and not neg:
        return "正面"
    if neg and not pos:
        return "负面"
    return "中性"

def normalize_source_name(source: str, url: str) -> str:
    s = (source or "").strip()
    cfg = get_news_config()
    domain_aliases = cfg.get("domain_aliases", {})
    source_aliases = cfg.get("source_aliases", {})
    # 1) 域名优先
    try:
        if url:
            netloc = urlparse(url).netloc or ""
            domain = netloc.split(":")[0]
            for d, canon in domain_aliases.items():
                if d in domain:
                    return canon
    except Exception:
        pass
    # 2) 文本别名
    for alias, canon in source_aliases.items():
        if alias and alias in s:
            return canon
    return s or ""

def _source_weight(source: str, url: str) -> float:
    cfg = get_news_config()
    source_weights = cfg.get("source_weights", {})
    domain_weights = cfg.get("domain_weights", {})
    w = 1.0
    norm = normalize_source_name(source, url)
    if norm:
        for k, v in source_weights.items():
            if k in norm:
                w = max(w, v)
    if url:
        try:
            netloc = urlparse(url).netloc or ""
            domain = netloc.split(":")[0]
            for k, v in domain_weights.items():
                if k in domain:
                    w = max(w, v)
        except Exception:
            pass
    return w

async def _fetch_with_retry(crawler, url: str, extraction_schema: dict, sem: asyncio.Semaphore, max_retries: int = 3) -> object:
    delay = 0.6
    url = _normalize_article_url(url)
    for i in range(max_retries):
        async with sem:
            try:
                res = await crawler.arun(
                    url=url,
                    word_count_threshold=10,
                    extraction_strategy="LLMExtractionStrategy",
                    extraction_strategy_args={"extraction_schema": extraction_schema},
                    bypass_cache=True,
                )
                return res
            except RuntimeError as e:
                msg = str(e)
                if "ACS-GOTO" in msg or "ERR_CONNECTION_RESET" in msg:
                    # 针对连接重置/跳转失败，快速再试一次（已是规范化URL）
                    await asyncio.sleep(0.4)
                    try:
                        res = await crawler.arun(
                            url=url,
                            word_count_threshold=10,
                            extraction_strategy="LLMExtractionStrategy",
                            extraction_strategy_args={"extraction_schema": extraction_schema},
                            bypass_cache=True,
                        )
                        return res
                    except Exception:
                        pass
                await asyncio.sleep(delay)
                delay = min(delay * 2, 5)
            except Exception:
                await asyncio.sleep(delay)
                delay = min(delay * 2, 5)
    return None

# 北京时间工具与文章页时间抽取
from datetime import timezone
CHINA_TZ = timezone(timedelta(hours=8))

TIME_KEYS = [
    "datePublished", "dateModified", "pubdate", "publishdate", "published_time",
    "发布时间", "发表时间", "时间", "datetime", "content_time"
]

async def _extract_publish_time_and_text(crawler, url: str, sem: asyncio.Semaphore, max_retries: int = 2):
    """返回 {'published_at': 'YYYY-MM-DD HH:MM', 'page_text': '<markdown or plain text>'}，失败时字段为空串。"""
    out = {"published_at": "", "page_text": ""}
    if not url:
        return out
    schema = {
        "type": "object",
        "properties": {k: {"type": "string"} for k in TIME_KEYS}
    }
    delay = 0.5
    for _ in range(max_retries):
        async with sem:
            try:
                r = await crawler.arun(
                    url=_normalize_article_url(url),
                    word_count_threshold=5,
                    extraction_strategy="LLMExtractionStrategy",
                    extraction_strategy_args={"extraction_schema": schema},
                    bypass_cache=True,
                )
                # 1) schema 时间字段
                if getattr(r, "success", False) and getattr(r, "extracted_content", None):
                    try:
                        data = json.loads(r.extracted_content) or {}
                        for k in TIME_KEYS:
                            val = (data.get(k) or "").strip()
                            if val:
                                dt = _parse_any_dt_cn(val)
                                if dt:
                                    out["published_at"] = dt.astimezone(CHINA_TZ).strftime("%Y-%m-%d %H:%M")
                                    break
                    except Exception:
                        pass
                # 2) 页面文本（优先markdown），清洗为“仅正文”
                page_md = (getattr(r, "markdown", "") or "").strip()
                if not page_md:
                    html = (getattr(r, "cleaned_html", "") or "")
                    if html:
                        page_md = re.sub(r"<[^>]+>", " ", html)
                cleaned = _clean_page_text(page_md)
                if not _has_enough_chinese(cleaned):
                    cleaned = ""  # 非中文或中文极少：视为无效正文
                out["page_text"] = cleaned[:120000]

                # 3) fallback：从可见文本里正则找日期
                if not out["published_at"]:
                    try:
                        raw_text = page_md
                        m = re.search(r"(20\\d{2}[\\-/.年]\\d{1,2}[\\-/.月]\\d{1,2}(?:[\\sT]\\d{1,2}:\\d{2}(?::\\d{2})?)?)", raw_text)
                        if m:
                            dt = _parse_any_dt_cn(m.group(1))
                            if dt:
                                out["published_at"] = dt.astimezone(CHINA_TZ).strftime("%Y-%m-%d %H:%M")
                    except Exception:
                        pass
                # 4) 再兜底：从URL中提取日期
                if not out["published_at"]:
                    try:
                        url_dt = _parse_dt_from_url(url)
                        if url_dt:
                            out["published_at"] = url_dt
                    except Exception:
                        pass

                return out
            except Exception:
                await asyncio.sleep(delay)
                delay = min(delay * 2, 3)
    return out

async def _extract_publish_time_from_url(crawler, url: str, sem: asyncio.Semaphore, max_retries: int = 2):
    res = await _extract_publish_time_and_text(crawler, url, sem, max_retries=max_retries)
    return res.get("published_at")

# 解析常见的中英文时间格式
def _parse_any_dt_cn(s: str):
    if not s:
        return None
    s = s.strip()
    # 去掉中文前缀
    s = re.sub(r"[\u3000\s]*发布时间[:：]\s*", "", s)
    fmts = [
        "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M", "%Y.%m.%d %H:%M",
        "%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d",
        "%Y年%m月%d日 %H:%M", "%Y年%m月%d日",
    ]
    for f in fmts:
        try:
            dt = datetime.strptime(s[:len(s)], f)
            return dt.replace(tzinfo=CHINA_TZ)
        except Exception:
            continue
    # 数字时间戳（秒/毫秒）
    if re.fullmatch(r"\d{10,13}", s):
        try:
            ts = int(s[:13])
            if len(s) == 10:
                ts *= 1000
            return datetime.fromtimestamp(ts/1000, tz=CHINA_TZ)
        except Exception:
            return None
    # 兜底：提取形如 2025-09-05 08:10 的片段
    m = re.search(r"(20\d{2}[-/.]\d{1,2}[-/.]\d{1,2}(?:[ T]\d{1,2}:\d{2}(?::\d{2})?)?)", s)
    if m:
        return _parse_any_dt_cn(m.group(1))
    return None

def get_data_processor():
    """获取数据处理器实例"""
    return DataProcessor()

async def _process_news_with_crawl4ai(
    stock_code: str,
    company_name: Optional[str] = None,
    end_date: str = None,
    lookback_days: int = 7,
    industry_keywords: Optional[List[str]] = None,
    macro_keywords: Optional[List[str]] = None,
) -> str:
    """
    使用 Crawl4AI 异步爬取 A 股相关新闻（面向中文语料、强调最近 N 天）。

    Args:
        stock_code: 股票代码，如 "000001.SZ" or "600519.SH"
        company_name: 公司简称/全称（若提供，可显著提升召回）
        end_date: 结束日期，格式为 "YYYYMMDD" 或 "YYYY-MM-DD"（默认今天）
        lookback_days: 回溯天数（默认 7 天）

    Returns:
        新闻分析结果字符串
    """
    try:
        # 1) 计算时间窗口
        today = datetime.utcnow()
        if end_date:
            try:
                if "-" in end_date:
                    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
                else:
                    end_dt = datetime.strptime(end_date, "%Y%m%d")
            except ValueError:
                end_dt = today
        else:
            end_dt = today
        
        # 如果搜索的是未来日期，则搜索当前时间附近的新闻
        if end_dt > today:
            logger.warning(f"搜索日期 {end_date} 是未来日期，使用当前时间进行搜索")
            end_dt = today
        
        start_dt = end_dt - timedelta(days=lookback_days)

        # 2) 构造三层关键词：company / industry / macro
        cfg = get_news_config()
        # company
        comp_terms: List[str] = []
        if company_name:
            comp_terms.append(company_name)
        if stock_code:
            comp_terms.append(stock_code)
        comp_tails = ["公告", "新闻", "研报", "投资者关系", "定增", "并购", "利润预警", "中报", "年报", "分红", "回购", "减持"]
        comp_queries = [f"{' '.join(comp_terms)} {t}".strip() for t in comp_tails if comp_terms]

        # industry
        ind_tails = cfg.get("industry_query_tails", ["政策", "消费数据", "价格", "行业报告", "库存", "销量", "景气度"])
        ind_bases = list(dict.fromkeys(expand_industry_keywords(industry_keywords or [])))
        ind_queries = [f"{b} {t}" for b in ind_bases for t in ind_tails]

        # macro
        mac_tails = cfg.get("macro_query_tails", ["中国经济", "消费政策", "监管措施", "货币政策", "财政政策", "房地产政策", "通胀", "社零", "制造业PMI"])
        mac_bases = list(dict.fromkeys([kw for kw in (macro_keywords or []) if kw])) or [""]
        mac_queries = [f"{b} {t}".strip() for b in mac_bases for t in mac_tails]

        # 统一构建 URL 列表并带上层级标签
        search_jobs = []  # list of dicts: {url, level}
        def _add_queries(queries: List[str], level: str, limit: int):
            for q in queries[:limit]:
                qb = quote_plus(q)
                # 仅使用百度(网页)搜索入口
                search_jobs.append({"url": f"https://www.baidu.com/s?wd={qb}", "level": level})
        _add_queries(comp_queries, "company", 5)
        _add_queries(ind_queries, "industry", 5)
        _add_queries(mac_queries, "macro", 4)

        # 使用更简单的提取策略
        extraction_schema = {
            "type": "object",
            "properties": {
                "content": {"type": "string", "description": "页面主要内容文本"}
            }
        }

        async with AsyncWebCrawler(verbose=False) as crawler:
            sem = asyncio.Semaphore(4)
            tasks = [
                _fetch_with_retry(crawler, job["url"], extraction_schema, sem)
                for job in search_jobs
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        # 3) 解析与去重 - 解析 markdown 链接，提取 title/url/snippet/source
        import json
        seen: set = set()
        items: List[Dict[str, str]] = []
        for r, job in zip(results, search_jobs):
            level = job.get("level", "company")
            if isinstance(r, Exception) or not r:
                continue
            # 优先用 markdown，其次 extracted_content
            md = getattr(r, "markdown", None) or ""
            extracted = []
            if md:
                extracted = _parse_search_markdown(md)
            if not extracted and getattr(r, "extracted_content", None):
                try:
                    data = json.loads(r.extracted_content)
                    content = data.get("content", "") if isinstance(data, dict) else str(r.extracted_content)
                except Exception:
                    content = getattr(r, "extracted_content", "")
                if content:
                    extracted = _parse_search_markdown(content)
            # 汇总
            for x in extracted:
                url = _normalize_article_url((x.get("url") or "").strip())
                title = (x.get("title") or "").strip()
                if not url or not title:
                    continue
                if url in seen:
                    continue
                seen.add(url)
                src_raw = x.get("source_raw") or ""
                src_norm = normalize_source_name(src_raw, url) or src_raw
                prelim_pt = _parse_dt_from_url(url) or ""
                items.append({
                    "title": title[:200],
                    "snippet": (x.get("snippet") or "")[:400],
                    "url": url,
                    "source": src_raw or src_norm or "",
                    "source_norm": src_norm or "",
                    "published_at": prelim_pt,
                    "level": level,
                })

        # 3.0) 从文章页补全发布时间（北京时间）+ 抓取全文文本
        async with AsyncWebCrawler(verbose=False) as sub_crawler:
            sub_sem = asyncio.Semaphore(4)
            subtasks = []
            for it in items:
                subtasks.append(_extract_publish_time_and_text(sub_crawler, it.get("url"), sub_sem))
            sub_results = await asyncio.gather(*subtasks, return_exceptions=True)
            for it, rt in zip(items, sub_results):
                pub = ""; page_text = ""
                if isinstance(rt, dict):
                    pub = rt.get("published_at") or ""
                    page_text = rt.get("page_text") or ""
                # 写入抓到的
                if pub:
                    it["published_at"] = pub
                if page_text:
                    it["page_text"] = page_text
                # 再兜底：URL中是否含日期
                if not it.get("published_at"):
                    url_dt = _parse_dt_from_url(it.get("url") or "")
                    if url_dt:
                        it["published_at"] = url_dt
                # 兜底：从摘要/标题里推断相对时间
                if not it.get("published_at"):
                    rel = _infer_relative_time((it.get("snippet") or "") + " " + (it.get("title") or ""), end_dt)
                    if rel:
                        it["published_at"] = rel
                # 统一一次来源规范
                if not it.get("source_norm"):
                    it["source_norm"] = normalize_source_name(it.get("source") or "", it.get("url") or "") or (it.get("source") or "")
                # page_text 占位
                if not it.get("page_text"):
                    it["page_text"] = ""
        # 3.0.x) 仅保留中文正文的条目
        items = [it for it in items if _has_enough_chinese(it.get("page_text") or "")]

        # (结构化摘要与分析对每条新闻的逻辑已移除，详见下方仅对优先/高影响项处理)

        # 3.0.1) 事件聚合：按规范化标题聚合多来源报道
        groups = {}
        for it in items:
            key = _canonical_event_key(it.get("title") or "") or (it.get("url") or "")
            groups.setdefault(key, []).append(it)
        merged = []
        for key, arr in groups.items():
            if not arr:
                continue
            # choose representative by: priority -> latest time -> source weight
            def _score(x):
                dt = _parse_any_dt_cn(x.get("published_at") or "")
                ts = int((dt or datetime.min.replace(tzinfo=CHINA_TZ)).timestamp())
                w = _source_weight(normalize_source_name(x.get("source") or "", x.get("url") or ""), x.get("url") or "")
                pr = 1 if get_priority_regex().search(x.get("title") or "") else 0
                return (pr, ts, w)
            rep = sorted(arr, key=_score, reverse=True)[0]
            # merge sources/urls
            srcs = []
            urls = []
            for x in arr:
                s = normalize_source_name(x.get("source") or "", x.get("url") or "") or (x.get("source") or "")
                if s and s not in srcs:
                    srcs.append(s)
                u = x.get("url") or ""
                if u and u not in urls:
                    urls.append(u)
            rep = {**rep}
            rep["sources"] = srcs
            rep["urls"] = urls
            merged.append(rep)
        items = merged

        # 3.1) 逐条打标：情绪、权重、是否优先
        enriched = []
        for it in items:
            title = it.get("title") or ""
            snippet = it.get("snippet") or ""
            url = it.get("url") or ""
            source_raw = it.get("source") or ""
            source = normalize_source_name(source_raw, url)
            published_at = it.get("published_at") or ""
            base_text = f"{title}\n{snippet}"
            label = _simple_cn_sentiment(base_text)
            reason = "关键词命中" if label != "中性" else "无明显情感关键词"
            weight = _source_weight(source, url)
            priority = bool(get_priority_regex().search(title))
            sign = 1 if label == "正面" else (-1 if label == "负面" else 0)
            cfg_now = get_news_config()
            layer_w = (cfg_now.get("layer_weights", {"company": 1.0, "industry": 0.8, "macro": 0.6}).get(it.get("level") or "company", 1.0))
            macro_boost = float(cfg_now.get("macro_event_boost", 1.4))
            macro_hot = cfg_now.get("macro_event_keywords", ["国常会", "中期借贷便利", "MLF", "降准", "降息", "地产新政", "房贷利率", "汇率稳定", "特别国债"])            
            text_all = f"{title}\n{snippet}"
            if (it.get("level") == "macro") and _contains_keywords(text_all, macro_hot):
                layer_w *= macro_boost
                it["macro_event"] = True
            else:
                it["macro_event"] = False
            impact = int((sign * weight * layer_w * 20 + 50))
            enriched.append({
                **it,
                "sentiment": label,
                "reason": reason,
                "weight": round(weight, 2),
                "priority": priority,
                "impact": max(0, min(100, impact)),
                "source_norm": source,
                "level": it.get("level") or "company",
            })

        # 3.2) 公告/监管等优先，随后按时间与impact排序（时间缺失的排后）
        def _parse_dt(s):
            dt = _parse_any_dt_cn(s or "")
            return dt or None

        enriched.sort(key=lambda x: (
            not x.get("priority", False),  # 优先项在前
            -int(((_parse_dt(x.get("published_at") or "")) or start_dt.replace(tzinfo=CHINA_TZ)).timestamp()),
            -(x.get("impact") or 0),
        ))

        items = enriched

        # 3.2.x) 仅取最近N天内的有效新闻（默认3天），最多TopK（默认10），可在YAML热加载
        cfg_now = get_news_config()
        window_days = int(cfg_now.get("news_window_days", 3))
        topk = int(cfg_now.get("news_topk", 10))
        cutoff_dt = (end_dt.astimezone(CHINA_TZ) if end_dt.tzinfo else end_dt.replace(tzinfo=CHINA_TZ)) - timedelta(days=window_days)

        def _parse_dt_safe(s):
            dt = _parse_any_dt_cn(s or "")
            return dt

        recent_items = []
        for it in items:
            dt = _parse_dt_safe(it.get("published_at") or "")
            if dt and dt >= cutoff_dt:
                recent_items.append(it)
        # 按时间倒序取TopK
        recent_items.sort(key=lambda x: int((_parse_dt_safe(x.get("published_at") or "") or datetime.min.replace(tzinfo=CHINA_TZ)).timestamp()), reverse=True)
        items_selected = recent_items[:topk]
        # 如果窗口内没有，按需求：有多少算多少（即允许为空，不回填更早）

        # 3.2.1) 逐条摘要与分析（仅对优先或高影响项，降低LLM调用量）
        dp = get_data_processor()
        try:
            eligible = [it for it in items_selected if it.get("priority") or (it.get("impact") or 0) > 60]
            # 可选上限，避免过多LLM调用
            max_summarize = 24
            for it in eligible[:max_summarize]:
                try:
                    sa = dp.summarize_single_news(it.get("title") or "", it.get("snippet") or "", it.get("page_text") or "")
                    if sa:
                        it["summary_per_item"] = sa.get("summary")
                        it["analysis_per_item"] = {
                            "key_points": sa.get("key_points") or [],
                            "sentiment": sa.get("sentiment") or it.get("sentiment") or "中性",
                            "confidence": sa.get("confidence") if isinstance(sa.get("confidence"), int) else None,
                        }
                except Exception:
                    continue
        except Exception:
            pass

        if not items_selected:
            # 构建查询描述
            query_desc = f"{stock_code}"
            if company_name:
                query_desc += f"({company_name})"
            return f"【新闻分析】: 近{lookback_days}天内未抓到与 {query_desc} 相关的新闻摘要", [], {}
        # 4) 组装语料（仅取前若干条，避免 prompt 爆炸）并统计占比
        news_texts = []
        pos_cnt = neu_cnt = neg_cnt = 0
        detail_lines = []
        for it in items_selected:
            title = it.get("title") or ""
            snippet = it.get("page_text") or it.get("snippet") or ""
            source = it.get("source_norm") or it.get("source") or ""
            when = it.get("published_at") or ""
            url = it.get("url") or ""
            label = it.get("sentiment") or "中性"
            if label == "正面":
                pos_cnt += 1
            elif label == "负面":
                neg_cnt += 1
            else:
                neu_cnt += 1
            src_list = it.get("sources") or [source]
            src_str = ",".join(src_list[:4]) + ("…" if len(src_list) > 4 else "")
            macro_tag = "★宏观事件" if it.get("macro_event") else ""
            line = "\n".join(filter(None, [title, snippet, f"来源:{src_str}", f"时间:{when}", f"情绪:{label}", f"影响分:{it.get('impact')}", url, macro_tag]))
            news_texts.append(line)
            urls_list = it.get("urls") or ([url] if url else [])
            one_url = urls_list[0] if urls_list else ""
            detail_lines.append(f"- [{label}][{it.get('impact')}][{it.get('level')}] {title} | {src_str} | {when} | {one_url} {macro_tag}")
        news_corpus = "\n\n".join(news_texts)
        stat_line = f"统计：正面{pos_cnt} | 中性{neu_cnt} | 负面{neg_cnt}（样本数:{pos_cnt+neu_cnt+neg_cnt}）"

        # 5) 调用 DataProcessor 做情绪/舆情总结
        processor = get_data_processor()
        structured = processor.summarize_news_corpus_structured(
            corpus=news_corpus,
            start_dt=start_dt,
            end_dt=end_dt,
            stat_line=stat_line,
        )
        if structured is not None:
            evidence = []
            for it in items_selected:
                evidence.append({
                    "title": it.get("title") or "",
                    "url": it.get("url") or "",
                    "source": it.get("source_norm") or it.get("source") or "",
                    "sentiment": it.get("sentiment") or "中性",
                    "impact": it.get("impact") or 50,
                    "published_at": it.get("published_at") or ""
                })
            structured["evidence"] = evidence
        if structured:
            overall = structured.get("overall_sentiment", "")
            reasons = structured.get("reasons", [])
            props = structured.get("proportions", {})
            cats = structured.get("catalysts", [])
            risks = structured.get("risks", [])
            policy = structured.get("policy_points", [])
            score = structured.get("score", "")
            one = structured.get("one_liner", "")

            def _fmt_points(arr):
                lines = []
                for x in arr or []:
                    if isinstance(x, dict):
                        lines.append(f"- {x.get('point','')}（{x.get('horizon','')}期）")
                    else:
                        lines.append(f"- {x}")
                return "\n".join([s for s in lines if s.strip()])

            text_parts = [
                f"总体情绪：{overall}（情绪分：{score}）",
                f"理由：\n- " + "\n- ".join(reasons[:3]) if reasons else "",
                ("占比解读：" +
                 f"正面{props.get('positive','')} / 中性{props.get('neutral','')} / 负面{props.get('negative','')}"
                ) if props else "",
                ("催化：\n" + _fmt_points(cats)) if cats else "",
                ("风险：\n" + _fmt_points(risks)) if risks else "",
                ("政策/监管要点：\n- " + "\n- ".join(policy)) if policy else "",
                f"一句话：{one}" if one else "",
            ]
            summary_text = "\n".join([p for p in text_parts if p])
        else:
            summary_text = processor.summarize_news_corpus(
                corpus=news_corpus,
                start_dt=start_dt,
                end_dt=end_dt,
                stat_line=stat_line,
            )
            final_summary = f"【新闻分析】\n{summary_text}\n\n【可溯源明细(Top{topk})】\n" + "\n".join(detail_lines)
            return final_summary, items, {}

        # 5.1) 结论依据（含链接）
        BAD_HOSTS = ("bing.com", "microsoft.com", "onedrive.live.com")
        def _bad(u: str) -> bool:
            ul = (u or "").lower()
            return any(h in ul for h in BAD_HOSTS)

        def _dt_ts2(x):
            dt = _parse_any_dt_cn(x.get("published_at") or "")
            return int((dt or datetime.min.replace(tzinfo=CHINA_TZ)).timestamp())

        ev_candidates = [x for x in items_selected if not _bad(x.get("url") or "")]
        ev_candidates.sort(key=lambda x: (
            not x.get("priority", False),
            -(x.get("impact") or 0),
            -_dt_ts2(x),
        ))
        evidence = []
        for e in ev_candidates[:6]:
            urls_list = e.get("urls") or ([e.get("url")] if e.get("url") else [])
            evidence.append({
                "title": e.get("title"),
                "url": urls_list[0] if urls_list else "",
                "source": e.get("source_norm") or e.get("source") or "",
                "sentiment": e.get("sentiment"),
                "impact": e.get("impact"),
                "published_at": e.get("published_at"),
            })
        structured["evidence"] = evidence

        ev_lines = []
        for ev in evidence:
            ev_lines.append(f"- {ev.get('source')}: {ev.get('title')}\n  {ev.get('url')}")
        evidence_text = ("\n【结论依据（示例）】\n" + "\n".join(ev_lines)) if ev_lines else ""

        # 6) 生成最终可读总结
        final_summary = f"【新闻分析】\n{summary_text}{evidence_text}\n\n【可溯源明细(Top{topk})】\n" + "\n".join(detail_lines)
        return final_summary, items, structured
    except Exception as e:
        logger.error(f"新闻分析过程中出错: {e}")
        return f"【新闻分析】: 分析过程中出错 - {e}", [], {}

def process_news_with_crawl4ai(
    stock_code: str,
    end_date: str = None,
    company_name: Optional[str] = None,
    industry_keywords: Optional[List[str]] = None,
    macro_keywords: Optional[List[str]] = None,
    lookback_days: int = 7,
) -> tuple:
    """
    同步包装器：在已运行事件循环的环境中安全调用异步爬取函数。
    """
    try:
        asyncio.get_running_loop()
        import threading
        result_box = {"val": ("【新闻分析】: 未获取到结果", [], {})}
        def _runner():
            result_box["val"] = asyncio.run(
                _process_news_with_crawl4ai(
                    stock_code,
                    company_name,
                    end_date,
                    lookback_days=lookback_days,
                    industry_keywords=industry_keywords,
                    macro_keywords=macro_keywords,
                )
            )
        t = threading.Thread(target=_runner, daemon=True)
        t.start()
        t.join()
        return result_box["val"]
    except RuntimeError:
        return asyncio.run(
            _process_news_with_crawl4ai(
                stock_code,
                company_name,
                end_date,
                lookback_days=lookback_days,
                industry_keywords=industry_keywords,
                macro_keywords=macro_keywords,
            )
        )