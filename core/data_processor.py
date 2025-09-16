import pandas as pd
from typing import Dict, List
import re
from datetime import datetime
from config.llm_config import get_llm
from prompts.stock.summarizer import COLUMN_SELECTOR_PROMPT, TABLE_SUMMARIZER_PROMPT, TECH_TABLE_ANALYZER_PROMPT, FUND_TABLE_ANALYZER_PROMPT
from langchain_core.output_parsers import JsonOutputParser
from logging import getLogger
logger = getLogger(__name__)

class DataProcessor:
    def __init__(self):
        self.llm = get_llm()
        self.json_parser = JsonOutputParser()

    
    

    def propose_upper_industry_terms(self, term: str, prompt_template: str | None = None) -> List[str]:
        """使用 LLM 为给定行业生成 2~5 个上位词，输出为去重后的短词列表。"""
        if not term:
            return []
        tpl = prompt_template or "请给出‘{term}’所属的上位行业词，不超过5个，用中文输出，使用逗号分隔，且只输出词本身。"
        prompt = tpl.format(term=term)
        try:
            resp = self.llm.invoke(prompt)
            text = (getattr(resp, 'content', None) or str(resp) or "").strip()
            parts = re.split(r"[，,\n]+", text)
            cand = [p.strip().strip('。；;') for p in parts if p and p.strip()]
            cand = [c for c in cand if c != term and len(c) <= 12]
            # 去重保序，最多5个
            seen = set()
            out: List[str] = []
            for c in cand:
                if c not in seen:
                    out.append(c)
                    seen.add(c)
                if len(out) >= 5:
                    break
            return out
        except Exception as e:
            logger.error(f"propose_upper_industry_terms 失败: {e}")
            return []

    def _select_important_columns(self, df: pd.DataFrame, objective: str) -> List[str]:
        """第一步：使用LLM选择重要列。"""
        if df.empty:
            return []
        
        column_names = df.columns.tolist()
        try:
            # 直接使用LLM调用，避免在工具函数中使用链式调用
            prompt_text = COLUMN_SELECTOR_PROMPT.format(
                objective=objective,
                column_names=str(column_names)
            )
            
            response = self.llm.invoke(prompt_text)
            # 处理不同类型的返回值
            if hasattr(response, 'content'):
                response_text = response.content
            else:
                response_text = str(response)
            
            # 使用JSON解析器解析响应
            selected_columns = self.json_parser.parse(response_text)
            # 确保返回的列名存在于原始DataFrame中，防止LLM幻觉
            logger.info(f"selected_columns: {selected_columns}")
            return [col for col in selected_columns if col in df.columns]
        except Exception as e:
            print(f"列选择失败: {e}. 返回所有列。")
            return column_names

    def _summarize_table(self, df: pd.DataFrame, objective: str) -> str:
        """第二步：使用LLM对表格进行摘要。"""
        if df.empty:
            return "无可用数据。"
            
        try:
            # 直接使用LLM调用，避免在工具函数中使用链式调用
            prompt_text = TABLE_SUMMARIZER_PROMPT.format(
                objective=objective,
                table_data=df.to_string(index=False)
            )
            
            summary = self.llm.invoke(prompt_text)
            logger.info(f"summary: {summary}")
            # 处理不同类型的返回值
            if hasattr(summary, 'content'):
                return summary.content
            else:
                return str(summary)
        except Exception as e:
            return f"生成摘要时出错: {e}"

    def process_and_summarize(self, df: pd.DataFrame, objective: str) -> str:
        """
        执行完整的两阶段处理流程。
        """
        print(f"--- 正在处理: {objective} ---")
        # 1. 智能列选择
        important_columns = self._select_important_columns(df, objective)
        print(f"  -> 智能选择的列: {important_columns}")
        
        if not important_columns:
            return f"【{objective}】: 未找到相关数据列。"
            
        # important_df = df[important_columns].head(5) # 只取最近5条记录进行摘要
        important_df = df[important_columns]
        
        # 2. 洞察性摘要
        summary = self._summarize_table(important_df, objective)
        print(f"  -> 生成的摘要: {summary}")
        
        return f"【{objective}】\n{summary}"

    def process_company_info(self, company_info: dict) -> str:
        """
        使用LLM处理公司基本信息，生成自然语言描述。
        """
        try:
            # 构建公司信息提示词
            company_prompt = f"""
请基于以下公司信息，生成一段简洁、专业的公司概况描述（100-200字）：

公司基本信息：
{company_info.get('stock_basic', {})}

公司详细信息：
{company_info.get('company_detail', {})}

要求：
1. 突出公司的核心特征和行业地位
2. 语言简洁专业，适合投资分析使用
3. 重点描述主营业务、行业分类、地理位置等关键信息
4. 如果有注册资本、员工数量等数据，请适当提及

请直接返回描述文本，不要添加任何格式标记。
"""
            
            # 调用LLM生成描述
            response = self.llm.invoke(company_prompt)
            # 处理不同类型的返回值
            if hasattr(response, 'content'):
                company_summary = response.content.strip()
            else:
                company_summary = str(response).strip()
            
            logger.info(f"公司信息摘要生成成功: {company_summary[:100]}...")
            return company_summary
            
        except Exception as e:
            logger.error(f"处理公司信息时出错: {e}")
            # 如果LLM处理失败，返回基本信息摘要
            return self._fallback_company_summary(company_info)
    
    def _fallback_company_summary(self, company_info: dict) -> str:
        """
        当LLM处理失败时的备用方案，生成基本的公司信息摘要。
        """
        summary_parts = []
        
        # 基本信息
        if company_info.get('stock_basic'):
            basic = company_info['stock_basic']
            summary_parts.append(f"{basic.get('name', '该公司')}是一家位于{basic.get('area', '未知地区')}的{basic.get('industry', '未知行业')}公司")
            summary_parts.append(f"于{basic.get('list_date', '未知日期')}在{basic.get('market', '未知市场')}上市")
        
        # 详细信息
        if company_info.get('company_detail'):
            detail = company_info['company_detail']
            if detail.get('chairman'):
                summary_parts.append(f"现任董事长为{detail.get('chairman')}")
            if detail.get('main_business'):
                summary_parts.append(f"主营业务为{detail.get('main_business')}")
            if detail.get('province') and detail.get('city'):
                summary_parts.append(f"注册地为{detail.get('province')}{detail.get('city')}")
        
        if summary_parts:
            return "。".join(summary_parts) + "。"
        else:
            return "公司基本信息暂不可用。"

    def _analyze_tech_table(self, df: pd.DataFrame, objective: str) -> str:

        if df.empty:
            return "无可用数据。"
            
        try:
            # 直接使用LLM调用，避免在工具函数中使用链式调用
            prompt_text = TECH_TABLE_ANALYZER_PROMPT.format(
                objective=objective,
                table_data=df.to_string(index=False)
            )
            
            report = self.llm.invoke(prompt_text)
            logger.info(f"report: {report}")
            # 处理不同类型的返回值
            if hasattr(report, 'content'):
                return report.content
            else:
                return str(report)
        except Exception as e:
            return f"生成报告时出错: {e}"

    def _analyze_news_table(self, df: pd.DataFrame, objective: str) -> str:
        """
        使用LLM对新闻表格进行分析（批处理合并短文，降低成本）。
        """
        return self.analyze_news_batched(df, objective, max_chars=7000, min_pack_chars=300)
    def _coerce_time_str(self, v) -> str:
        """将时间字段统一为可读字符串。"""
        try:
            if isinstance(v, str):
                return v
            from datetime import datetime as _dt
            return _dt.fromtimestamp(v).strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            return str(v)

    def _format_news_rows(self, df: pd.DataFrame) -> list[str]:
        """将新闻DataFrame转成按条目的紧凑语料（标题+摘要/正文片段+来源+时间）。"""
        cols = df.columns.str.lower().tolist()
        def pick(row, keys):
            for k in keys:
                if k in row:
                    return row.get(k)
            return None
        items: list[str] = []
        for _, r in df.iterrows():
            row = {str(k).lower(): r[k] for k in df.columns}
            title = pick(row, ['title', 't']) or ''
            content = pick(row, ['content', 'snippet', 'summary', 'desc']) or ''
            src = pick(row, ['src', 'source']) or ''
            dt = pick(row, ['datetime', 'pub_time', 'published_at', 'date']) or ''
            dt = self._coerce_time_str(dt)
            piece = f"【{dt} | {src}】{str(title).strip()}\n{str(content).strip()}".strip()
            if piece:
                items.append(piece)
        return items

    def _batch_strings_by_chars(self, parts: list[str], max_chars: int = 7000, min_pack_chars: int = 1500) -> list[str]:
        """按字符长度组包：尽量把短文本合并，单包不超过 max_chars。"""
        batches: list[str] = []
        buf: list[str] = []
        cur = 0
        for p in parts:
            p_len = len(p)
            # 如果当前缓冲区为空，直接放入
            if not buf:
                buf.append(p)
                cur = p_len
                continue
            # 如果加入后不超过上限，则合并
            if cur + 2 + p_len <= max_chars:  # +2 for '\n\n'
                buf.append(p)
                cur += 2 + p_len
            else:
                # 若当前缓冲太小（小于min_pack_chars），尝试继续塞直到达到一定体量或到达上限
                if cur < min_pack_chars:
                    # 强行塞一篇（即便稍超也要控制），但仍以不超过 max_chars 为硬限制
                    # 到不了这里，因为上一分支已判断会超；所以直接落袋
                    pass
                batches.append("\n\n".join(buf))
                buf = [p]
                cur = p_len
        if buf:
            batches.append("\n\n".join(buf))
        return batches

    def _cjk_ratio(self, text: str) -> float:
        """估计文本中的中日韩字符占比（粗略）。"""
        if not text:
            return 0.0
        total = len(text)
        if total == 0:
            return 0.0
        cjk = sum(1 for ch in text if '\u4e00' <= ch <= '\u9fff')
        return cjk / max(total, 1)

    def _calc_batch_char_cap(self,
                             sample_parts: list[str],
                             model_max_tokens: int = 65000,
                             input_ratio: float = 0.6,
                             prompt_tokens: int = 1200,
                             output_tokens: int = 1500) -> int:
        """
        根据模型最大长度和语料特征动态计算**单批最大字符数**。
        - input_ratio: 期望把总上下文中用于“输入语料”的比例（0~1），剩余给提示词/系统/输出。
        - prompt_tokens/output_tokens: 预留的提示与输出缓冲。
        估算规则：
          * 先计算可用于输入的 token 预算 = floor(model_max_tokens * input_ratio) - prompt_tokens - output_tokens
          * 使用 CJK 占比估算 char/token：CJK-heavy 取 1.0，otherwise 取 3.2（更保守于英文≈4）。
          * 再乘以 0.95 安全系数。
        """
        # 1) 采样若干条估算 CJK 占比
        sample = "\n".join(sample_parts[:20]) if sample_parts else ""
        cjk_r = self._cjk_ratio(sample)
        # 中文/混合文本下更保守：每 token ≈ 1.0 字符；英文/符号为主：每 token ≈ 3.2 字符
        chars_per_token = 1.0 if cjk_r >= 0.2 else 3.2

        # 2) token 预算
        input_token_budget = int(model_max_tokens * input_ratio) - prompt_tokens - output_tokens
        input_token_budget = max(input_token_budget, 8000)  # 最低不少于 8k token，避免过于保守

        # 3) 转换为字符并留安全边际
        cap = int(input_token_budget * chars_per_token * 0.95)
        # 限制上限，避免一次 batch 过大带来费用抖动（可按需调高/关闭）
        cap = min(cap, 38000)
        return max(cap, 4000)

    def analyze_news_batched(self,
                             df: pd.DataFrame,
                             objective: str,
                             max_chars: int | None = None,
                             min_pack_chars: int = 300,
                             model_max_tokens: int = 65000,
                             input_ratio: float = 0.6) -> str:
        """
        成本友好的按类型批量摘要：将多条短新闻合并成若干批次，每批一次LLM调用。
        - 确保同一类型（由上游传入的 objective 对应的数据表）在同一轮汇总。
        - 保留每则新闻的标题/时间/来源与摘要片段，保证篇幅完整。
        """
        if df.empty:
            return "无可用新闻数据。"

        # 1) 选列（尽量减小体积）
        important_columns = self._select_important_columns(df, objective)
        if important_columns:
            df_use = df[important_columns].copy()
        else:
            df_use = df.copy()

        # 2) 规范排序：按时间倒序（若有）
        ts_cols = [c for c in df_use.columns if str(c).lower() in ('datetime','pub_time','published_at','date')]
        if ts_cols:
            try:
                df_use = df_use.sort_values(by=ts_cols[0], ascending=False)
            except Exception:
                pass

        # 3) 构造条目文本 & 组包
        parts = self._format_news_rows(df_use)
        if not parts:
            return "无可用新闻数据。"
        # 动态计算单批最大字符数（若未显式给定）
        if max_chars is None:
            max_chars = self._calc_batch_char_cap(parts, model_max_tokens=model_max_tokens, input_ratio=input_ratio)
        batches = self._batch_strings_by_chars(parts, max_chars=max_chars, min_pack_chars=min_pack_chars)

        # 4) 统计行（用于提示词）
        stat_line = f"样本数:{len(parts)} 批次数:{len(batches)}（按长度合并摘要）"
        from datetime import datetime as _dt
        try:
            start_dt = _dt.now()
            end_dt = _dt.now()
            if ts_cols and not df_use.empty:
                try:
                    first = str(df_use[ts_cols[0]].iloc[-1])
                    last = str(df_use[ts_cols[0]].iloc[0])
                    start_dt = _dt.fromisoformat(first.replace('Z','').replace('/', '-').split('+')[0]) if first else start_dt
                    end_dt = _dt.fromisoformat(last.replace('Z','').replace('/', '-').split('+')[0]) if last else end_dt
                except Exception:
                    pass
        except Exception:
            start_dt = end_dt = _dt.now()

        # 5) 批量调用 LLM
        summaries: list[str] = []
        for i, corpus in enumerate(batches, 1):
            head = f"【批次 {i}/{len(batches)}】{objective}"
            sub = self.summarize_news_corpus(corpus, start_dt, end_dt, stat_line)
            summaries.append(f"{head}\n{sub}")

        return "\n\n---\n\n".join(summaries)

    def summarize_news_corpus(self, corpus: str, start_dt, end_dt, stat_line: str) -> str:
        """对新闻语料进行摘要分析"""
        try:
            # 构建新闻摘要提示词
            news_prompt = f"""
请基于以下新闻数据，生成专业的新闻分析摘要：

时间范围：{start_dt.strftime('%Y-%m-%d')} 到 {end_dt.strftime('%Y-%m-%d')}
{stat_line}

新闻数据：
{corpus}

要求：
1. 分析新闻的整体情绪倾向（正面/中性/负面）
2. 提取关键信息点和重要事件
3. 评估对相关股票或市场的影响
4. 语言简洁专业，适合投资分析使用
5. 重点关注与投资决策相关的信息

请直接返回分析结果，不要添加格式标记。
"""
            
            response = self.llm.invoke(news_prompt)
            # 处理不同类型的返回值
            if hasattr(response, 'content'):
                summary = response.content.strip()
            else:
                summary = str(response).strip()
            logger.info(f"新闻语料摘要生成成功: {summary[:100]}...")
            return summary
        except Exception as e:
            logger.error(f"生成新闻语料摘要时出错: {e}")
            return f"生成新闻语料摘要时出错: {e}"

    def _summarize_news_table(self, df: pd.DataFrame, objective: str) -> str:
        """使用LLM对新闻表格进行摘要"""
        if df.empty:
            return "无可用数据。"
        
        # 构建新闻摘要提示词
        news_prompt = f"""
请基于以下新闻数据，生成专业的新闻分析摘要：

分析目标：{objective}

新闻数据：
{df.to_string(index=False)}

要求：
1. 分析新闻的整体情绪倾向（正面/中性/负面）
2. 提取关键信息点和重要事件
3. 评估对相关股票或市场的影响
4. 语言简洁专业，适合投资分析使用
5. 重点关注与投资决策相关的信息

请直接返回分析结果，不要添加格式标记。
"""
        
        try:
            response = self.llm.invoke(news_prompt)
            # 处理不同类型的返回值
            if hasattr(response, 'content'):
                summary = response.content.strip()
            else:
                summary = str(response).strip()
            logger.info(f"新闻表格摘要生成成功: {summary[:100]}...")
            return summary
        except Exception as e:
            logger.error(f"生成新闻摘要时出错: {e}")
            return f"生成新闻摘要时出错: {e}"

    def _analyze_fund_table(self, df: pd.DataFrame, objective: str) -> str:
        """
        使用LLM对资金流向表格进行分析，先进行智能筛选。
        """
        if df.empty:
            return "无可用数据。"
        
        logger.info(f"[资金面分析] 开始处理 {objective}，原始数据维度: {df.shape}")
        
        # 1. 智能列选择
        important_columns = self._select_important_columns(df, objective)
        logger.info(f"[资金面分析] {objective} 识别的重要列: {important_columns}")
        
        if not important_columns:
            return f"【{objective}】: 未找到相关数据列。"
        
        # 2. 筛选重要列数据
        important_df = df[important_columns]
        logger.info(f"[资金面分析] {objective} 筛选后的重要列数据维度: {important_df.shape}")
        
        # 3. 如果数据仍然太大，进一步限制行数
        if len(important_df) > 100:
            # 取最近100条记录
            important_df = important_df.tail(100)
            logger.info(f"[资金面分析] {objective} 数据量过大，限制为最近100条记录")
        
        # 4. 生成分析报告
        try:
            # 直接使用LLM调用，避免在工具函数中使用链式调用
            prompt_text = FUND_TABLE_ANALYZER_PROMPT.format(
                objective=objective,
                table_data=important_df.to_string(index=False)
            )
            
            report = self.llm.invoke(prompt_text)
            logger.info(f"[资金面分析] {objective} 分析报告生成成功")
            # 处理不同类型的返回值
            if hasattr(report, 'content'):
                return report.content
            else:
                return str(report)
        except Exception as e:
            logger.error(f"[资金面分析] {objective} 生成报告时出错: {e}")
            return f"生成报告时出错: {e}"
    
    def process_and_summarize_text(self, text: str, objective: str) -> str:
        """
        处理文本内容并生成摘要
        
        Args:
            text: 要处理的文本内容
            objective: 处理目标描述
        
        Returns:
            处理后的摘要文本
        """
        try:
            # 构建文本分析提示词
            text_prompt = f"""
请分析以下文本内容，并生成关于"{objective}"的摘要报告：

文本内容：
{text}

要求：
1. 提取关键信息和观点
2. 分析情感倾向和舆情态度
3. 总结主要观点和结论
4. 语言简洁专业，适合投资分析使用
5. 字数控制在200-300字

请直接返回分析结果，不要添加任何格式标记。
"""
            
            # 调用LLM生成分析
            response = self.llm.invoke(text_prompt)
            # 处理不同类型的返回值
            if hasattr(response, 'content'):
                summary = response.content.strip()
            else:
                summary = str(response).strip()
            
            logger.info(f"文本分析完成: {summary[:100]}...")
            return summary
            
        except Exception as e:
            logger.error(f"处理文本内容时出错: {e}")
            return f"文本分析失败: {e}"

