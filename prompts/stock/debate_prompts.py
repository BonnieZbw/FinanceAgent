from langchain_core.prompts import PromptTemplate


BULL_DEBATER_PROMPT = PromptTemplate.from_template(
"""
{role_description}

你的任务是基于多位分析师的量化评分与观点，为股票 {stock_code} 构建**看涨论据**。

**分析时间段**: {analysis_period}

你将收到来自基本面、技术面、资金面、情绪面、舆情面的分析报告（包含分数与结论），请：
1. 选取并强调有利于看涨的维度与数据（分数高、结论为看多）。
2. 对不利数据（低分、看空结论）进行反驳或弱化解释。
3. 逻辑严谨、数据驱动，不得凭空捏造信息。

分析步骤：
- 汇总各维度的评分与结论
- 解释为什么这些数据支持上涨
- 回应并反驳不利观点
- 最终给出明确的看多结论

**多位分析师的报告（供参考）**：
---
{fundamental_report}
---
{technical_report}
---
{sentiment_report}
---
{fund_report}
---
{news_report}

**输出格式要求**：
```json
{{
  "analyst_name": "看涨派分析师",
  "viewpoint": "看多",
  "core_arguments": [
    "使用分数+结论支持上涨的论据1",
    "使用分数+结论支持上涨的论据2",
    "使用分数+结论支持上涨的论据3",
    ...
  ],
  "rebuttals": [
    "对看空论点的反驳1",
    "对看空论点的反驳2",
    ...
  ],
  "final_statement": "一句话坚定表明看多立场，<=50字"
}}
```
"""
)


BEAR_DEBATER_PROMPT = PromptTemplate.from_template(
"""
{role_description}

你的任务是基于多位分析师的量化评分与观点，为股票 {stock_code} 构建**看跌论据**。

**分析时间段**: {analysis_period}

你将收到来自基本面、技术面、资金面、情绪面、舆情面的分析报告（包含分数与结论），请：
1. 选取并强调有利于看跌的维度与数据（分数低、结论为看空）。
2. 对不利数据（高分、看多结论）进行反驳或弱化解释。
3. 逻辑严谨、数据驱动，不得凭空捏造信息。

分析步骤：
- 汇总各维度的评分与结论
- 解释为什么这些数据支持下跌
- 回应并反驳不利观点
- 最终给出明确的看空结论

**多位分析师的报告（供参考）**：
---
{fundamental_report}
---
{technical_report}
---
{sentiment_report}
---
{fund_report}
---
{news_report}

**输出格式要求**：
```json
{{
  "analyst_name": "看跌派分析师",
  "viewpoint": "看空",
  "core_arguments": [
    "使用分数+结论支持下跌的论据1",
    "使用分数+结论支持下跌的论据2",
    "使用分数+结论支持下跌的论据3"
    ...
  ],
  "rebuttals": [
    "对看多论点的反驳1",
    "对看多论点的反驳2",
    ...
  ],
  "final_statement": "一句话坚定表明看空立场，<=50字"
}}
```
"""
)


DEBATE_ANALYST_PROMPT = PromptTemplate.from_template(
"""
{role_description}

你是量化分析师，负责主持并总结对股票 {stock_code} 的多空辩论。

**分析时间段**: {analysis_period}

你将收到两位辩手（看涨派与看跌派）的观点，以及各维度分析师的量化评分。  
你的任务是整合这些信息，给出**全面、客观且有结论**的投资分析报告。

请遵循以下步骤：
1. **提炼双方核心观点**：找出看涨派和看跌派的主要论据（支持与反驳）。
2. **量化对比**：
   - 统计所有维度的平均得分（盈利能力、技术面、资金面、情绪面、舆情面）。
   - 分别计算看多倾向评分与看空倾向评分。
3. **判断结论**：
   - 如果看多平均分 ≥4 且明显高于看空 → "强烈看多"
   - 如果看多平均分 > 看空平均分且差距明显 → "看多"
   - 如果分数接近 → "中性"
   - 如果看空平均分 > 看多平均分且差距明显 → "看空"
   - 如果看空平均分 ≥4 且明显高于看多 → "强烈看空"
4. **形成最终投资建议**：明确结论并简述原因（50字以内）。
5. **输出结构化报告**：包括双方核心论点、分数对比、最终建议。

**输入数据（供参考）**：
- 各维度分析师的量化评分与结论：
---
{fundamental_report}
---
{technical_report}
---
{sentiment_report}
---
{fund_report}
---
{news_report}
---
- 看涨派辩手的观点：
{bull_report}
- 看跌派辩手的观点：
{bear_report}

**输出格式要求**：
```json
{{
  "analyst_name": "首席投资分析师",
  "bull_summary": [
    "看涨派的核心论点1",
    "看涨派的核心论点2",
    ...
  ],
  "bear_summary": [
    "看跌派的核心论点1",
    "看跌派的核心论点2",
    ...
  ],
  "score_comparison": {{
    "bull_avg_score": "数值（1-5）",
    "bear_avg_score": "数值（1-5）"
  }},
  "final_viewpoint": "强烈看多 / 看多 / 中性 / 看空 / 强烈看空",
  "final_reason": "一句话总结核心结论（<=50字）"
}}
```
"""
)
