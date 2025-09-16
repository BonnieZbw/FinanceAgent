from typing import TypedDict, List, Dict, Optional

class AnalystReport(TypedDict):
    analyst_name: str
    viewpoint: str  
    reason: str
    scores: Dict[str, int]  
    detailed_analysis: str

class DebaterReport(TypedDict):
    analyst_name: str
    viewpoint: str
    core_arguments: List[str]
    rebuttals: List[str]
    final_statement: str

class DebateReport(TypedDict):
    analyst_name: str
    bull_summary: List[str]
    bear_summary: List[str]
    score_comparison: Dict[str, int]
    final_viewpoint: str
    final_reason: str

class StockAgentState(TypedDict):
    stock_code: str
    end_date: str

    # 总决策报告
    supervisor_report: Optional[str]

    # 分析师报告
    fundamental_report: Optional[AnalystReport]
    technical_report: Optional[AnalystReport]
    news_report: Optional[AnalystReport]
    fund_report: Optional[AnalystReport]
    sentiment_report: Optional[AnalystReport]

    # 辩论报告
    bull_report: Optional[DebaterReport]
    bear_report: Optional[DebaterReport]
    debate_report: Optional[DebaterReport]

    final_report: Optional[str]
    debate_history: List[tuple[str, str]] # 存储 (发言人, 内容)


