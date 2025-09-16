from graph.type import StockAgentState

def start_node(state: StockAgentState):
    """
    工作流的单一入口节点。
    未来可在此处添加输入校验、任务初始化等逻辑。
    """
    print(f"--- [Graph Start] Kicking off analysis for: {state['stock_code']} ---")
    # 当前无需修改状态，直接返回即可
    return