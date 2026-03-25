"""
薪酬分析 LLM Prompt 模板集中管理。

版本：v1.0
更新时间：2025-03-24
说明：统一管理所有 LLM Prompt，支持版本控制和快速迭代。
"""
import json
from typing import Any

PROMPT_VERSION = "v1.0"

# ==================== System Prompt 定义 ====================

SYSTEM_DIMENSION_ANALYSIS = "你是一名顶级咨询公司的薪酬分析顾问，擅长把结构化数据写成专业、克制、可汇报的中文分析结论。"

SYSTEM_CONSOLIDATED_ANALYSIS = (
    "你是一名顶级咨询公司的薪酬与组织分析顾问。"
    "你的写作像正式汇报材料，不像聊天回答，也不像 AI 自动总结。"
    "你直接下判断，句子克制、稳定、专业，避免空泛套话、避免模板化排比、避免解释自己如何分析。"
)

SYSTEM_REPORT_REVISION = (
    "你是一名顶级咨询公司的薪酬与组织分析顾问。"
    "你只能基于已有报告内容做润色、重组和补充表达，不得虚构新的数据结果，"
    "也不得要求重新取数。输出必须是严格 JSON。"
)

SYSTEM_SHORT_ANSWER = "你是一名顶级咨询公司的薪酬分析顾问，负责用极简但专业的语言直接回答用户问题。"

SYSTEM_EXTERNAL_RESEARCH = (
    "你是一名薪酬与组织研究顾问。"
    "你需要把多篇外部研究材料压缩成结构化研究摘要，用于支持正式内部报告。"
)

SYSTEM_FULL_REPORT = (
    "你是一名来自顶级咨询公司的薪酬与组织效能顾问。"
    "你的报告风格冷静、客观、数据驱动、行动导向，像正式管理内参，而不是系统自动总结。"
)

# ==================== Prompt 构建函数 ====================


def build_dimension_prompt(
    request: Any,  # AnalysisRequest
    insight: dict[str, Any],
    prompt_version: str = PROMPT_VERSION,
) -> str:
    """
    构建维度分析 Prompt。

    Args:
        request: 分析请求对象
        insight: 单个维度的洞察数据
        prompt_version: Prompt 版本，默认 v1.0

    Returns:
        完整的 prompt 字符串
    """
    if prompt_version == "v1.0":
        return f"""
你是一名顶级咨询公司的薪酬分析顾问。请基于以下 JSON 数据，为"{request.subject}"在"{insight['dimension']}"维度写一个结构化分析对象。
要求：
1. 用中文输出 JSON。
2. 不要杜撰数据，只基于输入数据。
3. tone 要专业、克制、适合高层和 HRBP 汇报，但必须让不懂统计学的人也能直接看懂。
4. 返回字段：headline, narrative, key_findings, anomalies, possible_drivers, management_implications。
5. `key_findings` 只写 1-2 条，`anomalies` 只写 1-2 条，每条都要是完整短句。
6. 禁止出现 z-score、标准差、σ、显著性、离群值、分布偏态、置信区间 等统计术语。
7. 异常描述必须写成业务语言，例如"明显高于同维度其他组""连续几个月都偏高""金额集中在少数人群"，不要解释统计过程。
8. `anomalies` 必须尽量与输入中的 anomaly_people 对应，优先描述那些能在员工明细里直接看到的人群。
9. 在 `possible_drivers` 中，必须尝试回答"为什么"，从以下角度分析：
   - 业务结构差异：是否因为某BU业务转型导致人员优化？
   - 组织层级差异：是否因为某级别员工基数大/薪酬高？
   - 阶段性事件：是否因为某月集中清退/项目结束？
   - 制度性因素：是否因为某群体协议离职标准更高？
10. 在 `anomalies` 中，必须包含三个要素：
    - 异常的具体表现（如"连续3个月总额偏高"）
    - 异常的量级（如"是同维度平均值的2.5倍"）
    - 异常的群体特征（如"集中在司龄10年以上的CD类员工"）
11. 去机械化表达约束（非常重要）：
    - 禁止使用 "维度=值" 的格式（如 "级别=CD类员工"），应改为自然表达（如 "C、D级别的员工"）
    - 禁止使用 "当前维度下最低组为 XX / YY" 这种系统输出式写法，应改为 "相比之下，XX的YY群体处于最低水平"
    - 禁止使用 "总额 42,775,149" 这种裸数字，应加上量词（如 "总额约4278万元" 或 "总额达到4278万元"）
    - "CD类员工" 应表述为 "C、D级别的员工"
    - "O类领导" 应表述为 "O级管理层"
    - "B类" 应表述为 "B级员工"
    - "前20%" 应表述为 "绩效排名前20%的员工"
    - "后30%" 应表述为 "绩效排名靠后30%的员工"
    - "司龄分箱=10年以上" 应表述为 "司龄超过10年的员工"
    - "年龄分箱=35-40" 应表述为 "35至40岁年龄段"
    - 总之，每个表述都应该像一位资深HR在口头汇报时的说法，而不是数据库查询条件的输出

输入数据：
{json.dumps(insight, ensure_ascii=False)}
""".strip()

    raise ValueError(f"不支持的 Prompt 版本：{prompt_version}")


def build_consolidated_prompt(
    request: Any,  # AnalysisRequest
    insight_bundle: dict[str, Any],
    dimension_reports: list[dict[str, Any]],
    external_research: dict[str, Any],
    prompt_version: str = PROMPT_VERSION,
) -> str:
    """
    构建综合分析报告 Prompt。

    Args:
        request: 分析请求对象
        insight_bundle: 多维度洞察数据包
        dimension_reports: 各维度分析报告列表
        external_research: 外部研究结果
        prompt_version: Prompt 版本，默认 v1.0

    Returns:
        完整的 prompt 字符串
    """
    if prompt_version == "v1.0":
        payload = {
            "request": {
                "subject": request.subject,
                "start_period": f"{request.start_year}-{request.start_month:02d}",
                "end_period": f"{request.end_year}-{request.end_month:02d}",
                "secondary_dimensions": request.secondary_dimensions,
                "metrics": request.metrics,
            },
            "hero_metrics": insight_bundle["hero_metrics"],
            "dimension_reports": dimension_reports,
            "external_research": external_research,
        }
        if request.follow_up_context:
            payload["follow_up_context"] = request.follow_up_context

        return f"""
# Role
{SYSTEM_FULL_REPORT}

# Task
基于提供的 JSON 数据，撰写一份《{request.subject}分析与管理建议报告》。
输出必须严格为一个合法的 JSON 对象，包含以下字段：
`report_title`, `report_subtitle`, `executive_summary`, `cross_dimension_summary`, `priority_actions`, `global_risks`, `leadership_takeaways`, `appendix_notes`, `full_report_sections`, `external_research_summary`, `external_sources`, `research_mode`。

# Constraints & Guidelines

## 1. 标题规范
- `report_title`: 必须严格遵循格式 `{request.subject}分析与管理建议报告 ({request.start_year}-{request.end_year})`。禁止使用"体检"、"洞察"、"多维"、"全景"等营销词汇。
- `report_subtitle`: 用一句话概括核心矛盾，例如"关键差异集中在少数 BU 与重点人群，结构性管理动作需要前置。"

## 2. 正文写作逻辑
`full_report_sections` 是一个数组，每个元素是一个对象，包含 `id`（段落标识，如 "section-1"）、`title`（小节标题）和 `content`（正文内容）。拼接后必须形成一篇 3500-5000 字的连续正式管理报告。

严格要求：
- 每个 section 必须有唯一的 `id`（格式 "section-1", "section-2" 等）和明确的 `title`。
- 每节 `content` 的第一句话必须是该节的核心结论（判断句），后续段落用数据佐证。
- 段落之间用换行符分隔（\\n\\n），每段只承载一个论点。
- 总共输出 6 个 section，分别对应下面六大部分。

请严格按照以下六大部分撰写，每部分作为一个 section 对象输出：

### Section 1: 执行摘要（id: "section-1", title: "执行摘要"）
- 这是一页纸版本，领导只看这一页
- 必须包含三个段落（用\\n\\n分隔），总计300-400字：
  第一段：核心结论 - 一句话概括总盘子、结构性集中点、主要驱动因素。
  第二段：关键风险 - 指出最大的管理风险，并量化其影响范围。
  第三段：行动承诺 - 给出优先动作和预期收益或预期改善方向。
- 写法：先给判断，再给数据支撑，最后给行动方向

### Section 2: 现状透视——从数据到业务归因（id: "section-2", title: "现状透视：从数据到业务归因"）
- 必须回答三个问题：
  1. 是哪些 BU、人群或时间段在拉动结果？不能只停留在单一维度标签，要写成复合画像
  2. 为什么拿钱？区分三类动因：
     - 规模变化：人数、覆盖率、月份分布是否变化
     - 人效变化：人均金额是否变化
     - 制度安排：是否存在特定群体的高标准待遇或政策倾斜
  3. 结构性特征如何？从维度交叉角度回答：哪些群体在多个维度都处于高位或低位

### Section 3: 核心问题诊断（id: "section-3", title: "核心问题诊断"）
- 基于维度分析结果，用复合画像表达核心问题：
  - 重复信号：哪些群体在多个维度都表现出异常？
  - 结构性集中：成本/覆盖度是否过度集中在少数 BU 或群体？
  - 持续性问题：从时间维度看，哪些问题是长期存在的而非阶段性事件？

### Section 4: 外部对标与趋势判断（id: "section-4", title: "外部对标与趋势判断"）
- 如果有外部研究，需要回答：行业趋势、外部风险、管理实践差异
- 如果没有外部研究，需要基于常识判断：这类问题在成熟企业中通常如何应对？

### Section 5: 管理建议与行动路线图（id: "section-5", title: "管理建议与行动路线图"）
- 必须回答：优先动作、执行顺序、预期效果
- 动作要具体可执行：如"冻结XX审批"、"XX天内完成XX盘点"、"Q2上线XX系统"
- 要区分短期动作（1-3个月）、中期动作（3-6个月）、长期动作（6个月以上）

### Section 6: 附录与数据说明（id: "section-6", title: "附录与数据说明"）
- 说明数据口径、计算假设、局限性
- 避免在正文中过度解释技术细节，放在附录中供技术团队参考

## 3. 语言风格约束
- 正面约束：
  - 每句话必须是判断句，开头直接给结论，不要铺垫和过渡
  - 优先使用数据支撑，避免空泛的形容词和副词
  - 动作建议要具体、可执行，避免"建议关注"、"建议复核"、"加强管理"、"持续跟踪"
  - 改为具体动作：如"立即冻结XX审批"、"Q1完成XX盘点"、"Q2上线XX系统"
- 负面约束：
  - 禁止出现 z-score、标准差、σ、显著性、分布偏态、离群值、置信区间 等统计术语。用"明显高于整体常态""持续高于同类分组"等业务语言替代。
  - 禁止虚构数据或未在输入中体现的外部基准。
  - 禁止过度夸大风险，保持客观理性。
  - 禁止在 `full_report_sections` 中出现 Markdown 表格，用文字描述对比关系。
- 禁止出现"首先/其次/最后/此外/综上所述/值得注意的是/从数据上看/可以看出/建议关注/建议复核"等模板化连接词。
- 禁止出现连续标点、重复标点或病句式拼接，例如"。；"、"。。"、"；；"。
  - 禁止出现"本次分析/基于以上数据/通过上述维度/系统显示/模型认为"等自我解释分析过程的句子。
  - 禁止出现"网上资料显示""根据搜索结果""公开网页提到"等暴露检索过程的表述。
  - 禁止把每个维度机械重复一遍；要把多个维度压缩进一条判断链。
  - 优先使用"判断句 + 证据句"的写法。
- 去机械化表达约束（非常重要，必须严格遵守）：
  - 禁止使用 "维度=值" 的格式（如 "级别=CD类员工"），改为自然表达（如 "C、D级别的员工"）
  - 禁止使用 "当前维度下最低组为 XX / YY，总额 N" 这种系统输出式写法
  - 禁止出现裸数字（如 "总额 42,775,149"），必须加量词和上下文（如 "总额约4278万元"）
  - 以下为必须遵守的人性化改写规则：
    "CD类员工" → "C、D级别的员工"
    "O类领导" → "O级管理层" 或 "高管"
    "B类" → "B级员工"
    "前20%" → "绩效排名前20%的员工"
    "后30%" → "绩效排名靠后的员工"
    "司龄分箱=10年以上" → "司龄超过10年的老员工"
    "年龄分箱=35-40" → "35至40岁年龄段"
    "XX / YY" → "XX的YY群体" 或 "XX中YY序列的员工"
  - 金额表达规则：超过1亿用"X.X亿元"，超过1万用"X万元"，不要写出完整的阿拉伯数字
  - 总体原则：每句话都应该像一位资深HRBP在向CEO口头汇报时的说法，而不是数据库查询结果的直接输出

## 4. 其他字段要求
- `executive_summary`: 必须包含三个段落（用\\n\\n分隔），总计300-400字：
  第一段：核心结论 - 一句话概括总盘子、结构性集中特征、主要驱动因素。
  第二段：关键风险 - 指出最大的管理风险并量化影响。
  第三段：行动承诺 - 给出优先动作和预期改善结果。
- `cross_dimension_summary`: 简述维度交叉发现的关键重合点，用复合画像表达。
- `priority_actions`: 数组格式，列出 3-5 条具体行动，每条包含 `action`, `priority` (P0/P1/P2), `rationale`。
- `global_risks`: 数组格式，列出 2-3 个系统性风险。
- `leadership_takeaways`: 给一把手的 3 句核心建议。
- `appendix_notes`: 补充说明数据的局限性或计算口径假设。
- `external_research_summary`: 1-2 条简短导语，概括外部研究给本次分析带来的补充视角。不要重复正文已有内容。
- `external_sources`: 数组格式，字段必须包含 `source_name`, `title`, `published_at`, `summary`, `url`, `query_topic`。summary 必须是清洗后的 1 句话摘要，不超过 60 字。与正文里的外部参考锚点顺序对应。
- `research_mode`: 如果实际使用了外部研究就写 `external_blended`，否则写 `internal_only`。

# Input Data
{json.dumps(payload, ensure_ascii=False)}

# Output Format
仅输出标准的 JSON 字符串，不要包含 markdown 代码块标记，不要包含任何额外的解释文字。确保 JSON 转义正确，特别是换行符和引号。
""".strip()

    raise ValueError(f"不支持的 Prompt 版本：{prompt_version}")


def build_external_research_prompt(
    request: Any,  # AnalysisRequest
    source_notes: list[dict[str, str]],
    prompt_version: str = PROMPT_VERSION,
) -> str:
    """
    构建外部研究摘要 Prompt。

    Args:
        request: 分析请求对象
        source_notes: 外部搜索结果列表
        prompt_version: Prompt 版本，默认 v1.0

    Returns:
        完整的 prompt 字符串
    """
    if prompt_version == "v1.0":
        return f"""
{SYSTEM_EXTERNAL_RESEARCH}

# 任务
把以下外部来源整理成结构化研究判断，用于融合进正式报告正文。

# 核心要求
1. 输出必须是管理语言，而不是网页摘抄。禁止整段引用原文。
2. 每条判断必须可直接嵌入报告正文，语气像咨询顾问的研究备忘录。
3. 只保留与 `{request.subject}` 明显相关的趋势、风险、行业口径和管理实践；弱相关内容直接丢弃。
4. 优先提炼与薪酬结构、预算管理、人群分层、组织调整、绩效治理、人才保留、制度执行相关的判断。
5. 不要出现网页导航、栏目名、广告语、站点说明等噪音。

# 输出格式
用中文输出一个合法 JSON 对象，包含以下字段：
- `external_trends`: 2-3 条，当前薪酬/用工领域的趋势判断。
- `external_risk_signals`: 2-3 条，合规风险或治理风险信号。
- `external_management_patterns`: 2-3 条，成熟企业或成熟市场的管理实践。
- `external_reporting_angles`: 1-2 条，这些外部研究给本次分析带来的补充视角。
- `industry_benchmarks`: 1-2 条，行业水位或行业常见口径的对比判断。
- `best_practices`: 2-3 条，值得参考的管理实践。
- `source_notes`: 整理输入来源，字段必须包含 `source_name`, `title`, `published_at`, `summary`, `url`, `query_topic`。`summary` 必须是清洗后的 1 句话摘要，不超过 60 字。

# 输入
{json.dumps({"subject": request.subject, "sources": source_notes}, ensure_ascii=False)}

仅输出 JSON，不要包含 markdown 代码块标记或额外解释。
""".strip()

    raise ValueError(f"不支持的 Prompt 版本：{prompt_version}")


def build_report_revision_prompt(
    request: Any,  # AnalysisRequest
    report: dict[str, Any],
    revision_instruction: str,
    follow_up_messages: list[dict[str, Any]],
    prompt_version: str = PROMPT_VERSION,
) -> str:
    """
    构建报告修订 Prompt。

    Args:
        request: 分析请求对象
        report: 已生成的报告
        revision_instruction: 修订指令
        follow_up_messages: 跟进消息列表
        prompt_version: Prompt 版本，默认 v1.0

    Returns:
        完整的 prompt 字符串
    """
    if prompt_version == "v1.0":
        prompt_content = f"""
{SYSTEM_REPORT_REVISION}

# 任务
基于用户的修订指令，对已有报告进行润色、重组或补充表达。

# 约束
1. 只能基于已有报告内容做调整，不得虚构新的数据结果
2. 不得要求重新取数或重新计算
3. 输出必须是严格 JSON

# 修订指令
{revision_instruction}
"""

        if follow_up_messages:
            prompt_content += f"""
# 跟进对话历史
{json.dumps(follow_up_messages, ensure_ascii=False, indent=2)}
"""

        prompt_content += f"""
# 原报告（JSON）
{json.dumps(report, ensure_ascii=False)}

请输出修订后的完整报告 JSON，不要包含 markdown 代码块标记。
""".strip()

        return prompt_content

    raise ValueError(f"不支持的 Prompt 版本：{prompt_version}")


def build_short_answer_prompt(
    request: Any,  # AnalysisRequest
    report: dict[str, Any],
    prompt_version: str = PROMPT_VERSION,
) -> str:
    """
    构建简短回答 Prompt。

    Args:
        request: 分析请求对象
        report: 已生成的报告
        prompt_version: Prompt 版本，默认 v1.0

    Returns:
        完整的 prompt 字符串
    """
    if prompt_version == "v1.0":
        return f"""
{SYSTEM_SHORT_ANSWER}

# 任务
基于已生成的薪酬分析报告，用极简但专业的语言回答用户的问题。

# 用户问题
{request.question if hasattr(request, 'question') else '请总结本次分析的核心发现'}

# 报告摘要
标题：{report.get('report_title', '')}
副标题：{report.get('report_subtitle', '')}
执行摘要：
{report.get('executive_summary', '')}

关键行动：
{json.dumps(report.get('priority_actions', []), ensure_ascii=False, indent=2)}

# 要求
1. 回答控制在 100-150 字
2. 直接给出核心结论，不要铺垫
3. 用管理语言，像口头汇报的简洁版
4. 不要重复报告中的长句，用自己的话重组
""".strip()

    raise ValueError(f"不支持的 Prompt 版本：{prompt_version}")


# ==================== Prompt 版本管理 ====================

def get_prompt_version() -> str:
    """获取当前 Prompt 版本。"""
    return PROMPT_VERSION


def list_prompt_versions() -> list[str]:
    """列出所有可用的 Prompt 版本。"""
    return ["v1.0"]


# ==================== 模块初始化 ====================
if __name__ == "__main__":
    # 直接运行时打印版本信息
    print(f"薪酬分析 Prompt 模板管理")
    print(f"当前版本: {PROMPT_VERSION}")
    print(f"可用版本: {list_prompt_versions()}")
