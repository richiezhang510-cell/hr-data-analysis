from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SalarySchema:
    schema_id: str
    display_name: str
    wide_columns: list[str]
    text_dimension_columns: list[str]
    numeric_columns: list[str]
    dimension_columns: list[str]
    subject_columns: list[str]
    subject_aliases: dict[str, str]
    dimension_aliases: dict[str, str]
    default_subject: str
    default_secondary_dimensions: list[str]
    subject_categories: dict[str, str]
    ambiguous_terms: dict[str, list[str]]
    schema_mode: str = "registered"
    source_manifest: dict | None = None
    source_column_map: dict[str, str | None] | None = None
    synthetic_defaults: dict[str, str] | None = None
    capabilities: dict[str, bool] | None = None


LEGACY_COLUMNS = [
    "统计年度",
    "统计月份",
    "BU",
    "员工ID",
    "职能",
    "绩效分位",
    "级别",
    "司龄分箱",
    "年龄分箱",
    "底薪",
    "基本工资调整",
    "内勤绩效",
    "岗位津贴",
    "倒班津贴",
    "特招津贴",
    "加班费",
    "经济补偿金",
    "签约金",
    "降温取暖费",
    "配偶补贴",
    "借调补贴",
]

LEGACY_SUBJECT_COLUMNS = LEGACY_COLUMNS[9:]


PINGAN_FULL_DIMENSIONS = [
    "统计年度",
    "统计月份",
    "统计月",
    "员工ID",
    "员工UM",
    "BU",
    "部门",
    "职能序列",
    "职能",
    "级别",
    "去年绩效排名",
    "绩效分位",
    "年龄",
    "年龄分箱",
    "司龄",
    "司龄分箱",
]

PINGAN_FULL_SUBJECT_COLUMNS = [
    "养老保险扣款",
    "养老保险扣款补缴",
    "医疗保险扣款",
    "医疗保险扣款补缴",
    "工伤保险扣款",
    "工伤保险扣款补缴",
    "生育保险扣款",
    "生育保险扣款补缴",
    "失业保险扣款",
    "失业保险扣款补缴",
    "住房公积金扣款",
    "住房公积金扣款补缴",
    "商业保险",
    "税后扣款",
    "病假扣款",
    "事假扣款",
    "旷工扣款",
    "考勤扣款调整",
    "迟到款",
    "早退扣款",
    "中间外出扣款",
    "代扣房款",
    "代扣房租",
    "代扣工会费",
    "处罚扣款",
    "公益捐款",
    "其他代扣款",
    "手工扣税",
    "代扣个人所得税",
    "养老保险(公司)",
    "医疗保险(公司)",
    "工伤保险(公司)",
    "生育保险(公司)",
    "失业保险(公司)",
    "养老保险(公司)补缴",
    "医疗保险(公司)补缴",
    "工伤保险(公司)补缴",
    "生育保险(公司)补缴",
    "失业保险(公司)补缴",
    "残疾保障(公司)",
    "其他社保(公司)",
    "其他社保补缴(公司)",
    "住房公积金(公司)",
    "补充住房津贴",
    "计提工会费",
    "计提教育经费",
    "住房公积金(公司)补缴",
    "借调补贴",
    "岗位/技能津贴",
    "特招津贴",
    "考勤奖",
    "养老险短险产品绩效(实发)",
    "产险产品综合开拓绩效",
    "中期激励奖金",
    "年金奖励缴费(公司)",
    "健康险产品综合开拓绩效",
    "年金扣款(个人)",
    "内勤绩效",
    "内部推荐奖",
    "信托产品综合开拓绩效",
    "月度绩效",
    "特殊岗位津贴",
    "内勤绩效奖金",
    "养老险直投业务绩效(实发)",
    "底薪/基本工资",
    "养老险直投业务绩效(计提)",
    "养老资管产品绩效(实发)",
    "团险短险产品绩效(实发)",
    "级别津贴",
    "实习津贴",
    "年金产品综合开拓绩效",
    "教师课酬",
    "其他税前扣款",
    "企业年金产品绩效",
    "团险短险产品绩效(计提)",
    "年终奖",
    "蓄水奖金",
    "养老险短险产品绩效(计提)",
    "综合业务贡献绩效",
    "年金基础",
    "养老资管产品绩效(计提)",
    "养老险长险产品绩效(实发)",
    "递延年终奖",
    "其他税前收入",
    "养老险长险产品绩效(计提)",
    "团险长险产品绩效(实发)",
    "团险长险产品绩效(计提)",
    "外勤年中/年终奖(直投)",
    "外勤年中/年终奖(资管)",
    "外勤年中/年终奖(年金)",
    "外勤年中/年终奖(长险)",
    "外勤年中/年终奖(短险)",
    "外勤年中/年终奖(产险)",
    "外勤年中/年终奖",
    "长期出差补助",
    "零售业务提奖",
    "公司业务提奖",
    "普惠业务提奖",
    "特管业务提奖",
    "同业业务提奖",
    "业务浮动奖金",
    "总行其他奖金",
    "机构奖金",
    "健康互联(好医生)综合开拓绩效",
    "职务发明奖励",
    "累计赡养老人专项附加扣除",
    "个税调整",
    "商业保险免税额",
    "漏打卡扣款",
    "累计子女教育专项附加扣除",
    "累计继续学历教育专项附加扣除",
    "累计住房贷款利息专项附加扣除",
    "累计住房租金专项附加扣除",
    "商业保险税前扣款",
    "扣税不发薪(费用)",
    "扣税不发薪(福利)",
    "扣税不发薪(其他)",
    "政府奖励税前返还",
    "政府奖励税后返还",
    "全年一次性奖金",
    "代扣个税",
    "年底绩效奖",
    "签约金",
    "公司特殊奖励奖金",
    "奖金预发",
    "年度免税额累计值",
    "纳税主体变更",
    "交通补贴",
    "累计婴幼儿照护费专项附加扣除",
    "累计个人养老金扣除",
    "岗位津贴",
    "计件薪",
    "年资津贴",
    "展业津贴",
    "学历津贴",
    "异地任职津贴",
    "配偶补贴",
    "其他社保扣款",
    "其他社保扣款补缴",
    "加班费",
    "倒班津贴",
    "基本工资调整",
    "经济补偿金",
    "外勤管理绩效",
    "内勤其他奖金",
    "月度业务提奖",
    "其他税后收入",
    "团队长提奖",
    "医疗补助(非工伤)",
    "年休假未休补偿金",
    "误餐补贴",
    "独生子女费",
    "增员激励",
    "业务推动激励",
    "过节费",
    "降温取暖费",
    "内勤绩效调整",
    "外勤其他奖金",
    "机构班子成员奖",
    "竞业补偿金",
    "医疗补助(工伤)",
    "女工保健费",
    "托儿费",
    "调查奖金",
    "辅导专员奖金",
    "前线经理奖金",
    "其他津贴",
    "非月度业务提奖",
    "其他福利",
    "综合福利保障(公司)",
    "通讯补贴",
    "商外补贴",
]

PINGAN_FULL_COLUMNS = PINGAN_FULL_DIMENSIONS + PINGAN_FULL_SUBJECT_COLUMNS

COMMON_SUBJECT_ALIASES = {
    "补偿金": "经济补偿金",
    "经济补偿": "经济补偿金",
    "底薪": "底薪/基本工资",
    "基本工资": "底薪/基本工资",
    "底薪/基本工资": "底薪/基本工资",
    "工资调整": "基本工资调整",
    "绩效": "内勤绩效",
    "个税": "代扣个人所得税",
    "公积金公司缴纳": "住房公积金(公司)",
    "住房公积金公司缴纳": "住房公积金(公司)",
    "住房公积金个人缴纳": "住房公积金扣款",
    "养老补缴": "养老保险扣款补缴",
    "社保个人养老": "养老保险扣款",
    "社保公司养老": "养老保险(公司)",
    "月绩效": "月度绩效",
    "外勤年终奖": "外勤年中/年终奖",
    "专项附加扣除": "累计子女教育专项附加扣除",
}


def build_subject_categories(subjects: list[str]) -> dict[str, str]:
    categories: dict[str, str] = {}
    for subject in subjects:
        if any(token in subject for token in ["养老保险", "医疗保险", "工伤保险", "生育保险", "失业保险", "社保", "公积金", "残疾保障"]):
            categories[subject] = "社保公积金"
        elif any(token in subject for token in ["个税", "所得税", "扣税", "专项附加扣除", "免税额", "纳税"]):
            categories[subject] = "个税扣缴"
        elif any(token in subject for token in ["底薪", "基本工资", "工资调整"]):
            categories[subject] = "基础工资"
        elif any(token in subject for token in ["绩效", "提奖", "奖励", "奖金", "年终", "激励"]):
            categories[subject] = "绩效奖金"
        elif any(token in subject for token in ["津贴", "补贴", "补助", "课酬", "误餐", "通讯", "交通"]):
            categories[subject] = "补贴福利"
        elif any(token in subject for token in ["补偿金", "签约金", "竞业", "政府奖励"]):
            categories[subject] = "低频补偿项"
        elif any(token in subject for token in ["扣款", "代扣", "病假", "事假", "旷工", "迟到", "早退", "处罚"]):
            categories[subject] = "扣款代扣"
        else:
            categories[subject] = "其他"
    return categories


def build_ambiguous_terms(subjects: list[str]) -> dict[str, list[str]]:
    return {
        "绩效": [subject for subject in subjects if "绩效" in subject or "提奖" in subject],
        "津贴": [subject for subject in subjects if "津贴" in subject or "补助" in subject],
        "奖金": [subject for subject in subjects if "奖金" in subject or "奖励" in subject or "年终奖" in subject],
        "补贴": [subject for subject in subjects if "补贴" in subject],
        "社保": [subject for subject in subjects if "保险" in subject or "社保" in subject],
        "公积金": [subject for subject in subjects if "公积金" in subject],
        "个税": [subject for subject in subjects if "个税" in subject or "所得税" in subject or "扣税" in subject],
        "福利": [subject for subject in subjects if "福利" in subject or "保险" in subject],
    }


LEGACY_SUBJECT_CATEGORIES = build_subject_categories(LEGACY_SUBJECT_COLUMNS)
PINGAN_FULL_SUBJECT_CATEGORIES = build_subject_categories(PINGAN_FULL_SUBJECT_COLUMNS)

LEGACY_AMBIGUOUS_TERMS = {
    "绩效": [subject for subject in LEGACY_SUBJECT_COLUMNS if "绩效" in subject],
    "津贴": [subject for subject in LEGACY_SUBJECT_COLUMNS if "津贴" in subject],
    "奖金": [subject for subject in LEGACY_SUBJECT_COLUMNS if "奖" in subject],
}

PINGAN_FULL_AMBIGUOUS_TERMS = build_ambiguous_terms(PINGAN_FULL_SUBJECT_COLUMNS)


SCHEMA_REGISTRY: dict[str, SalarySchema] = {
    "legacy_simple": SalarySchema(
        schema_id="legacy_simple",
        display_name="旧版21列演示宽表",
        wide_columns=LEGACY_COLUMNS,
        text_dimension_columns=[
            "BU",
            "员工ID",
            "职能",
            "绩效分位",
            "级别",
            "司龄分箱",
            "年龄分箱",
        ],
        numeric_columns=LEGACY_SUBJECT_COLUMNS,
        dimension_columns=["BU", "职能", "绩效分位", "级别", "司龄分箱", "年龄分箱", "统计月份"],
        subject_columns=LEGACY_SUBJECT_COLUMNS,
        subject_aliases={
            **COMMON_SUBJECT_ALIASES,
            "底薪": "底薪",
            "基本工资": "基本工资调整",
        },
        dimension_aliases={
            "职级": "级别",
            "绩效档": "绩效分位",
            "司龄": "司龄分箱",
            "年龄": "年龄分箱",
            "月份": "统计月份",
        },
        default_subject="经济补偿金",
        default_secondary_dimensions=["职能"],
        subject_categories=LEGACY_SUBJECT_CATEGORIES,
        ambiguous_terms=LEGACY_AMBIGUOUS_TERMS,
    ),
    "pingan_full": SalarySchema(
        schema_id="pingan_full",
        display_name="中国平安风格全量薪酬宽表",
        wide_columns=PINGAN_FULL_COLUMNS,
        text_dimension_columns=[
            "统计月",
            "员工ID",
            "员工UM",
            "BU",
            "部门",
            "职能序列",
            "职能",
            "级别",
            "去年绩效排名",
            "绩效分位",
            "年龄",
            "年龄分箱",
            "司龄",
            "司龄分箱",
        ],
        numeric_columns=PINGAN_FULL_SUBJECT_COLUMNS,
        dimension_columns=["BU", "部门", "职能序列", "去年绩效排名", "级别", "年龄分箱", "司龄分箱", "统计月份"],
        subject_columns=PINGAN_FULL_SUBJECT_COLUMNS,
        subject_aliases=COMMON_SUBJECT_ALIASES,
        dimension_aliases={
            "职能": "部门",
            "职能条线": "部门",
            "序列": "职能序列",
            "M序列": "职能序列",
            "P序列": "职能序列",
            "T序列": "职能序列",
            "绩效档": "去年绩效排名",
            "绩效分位": "去年绩效排名",
            "绩效排名": "去年绩效排名",
            "年龄": "年龄分箱",
            "司龄": "司龄分箱",
            "月份": "统计月份",
        },
        default_subject="底薪/基本工资",
        default_secondary_dimensions=["部门"],
        subject_categories=PINGAN_FULL_SUBJECT_CATEGORIES,
        ambiguous_terms=PINGAN_FULL_AMBIGUOUS_TERMS,
    ),
}


DEFAULT_SCHEMA_ID = "pingan_full"


def get_schema(schema_id: str | None = None) -> SalarySchema:
    resolved = schema_id or DEFAULT_SCHEMA_ID
    if resolved not in SCHEMA_REGISTRY:
        resolved = DEFAULT_SCHEMA_ID
    return SCHEMA_REGISTRY[resolved]


def detect_schema_from_headers(headers: list[str]) -> SalarySchema:
    header_set = set(headers)
    for schema_id in ("pingan_full", "legacy_simple"):
        schema = SCHEMA_REGISTRY[schema_id]
        if all(column in header_set for column in schema.wide_columns):
            return schema
    missing_by_schema = {
        schema.display_name: [column for column in schema.wide_columns if column not in header_set][:10]
        for schema in SCHEMA_REGISTRY.values()
    }
    raise ValueError(f"CSV 字段无法匹配已知宽表结构：{missing_by_schema}")


def create_runtime_schema(manifest: dict) -> SalarySchema:
    dimensions = list(manifest.get("dimension_columns") or [])
    subjects = list(manifest.get("subject_columns") or [])
    text_dimension_columns = list(manifest.get("text_dimension_columns") or dimensions)
    subject_aliases = dict(COMMON_SUBJECT_ALIASES)
    subject_aliases.update(manifest.get("subject_aliases") or {})
    dimension_aliases = dict(manifest.get("dimension_aliases") or {})

    if not subjects:
        raise ValueError("运行时 schema 缺少科目列。")

    return SalarySchema(
        schema_id=manifest.get("schema_id") or "inferred_runtime",
        display_name=manifest.get("display_name") or "智能识别宽表",
        wide_columns=["统计年度", "统计月份", *text_dimension_columns, *subjects],
        text_dimension_columns=text_dimension_columns,
        numeric_columns=subjects,
        dimension_columns=list(manifest.get("display_dimension_columns") or dimensions),
        subject_columns=subjects,
        subject_aliases=subject_aliases,
        dimension_aliases=dimension_aliases,
        default_subject=manifest.get("default_subject") or subjects[0],
        default_secondary_dimensions=list(manifest.get("default_secondary_dimensions") or dimensions[:1]),
        subject_categories=build_subject_categories(subjects),
        ambiguous_terms=build_ambiguous_terms(subjects),
        schema_mode="inferred",
        source_manifest=manifest,
        source_column_map=dict(manifest.get("source_column_map") or {}),
        synthetic_defaults=dict(manifest.get("synthetic_defaults") or {}),
        capabilities=dict(manifest.get("capabilities") or {}),
    )
