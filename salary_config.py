"""
薪酬分析系统配置集中管理。

版本：v1.0
更新时间：2025-03-24
说明：统一管理所有环境变量和系统配置，支持安全验证和默认值。
"""
import os
from pathlib import Path

# ==================== 基础路径配置 ====================
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "data" / "salary_analysis.db"
UPLOADS_DIR = BASE_DIR / "uploads"
DEMO_DIR = BASE_DIR / "demo"

# ==================== LLM 配置 ====================
LLM_API_KEY = os.getenv("OPENAI_API_KEY", "")
LLM_BASE_URL = os.getenv("OPENAI_BASE_URL", "")
LLM_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
LLM_DEFAULT_MODEL = "gpt-4.1-mini"

# ==================== 外部搜索配置 ====================
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY", "")

# ==================== 安全配置 ====================
JWT_SECRET = os.getenv("JWT_SECRET", "")
JWT_SECRET_REQUIRED = os.getenv("JWT_SECRET_REQUIRED", "false").lower() == "true"
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")
AUTH_ENABLED = os.getenv("AUTH_ENABLED", "false").lower() == "true"

# 数据目录白名单（防止路径遍历攻击）
# 支持通过 ALLOWED_DATA_DIRS 环境变量扩展白名单，逗号分隔
_ALLOWED_DATA_DIRS_RAW = os.getenv("ALLOWED_DATA_DIRS", "")
ALLOWED_DATA_DIRS = [
    BASE_DIR / "uploads",
    BASE_DIR / "demo",
    *(Path(d).expanduser().resolve() for d in _ALLOWED_DATA_DIRS_RAW.split(",") if d.strip()),
]

# ==================== 缓存配置 ====================
CACHE_DEFAULT_TTL = int(os.getenv("CACHE_TTL", "300"))  # 默认 5 分钟
CACHE_MAX_SIZE = int(os.getenv("CACHE_MAX_SIZE", "1000"))

# ==================== 并发配置 ====================
MAX_CONCURRENT_LLM_CALLS = int(os.getenv("MAX_CONCURRENT_LLM_CALLS", "4"))
UPLOAD_MAX_FILE_SIZE = int(os.getenv("UPLOAD_MAX_FILE_SIZE", str(100 * 1024 * 1024)))  # 默认 100MB

# ==================== 性能配置 ====================
# 批量插入的行数
BATCH_INSERT_SIZE = int(os.getenv("BATCH_INSERT_SIZE", "5000"))

# ==================== 配置验证 ====================
def validate_config() -> list[str]:
    """
    验证配置的有效性，返回错误列表。
    空列表表示所有配置有效。
    """
    errors = []

    # JWT 安全验证
    if JWT_SECRET_REQUIRED and not JWT_SECRET:
        errors.append(
            "JWT_SECRET_REQUIRED=true 但未配置 JWT_SECRET 环境变量。"
            "请在生产环境中设置强密钥。"
        )

    # 目录存在性验证
    for dir_path in [UPLOADS_DIR, DEMO_DIR]:
        if not dir_path.exists():
            errors.append(
                f"配置的目录不存在：{dir_path}。请先创建该目录。"
            )

    # LLM 配置验证（仅在有 API Key 时警告）
    if not LLM_API_KEY and LLM_BASE_URL:
        errors.append(
            "配置了 OPENAI_BASE_URL 但未配置 OPENAI_API_KEY，"
            "LLM 功能将不可用。"
        )

    return errors


def print_config_status() -> None:
    """打印当前配置状态（用于启动日志）。"""
    print("=" * 60)
    print("薪酬分析系统配置状态")
    print("=" * 60)

    # 路径配置
    print(f"✓ 数据库路径: {DB_PATH}")
    print(f"✓ 上传目录: {UPLOADS_DIR}")
    print(f"✓ 演示数据目录: {DEMO_DIR}")

    # LLM 配置
    llm_status = "已启用" if LLM_API_KEY else "未启用（将回退到模板模式）"
    print(f"✓ LLM 服务: {llm_status}")
    if LLM_API_KEY:
        print(f"  - 模型: {LLM_MODEL}")
        if LLM_BASE_URL:
            print(f"  - API 地址: {LLM_BASE_URL}")

    # 安全配置
    auth_status = "已启用" if AUTH_ENABLED else "未启用"
    print(f"✓ 身份认证: {auth_status}")
    if JWT_SECRET_REQUIRED:
        print(f"  - JWT Secret: {'已配置' if JWT_SECRET else '未配置'}")

    # 缓存配置
    print(f"✓ 缓存 TTL: {CACHE_DEFAULT_TTL}秒")
    print(f"✓ 最大缓存条目: {CACHE_MAX_SIZE}")

    # 并发配置
    print(f"✓ 最大并发 LLM 调用: {MAX_CONCURRENT_LLM_CALLS}")
    print(f"✓ 上传文件大小限制: {UPLOAD_MAX_FILE_SIZE // (1024*1024)}MB")

    # 白名单目录
    print(f"✓ 允许访问的数据目录 ({len(ALLOWED_DATA_DIRS)}个):")
    for d in ALLOWED_DATA_DIRS:
        print(f"  - {d}")

    print("=" * 60)

    # 验证配置
    errors = validate_config()
    if errors:
        print("⚠️  配置警告：")
        for error in errors:
            print(f"  - {error}")
        print("=" * 60)


# ==================== 导出的便捷函数 ====================

def get_data_dir_whitelist() -> list[Path]:
    """
    获取允许访问的数据目录白名单。

    Returns:
        Path 对象列表，经过解析和去重。
    """
    # 去重（通过绝对路径）
    seen = set()
    unique_dirs = []
    for d in ALLOWED_DATA_DIRS:
        resolved = d.resolve()
        abs_path = str(resolved)
        if abs_path not in seen:
            seen.add(abs_path)
            unique_dirs.append(resolved)
    return unique_dirs


def is_path_allowed(resolved_path: Path) -> bool:
    """
    检查解析后的路径是否在允许的数据目录白名单内。

    Args:
        resolved_path: 已解析的绝对路径

    Returns:
        True 表示允许访问，False 表示禁止。
    """
    try:
        resolved_path = resolved_path.resolve()
        for allowed_dir in get_data_dir_whitelist():
            try:
                # 检查路径是否在允许目录下
                resolved_path.relative_to(allowed_dir)
                return True
            except ValueError:
                continue
        return False
    except Exception:
        return False


# ==================== 模块初始化 ====================
if __name__ == "__main__":
    # 直接运行时打印配置状态
    print_config_status()
