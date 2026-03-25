"""
薪酬分析系统优化测试套件

测试内容：
1. TTLCache 线程安全测试
2. 路径安全验证测试
3. 配置验证测试
4. Prompt 模块导入测试
5. 并发维度分析测试

运行方式：
    python -m pytest tests/test_optimizations.py -v
"""
import pytest
import threading
import time
from pathlib import Path
import sys

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))


class TestTTLCacheThreadSafety:
    """测试 TTLCache 线程安全性"""

    def test_concurrent_get_set(self):
        """测试并发读写操作"""
        from salary_reporting import TTLCache

        cache = TTLCache(default_ttl=60)
        results = []
        errors = []

        def worker(worker_id):
            try:
                # 每个线程写入自己的 key
                for i in range(100):
                    cache.set(f"key_{worker_id}_{i}", f"value_{worker_id}_{i}")

                # 读取所有 key
                for i in range(100):
                    value = cache.get(f"key_{worker_id}_{i}")
                    assert value == f"value_{worker_id}_{i}"

                results.append(True)
            except Exception as e:
                errors.append(e)

        # 创建 10 个并发线程
        threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0, f"并发操作出错: {errors}"
        assert len(results) == 10, f"部分线程未完成: {len(results)}/10"

    def test_ttl_expiration(self):
        """测试 TTL 过期"""
        from salary_reporting import TTLCache

        cache = TTLCache(default_ttl=1)
        cache.set("test_key", "test_value")

        # 立即读取，应该能读到
        assert cache.get("test_key") == "test_value"

        # 等待 2 秒后应该过期
        time.sleep(2)
        assert cache.get("test_key") is None

    def test_concurrent_clear(self):
        """测试并发清空操作"""
        from salary_reporting import TTLCache

        cache = TTLCache(default_ttl=60)

        def clear_worker():
            for _ in range(100):
                cache.clear()
                time.sleep(0.001)

        def read_worker(worker_id):
            for i in range(100):
                cache.set(f"key_{worker_id}_{i}", f"value_{worker_id}_{i}")
                time.sleep(0.001)

        # 启动并发操作
        threads = [threading.Thread(target=clear_worker)] + \
                  [threading.Thread(target=read_worker, args=(i,)) for i in range(5)]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # 不应该崩溃，无断言即可通过


class TestPathSecurity:
    """测试路径安全验证"""

    def test_allowed_paths(self):
        """测试允许的路径"""
        try:
            from app import _safe_resolve_path, UPLOADS_DIR, DEMO_DIR

            # 测试 uploads 目录下的绝对路径（白名单内）
            upload_path = str(UPLOADS_DIR / "test.csv")
            resolved = _safe_resolve_path(upload_path)
            assert resolved == (UPLOADS_DIR / "test.csv").resolve()

            # 测试 demo 目录下的路径
            demo_path = str(DEMO_DIR / "demo.csv")
            resolved = _safe_resolve_path(demo_path)
            assert resolved == (DEMO_DIR / "demo.csv").resolve()

        except ImportError:
            pytest.skip("app module 不可用")

    def test_path_traversal_attack(self):
        """测试路径遍历攻击"""
        try:
            from app import _safe_resolve_path
            from fastapi import HTTPException

            # 测试 ../ 攻击
            with pytest.raises(HTTPException) as exc_info:
                _safe_resolve_path("../../../etc/passwd")
            assert "白名单" in str(exc_info.value.detail)

            # 测试绝对路径攻击
            with pytest.raises(HTTPException) as exc_info:
                _safe_resolve_path("/etc/passwd")
            assert "白名单" in str(exc_info.value.detail)

        except ImportError:
            pytest.skip("app module 不可用")

    def test_empty_path(self):
        """测试空路径"""
        try:
            from app import _safe_resolve_path
            from fastapi import HTTPException

            with pytest.raises(HTTPException) as exc_info:
                _safe_resolve_path("")
            assert "不能为空" in str(exc_info.value.detail)

        except ImportError:
            pytest.skip("app module 不可用")


class TestConfigValidation:
    """测试配置验证"""

    def test_config_validation_dev_mode(self):
        """测试开发环境配置验证"""
        from salary_config import validate_config, LLM_API_KEY

        # 开发环境不需要 JWT_SECRET
        errors = validate_config()
        # 可能有警告，但不应该是致命错误
        assert len(errors) == 0 or "JWT_SECRET" not in str(errors[0])

    def test_path_allowed_check(self):
        """测试路径白名单检查"""
        from salary_config import is_path_allowed, UPLOADS_DIR

        # 测试允许的路径
        assert is_path_allowed(UPLOADS_DIR / "test.csv") is True
        assert is_path_allowed(UPLOADS_DIR / "subdir" / "test.csv") is True

        # 测试不允许的路径
        assert is_path_allowed(Path("/etc/passwd")) is False
        assert is_path_allowed(Path("/tmp/test.csv")) is False


class TestPromptModule:
    """测试 Prompt 模块"""

    def test_prompt_imports(self):
        """测试 Prompt 模块导入"""
        try:
            from salary_prompts import (
                build_dimension_prompt,
                build_consolidated_prompt,
                build_external_research_prompt,
                build_report_revision_prompt,
                build_short_answer_prompt,
                SYSTEM_DIMENSION_ANALYSIS,
                SYSTEM_CONSOLIDATED_ANALYSIS,
                SYSTEM_REPORT_REVISION,
                SYSTEM_SHORT_ANSWER,
                SYSTEM_EXTERNAL_RESEARCH,
                SYSTEM_FULL_REPORT,
                get_prompt_version,
                list_prompt_versions,
            )
            assert get_prompt_version() == "v1.0"
            assert "v1.0" in list_prompt_versions()
            assert isinstance(SYSTEM_DIMENSION_ANALYSIS, str)
            assert len(SYSTEM_DIMENSION_ANALYSIS) > 0

        except ImportError:
            pytest.skip("salary_prompts module 不可用")

    def test_prompt_version(self):
        """测试 Prompt 版本管理"""
        try:
            from salary_prompts import get_prompt_version, list_prompt_versions

            version = get_prompt_version()
            assert version.startswith("v")
            versions = list_prompt_versions()
            assert version in versions
            assert len(versions) > 0

        except ImportError:
            pytest.skip("salary_prompts module 不可用")


class TestConcurrentAnalysis:
    """测试并发维度分析"""

    def test_concurrent_analysis_function_exists(self):
        """测试并发分析函数是否存在"""
        from salary_reporting import analyze_dimensions_concurrent
        assert callable(analyze_dimensions_concurrent)

    def test_concurrent_analysis_without_llm(self):
        """测试无 LLM 时的并发分析（降级模式）"""
        from salary_reporting import analyze_dimensions_concurrent, LLMService, AnalysisRequest

        # 创建一个禁用 LLM 的服务
        llm_service = LLMService()
        # 如果未配置 API key，自动降级到模板模式

        # 创建模拟请求和洞察
        request = AnalysisRequest(
            subject="测试科目",
            primary_dimension="BU",
            secondary_dimensions=["级别"],
            start_year=2024,
            start_month=1,
            end_year=2024,
            end_month=12,
            metrics=["总额"],
            question="",
        )

        insights = [
            {
                "dimension": "级别",
                "derived_summary": {
                    "headline": "测试标题",
                    "facts": ["测试事实"],
                    "drivers": ["测试驱动"],
                    "management_implications": ["测试建议"],
                },
                "chart_bundle": {},
                "anomalies": [],
            }
        ]

        # 执行并发分析（应该降级到模板模式）
        reports = analyze_dimensions_concurrent(request, insights, llm_service)
        assert len(reports) == len(insights)
        assert all(r.get("dimension") == insights[0]["dimension"] for r in reports)


class TestRequirements:
    """测试依赖文件"""

    def test_requirements_exists(self):
        """测试 requirements.txt 存在"""
        req_file = Path(__file__).parent.parent / "requirements.txt"
        assert req_file.exists()

    def test_requirements_content(self):
        """测试 requirements.txt 内容"""
        req_file = Path(__file__).parent.parent / "requirements.txt"
        content = req_file.read_text()

        # 检查关键依赖
        assert "fastapi" in content
        assert "uvicorn" in content
        assert "openai" in content
        assert "python-multipart" in content
        assert "cachetools" in content


class TestNewModulesExist:
    """测试新模块文件存在"""

    def test_salary_config_exists(self):
        """测试 salary_config.py 存在"""
        config_file = Path(__file__).parent.parent / "salary_config.py"
        assert config_file.exists()

    def test_salary_prompts_exists(self):
        """测试 salary_prompts.py 存在"""
        prompts_file = Path(__file__).parent.parent / "salary_prompts.py"
        assert prompts_file.exists()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
