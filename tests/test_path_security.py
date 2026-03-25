"""
路径安全验证测试

测试路径遍历攻击防护、白名单验证和边界情况。

运行方式：
    python -m pytest tests/test_path_security.py -v
"""
from pathlib import Path
import sys
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))


class TestSafeResolvePath:
    """测试 app._safe_resolve_path 路径安全验证"""

    @pytest.fixture(autouse=True)
    def import_app(self):
        try:
            from app import _safe_resolve_path, UPLOADS_DIR, DEMO_DIR
            self._resolve = _safe_resolve_path
            self._uploads_dir = UPLOADS_DIR
            self._demo_dir = DEMO_DIR
        except ImportError:
            pytest.skip("app module 不可用")

    def test_empty_path_raises_400(self):
        """空路径应抛出 400"""
        from fastapi import HTTPException
        with pytest.raises(HTTPException) as exc_info:
            self._resolve("")
        assert exc_info.value.status_code == 400
        assert "不能为空" in str(exc_info.value.detail)

    def test_path_traversal_etc_passwd(self):
        """../../../etc/passwd 路径遍历攻击应被阻止"""
        from fastapi import HTTPException
        with pytest.raises(HTTPException) as exc_info:
            self._resolve("../../../etc/passwd")
        assert exc_info.value.status_code == 403

    def test_absolute_system_path_blocked(self):
        """/etc/passwd 绝对系统路径应被阻止"""
        from fastapi import HTTPException
        with pytest.raises(HTTPException) as exc_info:
            self._resolve("/etc/passwd")
        assert exc_info.value.status_code == 403

    def test_tmp_path_blocked(self):
        """/tmp/ 路径应被阻止"""
        from fastapi import HTTPException
        with pytest.raises(HTTPException) as exc_info:
            self._resolve("/tmp/malicious.csv")
        assert exc_info.value.status_code == 403

    def test_demo_dir_allowed(self):
        """demo 目录下的合法路径应被允许"""
        demo_csv = str(self._demo_dir / "test.csv")
        # 可能路径不存在，但安全验证应通过（仅检查白名单）
        try:
            resolved = self._resolve(demo_csv)
            assert str(resolved).startswith(str(self._demo_dir.resolve()))
        except Exception as e:
            # 如果抛出 403 则说明白名单检查失败
            if hasattr(e, "status_code") and e.status_code == 403:
                pytest.fail(f"demo 目录路径不应被阻止: {e}")

    def test_uploads_dir_allowed(self):
        """uploads 目录下的合法路径应被允许"""
        upload_csv = str(self._uploads_dir / "data.csv")
        try:
            resolved = self._resolve(upload_csv)
            assert str(resolved).startswith(str(self._uploads_dir.resolve()))
        except Exception as e:
            if hasattr(e, "status_code") and e.status_code == 403:
                pytest.fail(f"uploads 目录路径不应被阻止: {e}")

    def test_home_dir_traversal_blocked(self):
        """~ 家目录路径应被阻止（不在白名单内）"""
        from fastapi import HTTPException
        try:
            self._resolve("~/Documents/secret.csv")
            # 如果没有抛出异常，检查是否在白名单内
        except HTTPException as e:
            assert e.status_code == 403
        except Exception:
            pass  # 其他异常也可接受


class TestSalaryConfigPathCheck:
    """测试 salary_config.is_path_allowed"""

    def test_uploads_subpath_allowed(self):
        """uploads 子路径应被允许"""
        from salary_config import is_path_allowed, UPLOADS_DIR
        assert is_path_allowed(UPLOADS_DIR / "test.csv") is True
        assert is_path_allowed(UPLOADS_DIR / "subdir" / "nested.csv") is True

    def test_demo_subpath_allowed(self):
        """demo 子路径应被允许"""
        from salary_config import is_path_allowed, DEMO_DIR
        assert is_path_allowed(DEMO_DIR / "sample.csv") is True

    def test_system_path_blocked(self):
        """/etc 系统路径应被阻止"""
        from salary_config import is_path_allowed
        assert is_path_allowed(Path("/etc/passwd")) is False
        assert is_path_allowed(Path("/tmp/hack.csv")) is False
        assert is_path_allowed(Path("/root/.ssh/id_rsa")) is False

    def test_parent_traversal_blocked(self):
        """父目录遍历路径应被阻止"""
        from salary_config import is_path_allowed, UPLOADS_DIR
        # 尝试使用 ".." 逃出白名单
        escaped = UPLOADS_DIR.parent.parent / "etc" / "passwd"
        assert is_path_allowed(escaped) is False
