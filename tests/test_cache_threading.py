"""
缓存线程安全测试

测试 TTLCache 在多线程并发场景下的正确性和线程安全性。

运行方式：
    python -m pytest tests/test_cache_threading.py -v
"""
import threading
import time
from pathlib import Path
import sys
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from salary_reporting import TTLCache


class TestTTLCacheBasic:
    """基础功能测试"""

    def test_set_and_get(self):
        """写入后立即读取"""
        cache = TTLCache(default_ttl=60)
        cache.set("hello", "world")
        assert cache.get("hello") == "world"

    def test_get_missing_key(self):
        """读取不存在的 key 返回 None"""
        cache = TTLCache(default_ttl=60)
        assert cache.get("nonexistent") is None

    def test_ttl_expiration(self):
        """TTL 过期后读取返回 None"""
        cache = TTLCache(default_ttl=1)
        cache.set("temp_key", "temp_value")
        assert cache.get("temp_key") == "temp_value"
        time.sleep(1.5)
        assert cache.get("temp_key") is None

    def test_custom_ttl(self):
        """自定义 TTL"""
        cache = TTLCache(default_ttl=60)
        cache.set("short_key", "short_value", ttl=1)
        assert cache.get("short_key") == "short_value"
        time.sleep(1.5)
        assert cache.get("short_key") is None

    def test_overwrite_key(self):
        """覆盖写入"""
        cache = TTLCache(default_ttl=60)
        cache.set("key", "old_value")
        cache.set("key", "new_value")
        assert cache.get("key") == "new_value"

    def test_clear(self):
        """清空缓存"""
        cache = TTLCache(default_ttl=60)
        cache.set("key1", "value1")
        cache.set("key2", "value2")
        cache.clear()
        assert cache.get("key1") is None
        assert cache.get("key2") is None

    def test_make_key_deterministic(self):
        """make_key 对相同输入产生相同 hash"""
        cache = TTLCache()
        k1 = cache.make_key("subject", "BU", "2024-01", "2024-12")
        k2 = cache.make_key("subject", "BU", "2024-01", "2024-12")
        assert k1 == k2

    def test_make_key_different_inputs(self):
        """make_key 对不同输入产生不同 hash"""
        cache = TTLCache()
        k1 = cache.make_key("subject1", "BU")
        k2 = cache.make_key("subject2", "BU")
        assert k1 != k2


class TestTTLCacheThreadSafety:
    """线程安全性测试"""

    def test_concurrent_writes_no_data_race(self):
        """多线程并发写入不产生数据竞态"""
        cache = TTLCache(default_ttl=60)
        errors = []

        def worker(worker_id):
            try:
                for i in range(200):
                    key = f"w{worker_id}_k{i}"
                    cache.set(key, f"v{worker_id}_{i}")
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0, f"写入线程抛出异常: {errors}"

    def test_concurrent_reads_consistent(self):
        """多线程并发读取结果一致"""
        cache = TTLCache(default_ttl=60)
        cache.set("shared_key", "shared_value")
        results = []
        errors = []

        def reader():
            try:
                for _ in range(500):
                    val = cache.get("shared_key")
                    if val is not None:
                        results.append(val)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=reader) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        # 所有读到的值都应该是正确值
        assert all(v == "shared_value" for v in results)

    def test_concurrent_mixed_operations(self):
        """多线程混合读写清空操作不崩溃"""
        cache = TTLCache(default_ttl=60)

        def writer(wid):
            for i in range(100):
                cache.set(f"w{wid}_{i}", f"v{wid}_{i}")
                time.sleep(0.0005)

        def reader():
            for i in range(300):
                cache.get(f"w0_{i % 100}")
                time.sleep(0.0003)

        def cleaner():
            for _ in range(20):
                cache.clear()
                time.sleep(0.005)

        threads = (
            [threading.Thread(target=writer, args=(i,)) for i in range(4)]
            + [threading.Thread(target=reader) for _ in range(4)]
            + [threading.Thread(target=cleaner)]
        )
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        # 不崩溃即通过

    def test_global_cache_singleton_thread_safe(self):
        """全局 _cache 单例在多线程下安全"""
        from salary_reporting import _cache
        errors = []

        def use_global_cache(tid):
            try:
                key = _cache.make_key(f"thread_{tid}", "test")
                _cache.set(key, {"data": tid})
                val = _cache.get(key)
                assert val is not None
                assert val.get("data") == tid
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=use_global_cache, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0, f"全局缓存线程安全测试失败: {errors}"
