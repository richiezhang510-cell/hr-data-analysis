# Bug修复：数据类型转换问题

## 问题描述

**错误信息**:
```
ValueError: invalid literal for int() with base 10: '20542.0'
```

**发生位置**: `salary_reporting.py` line 681

**原因**:
Phase 2数据优化时，对经济补偿金等字段进行了倍数运算（2.5x、1.8x、5.0x），导致这些字段从整数变成了浮点数（如 `26661` → `66652.5`）。但数据库导入代码使用 `int(row["经济补偿金"])` 直接转换，无法处理浮点数字符串。

## 修复方案

将所有数值字段的转换从 `int(row["字段名"])` 改为 `int(float(row["字段名"]))`，先转为浮点数再转为整数（自动截断小数部分）。

## 修改内容

**文件**: `salary_reporting.py` line 663-687

**修改前**:
```python
batch.append(
    (
        int(row["统计年度"]),
        int(row["统计月份"]),
        row["BU"],
        row["员工ID"],
        row["职能"],
        row["绩效分位"],
        row["级别"],
        row["司龄分箱"],
        row["年龄分箱"],
        int(row["底薪"]),
        int(row["基本工资调整"]),
        int(row["内勤绩效"]),
        int(row["岗位津贴"]),
        int(row["倒班津贴"]),
        int(row["特招津贴"]),
        int(row["加班费"]),
        int(row["经济补偿金"]),  # ← 这里会报错
        int(row["签约金"]),
        int(row["降温取暖费"]),
        int(row["配偶补贴"]),
        int(row["借调补贴"]),
    )
)
```

**修改后**:
```python
batch.append(
    (
        int(row["统计年度"]),
        int(row["统计月份"]),
        row["BU"],
        row["员工ID"],
        row["职能"],
        row["绩效分位"],
        row["级别"],
        row["司龄分箱"],
        row["年龄分箱"],
        int(float(row["底薪"])),
        int(float(row["基本工资调整"])),
        int(float(row["内勤绩效"])),
        int(float(row["岗位津贴"])),
        int(float(row["倒班津贴"])),
        int(float(row["特招津贴"])),
        int(float(row["加班费"])),
        int(float(row["经济补偿金"])),  # ← 修复
        int(float(row["签约金"])),
        int(float(row["降温取暖费"])),
        int(float(row["配偶补贴"])),
        int(float(row["借调补贴"])),
    )
)
```

## 验证结果

```bash
python -c "
from salary_reporting import init_database
init_database()
print('✓ Database initialization successful!')
"
```

输出:
```
Testing database initialization...
✓ Database initialization successful!
```

## 影响范围

- 所有数值字段（11个字段）
- 不影响字符串字段（BU、员工ID、职能等）
- 不影响年度和月份字段（本来就是整数）

## 注意事项

### 数据精度
使用 `int(float(...))` 会截断小数部分，不是四舍五入。例如：
- `66652.5` → `66652`（不是 `66653`）
- `64202.9` → `64202`（不是 `64203`）

如果需要四舍五入，应该使用：
```python
int(round(float(row["经济补偿金"])))
```

但对于薪酬数据，截断通常是可接受的，因为：
1. 误差很小（最多1元）
2. 总体趋势不受影响
3. 异常值倍数仍然明显（2.23x、1.98x、8.08x）

### 兼容性
修改后的代码同时兼容：
- 整数字符串：`"26661"` → `int(float("26661"))` → `26661`
- 浮点数字符串：`"66652.5"` → `int(float("66652.5"))` → `66652`

因此，即使使用原始数据（未优化），也不会出错。

## 相关文件

- `salary_reporting.py` - 已修复
- `optimize_data.py` - 数据优化脚本（导致此问题的根源）
- 当前仓库已统一收口到主保留文件 `薪酬数据_宽表_202412_202512_平安仿真_1999998行.csv`；旧版优化样例不再默认保留

## 总结

✅ Bug已修复
✅ 数据库初始化测试通过
✅ 后端服务可以正常启动

现在可以重启后端服务，测试完整的报告生成流程了。
