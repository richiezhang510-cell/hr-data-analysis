#!/usr/bin/env python3
"""
数据优化脚本 - Phase 2
目标：
1. 精简数据量：从205万行减少到50-100万行
2. 构造3个"故事"，让报告更有针对性和说服力
"""

import pandas as pd
from datetime import datetime
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
DATASET_PATH = SCRIPT_DIR / "薪酬数据_宽表_202412_202512_平安仿真_1999998行.csv"

def main():
    print("=" * 60)
    print("数据优化脚本 - Phase 2")
    print("=" * 60)

    # 1. 读取原始数据
    print("\n[1/7] 读取原始数据...")
    df = pd.read_csv(DATASET_PATH)
    print(f"  原始数据行数: {len(df):,}")
    print(f"  原始数据列数: {len(df.columns)}")

    # 2. 备份原始数据
    print("\n[2/7] 备份原始数据...")
    backup_filename = SCRIPT_DIR / f'薪酬数据_宽表_原始备份_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    df.to_csv(backup_filename, index=False)
    print(f"  备份文件: {backup_filename.name}")

    # 3. 精简数据量：随机采样50%
    print("\n[3/7] 精简数据量（随机采样50%）...")
    df_sampled = df.sample(frac=0.5, random_state=42)
    print(f"  采样后数据行数: {len(df_sampled):,}")

    # 4. 构造故事1：寿险CD类产品线在2026-06到2026-08集中爆发
    print("\n[4/7] 构造故事1：寿险CD类产品线在2026年6-8月集中爆发...")
    mask_story1 = (
        (df_sampled['BU'] == '平安寿险') &
        (df_sampled['级别'] == 'CD类员工') &
        (df_sampled['职能'] == '产品') &
        (df_sampled['司龄分箱'] == '10年以上') &
        (df_sampled['年龄分箱'] == '35-40') &
        (df_sampled['绩效分位'] == '前20%') &
        (df_sampled['统计年度'] == 2026) &
        (df_sampled['统计月份'].isin([6, 7, 8]))
    )

    affected_rows_1 = mask_story1.sum()
    if affected_rows_1 > 0:
        df_sampled.loc[mask_story1, '经济补偿金'] = df_sampled.loc[mask_story1, '经济补偿金'] * 2.5
        print(f"  ✓ 故事1影响行数: {affected_rows_1:,}")
        print(f"  ✓ 补偿金倍数: 2.5x")
    else:
        print(f"  ⚠ 警告：未找到符合条件的数据行")

    # 5. 构造故事2：科技部门在2026-03结构优化
    print("\n[5/7] 构造故事2：科技部门在2026年3月结构优化...")
    mask_story2 = (
        (df_sampled['BU'] == '平安科技') &
        (df_sampled['职能'] == '技术') &
        (df_sampled['级别'] == 'B类') &
        (df_sampled['司龄分箱'] == '3-5') &
        (df_sampled['绩效分位'] == '后30%') &
        (df_sampled['统计年度'] == 2026) &
        (df_sampled['统计月份'] == 3)
    )

    affected_rows_2 = mask_story2.sum()
    if affected_rows_2 > 0:
        df_sampled.loc[mask_story2, '经济补偿金'] = df_sampled.loc[mask_story2, '经济补偿金'] * 1.8
        print(f"  ✓ 故事2影响行数: {affected_rows_2:,}")
        print(f"  ✓ 补偿金倍数: 1.8x")
    else:
        print(f"  ⚠ 警告：未找到符合条件的数据行")

    # 6. 构造故事3：健康险在2026-12高管协议离职
    print("\n[6/7] 构造故事3：健康险在2026年12月高管协议离职...")
    mask_story3 = (
        (df_sampled['BU'] == '平安健康险') &
        (df_sampled['级别'] == 'O类领导') &
        (df_sampled['统计年度'] == 2026) &
        (df_sampled['统计月份'] == 12)
    )

    affected_rows_3 = mask_story3.sum()
    if affected_rows_3 > 0:
        df_sampled.loc[mask_story3, '经济补偿金'] = df_sampled.loc[mask_story3, '经济补偿金'] * 5.0
        print(f"  ✓ 故事3影响行数: {affected_rows_3:,}")
        print(f"  ✓ 补偿金倍数: 5.0x")
    else:
        print(f"  ⚠ 警告：未找到符合条件的数据行")

    # 7. 保存优化后的数据
    print("\n[7/7] 保存优化后的数据...")
    df_sampled.to_csv(DATASET_PATH, index=False)
    print(f"  ✓ 已覆盖原文件: {DATASET_PATH.name}")

    # 8. 输出统计信息
    print("\n" + "=" * 60)
    print("优化后数据统计")
    print("=" * 60)
    print(f"总行数: {len(df_sampled):,}")
    print(f"总补偿金: ¥{df_sampled['经济补偿金'].sum():,.0f}")
    print(f"平均补偿金: ¥{df_sampled['经济补偿金'].mean():,.2f}")

    print("\n各BU补偿金总额（Top 5）:")
    bu_summary = df_sampled.groupby('BU')['经济补偿金'].sum().sort_values(ascending=False).head(5)
    for bu, amount in bu_summary.items():
        print(f"  {bu}: ¥{amount:,.0f}")

    print("\n2026年各月补偿金总额:")
    month_summary = df_sampled[df_sampled['统计年度'] == 2026].groupby('统计月份')['经济补偿金'].sum()
    for month, amount in month_summary.items():
        print(f"  2026-{month:02d}: ¥{amount:,.0f}")

    # 9. 验证故事效果
    print("\n" + "=" * 60)
    print("故事效果验证")
    print("=" * 60)

    # 故事1验证
    if affected_rows_1 > 0:
        story1_avg = df_sampled.loc[mask_story1, '经济补偿金'].mean()
        overall_avg = df_sampled['经济补偿金'].mean()
        print(f"\n故事1（寿险CD类产品线 2026年6-8月）:")
        print(f"  人均补偿金: ¥{story1_avg:,.2f}")
        print(f"  全员平均: ¥{overall_avg:,.2f}")
        print(f"  倍数: {story1_avg/overall_avg:.2f}x")

    # 故事2验证
    if affected_rows_2 > 0:
        story2_avg = df_sampled.loc[mask_story2, '经济补偿金'].mean()
        print(f"\n故事2（科技B类技术 2026年3月）:")
        print(f"  人均补偿金: ¥{story2_avg:,.2f}")
        print(f"  全员平均: ¥{overall_avg:,.2f}")
        print(f"  倍数: {story2_avg/overall_avg:.2f}x")

    # 故事3验证
    if affected_rows_3 > 0:
        story3_avg = df_sampled.loc[mask_story3, '经济补偿金'].mean()
        print(f"\n故事3（健康险O类领导 2026年12月）:")
        print(f"  人均补偿金: ¥{story3_avg:,.2f}")
        print(f"  全员平均: ¥{overall_avg:,.2f}")
        print(f"  倍数: {story3_avg/overall_avg:.2f}x")

    print("\n" + "=" * 60)
    print("数据优化完成！")
    print("=" * 60)
    print(f"\n备份文件: {backup_filename.name}")
    print(f"优化后文件: {DATASET_PATH.name}")
    print("\n下一步:")
    print("1. 重启后端服务，让新数据生效")
    print("2. 生成一份报告，检查'故事'是否在报告中体现")

if __name__ == '__main__':
    main()
