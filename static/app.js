(function () {
  const stateNode = document.getElementById("initial-state");
  const loadingBanner = document.querySelector("[data-loading-banner]");
  const sampleButton = document.querySelector('[data-action="sample-report"]');
  const copyButtons = document.querySelectorAll('[data-action="copy-report"]');
  const printButtons = document.querySelectorAll('[data-action="print-report"]');
  const multiSelect = document.querySelector("[data-multi-select]");
  const dimensionTabs = document.querySelectorAll("[data-dimension-tab]");
  const dimensionPanels = document.querySelectorAll("[data-dimension-panel]");

  function parseState() {
    if (!stateNode) return {};
    try {
      return JSON.parse(stateNode.textContent || "{}");
    } catch (error) {
      console.warn("Failed to parse initial state JSON.", error);
      return {};
    }
  }

  function syncBadges() {
    const badgeContainer = document.querySelector(".selected-badges");
    if (!badgeContainer || !multiSelect) return;

    const checked = Array.from(
      multiSelect.querySelectorAll('input[type="checkbox"]:checked')
    ).map((input) => input.value);

    badgeContainer.innerHTML = "";
    checked.forEach((value) => {
      const badge = document.createElement("span");
      badge.className = "badge badge--solid";
      badge.textContent = value;
      badgeContainer.appendChild(badge);
    });

    if (!checked.length) {
      const placeholder = document.createElement("span");
      placeholder.className = "badge";
      placeholder.textContent = "请选择至少一个次维度";
      badgeContainer.appendChild(placeholder);
    }
  }

  function applyLoadingState(initialState) {
    if (!loadingBanner) return;
    loadingBanner.classList.toggle("is-hidden", !initialState.loading);
  }

  function injectSampleState() {
    const summaryTitle = document.querySelector(".summary-block h3");
    const summaryNarrative = document.querySelector(".summary-block__content p:last-of-type");
    const priorityCallout = document.querySelector(".summary-block__callout strong");

    if (summaryTitle) {
      summaryTitle.textContent = "经济补偿金在少数 BU 出现结构性偏高，且差异主要由级别与职能双重因素推动。";
    }
    if (summaryNarrative) {
      summaryNarrative.textContent =
        "从 BU 总额、均值、覆盖率和趋势综合看，异常高值并非单一月份噪声，而是由部分 BU 在高职级与特定职能群体中的持续性补偿金发放驱动。建议优先核查异常 BU 的发放口径、组织事件与人员结构。";
    }
    if (priorityCallout) {
      priorityCallout.textContent = "优先锁定异常 BU 的高职级与高敏感职能人群，先做口径复核，再看政策一致性。";
    }
  }

  function formatCompactNumber(value) {
    const number = Number(value || 0);
    if (number >= 100000000) return `${(number / 100000000).toFixed(2)} 亿`;
    if (number >= 10000) return `${(number / 10000).toFixed(1)} 万`;
    return number.toLocaleString("zh-CN");
  }

  function baseOption() {
    return {
      backgroundColor: "transparent",
      textStyle: {
        fontFamily: '"Manrope","Noto Sans SC",sans-serif',
      },
      grid: {
        left: 64,
        right: 24,
        top: 38,
        bottom: 60,
        containLabel: true,
      },
      tooltip: {
        trigger: "item",
        backgroundColor: "rgba(20,31,37,0.92)",
        borderWidth: 0,
        textStyle: { color: "#f7faf9" },
      },
    };
  }

  function buildOption(type, payload) {
    const option = baseOption();
    const valueFormatter = payload.value_type === "percent"
      ? (value) => `${Number(value || 0).toFixed(2)}%`
      : (value) => formatCompactNumber(value);
    if (type === "bar" || type === "grouped-bar") {
      return {
        ...option,
        grid: {
          ...option.grid,
          left: 110,
          bottom: 40,
        },
        xAxis: {
          type: "value",
          axisLabel: { color: "#6f7c83", formatter: (value) => valueFormatter(value) },
          splitLine: { lineStyle: { color: "rgba(29,42,51,0.08)" } },
        },
        yAxis: {
          type: "category",
          data: payload.labels || payload.categories || [],
          axisLabel: { color: "#1d2a33", width: 110, overflow: "truncate" },
          axisTick: { show: false },
        },
        series: [
          {
            type: "bar",
            data: payload.series || [],
            barWidth: 18,
            itemStyle: {
              borderRadius: [0, 10, 10, 0],
              color: new echarts.graphic.LinearGradient(1, 0, 0, 0, [
                { offset: 0, color: "#124e78" },
                { offset: 1, color: "#0e6c74" },
              ]),
            },
          },
        ],
      };
    }
    if (type === "line") {
      return {
        ...option,
        grid: {
          ...option.grid,
          left: 64,
          bottom: 78,
        },
        xAxis: {
          type: "category",
          data: payload.periods || [],
          axisLabel: { color: "#6f7c83", rotate: 32, margin: 16 },
        },
        yAxis: {
          type: "value",
          axisLabel: { color: "#6f7c83", formatter: (value) => valueFormatter(value) },
          splitLine: { lineStyle: { color: "rgba(29,42,51,0.08)" } },
        },
        series: [
          {
            type: "line",
            smooth: true,
            symbolSize: 7,
            data: payload.series || [],
            lineStyle: { width: 3, color: "#0e6c74" },
            itemStyle: { color: "#c69239" },
            areaStyle: {
              color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                { offset: 0, color: "rgba(14,108,116,0.30)" },
                { offset: 1, color: "rgba(14,108,116,0.03)" },
              ]),
            },
          },
        ],
      };
    }
    if (type === "scatter") {
      return {
        ...option,
        grid: {
          ...option.grid,
          left: 74,
          bottom: 56,
        },
        xAxis: {
          type: "value",
          name: "覆盖率",
          axisLabel: { color: "#6f7c83", formatter: "{value}%" },
          splitLine: { lineStyle: { color: "rgba(29,42,51,0.08)" } },
        },
        yAxis: {
          type: "value",
          name: "均值",
          axisLabel: { color: "#6f7c83", formatter: (value) => formatCompactNumber(value) },
          splitLine: { lineStyle: { color: "rgba(29,42,51,0.08)" } },
        },
        series: [
          {
            type: "scatter",
            data: (payload.points || []).map((point) => ({
              name: point.name,
              value: [point.coverage_rate, point.avg_amount, point.employee_count],
            })),
            symbolSize: (value) => Math.max(12, Math.min(42, Math.sqrt(value[2]) / 5)),
            itemStyle: {
              color: "rgba(198,146,57,0.82)",
              borderColor: "#124e78",
              borderWidth: 1,
            },
          },
        ],
      };
    }
    if (type === "heatmap") {
      const rows = payload.rows || [];
      const uniqueBu = [...new Set(rows.map((row) => row.bu))];
      if (uniqueBu.length <= 2) {
        const fallbackLabels = rows
          .slice()
          .sort((a, b) => (b.total_amount || 0) - (a.total_amount || 0))
          .slice(0, 10)
          .map((row) => `${row.bu} / ${row.dimension_value}`);
        const fallbackSeries = rows
          .slice()
          .sort((a, b) => (b.total_amount || 0) - (a.total_amount || 0))
          .slice(0, 10)
          .map((row) => row.total_amount || 0);
        return buildOption("grouped-bar", {
          labels: fallbackLabels,
          series: fallbackSeries,
        });
      }
      const x = [...new Set(rows.map((row) => row.dimension_value))];
      const y = [...new Set(rows.map((row) => row.bu))];
      return {
        ...option,
        grid: {
          ...option.grid,
          left: 88,
          right: 18,
          top: 28,
          bottom: 102,
        },
        xAxis: {
          type: "category",
          data: x,
          axisLabel: { color: "#6f7c83", rotate: 34, interval: 0, margin: 18 },
        },
        yAxis: { type: "category", data: y, axisLabel: { color: "#6f7c83", margin: 12 } },
        visualMap: {
          min: 0,
          max: Math.max(...rows.map((row) => row.total_amount || 0), 1),
          orient: "horizontal",
          left: "center",
          bottom: 22,
          inRange: { color: ["#eef7f3", "#8ac5ba", "#0e6c74"] },
        },
        series: [
          {
            type: "heatmap",
            data: rows.map((row) => [x.indexOf(row.dimension_value), y.indexOf(row.bu), row.total_amount]),
            label: { show: false },
          },
        ],
      };
    }
    if (type === "radar") {
      return {
        ...option,
        radar: {
          indicator: payload.indicators || [],
          radius: "62%",
          splitLine: { lineStyle: { color: "rgba(29,42,51,0.08)" } },
          axisName: { color: "#1d2a33" },
        },
        series: [
          {
            type: "radar",
            data: [
              {
                value: payload.values || [],
                areaStyle: { color: "rgba(14,108,116,0.18)" },
                lineStyle: { color: "#0e6c74", width: 2 },
                itemStyle: { color: "#c69239" },
              },
            ],
          },
        ],
      };
    }
    if (type === "matrix") {
      return {
        ...option,
        grid: {
          ...option.grid,
          left: 96,
          right: 18,
          top: 24,
          bottom: 104,
        },
        xAxis: {
          type: "category",
          data: ["信号强度", "异常数量", "发现数量"],
          axisLabel: { color: "#6f7c83", interval: 0, margin: 16 },
        },
        yAxis: {
          type: "category",
          data: (payload.rows || []).map((row) => row.dimension),
          axisLabel: { color: "#1d2a33", width: 80, overflow: "break" },
        },
        visualMap: {
          min: 0,
          max: Math.max(...(payload.rows || []).map((row) => row.signal_strength || 0), 1),
          orient: "horizontal",
          left: "center",
          bottom: 24,
          inRange: { color: ["#f9f4ea", "#c69239", "#124e78"] },
        },
        series: [
          {
            type: "heatmap",
            data: (payload.rows || []).flatMap((row, index) => [
              [0, index, row.signal_strength],
              [1, index, row.anomaly_count],
              [2, index, row.finding_count],
            ]),
          },
        ],
      };
    }
    return option;
  }

  function renderEcharts() {
    if (!window.echarts) return;
    document.querySelectorAll("[data-echart]").forEach((node) => {
      const type = node.getAttribute("data-chart-type") || "bar";
      const payloadText = node.getAttribute("data-chart-payload") || "{}";
      let payload = {};
      try {
        payload = JSON.parse(payloadText);
      } catch (error) {
        payload = {};
      }
      const chart = echarts.init(node);
      chart.setOption(buildOption(type, payload));
      window.addEventListener("resize", () => chart.resize(), { passive: true });
    });
  }

  async function copyReportText() {
    const reportBody = document.querySelector("[data-full-report-body]");
    if (!reportBody) return;
    const text = reportBody.innerText.trim();
    if (!text) return;
    try {
      await navigator.clipboard.writeText(text);
    } catch (error) {
      console.warn("Copy failed", error);
    }
  }

  function activateDimensionPanel(targetId) {
    dimensionTabs.forEach((tab) => {
      tab.classList.toggle("is-active", tab.getAttribute("data-target") === targetId);
    });
    dimensionPanels.forEach((panel) => {
      panel.classList.toggle("is-hidden", panel.id !== targetId);
    });
    window.dispatchEvent(new Event("resize"));
  }

  const initialState = parseState();
  applyLoadingState(initialState);
  syncBadges();
  renderEcharts();

  if (multiSelect) {
    multiSelect.addEventListener("change", syncBadges);
  }

  if (sampleButton) {
    sampleButton.addEventListener("click", injectSampleState);
  }

  copyButtons.forEach((button) => {
    button.addEventListener("click", copyReportText);
  });

  printButtons.forEach((button) => {
    button.addEventListener("click", () => window.print());
  });

  dimensionTabs.forEach((tab) => {
    tab.addEventListener("click", () => activateDimensionPanel(tab.getAttribute("data-target")));
  });
})();
