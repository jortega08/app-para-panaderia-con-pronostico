/**
 * echarts-helpers.js
 * Paleta de colores y helpers compartidos para todos los charts de la app.
 * Depende de window.echarts (cargado via CDN en base.html).
 */

window.AppCharts = window.AppCharts || (() => {

    // ── Paleta de marca ──────────────────────────────────────────────────────
    const BRAND_COLORS = ['#E8B44D','#E07A5F','#81B29A','#9B8EA0','#B0BEC5',
                          '#FFB74D','#D4A373','#A8D5B5','#F4A261','#264653'];
    const CAJA_COLORS  = { efectivo: '#4F8C5B', tarjeta: '#D28B36', transferencia: '#8B5E3C' };
    const TEXT_COLOR   = '#5D4037';
    const GRID_COLOR   = '#EFEBE9';

    // ── Tema base compartido ─────────────────────────────────────────────────
    const BASE_TEXT  = { color: TEXT_COLOR, fontSize: 12, fontFamily: 'inherit' };
    const BASE_GRID  = { show: true, lineStyle: { color: GRID_COLOR } };
    const BASE_AXIS  = { axisLine: { lineStyle: { color: GRID_COLOR } },
                         axisLabel: { color: TEXT_COLOR },
                         splitLine: BASE_GRID };

    // ── Registro de instancias activas ────────────────────────────────────────
    const _instances = new Map();

    /**
     * Inicializa o re-inicializa un chart ECharts en el elemento `domId`.
     * Si ya existe una instancia la descarta antes de crear la nueva.
     * @param {string} domId
     * @returns {echarts.ECharts}
     */
    function init(domId) {
        dispose(domId);
        const el = document.getElementById(domId);
        if (!el) return null;
        // Asegurar altura mínima si no se definió en CSS/HTML
        if (!el.style.height && el.offsetHeight < 10) el.style.height = '260px';
        const instance = echarts.init(el);
        _instances.set(domId, instance);
        return instance;
    }

    /** Destruye la instancia asociada a `domId` (si existe). */
    function dispose(domId) {
        const prev = _instances.get(domId);
        if (prev && !prev.isDisposed()) prev.dispose();
        _instances.delete(domId);
    }

    /** Llama resize() en todas las instancias activas (útil en window.resize). */
    function resizeAll() {
        _instances.forEach(inst => { if (inst && !inst.isDisposed()) inst.resize(); });
    }

    // Escucha redimensionado de ventana
    window.addEventListener('resize', () => resizeAll());

    // ── Helpers de option ────────────────────────────────────────────────────

    /** Tooltip básico con formato de número */
    function tooltip(extra = {}) {
        return { trigger: 'item', textStyle: BASE_TEXT, ...extra };
    }

    /** Tooltip para series (axis trigger) */
    function tooltipAxis(extra = {}) {
        return { trigger: 'axis', axisPointer: { type: 'shadow' }, textStyle: BASE_TEXT, ...extra };
    }

    /** Grid estándar */
    function grid(extra = {}) {
        return { left: '4%', right: '4%', bottom: '10%', top: '8%', containLabel: true, ...extra };
    }

    /**
     * Crea la configuración (option) para un gráfico de barras.
     * @param {{labels:string[], values:number[], title?:string, colors?:string[]}} cfg
     */
    function barOption(cfg) {
        const colors = cfg.colors || BRAND_COLORS;
        return {
            color: colors,
            tooltip: tooltipAxis(),
            grid: grid(),
            xAxis: { type: 'category', data: cfg.labels, ...BASE_AXIS,
                     axisTick: { alignWithLabel: true } },
            yAxis: { type: 'value', ...BASE_AXIS, minInterval: 1 },
            series: [{
                type: 'bar',
                data: cfg.values.map((v, i) => ({ value: v, itemStyle: { color: colors[i % colors.length], borderRadius: [6, 6, 0, 0] } })),
                barMaxWidth: 56,
            }],
        };
    }

    /**
     * Crea la configuración para un gráfico de pastel / dona.
     * @param {{labels:string[], values:number[], colors?:string[], radius?:string|string[]}} cfg
     */
    function pieOption(cfg) {
        const colors = cfg.colors || BRAND_COLORS;
        const radius  = cfg.radius || ['40%', '70%'];
        return {
            color: colors,
            tooltip: { ...tooltip(), formatter: (p) => `${p.name}: <b>${p.value}</b> (${p.percent}%)` },
            legend: { bottom: 0, textStyle: BASE_TEXT },
            series: [{
                type: 'pie',
                radius,
                data: cfg.labels.map((l, i) => ({ name: l, value: cfg.values[i] })),
                emphasis: { itemStyle: { shadowBlur: 10, shadowOffsetX: 0, shadowColor: 'rgba(0,0,0,.2)' } },
                label: { show: false },
                labelLine: { show: false },
            }],
        };
    }

    /**
     * Crea la configuración para un gráfico de línea.
     * @param {{labels:string[], series: Array<{name:string,data:number[],color?:string}>, smooth?:boolean}} cfg
     */
    function lineOption(cfg) {
        return {
            color: BRAND_COLORS,
            tooltip: tooltipAxis(),
            legend: { textStyle: BASE_TEXT, bottom: 0 },
            grid: grid({ bottom: '15%' }),
            xAxis: { type: 'category', data: cfg.labels, ...BASE_AXIS,
                     axisLabel: { rotate: cfg.labels.length > 15 ? 35 : 0, color: TEXT_COLOR } },
            yAxis: { type: 'value', ...BASE_AXIS },
            series: cfg.series.map(s => ({
                name: s.name,
                type: 'line',
                data: s.data,
                smooth: cfg.smooth !== false,
                symbol: 'circle',
                symbolSize: 5,
                lineStyle: { width: 2 },
                ...(s.color ? { itemStyle: { color: s.color }, lineStyle: { color: s.color, width: 2 } } : {}),
            })),
        };
    }

    /**
     * Crea la configuración para un gráfico de barras horizontal.
     */
    function barHorizOption(cfg) {
        const colors = cfg.colors || BRAND_COLORS;
        return {
            color: colors,
            tooltip: tooltip({ formatter: (p) => `${p.name}: <b>${p.value}</b>` }),
            grid: grid({ left: '8%' }),
            xAxis: { type: 'value', ...BASE_AXIS, minInterval: 1 },
            yAxis: { type: 'category', data: cfg.labels, ...BASE_AXIS,
                     axisLabel: { color: TEXT_COLOR, width: 100, overflow: 'truncate' } },
            series: [{
                type: 'bar',
                data: cfg.values.map((v, i) => ({ value: v, itemStyle: { color: colors[i % colors.length], borderRadius: [0, 6, 6, 0] } })),
                barMaxWidth: 40,
            }],
        };
    }

    // ── API pública ──────────────────────────────────────────────────────────
    return {
        BRAND_COLORS,
        CAJA_COLORS,
        init,
        dispose,
        resizeAll,
        barOption,
        pieOption,
        lineOption,
        barHorizOption,
        tooltip,
        tooltipAxis,
        grid,
    };
})();
