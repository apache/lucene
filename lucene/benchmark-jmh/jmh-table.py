#!/usr/bin/env python3
"""Parse JMH JSON output from stdin, produce an interactive HTML table on stdout.

Supports both JSON (-rf json) and plain text JMH output.
With JSON input, clicking a cell shows a histogram of the raw iteration samples
and the benchmark method source code.

Usage:
  # JSON (recommended – enables histograms + source):
  java --module-path ... --module org.apache.lucene.benchmark.jmh ScoreDocSortBenchmark \
    -rf json -rff results.json \
    && python3 jmh-table.py [BenchmarkSource.java] < results.json > results.html

  # Plain text (no histograms):
  java --module-path ... --module org.apache.lucene.benchmark.jmh ScoreDocSortBenchmark \
    | python3 jmh-table.py > results.html

  The optional positional argument is the path to the Java source file containing
  the @Benchmark methods. If provided, clicking a cell also shows the method source.
"""

import sys
import re
import json
import html
import math


def parse_jmh_text(text):
    """Parse plain-text JMH output."""
    entries = []
    for line in text.splitlines():
        m = re.match(
            r'\S+\.(\S+)\s+'
            r'(\S+)\s+'
            r'\S+\s+'
            r'\d+\s+'
            r'(\S+)\s+'
            r'.\s+'
            r'(\S+)\s+'
            r'(\S+)',
            line,
        )
        if m:
            method, param, score, error, unit = m.groups()
            entries.append({
                'method': method,
                'param': param,
                'score': float(score),
                'error': float(error),
                'unit': unit,
                'raw': [],
            })
    return entries, {}


def parse_jmh_json(data):
    """Parse JMH JSON output. Returns (entries, config_dict)."""
    entries = []
    config = {}
    for i, result in enumerate(data):
        bench = result['benchmark'].rsplit('.', 1)[-1]
        params = result.get('params', {})
        # Handle multiple params: ScoreDocSortBenchmark uses 'size' and 'distribution'
        size = params.get('size', '')
        dist = params.get('distribution', 'random')
        pm = result['primaryMetric']
        raw = []
        for fork_data in pm.get('rawData', []):
            raw.extend(fork_data)
        entries.append({
            'method': bench,
            'size': size,
            'dist': dist,
            'score': pm['score'],
            'error': pm['scoreError'],
            'unit': pm['scoreUnit'],
            'raw': raw,
        })
        if i == 0:
            mode_map = {'avgt': 'Average Time', 'thrpt': 'Throughput',
                        'sample': 'Sampling', 'ss': 'Single Shot'}
            # split jvmArgs into harness args (module-path, module-main)
            # vs benchmark args (user/annotation provided like -Xmx, -XX:)
            all_jvm_args = result.get('jvmArgs', [])
            harness_prefixes = ('--module-path', '-Djdk.module.main', '-Djmh.')
            harness_args = [a for a in all_jvm_args
                            if any(a.startswith(p) for p in harness_prefixes)]
            benchmark_args = [a for a in all_jvm_args
                              if not any(a.startswith(p) for p in harness_prefixes)]
            config = {
                'mode': mode_map.get(result.get('mode', ''), result.get('mode', '?')),
                'forks': result.get('forks', '?'),
                'threads': result.get('threads', '?'),
                'warmupIterations': result.get('warmupIterations', '?'),
                'warmupTime': result.get('warmupTime', '?'),
                'measurementIterations': result.get('measurementIterations', '?'),
                'measurementTime': result.get('measurementTime', '?'),
                'harnessJvmArgs': harness_args,
                'benchmarkJvmArgs': benchmark_args,
                'jvm': result.get('jvm', ''),
                'jdkVersion': result.get('jdkVersion', ''),
                'vmName': result.get('vmName', ''),
                'vmVersion': result.get('vmVersion', ''),
                'jmhVersion': result.get('jmhVersion', ''),
            }
    return entries, config


def extract_methods(source_path):
    """Extract @Benchmark method bodies and their runXXX helpers from a Java source file.

    Returns dict of method_name -> source_code_string.
    """
    methods = {}
    if not source_path:
        return methods
    try:
        with open(source_path, 'r') as f:
            content = f.read()
    except (OSError, IOError):
        return methods

    # 1. Find all methods first (crude but effective for this benchmark style)
    all_methods = {}
    # Matches: [modifiers] [type] name([args]) { [body] }
    # Handles nested braces
    pos = 0
    while True:
        m = re.search(r'(?:public|private|protected|static|\s)+\s+[\w<>[\]]+\s+(\w+)\s*\([^)]*\)\s*(?:throws\s+[\w, \t]+)?\s*\{', content[pos:])
        if not m:
            break
        method_name = m.group(1)
        start_brace = pos + m.end() - 1
        
        # Find matching closing brace
        depth = 0
        end_brace = -1
        for i in range(start_brace, len(content)):
            if content[i] == '{':
                depth += 1
            elif content[i] == '}':
                depth -= 1
                if depth == 0:
                    end_brace = i
                    break
        
        if end_brace != -1:
            # Find start of method (including annotations/comments)
            method_start = pos + m.start()
            # Look back for comments or annotations
            lines = content[:method_start].splitlines()
            actual_start = method_start
            for i in range(len(lines) - 1, -1, -1):
                line = lines[i].strip()
                if line.startswith('@') or line.startswith('//') or line.startswith('*') or line.startswith('/*'):
                    actual_start = content.rfind(lines[i], 0, actual_start)
                elif not line:
                    continue
                else:
                    break
            
            body = content[actual_start:end_brace + 1]
            # Dedent
            lines = body.splitlines()
            non_empty = [l for l in lines if l.strip()]
            if non_empty:
                min_indent = min(len(l) - len(l.lstrip()) for l in non_empty)
                body = '\n'.join(l[min_indent:] if len(l) > min_indent else l for l in lines)
            
            all_methods[method_name] = body
            pos = end_brace + 1
        else:
            pos += m.end()

    # 2. Filter for @Benchmark methods and attach runXXX helpers
    for name, body in all_methods.items():
        if '@Benchmark' in body:
            # Look for runXXX call: e.g. runJdkSortLambda(work)
            # Pattern: run followed by capitalized method name
            run_name = "run" + name[0].upper() + name[1:]
            if run_name in all_methods:
                methods[name] = body + "\n\n" + all_methods[run_name]
            else:
                methods[name] = body
    
    return methods


def lerp_color(t):
    """Green (t=0, best) -> yellow (t=0.5) -> red (t=1, worst)."""
    t = max(0.0, min(1.0, t))
    if t < 0.5:
        u = t * 2
        r = int(120 * u)
        g = 180
        b = int(80 * (1 - u))
    else:
        u = (t - 0.5) * 2
        r = 120 + int(100 * u)
        g = int(180 * (1 - u))
        b = 0
    return r, g, b


def sparkline_svg(raw_samples, width=120, height=24, num_bins=20):
    """Generate a tiny inline SVG histogram sparkline from raw samples."""
    if not raw_samples or len(raw_samples) < 2:
        return ''
    lo = min(raw_samples)
    hi = max(raw_samples)
    span = hi - lo
    if span == 0:
        span = 1
    bins = [0] * num_bins
    for v in raw_samples:
        idx = int((v - lo) / span * num_bins)
        if idx >= num_bins:
            idx = num_bins - 1
        bins[idx] += 1
    max_count = max(bins)
    if max_count == 0:
        return ''
    bar_w = width / num_bins
    bars = []
    for i, count in enumerate(bins):
        bar_h = (count / max_count) * height
        x = i * bar_w
        y = height - bar_h
        # Monochrome sparkline to avoid confusion with heatmap colors
        r, g, b = 102, 136, 170
        bars.append(
            f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_w:.1f}" '
            f'height="{bar_h:.1f}" fill="rgb({r},{g},{b})" />'
        )
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'style="display:block;margin:3px auto 0">'
        + ''.join(bars)
        + '</svg>'
    )


def build_html(entries, config, method_sources):
    if not entries:
        print("No JMH results found on stdin.", file=sys.stderr)
        sys.exit(1)

    has_raw = any(e['raw'] for e in entries)
    has_source = bool(method_sources)

    seen_sizes = dict()
    seen_methods = dict()
    seen_dists = dict()
    for e in entries:
        seen_sizes[e['size']] = None
        seen_methods[e['method']] = None
        seen_dists[e['dist']] = None

    # Sort sizes numerically if possible
    try:
        sizes = sorted(seen_sizes.keys(), key=lambda x: int(x))
    except ValueError:
        sizes = sorted(seen_sizes.keys())

    methods = sorted(seen_methods.keys())
    dists = sorted(seen_dists.keys())
    unit = entries[0]['unit']

    # grid[dist][method][size] = entry
    grid = {}
    for e in entries:
        grid.setdefault(e['dist'], {}).setdefault(e['method'], {})[e['size']] = e

    # Precalculate mins/maxs per (dist, size) for heatmap
    # stats[dist][size] = {min, max}
    stats = {}
    for d in dists:
        stats[d] = {}
        for s in sizes:
            scores = [grid[d][m][s]['score'] for m in methods if s in grid[d].get(m, {})]
            if scores:
                stats[d][s] = {'min': min(scores), 'max': max(scores)}

    h = html.escape

    # JSON data for JS
    # data_js[dist][method][size] = {score, error, rel, color, spark}
    data_js = {}
    for d in dists:
        data_js[d] = {}
        for m in methods:
            data_js[d][m] = {}
            for s in sizes:
                if s in grid[d].get(m, {}):
                    e = grid[d][m][s]
                    score = e['score']
                    lo, hi = stats[d][s]['min'], stats[d][s]['max']
                    if lo > 0 and hi > lo:
                        t = math.log(score / lo) / math.log(hi / lo)
                    else:
                        span = hi - lo
                        t = (score - lo) / span if span > 0 else 0
                    r, g, b = lerp_color(t)
                    rel = score / lo if lo > 0 else 1.0
                    data_js[d][m][s] = {
                        'score': f"{score:.3f}",
                        'error': f"{e['error']:.3f}",
                        'rel': f"{rel:.2f}&times;",
                        'color': f"rgb({r},{g},{b})",
                        'spark': sparkline_svg(e['raw']) if e['raw'] else '',
                        'raw': e['raw']
                    }

    sources_js = {name: src for name, src in method_sources.items()}

    out = []
    out.append(f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>JMH Results</title>
<style>
  body {{ font-family: system-ui, sans-serif; margin: 2rem; background: #fafafa; }}
  .config {{ background: #f0f0f0; border: 1px solid #ccc; border-radius: 4px;
             margin-bottom: 1.5rem; font-size: 0.9em;
             border-collapse: collapse; }}
  .config td {{ padding: 4px 12px; border: none; }}
  .config .label {{ color: #666; text-align: right; }}
  .config .val {{ font-weight: 600; }}
  .main-area {{ display: flex; gap: 2rem; align-items: flex-start; }}
  .left-col {{ flex-shrink: 0; }}
  .right-col {{ flex-grow: 1; min-width: 0; }}
  #source-panel {{ display: none; background: #1e1e1e; color: #d4d4d4; border-radius: 6px;
                   padding: 1rem; max-width: 800px; box-shadow: 0 2px 8px rgba(0,0,0,0.2); }}
  #source-panel h3 {{ margin: 0 0 0.5rem 0; color: #9cdcfe; font-size: 0.95em; }}
  #source-panel pre {{ margin: 0; font-family: 'JetBrains Mono', 'Fira Code', 'Cascadia Code',
                       'Consolas', monospace; font-size: 13px; line-height: 1.5;
                       overflow-x: auto; white-space: pre; }}
  table {{ border-collapse: collapse; box-shadow: 0 2px 8px rgba(0,0,0,0.12); }}
  th, td {{ padding: 8px 16px; border: 1px solid #bbb; text-align: right; white-space: nowrap;
            vertical-align: top; }}
  th {{ background: #444; color: #fff; cursor: pointer; user-select: none; position: relative; }}
  th:first-child {{ text-align: left; }}
  td:first-child {{ text-align: left; font-weight: 600; background: #f5f5f5; }}
  th:hover {{ background: #666; }}
  .arrow {{ font-size: 0.7em; margin-left: 4px; }}
  td .err {{ color: #666; font-size: 0.85em; }}
  td.clickable {{ cursor: pointer; }}
  td.clickable:hover {{ outline: 2px solid #333; outline-offset: -2px; }}
  td.selected {{ outline: 2px solid #0066cc; outline-offset: -2px; }}
  #hist-panel {{ margin-top: 1.5rem; }}
  #hist-panel h3 {{ margin: 0 0 0.5rem 0; }}
  #hist-panel .stats {{ color: #555; font-size: 0.9em; margin-bottom: 0.5rem; }}
  #hist-canvas {{ border: 1px solid #ccc; background: #fff; }}
  .controls {{ display: flex; gap: 2rem; align-items: center; margin-bottom: 1rem; }}
  select {{ padding: 4px 8px; border-radius: 4px; border: 1px solid #ccc; }}
</style>
</head><body>
<h2>JMH Results</h2>""")

    # Config banner
    if config:
        out.append('<table class="config">')
        items = [
            ('Mode', str(config.get('mode', '?'))),
            ('Forks', str(config.get('forks', '?'))),
            ('Threads', str(config.get('threads', '?'))),
            ('Warmup', f"{config.get('warmupIterations','?')} iter \u00d7 {config.get('warmupTime','?')}"),
            ('Measurement', f"{config.get('measurementIterations','?')} iter \u00d7 {config.get('measurementTime','?')}"),
        ]
        jvm_desc = ' '.join(s for s in [config.get('vmName', ''), config.get('vmVersion', '')] if s)
        if config.get('jdkVersion'): jvm_desc = f"JDK {config.get('jdkVersion')}, {jvm_desc}"
        if jvm_desc: items.append(('JVM', jvm_desc))
        if config.get('jmhVersion'): items.append(('JMH version', config.get('jmhVersion')))
        if config.get('benchmarkJvmArgs'): items.append(('Fork JVM args', ' '.join(config.get('benchmarkJvmArgs'))))
        for label, val in items:
            out.append(f'<tr><td class="label">{h(label)}</td><td class="val">{h(val)}</td></tr>')
        out.append('</table>')

    click_hint = ''
    if has_raw or has_source:
        click_hint = ' Click a data cell to see'
        parts = []
        if has_raw: parts.append('its iteration histogram')
        if has_source: parts.append('the method source code')
        click_hint += ' ' + ' and '.join(parts) + '.'

    out.append(f'<p>Click column headers to sort.{click_hint}</p>')

    out.append('<div class="controls">')
    out.append('<div><label>Distribution: </label><select id="dist-picker">')
    for d in dists:
        selected = ' selected' if d == 'random' else ''
        out.append(f'<option value="{h(d)}"{selected}>{h(d)}</option>')
    out.append('</select></div>')
    out.append('<div><label style="font-size: 0.9em; user-select: none; cursor: pointer;">'
               '<input type="checkbox" id="rel-toggle"> Show relative (&times;fastest)'
               '</label></div>')
    out.append('</div>')

    out.append('<div class="main-area"><div class="left-col">')
    out.append('<table id="t"><thead><tr>')
    out.append('<th data-col="0">Algorithm</th>')
    for i, s in enumerate(sizes):
        out.append(f'<th data-col="{i+1}">size={h(s)}<br><small>{h(unit)}</small></th>')
    out.append('</tr></thead><tbody>')

    for method in methods:
        out.append('<tr>')
        out.append(f'<td>{h(method)}</td>')
        for s in sizes:
            out.append(f'<td data-size="{h(s)}" data-method="{h(method)}"></td>')
        out.append('</tr>')

    out.append('</tbody></table>')
    out.append('</div>')
    out.append('<div class="right-col"><div id="source-panel"><h3 id="source-title"></h3><pre id="source-code"></pre></div></div>')
    out.append('</div>')
    out.append('<div id="hist-panel"></div>')

    out.append('<script>')
    out.append(f'const UNIT = {json.dumps(unit)};')
    out.append(f'const DATA = {json.dumps(data_js)};')
    out.append(f'const SOURCES = {json.dumps(sources_js)};')
    out.append(r"""
const table = document.getElementById('t');
const headers = table.querySelectorAll('th');
const distPicker = document.getElementById('dist-picker');
const relToggle = document.getElementById('rel-toggle');
let sortCol = -1, sortAsc = true;
let activeKey = ''; // format: dist|method|size

function updateHash() {
  let hash = activeKey || '';
  if (sortCol >= 0) {
    hash += ';sort=' + sortCol + ',' + (sortAsc ? 'asc' : 'desc');
  }
  hash += ';dist=' + distPicker.value;
  if (relToggle.checked) {
    hash += ';rel=1';
  }
  history.replaceState(null, '', hash ? '#' + hash : location.pathname);
}

function updateTable() {
  const dist = distPicker.value;
  const showRel = relToggle.checked;

  table.querySelectorAll('tbody td[data-size]').forEach(td => {
    const size = td.dataset.size;
    const method = td.dataset.method;
    const d = DATA[dist][method] ? DATA[dist][method][size] : null;

    if (d) {
      td.style.background = d.color;
      td.className = 'clickable';
      if (activeKey === `${dist}|${method}|${size}`) td.classList.add('selected');
      td.dataset.v = d.score;
      td.innerHTML = `<span class="val-text">${showRel ? d.rel : d.score}</span> ` +
                     `<span class="err">&plusmn; ${d.error}</span>${d.spark}`;
    } else {
      td.style.background = '';
      td.className = '';
      td.innerHTML = '-';
      td.dataset.v = '999999';
    }
  });

  if (sortCol >= 0) applySort(sortCol, sortAsc);
  updateHash();
}

distPicker.addEventListener('change', updateTable);
relToggle.addEventListener('change', updateTable);

function applySort(col, asc) {
  sortCol = col;
  sortAsc = asc;
  headers.forEach(h => { const a = h.querySelector('.arrow'); if (a) a.remove(); });
  const th = table.querySelector(`th[data-col="${col}"]`);
  if (th) {
    const arrow = document.createElement('span');
    arrow.className = 'arrow';
    arrow.textContent = sortAsc ? '\u25B2' : '\u25BC';
    th.appendChild(arrow);
  }
  const tbody = table.querySelector('tbody');
  const rows = Array.from(tbody.querySelectorAll('tr'));
  rows.sort((a, b) => {
    if (col === 0) {
      const av = a.children[0].textContent, bv = b.children[0].textContent;
      return sortAsc ? av.localeCompare(bv) : bv.localeCompare(av);
    }
    const av = parseFloat(a.children[col].dataset.v || 999999);
    const bv = parseFloat(b.children[col].dataset.v || 999999);
    return sortAsc ? av - bv : bv - av;
  });
  rows.forEach(r => tbody.appendChild(r));
}

headers.forEach(th => {
  th.addEventListener('click', e => {
    e.stopPropagation();
    const col = parseInt(th.dataset.col);
    const asc = (sortCol === col) ? !sortAsc : true;
    applySort(col, asc);
    updateHash();
  });
});

function activateCell(dist, method, size) {
  const td = table.querySelector(`td[data-method="${CSS.escape(method)}"][data-size="${CSS.escape(size)}"]`);
  if (!td) return;
  table.querySelectorAll('td.selected').forEach(el => el.classList.remove('selected'));
  td.classList.add('selected');
  activeKey = `${dist}|${method}|${size}`;

  // Show source
  const srcPanel = document.getElementById('source-panel');
  const src = SOURCES[method];
  if (src) {
    document.getElementById('source-title').textContent = method + '()';
    document.getElementById('source-code').textContent = src;
    srcPanel.style.display = 'block';
  } else {
    srcPanel.style.display = 'none';
  }

  // Show histogram
  const d = DATA[dist][method][size];
  if (d && d.raw && d.raw.length > 0) {
    drawHistogram(dist, method, size, d.raw);
  } else {
    document.getElementById('hist-panel').innerHTML = '';
  }
  updateHash();
}

table.querySelector('tbody').addEventListener('click', e => {
  const td = e.target.closest('td.clickable');
  if (!td) return;
  activateCell(distPicker.value, td.dataset.method, td.dataset.size);
});

// Restore from hash
if (location.hash.length > 1) {
  const raw = decodeURIComponent(location.hash.slice(1));
  const parts = raw.split(';');
  const cellKey = parts[0] || '';
  for (let i = 1; i < parts.length; i++) {
    const mSort = parts[i].match(/^sort=(\d+),(asc|desc)$/);
    if (mSort) { applySort(parseInt(mSort[1]), mSort[2] === 'asc'); }
    const mDist = parts[i].match(/^dist=(.+)$/);
    if (mDist) { distPicker.value = mDist[1]; }
    if (parts[i] === 'rel=1') { relToggle.checked = true; }
  }
  updateTable();
  if (cellKey) {
    const [d, m, s] = cellKey.split('|');
    if (d && m && s) activateCell(d, m, s);
  }
} else {
  updateTable();
}

// Pick the best display unit and scale factor.
function pickDisplayUnit(values) {
  const mean = values.reduce((a, b) => a + b, 0) / values.length;
  if (UNIT === 'us/op') {
    if (mean < 1) return { label: 'ns/op', scale: 1000 };
    if (mean >= 1000) return { label: 'ms/op', scale: 0.001 };
  }
  if (UNIT === 'ms/op') {
    if (mean < 1) return { label: 'us/op', scale: 1000 };
    if (mean >= 1000) return { label: 's/op', scale: 0.001 };
  }
  if (UNIT === 'ns/op' && mean >= 1000) {
    return { label: 'us/op', scale: 0.001 };
  }
  return { label: UNIT, scale: 1 };
}

function smartPrecision(range, numTicks) {
  if (range === 0) return 1;
  const step = range / Math.max(numTicks, 1);
  const digits = Math.max(0, Math.ceil(-Math.log10(step)) + 1);
  return Math.min(digits, 8);
}

function fmtVal(v, prec) {
  return v.toFixed(prec);
}

function drawHistogram(dist, method, size, samples) {
  const panel = document.getElementById('hist-panel');
  const n = samples.length;

  const du = pickDisplayUnit(samples);
  const vals = samples.map(v => v * du.scale);
  const displayUnit = du.label;

  const sorted = [...vals].sort((a, b) => a - b);
  const mean = vals.reduce((a, b) => a + b, 0) / n;
  const min = sorted[0], max = sorted[n - 1];
  const median = n % 2 === 0 ? (sorted[n/2 - 1] + sorted[n/2]) / 2 : sorted[Math.floor(n/2)];
  const p5 = sorted[Math.floor(n * 0.05)];
  const p95 = sorted[Math.floor(n * 0.95)];
  const stddev = Math.sqrt(vals.reduce((s, v) => s + (v - mean) ** 2, 0) / n);

  const statPrec = smartPrecision(max - min, 20);

  const numBins = Math.max(10, Math.min(50, Math.ceil(Math.sqrt(n))));
  const binWidth = (max - min) / numBins || 1;
  const bins = new Array(numBins).fill(0);
  for (const v of vals) {
    let idx = Math.floor((v - min) / binWidth);
    if (idx >= numBins) idx = numBins - 1;
    bins[idx]++;
  }
  const maxCount = Math.max(...bins);

  const W = 700, H = 300;
  const pad = { top: 20, right: 20, bottom: 50, left: 55 };
  const cw = W - pad.left - pad.right;
  const ch = H - pad.top - pad.bottom;

  panel.innerHTML = `
    <h3>${method} &mdash; size=${size} (${dist})</h3>
    <div class="stats">
      ${n} samples &nbsp;|&nbsp;
      mean: ${fmtVal(mean, statPrec)} ${displayUnit} &nbsp;|&nbsp;
      median: ${fmtVal(median, statPrec)} &nbsp;|&nbsp;
      stddev: ${fmtVal(stddev, statPrec)} &nbsp;|&nbsp;
      range: [${fmtVal(min, statPrec)}, ${fmtVal(max, statPrec)}] &nbsp;|&nbsp;
      p5: ${fmtVal(p5, statPrec)} &nbsp;|&nbsp; p95: ${fmtVal(p95, statPrec)}
    </div>
    <canvas id="hist-canvas" width="${W}" height="${H}"></canvas>
  `;

  const canvas = document.getElementById('hist-canvas');
  const ctx = canvas.getContext('2d');

  ctx.fillStyle = '#fff';
  ctx.fillRect(0, 0, W, H);

  const barW = cw / numBins;
  for (let i = 0; i < numBins; i++) {
    const barH = maxCount > 0 ? (bins[i] / maxCount) * ch : 0;
    const x = pad.left + i * barW;
    const y = pad.top + ch - barH;
    const binCenter = min + (i + 0.5) * binWidth;
    const t = max > min ? (binCenter - min) / (max - min) : 0;
    const r = Math.round(40 + 180 * t);
    const g = Math.round(160 - 80 * t);
    const b = Math.round(80 - 60 * t);
    ctx.fillStyle = `rgb(${r},${g},${b})`;
    ctx.fillRect(x + 1, y, barW - 2, barH);
  }

  const meanX = pad.left + ((mean - min) / (binWidth * numBins)) * cw;
  ctx.strokeStyle = '#0066cc';
  ctx.lineWidth = 2;
  ctx.setLineDash([5, 3]);
  ctx.beginPath(); ctx.moveTo(meanX, pad.top); ctx.lineTo(meanX, pad.top + ch); ctx.stroke();
  ctx.setLineDash([]);

  ctx.fillStyle = '#0066cc';
  ctx.font = '11px system-ui';
  ctx.textAlign = 'center';
  ctx.fillText('mean', meanX, pad.top - 5);

  ctx.strokeStyle = '#333';
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(pad.left, pad.top);
  ctx.lineTo(pad.left, pad.top + ch);
  ctx.lineTo(pad.left + cw, pad.top + ch);
  ctx.stroke();

  ctx.fillStyle = '#333';
  ctx.font = '11px system-ui';
  ctx.textAlign = 'center';
  const numXLabels = Math.min(numBins, 8);
  const xStep = Math.max(1, Math.floor(numBins / numXLabels));
  const xRange = max - min;
  const xPrec = smartPrecision(xRange, numXLabels);
  for (let i = 0; i <= numBins; i += xStep) {
    const val = min + i * binWidth;
    const x = pad.left + i * barW;
    ctx.fillText(fmtVal(val, xPrec), x, pad.top + ch + 16);
  }
  ctx.fillText(displayUnit, pad.left + cw / 2, pad.top + ch + 38);

  ctx.textAlign = 'right';
  const yTicks = 5;
  for (let i = 0; i <= yTicks; i++) {
    const count = Math.round(maxCount * i / yTicks);
    const y = pad.top + ch - (i / yTicks) * ch;
    ctx.fillText(count.toString(), pad.left - 6, y + 4);
    ctx.strokeStyle = '#eee';
    ctx.beginPath(); ctx.moveTo(pad.left + 1, y); ctx.lineTo(pad.left + cw, y); ctx.stroke();
  }
  ctx.strokeStyle = '#333';
}
""")
    out.append('</script></body></html>')
    return '\n'.join(out)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Parse JMH JSON/text output into an interactive HTML table.')
    parser.add_argument('source', help='Path to Java source file containing @Benchmark methods')
    parser.add_argument('--skip', nargs='+', default=[], metavar='ALGO',
                        help='Algorithm names to exclude (substring match, case-insensitive)')
    args = parser.parse_args()

    method_sources = extract_methods(args.source)
    if not method_sources:
        print(f"No @Benchmark methods found in {args.source}", file=sys.stderr)
        sys.exit(1)

    text = sys.stdin.read().strip()
    if not text:
        print("No input on stdin.", file=sys.stderr)
        sys.exit(1)

    if text.startswith('[') or text.startswith('{'):
        data = json.loads(text)
        if isinstance(data, dict):
            data = [data]
        entries, config = parse_jmh_json(data)
    else:
        entries, config = parse_jmh_text(text)

    if args.skip:
        skip_lower = [s.lower() for s in args.skip]
        entries = [e for e in entries
                   if not any(sk in e['method'].lower() for sk in skip_lower)]

    print(build_html(entries, config, method_sources))
