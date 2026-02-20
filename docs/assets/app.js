window.addEventListener("error", (e) => {
  try {
    const el = document.getElementById("metaBox");
    if (el) el.innerText = "Errore JS: " + (e && e.message ? e.message : "sconosciuto");
  } catch {}
  console.error(e.error || e);
});

window.addEventListener("unhandledrejection", (e) => {
  const r = e && e.reason ? e.reason : "";
  const msg = r && r.message ? r.message : String(r);
  if (msg && msg.includes("verticalFillMode")) return;
  try {
    const el = document.getElementById("metaBox");
    if (el) el.innerText = "Promise rejection: " + msg;
  } catch {}
  console.error(r);
});

async function fetchText(path) {
  const r = await fetch(path, { cache: "default" });
  if (!r.ok) throw new Error("Failed fetch " + path + " (" + r.status + ")");
  return await r.text();
}

async function fetchJson(path) {
  const r = await fetch(path, { cache: "default" });
  if (!r.ok) throw new Error("Failed fetch " + path + " (" + r.status + ")");
  return await r.json();
}

async function fetchTextOrNull(path) {
  try {
    return await fetchText(path);
  } catch {
    return null;
  }
}

async function fetchJsonOrNull(path) {
  try {
    return await fetchJson(path);
  } catch {
    return null;
  }
}

function ensureTrailingSlash(p) {
  const s = String(p || "");
  return s.endsWith("/") ? s : s + "/";
}

const DATA_ROOT_CANDIDATES = ["data/", "./data/", "docs/data/", "site/data/"];

function isLfsPointer(t) {
  if (typeof t !== "string") return false;
  const trimmed = t.trimStart();
  if (!trimmed.startsWith("version https://git-lfs.github.com")) return false;
  // A real LFS pointer has exactly 3 lines with no commas on the version line.
  // A corrupted CSV (produced when save_gold_tables reads a pointer as CSV) has
  // comma-separated column names on the first line — treat that as real data.
  return !trimmed.split("\n")[0].includes(",");
}

async function fetchTextAny(paths) {
  for (const p of paths) {
    const t = await fetchTextOrNull(p);
    if (t && String(t).trim().length && !isLfsPointer(t)) return t;
  }
  return null;
}

async function fetchJsonAny(paths) {
  for (const p of paths) {
    const j = await fetchJsonOrNull(p);
    if (j && typeof j === "object") return j;
  }
  return null;
}

function uniq(arr) {
  return Array.from(new Set(arr));
}

function candidateFilePaths(root, rel) {
  const r = ensureTrailingSlash(root);
  const clean = String(rel || "").replace(/^\/+/, "");
  const out = [r + clean];
  if (!clean.startsWith("gold/")) out.push(r + "gold/" + clean);
  return uniq(out);
}

async function pickDataBase() {
  const probes = [
    "manifest.json",
    "gold/manifest.json",
    "kpi_mese_categoria.csv",
    "gold/kpi_mese_categoria.csv",
    "kpi_mese.csv",
    "gold/kpi_mese.csv"
  ];

  for (const base0 of DATA_ROOT_CANDIDATES) {
    const base = ensureTrailingSlash(base0);
    for (const p of probes) {
      const t = await fetchTextOrNull(base + p);
      if (t && String(t).trim().length > 20 && !isLfsPointer(t)) return base;
    }
  }
  return "data/";
}

function detectDelimiter(line) {
  const s = String(line || "");
  let comma = 0,
    semi = 0,
    tab = 0;
  let inQ = false;
  for (let i = 0; i < s.length; i++) {
    const ch = s[i];
    if (ch === '"') {
      if (inQ && s[i + 1] === '"') i++;
      else inQ = !inQ;
      continue;
    }
    if (!inQ) {
      if (ch === ",") comma++;
      else if (ch === ";") semi++;
      else if (ch === "\t") tab++;
    }
  }
  if (semi > comma && semi >= tab) return ";";
  if (tab > comma && tab > semi) return "\t";
  return ",";
}

function splitCSVLine(line, delim) {
  const d = delim || ",";
  const out = [];
  let cur = "";
  let inQ = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQ && line[i + 1] === '"') {
        cur += '"';
        i++;
      } else {
        inQ = !inQ;
      }
      continue;
    }
    if (ch === d && !inQ) {
      out.push(cur);
      cur = "";
      continue;
    }
    cur += ch;
  }
  out.push(cur);
  return out;
}

function parseCSV(text) {
  const t = String(text || "").trim();
  if (!t) return [];
  const lines = t.split(/\r?\n/).filter((x) => String(x || "").length);
  if (lines.length <= 1) return [];
  const delim = detectDelimiter(lines[0]);
  const header = splitCSVLine(lines[0], delim).map((x) => String(x || "").trim());
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const line = String(lines[i] || "");
    if (!line.trim()) continue;
    const cols = splitCSVLine(line, delim);
    const obj = {};
    for (let j = 0; j < header.length; j++) obj[header[j]] = cols[j] ?? "";
    rows.push(obj);
  }
  return rows;
}

function toNum(x) {
  const v = Number(x);
  return Number.isFinite(v) ? v : 0;
}

function parseNumberAny(x) {
  if (x === null || typeof x === "undefined") return NaN;
  if (typeof x === "number") return x;
  let s = String(x).trim();
  if (!s) return NaN;
  s = s.replace(/\s+/g, "");
  if (s.includes(",") && !s.includes(".")) s = s.replace(",", ".");
  const v = Number(s);
  return Number.isFinite(v) ? v : NaN;
}

function fmtInt(x) {
  return Math.round(Number(x) || 0).toLocaleString("it-IT");
}

function fmtFloat(x) {
  const v = Number(x);
  if (!Number.isFinite(v)) return "";
  return v.toLocaleString("it-IT", { maximumFractionDigits: 2 });
}

function normalizeText(s) {
  const raw = String(s || "").toLowerCase().trim();
  const base = typeof raw.normalize === "function" ? raw.normalize("NFD") : raw;
  return base.replace(/[\u0300-\u036f]/g, "");
}

function yearFromMonth(mese) {
  return String(mese || "").slice(0, 4);
}

function firstEl(ids) {
  for (const id of ids) {
    const el = document.getElementById(id);
    if (el) return el;
  }
  return null;
}

function setTextByIds(ids, value) {
  const el = firstEl(ids);
  if (el) el.innerText = value;
}

function setMeta(text) {
  const el = document.getElementById("metaBox");
  if (el) el.innerText = text;
}

function safeManifestDefaults() {
  return {
    built_at_utc: "",
    gold_files: [
      "kpi_mese.csv",
      "kpi_mese_categoria.csv",
      "kpi_mese_categoria_segmenti.csv",
      "hist_mese_categoria.csv",
      "hist_mese_categoria_segmenti.csv",
      "stazioni_mese_categoria_nodo.csv",
      "stazioni_mese_categoria_nodo_segmenti.csv",
      "od_mese_categoria.csv",
      "od_mese_categoria_segmenti.csv",
      "hist_stazioni_mese_categoria_ruolo.csv",
      "hist_stazioni_mese_categoria_ruolo_segmenti.csv"
    ],
    delay_bucket_labels: [
      "<=-60",
      "(-60,-30]",
      "(-30,-15]",
      "(-15,-10]",
      "(-10,-5]",
      "(-5,-1]",
      "(-1,0]",
      "(0,1]",
      "(1,5]",
      "(5,10]",
      "(10,15]",
      "(15,30]",
      "(30,60]",
      "(60,120]",
      ">120"
    ]
  };
}

const state = {
  dataBase: "data/",
  manifest: safeManifestDefaults(),
  data: {
    kpiMonth: [],
    kpiMonthCat: [],
    kpiMonthCatSeg: [],
    histMonthCat: [],
    histMonthCatSeg: [],
    stationsMonthNode: [],
    stationsMonthNodeSeg: [],
    odMonthCat: [],
    odMonthCatSeg: [],
    histStationsMonthRuolo: [],
    histStationsMonthRuoloSeg: [],
  },
  stationsRef: new Map(),
  capoluoghiSet: new Set(),
  map: null,
  markers: [],
  filters: {
    year: "all",
    cat: "all",
    dep: "all",
    arr: "all",
    month_from: "",
    month_to: "",
    day_type: { infrasettimanale: true, weekend: true },
    time_slots: { mattina: true, tarda_mattina: true, pomeriggio: true, sera: true, notte: true }
  }
};

function stationName(code, fallback) {
  const c = String(code || "").trim();
  const ref = state.stationsRef.get(c);
  const n = ref && ref.name ? String(ref.name).trim() : "";
  if (n) return n;
  const fb = String(fallback || "").trim();
  return fb || c;
}

function stationCity(code, fallbackStationName) {
  const c = String(code || "").trim();
  const ref = state.stationsRef.get(c);
  const city = ref && ref.city ? String(ref.city).trim() : "";
  if (city) return city;
  return stationName(c, fallbackStationName);
}

function stationCoords(code) {
  const c = String(code || "").trim();
  const ref = state.stationsRef.get(c);
  if (!ref) return null;
  const lat = ref.lat;
  const lon = ref.lon;
  if (typeof lat !== "number" || typeof lon !== "number") return null;
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
  return { lat, lon };
}

function buildStationItems(codes) {
  const items = (codes || []).map((code) => {
    const name = stationName(code, code);
    return { code, name, needle: normalizeText(name + " " + code) };
  });
  items.sort((a, b) => a.name.localeCompare(b.name, "it", { sensitivity: "base" }));
  return items;
}

function fillStationSelect(selectEl, items, query) {
  if (!selectEl) return;
  const q = normalizeText(query);
  const cur = selectEl.value;

  selectEl.innerHTML = "";
  selectEl.appendChild(new Option("Tutte", "all"));

  for (const it of items) {
    if (q && !it.needle.includes(q)) continue;
    selectEl.appendChild(new Option(it.name + " (" + it.code + ")", it.code));
  }

  const stillThere = Array.from(selectEl.options).some((o) => o.value === cur);
  selectEl.value = stillThere ? cur : "all";
}

function ensureSearchInput(selectEl, inputId, placeholder, items) {
  if (!selectEl || !selectEl.parentNode) return;
  let input = document.getElementById(inputId);
  if (!input) {
    input = document.createElement("input");
    input.id = inputId;
    input.type = "search";
    input.autocomplete = "off";
    input.placeholder = placeholder;
    input.style.width = "100%";
    input.style.margin = "0 0 6px 0";
    selectEl.parentNode.insertBefore(input, selectEl);
  }
  input.oninput = () => fillStationSelect(selectEl, items, input.value);
}

function passCat(r) {
  if (state.filters.cat === "all") return true;
  return String(r.categoria || "").trim() === state.filters.cat;
}

function passDep(r) {
  if (state.filters.dep === "all") return true;
  return String(r.cod_partenza || "").trim() === state.filters.dep;
}

function passArr(r) {
  if (state.filters.arr === "all") return true;
  return String(r.cod_arrivo || "").trim() === state.filters.arr;
}

function passYear(r, field) {
  if (state.filters.year === "all") return true;
  const k = String(r[field] || "");
  return k.slice(0, 4) === state.filters.year;
}

function hasMonthFilter() {
  return !!(state.filters.month_from || state.filters.month_to);
}

function hasDayTypeFilter() {
  const f = state.filters.day_type || {};
  return !(f.infrasettimanale && f.weekend);
}

function hasTimeSlotFilter() {
  const f = state.filters.time_slots || {};
  return !(f.mattina && f.tarda_mattina && f.pomeriggio && f.sera && f.notte);
}

function passDayType(row) {
  const selected = state.filters.day_type || {};
  const v = String(row.tipo_giorno || '').trim();
  if (!v) return true;
  if (v === 'infrasettimanale') return !!selected.infrasettimanale;
  if (v === 'weekend') return !!selected.weekend;
  return true;
}

function passTimeSlot(row) {
  const selected = state.filters.time_slots || {};
  const v = String(row.fascia_oraria || '').trim();
  if (!v || v === 'missing') return true;
  if (v in selected) return !!selected[v];
  return true;
}

function passMonthRange(row, field) {
  if (!hasMonthFilter()) return true;
  const from = String(state.filters.month_from || '').trim();
  const to = String(state.filters.month_to || '').trim();
  const a = from || to;
  const b = to || from;
  const lo = a <= b ? a : b;
  const hi = a <= b ? b : a;
  const m = String(row[field] || '').slice(0, 7);
  if (!m) return false;
  return m >= lo && m <= hi;
}

function passSegmentFilters(row) {
  return passDayType(row) && passTimeSlot(row);
}

function initMap() {
  const mapEl = firstEl(["map", "mapStations", "stationsMap"]);
  if (!mapEl) return;
  if (typeof L !== "object" || typeof L.map !== "function") return;
  if (state.map) return;

  state.map = L.map(mapEl.id, { center: [42.5, 12.5], zoom: 6, zoomSnap: 0.5 });

  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    attribution: "&copy; OpenStreetMap contributors",
    maxZoom: 18
  }).addTo(state.map);

  setTimeout(() => {
    try {
      state.map.invalidateSize();
    } catch {}
  }, 150);
}

function clearMarkers() {
  if (!state.map) return;
  for (const m of state.markers) {
    try {
      state.map.removeLayer(m);
    } catch {}
  }
  state.markers = [];
}

function ensureExtraControls() {
  const anchor = firstEl(["yearSel", "annoSel", "year"]);
  if (!anchor) return null;

  let extra = document.getElementById("filtersExtra");
  if (extra) return extra;

  const host =
    anchor.closest("#filters") ||
    anchor.closest(".filters") ||
    anchor.closest(".controls") ||
    anchor.closest(".filtersRow") ||
    anchor.closest(".filtersGrid") ||
    anchor.parentNode;

  extra = document.createElement("div");
  extra.id = "filtersExtra";
  extra.style.display = "flex";
  extra.style.alignItems = "center";
  extra.style.gap = "10px";
  extra.style.marginTop = "8px";
  extra.style.flexWrap = "wrap";

  if (host && host.parentNode) host.parentNode.insertBefore(extra, host.nextSibling);
  else (document.body || document.documentElement).appendChild(extra);

  return extra;
}

function initSegmentControls() {
  if (document.getElementById("monthFrom")) return;

  const extra = ensureExtraControls();
  if (!extra) return;

  const monthWrap = document.createElement("div");
  monthWrap.style.display = "flex";
  monthWrap.style.alignItems = "center";
  monthWrap.style.gap = "8px";

  const monthFrom = document.createElement("input");
  monthFrom.type = "month";
  monthFrom.id = "monthFrom";
  monthFrom.value = state.filters.month_from || "";

  const monthTo = document.createElement("input");
  monthTo.type = "month";
  monthTo.id = "monthTo";
  monthTo.value = state.filters.month_to || "";

  monthWrap.appendChild(monthFrom);
  monthWrap.appendChild(monthTo);

  const dayWrap = document.createElement("div");
  dayWrap.id = "dayTypeWrap";
  dayWrap.style.display = "flex";
  dayWrap.style.alignItems = "center";
  dayWrap.style.gap = "6px";

  const dayInfo = document.createElement("span");
  dayInfo.innerText = "(i)";
  dayInfo.title = "Infrasettimanale = Lun-Ven, Weekend = Sab-Dom";
  dayInfo.style.opacity = "0.8";
  dayWrap.appendChild(dayInfo);

  const dayDef = [
    ["infrasettimanale", "Infrasettimanale", "Lun-Ven"],
    ["weekend", "Weekend", "Sab-Dom"]
  ];

  const slotWrap = document.createElement("div");
  slotWrap.id = "timeSlotWrap";
  slotWrap.style.display = "flex";
  slotWrap.style.alignItems = "center";
  slotWrap.style.gap = "6px";
  slotWrap.style.flexWrap = "wrap";

  const slotInfo = document.createElement("span");
  slotInfo.innerText = "(i)";
  slotInfo.title = "Mattina 6-9, Tarda mattina 9-13, Pomeriggio 14-18, Sera 18-22, Notte 22-6";
  slotInfo.style.opacity = "0.8";
  slotWrap.appendChild(slotInfo);

  const slotDef = [
    ["mattina", "Mattina", "Fascia mattino"],
    ["tarda_mattina", "Tarda mattina", "Fascia tarda mattina"],
    ["pomeriggio", "Pomeriggio", "Fascia pomeriggio"],
    ["sera", "Sera", "Fascia sera"],
    ["notte", "Notte", "Fascia notte"]
  ];

  const mkToggle = (key, label, hint, source, onChange) => {
    const b = document.createElement("button");
    b.type = "button";
    b.innerText = label;
    b.title = hint;
    b.dataset.key = key;
    b.style.height = "28px";
    b.style.borderRadius = "999px";
    b.style.border = "1px solid rgba(255,255,255,0.85)";
    b.style.background = "transparent";
    b.style.color = "inherit";
    b.style.cursor = "pointer";
    b.style.padding = "0 10px";
    const repaint = () => {
      const on = !!source[key];
      b.style.opacity = on ? "1" : "0.35";
      b.style.borderColor = on ? "rgba(255,255,255,0.85)" : "rgba(255,255,255,0.35)";
    };
    repaint();
    b.onclick = () => {
      source[key] = !source[key];
      repaint();
      updateFiltersNote();
      renderAll();
      onChange();
    };
    return b;
  };

  dayDef.forEach(([k, l, h]) => dayWrap.appendChild(mkToggle(k, l, h, state.filters.day_type, () => {})));
  slotDef.forEach(([k, l, h]) => slotWrap.appendChild(mkToggle(k, l, h, state.filters.time_slots, () => {})));

  const note = document.createElement("div");
  note.id = "filtersNote";
  note.style.fontSize = "12px";
  note.style.opacity = "0.75";

  const apply = () => {
    state.filters.month_from = String(monthFrom.value || "").trim();
    state.filters.month_to = String(monthTo.value || "").trim();
    updateFiltersNote();
    renderAll();
  };

  monthFrom.onchange = apply;
  monthTo.onchange = apply;

  extra.appendChild(monthWrap);
  extra.appendChild(dayWrap);
  extra.appendChild(slotWrap);
  extra.appendChild(note);

  updateFiltersNote();
}

function updateFiltersNote() {
  const el = document.getElementById("filtersNote");
  if (!el) return;
  const m = hasMonthFilter();
  const d = hasDayTypeFilter();
  const t = hasTimeSlotFilter();
  if (!m && !d && !t) {
    el.innerText = "";
    return;
  }
  el.innerText = "Filtri segmentazione attivi su mese/tipologia giornata/fascia oraria.";
}

function initFilters() {
  const yearSel = firstEl(["yearSel", "annoSel", "year"]);
  const catSel = firstEl(["catSel", "categoriaSel", "category"]);
  const depSel = firstEl(["depSel", "stazionePartenzaSel", "depStationSel"]);
  const arrSel = firstEl(["arrSel", "stazioneArrivoSel", "arrStationSel"]);
  const mapMetricSel = firstEl(["mapMetricSel", "mapSel", "mappaSel"]);
  const resetBtn = firstEl(["resetBtn", "btnReset", "reset"]);

  const years = uniq(state.data.kpiMonth.map((r) => yearFromMonth(r.mese)).filter(Boolean)).sort();
  const cats = uniq(state.data.kpiMonthCat.map((r) => String(r.categoria || "").trim()).filter(Boolean)).sort((a, b) =>
    String(a).localeCompare(String(b), "it", { sensitivity: "base" })
  );

  if (yearSel) {
    yearSel.innerHTML = "";
    yearSel.appendChild(new Option("Tutti", "all"));
    years.forEach((y) => yearSel.appendChild(new Option(y, y)));
    yearSel.value = state.filters.year || "all";
    yearSel.onchange = () => {
      state.filters.year = yearSel.value || "all";
      renderAll();
    };
  }

  if (catSel) {
    catSel.innerHTML = "";
    catSel.appendChild(new Option("Tutte", "all"));
    cats.forEach((c) => catSel.appendChild(new Option(c, c)));
    catSel.value = state.filters.cat || "all";
    catSel.onchange = () => {
      state.filters.cat = catSel.value || "all";
      renderAll();
    };
  }

  const deps = uniq(
    [
      ...(state.data.odMonthCat || []).map((r) => r.cod_partenza),
      ...(state.data.odMonthCatSeg || []).map((r) => r.cod_partenza)
    ].filter(Boolean)
  );
  const arrs = uniq(
    [
      ...(state.data.odMonthCat || []).map((r) => r.cod_arrivo),
      ...(state.data.odMonthCatSeg || []).map((r) => r.cod_arrivo)
    ].filter(Boolean)
  );

  const depItems = buildStationItems(deps);
  const arrItems = buildStationItems(arrs);

  if (depSel) {
    fillStationSelect(depSel, depItems, "");
    ensureSearchInput(depSel, "depSearch", "Cerca stazione di partenza", depItems);
    depSel.value = state.filters.dep || "all";
    depSel.onchange = () => {
      state.filters.dep = depSel.value || "all";
      renderAll();
    };
  }

  if (arrSel) {
    fillStationSelect(arrSel, arrItems, "");
    ensureSearchInput(arrSel, "arrSearch", "Cerca stazione di arrivo", arrItems);
    arrSel.value = state.filters.arr || "all";
    arrSel.onchange = () => {
      state.filters.arr = arrSel.value || "all";
      renderAll();
    };
  }

  if (mapMetricSel) {
    if (!mapMetricSel.value) mapMetricSel.value = "pct_ritardo";
    mapMetricSel.onchange = () => {
      renderSeries();
      renderMap();
    };
  }

  if (resetBtn) {
    resetBtn.onclick = () => {
      state.filters.year = "all";
      state.filters.cat = "all";
      state.filters.dep = "all";
      state.filters.arr = "all";
      state.filters.month_from = "";
      state.filters.month_to = "";
      state.filters.day_type = { infrasettimanale: true, weekend: true };
      state.filters.time_slots = { mattina: true, tarda_mattina: true, pomeriggio: true, sera: true, notte: true };

      if (yearSel) yearSel.value = "all";
      if (catSel) catSel.value = "all";
      if (depSel) depSel.value = "all";
      if (arrSel) arrSel.value = "all";

      const monthFrom = document.getElementById("monthFrom");
      const monthTo = document.getElementById("monthTo");
      if (monthFrom) monthFrom.value = "";
      if (monthTo) monthTo.value = "";

      initSegmentControls();
      updateFiltersNote();
      renderAll();
    };
  }
}

function ensureHistToggleStyles() {
  if (document.getElementById("histToggleStyles")) return;
  const style = document.createElement("style");
  style.id = "histToggleStyles";
  style.textContent = `
    .histToggleWrap { display:flex; align-items:center; gap:10px; margin:0 0 8px 0; }
    .histModeText { font-size:13px; color:#e6e9f2; opacity:0.65; user-select:none; }
    .histModeText.active { opacity:1; font-weight:600; }
    .histSwitch { position:relative; display:inline-block; width:44px; height:24px; }
    .histSwitch input { opacity:0; width:0; height:0; }
    .histSlider { position:absolute; cursor:pointer; inset:0; background:rgba(255,255,255,0.22); transition:0.18s; border-radius:24px; }
    .histSlider:before { position:absolute; content:""; height:18px; width:18px; left:3px; top:3px; background:#ffffff; transition:0.18s; border-radius:50%; }
    .histSwitch input:checked + .histSlider { background:rgba(255,255,255,0.38); }
    .histSwitch input:checked + .histSlider:before { transform: translateX(20px); }
  `;
  document.head.appendChild(style);
}

function updateHistToggleUI() {
  const t = document.getElementById("histModeToggle");
  const left = document.getElementById("histModeTextCount");
  const right = document.getElementById("histModeTextPct");
  if (!t || !left || !right) return;
  if (t.checked) {
    left.classList.remove("active");
    right.classList.add("active");
  } else {
    left.classList.add("active");
    right.classList.remove("active");
  }
}

function ensureHistToggle() {
  const chart = firstEl(["chartHist", "histChart", "chartDistribution"]);
  if (!chart) return;

  ensureHistToggleStyles();

  let t = document.getElementById("histModeToggle");
  if (t) {
    updateHistToggleUI();
    t.onchange = () => {
      updateHistToggleUI();
      renderHist();
    };
    return;
  }

  const wrap = document.createElement("div");
  wrap.className = "histToggleWrap";

  const left = document.createElement("span");
  left.id = "histModeTextCount";
  left.className = "histModeText active";
  left.innerText = "Conteggi";

  const right = document.createElement("span");
  right.id = "histModeTextPct";
  right.className = "histModeText";
  right.innerText = "%";

  const sw = document.createElement("label");
  sw.className = "histSwitch";

  t = document.createElement("input");
  t.id = "histModeToggle";
  t.type = "checkbox";
  t.checked = false;

  const slider = document.createElement("span");
  slider.className = "histSlider";

  sw.appendChild(t);
  sw.appendChild(slider);

  wrap.appendChild(left);
  wrap.appendChild(sw);
  wrap.appendChild(right);

  const parent = chart.parentNode;
  if (parent) parent.insertBefore(wrap, chart);

  t.onchange = () => {
    updateHistToggleUI();
    renderHist();
  };
}


function getMetricMode() {
  const sel = firstEl(["mapMetricSel", "mapSel", "mappaSel"]);
  const v = sel ? String(sel.value || "") : "";
  if (v === "in_ritardo" || v === "conteggio_ritardo") return "count_late";
  if (v === "corse_osservate") return "count_total";
  if (v === "minuti_ritardo_tot") return "minutes";
  if (v === "soppresse" || v === "soppressi") return "suppressed";
  if (v === "cancellate" || v === "cancellati" || v === "cancellate_tot") return "cancelled";
  return "pct";
}

function metricLabel() {
  const mode = getMetricMode();
  if (mode === "count_late") return "In ritardo";
  if (mode === "count_total") return "Corse";
  if (mode === "minutes") return "Minuti";
  if (mode === "suppressed") return "Soppressi";
  if (mode === "cancelled") return "Cancellati";
  return "% in ritardo";
}

function computeValue(corse, ritardo, minuti, sopp, canc) {
  const mode = getMetricMode();
  if (mode === "count_late") return ritardo;
  if (mode === "count_total") return corse;
  if (mode === "minutes") return minuti;
  if (mode === "suppressed") return sopp || 0;
  if (mode === "cancelled") return canc || 0;
  return corse > 0 ? (ritardo / corse) * 100 : 0;
}


function renderKPI() {
  const stationFiltered = hasStationFilter();
  let base, keyField;

  if (stationFiltered) {
    base = state.data.odMonthCatSeg;
    keyField = "mese";
  } else {
    base = state.data.kpiMonthCatSeg;
    keyField = "mese";
  }

  let rows = base || [];
  if (state.filters.year !== "all") rows = rows.filter((r) => passYear(r, keyField));
  if (state.filters.cat !== "all") rows = rows.filter(passCat);

  if (stationFiltered) {
    if (state.filters.dep !== "all") rows = rows.filter(passDep);
    if (state.filters.arr !== "all") rows = rows.filter(passArr);
  }

  rows = rows.filter((r) => passMonthRange(r, keyField));
  rows = rows.filter(passSegmentFilters);

  const total = rows.reduce((a, r) => a + toNum(r.corse_osservate), 0);
  const late = rows.reduce((a, r) => a + toNum(r.in_ritardo), 0);
  const mins = rows.reduce((a, r) => a + toNum(r.minuti_ritardo_tot), 0);

  const canc = rows.reduce((a, r) => {
    const v = r.cancellate_tot !== undefined && r.cancellate_tot !== "" ? r.cancellate_tot : r.cancellate;
    return a + toNum(v);
  }, 0);

  const sopp = rows.reduce((a, r) => a + toNum(r.soppresse), 0);

  setTextByIds(["cardTotal", "kpiTotal", "kpiCorse", "totalRuns", "corseOsservate", "corse_osservate"], fmtInt(total));
  setTextByIds(["cardLate", "kpiLate", "kpiRitardo", "lateRuns", "inRitardo", "in_ritardo"], fmtInt(late));

  setTextByIds(
    [
      "cardMin",
      "kpiMinutes",
      "kpiMinuti",
      "delayMinutes",
      "kpiLateMin",
      "kpiDelayMinutes",
      "kpiMinTotRitardo",
      "minutiTotali",
      "minuti_totali_ritardo",
      "minutiRitardoTotali",
      "minutesTotal"
    ],
    fmtInt(mins)
  );

  setTextByIds(["cardCanc", "kpiCancelled", "kpiCancellati", "cancellati", "cancellate"], fmtInt(canc));
  setTextByIds(["cardSopp", "kpiSuppressed", "kpiSoppressi", "soppressi", "soppresse"], fmtInt(sopp));
}

function seriesMonthly() {
  const stationFiltered = hasStationFilter();
  let rows;

  if (stationFiltered) {
    rows = state.data.odMonthCatSeg && state.data.odMonthCatSeg.length ? state.data.odMonthCatSeg : [];
  } else {
    rows = state.data.kpiMonthCatSeg && state.data.kpiMonthCatSeg.length ? state.data.kpiMonthCatSeg : state.data.kpiMonthCat;
  }
  rows = rows || [];

  if (state.filters.year !== "all") rows = rows.filter((r) => passYear(r, "mese"));
  if (state.filters.cat !== "all") rows = rows.filter(passCat);

  if (stationFiltered) {
    if (state.filters.dep !== "all") rows = rows.filter(passDep);
    if (state.filters.arr !== "all") rows = rows.filter(passArr);
  }

  rows = rows.filter((r) => passMonthRange(r, "mese"));
  rows = rows.filter(passSegmentFilters);

  const by = new Map();
  for (const r of rows) {
    const m = String(r.mese || "").slice(0, 7);
    if (!m) continue;
    if (!by.has(m)) by.set(m, { key: m, corse: 0, rit: 0, min: 0, sopp: 0, canc: 0 });
    const o = by.get(m);
    o.corse += toNum(r.corse_osservate);
    o.rit += toNum(r.in_ritardo);
    o.min += toNum(r.minuti_ritardo_tot);
    o.sopp += toNum(r.soppresse);
    const cv = r.cancellate_tot !== undefined && r.cancellate_tot !== "" ? r.cancellate_tot : r.cancellate;
    o.canc += toNum(cv);
  }

  const out = Array.from(by.values()).sort((a, b) => String(a.key).localeCompare(String(b.key)));
  const x = out.map((o) => o.key);
  const y = out.map((o) => computeValue(o.corse, o.rit, o.min, o.sopp, o.canc));

  return { x, y };
}

function isCardCollapsed(el) {
  if (!el) return false;
  const card = el.closest && el.closest(".card");
  return card ? card.classList.contains("card--collapsed") : false;
}

function seriesDelayIndex() {
  const stationFiltered = hasStationFilter();
  let rows;

  if (stationFiltered) {
    rows = state.data.odMonthCatSeg && state.data.odMonthCatSeg.length ? state.data.odMonthCatSeg : [];
  } else {
    rows = state.data.kpiMonthCatSeg && state.data.kpiMonthCatSeg.length ? state.data.kpiMonthCatSeg : state.data.kpiMonthCat;
  }
  rows = rows || [];

  if (state.filters.year !== "all") rows = rows.filter((r) => passYear(r, "mese"));
  if (state.filters.cat !== "all") rows = rows.filter(passCat);

  if (stationFiltered) {
    if (state.filters.dep !== "all") rows = rows.filter(passDep);
    if (state.filters.arr !== "all") rows = rows.filter(passArr);
  }

  rows = rows.filter((r) => passMonthRange(r, "mese"));
  rows = rows.filter(passSegmentFilters);

  const by = new Map();
  for (const r of rows) {
    const m = String(r.mese || "").slice(0, 7);
    if (!m) continue;
    if (!by.has(m)) by.set(m, { key: m, corse: 0, rit: 0, canc: 0, sopp: 0 });
    const o = by.get(m);
    o.corse += toNum(r.corse_osservate);
    o.rit += toNum(r.in_ritardo);
    const cv = r.cancellate_tot !== undefined && r.cancellate_tot !== "" ? r.cancellate_tot : r.cancellate;
    o.canc += toNum(cv);
    o.sopp += toNum(r.soppresse);
  }

  const out = Array.from(by.values()).sort((a, b) => String(a.key).localeCompare(String(b.key)));
  const x = out.map((o) => o.key);
  const y = out.map((o) => o.corse > 0 ? ((o.rit + o.canc + o.sopp) / o.corse) * 100 : 0);

  return { x, y };
}

function renderSeries() {
  if (typeof Plotly !== "object") return;

  const diEl = document.getElementById("chartDelayIndex");
  const mEl = firstEl(["chartMonthly", "chartMonth", "chartMese", "chartSeriesMonthly"]);

  if (diEl && !isCardCollapsed(diEl)) {
    const di = seriesDelayIndex();
    Plotly.react(
      diEl,
      [{ x: di.x, y: di.y, type: "scatter", mode: "lines+markers", name: "Delay Index (%)", line: { color: "#ff7aa2" } }],
      {
        margin: { l: 55, r: 20, t: 10, b: 50 },
        yaxis: { title: "Delay Index (%)", rangemode: "tozero" },
        xaxis: { type: "category" },
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
        font: { color: "#e8eefc" }
      },
      { displayModeBar: false, responsive: true }
    );
  }

  if (mEl && !isCardCollapsed(mEl)) {
    const m = seriesMonthly();
    const yTitle = metricLabel();
    Plotly.react(
      mEl,
      [{ x: m.x, y: m.y, type: "scatter", mode: "lines+markers", name: yTitle }],
      {
        margin: { l: 50, r: 20, t: 10, b: 50 },
        yaxis: { title: yTitle, rangemode: "tozero" },
        xaxis: { type: "category" },
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
        font: { color: "#e8eefc" }
      },
      { displayModeBar: false, responsive: true }
    );
  }
}

function normalizeBucketLabel(s) {
  return String(s || "").replace(/\s+/g, "").trim();
}

function renderHist() {
  if (typeof Plotly !== "object") return;

  const chart = firstEl(["chartHist", "histChart", "chartDistribution"]);
  if (!chart) return;
  if (isCardCollapsed(chart)) return;

  ensureHistToggle();

  const toggle = document.getElementById("histModeToggle");
  const showPct = !!(toggle && toggle.checked);

  const stationFiltered = hasStationFilter();
  let base, keyField;

  if (stationFiltered) {
    base = state.data.histStationsMonthRuoloSeg;
    keyField = "mese";
  } else {
    base = state.data.histMonthCatSeg;
    keyField = "mese";
  }

  let rows = base || [];

  if (state.filters.year !== "all") rows = rows.filter((r) => passYear(r, keyField));
  if (state.filters.cat !== "all") rows = rows.filter(passCat);

  if (stationFiltered) {
    const dep = state.filters.dep;
    const arr = state.filters.arr;
    if (arr !== "all") {
      rows = rows.filter((r) => String(r.cod_stazione || "").trim() === arr && String(r.ruolo || "").trim() === "arrivo");
    } else if (dep !== "all") {
      rows = rows.filter((r) => String(r.cod_stazione || "").trim() === dep && String(r.ruolo || "").trim() === "partenza");
    }
  }

  rows = rows.filter((r) => passMonthRange(r, "mese"));
  rows = rows.filter(passSegmentFilters);

  const byBucket = new Map();
  let total = 0;

  for (const r of rows) {
    const raw = String(r.bucket_ritardo_arrivo || r.bucket || "").trim();
    if (!raw) continue;
    const key = normalizeBucketLabel(raw);
    const c = toNum(r.count);
    total += c;
    if (!byBucket.has(key)) byBucket.set(key, { label: raw, count: 0 });
    byBucket.get(key).count += c;
  }

  const order =
    Array.isArray(state.manifest.delay_bucket_labels) && state.manifest.delay_bucket_labels.length
      ? state.manifest.delay_bucket_labels
      : safeManifestDefaults().delay_bucket_labels;

  const x = [];
  const y = [];

  for (const lab of order) {
    const key = normalizeBucketLabel(lab);
    const obj = byBucket.get(key);
    const c = obj ? obj.count : 0;
    x.push(lab);
    y.push(showPct ? (total > 0 ? (c / total) * 100 : 0) : c);
  }

  Plotly.react(
    chart,
    [{ x, y, type: "bar", name: showPct ? "%" : "Conteggio" }],
    {
      margin: { l: 50, r: 20, t: 10, b: 70 },
      yaxis: { title: showPct ? "%" : "Conteggio", rangemode: "tozero" },
      xaxis: { tickangle: -35 },
      paper_bgcolor: "rgba(0,0,0,0)",
      plot_bgcolor: "rgba(0,0,0,0)",
      font: { color: "#e8eefc" }
    },
    { displayModeBar: false, responsive: true }
  );
}

function capoluogoKey(cityName) {
  const name = normalizeText(cityName);
  if (!name) return "";
  if (!state.capoluoghiSet || state.capoluoghiSet.size === 0) return name;
  if (state.capoluoghiSet.has(name)) return name;

  for (const cap of state.capoluoghiSet) {
    if (name.startsWith(cap + " ") || name.startsWith(cap + "-") || name.startsWith(cap + "'")) return cap;
  }

  return "";
}


function prettyCityName(cityKey, fallback) {
  const raw = String(fallback || "").trim();
  if (raw && normalizeText(raw) === cityKey) return raw;

  return String(cityKey || "")
    .toLowerCase()
    .replace(/\b([a-zàèéìòù])/g, (m) => m.toUpperCase());
}

function getStationsMetric() {
  const sel = document.getElementById("stationsMetricSel");
  return sel ? (sel.value || "pct_ritardo") : "pct_ritardo";
}

function stationsMetricLabel() {
  const m = getStationsMetric();
  const labels = {
    pct_ritardo: "% in ritardo",
    in_ritardo: "In ritardo",
    minuti_ritardo_tot: "Minuti ritardo",
    cancellate_tot: "Cancellati",
    soppresse: "Soppressi",
    corse_osservate: "Corse osservate"
  };
  return labels[m] || m;
}

function renderStationsTop10() {
  if (typeof Plotly !== "object") return;

  const chart = document.getElementById("chartStationsTop10");
  if (!chart) return;
  if (isCardCollapsed(chart)) return;

  const base = state.data.stationsMonthNodeSeg;
  const keyField = "mese";

  let rows = base || [];

  if (state.filters.year !== "all") rows = rows.filter((r) => passYear(r, keyField));
  if (state.filters.cat !== "all") rows = rows.filter(passCat);

  rows = rows.filter((r) => passMonthRange(r, "mese"));
  rows = rows.filter(passSegmentFilters);

  const agg = new Map();

  for (const r of rows) {
    const code = String(r.cod_stazione || "").trim();
    if (!code) continue;

    if (!agg.has(code)) {
      agg.set(code, {
        cod_stazione: code,
        nome_stazione: stationName(code, r.nome_stazione || ""),
        corse_osservate: 0,
        in_ritardo: 0,
        minuti_ritardo_tot: 0,
        cancellate_tot: 0,
        soppresse: 0
      });
    }

    const a = agg.get(code);
    a.corse_osservate += toNum(r.corse_osservate);
    a.in_ritardo += toNum(r.in_ritardo);
    a.minuti_ritardo_tot += toNum(r.minuti_ritardo_tot);
    const canc = r.cancellate_tot !== undefined && r.cancellate_tot !== "" ? r.cancellate_tot : r.cancellate;
    a.cancellate_tot += toNum(canc);
    a.soppresse += toNum(r.soppresse);
  }

  let out = Array.from(agg.values());
  out.forEach((o) => {
    o.pct_ritardo = o.corse_osservate > 0 ? (o.in_ritardo / o.corse_osservate) * 100 : 0;
  });

  const metric = getStationsMetric();
  out.sort((a, b) => toNum(b[metric]) - toNum(a[metric]));
  const top10 = out.slice(0, 10).reverse();

  const yLabels = top10.map((o) => o.nome_stazione || o.cod_stazione);
  const xValues = top10.map((o) => toNum(o[metric]));
  const label = stationsMetricLabel();

  Plotly.react(
    chart,
    [{
      x: xValues,
      y: yLabels,
      type: "bar",
      orientation: "h",
      name: label,
      marker: { color: "rgba(122, 162, 255, 0.75)" }
    }],
    {
      margin: { l: 180, r: 30, t: 10, b: 50 },
      xaxis: { title: label, rangemode: "tozero" },
      yaxis: { automargin: true },
      paper_bgcolor: "rgba(0,0,0,0)",
      plot_bgcolor: "rgba(0,0,0,0)",
      font: { color: "#e8eefc" }
    },
    { displayModeBar: false, responsive: true }
  );
}

function mapMetricValue(row) {
  const corse = toNum(row.corse_osservate);
  const rit = toNum(row.in_ritardo);
  const min = toNum(row.minuti_ritardo_tot);
  const sopp = toNum(row.soppresse);
  const canc = toNum(row.cancellate_tot);
  return computeValue(corse, rit, min, sopp, canc);
}

function renderMap() {
  if (!state.map) return;

  const mapEl = document.getElementById("map");
  if (isCardCollapsed(mapEl)) return;

  clearMarkers();

  const base = state.data.stationsMonthNodeSeg;
  const keyField = "mese";

  let rows = base || [];

  if (state.filters.year !== "all") rows = rows.filter((r) => passYear(r, keyField));
  if (state.filters.cat !== "all") rows = rows.filter(passCat);

  rows = rows.filter((r) => passMonthRange(r, "mese"));
  rows = rows.filter(passSegmentFilters);

  const agg = new Map();

  for (const r of rows) {
    const code = String(r.cod_stazione || "").trim();
    if (!code) continue;

    const city = stationCity(code, r.nome_stazione || code);
    if (!city) continue;

    const cityKey = capoluogoKey(city);
    if (!cityKey) continue;

    const coords = stationCoords(code);
    if (!coords) continue;

    if (!agg.has(cityKey)) {
      agg.set(cityKey, {
        cityKey,
        nome: prettyCityName(cityKey, city),
        corse_osservate: 0,
        in_ritardo: 0,
        minuti_ritardo_tot: 0,
        soppresse: 0,
        cancellate_tot: 0,
        lat_weighted_sum: 0,
        lon_weighted_sum: 0,
        weight_sum: 0
      });
    }

    const a = agg.get(cityKey);
    const corse = toNum(r.corse_osservate);
    const weight = Math.max(1, corse);

    a.corse_osservate += corse;
    a.in_ritardo += toNum(r.in_ritardo);
    a.minuti_ritardo_tot += toNum(r.minuti_ritardo_tot);
    a.soppresse += toNum(r.soppresse);
    const canc = r.cancellate_tot !== undefined && r.cancellate_tot !== "" ? r.cancellate_tot : r.cancellate;
    a.cancellate_tot += toNum(canc);

    a.lat_weighted_sum += toNum(coords.lat) * weight;
    a.lon_weighted_sum += toNum(coords.lon) * weight;
    a.weight_sum += weight;
  }

  const pts = Array.from(agg.values())
    .map((o) => {
      const w = o.weight_sum > 0 ? o.weight_sum : 1;
      const v = mapMetricValue(o);
      return {
        ...o,
        coords: { lat: o.lat_weighted_sum / w, lon: o.lon_weighted_sum / w },
        v
      };
    })
    .filter((o) => Number.isFinite(o.coords.lat) && Number.isFinite(o.coords.lon));

  pts.sort((a, b) => toNum(b.v) - toNum(a.v));
  const top = pts.slice(0, 250);

  const values = top.map((p) => Math.max(0, Number(p.v) || 0));
  const maxValue = values.length ? Math.max(...values) : 0;
  const minRadius = 5;
  const maxRadius = 22;

  const bounds = [];
  for (const p of top) {
    const val = Math.max(0, Number(p.v) || 0);
    const label = p.nome + "<br>" + metricLabel() + ": " + fmtFloat(val);

    const ratio = maxValue > 0 ? Math.sqrt(val / maxValue) : 0;
    const radius = minRadius + ratio * (maxRadius - minRadius);

    const m = L.circleMarker([p.coords.lat, p.coords.lon], {
      radius,
      opacity: 0.9,
      fillOpacity: 0.6
    }).addTo(state.map);

    try {
      m.bindPopup(label);
    } catch {}

    state.markers.push(m);
    bounds.push([p.coords.lat, p.coords.lon]);
  }

  if (bounds.length > 3) {
    try {
      state.map.fitBounds(bounds, { padding: [20, 20] });
    } catch {}
  }

  setTimeout(() => {
    try {
      state.map.invalidateSize();
    } catch {}
  }, 100);
}

function renderTables() {
  renderStationsTop10();
}

function initCollapsibleCards() {
  document.querySelectorAll(".card.collapsible").forEach(function(card) {
    const toggle = card.querySelector(".card-toggle");
    if (!toggle) return;

    toggle.addEventListener("click", function() {
      const collapsed = card.classList.toggle("card--collapsed");
      toggle.textContent = collapsed ? "\u25B6" : "\u25BC";

      if (!collapsed) {
        const chartEl = card.querySelector(".chart, .map");
        if (!chartEl) return;
        const id = chartEl.id;
        if (id === "chartDelayIndex" || id === "chartMonthly") renderSeries();
        else if (id === "chartHist") renderHist();
        else if (id === "map") {
          initMap();
          renderMap();
          setTimeout(function() { try { state.map.invalidateSize(); } catch {} }, 200);
        } else if (id === "chartStationsTop10") renderStationsTop10();
      }
    });
  });
}

function initStationsMetricSel() {
  const sel = document.getElementById("stationsMetricSel");
  if (!sel) return;
  sel.onchange = function() { renderStationsTop10(); };
}

function renderAll() {
  renderKPI();
  renderSeries();
  renderHist();
  renderTables();
  renderMap();
}

async function loadStationsDimAnyBase(primaryBase) {
  const base = ensureTrailingSlash(primaryBase);
  const tries = uniq([
    ...candidateFilePaths(base, "stations_dim.csv"),
    ...candidateFilePaths("data/", "stations_dim.csv"),
    ...candidateFilePaths("./data/", "stations_dim.csv")
  ]);
  for (const p of tries) {
    const t = await fetchTextOrNull(p);
    if (t && String(t).trim().length > 20) return parseCSV(t);
  }
  return [];
}

async function loadCapoluoghiAnyBase(primaryBase) {
  const base = ensureTrailingSlash(primaryBase);
  const tries = uniq([
    ...candidateFilePaths(base, "capoluoghi_provincia.csv"),
    ...candidateFilePaths("data/", "capoluoghi_provincia.csv"),
    ...candidateFilePaths("./data/", "capoluoghi_provincia.csv")
  ]);
  for (const p of tries) {
    const t = await fetchTextOrNull(p);
    if (t && String(t).trim().length > 5) return parseCSV(t);
  }
  return [];
}

async function loadAll() {
  setMeta("Caricamento dati...");

  const base = await pickDataBase();
  state.dataBase = base;

  const man = await fetchJsonAny(candidateFilePaths(base, "manifest.json"));
  state.manifest = man || safeManifestDefaults();

  const built = state.manifest && state.manifest.built_at_utc ? state.manifest.built_at_utc : "";
  setMeta((built ? "Build: " + built : "Build: sconosciuta") + " | base: " + base);

  const files =
    state.manifest && Array.isArray(state.manifest.gold_files) && state.manifest.gold_files.length
      ? state.manifest.gold_files
      : safeManifestDefaults().gold_files;

  const wanted = uniq([
    ...files,
    "kpi_mese.csv",
    "kpi_mese_categoria.csv",
    "kpi_mese_categoria_segmenti.csv",
    "hist_mese_categoria.csv",
    "hist_mese_categoria_segmenti.csv",
    "stazioni_mese_categoria_nodo.csv",
    "stazioni_mese_categoria_nodo_segmenti.csv",
    "od_mese_categoria.csv",
    "od_mese_categoria_segmenti.csv",
    "hist_stazioni_mese_categoria_ruolo.csv",
    "hist_stazioni_mese_categoria_ruolo_segmenti.csv"
  ]);

  const texts = await Promise.all(wanted.map((f) => fetchTextAny(candidateFilePaths(base, f))));

  const parsed = {};
  for (let i = 0; i < wanted.length; i++) {
    const txt = texts[i];
    parsed[wanted[i]] = txt ? parseCSV(txt) : [];
  }

  state.data.kpiMonth = parsed["kpi_mese.csv"] || [];
  state.data.kpiMonthCat = parsed["kpi_mese_categoria.csv"] || [];
  state.data.kpiMonthCatSeg = parsed["kpi_mese_categoria_segmenti.csv"] || state.data.kpiMonthCat;
  state.data.histMonthCat = parsed["hist_mese_categoria.csv"] || [];
  state.data.histMonthCatSeg = parsed["hist_mese_categoria_segmenti.csv"] || state.data.histMonthCat;
  state.data.stationsMonthNode = parsed["stazioni_mese_categoria_nodo.csv"] || [];
  state.data.stationsMonthNodeSeg = parsed["stazioni_mese_categoria_nodo_segmenti.csv"] || state.data.stationsMonthNode;
  state.data.odMonthCat = parsed["od_mese_categoria.csv"] || [];
  state.data.odMonthCatSeg = parsed["od_mese_categoria_segmenti.csv"] || state.data.odMonthCat;
  state.data.histStationsMonthRuolo = parsed["hist_stazioni_mese_categoria_ruolo.csv"] || [];
  state.data.histStationsMonthRuoloSeg = parsed["hist_stazioni_mese_categoria_ruolo_segmenti.csv"] || state.data.histStationsMonthRuolo;

  const stRows = await loadStationsDimAnyBase(base);

  state.stationsRef.clear();

  let stationDimBuilt = "";
  for (const r of stRows) {
    const b = r.built_at_utc || r.built_at || r.data_build || r.data || "";
    if (b && !stationDimBuilt) stationDimBuilt = String(b).trim();

    const code = String(r.cod_stazione || r.codice || r.cod || "").trim();
    if (!code) continue;

    const name = String(r.nome_stazione || r.nome_norm || r.nome || "").trim();
    const city = String(r.citta || r.comune || r.city || r.nome_comune || "").trim();

    const lat = parseNumberAny(r.lat ?? r.latitude ?? r.latitudine ?? r.y);
    const lon = parseNumberAny(r.lon ?? r.lng ?? r.longitude ?? r.longitudine ?? r.x);

    state.stationsRef.set(code, {
      code,
      name,
      lat: Number.isFinite(lat) ? lat : NaN,
      lon: Number.isFinite(lon) ? lon : NaN,
      city
    });
  }

  const capRows = await loadCapoluoghiAnyBase(base);
  state.capoluoghiSet = new Set(
    capRows
      .map((r) => normalizeText(r.citta || r.capoluogo || r.nome || r.city || ""))
      .filter(Boolean)
  );

  initFilters();
  initSegmentControls();
  const mapCardCollapsed = (function() {
    const mapEl = document.getElementById("map");
    const card = mapEl && mapEl.closest && mapEl.closest(".card");
    return card && card.classList.contains("card--collapsed");
  }());
  if (!mapCardCollapsed) initMap();
  ensureHistToggle();
  initCollapsibleCards();
  initStationsMetricSel();

  renderAll();

  const haveAny =
    (state.data.kpiMonthCat && state.data.kpiMonthCat.length) ||
    (state.data.kpiMonthCatSeg && state.data.kpiMonthCatSeg.length) ||
    (state.data.histMonthCatSeg && state.data.histMonthCatSeg.length);

  const coordCount = Array.from(state.stationsRef.values()).filter((s) => Number.isFinite(s.lat) && Number.isFinite(s.lon)).length;

  const metaExtra =
    " | mese cat: " +
    (state.data.kpiMonthCat ? state.data.kpiMonthCat.length : 0) +
    " | mese cat segmenti: " +
    (state.data.kpiMonthCatSeg ? state.data.kpiMonthCatSeg.length : 0) +
    " | stazioni dim: " +
    stRows.length +
    " | stazioni coord: " +
    coordCount +
    (stationDimBuilt ? " | stations_dim build: " + stationDimBuilt : "");

  if (!haveAny) setMeta("Errore: non trovo CSV validi. Base: " + base + metaExtra);
  else setMeta((built ? "Build: " + built : "Build: sconosciuta") + " | base: " + base + metaExtra);
}

loadAll().catch((err) => {
  console.error(err);
  setMeta("Errore caricamento dati: " + (err && err.message ? err.message : String(err)));
});
