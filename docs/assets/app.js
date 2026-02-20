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

/* ────────────────── fetch helpers ────────────────── */

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
  try { return await fetchText(path); } catch { return null; }
}

async function fetchJsonOrNull(path) {
  try { return await fetchJson(path); } catch { return null; }
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

function uniq(arr) { return Array.from(new Set(arr)); }

function isMobile() { return window.innerWidth <= 600; }

function mobileChartMargins(desktop) {
  if (!isMobile()) return desktop;
  return { l: Math.min(desktop.l || 50, 35), r: Math.min(desktop.r || 20, 10), t: Math.min(desktop.t || 10, 5), b: Math.min(desktop.b || 50, 40) };
}

function mobileFont() {
  return isMobile() ? { color: "#e8eefc", size: 9 } : { color: "#e8eefc" };
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
    "kpi_dettaglio_categoria.csv",
    "gold/kpi_dettaglio_categoria.csv",
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

/* ────────────────── CSV parser ────────────────── */

function detectDelimiter(line) {
  const s = String(line || "");
  let comma = 0, semi = 0, tab = 0;
  let inQ = false;
  for (let i = 0; i < s.length; i++) {
    const ch = s[i];
    if (ch === '"') { if (inQ && s[i + 1] === '"') i++; else inQ = !inQ; continue; }
    if (!inQ) { if (ch === ",") comma++; else if (ch === ";") semi++; else if (ch === "\t") tab++; }
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
    if (ch === '"') { if (inQ && line[i + 1] === '"') { cur += '"'; i++; } else { inQ = !inQ; } continue; }
    if (ch === d && !inQ) { out.push(cur); cur = ""; continue; }
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

/* ────────────────── format helpers ────────────────── */

function toNum(x) { const v = Number(x); return Number.isFinite(v) ? v : 0; }

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

function fmtInt(x) { return Math.round(Number(x) || 0).toLocaleString("it-IT"); }

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

function yearFromMonth(mese) { return String(mese || "").slice(0, 4); }

function firstEl(ids) {
  for (const id of ids) { const el = document.getElementById(id); if (el) return el; }
  return null;
}

function setTextByIds(ids, value) { const el = firstEl(ids); if (el) el.innerText = value; }

function setMeta(text) { const el = document.getElementById("metaBox"); if (el) el.innerText = text; }

/* ────────────────── manifest defaults ────────────────── */

function safeManifestDefaults() {
  return {
    built_at_utc: "",
    gold_files: [
      "kpi_mese.csv",
      "kpi_mese_categoria.csv",
      "kpi_dettaglio.csv",
      "kpi_dettaglio_categoria.csv",
      "hist_mese_categoria.csv",
      "hist_dettaglio_categoria.csv",
      "stazioni_mese_categoria_nodo.csv",
      "stazioni_dettaglio_categoria_nodo.csv",
      "od_mese_categoria.csv",
      "od_dettaglio_categoria.csv",
      "hist_stazioni_mese_categoria_ruolo.csv",
      "hist_stazioni_dettaglio_categoria_ruolo.csv"
    ],
    delay_bucket_labels: [
      "<=-60","(-60,-30]","(-30,-15]","(-15,-10]","(-10,-5]","(-5,-1]",
      "(-1,0]","(0,1]","(1,5]","(5,10]","(10,15]","(15,30]","(30,60]","(60,120]",">120"
    ]
  };
}

/* ────────────────── global state ────────────────── */

const DAY_TYPES   = ["infrasettimanale", "weekend"];
const TIME_SLOTS  = ["mattina", "tarda_mattina", "pomeriggio", "sera", "notte"];

const state = {
  dataBase: "data/",
  manifest: safeManifestDefaults(),
  data: {
    kpiMonth: [],
    kpiMonthCat: [],
    kpiDetail: [],
    kpiDetailCat: [],
    histMonthCat: [],
    histDetailCat: [],
    stationsMonthNode: [],
    stationsDetailNode: [],
    odMonthCat: [],
    odDetailCat: [],
    histStationsMonthRuolo: [],
    histStationsDetailRuolo: []
  },
  stationsRef: new Map(),
  capoluoghiSet: new Set(),
  _depItems: [],
  _arrItems: [],
  _depAliases: null,  // Set of all codes for selected dep station
  _arrAliases: null,  // Set of all codes for selected arr station
  map: null,
  markers: [],
  filters: {
    year: "all",
    cat: "all",
    dep: "all",
    arr: "all",
    month_from: "",
    month_to: "",
    day_types:  [true, true],                    // infrasettimanale, weekend
    time_slots: [true, true, true, true, true]   // mattina, tarda_mattina, pomeriggio, sera, notte
  }
};

/* ────────────────── station helpers ────────────────── */

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
  const lat = ref.lat, lon = ref.lon;
  if (typeof lat !== "number" || typeof lon !== "number") return null;
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
  return { lat, lon };
}

/**
 * Deduplicate stations: group codes sharing the same name,
 * keep one representative code per name (prefer codes with coordinates).
 */
function buildStationItems(codes) {
  const byName = new Map();
  for (const code of (codes || [])) {
    const name = stationName(code, code);
    const key = normalizeText(name);
    if (!byName.has(key)) {
      byName.set(key, { code, name, codes: [code] });
    } else {
      const entry = byName.get(key);
      entry.codes.push(code);
      // prefer a code that has coordinates
      const curCoords = stationCoords(entry.code);
      const newCoords = stationCoords(code);
      if (!curCoords && newCoords) {
        entry.code = code;
        entry.name = name;
      }
    }
  }
  const items = Array.from(byName.values()).map((e) => ({
    code: e.code, name: e.name, codes: e.codes,
    needle: normalizeText(e.name + " " + e.codes.join(" "))
  }));
  items.sort((a, b) => a.name.localeCompare(b.name, "it", { sensitivity: "base" }));
  return items;
}

/* Map: station name (normalized) -> array of all codes sharing that name */
function buildNameToCodesMap() {
  const map = new Map();
  for (const [code, ref] of state.stationsRef) {
    const key = normalizeText(ref.name || code);
    if (!map.has(key)) map.set(key, []);
    map.get(key).push(code);
  }
  return map;
}

function fillStationSelect(selectEl, items, query) {
  if (!selectEl) return;
  const q = normalizeText(query);
  const cur = selectEl.value;
  selectEl.innerHTML = "";
  selectEl.appendChild(new Option("Tutte", "all"));
  for (const it of items) {
    if (q && !it.needle.includes(q)) continue;
    selectEl.appendChild(new Option(it.name, it.code));
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

/* ────────────────── new filter logic ────────────────── */

function hasDetailFilter() {
  return state.filters.day_types.some((x) => !x) || state.filters.time_slots.some((x) => !x);
}

function hasStationFilter() {
  return state.filters.dep !== "all" || state.filters.arr !== "all";
}

function hasMonthRange() {
  return !!(state.filters.month_from || state.filters.month_to);
}

function passCat(r) {
  if (state.filters.cat === "all") return true;
  return String(r.categoria || "").trim() === state.filters.cat;
}

function passDep(r) {
  if (state.filters.dep === "all") return true;
  const code = String(r.cod_partenza || "").trim();
  if (code === state.filters.dep) return true;
  const aliases = state._depAliases;
  return aliases ? aliases.has(code) : false;
}

function passArr(r) {
  if (state.filters.arr === "all") return true;
  const code = String(r.cod_arrivo || "").trim();
  if (code === state.filters.arr) return true;
  const aliases = state._arrAliases;
  return aliases ? aliases.has(code) : false;
}

function passYear(r, field) {
  if (state.filters.year === "all") return true;
  return String(r[field] || "").slice(0, 4) === state.filters.year;
}

function passMonthRange(r, field) {
  if (!hasMonthRange()) return true;
  // Extract month number (MM) from YYYY-MM field
  const raw = String(r[field] || "").slice(5, 7);
  if (!raw) return false;
  const mm = parseInt(raw, 10);
  if (!mm) return false;

  const from = parseInt(state.filters.month_from || "0", 10);
  const to   = parseInt(state.filters.month_to || "0", 10);
  const a = from || to;
  const b = to || from;
  const lo = Math.min(a, b);
  const hi = Math.max(a, b);
  return mm >= lo && mm <= hi;
}

function passDetailDimensions(r) {
  // tipo_giorno
  const dt = state.filters.day_types;
  if (dt.some((x) => !x)) {
    const tg = String(r.tipo_giorno || "").trim();
    const idx = DAY_TYPES.indexOf(tg);
    if (idx === -1 || !dt[idx]) return false;
  }
  // fascia_oraria
  const ts = state.filters.time_slots;
  if (ts.some((x) => !x)) {
    const fa = String(r.fascia_oraria || "").trim();
    const idx = TIME_SLOTS.indexOf(fa);
    if (idx === -1 || !ts[idx]) return false;
  }
  return true;
}

/* ────────────────── toggle controls init ────────────────── */

function initToggleControls() {
  const dayTypeWrap = document.getElementById("dayTypeWrap");
  const timeSlotWrap = document.getElementById("timeSlotWrap");
  if (!dayTypeWrap || !timeSlotWrap) return;

  // Already built?
  if (dayTypeWrap.children.length) return;

  const dayLabels = ["I", "W"];
  const dayTitles = ["Infrasettimanale (Lun\u2013Ven)", "Weekend (Sab\u2013Dom)"];
  dayLabels.forEach((label, i) => {
    const b = document.createElement("button");
    b.type = "button";
    b.className = "toggle-pill" + (state.filters.day_types[i] ? "" : " off");
    b.innerText = label;
    b.title = dayTitles[i];
    b.onclick = () => {
      state.filters.day_types[i] = !state.filters.day_types[i];
      b.classList.toggle("off", !state.filters.day_types[i]);
      renderAll();
    };
    dayTypeWrap.appendChild(b);
  });

  const slotLabels = ["Ma", "TM", "Po", "Se", "No"];
  const slotTitles = ["Mattina (6\u201308:59)", "Tarda mattina (9\u201313:59)", "Pomeriggio (14\u201317:59)", "Sera (18\u201321:59)", "Notte (22\u201305:59)"];
  slotLabels.forEach((label, i) => {
    const b = document.createElement("button");
    b.type = "button";
    b.className = "toggle-pill" + (state.filters.time_slots[i] ? "" : " off");
    b.innerText = label;
    b.title = slotTitles[i];
    b.onclick = () => {
      state.filters.time_slots[i] = !state.filters.time_slots[i];
      b.classList.toggle("off", !state.filters.time_slots[i]);
      renderAll();
    };
    timeSlotWrap.appendChild(b);
  });
}

function syncToggleUI() {
  const dayTypeWrap = document.getElementById("dayTypeWrap");
  const timeSlotWrap = document.getElementById("timeSlotWrap");
  if (dayTypeWrap) {
    Array.from(dayTypeWrap.children).forEach((b, i) => {
      b.classList.toggle("off", !state.filters.day_types[i]);
    });
  }
  if (timeSlotWrap) {
    Array.from(timeSlotWrap.children).forEach((b, i) => {
      b.classList.toggle("off", !state.filters.time_slots[i]);
    });
  }
}

/* ────────────────── map init ────────────────── */

function initMap() {
  const mapEl = firstEl(["map", "mapStations", "stationsMap"]);
  if (!mapEl) return;
  if (typeof L !== "object" || typeof L.map !== "function") return;
  if (state.map) return;

  state.map = L.map(mapEl.id, { center: [42.5, 12.5], zoom: 6, zoomSnap: 0.5 });
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    attribution: "&copy; OpenStreetMap contributors", maxZoom: 18
  }).addTo(state.map);

  setTimeout(() => { try { state.map.invalidateSize(); } catch {} }, 150);
}

function clearMarkers() {
  if (!state.map) return;
  for (const m of state.markers) { try { state.map.removeLayer(m); } catch {} }
  state.markers = [];
}

/* ────────────────── filters init ────────────────── */

/* ────────────────── category label map ────────────────── */

const CATEGORY_LABELS = {
  "DIR":  "DIR \u2013 Diretto",
  "EC":   "EC \u2013 EuroCity",
  "ECFR": "ECFR \u2013 EuroCity FrecciaRossa",
  "EN":   "EN \u2013 EuroNight",
  "EXP":  "EXP \u2013 Espresso",
  "FA":   "FA \u2013 Freccia Argento",
  "FB":   "FB \u2013 Freccia Bianca",
  "FR":   "FR \u2013 Freccia Rossa",
  "IC":   "IC \u2013 InterCity",
  "ICN":  "ICN \u2013 InterCity Notte",
  "IR":   "IR \u2013 InterRegionale",
  "MET":  "MET \u2013 Metropolitano",
  "NCL":  "NCL \u2013 Notte cuccette",
  "REG":  "REG \u2013 Regionale"
};

function categoryDisplayName(cat) {
  const c = String(cat || "").trim();
  return CATEGORY_LABELS[c] || c;
}

/* ────────────────── month names ────────────────── */

const MONTH_NAMES = [
  "Gennaio","Febbraio","Marzo","Aprile","Maggio","Giugno",
  "Luglio","Agosto","Settembre","Ottobre","Novembre","Dicembre"
];

/** Convert "YYYY-MM" to "MM/AA" for compact x-axis labels */
function fmtMonthShort(ym) {
  const parts = String(ym || "").split("-");
  if (parts.length < 2) return ym;
  return parts[1] + "/" + parts[0].slice(2);
}

function updateDepAliases() {
  if (state.filters.dep === "all") { state._depAliases = null; return; }
  const item = state._depItems.find((it) => it.code === state.filters.dep);
  state._depAliases = item ? new Set(item.codes) : new Set([state.filters.dep]);
}

function updateArrAliases() {
  if (state.filters.arr === "all") { state._arrAliases = null; return; }
  const item = state._arrItems.find((it) => it.code === state.filters.arr);
  state._arrAliases = item ? new Set(item.codes) : new Set([state.filters.arr]);
}

function initFilters() {
  const yearSel = firstEl(["yearSel", "annoSel", "year"]);
  const catSel = firstEl(["catSel", "categoriaSel", "category"]);
  const depSel = firstEl(["depSel", "stazionePartenzaSel", "depStationSel"]);
  const arrSel = firstEl(["arrSel", "stazioneArrivoSel", "arrStationSel"]);
  const mapMetricSel = firstEl(["mapMetricSel", "mapSel", "mappaSel"]);
  const resetBtn = firstEl(["resetBtn", "btnReset", "reset"]);
  const monthFrom = document.getElementById("monthFrom");
  const monthTo   = document.getElementById("monthTo");

  const years = uniq(state.data.kpiMonth.map((r) => yearFromMonth(r.mese)).filter(Boolean)).sort();
  const cats = uniq(state.data.kpiMonthCat.map((r) => String(r.categoria || "").trim()).filter((c) => c && c !== "NaN"))
    .sort((a, b) => String(a).localeCompare(String(b), "it", { sensitivity: "base" }));

  if (yearSel) {
    yearSel.innerHTML = "";
    yearSel.appendChild(new Option("Tutti", "all"));
    years.forEach((y) => yearSel.appendChild(new Option(y, y)));
    yearSel.value = state.filters.year || "all";
    yearSel.onchange = () => { state.filters.year = yearSel.value || "all"; renderAll(); };
  }

  if (catSel) {
    catSel.innerHTML = "";
    catSel.appendChild(new Option("Tutte", "all"));
    cats.forEach((c) => catSel.appendChild(new Option(categoryDisplayName(c), c)));
    catSel.value = state.filters.cat || "all";
    catSel.onchange = () => { state.filters.cat = catSel.value || "all"; renderAll(); };
  }

  const deps = uniq([
    ...(state.data.odMonthCat || []).map((r) => r.cod_partenza),
    ...(state.data.odDetailCat || []).map((r) => r.cod_partenza)
  ].filter(Boolean));
  const arrs = uniq([
    ...(state.data.odMonthCat || []).map((r) => r.cod_arrivo),
    ...(state.data.odDetailCat || []).map((r) => r.cod_arrivo)
  ].filter(Boolean));

  const depItems = buildStationItems(deps);
  const arrItems = buildStationItems(arrs);
  state._depItems = depItems;
  state._arrItems = arrItems;

  if (depSel) {
    fillStationSelect(depSel, depItems, "");
    ensureSearchInput(depSel, "depSearch", "Cerca stazione di partenza", depItems);
    depSel.value = state.filters.dep || "all";
    depSel.onchange = () => { state.filters.dep = depSel.value || "all"; updateDepAliases(); renderAll(); };
  }

  if (arrSel) {
    fillStationSelect(arrSel, arrItems, "");
    ensureSearchInput(arrSel, "arrSearch", "Cerca stazione di arrivo", arrItems);
    arrSel.value = state.filters.arr || "all";
    arrSel.onchange = () => { state.filters.arr = arrSel.value || "all"; updateArrAliases(); renderAll(); };
  }

  if (mapMetricSel) {
    if (!mapMetricSel.value) mapMetricSel.value = "pct_ritardo";
    mapMetricSel.onchange = () => { renderSeries(); renderMap(); };
  }

  // Month-only selects (1-12) instead of YYYY-MM inputs
  if (monthFrom) {
    monthFrom.innerHTML = "";
    monthFrom.appendChild(new Option("--", ""));
    for (let i = 0; i < 12; i++) monthFrom.appendChild(new Option(MONTH_NAMES[i], String(i + 1).padStart(2, "0")));
    monthFrom.value = state.filters.month_from || "";
    monthFrom.onchange = () => { state.filters.month_from = monthFrom.value || ""; renderAll(); };
  }
  if (monthTo) {
    monthTo.innerHTML = "";
    monthTo.appendChild(new Option("--", ""));
    for (let i = 0; i < 12; i++) monthTo.appendChild(new Option(MONTH_NAMES[i], String(i + 1).padStart(2, "0")));
    monthTo.value = state.filters.month_to || "";
    monthTo.onchange = () => { state.filters.month_to = monthTo.value || ""; renderAll(); };
  }

  if (resetBtn) {
    resetBtn.onclick = () => {
      state.filters.year = "all";
      state.filters.cat = "all";
      state.filters.dep = "all";
      state.filters.arr = "all";
      state.filters.month_from = "";
      state.filters.month_to = "";
      state.filters.day_types = [true, true];
      state.filters.time_slots = [true, true, true, true, true];
      state._depAliases = null;
      state._arrAliases = null;

      if (yearSel) yearSel.value = "all";
      if (catSel) catSel.value = "all";
      if (depSel) depSel.value = "all";
      if (arrSel) arrSel.value = "all";
      if (monthFrom) monthFrom.value = "";
      if (monthTo) monthTo.value = "";

      syncToggleUI();
      renderAll();
    };
  }
}

/* ────────────────── hist toggle (count / %) ────────────────── */

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
  if (t.checked) { left.classList.remove("active"); right.classList.add("active"); }
  else { left.classList.add("active"); right.classList.remove("active"); }
}

function ensureHistToggle() {
  const chart = firstEl(["chartHist", "histChart", "chartDistribution"]);
  if (!chart) return;
  ensureHistToggleStyles();

  let t = document.getElementById("histModeToggle");
  if (t) { updateHistToggleUI(); t.onchange = () => { updateHistToggleUI(); renderHist(); }; return; }

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
  t.onchange = () => { updateHistToggleUI(); renderHist(); };
}

/* ────────────────── metric helpers ────────────────── */

function useDetailAggregation() {
  return hasDetailFilter() && state.data.kpiDetailCat && state.data.kpiDetailCat.length > 0;
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

function isCardCollapsed(el) {
  if (!el) return false;
  const card = el.closest && el.closest(".card");
  return card ? card.classList.contains("card--collapsed") : false;
}

/* ────────────────── common filter pipeline ────────────────── */

function applyCommonFilters(rows, keyField) {
  if (state.filters.year !== "all") rows = rows.filter((r) => passYear(r, keyField));
  if (state.filters.cat !== "all") rows = rows.filter(passCat);
  if (hasMonthRange()) rows = rows.filter((r) => passMonthRange(r, keyField));
  return rows;
}

function applyDetailDimFilter(rows) {
  if (hasDetailFilter()) rows = rows.filter(passDetailDimensions);
  return rows;
}

/* ────────────────── KPI ────────────────── */

function renderKPI() {
  const stationFiltered = hasStationFilter();
  const useDetail = useDetailAggregation();
  let base;

  if (stationFiltered) {
    const haveOdDet = state.data.odDetailCat && state.data.odDetailCat.length > 0;
    base = (useDetail && haveOdDet) ? state.data.odDetailCat : state.data.odMonthCat;
  } else {
    base = useDetail ? state.data.kpiDetailCat : state.data.kpiMonthCat;
  }

  let rows = base || [];
  rows = applyCommonFilters(rows, "mese");
  if (useDetail) rows = applyDetailDimFilter(rows);

  if (stationFiltered) {
    if (state.filters.dep !== "all") rows = rows.filter(passDep);
    if (state.filters.arr !== "all") rows = rows.filter(passArr);
  }

  const total = rows.reduce((a, r) => a + toNum(r.corse_osservate), 0);
  const late  = rows.reduce((a, r) => a + toNum(r.in_ritardo), 0);
  const mins  = rows.reduce((a, r) => a + toNum(r.minuti_ritardo_tot), 0);
  const canc  = rows.reduce((a, r) => {
    const v = r.cancellate_tot !== undefined && r.cancellate_tot !== "" ? r.cancellate_tot : r.cancellate;
    return a + toNum(v);
  }, 0);
  const sopp = rows.reduce((a, r) => a + toNum(r.soppresse), 0);

  setTextByIds(["cardTotal","kpiTotal","kpiCorse","totalRuns","corseOsservate","corse_osservate"], fmtInt(total));
  setTextByIds(["cardLate","kpiLate","kpiRitardo","lateRuns","inRitardo","in_ritardo"], fmtInt(late));
  setTextByIds(["cardMin","kpiMinutes","kpiMinuti","delayMinutes","kpiLateMin","kpiDelayMinutes","kpiMinTotRitardo","minutiTotali","minuti_totali_ritardo","minutiRitardoTotali","minutesTotal"], fmtInt(mins));
  setTextByIds(["cardCanc","kpiCancelled","kpiCancellati","cancellati","cancellate"], fmtInt(canc));
  setTextByIds(["cardSopp","kpiSuppressed","kpiSoppressi","soppressi","soppresse"], fmtInt(sopp));
}

/* ────────────────── series helpers ────────────────── */

function aggregateByMonth(rows) {
  const by = new Map();
  for (const r of rows) {
    const m = String(r.mese || "").slice(0, 7);
    if (!m) continue;
    if (!by.has(m)) by.set(m, { key: m, corse: 0, rit: 0, min: 0, sopp: 0, canc: 0 });
    const o = by.get(m);
    o.corse += toNum(r.corse_osservate);
    o.rit   += toNum(r.in_ritardo);
    o.min   += toNum(r.minuti_ritardo_tot);
    o.sopp  += toNum(r.soppresse);
    const cv = r.cancellate_tot !== undefined && r.cancellate_tot !== "" ? r.cancellate_tot : r.cancellate;
    o.canc += toNum(cv);
  }
  return Array.from(by.values()).sort((a, b) => String(a.key).localeCompare(String(b.key)));
}

function getFilteredSeriesRows() {
  const stationFiltered = hasStationFilter();
  const useDetail = useDetailAggregation();
  let rows;

  if (stationFiltered) {
    const haveOdDet = state.data.odDetailCat && state.data.odDetailCat.length > 0;
    rows = (useDetail && haveOdDet) ? state.data.odDetailCat : state.data.odMonthCat;
  } else {
    rows = useDetail
      ? (state.data.kpiDetailCat || [])
      : (state.data.kpiMonthCat && state.data.kpiMonthCat.length ? state.data.kpiMonthCat : state.data.kpiMonth);
  }
  rows = rows || [];

  rows = applyCommonFilters(rows, "mese");
  if (useDetail) rows = applyDetailDimFilter(rows);

  if (stationFiltered) {
    if (state.filters.dep !== "all") rows = rows.filter(passDep);
    if (state.filters.arr !== "all") rows = rows.filter(passArr);
  }

  return rows;
}

function seriesMonthly() {
  const rows = getFilteredSeriesRows();
  const out = aggregateByMonth(rows);
  return {
    x: out.map((o) => fmtMonthShort(o.key)),
    y: out.map((o) => computeValue(o.corse, o.rit, o.min, o.sopp, o.canc))
  };
}

function seriesDelayIndex() {
  const rows = getFilteredSeriesRows();
  const out = aggregateByMonth(rows);
  return {
    x: out.map((o) => fmtMonthShort(o.key)),
    y: out.map((o) => o.corse > 0 ? ((o.rit + o.canc + o.sopp) / o.corse) * 100 : 0)
  };
}

/* ────────────────── render series ────────────────── */

function renderSeries() {
  if (typeof Plotly !== "object") return;

  const diEl = document.getElementById("chartDelayIndex");
  const mEl = firstEl(["chartMonthly","chartMonth","chartMese","chartSeriesMonthly"]);

  if (diEl && !isCardCollapsed(diEl)) {
    const di = seriesDelayIndex();
    Plotly.react(diEl,
      [{ x: di.x, y: di.y, type: "scatter", mode: "lines+markers", name: "Delay Index (%)", line: { color: "#ff7aa2" } }],
      { margin:mobileChartMargins({l:55,r:20,t:10,b:50}), yaxis:{title:isMobile()?"":"Delay Index (%)",rangemode:"tozero"}, xaxis:{type:"category"}, paper_bgcolor:"rgba(0,0,0,0)", plot_bgcolor:"rgba(0,0,0,0)", font:mobileFont() },
      { displayModeBar: false, responsive: true }
    );
  }

  if (mEl && !isCardCollapsed(mEl)) {
    const m = seriesMonthly();
    const yTitle = metricLabel();
    Plotly.react(mEl,
      [{ x: m.x, y: m.y, type: "scatter", mode: "lines+markers", name: yTitle }],
      { margin:mobileChartMargins({l:50,r:20,t:10,b:50}), yaxis:{title:isMobile()?"":yTitle,rangemode:"tozero"}, xaxis:{type:"category"}, paper_bgcolor:"rgba(0,0,0,0)", plot_bgcolor:"rgba(0,0,0,0)", font:mobileFont() },
      { displayModeBar: false, responsive: true }
    );
  }
}

/* ────────────────── render histogram ────────────────── */

function normalizeBucketLabel(s) { return String(s || "").replace(/\s+/g, "").trim(); }

function renderHist() {
  if (typeof Plotly !== "object") return;
  const chart = firstEl(["chartHist","histChart","chartDistribution"]);
  if (!chart || isCardCollapsed(chart)) return;

  ensureHistToggle();
  const toggle = document.getElementById("histModeToggle");
  const showPct = !!(toggle && toggle.checked);

  const stationFiltered = hasStationFilter();
  const useDetail = useDetailAggregation();
  let base;

  if (stationFiltered) {
    const haveStDet = state.data.histStationsDetailRuolo && state.data.histStationsDetailRuolo.length > 0;
    base = (useDetail && haveStDet) ? state.data.histStationsDetailRuolo : state.data.histStationsMonthRuolo;
  } else {
    const haveDetHist = state.data.histDetailCat && state.data.histDetailCat.length > 0;
    base = (useDetail && haveDetHist) ? state.data.histDetailCat : state.data.histMonthCat;
  }

  let rows = base || [];
  rows = applyCommonFilters(rows, "mese");
  if (useDetail) rows = applyDetailDimFilter(rows);

  if (stationFiltered) {
    const dep = state.filters.dep;
    const arr = state.filters.arr;
    if (arr !== "all") {
      rows = rows.filter((r) => String(r.cod_stazione || "").trim() === arr && String(r.ruolo || "").trim() === "arrivo");
    } else if (dep !== "all") {
      rows = rows.filter((r) => String(r.cod_stazione || "").trim() === dep && String(r.ruolo || "").trim() === "partenza");
    }
  }

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

  const order = Array.isArray(state.manifest.delay_bucket_labels) && state.manifest.delay_bucket_labels.length
    ? state.manifest.delay_bucket_labels : safeManifestDefaults().delay_bucket_labels;

  const x = [], y = [];
  for (const lab of order) {
    const key = normalizeBucketLabel(lab);
    const obj = byBucket.get(key);
    const c = obj ? obj.count : 0;
    x.push(lab);
    y.push(showPct ? (total > 0 ? (c / total) * 100 : 0) : c);
  }

  Plotly.react(chart,
    [{ x, y, type: "bar", name: showPct ? "%" : "Conteggio" }],
    { margin:mobileChartMargins({l:50,r:20,t:10,b:70}), yaxis:{title:isMobile()?"":showPct?"%":"Conteggio",rangemode:"tozero"}, xaxis:{tickangle:isMobile()?-45:-35,tickfont:{size:isMobile()?7:undefined}}, paper_bgcolor:"rgba(0,0,0,0)", plot_bgcolor:"rgba(0,0,0,0)", font:mobileFont() },
    { displayModeBar: false, responsive: true }
  );
}

/* ────────────────── map helpers ────────────────── */

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
  return String(cityKey || "").toLowerCase().replace(/\b([a-zàèéìòù])/g, (m) => m.toUpperCase());
}

function getStationsMetric() {
  const sel = document.getElementById("stationsMetricSel");
  return sel ? (sel.value || "pct_ritardo") : "pct_ritardo";
}

function stationsMetricLabel() {
  const m = getStationsMetric();
  const labels = { pct_ritardo:"% in ritardo", in_ritardo:"In ritardo", minuti_ritardo_tot:"Minuti ritardo", cancellate_tot:"Cancellati", soppresse:"Soppressi", corse_osservate:"Corse osservate" };
  return labels[m] || m;
}

/* ────────────────── stations top 10 (capoluoghi only) ────────────────── */

function renderStationsTop10() {
  if (typeof Plotly !== "object") return;
  const chart = document.getElementById("chartStationsTop10");
  if (!chart || isCardCollapsed(chart)) return;

  const useDetail = useDetailAggregation() && state.data.stationsDetailNode && state.data.stationsDetailNode.length > 0;
  const base = useDetail ? state.data.stationsDetailNode : state.data.stationsMonthNode;

  let rows = base || [];
  rows = applyCommonFilters(rows, "mese");
  if (useDetail) rows = applyDetailDimFilter(rows);

  // Aggregate by capoluogo (provincial capital) instead of individual station
  const agg = new Map();
  for (const r of rows) {
    const code = String(r.cod_stazione || "").trim();
    if (!code) continue;
    const city = stationCity(code, r.nome_stazione || code);
    if (!city) continue;
    const cityKey = capoluogoKey(city);
    if (!cityKey) continue;  // skip non-capoluogo stations

    if (!agg.has(cityKey)) {
      agg.set(cityKey, { nome: prettyCityName(cityKey, city), corse_osservate:0, in_ritardo:0, minuti_ritardo_tot:0, cancellate_tot:0, soppresse:0 });
    }
    const a = agg.get(cityKey);
    a.corse_osservate += toNum(r.corse_osservate);
    a.in_ritardo += toNum(r.in_ritardo);
    a.minuti_ritardo_tot += toNum(r.minuti_ritardo_tot);
    const canc = r.cancellate_tot !== undefined && r.cancellate_tot !== "" ? r.cancellate_tot : r.cancellate;
    a.cancellate_tot += toNum(canc);
    a.soppresse += toNum(r.soppresse);
  }

  let out = Array.from(agg.values());
  out.forEach((o) => { o.pct_ritardo = o.corse_osservate > 0 ? (o.in_ritardo / o.corse_osservate) * 100 : 0; });

  const metric = getStationsMetric();
  out.sort((a, b) => toNum(b[metric]) - toNum(a[metric]));
  const top10 = out.slice(0, 10).reverse();

  const yLabels = top10.map((o) => o.nome);
  const xValues = top10.map((o) => toNum(o[metric]));
  const label = stationsMetricLabel();

  Plotly.react(chart,
    [{ x:xValues, y:yLabels, type:"bar", orientation:"h", name:label, marker:{color:"rgba(122,162,255,0.75)"} }],
    { margin:isMobile()?{l:10,r:10,t:10,b:40}:{l:180,r:30,t:10,b:50}, xaxis:{title:isMobile()?"":label,rangemode:"tozero"}, yaxis:{automargin:true}, paper_bgcolor:"rgba(0,0,0,0)", plot_bgcolor:"rgba(0,0,0,0)", font:mobileFont() },
    { displayModeBar: false, responsive: true }
  );
}

/* ────────────────── map render ────────────────── */

function mapMetricValue(row) {
  return computeValue(toNum(row.corse_osservate), toNum(row.in_ritardo), toNum(row.minuti_ritardo_tot), toNum(row.soppresse), toNum(row.cancellate_tot));
}

function renderMap() {
  if (!state.map) return;
  const mapEl = document.getElementById("map");
  if (isCardCollapsed(mapEl)) return;

  clearMarkers();

  const useDetail = useDetailAggregation() && state.data.stationsDetailNode && state.data.stationsDetailNode.length > 0;
  const base = useDetail ? state.data.stationsDetailNode : state.data.stationsMonthNode;

  let rows = base || [];
  rows = applyCommonFilters(rows, "mese");
  if (useDetail) rows = applyDetailDimFilter(rows);

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
      agg.set(cityKey, { cityKey, nome:prettyCityName(cityKey,city), corse_osservate:0, in_ritardo:0, minuti_ritardo_tot:0, soppresse:0, cancellate_tot:0, lat_weighted_sum:0, lon_weighted_sum:0, weight_sum:0 });
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

  const pts = Array.from(agg.values()).map((o) => {
    const w = o.weight_sum > 0 ? o.weight_sum : 1;
    return { ...o, coords:{ lat: o.lat_weighted_sum/w, lon: o.lon_weighted_sum/w }, v: mapMetricValue(o) };
  }).filter((o) => Number.isFinite(o.coords.lat) && Number.isFinite(o.coords.lon));

  pts.sort((a, b) => toNum(b.v) - toNum(a.v));
  const top = pts.slice(0, 250);

  const values = top.map((p) => Math.max(0, Number(p.v) || 0));
  const maxValue = values.length ? Math.max(...values) : 0;
  const minRadius = 5, maxRadius = 22;

  const bounds = [];
  for (const p of top) {
    const val = Math.max(0, Number(p.v) || 0);
    const ratio = maxValue > 0 ? Math.sqrt(val / maxValue) : 0;
    const radius = minRadius + ratio * (maxRadius - minRadius);
    const label = p.nome + "<br>" + metricLabel() + ": " + fmtFloat(val);

    const m = L.circleMarker([p.coords.lat, p.coords.lon], { radius, opacity:0.9, fillOpacity:0.6 }).addTo(state.map);
    try { m.bindPopup(label); } catch {}
    state.markers.push(m);
    bounds.push([p.coords.lat, p.coords.lon]);
  }

  if (bounds.length > 3) { try { state.map.fitBounds(bounds, { padding:[20,20] }); } catch {} }
  setTimeout(() => { try { state.map.invalidateSize(); } catch {} }, 100);
}

/* ────────────────── collapsible cards ────────────────── */

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
        else if (id === "map") { initMap(); renderMap(); setTimeout(function(){ try{state.map.invalidateSize();}catch{} },200); }
        else if (id === "chartStationsTop10") renderStationsTop10();
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
  renderStationsTop10();
  renderMap();
}

/* ────────────────── data loading ────────────────── */

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

  const files = state.manifest && Array.isArray(state.manifest.gold_files) && state.manifest.gold_files.length
    ? state.manifest.gold_files : safeManifestDefaults().gold_files;

  const wanted = uniq([
    ...files,
    "kpi_mese.csv",
    "kpi_mese_categoria.csv",
    "kpi_dettaglio.csv",
    "kpi_dettaglio_categoria.csv",
    "hist_mese_categoria.csv",
    "hist_dettaglio_categoria.csv",
    "stazioni_mese_categoria_nodo.csv",
    "stazioni_dettaglio_categoria_nodo.csv",
    "od_mese_categoria.csv",
    "od_dettaglio_categoria.csv",
    "hist_stazioni_mese_categoria_ruolo.csv",
    "hist_stazioni_dettaglio_categoria_ruolo.csv"
  ]);

  const texts = await Promise.all(wanted.map((f) => fetchTextAny(candidateFilePaths(base, f))));

  const parsed = {};
  for (let i = 0; i < wanted.length; i++) {
    parsed[wanted[i]] = texts[i] ? parseCSV(texts[i]) : [];
  }

  const mobile = isMobile();
  state.data.kpiMonth              = parsed["kpi_mese.csv"] || [];
  state.data.kpiMonthCat           = parsed["kpi_mese_categoria.csv"] || [];
  state.data.kpiDetail             = mobile ? [] : (parsed["kpi_dettaglio.csv"] || []);
  state.data.kpiDetailCat          = mobile ? [] : (parsed["kpi_dettaglio_categoria.csv"] || []);
  state.data.histMonthCat          = parsed["hist_mese_categoria.csv"] || [];
  state.data.histDetailCat         = mobile ? [] : (parsed["hist_dettaglio_categoria.csv"] || []);
  state.data.stationsMonthNode     = parsed["stazioni_mese_categoria_nodo.csv"] || [];
  state.data.stationsDetailNode    = mobile ? [] : (parsed["stazioni_dettaglio_categoria_nodo.csv"] || []);
  state.data.odMonthCat            = parsed["od_mese_categoria.csv"] || [];
  state.data.odDetailCat           = mobile ? [] : (parsed["od_dettaglio_categoria.csv"] || []);
  state.data.histStationsMonthRuolo  = parsed["hist_stazioni_mese_categoria_ruolo.csv"] || [];
  state.data.histStationsDetailRuolo = mobile ? [] : (parsed["hist_stazioni_dettaglio_categoria_ruolo.csv"] || []);

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
    state.stationsRef.set(code, { code, name, lat: Number.isFinite(lat)?lat:NaN, lon: Number.isFinite(lon)?lon:NaN, city });
  }

  const capRows = await loadCapoluoghiAnyBase(base);
  state.capoluoghiSet = new Set(
    capRows.map((r) => normalizeText(r.citta || r.capoluogo || r.nome || r.city || "")).filter(Boolean)
  );

  // Enable tap-to-show tooltips on mobile
  document.querySelectorAll(".info-tip[data-tooltip]").forEach(function(tip) {
    tip.addEventListener("click", function(e) {
      e.stopPropagation();
      // Toggle a "tapped" class to show tooltip on mobile
      const isActive = tip.classList.contains("tip-active");
      document.querySelectorAll(".info-tip.tip-active").forEach(function(t) { t.classList.remove("tip-active"); });
      if (!isActive) tip.classList.add("tip-active");
    });
  });
  document.addEventListener("click", function() {
    document.querySelectorAll(".info-tip.tip-active").forEach(function(t) { t.classList.remove("tip-active"); });
  });

  initFilters();
  initToggleControls();

  const mapCardCollapsed = (function() {
    const mapEl = document.getElementById("map");
    const card = mapEl && mapEl.closest && mapEl.closest(".card");
    return card && card.classList.contains("card--collapsed");
  }());
  if (!mapCardCollapsed) initMap();

  ensureHistToggle();
  initCollapsibleCards();
  initStationsMetricSel();

  // On mobile, collapse all cards by default to reduce initial rendering cost
  if (isMobile()) {
    document.querySelectorAll(".card.collapsible").forEach(function(card) {
      if (!card.classList.contains("card--collapsed")) {
        card.classList.add("card--collapsed");
        const toggle = card.querySelector(".card-toggle");
        if (toggle) toggle.textContent = "\u25B6";
      }
    });
  }

  renderAll();

  const haveAny =
    (state.data.kpiMonthCat && state.data.kpiMonthCat.length) ||
    (state.data.kpiDetailCat && state.data.kpiDetailCat.length) ||
    (state.data.histMonthCat && state.data.histMonthCat.length);

  const coordCount = Array.from(state.stationsRef.values()).filter((s) => Number.isFinite(s.lat) && Number.isFinite(s.lon)).length;

  const metaExtra =
    " | mese cat: " + (state.data.kpiMonthCat ? state.data.kpiMonthCat.length : 0) +
    " | dettaglio cat: " + (state.data.kpiDetailCat ? state.data.kpiDetailCat.length : 0) +
    " | stazioni dim: " + stRows.length +
    " | stazioni coord: " + coordCount +
    (stationDimBuilt ? " | stations_dim build: " + stationDimBuilt : "");

  if (!haveAny) setMeta("Errore: non trovo CSV validi. Base: " + base + metaExtra);
  else setMeta((built ? "Build: " + built : "Build: sconosciuta") + " | base: " + base + metaExtra);
}

loadAll().catch((err) => {
  console.error(err);
  setMeta("Errore caricamento dati: " + (err && err.message ? err.message : String(err)));
});
