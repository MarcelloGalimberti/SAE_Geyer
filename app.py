"""
Monitor Ordini TH. Geyer
Applicazione Streamlit per il tracciamento delle consegne dal fornitore TH. Geyer.

File attesi ogni giovedì · Spedizioni schedulate ogni mercoledì
"""

import streamlit as st
import pandas as pd
import sqlite3
import base64
import re
import io
from datetime import datetime, date, timedelta
from pathlib import Path
import plotly.express as px
import plotly.graph_objects as go

# ══════════════════════════════════════════════════════════════════
# CONFIGURAZIONE
# ══════════════════════════════════════════════════════════════════

APP_DIR = Path(__file__).parent
DB_PATH = APP_DIR / "data" / "geyer.db"
ASSETS  = APP_DIR / "assets"

# Nomi interni delle 18 colonne dati del file Excel Geyer
DB_COLS = [
    "order_conf",     # 1  Order Confirmation
    "pos",            # 2  Pos.
    "your_order",     # 3  Your Order
    "ident_no",       # 4  IdentNo
    "item_no",        # 5  ItemNo (codice articolo)
    "item_no_mfr",    # 6  ItemNo manufacturer
    "hs_code",        # 7  HS Code
    "manufacturer",   # 8  Manufacturer
    "description",    # 9  Description
    "ordered_qty",    # 10 Ordered Quantity
    "directed_qty",   # 11 Directed Quantity
    "unit_price",     # 12 Unit price (EUR)
    "discount_pct",   # 13 % (sconto)
    "value_line",     # 14 Value (totale riga)
    "delivery_date",  # 15 Delivery Date (to Renningen)
    "stock_geyer",    # 16 STOCK TH. GEYER
    "value_in_stock", # 17 Value in Stock
    "information",    # 18 Information / requested delivery date
]

# Configurazione stati
# SCHED  = information contiene una data (requested delivery date)
# HAS_DATE = delivery_date confermata, nessuna indicazione speciale
STATUS_CFG = {
    "READY":    {"label": "Pronto",              "color": "#1e8449", "bg": "#d5f5e3", "emoji": "✅", "priority": 0},
    "EXP":      {"label": "Atteso breve",        "color": "#148f77", "bg": "#d1f2eb", "emoji": "🔜", "priority": 1},
    "HAS_DATE": {"label": "Data conf.",          "color": "#2980b9", "bg": "#d6eaf8", "emoji": "📅", "priority": 2},
    "SCHED":    {"label": "Data richiesta",      "color": "#5d6d7e", "bg": "#eaecee", "emoji": "🗓️", "priority": 3},
    "NOD":      {"label": "Nessuna data",        "color": "#b7770d", "bg": "#fef9e7", "emoji": "🟡", "priority": 4},
    "IC":       {"label": "In chiarimento",      "color": "#a04000", "bg": "#fdebd0", "emoji": "🔔", "priority": 5},
    "EC":       {"label": "Export control",      "color": "#6c3483", "bg": "#f4ecf7", "emoji": "🔒", "priority": 6},
    "ECD":      {"label": "End cust. decl.",     "color": "#1a5276", "bg": "#d6eaf8", "emoji": "📋", "priority": 7},
    "NO_INFO":  {"label": "Nessuna info",        "color": "#717d7e", "bg": "#f2f3f4", "emoji": "❓", "priority": 8},
}

COL_LABELS = {
    "order_conf":     "N° Conferma",
    "pos":            "Pos.",
    "your_order":     "N° Ordine",
    "ident_no":       "IdentNo",
    "item_no":        "Codice Articolo",
    "item_no_mfr":    "Cod. Produttore",
    "hs_code":        "HS Code",
    "manufacturer":   "Produttore",
    "description":    "Descrizione",
    "ordered_qty":    "Qtà Ord.",
    "directed_qty":   "Qtà Dir.",
    "unit_price":     "Prezzo Unit. (€)",
    "discount_pct":   "Sconto %",
    "value_line":     "Valore (€)",
    "delivery_date":  "Data Consegna",
    "stock_geyer":    "Stock Geyer",
    "value_in_stock": "Val. Stock (€)",
    "information":    "Informazioni",
    "status_class":   "Stato",
}

# Legenda codici Information
INFO_LEGEND = {
    "IC":   "In Clarification — in fase di chiarimento",
    "EC":   "Export Control — soggetto a controllo export",
    "ECD":  "End Customer Declaration — richiesta dichiarazione cliente finale",
    "EXP":  "Expected this or next week — atteso questa o prossima settimana",
    "NOD":  "No Date Confirmed Yet — nessuna data confermata",
    "data": "Data richiesta dal cliente (campo 'Information/requested delivery date')",
}

# ══════════════════════════════════════════════════════════════════
# DATABASE
# ══════════════════════════════════════════════════════════════════

def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as c:
        c.executescript("""
            CREATE TABLE IF NOT EXISTS snapshots (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                file_date TEXT UNIQUE,
                upload_ts TEXT,
                filename  TEXT,
                row_count INTEGER
            );
            CREATE TABLE IF NOT EXISTS positions (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_id    INTEGER,
                order_conf     TEXT,
                pos            TEXT,
                your_order     TEXT,
                ident_no       TEXT,
                item_no        TEXT,
                item_no_mfr    TEXT,
                hs_code        TEXT,
                manufacturer   TEXT,
                description    TEXT,
                ordered_qty    REAL,
                directed_qty   REAL,
                unit_price     REAL,
                discount_pct   REAL,
                value_line     REAL,
                delivery_date  TEXT,
                stock_geyer    REAL,
                value_in_stock REAL,
                information    TEXT,
                status_class   TEXT,
                FOREIGN KEY (snapshot_id) REFERENCES snapshots(id)
            );
            CREATE INDEX IF NOT EXISTS idx_pos_snap ON positions(snapshot_id);
            CREATE INDEX IF NOT EXISTS idx_pos_key  ON positions(order_conf, pos);
        """)


def _conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def get_snapshots() -> pd.DataFrame:
    with _conn() as c:
        return pd.read_sql("SELECT * FROM snapshots ORDER BY file_date DESC", c)


def get_positions_for_date(file_date: str) -> pd.DataFrame:
    with _conn() as c:
        return pd.read_sql(
            "SELECT p.* FROM positions p "
            "JOIN snapshots s ON p.snapshot_id = s.id "
            "WHERE s.file_date = ?",
            c, params=(file_date,)
        )


def save_snapshot(df: pd.DataFrame, file_date: str, filename: str, overwrite: bool = False):
    with _conn() as c:
        exists = c.execute(
            "SELECT id FROM snapshots WHERE file_date=?", (file_date,)
        ).fetchone()
        if exists and not overwrite:
            return False, "Esiste già un file caricato per questa data."
        if exists and overwrite:
            c.execute("DELETE FROM positions WHERE snapshot_id=?", (exists[0],))
            c.execute("DELETE FROM snapshots WHERE id=?", (exists[0],))
        cur = c.execute(
            "INSERT INTO snapshots (file_date, upload_ts, filename, row_count) VALUES (?,?,?,?)",
            (file_date, datetime.now().isoformat(), filename, len(df))
        )
        snap_id = cur.lastrowid
        df2 = df.copy()
        df2["snapshot_id"] = snap_id
        cols = ["snapshot_id"] + DB_COLS + ["status_class"]
        for col in cols:
            if col not in df2.columns:
                df2[col] = None
        df2[cols].to_sql("positions", c, if_exists="append", index=False)
    return True, snap_id


def delete_snapshot(file_date: str):
    with _conn() as c:
        row = c.execute(
            "SELECT id FROM snapshots WHERE file_date=?", (file_date,)
        ).fetchone()
        if row:
            c.execute("DELETE FROM positions WHERE snapshot_id=?", (row[0],))
            c.execute("DELETE FROM snapshots WHERE id=?", (row[0],))


# ══════════════════════════════════════════════════════════════════
# EXCEL PARSER
# ══════════════════════════════════════════════════════════════════

def parse_excel(content: bytes, filename: str, override_date: str = None):
    """
    Parsa il file Excel TH. Geyer.
    Ritorna (df, file_date, warnings) oppure (None, None, [errori]).
    file_date è in formato YYYYMMDD.
    """
    warnings = []

    # Estrai data dal nome file: OOL_XXXXXX_YYYY-MM-DD-AFER.xlsx
    if override_date:
        file_date = override_date
    else:
        m = re.search(r"(\d{4}-\d{2}-\d{2})", filename)
        if m:
            file_date = m.group(1).replace("-", "")  # → YYYYMMDD
        else:
            file_date = None
            warnings.append("⚠️ Data non trovata nel nome file: selezionala manualmente.")

    try:
        raw = pd.read_excel(
            io.BytesIO(content),
            header=8,       # header alla riga 9 (0-indexed: 8)
            sheet_name=0,
            engine="openpyxl",
        )
    except Exception as e:
        return None, None, [f"❌ Errore nella lettura del file Excel: {e}"]

    if raw.shape[1] < 18:
        return None, None, [f"❌ Il file ha solo {raw.shape[1]} colonne, ne servono almeno 18."]

    # Prendi solo le prime 18 colonne e assegna nomi interni
    raw = raw.iloc[:, :18].copy()
    raw.columns = DB_COLS

    # Rimuovi righe senza order_conf (righe vuote o di riepilogo)
    raw = raw[
        raw["order_conf"].notna() &
        (raw["order_conf"].astype(str).str.strip() != "") &
        (raw["order_conf"].astype(str).str.strip() != "nan")
    ].copy()

    if raw.empty:
        return None, None, ["❌ Nessuna riga valida trovata nel file."]

    # Normalizza colonne numeriche
    num_cols = ["ordered_qty", "directed_qty", "unit_price", "discount_pct",
                "value_line", "stock_geyer", "value_in_stock"]
    for col in num_cols:
        raw[col] = pd.to_numeric(raw[col], errors="coerce")

    # Normalizza colonne stringa semplici (escluso information e delivery_date)
    str_cols = ["order_conf", "pos", "your_order", "ident_no", "item_no",
                "item_no_mfr", "hs_code", "manufacturer", "description"]
    for col in str_cols:
        raw[col] = raw[col].astype(str).str.strip()
        raw[col] = raw[col].replace({"nan": "", "None": "", "NaT": ""})

    # Funzione per convertire un valore a stringa ISO data (YYYY-MM-DD) o ""
    def _to_date_str(v) -> str:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return ""
        try:
            if pd.isna(v):
                return ""
        except Exception:
            pass
        if isinstance(v, (datetime, date)):
            return v.strftime("%Y-%m-%d")
        try:
            return pd.to_datetime(v).strftime("%Y-%m-%d")
        except Exception:
            return ""

    # delivery_date → stringa ISO o ""
    raw["delivery_date"] = raw["delivery_date"].apply(_to_date_str)

    # information: può essere NaN, datetime, o testo (IC/EC/EXP/NOD/ECD)
    # → si normalizza: datetime → stringa ISO (viene salvata come "SCHED:YYYY-MM-DD")
    #                  testo    → stringa maiuscola
    #                  NaN/vuoto → ""
    def _parse_information(v) -> str:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return ""
        try:
            if pd.isna(v):
                return ""
        except Exception:
            pass
        if isinstance(v, (datetime, date)):
            return "SCHED:" + v.strftime("%Y-%m-%d")
        s = str(v).strip()
        if not s or s.lower() in ("nan", "none", "nat"):
            return ""
        # Tenta parsing come data
        try:
            dt = pd.to_datetime(s)
            return "SCHED:" + dt.strftime("%Y-%m-%d")
        except Exception:
            return s.upper()

    raw["information"] = raw["information"].apply(_parse_information)

    # Classifica stato
    raw["status_class"] = raw.apply(_classify_row, axis=1)

    return raw, file_date, warnings


def _classify_row(row) -> str:
    """
    Logica di classificazione:
      READY    : stock_geyer > 0  AND  information vuota → pronto per spedire
      EXP      : information testo che inizia con "EXP"
      HAS_DATE : delivery_date presente AND information vuota (data confermata, no stock)
      SCHED    : information contiene "SCHED:YYYY-MM-DD" (requested delivery date)
      NOD      : information testo "NOD"
      IC       : information testo "IC"
      EC       : information testo "EC" (non ECD)
      ECD      : information testo "ECD"
      NO_INFO  : tutto il resto
    """
    info     = str(row.get("information", "") or "").strip()
    info_up  = info.upper()
    stock    = row.get("stock_geyer",   None)
    del_date = row.get("delivery_date", "")

    # Testo (non date): ECD prima di EC per evitare match parziale
    if info_up.startswith("EXP"):  return "EXP"
    if info_up.startswith("ECD"):  return "ECD"
    if info_up.startswith("EC"):   return "EC"
    if info_up.startswith("IC"):   return "IC"
    if info_up.startswith("NOD"):  return "NOD"

    # SCHED: information contiene data richiesta
    if info_up.startswith("SCHED:"):
        return "SCHED"

    # A questo punto information è vuota
    # READY: stock disponibile, nessuna indicazione speciale
    try:
        if stock and float(stock) > 0:
            return "READY"
    except (ValueError, TypeError):
        pass

    # HAS_DATE: data di consegna confermata ma nessuno stock
    if del_date:
        return "HAS_DATE"

    return "NO_INFO"


# ══════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════

def next_wednesday(d: date) -> date:
    """Mercoledì successivo a d (mai lo stesso giorno se è già mercoledì)."""
    days_ahead = (2 - d.weekday()) % 7
    if days_ahead == 0:
        days_ahead = 7
    return d + timedelta(days=days_ahead)


def parse_file_date(file_date: str) -> date:
    return date(int(file_date[:4]), int(file_date[4:6]), int(file_date[6:8]))


def fmt_date_str(file_date: str) -> str:
    """YYYYMMDD → DD/MM/YYYY."""
    if len(file_date) == 8:
        return f"{file_date[6:8]}/{file_date[4:6]}/{file_date[:4]}"
    return file_date


def fmt_delivery(date_str: str) -> str:
    """YYYY-MM-DD → DD/MM/YYYY, vuoto → '—'."""
    if not date_str:
        return "—"
    try:
        return pd.to_datetime(date_str).strftime("%d/%m/%Y")
    except Exception:
        return date_str


def fmt_information(info_str: str) -> str:
    """Formatta il campo information per la visualizzazione.
    'SCHED:YYYY-MM-DD' → '🗓️ DD/MM/YYYY (richiesta)'
    'IC' → 'IC', etc.
    """
    if not info_str:
        return ""
    if info_str.upper().startswith("SCHED:"):
        date_part = info_str[6:]
        try:
            return "🗓️ " + pd.to_datetime(date_part).strftime("%d/%m/%Y") + " (rich.)"
        except Exception:
            return info_str
    return info_str


def sched_date(info_str: str) -> str:
    """Estrae la data ISO da 'SCHED:YYYY-MM-DD', altrimenti ''."""
    if info_str and info_str.upper().startswith("SCHED:"):
        return info_str[6:]
    return ""


def stato_label(sc: str) -> str:
    cfg = STATUS_CFG.get(sc, {})
    return f"{cfg.get('emoji', '')} {cfg.get('label', sc)}"


# ══════════════════════════════════════════════════════════════════
# COMPONENTI UI
# ══════════════════════════════════════════════════════════════════

def page_title(title: str):
    p_adi = ASSETS / "logo_adi.png"
    st.write("")
    col_l, col_mid, col_r = st.columns([1, 5, 1], vertical_alignment="center")
    with col_l:
        st.markdown(
            "<span style='font-size:28px;font-weight:700;color:#1a5276;'>TH. Geyer</span>",
            unsafe_allow_html=True
        )
    with col_mid:
        st.header(title)
    with col_r:
        if p_adi.exists():
            st.image(str(p_adi), width=89)
    st.divider()


def status_cards(df: pd.DataFrame, exclude_complete: bool = True):
    items = [(k, v) for k, v in STATUS_CFG.items()
             if not (exclude_complete and k == "COMPLETE")]
    cols = st.columns(len(items))
    for i, (sc, cfg) in enumerate(items):
        count = int((df["status_class"] == sc).sum())
        with cols[i]:
            st.markdown(f"""
            <div style="background:{cfg['bg']};border-left:4px solid {cfg['color']};
                        padding:12px 8px;border-radius:8px;text-align:center;">
                <div style="font-size:22px;">{cfg['emoji']}</div>
                <div style="font-size:26px;font-weight:700;color:{cfg['color']};">{count}</div>
                <div style="font-size:11px;color:#555;margin-top:2px;">{cfg['label']}</div>
            </div>
            """, unsafe_allow_html=True)


def snap_selector(label: str = "📅 Snapshot", key: str = "snap_sel") -> tuple[str | None, pd.DataFrame | None]:
    """Selettore snapshot universale. Ritorna (file_date, df) o (None, None)."""
    snaps = get_snapshots()
    if snaps.empty:
        st.info("Nessun dato. Carica un file Excel dalla sezione '📤 Carica Excel'.")
        return None, None
    opts = {fmt_date_str(r["file_date"]): r["file_date"] for _, r in snaps.iterrows()}
    sel  = st.selectbox(label, list(opts.keys()), index=0, key=key)
    return opts[sel], get_positions_for_date(opts[sel])


# ══════════════════════════════════════════════════════════════════
# PAGINA 1 — CARICA EXCEL
# ══════════════════════════════════════════════════════════════════

def page_upload():
    page_title("📤 Carica Excel")

    tab_daily, tab_bulk = st.tabs(["📅 File del giovedì", "📦 Importazione storica / multipla"])

    with tab_daily:
        _upload_single()

    with tab_bulk:
        _upload_bulk()

    # Storico caricamenti
    snaps = get_snapshots()
    if not snaps.empty:
        st.markdown("---")
        st.subheader("📋 Storico caricamenti")
        disp = snaps.copy()
        disp["Data"]        = disp["file_date"].apply(fmt_date_str)
        disp["Caricato il"] = pd.to_datetime(disp["upload_ts"]).dt.strftime("%d/%m/%Y %H:%M")
        disp["File"]        = disp["filename"]
        disp["Righe"]       = disp["row_count"]

        col_tbl, col_del = st.columns([4, 1])
        with col_tbl:
            st.dataframe(disp[["Data", "Righe", "File", "Caricato il"]], hide_index=True)
        with col_del:
            st.markdown("**🗑️ Elimina snapshot**")
            date_opts = disp["Data"].tolist()
            to_del = st.selectbox("Data", date_opts, key="del_snap_sel",
                                  label_visibility="collapsed")
            if st.button("Elimina", type="secondary", key="del_snap_btn"):
                raw_date = snaps[
                    snaps["file_date"].apply(fmt_date_str) == to_del
                ]["file_date"].iloc[0]
                delete_snapshot(raw_date)
                st.success(f"Snapshot {to_del} eliminato.")
                st.rerun()


def _upload_single():
    st.markdown("#### Come fare ogni giovedì")
    st.info(
        "1. Apri l'email ricevuta da TH. Geyer  \n"
        "2. Scarica il file Excel allegato (`OOL_528432_YYYY-MM-DD-AFER.xlsx`)  \n"
        "3. Caricalo qui sotto — la data viene estratta automaticamente dal nome  \n"
        "4. Premi **Salva nel database**"
    )

    uploaded = st.file_uploader(
        "Seleziona il file Excel di TH. Geyer",
        type=["xlsx"],
        key="uploader_single",
        help="Formato atteso: OOL_XXXXXX_YYYY-MM-DD-AFER.xlsx  •  Header alla riga 9",
    )
    if not uploaded:
        return

    content = uploaded.read()
    df, file_date, warns = parse_excel(content, uploaded.name)

    for w in warns:
        st.warning(w)

    if df is None:
        st.error(warns[0] if warns else "Errore nella lettura del file.")
        return

    # Gestione data mancante
    if file_date is None:
        st.warning("**Data non rilevata dal nome file.** Selezionala manualmente:")
        sel = st.date_input("Data del file", value=date.today(), key="manual_date_single")
        file_date = sel.strftime("%Y%m%d")

    fdate_parsed = parse_file_date(file_date)
    next_wed     = next_wednesday(fdate_parsed)

    # Metriche di anteprima
    st.markdown("---")
    ready_df = df[df["status_class"] == "READY"]
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("📅 Data file",         fmt_date_str(file_date))
    c2.metric("📋 Righe totali",       len(df))
    c3.metric("✅ Pronti (READY)",     len(ready_df))
    c4.metric("💰 Valore in stock",   f"€ {ready_df['value_in_stock'].sum():,.0f}")
    c5.metric("🚚 Prossima sped.",     next_wed.strftime("%d/%m/%Y"))

    st.markdown("")
    status_cards(df)

    # Anteprima articoli READY
    if not ready_df.empty:
        with st.expander(f"👁️ Anteprima articoli READY ({len(ready_df)})"):
            st.dataframe(
                ready_df[["order_conf", "pos", "item_no", "description",
                           "ordered_qty", "stock_geyer", "value_in_stock"]].rename(
                    columns={"order_conf": "N° Conf.", "pos": "Pos.", "item_no": "Articolo",
                             "description": "Descrizione", "ordered_qty": "Qtà Ord.",
                             "stock_geyer": "Stock", "value_in_stock": "Val. Stock (€)"}
                ).head(15),
                hide_index=True,
            )

    # Salva
    snaps = get_snapshots()
    already = (not snaps.empty) and (file_date in snaps["file_date"].values)
    if already:
        st.warning(f"⚠️ Esiste già un caricamento per il {fmt_date_str(file_date)}.")
        overwrite = st.checkbox("Sovrascrivi i dati esistenti", key="overwrite_single")
    else:
        overwrite = False

    if st.button("💾 Salva nel database", type="primary", key="save_single"):
        if already and not overwrite:
            st.error("Seleziona 'Sovrascrivi' per procedere.")
        else:
            ok, result = save_snapshot(df, file_date, uploaded.name, overwrite=overwrite)
            if ok:
                st.success(f"✅ Salvato! {len(df)} righe per il {fmt_date_str(file_date)}.")
                st.rerun()
            else:
                st.error(result)


def _upload_bulk():
    st.markdown("#### Importazione multipla (storico)")
    st.info(
        "Carica più file Excel contemporaneamente per ricostruire lo storico.  \n"
        "La data viene estratta automaticamente dal nome di ciascun file."
    )

    files = st.file_uploader(
        "Seleziona i file Excel",
        type=["xlsx"],
        accept_multiple_files=True,
        key="uploader_bulk",
    )
    if not files:
        return

    results = []
    progress = st.progress(0)
    for i, f in enumerate(files):
        content = f.read()
        df, file_date, warns = parse_excel(content, f.name)
        progress.progress((i + 1) / len(files))
        if df is None:
            results.append({"File": f.name, "Stato": "❌ Errore parsing", "Righe": 0})
            continue
        if file_date is None:
            results.append({"File": f.name, "Stato": "⚠️ Data mancante (file saltato)", "Righe": len(df)})
            continue
        ok, msg = save_snapshot(df, file_date, f.name, overwrite=False)
        if ok:
            results.append({
                "File": f.name,
                "Stato": f"✅ Importato ({fmt_date_str(file_date)})",
                "Righe": len(df),
            })
        else:
            results.append({"File": f.name, "Stato": f"⚠️ {msg}", "Righe": len(df)})

    progress.empty()
    if results:
        st.dataframe(pd.DataFrame(results), hide_index=True)


# ══════════════════════════════════════════════════════════════════
# PAGINA 2 — SITUAZIONE ATTUALE
# ══════════════════════════════════════════════════════════════════

def page_current():
    page_title("📊 Situazione Attuale")

    file_date, df = snap_selector("📅 Seleziona snapshot", key="cur_snap")
    if df is None:
        return

    fdate_parsed = parse_file_date(file_date)
    next_wed     = next_wednesday(fdate_parsed)
    open_df      = df[df["status_class"] != "COMPLETE"]
    ready_df     = df[df["status_class"] == "READY"]

    # KPI header
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("📋 Posizioni totali",     len(df))
    c2.metric("📂 Posizioni aperte",     len(open_df))
    c3.metric("✅ Pronte per sped.",     len(ready_df))
    c4.metric("💰 Valore pronto",       f"€ {ready_df['value_in_stock'].sum():,.0f}")
    c5.metric("🚚 Prossima sped.",       next_wed.strftime("%d/%m/%Y"))

    st.markdown("")
    status_cards(open_df)

    st.markdown("---")
    st.subheader("🔍 Filtri")

    col_f1, col_f2, col_f3 = st.columns(3)
    with col_f1:
        stati_disp = ["Tutti"] + list(STATUS_CFG.keys())
        filtro_stato = st.selectbox("Stato", stati_disp, key="cur_stato")
    with col_f2:
        prods = ["Tutti"] + sorted(
            [p for p in df["manufacturer"].dropna().unique() if p]
        )
        filtro_prod = st.selectbox("Produttore", prods, key="cur_prod")
    with col_f3:
        testo = st.text_input("🔎 Cerca (articolo, descrizione, ordine)", key="cur_testo")

    # Applica filtri
    disp = df.copy()
    if filtro_stato != "Tutti":
        disp = disp[disp["status_class"] == filtro_stato]
    if filtro_prod != "Tutti":
        disp = disp[disp["manufacturer"] == filtro_prod]
    if testo:
        mask = (
            disp["item_no"].str.contains(testo, case=False, na=False)
            | disp["description"].str.contains(testo, case=False, na=False)
            | disp["order_conf"].str.contains(testo, case=False, na=False)
            | disp["your_order"].str.contains(testo, case=False, na=False)
        )
        disp = disp[mask]

    disp["Stato"]          = disp["status_class"].map(stato_label)
    disp["Data Consegna"]  = disp["delivery_date"].map(fmt_delivery)
    disp["Info display"]   = disp["information"].map(fmt_information)

    show_cols = ["order_conf", "pos", "your_order", "item_no", "description",
                 "manufacturer", "ordered_qty", "stock_geyer", "value_in_stock",
                 "Data Consegna", "Info display", "Stato"]
    rename_map = {
        "order_conf": "N° Conf.", "pos": "Pos.", "your_order": "N° Ord.",
        "item_no": "Articolo", "description": "Descrizione",
        "manufacturer": "Produttore", "ordered_qty": "Qtà Ord.",
        "stock_geyer": "Stock", "value_in_stock": "Val. Stock (€)",
        "Info display": "Info",
    }
    st.markdown(f"**{len(disp)} posizioni**")
    st.dataframe(
        disp[show_cols].rename(columns=rename_map),
        hide_index=True,
        use_container_width=True,
    )


# ══════════════════════════════════════════════════════════════════
# PAGINA 3 — SPEDIZIONI DEL MERCOLEDÌ
# ══════════════════════════════════════════════════════════════════

def page_shipments():
    page_title("🚚 Spedizioni del Mercoledì")

    snaps = get_snapshots()
    if snaps.empty:
        st.info("Nessun dato disponibile.")
        return

    file_date, df = snap_selector("📅 Snapshot di riferimento", key="ship_snap")
    if df is None:
        return

    fdate_parsed = parse_file_date(file_date)
    next_wed     = next_wednesday(fdate_parsed)
    ready        = df[df["status_class"] == "READY"].copy()

    st.markdown(f"### 🚚 Spedizione del **{next_wed.strftime('%A %d/%m/%Y')}**")

    if ready.empty:
        st.warning("⚠️ Nessun articolo pronto per la spedizione (stato READY).")
    else:
        c1, c2, c3 = st.columns(3)
        c1.metric("✅ Articoli pronti",   len(ready))
        c2.metric("💰 Valore totale",    f"€ {ready['value_in_stock'].sum():,.0f}")
        c3.metric("📦 Qtà totale stock", f"{ready['stock_geyer'].sum():,.0f}")

        st.dataframe(
            ready[["order_conf", "pos", "your_order", "item_no", "description",
                   "manufacturer", "ordered_qty", "stock_geyer", "value_in_stock"]].rename(
                columns={
                    "order_conf": "N° Conf.", "pos": "Pos.", "your_order": "N° Ord.",
                    "item_no": "Articolo", "description": "Descrizione",
                    "manufacturer": "Produttore", "ordered_qty": "Qtà Ord.",
                    "stock_geyer": "Stock", "value_in_stock": "Val. Stock (€)",
                }
            ),
            hide_index=True,
            use_container_width=True,
        )

        # Download lista spedizione
        sped_buffer = io.BytesIO()
        with pd.ExcelWriter(sped_buffer, engine="openpyxl") as writer:
            ready.rename(columns={
                "order_conf": "N_CONFERMA", "pos": "POSIZIONE",
                "your_order": "N_ORDINE", "item_no": "CODICE_ARTICOLO",
                "description": "DESCRIZIONE", "manufacturer": "PRODUTTORE",
                "ordered_qty": "QTA_ORDINATA", "stock_geyer": "STOCK",
                "value_in_stock": "VALORE_STOCK_EUR",
            }).to_excel(writer, index=False, sheet_name="Spedizione")
        st.download_button(
            "⬇️ Scarica lista spedizione (Excel)",
            data=sped_buffer.getvalue(),
            file_name=f"spedizione_{next_wed.strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    # Articoli già READY la settimana scorsa (in coda)
    st.markdown("---")
    st.subheader("⏰ Articoli READY già la settimana scorsa (in coda)")
    if len(snaps) >= 2:
        prev_date  = snaps.iloc[1]["file_date"] if snaps.iloc[0]["file_date"] == file_date else snaps.iloc[0]["file_date"]
        # Trova il secondo snapshot più recente rispetto a file_date
        dates_sorted = sorted(snaps["file_date"].tolist(), reverse=True)
        idx = dates_sorted.index(file_date) if file_date in dates_sorted else -1
        if idx >= 0 and idx + 1 < len(dates_sorted):
            prev_date  = dates_sorted[idx + 1]
            df_prev    = get_positions_for_date(prev_date)
            ready_prev = df_prev[df_prev["status_class"] == "READY"]

            ready_keys      = set(zip(ready["order_conf"], ready["pos"]))
            ready_prev_keys = set(zip(ready_prev["order_conf"], ready_prev["pos"]))
            queue_keys      = ready_keys & ready_prev_keys

            if queue_keys:
                in_queue = ready[ready.apply(
                    lambda r: (r["order_conf"], r["pos"]) in queue_keys, axis=1
                )]
                st.warning(
                    f"⚠️ **{len(in_queue)} articoli** erano già READY il "
                    f"{fmt_date_str(prev_date)} e risultano ancora in stock."
                )
                st.dataframe(
                    in_queue[["order_conf", "pos", "item_no", "description",
                               "stock_geyer", "value_in_stock"]].rename(
                        columns={
                            "order_conf": "N° Conf.", "pos": "Pos.",
                            "item_no": "Articolo", "description": "Descrizione",
                            "stock_geyer": "Stock", "value_in_stock": "Val. Stock (€)",
                        }
                    ),
                    hide_index=True,
                )
            else:
                st.success("✅ Tutti gli articoli READY sono nuovi rispetto alla settimana scorsa.")
        else:
            st.info("Snapshot precedente non trovato.")
    else:
        st.info("Serve almeno un secondo snapshot per il confronto.")

    # Storico READY per snapshot
    st.markdown("---")
    st.subheader("📅 Storico articoli READY per snapshot")
    hist_data = []
    for _, snap in snaps.iterrows():
        d  = get_positions_for_date(snap["file_date"])
        rd = d[d["status_class"] == "READY"]
        hist_data.append({
            "Data":           fmt_date_str(snap["file_date"]),
            "Articoli READY": len(rd),
            "Valore (€)":     round(rd["value_in_stock"].sum(), 2),
            "Stock totale":   rd["stock_geyer"].sum(),
        })
    if hist_data:
        hist_df = pd.DataFrame(hist_data)
        fig = px.bar(
            hist_df, x="Data", y="Articoli READY",
            title="Articoli READY per settimana",
            color_discrete_sequence=["#1e8449"],
        )
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(hist_df, hide_index=True)


# ══════════════════════════════════════════════════════════════════
# PAGINA 4 — NOVITÀ DELLA SETTIMANA
# ══════════════════════════════════════════════════════════════════

def page_news():
    page_title("🔔 Novità della Settimana")

    snaps = get_snapshots()
    if len(snaps) < 2:
        st.info("Servono almeno 2 snapshot per il confronto.")
        return

    snap_opts = {fmt_date_str(r["file_date"]): r["file_date"] for _, r in snaps.iterrows()}
    labels    = list(snap_opts.keys())

    col_a, col_b = st.columns(2)
    with col_a:
        sel_new = st.selectbox("📅 Snapshot più recente", labels,
                               index=0, key="news_new")
    with col_b:
        sel_old = st.selectbox("📅 Snapshot precedente", labels,
                               index=min(1, len(labels) - 1), key="news_old")

    if snap_opts[sel_new] == snap_opts[sel_old]:
        st.warning("Seleziona due snapshot diversi.")
        return

    df_new = get_positions_for_date(snap_opts[sel_new])
    df_old = get_positions_for_date(snap_opts[sel_old])

    def keyed(df):
        return df.set_index(["order_conf", "pos"])

    new_keyed = keyed(df_new)
    old_keyed = keyed(df_old)
    new_keys  = set(new_keyed.index)
    old_keys  = set(old_keyed.index)

    # 1. Nuove posizioni
    entered_keys = new_keys - old_keys
    entered = df_new[df_new.apply(
        lambda r: (r["order_conf"], r["pos"]) in entered_keys, axis=1
    )]

    # 2. Posizioni uscite
    exited_keys = old_keys - new_keys
    exited = df_old[df_old.apply(
        lambda r: (r["order_conf"], r["pos"]) in exited_keys, axis=1
    )]

    # 3. Cambi di stato, diventati READY, ritardi data
    common_keys    = new_keys & old_keys
    status_changes = []
    became_ready   = []
    delays         = []

    for key in common_keys:
        rn = new_keyed.loc[key]
        ro = old_keyed.loc[key]
        # Gestione duplicati (multi-index)
        if isinstance(rn, pd.DataFrame):
            rn = rn.iloc[0]
        if isinstance(ro, pd.DataFrame):
            ro = ro.iloc[0]

        sc_new = rn["status_class"]
        sc_old = ro["status_class"]
        dd_new = rn["delivery_date"]
        dd_old = ro["delivery_date"]
        item   = rn["item_no"]
        desc   = rn["description"]

        if sc_new != sc_old:
            status_changes.append({
                "N° Conf.":   key[0], "Pos.": key[1],
                "Articolo":   item,   "Descrizione": desc,
                "Da":         stato_label(sc_old),
                "A":          stato_label(sc_new),
            })
            if sc_new == "READY" and sc_old != "READY":
                became_ready.append(key)

        if dd_new and dd_old and dd_new > dd_old:
            try:
                slitt = (pd.to_datetime(dd_new) - pd.to_datetime(dd_old)).days
                delays.append({
                    "N° Conf.":        key[0], "Pos.": key[1],
                    "Articolo":        item,   "Descrizione": desc,
                    "Data precedente": fmt_delivery(dd_old),
                    "Nuova data":      fmt_delivery(dd_new),
                    "Slittamento (gg)": slitt,
                })
            except Exception:
                pass

    # Metriche riepilogative
    st.markdown(f"**Confronto: {sel_old} → {sel_new}**")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("🆕 Nuovi",             len(entered))
    c2.metric("📦 Usciti",            len(exited))
    c3.metric("✅ Diventati READY",   len(became_ready))
    c4.metric("🔄 Cambi stato",       len(status_changes))
    c5.metric("⚠️ Ritardi data",      len(delays))

    st.markdown("---")

    if delays:
        with st.expander(f"⚠️ Ritardi data consegna ({len(delays)})", expanded=True):
            st.dataframe(pd.DataFrame(delays), hide_index=True, use_container_width=True)

    if became_ready:
        br_df = df_new[df_new.apply(
            lambda r: (r["order_conf"], r["pos"]) in became_ready, axis=1
        )]
        with st.expander(f"✅ Diventati READY ({len(br_df)})", expanded=True):
            st.dataframe(
                br_df[["order_conf", "pos", "item_no", "description",
                        "stock_geyer", "value_in_stock"]].rename(
                    columns={
                        "order_conf": "N° Conf.", "pos": "Pos.",
                        "item_no": "Articolo", "description": "Descrizione",
                        "stock_geyer": "Stock", "value_in_stock": "Val. Stock (€)",
                    }
                ),
                hide_index=True,
            )

    if not entered.empty:
        with st.expander(f"🆕 Nuove posizioni entrate ({len(entered)})", expanded=False):
            st.dataframe(
                entered[["order_conf", "pos", "item_no", "description",
                          "ordered_qty", "delivery_date", "information",
                          "status_class"]].assign(
                    delivery_date=entered["delivery_date"].map(fmt_delivery),
                    status_class=entered["status_class"].map(stato_label),
                ).rename(
                    columns={
                        "order_conf": "N° Conf.", "pos": "Pos.",
                        "item_no": "Articolo", "description": "Descrizione",
                        "ordered_qty": "Qtà", "delivery_date": "Data Cons.",
                        "information": "Info", "status_class": "Stato",
                    }
                ),
                hide_index=True,
            )

    if not exited.empty:
        with st.expander(f"📦 Posizioni uscite / completate ({len(exited)})", expanded=False):
            st.dataframe(
                exited[["order_conf", "pos", "item_no", "description",
                         "ordered_qty", "status_class"]].assign(
                    status_class=exited["status_class"].map(stato_label)
                ).rename(
                    columns={
                        "order_conf": "N° Conf.", "pos": "Pos.",
                        "item_no": "Articolo", "description": "Descrizione",
                        "ordered_qty": "Qtà", "status_class": "Stato",
                    }
                ),
                hide_index=True,
            )

    if status_changes:
        with st.expander(f"🔄 Tutti i cambi di stato ({len(status_changes)})", expanded=False):
            st.dataframe(pd.DataFrame(status_changes), hide_index=True, use_container_width=True)


# ══════════════════════════════════════════════════════════════════
# PAGINA 5 — CALENDARIO CONSEGNE
# ══════════════════════════════════════════════════════════════════

def page_calendar():
    page_title("📅 Calendario Consegne")

    file_date, df = snap_selector("📅 Snapshot", key="cal_snap")
    if df is None:
        return

    all_open = df.copy()

    # HAS_DATE: data confermata nel campo delivery_date
    has_conf   = all_open[all_open["delivery_date"] != ""].copy()
    # SCHED: data richiesta nel campo information
    has_sched  = all_open[all_open["status_class"] == "SCHED"].copy()
    has_sched["sched_dt"] = has_sched["information"].map(sched_date)
    has_sched = has_sched[has_sched["sched_dt"] != ""].copy()
    # Nessuna data (né confermata né richiesta)
    no_date    = all_open[
        (all_open["delivery_date"] == "") & (all_open["status_class"] != "SCHED")
    ].copy()

    st.markdown(
        f"**{len(all_open)} posizioni totali** · "
        f"📅 {len(has_conf)} data confermata · "
        f"🗓️ {len(has_sched)} data richiesta · "
        f"❓ {len(no_date)} senza data"
    )

    tab_conf, tab_sched, tab_nodate = st.tabs([
        f"📅 Data confermata ({len(has_conf)})",
        f"🗓️ Data richiesta ({len(has_sched)})",
        f"❓ Senza data ({len(no_date)})",
    ])

    def _weekly_chart_and_detail(df_sub: pd.DataFrame, date_col: str, title_sfx: str, color: str):
        df_sub = df_sub.copy()
        df_sub["dt"]    = pd.to_datetime(df_sub[date_col])
        df_sub["week"]  = df_sub["dt"].dt.isocalendar().week.astype(int)
        df_sub["year"]  = df_sub["dt"].dt.year
        df_sub["wlbl"]  = df_sub.apply(lambda r: f"KW{r['week']:02d}/{r['year']}", axis=1)

        weekly = df_sub.groupby("wlbl").agg(
            Articoli=("item_no", "count"),
            Valore=("value_line", "sum"),
        ).reset_index().sort_values("wlbl")

        col_g1, col_g2 = st.columns(2)
        with col_g1:
            f1 = px.bar(weekly, x="wlbl", y="Articoli",
                        title=f"Articoli per settimana ({title_sfx})",
                        labels={"wlbl": "Settimana"},
                        color_discrete_sequence=[color])
            f1.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(f1, use_container_width=True)
        with col_g2:
            f2 = px.bar(weekly, x="wlbl", y="Valore",
                        title=f"Valore (€) per settimana ({title_sfx})",
                        labels={"wlbl": "Settimana", "Valore": "Valore (€)"},
                        color_discrete_sequence=[color])
            f2.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(f2, use_container_width=True)

        st.subheader("📋 Dettaglio per settimana")
        for _, row in weekly.iterrows():
            items = df_sub[df_sub["wlbl"] == row["wlbl"]]
            with st.expander(f"**{row['wlbl']}** — {row['Articoli']} art. · € {row['Valore']:,.0f}"):
                st.dataframe(
                    items[["order_conf", "pos", "item_no", "description",
                            "manufacturer", "ordered_qty", date_col, "status_class"]].assign(
                        **{date_col: items[date_col].map(fmt_delivery)},
                        status_class=items["status_class"].map(stato_label),
                    ).rename(columns={
                        "order_conf": "N° Conf.", "pos": "Pos.",
                        "item_no": "Articolo", "description": "Descrizione",
                        "manufacturer": "Produttore", "ordered_qty": "Qtà",
                        date_col: "Data", "status_class": "Stato",
                    }),
                    hide_index=True,
                )

    with tab_conf:
        if has_conf.empty:
            st.info("Nessuna posizione con data di consegna confermata.")
        else:
            _weekly_chart_and_detail(has_conf, "delivery_date", "confermata", "#2980b9")

    with tab_sched:
        if has_sched.empty:
            st.info("Nessuna posizione con data richiesta.")
        else:
            st.caption("Date richieste dal cliente (campo 'Information/requested delivery date')")
            _weekly_chart_and_detail(has_sched, "sched_dt", "richiesta", "#5d6d7e")

    with tab_nodate:
        if no_date.empty:
            st.success("✅ Tutte le posizioni hanno una data.")
        else:
            nd_stati = no_date["status_class"].value_counts().reset_index()
            nd_stati.columns = ["Stato", "Conteggio"]
            nd_stati["Label"] = nd_stati["Stato"].map(stato_label)

            col_p, col_t = st.columns([1, 2])
            with col_p:
                fig3 = px.pie(
                    nd_stati, names="Label", values="Conteggio",
                    title="Distribuzione per stato",
                    color_discrete_sequence=px.colors.qualitative.Set2,
                )
                st.plotly_chart(fig3, use_container_width=True)
            with col_t:
                st.dataframe(
                    no_date[["order_conf", "pos", "item_no", "description",
                              "ordered_qty", "information", "status_class"]].assign(
                        information=no_date["information"].map(fmt_information),
                        status_class=no_date["status_class"].map(stato_label),
                    ).rename(columns={
                        "order_conf": "N° Conf.", "pos": "Pos.",
                        "item_no": "Articolo", "description": "Descrizione",
                        "ordered_qty": "Qtà", "information": "Info",
                        "status_class": "Stato",
                    }),
                    hide_index=True,
                    use_container_width=True,
                )


# ══════════════════════════════════════════════════════════════════
# PAGINA 6 — KPI FORNITORE
# ══════════════════════════════════════════════════════════════════

def page_kpi():
    page_title("📈 KPI Fornitore")

    snaps = get_snapshots()
    if snaps.empty:
        st.info("Nessun dato disponibile.")
        return

    dates_sorted = sorted(snaps["file_date"].tolist())
    dates_labels = [fmt_date_str(d) for d in dates_sorted]

    # Precarica dati storici una volta sola
    @st.cache_data(ttl=60)
    def load_all_snapshots(dates):
        result = {}
        for d in dates:
            with _conn() as c:
                result[d] = pd.read_sql(
                    "SELECT p.* FROM positions p JOIN snapshots s ON p.snapshot_id=s.id "
                    "WHERE s.file_date=?", c, params=(d,)
                )
        return result

    all_data = load_all_snapshots(tuple(dates_sorted))

    # ── KPI 1: Fill Rate nel tempo ──────────────────────────────
    st.subheader("📊 KPI 1 — Fill Rate settimanale (% READY / posizioni aperte)")
    fill_rows = []
    for d in dates_sorted:
        df_d   = all_data[d]
        open_d = df_d[df_d["status_class"] != "COMPLETE"]
        ready  = df_d[df_d["status_class"] == "READY"]
        total  = len(open_d)
        fill_rows.append({
            "Data":          fmt_date_str(d),
            "Fill Rate (%)": (len(ready) / total * 100) if total > 0 else 0,
            "READY":         len(ready),
            "Totale open":   total,
            "Valore pronto": ready["value_in_stock"].sum(),
        })
    fill_df = pd.DataFrame(fill_rows)

    col_f1, col_f2 = st.columns([2, 1])
    with col_f1:
        fig1 = px.line(
            fill_df, x="Data", y="Fill Rate (%)", markers=True,
            title="Fill Rate settimanale",
            color_discrete_sequence=["#1e8449"],
        )
        fig1.update_layout(yaxis_range=[0, 100])
        st.plotly_chart(fig1, use_container_width=True)
    with col_f2:
        st.dataframe(fill_df[["Data", "READY", "Totale open", "Fill Rate (%)"]].assign(
            **{"Fill Rate (%)": fill_df["Fill Rate (%)"].round(1)}
        ), hide_index=True)

    # ── KPI 2: Distribuzione stati nel tempo ────────────────────
    st.markdown("---")
    st.subheader("📊 KPI 2 — Distribuzione stati nel tempo")
    pivot_rows = []
    for d in dates_sorted:
        df_d = all_data[d]
        row  = {"Data": fmt_date_str(d)}
        for sc, cfg in STATUS_CFG.items():
            row[cfg["label"]] = int((df_d["status_class"] == sc).sum())
        pivot_rows.append(row)
    pivot_df = pd.DataFrame(pivot_rows)

    melted = pivot_df.melt(id_vars="Data", var_name="Stato", value_name="Conteggio")
    color_map = {cfg["label"]: cfg["color"] for cfg in STATUS_CFG.values()}
    fig2 = px.bar(
        melted, x="Data", y="Conteggio", color="Stato", barmode="stack",
        title="Distribuzione stati nel tempo",
        color_discrete_map=color_map,
    )
    st.plotly_chart(fig2, use_container_width=True)

    # ── KPI 3: Drift data di consegna ───────────────────────────
    st.markdown("---")
    st.subheader("📊 KPI 3 — Drift data di consegna")
    if len(dates_sorted) >= 2:
        snap_opts   = {fmt_date_str(d): d for d in dates_sorted}
        col_k1, col_k2 = st.columns(2)
        with col_k1:
            d_new_lbl = st.selectbox("Snapshot corrente",  list(snap_opts.keys()),
                                     index=len(snap_opts)-1, key="kpi3_new")
        with col_k2:
            d_old_lbl = st.selectbox("Snapshot precedente", list(snap_opts.keys()),
                                     index=max(0, len(snap_opts)-2), key="kpi3_old")

        df_kpi_new = all_data[snap_opts[d_new_lbl]]
        df_kpi_old = all_data[snap_opts[d_old_lbl]]
        nk = df_kpi_new.set_index(["order_conf", "pos"])
        ok_idx = df_kpi_old.set_index(["order_conf", "pos"])
        common = set(nk.index) & set(ok_idx.index)

        drifts = []
        for key in common:
            rn = nk.loc[key]
            ro = ok_idx.loc[key]
            if isinstance(rn, pd.DataFrame):
                rn = rn.iloc[0]
            if isinstance(ro, pd.DataFrame):
                ro = ro.iloc[0]
            dd_n, dd_o = rn["delivery_date"], ro["delivery_date"]
            if dd_n and dd_o:
                try:
                    drifts.append((pd.to_datetime(dd_n) - pd.to_datetime(dd_o)).days)
                except Exception:
                    pass

        if drifts:
            drift_df = pd.DataFrame({"Drift (giorni)": drifts})
            fig3 = px.histogram(
                drift_df, x="Drift (giorni)", nbins=20,
                title="Distribuzione slittamento data consegna",
                color_discrete_sequence=["#2980b9"],
            )
            fig3.add_vline(x=0, line_dash="dash", line_color="red",
                           annotation_text="Nessun slittamento")
            st.plotly_chart(fig3, use_container_width=True)
            c1, c2, c3, c4 = st.columns(4)
            avg = sum(drifts) / len(drifts)
            c1.metric("Media slittamento", f"{avg:+.1f} gg")
            c2.metric("Max ritardo",        f"{max(drifts):+d} gg")
            c3.metric("In ritardo",         f"{sum(1 for x in drifts if x > 0)}")
            c4.metric("In anticipo",        f"{sum(1 for x in drifts if x < 0)}")
        else:
            st.info("Nessuna data di consegna comune trovata nei due snapshot.")
    else:
        st.info("Servono almeno 2 snapshot.")

    # ── KPI 4: Valore per stato (snapshot attuale) ───────────────
    st.markdown("---")
    st.subheader("📊 KPI 4 — Valore (€) per stato — snapshot più recente")
    df_last = all_data[dates_sorted[-1]]
    val_rows = []
    for sc, cfg in STATUS_CFG.items():
        sub = df_last[df_last["status_class"] == sc]
        val = sub["value_line"].sum()
        if val > 0:
            val_rows.append({
                "Stato":    f"{cfg['emoji']} {cfg['label']}",
                "Valore (€)": val,
                "color":    cfg["color"],
            })
    if val_rows:
        val_df = pd.DataFrame(val_rows)
        fig4 = px.bar(
            val_df, x="Stato", y="Valore (€)",
            title="Valore in attesa per stato",
            color="Stato",
            color_discrete_map={r["Stato"]: r["color"] for _, r in val_df.iterrows()},
        )
        st.plotly_chart(fig4, use_container_width=True)

    # ── KPI 5: Volume posizioni aperte nel tempo ─────────────────
    st.markdown("---")
    st.subheader("📊 KPI 5 — Volume posizioni aperte nel tempo")
    vol_rows = []
    for d in dates_sorted:
        df_d = all_data[d]
        vol_rows.append({
            "Data":              fmt_date_str(d),
            "Posizioni aperte":  int((df_d["status_class"] != "COMPLETE").sum()),
            "Posizioni READY":   int((df_d["status_class"] == "READY").sum()),
        })
    vol_df = pd.DataFrame(vol_rows)
    fig5 = px.line(
        vol_df.melt(id_vars="Data", var_name="Tipo", value_name="Conteggio"),
        x="Data", y="Conteggio", color="Tipo", markers=True,
        title="Posizioni aperte e READY nel tempo",
        color_discrete_map={
            "Posizioni aperte": "#8e44ad",
            "Posizioni READY":  "#1e8449",
        },
    )
    st.plotly_chart(fig5, use_container_width=True)


# ══════════════════════════════════════════════════════════════════
# PAGINA 7 — ESPORTA
# ══════════════════════════════════════════════════════════════════

def page_export():
    page_title("📥 Esporta Dati")

    file_date, df = snap_selector("📅 Snapshot da esportare", key="exp_snap")
    if df is None:
        return

    fdate_parsed = parse_file_date(file_date)
    next_wed     = next_wednesday(fdate_parsed)

    col_f1, col_f2 = st.columns(2)
    with col_f1:
        stati_opts   = ["Tutti"] + list(STATUS_CFG.keys())
        filtro_stato = st.selectbox("Filtra per stato", stati_opts, key="exp_stato")
    with col_f2:
        solo_open = st.checkbox(
            "Solo posizioni aperte (escludi COMPLETE)", value=True, key="exp_open"
        )

    export_df = df.copy()
    if solo_open:
        export_df = export_df[export_df["status_class"] != "COMPLETE"]
    if filtro_stato != "Tutti":
        export_df = export_df[export_df["status_class"] == filtro_stato]

    export_df["STATO"]             = export_df["status_class"].map(
        lambda s: STATUS_CFG.get(s, {}).get("label", s)
    )
    export_df["PROSSIMA_SPED"]     = next_wed.strftime("%d/%m/%Y")
    export_df["DATA_CONSEGNA_FMT"] = export_df["delivery_date"].map(fmt_delivery)

    rename_map = {
        "order_conf":      "N_CONFERMA_GEYER",
        "pos":             "POSIZIONE",
        "your_order":      "N_ORDINE_CLIENTE",
        "item_no":         "CODICE_ARTICOLO",
        "item_no_mfr":     "COD_PRODUTTORE",
        "manufacturer":    "PRODUTTORE",
        "description":     "DESCRIZIONE",
        "ordered_qty":     "QTA_ORDINATA",
        "directed_qty":    "QTA_DIRETTA",
        "stock_geyer":     "STOCK_GEYER",
        "unit_price":      "PREZZO_UNITARIO_EUR",
        "value_line":      "VALORE_EUR",
        "value_in_stock":  "VALORE_STOCK_EUR",
        "DATA_CONSEGNA_FMT": "DATA_CONSEGNA",
        "information":     "INFORMAZIONI",
        "STATO":           "STATO",
        "PROSSIMA_SPED":   "PROSSIMA_SPEDIZIONE",
    }
    out_cols = list(rename_map.values())
    out = export_df.rename(columns=rename_map)
    out = out[[c for c in out_cols if c in out.columns]]

    st.markdown(f"**{len(out)} posizioni da esportare**")
    st.dataframe(out.head(20), hide_index=True, use_container_width=True)

    col_d1, col_d2 = st.columns(2)
    with col_d1:
        csv_bytes = out.to_csv(index=False, sep=";", decimal=",").encode("utf-8-sig")
        st.download_button(
            "⬇️ Scarica CSV (;)",
            data=csv_bytes,
            file_name=f"geyer_export_{file_date}.csv",
            mime="text/csv",
        )
    with col_d2:
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            out.to_excel(w, index=False, sheet_name="Geyer Export")
        st.download_button(
            "⬇️ Scarica Excel (.xlsx)",
            data=buf.getvalue(),
            file_name=f"geyer_export_{file_date}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )


# ══════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════

def main():
    st.set_page_config(
        page_title="Monitor TH. Geyer",
        page_icon="🧪",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    init_db()

    with st.sidebar:
        st.markdown("## 🧪 Monitor TH. Geyer")
        st.markdown("Monitoraggio consegne fornitore")
        st.markdown("---")

        page = st.radio(
            "Navigazione",
            [
                "📤 Carica Excel",
                "📊 Situazione Attuale",
                "🚚 Spedizioni del Mercoledì",
                "🔔 Novità della Settimana",
                "📅 Calendario Consegne",
                "📈 KPI Fornitore",
                "📥 Esporta",
            ],
            label_visibility="collapsed",
        )

        st.markdown("---")
        st.markdown(
            "📅 File ricevuti ogni **giovedì**  \n"
            "🚚 Spedizioni ogni **mercoledì**"
        )
        st.markdown("---")
        with st.expander("📖 Legenda Information"):
            for code, desc in INFO_LEGEND.items():
                st.markdown(f"**{code}** — {desc}")

    if page == "📤 Carica Excel":
        page_upload()
    elif page == "📊 Situazione Attuale":
        page_current()
    elif page == "🚚 Spedizioni del Mercoledì":
        page_shipments()
    elif page == "🔔 Novità della Settimana":
        page_news()
    elif page == "📅 Calendario Consegne":
        page_calendar()
    elif page == "📈 KPI Fornitore":
        page_kpi()
    elif page == "📥 Esporta":
        page_export()


if __name__ == "__main__":
    main()
