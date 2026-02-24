# streamlit_app.py
import subprocess
from pathlib import Path
from typing import Tuple, Dict
import sys

import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

DIRECTORIES = sys.argv[1:]

def run_cmd(cmd: list[str]) -> tuple[str | None, str | None, int]:
    """Run a command. Return (stdout, stderr, returncode). Never raises."""
    try:
        p = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        out = (p.stdout or "").strip()
        err = (p.stderr or "").strip()
        return out, err, p.returncode
    except Exception as e:
        return None, str(e), 1

def bytes_to_human(n: int) -> str:
    """
    Convert bytes to human-readable string using full unit labels:
    PB, TB, GB, MB, KB, B (base 1024).
    """
    n = int(n) if n is not None else 0
    if n >= 1024**5:
        return f"{n / 1024**5:.2f} PB"
    if n >= 1024**4:
        return f"{n / 1024**4:.2f} TB"
    if n >= 1024**3:
        return f"{n / 1024**3:.2f} GB"
    if n >= 1024**2:
        return f"{n / 1024**2:.2f} MB"
    if n >= 1024:
        return f"{n / 1024:.2f} KB"
    return f"{n} B"

def parse_df_limit_bytes(path: str) -> int | None:
    """
    Return filesystem capacity in bytes for the filesystem that contains `path`.
    Uses `df -k` for portability (1K-blocks), then converts to bytes.
    """
    out, err, rc = run_cmd(["df", "-k", path])
    if rc != 0 or not out:
        st.warning(f"`df -k {path}` failed: {err or 'unknown error'}")
        return None

    lines = out.splitlines()
    if len(lines) < 2:
        return None

    fields = lines[1].split()
    if len(fields) < 2:
        return None

    try:
        blocks_1k = int(fields[1])
        return blocks_1k * 1024
    except Exception:
        return None
    
def du_depth1_with_total_bytes(path: str) -> Tuple[int | None, Dict[str, int]]:
    base = Path(path).resolve()
    breakdown: Dict[str, int] = {}

    # Try for Linux
    #Getting total directory size
    out, err, rc = run_cmd(["du", "-bs", path])
    if rc == 0 and out:
        try:
            total_bytes = int(out.split()[0])
        except Exception:
            total_bytes = None

        if total_bytes is None:
            return None, {}
    
        #Getting top-level directories
        out2, err2, rc2 = run_cmd(["du", "-b", "--max-depth=1", path])
        if rc2 == 0 and out2:
            for line in out2.splitlines():
                parts = line.split(maxsplit=1)
                if len(parts) != 2:
                    continue
                size_s, name = parts
                try:
                    size_b = int(size_s)
                except Exception:
                    continue

                try:
                    resolved = Path(name).resolve()
                except Exception:
                    resolved = None

                if resolved == base:
                    continue
                breakdown[name] = size_b

        #Adding top-level files
        try:
            for p in base.iterdir():
                if p.is_file():
                    out3, err3, rc3 = run_cmd(["du", "-b", str(p)])
                    if rc3 == 0 and out3:
                        try:
                            size_b = int(out3.split()[0])
                            breakdown[str(p)] = size_b
                        except Exception:
                            pass
        except Exception:
            pass

        return total_bytes, breakdown

    # Try for macOS
    #Getting total directory size
    out, err, rc = run_cmd(["du", "-k", "-s", path])
    if rc != 0 or not out:
        st.warning(f"`du {path}` failed: {err or 'unknown error'}")
        return None, {}

    try:
        total_kib = int(out.split()[0])
        total_bytes = total_kib * 1024
    except Exception:
        return None, {}
    
    #Getting top-level directories
    out2, err2, rc2 = run_cmd(["du", "-k", "-d", "1", path])
    if rc2 == 0 and out2:
        for line in out2.splitlines():
            parts = line.split(maxsplit=1)
            if len(parts) != 2:
                continue
            size_s, name = parts
            try:
                size_kib = int(size_s)
            except Exception:
                continue

            try:
                resolved = Path(name).resolve()
            except Exception:
                resolved = None

            if resolved == base:
                continue
            breakdown[name] = size_kib * 1024

    #Adding top-level files
    try:
        for p in base.iterdir():
            if p.is_file():
                out3, err3, rc3 = run_cmd(["du", "-k", str(p)])
                if rc3 == 0 and out3:
                    try:
                        size_kib = int(out3.split()[0])
                        breakdown[str(p)] = size_kib * 1024
                    except Exception:
                        pass
    except Exception:
        pass

    return total_bytes, breakdown


@st.cache_data(ttl=60)  # refresh every 60s
def collect_data(dirs: list[str]) -> tuple[pd.DataFrame, dict[str, dict[str, int]]]:
    rows = []
    du_map: dict[str, dict[str, int]] = {}

    for d in dirs:
        p = Path(d)
        if not p.exists():
            continue

        total_b, breakdown = du_depth1_with_total_bytes(d)
        if total_b is None:
            continue

        limit_b = parse_df_limit_bytes(d)

        rows.append({"directory": d, "total_bytes": int(total_b), "limit_bytes": int(limit_b)})
        du_map[d] = breakdown

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("total_bytes", ascending=False).reset_index(drop=True)
    return df, du_map


st.set_page_config(page_title="Diana Dashboard", layout="wide")
st.title("Diana Dashboard")

df_table, du_map = collect_data(DIRECTORIES)

if df_table.empty:
    st.error("No valid directories found.")
    st.stop()

st.subheader("Directory breakdown")

df_view = df_table.copy()
df_view["total"] = df_view["total_bytes"].apply(bytes_to_human)
df_view["limit"] = df_view["limit_bytes"].apply(bytes_to_human)

df_view.insert(0, "select", False)

edited = st.data_editor(
    df_view[["select", "directory", "total", "limit"]],
    width="stretch",
    hide_index=True,
    column_config={
        "select": st.column_config.CheckboxColumn("Select"),
        "directory": st.column_config.TextColumn("Directory"),
        "limit": st.column_config.TextColumn("Storage Limit"),
        "total": st.column_config.TextColumn("Total Storage Used"),
    },
    disabled=["directory", "total", "limit"],
    key="dir_table",
)

selected_dir = None
try:
    selected_rows = edited[edited["select"] == True]
    if not selected_rows.empty:
        selected_dir = selected_rows.iloc[0]["directory"]
except Exception:
    pass

if not selected_dir:
    st.info("Check a directory to see its top-level breakdown.")

    st.subheader("Directory usage vs storage limit")

    dirs = df_table["directory"].tolist()
    used_vals = [max(int(x), 1) for x in df_table["total_bytes"].tolist()]
    limit_vals = [max(int(x), 1) for x in df_table["limit_bytes"].tolist()]

    x = list(range(len(dirs)))

    def _fmt_bytes_tick(v, _pos):
        try: 
            return bytes_to_human(int(v))
        except Exception:
            return ""

    fig0, ax0 = plt.subplots(figsize=(8, 4))

    bar_w = 0.8
    # Limit bars
    limit_positions = [i for i, lv in enumerate(limit_vals)]
    ax0.bar(limit_positions, limit_vals, width=bar_w, alpha=bar_w, color = "steelblue", label="Limit")

    # Used bars
    ax0.bar(x, used_vals, width=bar_w, color = "orange", edgecolor="black", linewidth=0.8, label="Used")

    if len(dirs) == 1:
        ax0.set_xlim(-1, 1)

    ax0.set_yscale("log")
    ax0.yaxis.set_major_formatter(FuncFormatter(_fmt_bytes_tick))
    ax0.set_xticks(x)
    ax0.set_xticklabels(dirs, rotation=45)
    plt.tight_layout()
    ax0.set_ylabel("Bytes (log scale)")
    ax0.legend(loc="best")
    st.pyplot(fig0)

    st.stop()

df_row = df_table[df_table["directory"] == selected_dir].iloc[0]
total_bytes = int(df_row["total_bytes"])
limit_bytes = df_row["limit_bytes"]

subitems = du_map.get(selected_dir, {})
if subitems is None:
    st.info("No top-level item data (or du returned nothing).")
    st.stop()

pairs = []
for full_path, b in subitems.items():
    name = Path(full_path).name or full_path
    b = int(b)
    if b <= 0:
        continue
    pct = (b / total_bytes) * 100.0 if total_bytes > 0 else 0.0
    pairs.append((name, b, pct))

if total_bytes <= 0 or not pairs:
    st.info("Directory total is 0 bytes or no measurable children.")
    st.stop()

# Group small items into a single slice
SMALL_THRESHOLD_PCT = 1.0
small = [(n, b, p) for (n, b, p) in pairs if p < SMALL_THRESHOLD_PCT]
big = [(n, b, p) for (n, b, p) in pairs if p >= SMALL_THRESHOLD_PCT]

small_bytes = sum(b for _, b, _ in small)

final = big[:]
if small_bytes > 0:
    small_pct = (small_bytes / total_bytes) * 100.0
    final.append(("Small items (each <1%)", int(small_bytes), float(small_pct)))

# if sum(final_values) does not match total_bytes, add Other slice to chart
sum_final = sum(b for (_, b, _) in final)
diff = int(total_bytes - sum_final)

if diff != 0:
    if diff > 0: # add Other (in dir) to account for missing bytes
        final.append(("Other items", int(diff), (diff / total_bytes) * 100.0))
    else: # negative diff: breakdown > total; represent as Overcount (absolute value)
        over = abs(diff)
        final.append(("Overcount (breakdown > total)", int(over), (over / total_bytes) * 100.0))

# Sort by decreasing percent
final = sorted(final, key=lambda x: x[2], reverse=True)

final_labels = [n for (n, _, _) in final]
final_values = [b for (_, b, _) in final]


#only add # for slices > 3% -- change min_pct_show to be whatever
def autopct_fmt(pct: float, min_pct_show = 3) -> str: #
    return f"{pct:.1f}%" if pct >= min_pct_show else ""

fig, ax = plt.subplots(figsize=(6, 5))

wedges, _, _ = ax.pie(
    final_values,
    labels=None,
    autopct=autopct_fmt,
    startangle=90,
    pctdistance=0.75,
)

ax.set_title(f"Breakdown of Used Storage in {selected_dir}")
ax.axis("equal")

legend_texts = []
for name, b, _ in final:
    pct_val = 100.0 * b / sum(final_values)
    if name.startswith("_"):
        name = f"'{name}'"
    if name == "Small items (each <1%)":
        legend_texts.append(f"{name} ({bytes_to_human(b)})")
    else:
        legend_texts.append(f"{name} ({bytes_to_human(b)}) - {autopct_fmt(pct_val, min_pct_show=0)}")

ax.legend(
    wedges,
    legend_texts,
    title="Items (Size)",
    loc="center left",
    bbox_to_anchor=(1.02, 0.5),
    fontsize=9,
)

st.pyplot(fig)

if small:
    st.markdown("#### Small Items Breakdown")
    small_sorted = sorted(small, key=lambda x: x[1], reverse=True)
    small_df = pd.DataFrame(
        {
            "item": [n for (n, _, _) in small_sorted],
            "size": [bytes_to_human(b) for (_, b, _) in small_sorted],
            "percent": [f"{p:.2f}%" for (_, _, p) in small_sorted],
            "bytes": [b for (_, b, _) in small_sorted],
        }
    ).sort_values("bytes", ascending=False)
    # st.dataframe(small_df[["item", "size", "percent"]], width="stretch", hide_index=True)

    edited = st.data_editor(
        small_df[["item", "size", "percent"]], width="stretch", hide_index=True,
        column_config={
            "item": st.column_config.TextColumn("Item"),
            "size": st.column_config.TextColumn("Size"),
            "percent": st.column_config.TextColumn("% of Total Storage Used"),
        },
        disabled=["item", "size", "percent"],
        key="small_dir_table",
    )