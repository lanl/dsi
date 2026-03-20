import subprocess
from pathlib import Path
from typing import Tuple, Dict
import sys

import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import matplotlib.cm as cm

DIRECTORIES = sys.argv[1:]

permission_denied_paths = set()
PERMISSION_ERRORS = ["permission denied", "operation not permitted"]

def run_cmd(cmd: list[str], top_path: str | None = None) -> tuple[str | None, str | None, int]:
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

        if err and top_path:
            err_l = err.lower()
            if any(e in err_l for e in PERMISSION_ERRORS):
                permission_denied_paths.add(top_path)

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
    # Getting total directory size
    out, err, rc = run_cmd(["du", "-bs", path], top_path=path)
    if rc == 0 and out:
        try:
            total_bytes = int(out.split()[0])
        except Exception:
            total_bytes = None

        if total_bytes is None:
            return None, {}
    
        # Getting top-level directories
        out2, err2, rc2 = run_cmd(["du", "-b", "--max-depth=1", path], top_path=path)
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

        # Adding top-level files
        try:
            for p in base.iterdir():
                if p.is_file():
                    out3, err3, rc3 = run_cmd(["du", "-b", str(p)], top_path=path)
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
    # Getting total directory size
    out, err, rc = run_cmd(["du", "-k", "-s", path], top_path=path)
    if rc != 0 or not out:
        if not any(e in err.lower() for e in PERMISSION_ERRORS): # error unrelated to permissions
            st.warning(f"`du {path}` failed: {err or 'unknown error'}")
        return None, {}

    try:
        total_kib = int(out.split()[0])
        total_bytes = total_kib * 1024
    except Exception:
        return None, {}
    
    # Getting top-level directories
    out2, err2, rc2 = run_cmd(["du", "-k", "-d", "1", path], top_path=path)
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

    # Adding top-level files
    try:
        for p in base.iterdir():
            if p.is_file():
                out3, err3, rc3 = run_cmd(["du", "-k", str(p)], top_path=path)
                if rc3 == 0 and out3:
                    try:
                        size_kib = int(out3.split()[0])
                        breakdown[str(p)] = size_kib * 1024
                    except Exception:
                        pass
    except Exception:
        pass

    return total_bytes, breakdown


@st.cache_data(ttl=60, show_spinner="Scanning all input directories...")
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


st.set_page_config(page_title="DSI Dashboard", layout="wide")
st.title("DSI Dashboard")

df_table, du_map = collect_data(DIRECTORIES)

if permission_denied_paths:
    msg = "\n".join(f"- {p}" for p in sorted(permission_denied_paths))
    st.warning(f"Permission denied while scanning:\n{msg}")

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

    st.subheader("Overall directory used vs storage limit")

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
    limit_positions = [i for i, _ in enumerate(limit_vals)]
    ax0.bar(limit_positions, limit_vals, width=bar_w, alpha=0.35, color="steelblue", label="Limit")
    ax0.bar(x, used_vals, width=bar_w, color="orange", edgecolor="black", linewidth=0.8, label="Used")

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

# Group small items into a single category
SMALL_THRESHOLD_PCT = 1.0
small = [(n, b, p) for (n, b, p) in pairs if p < SMALL_THRESHOLD_PCT]
big = [(n, b, p) for (n, b, p) in pairs if p >= SMALL_THRESHOLD_PCT]

small_bytes = sum(b for _, b, _ in small)

final = big[:]
if small_bytes > 0:
    small_pct = (small_bytes / total_bytes) * 100.0
    final.append(("Small items (each <1%)", int(small_bytes), float(small_pct)))

# If sum(final_values) does not match total_bytes, add Other category to chart
sum_final = sum(b for (_, b, _) in final)
diff = int(total_bytes - sum_final)

if diff != 0:
    if diff > 0:
        final.append(("Other items", int(diff), (diff / total_bytes) * 100.0))
    else:
        over = abs(diff)
        final.append(("Overcount (breakdown > total)", int(over), (over / total_bytes) * 100.0))

# Sort by decreasing size
final = sorted(final, key=lambda x: x[1], reverse=True)

# BAR CHART
labels = []
values = []

for name, b, _ in final:
    display_name = f"'{name}'" if name.startswith("_") else name
    labels.append(display_name)
    values.append(b)

# darker colors from tab20
cmap = cm.get_cmap("tab20")
palette = [cmap(i) for i in range(0, 20, 2)]  # use darker half only

colors = []
palette_i = 0

for name, _, _ in final:
    if name == "Small items (each <1%)":
        colors.append("dimgray")
    elif name == "Other items":
        colors.append("darkgray")
    elif name == "Overcount (breakdown > total)":
        colors.append("firebrick")
    else:
        colors.append(palette[palette_i % len(palette)])
        palette_i += 1

fig, ax = plt.subplots(figsize=(max(8, len(labels) * 0.6), 5))

bars = ax.bar(range(len(labels)), values, color=colors, edgecolor="black", linewidth=0.6)

def _fmt_bytes_tick(v, _pos):
    try:
        return bytes_to_human(int(v))
    except Exception:
        return ""

ax.yaxis.set_major_formatter(FuncFormatter(_fmt_bytes_tick))
ax.set_xticks(range(len(labels)))
ax.set_xticklabels(labels, rotation=45, ha="right")
ax.set_ylabel("Storage Used")
ax.set_title(f"Breakdown of Used Storage in {selected_dir}")

for bar, (name, b, p) in zip(bars, final):
    # Always show % for the small-items bar
    if p >= 3 or name == "Small items (each <1%)":
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height(),
            f"{p:.1f}% ({bytes_to_human(b)})",
            ha="center",
            va="bottom",
            fontsize=8,
        )

plt.tight_layout()
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

    st.data_editor(
        small_df[["item", "size", "percent"]],
        width="stretch",
        hide_index=True,
        column_config={
            "item": st.column_config.TextColumn("Item"),
            "size": st.column_config.TextColumn("Size"),
            "percent": st.column_config.TextColumn("% of Total Storage Used"),
        },
        disabled=["item", "size", "percent"],
        key="small_dir_table",
    )