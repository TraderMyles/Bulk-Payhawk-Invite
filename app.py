import os
import io
import time
import json
import requests
import streamlit as st
import pandas as pd
from dotenv import load_dotenv

BASE = "https://api.payhawk.com/api/v3"

# --------------------------
# Credentials (secrets first, then .env)
# --------------------------
def get_creds():
    try:
        api_key = (st.secrets["PAYHAWK_API_KEY"] or "").strip()
        account_id = (st.secrets["PAYHAWK_ACCOUNT_ID"] or "").strip()
        if api_key and account_id:
            return api_key, account_id, "secrets.toml"
    except Exception:
        pass

    load_dotenv()
    api_key = (os.getenv("PAYHAWK_API_KEY") or "").strip()
    account_id = (os.getenv("PAYHAWK_ACCOUNT_ID") or "").strip()
    return api_key, account_id, ".env"


# --------------------------
# API call with backoff
# --------------------------
def post_invite(api_key, account_id, email, first=None, last=None, role="employee", max_retries=5):
    url = f"{BASE}/accounts/{account_id}/users"
    headers = {
        "X-Payhawk-ApiKey": api_key,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    payload = {"email": email, "role": role or "employee"}
    if first:
        payload["firstName"] = first
    if last:
        payload["lastName"] = last

    retry = 0
    while True:
        r = requests.post(url, headers=headers, json=payload, timeout=30)
        # Respect rate limiting with exponential backoff (cap 10s)
        if r.status_code == 429 and retry < max_retries:
            retry += 1
            time.sleep(min(2 ** retry, 10))
            continue
        return r, payload


# --------------------------
# UI
# --------------------------
st.set_page_config(page_title="Payhawk – Bulk Invite", page_icon="📧", layout="wide")
st.title("📧 Payhawk – Bulk Invite Users from CSV")

st.markdown(
    "Upload a CSV with at least an **email** column. Optional columns: **first**, **last**, **role**.\n\n"
    "Tip: You can add a header row like: `email,first,last,role`."
)

# Credentials status
api_key, account_id, creds_source = get_creds()
if not api_key or not account_id:
    st.error(
        "Missing credentials. Set `PAYHAWK_API_KEY` and `PAYHAWK_ACCOUNT_ID` in "
        "**.streamlit/secrets.toml** (recommended) or a local **.env** file."
    )
    st.stop()

st.caption(f"Credentials source: {creds_source}")

# Sample CSV download
sample = pd.DataFrame(
    [
        {"email": "alice@example.com", "first": "Alice", "last": "Ng", "role": "employee"},
        {"email": "bob@example.com", "first": "Bob", "last": "Jones", "role": "admin"},
    ]
)
sample_csv = sample.to_csv(index=False)
st.download_button("Download CSV template", data=sample_csv, file_name="payhawk_invites_template.csv", mime="text/csv")

# File uploader
uploaded = st.file_uploader("Upload CSV", type=["csv"])
if not uploaded:
    st.stop()

# Read CSV (robust to BOM)
try:
    # Try pandas read with UTF-8; fallback to utf-8-sig
    try:
        df = pd.read_csv(uploaded)
    except UnicodeDecodeError:
        uploaded.seek(0)
        df = pd.read_csv(uploaded, encoding="utf-8-sig")
except Exception as e:
    st.error(f"Could not read CSV: {e}")
    st.stop()

if df.empty:
    st.warning("The CSV appears to be empty.")
    st.stop()

# Normalize headers for auto-detect
cols_lower = {c.lower(): c for c in df.columns}

def pick(existing_keys, label, required=False):
    # Suggest auto-detected match if present
    default = None
    for k in ["email", "first", "last", "role"]:
        if label == k and k in cols_lower:
            default = cols_lower[k]
    choices = ["(none)"] + list(df.columns)
    idx = 0
    if default in df.columns:
        idx = choices.index(default)
    sel = st.selectbox(
        f"Map column for **{label}**" + (" (required)" if required else ""),
        choices,
        index=idx,
        key=f"map_{label}",
    )
    return None if sel == "(none)" else sel

st.subheader("Column mapping")
col1, col2, col3, col4 = st.columns(4)
email_col = col1.selectbox("Email (required)", list(df.columns), index=list(df.columns).index(cols_lower["email"]) if "email" in cols_lower else 0)
first_col = col2.selectbox("First (optional)", ["(none)"] + list(df.columns), index=(["(none)"] + list(df.columns)).index(cols_lower["first"]) if "first" in cols_lower else 0)
last_col  = col3.selectbox("Last (optional)",  ["(none)"] + list(df.columns), index=(["(none)"] + list(df.columns)).index(cols_lower["last"]) if "last" in cols_lower else 0)
role_col  = col4.selectbox("Role (optional)",  ["(none)"] + list(df.columns), index=(["(none)"] + list(df.columns)).index(cols_lower["role"]) if "role" in cols_lower else 0)

# Options
st.markdown("---")
dry_run = st.checkbox("Dry-run (don’t call the API; just simulate)", value=False)
throttle_ms = st.number_input("Delay between requests (ms)", min_value=0, max_value=2000, value=100, step=50, help="Small delay helps avoid rate limits.")
st.markdown("---")

# Preview
st.subheader("Preview (first 10 rows)")
st.dataframe(df.head(10), use_container_width=True)

# Process button
run = st.button("Run Bulk Invite")
if not run:
    st.stop()

# Validate required mapping
if not email_col:
    st.error("Please select an **Email** column.")
    st.stop()

# Prepare results
results = []
progress = st.progress(0, text="Starting...")
status_area = st.empty()
total = len(df)

for idx, row in df.iterrows():
    email = str(row.get(email_col, "") or "").strip()
    first = str(row.get(first_col, "")).strip() if first_col and first_col != "(none)" else ""
    last  = str(row.get(last_col, "")).strip() if last_col and last_col != "(none)" else ""
    role  = str(row.get(role_col, "")).strip() if role_col and role_col != "(none)" else "employee"

    if not email:
        results.append({"row": idx + 2, "email": "", "status": "SKIPPED: missing email"})
        progress.progress(min((idx + 1) / total, 1.0), text=f"Row {idx+1}/{total} — skipped (no email)")
        continue

    if dry_run:
        results.append({"row": idx + 2, "email": email, "status": "DRY_RUN: would INVITE", "payload": {"email": email, "first": first, "last": last, "role": role}})
        progress.progress(min((idx + 1) / total, 1.0), text=f"Row {idx+1}/{total} — simulated")
        continue

    # Real API call
    try:
        resp, payload = post_invite(api_key, account_id, email, first or None, last or None, role or "employee")
        if resp.status_code in (200, 201):
            status = "INVITED"
        elif resp.status_code in (400, 409) and "already" in resp.text.lower():
            status = "ALREADY_EXISTS"
        elif resp.status_code == 401:
            st.error("Unauthorized (401). Check API key/account ID and Payhawk API connection.")
            results.append({"row": idx + 2, "email": email, "status": "INVITE_ERROR 401", "response": resp.text[:300]})
            break
        else:
            status = f"INVITE_ERROR {resp.status_code}"
        results.append(
            {
                "row": idx + 2,
                "email": email,
                "first": first,
                "last": last,
                "role": role,
                "status": status,
                "response_snippet": resp.text[:300] if resp is not None else "",
            }
        )
    except requests.RequestException as e:
        results.append({"row": idx + 2, "email": email, "status": "NETWORK_ERROR", "error": str(e)})

    # Gentle pacing
    if throttle_ms:
        time.sleep(throttle_ms / 1000.0)

    progress.progress(min((idx + 1) / total, 1.0), text=f"Row {idx+1}/{total} — {results[-1]['status']}")

# Results table
st.success("Done.")
res_df = pd.DataFrame(results)
st.subheader("Results")
st.dataframe(res_df, use_container_width=True)

# Download results
out_csv = res_df.to_csv(index=False).encode("utf-8")
st.download_button("Download results CSV", data=out_csv, file_name="invite_results.csv", mime="text/csv")

# Optional raw JSON preview for debugging
with st.expander("Debug info"):
    st.write({"account_id": account_id, "dry_run": dry_run, "rows": total})
