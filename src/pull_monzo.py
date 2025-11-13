#!/usr/bin/env python3
from contextlib import nullcontext
import csv
import os
import pprint
import sys
import time
from typing import Any
import webbrowser
from datetime import datetime as dt, timedelta, timezone
from urllib.parse import urlencode, parse_qs, urlparse
from dateutil import parser
from dotenv import load_dotenv

import requests
import click

load_dotenv(override=True)

CLIENT_ID = os.getenv("MONZO_CLIENT_ID") or "YOUR_CLIENT_ID"
CLIENT_SECRET = os.getenv("MONZO_CLIENT_SECRET") or "YOUR_CLIENT_SECRET"
REDIRECT_URI = os.getenv("MONZO_REDIRECT_URI") or "http://localhost:8080/callback"

TOKEN_FILE = ".monzo_token"
CSV_FILE = "monzo_transactions.csv"
AUTH_URL = "https://auth.monzo.com/"
API_ROOT = "https://api.monzo.com/"

OUTPUT_DIRECTORY = "beancount_data/beancount_import_data/"

ACCOUNTS = {
}


def save_tokens(access: str, refresh: str):
    with open(TOKEN_FILE, "w") as fh:
        fh.write(f"{access}\n{refresh}")


def load_tokens():
    if not os.path.isfile(TOKEN_FILE):
        return None, None
    with open(TOKEN_FILE) as fh:
        parts = fh.read().strip().split("\n")
        access = parts[0]
        if len(parts) > 1:
            refresh = parts[1]
        else:
            refresh = None
    return access, refresh


def refresh_access_token(refresh: str):
    """Exchange refresh token for a new access token."""
    url = API_ROOT + "oauth2/token"
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }
    r = requests.post(url, data=data)
    r.raise_for_status()
    js = r.json()
    save_tokens(js["access_token"], js["refresh_token"])
    return js["access_token"]


def start_oauth():
    state = str(int(time.time()))
    qs = urlencode(
        {
            "client_id": CLIENT_ID,
            "redirect_uri": REDIRECT_URI,
            "response_type": "code",
            "state": state,
        }
    )
    print("\nOpening browser so you can authorise this app…\n")
    webbrowser.open(f"{AUTH_URL}?{qs}")

    print("After you authorise, Monzo will redirect to a localhost URL.")
    resp_url = input("Paste the FULL redirect URL here → ").strip()
    parsed = urlparse(resp_url)
    qs_back = parse_qs(parsed.query)
    if qs_back.get("state") != [state]:
        sys.exit("State mismatch – possible CSRF, aborting.")
    if "error" in qs_back:
        sys.exit("OAuth error: " + qs_back["error"][0])
    auth_code = qs_back["code"][0]

    # Exchange code for tokens
    url = API_ROOT + "oauth2/token"
    data = {
        "grant_type": "authorization_code",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "redirect_uri": REDIRECT_URI,
        "code": auth_code,
    }

    r = requests.post(url, data=data)
    r.raise_for_status()
    js = r.json()
    save_tokens(js["access_token"], js.get("refresh_token", ""))
    return js["access_token"]


def api_get(path: str, token: str, params=None):
    """Convenience wrapper around GET requests."""
    url = API_ROOT + path.lstrip("/")
    hdr = {"Authorization": f"Bearer {token}"}
    r = requests.get(url, headers=hdr, params=params or {})
    if r.status_code == 401:  # expired token – refresh once
        refresh_token = load_tokens()[1]
        if refresh_token is None:
            return
        token = refresh_access_token(refresh_token)
        hdr = {"Authorization": f"Bearer {token}"}
        r = requests.get(url, headers=hdr, params=params or {})
    r.raise_for_status()
    return r.json()


def get_accounts(token: str) -> list[dict[str, Any]]:
    data = api_get("/accounts", token)
    if data is None:
        sys.exit("No data returned")

    accounts = [a for a in data.get("accounts", []) if not a.get("closed")]

    return accounts


def get_pots(token: str, account_id: str) -> list[dict[str, Any]]:
    data = api_get(
        "/pots",
        token,
        {
            "current_account_id": account_id,
        },
    )
    if data is None:
        sys.exit("No data returned")

    accounts = [a for a in data.get("pots", []) if not a.get("closed")]

    return accounts


def fetch_all_transactions(
    token: str,
    account: dict[str, Any],
    start_date: str,
    end_date: str | None = None,
):
    """Download every transaction using pagination."""

    # Pagination is broken. Need to just keep going from the date of the last
    # transaction in each request until nothing is returned
    txns = []

    while True:
        params = {
            "account_id": account.get("id"),
            "limit": 100,
            "expand[]": "merchant",
            "since": start_date,
        }
        if end_date is not None:
            params["before"] = end_date

        print(f"Fetching transactions for {account["id"]} from {start_date}")
        data = api_get("/transactions", token, params)
        if data is None or len(data["transactions"]) == 0:
            break

        print(f"Got {len(data["transactions"])} transactions")
        txns.extend(data["transactions"])

        sd = parser.isoparse(txns[-1]["created"])
        sd = sd + timedelta(seconds=1)
        start_date = sd.astimezone().isoformat()

    return txns


def extract_payee(txn) -> str:
    if txn["metadata"] and txn["metadata"].get("pot_id", None) is not None:
        return txn["metadata"]["pot_id"]
    elif txn["merchant"] is not None and len(txn["merchant"]) > 0:
        return txn["merchant"]["name"]
    elif txn["counterparty"] is not None and len(txn["counterparty"]) > 0:
        return txn["counterparty"]["name"]
    else:
        return "UNKNOWN"


def extract_narration(txn) -> str:
    if txn["notes"] != "":
        return txn["notes"]
    # elif txn["metadata"] and txn["metadata"].get("pot_id", None) is not None:
    #     return "Round up"
    return txn["description"]


TX_FIELDS = [
    {
        "key": "Transaction ID",
        "fn": lambda x: x["id"],
    },
    {"key": "Date", "fn": lambda x: parser.parse(x["created"]).strftime("%d/%m/%Y")},
    {"key": "Name", "fn": extract_payee},
    {
        "key": "Description",
        "fn": extract_narration,
    },
    {
        "key": "Currency",
        "fn": lambda x: x["currency"],
    },
    {
        "key": "Amount",
        "fn": lambda x: x["amount"] / 100,  # Currency is in minor units.
    },
    {
        "key": "Category",
        "fn": lambda x: " ".join(x["category"].split("_")).capitalize(),
    },
]


def write_csv(transactions, pots, filename: str | None = None):
    """Write list of transaction dicts to CSV."""

    rows = []
    for t in transactions:
        r = {}
        
        if "decline_reason" in t:
            continue

        for k in TX_FIELDS:
            r[k["key"]] = k["fn"](t)

        rows.append(r)

        if r["Name"].startswith("pot_"):
            r["Name"] = pots[r["Name"]]["name"]

        if r["Name"] == "UNKNOWN":
            print(t)

    with (
        open(filename, "x", newline="", encoding="utf-8")
        if filename
        else nullcontext(sys.stdout)
    ) as fh:
        writer = csv.DictWriter(
            fh, fieldnames=[x["key"] for x in TX_FIELDS], extrasaction="ignore"
        )
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
    print(f"\n✅  {len(transactions)} transactions written to {filename}")


@click.command("txn")
@click.option("--txid", "txid")
def txn(txid: str|None) -> None:
    if txid is None:
        return
    access, refresh = load_tokens()
    if not access:
        access = start_oauth()
    elif not refresh:
        sys.exit("Refresh token missing – delete .monzo_token and rerun.")
    else:
        # ensure token is alive
        access = refresh_access_token(refresh)
        
    tx = api_get(f"/transactions/{txid}", access)
    
    pprint.pprint(tx)

@click.command()
@click.option("--start-date", "start_date")
@click.option("--end-date", "end_date")
@click.option("--save/--no-save", default=False)
@click.option("--out-dir", "out_dir")
def download(
    start_date: str | None, end_date: str | None, save: bool, out_dir: str | None
):

    if start_date is None:
        start_date = (dt.now(timezone.utc) - timedelta(10)).astimezone().isoformat()
    else:
        parsed_start_date = dt.strptime(start_date, "%Y-%m-%d")
        start_date = parsed_start_date.astimezone().isoformat()

    if end_date is not None:
        parsed_end_date = dt.strptime(end_date, "%Y-%m-%d")
        end_date = parsed_end_date.astimezone().isoformat()

    access, refresh = load_tokens()
    if not access:
        access = start_oauth()
    elif not refresh:
        sys.exit("Refresh token missing – delete .monzo_token and rerun.")
    else:
        # ensure token is alive
        access = refresh_access_token(refresh)

    accounts = get_accounts(access)
    pots_list = []
    for a in accounts:
        pots_list.extend(get_pots(access, a["id"]))

    pots = {}
    for p in pots_list:
        pots[p["id"]] = p

    for a in accounts:
        if a["id"] in ACCOUNTS.keys():
            txns = fetch_all_transactions(
                access,
                a,
                start_date=start_date,
                end_date=end_date,
            )
            if len(txns) == 0:
                continue

            first_date = parser.parse(txns[0]["created"]).strftime("%Y-%m-%d")
            last_date = parser.parse(txns[-1]["created"]).strftime("%Y-%m-%d")
            filename = f"MonzoExport_{ACCOUNTS[a["id"]]}_{first_date}_{last_date}.csv"

            write_csv(
                txns,
                pots,
                (
                    os.path.join(
                        os.getcwd(),
                        out_dir if out_dir else "",
                        ACCOUNTS[a["id"]] if out_dir else "",
                        filename,
                    )
                    if save
                    else None
                ),
            )


if __name__ == "__main__":
    download()
