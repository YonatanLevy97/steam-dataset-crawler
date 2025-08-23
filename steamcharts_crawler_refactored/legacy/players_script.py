import requests
from bs4 import BeautifulSoup
import csv
import time
import random

POLITE_DELAY = 0.3


def read_appids_from_csv(metadata_csv_path):
    """
    Read Steam appids from a metadata CSV file with columns: appid, name
    Returns a list of integers (appids).
    """
    appids = []
    with open(metadata_csv_path, mode="r", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            try:
                appid = int(row["appid"])
                appids.append(appid)
            except (ValueError, KeyError):
                continue
    return appids


def get_app_list():
    """
    Fetch the full list of Steam apps (appid → name) using Steam Web API.
    Returns a dict mapping int(appid) to string(name).
    """
    url = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"
    response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    response.raise_for_status()
    apps = response.json().get("applist", {}).get("apps", [])
    return {int(app["appid"]): app["name"] for app in apps}


def get_all_monthly_players(appid):
    """
    Given a SteamCharts appid, return a list of monthly player records:
        [
            {
                "month": "Month Year" or "Last 30 Days",
                "avg_players": float,
                "peak_players": int or None,
                "change_percent": float or None
            },
            ...
        ]
    Scrapes https://steamcharts.com/app/{appid} and parses the table.
    """
    url = f"https://steamcharts.com/app/{appid}"
    headers = {"User-Agent": "Mozilla/5.0"}

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        # no data or page not found
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", class_="common-table")
    if not table:
        return []

    rows = table.find_all("tr")[1:]
    monthly_data = []

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 5:
            continue

        month_text = cols[0].text.strip()
        avg_text = cols[1].text.strip().replace(",", "")
        peak_text = cols[4].text.strip().replace(",", "")
        pct_text = cols[3].text.strip().replace("%", "").replace(",", "")

        try:
            avg_players = float(avg_text)
        except ValueError:
            continue

        try:
            peak_players = int(peak_text)
        except ValueError:
            peak_players = None

        try:
            change_percent = float(pct_text)
        except ValueError:
            change_percent = None

        monthly_data.append({
            "month": month_text,
            "avg_players": avg_players,
            "peak_players": peak_players,
            "change_percent": change_percent
        })

    return monthly_data


def write_games_to_csv(metadata_csv_path, output_csv_path, sample_size=50000):
    """
    Read all appids from a metadata CSV, randomly sample `sample_size` מהם (ללא חזרות),
    ולכל אחד מהם לגרד את הנתונים החודשיים ולכתוב ל-CSV יחיד.
    עמודות ה-CSV: appid, name, month, avg_players, peak_players, change_percent
    """
    all_appids = read_appids_from_csv(metadata_csv_path)

    if len(all_appids) <= sample_size:
        sampled_appids = all_appids.copy()
    else:
        sampled_appids = random.sample(all_appids, sample_size)

    app_list = get_app_list()

    with open(output_csv_path, mode="w", newline="", encoding="utf-8") as csvfile:
        fieldnames = ["appid", "name", "month", "avg_players", "peak_players", "change_percent"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        total = len(sampled_appids)
        for idx, appid in enumerate(sampled_appids, start=1):
            name = app_list.get(appid, "")
            monthly_records = get_all_monthly_players(appid)

            if not monthly_records:
                print(f"[{idx}/{total}] AppID {appid} ('{name}') → no data, skipping")
            else:
                print(f"[{idx}/{total}] AppID {appid} ('{name}') → {len(monthly_records)} months")
                for record in monthly_records:
                    writer.writerow({
                        "appid": appid,
                        "name": name,
                        "month": record["month"],
                        "avg_players": record["avg_players"],
                        "peak_players": record["peak_players"] if record["peak_players"] is not None else 0,
                        "change_percent": record["change_percent"] if record["change_percent"] is not None else 0
                    })

            time.sleep(POLITE_DELAY)

    print(f"\nDataset written to {output_csv_path} (sampled {total} apps)")


if __name__ == "__main__":
    metadata_csv = "steam_app_metadata.csv"
    output_path = "steamcharts_dataset_sampled.csv"

    write_games_to_csv(metadata_csv, output_path, sample_size=70000)