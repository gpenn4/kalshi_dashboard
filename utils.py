import requests
import pandas as pd
import numpy as np
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive


def get_data_from_api(url: str, limit: int = 200) -> pd.DataFrame:
    """
    Fetch data from the given API endpoint and return it as a pandas DataFrame.

    Parameters:
    url (str): The API endpoint URL.
    limit (int): The number of records to fetch.

    Returns:
    pd.DataFrame: DataFrame containing the fetched data.
    """
    full_url = f"{url}?limit={limit}&with_nested_markets=true&status=open"
    # all_markets = pd.DataFrame()
    frames = []
    cursor = None
    tot = 0

    try:
        while True:
            paged_url = full_url + (f"&cursor={cursor}" if cursor else "")
            response = requests.get(paged_url)
            response.raise_for_status()
            data = response.json()

            incl_markets = pd.json_normalize(data["events"], "markets")
            cursor = data.get("cursor")
            # all_markets = pd.concat([all_markets, incl_markets])
            frames.append(incl_markets)
            tot += len(incl_markets)
            print(f"Fetched {len(data['events'])} events, total: {tot} markets.")

            if not cursor:
                break

    except Exception as e:
        print(f"An error occurred: {e}")

    all_markets = pd.concat(frames, ignore_index=True)

    return all_markets


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and preprocess the DataFrame.

    Parameters:
    df (pd.DataFrame): The DataFrame to clean.

    Returns:
    pd.DataFrame: The cleaned DataFrame.
    """
    # df = df.dropna(subset=['market_type', 'status'])
    # df['created_at'] = pd.to_datetime(df['created_at'])
    # df['updated_at'] = pd.to_datetime(df['updated_at'])

    relevant_columns = [
        "event_ticker",
        "liquidity",
        "liquidity_dollars",
        "market_type",
        "no_ask",
        "no_ask_dollars",
        "no_bid",
        "no_bid_dollars",
        "no_sub_title",
        "title",
        "yes_ask",
        "yes_ask_dollars",
        "yes_bid",
        "yes_bid_dollars",
        "yes_sub_title",
    ]

    numeric_cols = [
        "liquidity",
        "liquidity_dollars",
        "no_ask",
        "no_ask_dollars",
        "no_bid",
        "no_bid_dollars",
        "yes_ask",
        "yes_ask_dollars",
        "yes_bid",
        "yes_bid_dollars",
    ]

    cleaned = df[relevant_columns].copy()

    for nc in numeric_cols:
        cleaned[nc] = pd.to_numeric(cleaned[nc], errors="coerce")

    return cleaned


def get_top_2(df: pd.DataFrame) -> pd.DataFrame:
    """
    Get the top 2 markets by liquidity for each event ticker.

    Parameters:
    df (pd.DataFrame): The DataFrame containing market data.

    Returns:
    pd.DataFrame: DataFrame with top 2 markets by liquidity for each event ticker.
    """
    top2 = (
        df.dropna(subset=["event_ticker", "liquidity"])
        .sort_values(["event_ticker", "liquidity"], ascending=[True, False])
        .groupby("event_ticker", as_index=False)
        .head(2)
    )

    top2["m_rank"] = top2.groupby("event_ticker").cumcount() + 1

    # 3) Choose which columns you want to preserve from each outcome/market
    market_cols = [
        "title",
        "yes_sub_title",
        "no_sub_title",
        "liquidity",
        "liquidity_dollars",
        "yes_bid",
        "yes_ask",
        "yes_bid_dollars",
        "yes_ask_dollars",
        "no_bid",
        "no_ask",
        "no_bid_dollars",
        "no_ask_dollars",
    ]

    # 4) Pivot to wide: one row per event, two "market slots"
    wide = top2[["event_ticker", "m_rank"] + market_cols].pivot(
        index="event_ticker", columns="m_rank", values=market_cols
    )

    # 5) Flatten MultiIndex columns into names like "liquidity_m1", "liquidity_m2"
    wide.columns = [f"{col}_m{rank}" for col, rank in wide.columns]
    # wide = wide.reset_index()

    wide_reordered = wide.loc[
        :,
        [
            "title_m1",
            "yes_sub_title_m1",
            "yes_bid_m1",
            "yes_ask_m1",
            "no_bid_m1",
            "no_ask_m1",
            "yes_sub_title_m2",
            "yes_bid_m2",
            "yes_ask_m2",
            "no_bid_m2",
            "no_ask_m2",
        ],
    ]

    numeric_cols = [
        "yes_bid_m1",
        "yes_ask_m1",
        "no_bid_m1",
        "no_ask_m1",
        "yes_bid_m2",
        "yes_ask_m2",
        "no_bid_m2",
        "no_ask_m2",
    ]
    for nc in numeric_cols:
        wide_reordered[nc] = pd.to_numeric(wide_reordered[nc], errors="coerce")

    return wide_reordered


def add_spread_col(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add spread columns to the DataFrame.

    Parameters:
    df (pd.DataFrame): The DataFrame to modify.

    Returns:
    pd.DataFrame: The DataFrame with added spread columns.
    """
    df["yes_spread_m1"] = df["yes_ask_m1"] - df["yes_bid_m1"]

    mask = df["yes_ask_m1"].ne(0) & df["yes_ask_m1"].notna()

    df["yes_spread_m1_percentage"] = 0.0

    df.loc[mask, "yes_spread_m1_percentage"] = (
        df.loc[mask, "yes_spread_m1"] / df.loc[mask, "yes_ask_m1"]
    )

    df["midprice_m1"] = (df["yes_bid_m1"] + df["no_bid_m1"]) / 2

    df["Liquidity_Rating"] = np.where(df["yes_spread_m1"] <= 0.02, "High", "Low")

    return df


def write_to_google_sheet(
    df: pd.DataFrame, worksheet_key: str, worksheet_name: str, credentials_path: str
):
    """
    Write the DataFrame to a Google Sheet.

    Parameters:
    df (pd.DataFrame): The DataFrame to write.
    worksheet_key (str): The key of the Google Sheet.
    worksheet_name (str): The name of the worksheet within the Google Sheet.
    credentials_path (str): The path to the service account credentials JSON file.
    """
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    credentials = Credentials.from_service_account_file(credentials_path, scopes=scopes)

    gc = gspread.authorize(credentials)

    gauth = GoogleAuth()
    drive = GoogleDrive(gauth)

    gs = gc.open_by_key(worksheet_key)
    worksheet = gs.worksheet(worksheet_name)

    worksheet.clear()
    set_with_dataframe(
        worksheet=worksheet,
        dataframe=df,
        include_index=False,
        include_column_header=True,
        resize=True,
    )
