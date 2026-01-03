import requests
import pandas as pd
import utils

if __name__ == "__main__":
    markets_url = "https://api.elections.kalshi.com/trade-api/v2/markets"
    events_url = "https://api.elections.kalshi.com/trade-api/v2/events"

    data = utils.get_data_from_api(events_url)

    top_2 = utils.get_top_2(data)

    final_df = utils.add_spread_col(top_2)

    utils.write_to_google_sheet(
        df=final_df,
        worksheet_key="1zL-RvZU9tXp67ZBgw10Ls1hvN4IbP1Tqr0-o1yA4N2g",
        worksheet_name="Sheet1",
        credentials_path="data/certain-drake-475803-v9-e942e89acc06.json",
    )

