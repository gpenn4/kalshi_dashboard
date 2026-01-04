import os
import utils

SERVICE_ACCOUNT_FILE = os.getenv(
    "GOOGLE_APPLICATION_CREDENTIALS", "data/service_account.json"
)

MARKETS_URL = "https://api.elections.kalshi.com/trade-api/v2/markets"
EVENTS_URL = "https://api.elections.kalshi.com/trade-api/v2/events"

if __name__ == "__main__":
    data = utils.get_data_from_api(EVENTS_URL)

    top_2 = utils.get_top_2(data)

    final_df = utils.add_spread_col(top_2)

    utils.write_to_google_sheet(
        df=final_df,
        worksheet_key="1zL-RvZU9tXp67ZBgw10Ls1hvN4IbP1Tqr0-o1yA4N2g",
        worksheet_name="Sheet1",
        credentials_path=SERVICE_ACCOUNT_FILE,
    )
