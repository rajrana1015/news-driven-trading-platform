"""import os
import requests
import pandas as pd
# Fetch data from Finnhub
API_KEY = os.getenv("FINNHUB_API_KEY")
print(API_KEY)"""
import os
import requests
import pandas as pd

# Fetch data from Finnhub
API_KEY = os.getenv("FINNHUB_API_KEY")
url = "https://finnhub.io/api/v1/company-news"
params = {
    "symbol": "AAPL",
    "from": "2025-05-20",
    "to": "2025-05-27",
    "token": API_KEY
}
response = requests.get(url, params=params)
news_data = response.json()

# Normalize JSON into a pandas DataFrame
df = pd.json_normalize(news_data)

# Display DataFrame to user
import ace_tools as tools; tools.display_dataframe_to_user(name="AAPL Company News", dataframe=df)
