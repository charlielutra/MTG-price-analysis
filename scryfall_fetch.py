import requests
import pandas as pd

def get_cards():
    """
    Fetches the latest Scryfall bulk data and returns it as a DataFrame.
    """

    f=requests.get("https://api.scryfall.com/bulk-data")

    download_uri = f.json()["data"][0]["download_uri"]

    oracle_cards = requests.get(download_uri).json()

    cards = pd.DataFrame(oracle_cards)
    return cards
