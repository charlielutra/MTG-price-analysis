import io
import pandas as pd
import requests

def get_cards():
    """
    Fetches the latest Scryfall bulk data and returns it as a DataFrame.
    """

    response = requests.get(
        "https://api.scryfall.com/bulk-data",
        headers={
            "User-Agent": "mtg-price-analysis/1.0",
            "Accept": "application/json",
        },
        timeout=30,
    )
    response.raise_for_status()
    bulk_data = response.json()

    entries = bulk_data.get("data") if isinstance(bulk_data, dict) else None
    if not isinstance(entries, list):
        raise ValueError("Scryfall /bulk-data response did not contain a 'data' list.")

    oracle_cards = next(
        (
            entry
            for entry in entries
            if isinstance(entry, dict) and entry.get("type") == "oracle_cards"
        ),
        None,
    )
    if oracle_cards is None:
        raise ValueError("Scryfall /bulk-data response did not include 'oracle_cards'.")

    download_uri = oracle_cards.get("jsonl_download_uri")
    if not download_uri:
        raise ValueError(
            "Scryfall 'oracle_cards' bulk data did not include 'jsonl_download_uri'."
        )

    download_response = requests.get(download_uri, timeout=120)
    download_response.raise_for_status()
    return pd.read_json(
        io.BytesIO(download_response.content),
        lines=True,
        compression="gzip",
    )
