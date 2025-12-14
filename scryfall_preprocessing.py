import pandas as pd
import requests
import json


def get_cards():
    """
    Fetches the latest Scryfall bulk data and returns it as a DataFrame.
    """

    f=requests.get("https://api.scryfall.com/bulk-data")

    download_uri = f.json()["data"][0]["download_uri"]

    oracle_cards = requests.get(download_uri).json()

    cards = pd.DataFrame(oracle_cards)
    return cards


def expand_dict_features(data: pd.DataFrame) -> pd.DataFrame:
    engineered_data = data.copy()
    legalities_df = pd.DataFrame(engineered_data["legalities"].tolist())
    prices_df = pd.DataFrame(engineered_data["prices"].tolist())
    engineered_data = engineered_data.drop(columns=["legalities","prices"])
    engineered_data = engineered_data.join(legalities_df)
    engineered_data = engineered_data.join(prices_df)
    return engineered_data


def drop_junk_features(data: pd.DataFrame) -> pd.DataFrame:
    junk_columns = [
    # These provide no info - the entire dataset has "card", "en", and null respectively
    "object",
    "lang",
    "attraction_lights",


    # Others that need to gtfo:
    "games",

    # Various IDs
    "id",
    "set_id",
    "mtgo_id",
    "oracle_id", 
    "arena_id", 
    "multiverse_ids", 
    "tcgplayer_id", 
    "cardmarket_id",
    "artist_ids",
    "illustration_id", 
    "tcgplayer_etched_id",
    "mtgo_foil_id",


    # Various URIs 
    "uri", 
    "scryfall_uri", 
    "image_status",
    "image_uris",
    "set_name",
    "set_uri",
    "set_search_uri",
    "scryfall_set_uri",
    "rulings_uri",
    "prints_search_uri",
    "related_uris",
    "purchase_uris",
    "card_back_id",


    # Random text-based stuff the algo won't understand
    "oracle_text",
    "flavor_text",
    

    # Misc other stuff irrelevant to data analysis
    "preview",
    "highres_image", 
    "all_parts",

    # Stuff with <1000 datapoints
    "card_faces",
    "loyalty",          # Tempted to keep this one, but I'll be consistent for now
    #"flavor_name",     #apparently this is gone
    "color_indicator",
    "life_modifier",
    "hand_modifier",
    "content_warning",

    # Misc other stuff I don't care about enough to want to bother with (at least for now)
    #"lang",
    "artist",
    "frame",
    "frame_effects",
    "border_color",
    "reserved",
    "promo",
    "oversized",
    "finishes",
    #"foil",
    "nonfoil",
    #"set_type",
    #"oversized",
    "collector_number",
    "full_art",
    "textless",
    #"booster",
    "story_spotlight",
    "digital",

    "reprint",

    "promo_types",
    "produced_mana",
    "defense",

    # Maybe come back to these at some point
    #"colors",
    #"color_identity",



    # Prices other than *EUR NOT TIX (I don't want the program to have these)
    "usd",
    "usd_foil",
    "usd_etched",
    "eur_foil",
    "tix",
    
    # Arena-specific format legalities (irrelevant to MTGO)
    "brawl",
    "historic",
    "standardbrawl",
    "alchemy",
    "gladiator",
    "timeless",

    ]
    engineered_data = data.copy()
    engineered_data = engineered_data.drop(columns=junk_columns)
    return engineered_data


def drop_more_features(data: pd.DataFrame) -> pd.DataFrame:
    more_columns = [
        # Other stuff that doesn't matter
        "layout",
        #"mana_cost",
        "reserved",
        "foil",
        "nonfoil",
        "oversized",
        "promo",
        "reprint",
        "variation",
        "digital",
        "watermark",
        
        "border_color",
        "frame",
        "frame_effects",
        "security_stamp",
        "full_art",
        "textless",
        "booster",
        "story_spotlight",
    ]
    engineered_data = data.copy()
    engineered_data = engineered_data.drop(columns=more_columns)
    return engineered_data

def clean_dataset(data: pd.DataFrame) -> pd.DataFrame:
    # I want to only keep cards that are relevant to actual play of the game, on MTGO
    # Therefore a) remove any card not on MTGO (eg. alchemy-only cards, Fallout cards etc)
    # ... and also need to remove cards which are on mtgo but inexplicably don't have a tix value stored! 

    # b) remove anything "banned" in Vintage (ie. ante, conspiracy, dexterity, subgame cards)
    # ...actually those cards aren't on MTGO anyway! -> unnecessary
    
    engineered_data = data.copy()
    #engineered_data = engineered_data.dropna()
    engineered_data = engineered_data[engineered_data.tix.notna()]
    engineered_data = engineered_data[engineered_data.vintage != "not_legal"]
    engineered_data = engineered_data[engineered_data.vintage != "banned"]
    #engineered_data = engineered_data[engineered_data.colors.notna()] # Temporary fix, removes all DFCs, replace and impute them at some point
    
    print("Legacy - should be using sf.standard_only instead!")
    return engineered_data


def standard_only(data: pd.DataFrame) -> pd.DataFrame:
    # Remove all non-Standard-legal cards
    engineered_data = data.copy()
    engineered_data = engineered_data[engineered_data.standard != "not_legal"]
    engineered_data = engineered_data[engineered_data.standard != "banned"]
    engineered_data = engineered_data[engineered_data.eur.notna()]
    return engineered_data.reset_index(drop=True)


def engineer_legalities(data: pd.DataFrame):
    # Legacy - should be replaced by sf.remove_legalities
    print("Legacy - should be replaced by sf.remove_legalities")

    formats = ['standard', 'future', 'pioneer', 'modern', 'legacy', 'pauper', 'vintage', 'penny', 
           'commander', 'oathbreaker', 'paupercommander', 'duel', 'oldschool', 'premodern', 'predh',]
    engineered_data = data.copy()
    text_columns = engineered_data.loc[:,formats]
    bin_columns = (text_columns=="legal").astype("int")
    return engineered_data.drop(columns=formats).join(bin_columns)


def remove_legalities(data: pd.DataFrame):
    formats = ['standard', 'future', 'pioneer', 'modern', 'legacy', 'pauper', 'vintage', 'penny', 
           'commander', 'oathbreaker', 'paupercommander', 'duel', 'oldschool', 'premodern', 'predh',]
    engineered_data = data.copy()

    return engineered_data.drop(columns=formats)


from datetime import date
def card_age(data: pd.DataFrame) -> pd.DataFrame:
    # This is bugged
    # Also I don't wanna use this any more anyway!

    print("Why are using sf.card_age??? Bugged, AND I don't think I want it any more")

    engineered_data = data.copy()
    release_date_obj = engineered_data.released_at
    release_date = pd.to_datetime(release_date_obj)
    days_ago = (todays_date - release_date[:])
    int_days_ago = days_ago.map(datetime.datetime.date)
    #return engineered_data.drop(released_at).join(release_date)

    return int_days_ago


def abnormal_layout(data: pd.DataFrame) -> pd.DataFrame:
    # This is bugged

    "sf.abnormal_layout suspected to be bugged"

    engineered_data = data.copy()
    layout = engineered_data.layout
    is_abnormal = (layout[:]!="normal").astype("int")
    return engineered_data.drop(layout).join(is_abnormal)


def ordinate_rarities(data: pd.DataFrame) -> pd.DataFrame:

    ordinate_rarities_dict = {
        "common": 1, 
        "uncommon": 2,
        "rare": 3, 
        "mythic": 4, 
        "special": 5, 
        "bonus": 5, 
    }
    engineered_data = data.copy()
    ordinate_rarity = [ordinate_rarities_dict[x] for x in engineered_data["rarity"]]
    engineered_data = engineered_data.drop(columns="rarity")
    engineered_data["ordinate_rarity"] = ordinate_rarity
    return engineered_data


def cut_subtypes(data: pd.DataFrame) -> pd.DataFrame:
    engineered_data = data.copy()
    type_lines = engineered_data.type_line

    types = type_lines.copy()
    contains_dash = types.str.contains("—") # Create new boolean series, contains_dash: = 1 if "-" present (ie. if there are subtypes) ; = 0 otherwise
    types[contains_dash] = type_lines[contains_dash].str.rpartition(" —")[0] # For lines where contains_dash==1: Cut off the string after " -" is seen (ie. remove the subtypes)
    # Now types contains eg. "Legendary Creature", "Instant", "Creature", "Artifact Creature" etc.

    types = types.str.split(" ") # Turn this into a list split by spaces: -> each entry is ["Legendary","Creature"] or ["Instant"] or ... etc.

    engineered_data = engineered_data.drop(columns="type_line")
    engineered_data["types"] = types

    return engineered_data


def convert_variable_to_numbers(data, column_name, data_type):
    engineered_data = data.copy()

    new_column = engineered_data[column_name].notnull().astype(data_type)
    engineered_data = engineered_data.drop(columns=column_name).join(new_column)
    return engineered_data


def convert_all_to_numbers(data = pd.DataFrame) -> pd.DataFrame:
    # This is currently bugged.
    # Problem is about null values in columns. 
    # eg. some cards don't have a listed cmc (?)
    # So running convert_variable_to_numbers doesn't work for this. 
    # I tried using notna() in convert_variable_to_numbers:
        # new_column = engineered_data[column_name].NOTNA().astype(data_type)
    # But then I think this also removed all the False entries from the bool ones??
    # Come back to this


    engineered_data = data.copy()

    #engineered_data = convert_variable_to_numbers(engineered_data, "eur", "float64")
    #engineered_data = convert_variable_to_numbers(engineered_data, "cmc", "int")


    bool_vars = ["foil","booster"
                ]
                # "standard","future","pioneer","modern","legacy","pauper","vintage",
                # "penny","commander","oathbreaker","paupercommander","duel",
                # "oldschool","premodern","predh"
 
    for column_name in bool_vars:
        engineered_data = convert_variable_to_numbers(engineered_data, column_name, "int")


    num_vars = ["eur","cmc","power","toughness"]

    for column_name in num_vars:
        engineered_data = convert_variable_to_numbers(engineered_data, column_name, "float64")

    return engineered_data



#def add_desired_features(data = pd.DataFrame) -> pd.DataFrame:
    #engineered_data = data.copy()

    # Num. colours:

    #colors = 






