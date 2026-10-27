import pandas as pd
import numpy as np

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

    "resource_id",
    "variation_of",
    "flavor_name",
    "printed_type_line",
    "printed_text",

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
    "image_updated_at",


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
           'commander', 'oathbreaker', 'paupercommander', 'duel', 'oldschool', 'premodern', 'predh',
           "competitivebrawl", "tlr"]
    engineered_data = data.copy()
    text_columns = engineered_data.loc[:,formats]
    bin_columns = (text_columns=="legal").astype("int")
    return engineered_data.drop(columns=formats).join(bin_columns)


def remove_legalities(data: pd.DataFrame):
    formats = ['standard', 'future', 'pioneer', 'modern', 'legacy', 'pauper', 'vintage', 'penny', 
           'commander', 'oathbreaker', 'paupercommander', 'duel', 'oldschool', 'premodern', 'predh', 
           "competitivebrawl", "tlr"]
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


def ordinate_rarities(data: pd.DataFrame, drop_orig=True) -> pd.DataFrame:
    """ 
    Ordinate the "rarity" field into numbers representative of the card's rarity.
    """

    """
    "rarity" can take the following values: common, uncommon, rare, mythic, special, or bonus.
    Turn these into numbers according to mtgwiki: a booster pack contains 11 C, 3 U, and one R/M.
    Therefore use the inverse of this as the rarity: C 1/11, U 1/3, R 8/7, M 8/1 = 8.
    (In Standard-only cards, special and bonus don't exist)
    """

    # integer_rarities_dict = {
    #     "common": 1, 
    #     "uncommon": 2,
    #     "rare": 3, 
    #     "mythic": 4, 
    #     "special": 5, 
    #     "bonus": 5, 
    # }

    ordinate_rarities_dict = {
        "common": 1/11, 
        "uncommon": 1/3,
        "rare": 8/7, 
        "mythic": 8., 
        # "special": , 
        # "bonus": , 
    }

    engineered_data = data.copy()
    ordinate_rarity = [ordinate_rarities_dict[x] for x in engineered_data["rarity"]]
    engineered_data["ordinate_rarity"] = ordinate_rarity

    if drop_orig:
        engineered_data = engineered_data.drop(columns="rarity")
        
    return engineered_data


def onehot_cardtypes(data: pd.DataFrame) -> pd.DataFrame:
    """
    "typeline" is a text field eg. Legendary Creature - Human Soldier.
    We want to: 
        Split text by spaces
        Discard  subtypes (ie. the dash and everything after it)
        One-hot encode each card supertype and type (Legendary, Creature, Artifact etc.)
    """

    engineered_data = data.copy()
    type_lines = engineered_data.type_line

    types = type_lines.copy()
    
    # types = engineered_data["type_line"].str.partition(" —")[0] # Discard everything after the first dash
    types = engineered_data["type_line"].str.split(r"\s*(?://|—|-)\s*", n=1).str[0] # More complicated version that also handles DFCs
    types = types.str.split() # Split by spaces

    # engineered_data["types"] = types

    """
    Older version:
    contains_dash = types.str.contains("—") # Create new boolean series, contains_dash: = 1 if "-" present (ie. if there are subtypes) ; = 0 otherwise
    types[contains_dash] = type_lines[contains_dash].str.rpartition(" —")[0] # For lines where contains_dash==1: Cut off the string after " -" is seen (ie. remove the subtypes)
    # Now types contains eg. "Legendary Creature", "Instant", "Creature", "Artifact Creature" etc.

    types = types.str.split(" ") # Turn this into a list split by spaces: -> each entry is ["Legendary","Creature"] or ["Instant"] or ... etc.
    """
    
    # Now extract all unique values:
    all_categories = set(cat for sublist in types for cat in sublist)
    print(all_categories)

    # for category in all_categories:
        # engineered_data[category] = engineered_data.type_line.apply(lambda x: 1 if category in x else 0)

    for category in all_categories:
        engineered_data[category] = types.apply(
            lambda x: int(category in x)
        )

    # Drop the original 'type_line' column
    engineered_data = engineered_data.drop(columns="type_line")

    return engineered_data


def convert_all_to_numbers(data = pd.DataFrame) -> pd.DataFrame:

    engineered_data = data.copy()

    bool_vars = ["foil","booster", "game_changer", "variation"
                ]
                # "standard","future","pioneer","modern","legacy","pauper","vintage",
                # "penny","commander","oathbreaker","paupercommander","duel",
                # "oldschool","premodern","predh"

    for column_name in bool_vars:
        engineered_data[column_name] = (
            engineered_data[column_name].astype("Int64")
        )

    numeric_vars = ["eur","cmc","power","toughness"]

    for column_name in numeric_vars:
        new_column = pd.to_numeric(engineered_data[column_name], errors="coerce")
        engineered_data = engineered_data.drop(columns=column_name).join(new_column)

    return engineered_data


#def add_desired_features(data = pd.DataFrame) -> pd.DataFrame:
    #engineered_data = data.copy()

    # Num. colours:

    #colors = 

def create_log_eur(data = pd.DataFrame, drop_orig=True) -> pd.DataFrame:
    """
    Replace price in Euros ("eur") to its log10 value ("log_eur") - this creates a much more learnable distribution.
    """
    engineered_data = data.copy()

    log_eur = np.log10(engineered_data["eur"])
    # print(log_eur)
    engineered_data["log_eur"] = log_eur

    if drop_orig:
        engineered_data = engineered_data.drop(columns="eur")

    return engineered_data




"""
More preprocessing to do: 
Onehot encode set_type: 
{'commander',
 'core',
 'draft_innovation',
 'duel_deck',
 'eternal',
 'expansion',
 'masters',
 'promo',
 'starter'}

...At some point I really do need to move to default_cards, not oracle cards.

Feature engineering I'd like to do:

BIG ONE: ADD SCRYFALL TAGS. 

"""

