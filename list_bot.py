import os
import discord
import re
import time
import asyncio
import json
from discord.ui import View, Button, button
from discord.enums import ButtonStyle
from collections import Counter
import aiohttp.web

# --- CONFIGURATION ---
BOT_TOKEN = os.environ.get("BOT_TOKEN")
DATA_FILE = "data.json"

TARGET_BOT_ID_FOR_AUTO_UPDATES = 1379160458698690691
YOUR_USER_ID = 1280968897654292490
ADMIN_USER_IDS = [
    YOUR_USER_ID, 1020489591800729610, 1188633780261507102,
    934249510127935529, 504748495933145103, 1095468010564767796
]

RESTART_COMMAND = "list.bot restart"
MANUAL_ADD_COMMAND_PREFIX = "list.bot add"
CLOSE_LISTS_COMMAND = "list.bot close"
ANNOUNCE_COMMAND = "list.bot announce"
DELETE_COMMAND_PREFIX = "list.bot delete"
SAY_COMMAND_PREFIX = "list.bot say"

AUTO_UPDATE_MESSAGE_REGEX = re.compile(
    r"The Unique\s+([a-zA-Z0-9_\-\s'.]+?)\s+has been forged by\s+([a-zA-Z0-9_\-\s'.]+?)(?:!|$|\s+@)",
    re.IGNORECASE
)

INTERACTIVE_LIST_TARGET_CHANNEL_IDS = [
    1379541947189821460, 1355614867490345192, 1378070555638628383, 1378850404565127229, 1385453089338818651
]

EPHEMERAL_REQUEST_LOG_CHANNEL_ID = 1385094756912205984

channel_list_states = {}
DEFAULT_PERSISTENT_SORT_KEY = "sort_config_item"
MAX_RECENT_ITEMS_TO_SHOW = 30
MAX_MESSAGE_LENGTH = 1900

INITIAL_DATA_LIST = [
  ["Corruption", "Pehiley", 6],
  ["Dandelion", "FREEDOM08", 6],
  ["Mysterious Stick", "BaiLin2", 6],
  ["Web", "Manfred", 6],
  ["Salt", "tarou9n", 6],
  ["Fang", "hqyx", 8],
  ["Orange", "Solar", 9],
  ["Mysterious Relic", "gujiga", 6],
  ["Dice", "Pehiley", 6],
  ["Card", "hqyx", 6],
  ["Magic Stick", "Pehiley", 6],
  ["Heavy", "asds", 6],
  ["Iris", "Craft_Super", 9],
  ["Beetle Egg", "hqyx", 11],
  ["Bone", "HUDUMOC", 10],
  ["Mana Orb", "BONER_ALERT", 6],
  ["Poker Chip", "PlayFlorrio", 12],
  ["Pearl", "gachanchall", 6],
  ["Missile", "Missile", 8],
  ["Dark Mark", "Craft_Super", 6],
  ["Magic Leaf", "Manfred", 6],
  ["Bubble", "Recon", 6],
  ["Rice", "Manfred", 6],
  ["Peas", "WTJ", 6],
  ["Yucca", "Pehiley", 12],
  ["Leaf", "Etin", 7],
  ["Mecha Missile", "Mario", 6],
  ["Magic Missile", "Pehiley", 6],
  ["Sand", "Zorat", 13],
  ["Magic Cactus", "Pehiley", 6],
  ["Starfish", "CarrotJuice", 7],
  ["Air", "gachanchall", 11],
  ["Cactus", "tianleshan", 7],
  ["Corn", "-Sam8375", 6],
  ["Ant Egg", "tianleshan", 11],
  ["MjÃ¶lnir", "Manfred", 14],
  ["Wax", "ProH", 6],
  ["Coin", "givemeygg", 26],
  ["Totem", "BONER_ALERT", 6],
  ["Jelly", "tarou9n", 7],
  ["Pincer", "Avril", 8],
  ["Triangle", "gujiga", 8],
  ["Antennae", "Manfred", 9],
  ["Privet Berry", "Abstract", 8],
  ["Battery", "oar", 7],
  ["Mecha Antennae", "Mr_Alex", 6],
  ["Faster", "-Sam8375", 9],
  ["Lightning", "Wolxs", 6],
  ["Claw", "gainer", 7],
  ["Light", "Bibi", 7],
  ["Clover", "-Sam8375", 27],
  ["Pharaoh's Crown", "FuGang", 42],
  ["Poo", "gainer", 15],
  ["Wing", "gainer", 25],
  ["Talisman of Evasion", "gainer", 9]
  ["Glass", "-Sam8375", 22],
  ["Light Bulb", "BaiLin2", 8],
]

data_list = []

# Helper function for the new 'Owner' sort
def sort_by_owner_tally(data):
    if not data:
        return []
    name_counts = Counter(row[1].lower() for row in data)
    def custom_sort_key(row):
        name = row[1].lower()
        cost = int(row[2])
        return (-name_counts[name], name, -cost)
    return sorted(data, key=custom_sort_key)


SORT_CONFIGS = {
    "sort_config_item": {
        "label": "by Item", "button_label": "Sort: Item",
        "sort_lambda": lambda data: sorted(data, key=lambda x: (x[0].lower(), x[1].lower())),
        "column_order_indices": [0, 1, 2], "headers": ["Item", "Name", "Cost"]
    },
    "sort_config_name": {
        "label": "by Name", "button_label": "Sort: Name",
        "sort_lambda": lambda data: sorted(data, key=lambda x: (x[1].lower(), x[0].lower())),
        "column_order_indices": [1, 0, 2], "headers": ["Name", "Item", "Cost"]
    },
    "sort_config_cost": {
        "label": "by Cost", "button_label": "Sort: Cost",
        "sort_lambda": lambda data: sorted(data, key=lambda x: (int(x[2]), x[0].lower())),
        "column_order_indices": [2, 0, 1], "headers": ["Cost", "Item", "Name"]
    },
    "sort_config_recent": {
        "label": "by Recent", "button_label": "Sort: Recent",
        "sort_lambda": lambda data: data[-MAX_RECENT_ITEMS_TO_SHOW:],
        "column_order_indices": [0, 1, 2], "headers": ["Item", "Name", "Cost (Recent)"]
    },
    "sort_config_owner": {
        "label": "by Owner Count", "button_label": "Sort: Owner",
        "sort_lambda": sort_by_owner_tally,
        "column_order_indices": [1, 0, 2], "headers": ["Name", "Item", "Cost"]
    }
}

UPDATE_NOTIFICATION_CONFIG = [
    {
        "channel_id": 1349793261908262942,
        "message_format": "{item_val} - {name_val} - {cost_val}\n{role_ping}",
        "role_id_to_ping": 1357477336282566766
    },
    {
        "channel_id": 1378070194148217012,
        "message_format": "{name_val} - {item_val} - {cost_val}\n{role_ping}",
        "role_id_to_ping": 1378071231252926514
    },
    {
        "channel_id": 1383418429460971520,
        "message_format": "{name_val} - {item_val} - {cost_val}\n{role_ping}",
        "role_id_to_ping": 1271018797238583337
    },
]
# --- END CONFIGURATION ---

intents = discord.Intents.default()
intents.messages = True
intents.message_content = True
intents.guilds = True
client = discord.Client(intents=intents)

last_updated_item_details = {"item_val": None, "name_val": None, "cost_val": None}
view_message_tracker = {}

# NEW: Functions for data persistence
def load_data_list():
    global data_list
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r") as f:
                loaded_data = json.load(f)
                if isinstance(loaded_data, list):
                    data_list = loaded_data
                    print(f"Successfully loaded {len(data_list)} items from {DATA_FILE}")
                else:
                    print(f"ERROR: {DATA_FILE} is corrupted or invalid. Initializing with hardcoded data.")
                    data_list = list(INITIAL_DATA_LIST)
        except (IOError, json.JSONDecodeError) as e:
            print(f"ERROR loading data from {DATA_FILE}: {e}. Initializing with hardcoded data.")
            data_list = list(INITIAL_DATA_LIST)
    else:
        print(f"Data file {DATA_FILE} not found. Initializing with hardcoded data.")
        data_list = list(INITIAL_DATA_LIST)
    for row in data_list:
      if len(row) > 2:
        row[2] = str(row[2])

def save_data_list():
    global data_list
    try:
        temp_data_file = DATA_FILE + ".tmp"
        with open(temp_data_file, "w") as f:
            json.dump(data_list, f, indent=4)
        os.replace(temp_data_file, DATA_FILE)
        print(f"Successfully saved {len(data_list)} items to {DATA_FILE}.")
    except (IOError, TypeError) as e:
        print(f"ERROR: Failed to save data to {DATA_FILE}: {e}")

class EphemeralListView(View):
    def __init__(self, initial_sort_key: str, timeout=300):
        super().__init__(timeout=timeout)
        self.current_sort_key = initial_sort_key
        self._update_button_states()

    def _update_button_states(self):
        for child in self.children:
            if isinstance(child, Button):
                if child.custom_id == f"ephem_btn_{self.current_sort_key}":
                    child.disabled = True
                    child.style = ButtonStyle.success
                else:
                    child.disabled = False
                    child.style = ButtonStyle.secondary

    async def _update_ephemeral_message(self, interaction: discord.Interaction, new_sort_key: str):
        self.current_sort_key = new_sort_key
        self._update_button_states()
        full_content_parts = format_sorted_list_content(new_sort_key, is_ephemeral=True)
        try:
            content_to_send = full_content_parts[0] if isinstance(full_content_parts, list) else full_content_parts
            await interaction.response.edit_message(content=content_to_send, view=self)
        except discord.HTTPException as e:
            print(f"Failed to edit ephemeral message for {interaction.user.name}: {e}")

    @button(label=SORT_CONFIGS["sort_config_item"]["button_label"], style=ButtonStyle.secondary, custom_id="ephem_btn_sort_config_item")
    async def sort_item_btn_e(self, i: discord.Interaction, b: Button):
        await self._update_ephemeral_message(i, "sort_config_item")

    @button(label=SORT_CONFIGS["sort_config_name"]["button_label"], style=ButtonStyle.secondary, custom_id="ephem_btn_sort_config_name")
    async def sort_name_btn_e(self, i: discord.Interaction, b: Button):
        await self._update_ephemeral_message(i, "sort_config_name")

    @button(label=SORT_CONFIGS["sort_config_cost"]["button_label"], style=ButtonStyle.secondary, custom_id="ephem_btn_sort_config_cost")
    async def sort_cost_btn_e(self, i: discord.Interaction, b: Button):
        await self._update_ephemeral_message(i, "sort_config_cost")

    @button(label=SORT_CONFIGS["sort_config_recent"]["button_label"], style=ButtonStyle.secondary, custom_id="ephem_btn_sort_recent")
    async def sort_recent_btn_e(self, i: discord.Interaction, b: Button):
        await self._update_ephemeral_message(i, "sort_config_recent")

    @button(label=SORT_CONFIGS["sort_config_owner"]["button_label"], style=ButtonStyle.secondary, custom_