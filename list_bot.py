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
RAW_LIST_COMMAND = "list.bot raw"
MESSAGE_COMMAND_PREFIX = "list.bot message" # <- GLOBAL BROADCAST COMMAND

AUTO_UPDATE_MESSAGE_REGEX = re.compile(
    r"The Unique\s+([a-zA-Z0-9_\-\s'.]+?)\s+has been forged by\s+([a-zA-Z0-9_\-\s'.]+?)(?:!|$|\s+@)",
    re.IGNORECASE
)

INTERACTIVE_LIST_TARGET_CHANNEL_IDS = [
    1379541947189821460, 1355614867490345192, 1378070555638628383, 1378850404565127229, 1385453089338818651
]

EPHEMERAL_REQUEST_LOG_CHANNEL_ID = 1385094756912205984

# GLOBAL VARIABLES FOR PERSISTENT DATA
data_list = []
channel_list_states = {} # This state data will now be saved to the file
DEFAULT_PERSISTENT_SORT_KEY = "sort_config_item"

SECONDS_IN_WEEK = 604800
MAX_MESSAGE_LENGTH = 1900

INITIAL_DATA_LIST = []

# --- END CONFIGURATION (except for initial data) ---


intents = discord.Intents.default()
intents.messages = True
intents.message_content = True
intents.guilds = True
client = discord.Client(intents=intents)

last_updated_item_details = {"item_val": None, "name_val": None, "cost_val": None}
view_message_tracker = {}

# Helper function for the new 'Owner' sort (FIXED for stability)
def sort_by_owner_tally(data):
    if not data:
        return []
    name_counts = Counter(row[1].lower() for row in data)
    def custom_sort_key(row):
        name = row[1].lower()
        # Safely convert cost to int, defaulting to 0
        try:
            cost = int(row[2])
        except (IndexError, ValueError):
            cost = 0
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
        # Use safe check for isdigit() to prevent ValueError crash on non-numeric cost
        "sort_lambda": lambda data: sorted(data, key=lambda x: (int(x[2]) if str(x[2]).isdigit() else 0, x[0].lower())),
        "column_order_indices": [2, 0, 1], "headers": ["Cost", "Item", "Name"]
    },
    "sort_config_recent": {
        # CHANGED: Label reflects the new 7-day filter
        "label": "by Recent (Last 7 Days)", "button_label": "Sort: Recent",
        # CHANGED: The lambda filters items where the timestamp (index 3) is within the last 7 days, and reverses the result to show newest first.
        "sort_lambda": lambda data: [
            row for row in data
            if len(row) > 3 and row[3] >= (time.time() - SECONDS_IN_WEEK)
        ][::-1],
        "column_order_indices": [0, 1, 2], "headers": ["Item", "Name", "Cost (7 Days)"]
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


# Functions for data persistence

def load_data_list():
    """
    Loads item list data AND channel state data from the JSON file.
    """
    global data_list
    global channel_list_states # ADDED
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r") as f:
                loaded_data = json.load(f)

                # --- Load Item List Data ---
                if isinstance(loaded_data, dict) and "list_data" in loaded_data and isinstance(loaded_data["list_data"], list):
                    data_list = loaded_data["list_data"]
                    print(f"Successfully loaded {len(data_list)} items from 'list_data' in {DATA_FILE}")
                elif isinstance(loaded_data, list): # Backward compatibility for old format
                    data_list = loaded_data
                    print(f"Successfully loaded {len(data_list)} items from old format in {DATA_FILE}")
                else:
                    print(f"ERROR: 'list_data' in {DATA_FILE} is corrupted or invalid. Initializing with hardcoded data.")
                    data_list = list(INITIAL_DATA_LIST)

                # --- Load State Data (New Permanent State) ---
                if isinstance(loaded_data, dict) and "state_data" in loaded_data and isinstance(loaded_data["state_data"], dict):
                    # Ensure keys are integers if they were stored as strings
                    raw_states = loaded_data["state_data"].get("channel_list_states", {})
                    channel_list_states = {int(k): v for k, v in raw_states.items() if str(k).isdigit()}

                    print(f"Successfully loaded state data for {len(channel_list_states)} channels.")
                else:
                    print(f"WARNING: No 'state_data' found in {DATA_FILE}. Initializing channel states to empty.")
                    channel_list_states = {}

        except (IOError, json.JSONDecodeError) as e:
            print(f"ERROR loading data from {DATA_FILE}: {e}. Initializing with hardcoded data and empty states.")
            data_list = list(INITIAL_DATA_LIST)
            channel_list_states = {} # Ensure it's clean on error
    else:
        print(f"Data file {DATA_FILE} not found. Initializing with hardcoded data and empty states.")
        data_list = list(INITIAL_DATA_LIST)
        channel_list_states = {}

    # NEW LOGIC: Ensure all rows have a timestamp (index 3) for the new Recent filter
    for row in data_list:
        if len(row) > 2:
            # Ensure cost is stored as a string
            row[2] = str(row[2])
        # If old data is loaded (length < 4), append a 0 timestamp (which fails the 7-day filter)
        if len(row) < 4:
            row.append(0)

def save_data_list():
    """
    Saves item list data AND channel state data to the JSON file.
    """
    global data_list
    global channel_list_states # ADDED

    data_to_save = {
        "list_data": data_list,
        "state_data": {
            "channel_list_states": channel_list_states
        }
    }

    try:
        temp_data_file = DATA_FILE + ".tmp"
        with open(temp_data_file, "w") as f:
            # Save the combined dictionary
            json.dump(data_to_save, f, indent=4)
        os.replace(temp_data_file, DATA_FILE)
        print(f"Successfully saved {len(data_list)} items and {len(channel_list_states)} channel states to {DATA_FILE}.")
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

    @button(label=SORT_CONFIGS["sort_config_owner"]["button_label"], style=ButtonStyle.secondary, custom_id="ephem_btn_sort_config_owner")
    async def sort_owner_btn_e(self, i: discord.Interaction, b: Button):
        await self._update_ephemeral_message(i, "sort_config_owner")

    async def on_timeout(self):
        print(f"EphemeralListView for sort {self.current_sort_key} timed out.")


class PersistentListPromptView(View):
    def __init__(self, target_channel_id: int, timeout=None):
        super().__init__(timeout=timeout)
        self.target_channel_id = target_channel_id

    async def _send_ephemeral_sorted_list(self, interaction: discord.Interaction, sort_key: str):
        print(f"User {interaction.user.name} (ID: {interaction.user.id}) requested sorted list ('{sort_key}') ephemerally in channel <#{interaction.channel_id}>.")
        if EPHEMERAL_REQUEST_LOG_CHANNEL_ID and EPHEMERAL_REQUEST_LOG_CHANNEL_ID != 0:
            log_channel = client.get_channel(EPHEMERAL_REQUEST_LOG_CHANNEL_ID)
            if log_channel:
                try:
                    log_message = (
                        f"📋 Ephemeral list generated:\n"
                        f"▫️ **User:** {interaction.user.mention} (`{interaction.user.name}#{interaction.user.discriminator}` - ID: `{interaction.user.id}`)\n"
                        f"▫️ **Requested Sort:** `{SORT_CONFIGS[sort_key]['label']}` (Key: `{sort_key}`)\n"
                        f"▫️ **Interaction Channel:** <#{interaction.channel_id}> (Prompt in channel ID: `{self.target_channel_id}`)"
                    )
                    await log_channel.send(log_message)
                except discord.Forbidden:
                    print(f"Log Error: No permission to send messages in log channel {EPHEHERMAL_REQUEST_LOG_CHANNEL_ID}.")
                except Exception as e:
                    print(f"Log Error: Failed to send log to channel {EPHEMERAL_REQUEST_LOG_CHANNEL_ID}: {e}")
            else:
                print(f"Log Error: Ephemeral request log channel ID {EPHEMERAL_REQUEST_LOG_CHANNEL_ID} not found.")

        full_content_parts = format_sorted_list_content(sort_key, is_ephemeral=True)
        ephemeral_view = EphemeralListView(initial_sort_key=sort_key)
        try:
            content_to_send = full_content_parts[0] if isinstance(full_content_parts, list) else full_content_parts
            await interaction.response.send_message(content=content_to_send, view=ephemeral_view, ephemeral=True)
        except Exception as e:
            print(f"Failed to send ephemeral sorted list to {interaction.user.name}: {e}")
            try:
                await interaction.followup.send("Sorry, I couldn't generate your list view at this time.", ephemeral=True)
            except:
                pass

    @button(label=SORT_CONFIGS["sort_config_item"]["button_label"], style=ButtonStyle.primary, custom_id="persist_btn_sort_item")
    async def sort_item_btn_p(self, i: discord.Interaction, b: Button):
        await self._send_ephemeral_sorted_list(i, "sort_config_item")

    @button(label=SORT_CONFIGS["sort_config_name"]["button_label"], style=ButtonStyle.primary, custom_id="persist_btn_sort_name")
    async def sort_name_btn_p(self, i: discord.Interaction, b: Button):
        await self._send_ephemeral_sorted_list(i, "sort_config_name")

    @button(label=SORT_CONFIGS["sort_config_cost"]["button_label"], style=ButtonStyle.primary, custom_id="persist_btn_sort_cost")
    async def sort_cost_btn_p(self, i: discord.Interaction, b: Button):
        await self._send_ephemeral_sorted_list(i, "sort_config_cost")

    @button(label=SORT_CONFIGS["sort_config_recent"]["button_label"], style=ButtonStyle.primary, custom_id="persist_btn_sort_recent")
    async def sort_recent_btn_p(self, i: discord.Interaction, b: Button):
        await self._send_ephemeral_sorted_list(i, "sort_config_recent")

    @button(label=SORT_CONFIGS["sort_config_owner"]["button_label"], style=ButtonStyle.primary, custom_id="persist_btn_sort_owner")
    async def sort_owner_btn_p(self, i: discord.Interaction, b: Button):
        await self._send_ephemeral_sorted_list(i, "sort_config_owner")

    async def on_timeout(self):
        print(f"PersistentListPromptView for channel {self.target_channel_id} supposedly timed out.")

def _update_last_changed_details(item_val, name_val, cost_val):
    global last_updated_item_details
    last_updated_item_details = {"item_val": item_val, "name_val": name_val, "cost_val": cost_val}

def update_data_for_auto(item_val, name_val):
    global data_list
    found_idx = -1
    final_cost = "1"
    for i, row in enumerate(data_list):
        if row[0].lower() == item_val.lower():
            found_idx = i
            break

    current_time_epoch = time.time()

    if found_idx != -1:
        existing_row = data_list.pop(found_idx)
        existing_row[1] = name_val
        try:
            final_cost = str(int(existing_row[2]) + 1)
        except ValueError:
            final_cost = "1"
        existing_row[2] = final_cost
        # NEW: Update timestamp
        existing_row[3] = current_time_epoch
        data_list.append(existing_row)
    else:
        # NEW: Add timestamp
        new_row = [item_val, name_val, final_cost, current_time_epoch]
        data_list.append(new_row)

    _update_last_changed_details(item_val, name_val, final_cost)
    save_data_list()
    print(f"Data update: Item='{item_val}',Name='{name_val}',NewCost='{final_cost}' (Auto)")
    return final_cost

def format_list_for_display(data, col_indices, headers):
    if not data:
        return []

    # Calculate max widths for each column based on headers and data
    widths = [len(h) for h in headers]
    for r in data:
        # NOTE: data rows now have 4 elements (Item, Name, Cost, Timestamp)
        # We only display the first 3 elements defined by col_indices.
        disp_row = [str(r[i]) for i in col_indices]
        for i, val in enumerate(disp_row):
            widths[i] = max(widths[i], len(val))

    # Calculate padding for each column.
    padding = [2] * len(widths)
    # The total line length is the sum of widths and padding
    total_line_length = sum(widths) + sum(padding) - padding[-1]

    # Dynamically adjust formatting if the line length is too large
    if total_line_length > MAX_MESSAGE_LENGTH - 50:
        padding = [1] * len(widths)
        total_line_length = sum(widths) + sum(padding) - padding[-1]

    # Create the header line
    header_line = " ".join(f"{headers[i]:<{widths[i]}}" for i in range(len(headers)))

    message_parts = []
    current_part_lines = [header_line]
    current_length = len(header_line)

    for row in data:
        disp_row = [str(row[i]) for i in col_indices]
        line = " ".join(f"{disp_row[i]:<{widths[i]}}" for i in range(len(headers)))

        # Check if adding the new line will exceed the max length
        if current_length + len(line) + 1 + 100 > MAX_MESSAGE_LENGTH:
            message_parts.append("\n".join(current_part_lines))
            current_part_lines = [header_line, line]
            current_length = len(header_line) + len(line) + 1
        else:
            current_part_lines.append(line)
            current_length += len(line) + 1

    if current_part_lines:
        message_parts.append("\n".join(current_part_lines))

    return message_parts

def format_sorted_list_content(sort_key: str, is_ephemeral: bool = False):
    sort_details = SORT_CONFIGS[sort_key]
    list_data_source = data_list
    processed_data = []

    # Define a standardized timestamp line for all list updates
    current_epoch = int(time.time())
    timestamp_base = f"<t:{current_epoch}:F> (<t:{current_epoch}:R>)"

    if sort_key == "sort_config_recent":
        # Processed data is filtered by the 7-day lambda defined in SORT_CONFIGS
        processed_data = sort_details["sort_lambda"](list_data_source)

        if not processed_data:
            empty_msg = "No items have been updated in the last 7 days."
            timestamp_line = f"{empty_msg}\nLast Updated: {timestamp_base} (Sorted {sort_details['label']})"
            return [timestamp_line]

        formatted_text_parts = format_list_for_display(processed_data,
                                                       sort_details["column_order_indices"],
                                                       sort_details["headers"])
    else:
        # Handling all other sort configurations
        if not list_data_source:
            timestamp_line = f"The list is currently empty.\nLast Updated: {timestamp_base} (List is Empty)"
            return [timestamp_line]

        processed_data = sort_details["sort_lambda"](list_data_source)

        if not processed_data:
            timestamp_line = f"The list is empty after applying the sort/filter.\nLast Updated: {timestamp_base} (List is Empty)"
            return [timestamp_line]

        formatted_text_parts = format_list_for_display(processed_data,
                                                       sort_details["column_order_indices"],
                                                       sort_details["headers"])

    final_message_parts = []
    ts_msg_base = f"(Sorted {sort_details['label']})"
    code_block_overhead = 8  # "```\n" at start, "\n```" at end

    for i, part in enumerate(formatted_text_parts):
        part_header = ""
        if len(formatted_text_parts) > 1:
            part_header = f"Part {i+1}/{len(formatted_text_parts)} - "

        # Inject the clear, standardized timestamp
        timestamp_line = f"Last Updated: {timestamp_base} | {part_header}{ts_msg_base}"

        # Recalculate content length, leaving space for the timestamp and code block
        content_length_with_meta = len(timestamp_line) + len(part) + code_block_overhead + 1 # +1 for newline
        if content_length_with_meta > MAX_MESSAGE_LENGTH:
            # If a part is still too big, this is the fall-back
            final_message_parts.append(f"List is too large to display. Please contact an admin.")
            print(f"Warning: A list part exceeded max length. Length: {content_length_with_meta}")
            break

        final_message_parts.append(f"{timestamp_line}\n```\n{part}\n```")

    return final_message_parts if final_message_parts else ["The list is currently empty."]


async def send_or_edit_persistent_list_prompt(target_channel_id: int, force_new: bool = False):
    """
    Handles editing or sending the list prompt messages, and saves state when message IDs change.
    """
    global channel_list_states
    if target_channel_id not in channel_list_states:
        channel_list_states[target_channel_id] = {"message_ids": [], "default_sort_key_for_display": DEFAULT_PERSISTENT_SORT_KEY}

    state = channel_list_states[target_channel_id]
    msg_ids = state.get("message_ids", [])
    default_sort = state.get("default_sort_key_for_display", DEFAULT_PERSISTENT_SORT_KEY)

    channel = client.get_channel(target_channel_id)
    if not channel:
        if state["message_ids"]:
            state["message_ids"] = []
            save_data_list() # SAVE: Channel disappeared, so state must be cleared.
        return

    content_parts = format_sorted_list_content(default_sort, is_ephemeral=False)
    view = PersistentListPromptView(target_channel_id=target_channel_id)

    ids_changed = False # Flag to track if the set of message IDs has been altered

    if force_new or len(msg_ids) != len(content_parts):
        ids_changed = True
        for msg_id in msg_ids:
            try:
                old_msg = await channel.fetch_message(msg_id)
                await old_msg.delete()
            except:
                pass
            if msg_id in view_message_tracker:
                del view_message_tracker[msg_id]
        msg_ids = []
        state["message_ids"] = []

    sent_messages = []
    for i, content in enumerate(content_parts):
        # Existing logic to edit or send new messages
        if i < len(msg_ids):
            try:
                m = await channel.fetch_message(msg_ids[i])
                # If message is found, try to edit (less likely to set ids_changed=True)
                if i == 0:
                    await m.edit(content=content, view=view)
                    view_message_tracker[m.id] = ("PersistentListPromptView", target_channel_id)
                else:
                    await m.edit(content=content, view=None)
                sent_messages.append(m.id)
            except discord.NotFound:
                # If message not found, we send a new one
                ids_changed = True
                new_m = None
                if i == 0 and not sent_messages:
                    new_m = await channel.send(content=content, view=view)
                    view_message_tracker[new_m.id] = ("PersistentListPromptView", target_channel_id)
                else:
                    new_m = await channel.send(content=content, view=None)
                sent_messages.append(new_m.id)
            except Exception as e:
                # On edit error, send a new message as a fallback
                ids_changed = True
                print(f"Error editing message {msg_ids[i]} in {target_channel_id}, sending new one: {e}")
                new_m = None
                if i == 0 and not sent_messages:
                    new_m = await channel.send(content=content, view=view)
                    view_message_tracker[new_m.id] = ("PersistentListPromptView", target_channel_id)
                else:
                    new_m = await channel.send(content=content, view=None)
                sent_messages.append(new_m.id)
        else:
            # New message must be sent (list grew)
            ids_changed = True
            try:
                new_m = None
                if i == 0 and not msg_ids:
                    new_m = await channel.send(content=content, view=view)
                    view_message_tracker[new_m.id] = ("PersistentListPromptView", target_channel_id)
                else:
                    new_m = await channel.send(content=content, view=None)
                sent_messages.append(new_m.id)
            except Exception as e:
                print(f"Error sending new part {i} of persistent prompt to {target_channel_id}: {e}")

        await asyncio.sleep(0.5)

    # Delete any leftover messages if the list shrunk
    for old_msg_id in msg_ids[len(content_parts):]:
        ids_changed = True
        try:
            old_msg = await channel.fetch_message(old_msg_id)
            await old_msg.delete()
        except:
            pass
        if old_msg_id in view_message_tracker:
            del view_message_tracker[old_msg_id]

    # Check for final message ID list change (e.g., failed edit resulted in a new ID)
    if set(state["message_ids"]) != set(sent_messages):
        ids_changed = True

    # Finalize state update
    state["message_ids"] = sent_messages

    if ids_changed:
        save_data_list() # <--- CRITICAL: SAVE THE STATE HERE


async def update_all_persistent_list_prompts(force_new: bool = False):
    """
    Iterates through all configured channels and calls the update/edit function.
    """
    for cid in INTERACTIVE_LIST_TARGET_CHANNEL_IDS:
        if cid and isinstance(cid, int):
            # force_new=False means it will try to EDIT existing messages based on the loaded IDs.
            await send_or_edit_persistent_list_prompt(cid, force_new)
        await asyncio.sleep(1)

async def clear_all_persistent_list_prompts():
    """
    Deletes all messages and clears the state in memory AND file.
    """
    for cid in list(channel_list_states.keys()):
        state = channel_list_states[cid]
        msg_ids = state.get("message_ids", [])
        for msg_id in msg_ids:
            if not msg_id:
                continue
            channel = client.get_channel(cid)
            if not channel:
                state["message_ids"] = []
                continue
            try:
                m = await channel.fetch_message(msg_id)
                await m.delete()
            except:
                pass
            if msg_id in view_message_tracker:
                del view_message_tracker[msg_id]
            await asyncio.sleep(0.5)
        state["message_ids"] = []

    # CRITICAL: Save the empty state
    save_data_list()


async def handle_restart_command(m: discord.Message):
    """
    Forces a complete delete and re-create of the list messages.
    """
    try:
        await m.add_reaction("🔄")
        # Unlike a normal bot restart, this command FORCES a delete and recreate
        # to ensure any corrupted messages are fixed.
        await update_all_persistent_list_prompts(force_new=True)
    except Exception as e:
        await m.channel.send(f"Restart failed: {e}")
        try:
            await m.add_reaction("❌")
        except:
            pass
        return

    try:
        await m.add_reaction("✅")
    except:
        pass


async def handle_manual_add_command(m: discord.Message):
    parts = m.content[len(MANUAL_ADD_COMMAND_PREFIX):].strip()
    match = re.fullmatch(r"\"([^\"]+)\"\s+\"([^\"]+)\"(?:\s+(\d+))?", parts)
    if not match:
        await m.channel.send(f"Format: `{MANUAL_ADD_COMMAND_PREFIX} \"Item\" \"Name\" [Cost]`")
        return

    item_in, name_in, cost_s = match.group(1), match.group(2), match.group(3)
    global data_list
    found_idx = -1
    resp = ""
    final_cost = "1"

    for i, r in enumerate(data_list):
        if r[0].lower() == item_in.lower():
            found_idx = i
            break

    current_time_epoch = time.time()

    if found_idx != -1:
        row_to_update = data_list.pop(found_idx)
        row_to_update[1] = name_in
        if cost_s:
            final_cost = cost_s
        else:
            try:
                final_cost = str(int(row_to_update[2]) + 1)
            except:
                final_cost = "1"
        row_to_update[2] = final_cost
        # NEW: Update timestamp
        row_to_update[3] = current_time_epoch
        data_list.append(row_to_update)
        resp = f"Updated Item '{item_in}'. Name:'{name_in}',Cost:{final_cost}."
    else:
        final_cost = cost_s if cost_s else "1"
        # NEW: Add timestamp
        new_row = [item_in, name_in, final_cost, current_time_epoch]
        data_list.append(new_row)
        resp = f"Added Item '{item_in}'. Name:'{name_in}',Cost:{final_cost}."

    _update_last_changed_details(item_in, name_in, final_cost)
    save_data_list()
    await m.channel.send(resp)
    await update_all_persistent_list_prompts()


async def handle_delete_command(message: discord.Message):
    parts_str = message.content[len(DELETE_COMMAND_PREFIX):].strip()
    if not (parts_str.startswith('"') and parts_str.endswith('"')):
        await message.channel.send(f"Format: `{DELETE_COMMAND_PREFIX} \"Item Name\"`")
        return
    item_to_delete = parts_str[1:-1]
    global data_list
    original_len = len(data_list)
    # Filter by item name (index 0). Retain existing 4-element structure
    data_list = [r for r in data_list if r[0].lower() != item_to_delete.lower()]
    if len(data_list) < original_len:
        await message.channel.send(f"Item '{item_to_delete}' deleted.")
        if last_updated_item_details.get("item_val") and \
           last_updated_item_details["item_val"].lower() == item_to_delete.lower():
            _update_last_changed_details(None, None, None)
        save_data_list()
        await update_all_persistent_list_prompts()
    else:
        await message.channel.send(f"Item '{item_to_delete}' not found.")


async def handle_announce_command(message: discord.Message):
    item, name, cost = last_updated_item_details.get("item_val"), last_updated_item_details.get("name_val"), last_updated_item_details.get("cost_val")
    if item and name and cost is not None:
        await send_custom_update_notifications(item, name, cost)
        await message.channel.send(f"Announcement: Item: {item}, Name: {name}, Cost: {cost}.")
        try:
            await message.add_reaction("📢")
        except:
            pass
    else:
        await message.channel.send("No recent update (with cost) to announce.")


async def handle_say_command(message: discord.Message):
    """
    Handles the 'list.bot say' command by sending a quoted message to the configured notification channels.
    NOTE: The target channels are defined in UPDATE_NOTIFICATION_CONFIG.
    """
    match = re.match(rf"{re.escape(SAY_COMMAND_PREFIX)}\s*\"([^\"]*)\"$", message.content.strip(), re.IGNORECASE)
    if not match:
        await message.channel.send(f"Format: `{SAY_COMMAND_PREFIX} \"Your message here\"`")
        return

    message_to_say = match.group(1).strip()

    if not message_to_say:
        await message.channel.send("Please provide a message to say.")
        return

    print(f"Admin {message.author.name} (ID: {message.author.id}) requested to say: '{message_to_say}'")

    sent_to_channels = []
    for cfg in UPDATE_NOTIFICATION_CONFIG:
        cid = cfg.get("channel_id")
        if not cid or cid == 0:
            continue
        chan = client.get_channel(cid)
        if not chan:
            print(f"Say Command Err: Channel {cid} not found for saying message. Please update or disable the ID in UPDATE_NOTIFICATION_CONFIG.")
            continue

        try:
            await chan.send(message_to_say)
            sent_to_channels.append(chan.name if hasattr(chan, 'name') else str(cid))
        except Exception as e:
            print(f"Say Command Err: Failed to send message to channel {cid}: {e}")
        await asyncio.sleep(0.5)

    if sent_to_channels:
        await message.channel.send(f"Your message was sent to: {', '.join(sent_to_channels)}.")
        try:
            await message.add_reaction("✅")
        except:
            pass
    else:
        await message.channel.send("Could not send your message to any configured channels. Check the IDs in `UPDATE_NOTIFICATION_CONFIG` and bot permissions.")
        try:
            await message.add_reaction("❌")
        except:
            pass

async def handle_message_command(message: discord.Message):
    """Handles the list.bot message command for a global broadcast."""
    match = re.match(rf"{re.escape(MESSAGE_COMMAND_PREFIX)}\s*\"([^\"]*)\"$", message.content.strip(), re.IGNORECASE)
    if not match:
        await message.channel.send(f"Format: `{MESSAGE_COMMAND_PREFIX} \"Your message here\"`")
        return

    message_to_say = match.group(1).strip()
    if not message_to_say:
        await message.channel.send("Please provide a message to broadcast.")
        return

    print(f"Admin {message.author.name} (ID: {message.author.id}) initiated broadcast: '{message_to_say}'")

    sent_count = 0
    # Iterate over all guilds (servers) the bot is currently in
    for guild in client.guilds:
        target_channel = None
        
        # Priority 1: Use the system channel (often a general or welcome channel)
        if guild.system_channel and guild.system_channel.permissions_for(guild.me).send_messages:
            target_channel = guild.system_channel
        
        # Priority 2: Find the first text channel the bot can send messages in
        if not target_channel:
            for channel in guild.text_channels:
                if channel.permissions_for(guild.me).send_messages:
                    target_channel = channel
                    break

        if target_channel:
            try:
                # Send the message to the found channel
                await target_channel.send(f"**Admin Broadcast:** {message_to_say}")
                sent_count += 1
            except Exception as e:
                # Log an error if sending fails for that specific channel/server
                print(f"Broadcast Error: Failed to send to {guild.name} ({guild.id}) in channel {target_channel.name}: {e}")
        else:
            print(f"Broadcast Skip: No suitable channel found in {guild.name} ({guild.id}).")
        
        # Pause slightly to prevent Discord rate-limiting across servers
        await asyncio.sleep(0.5)

    await message.channel.send(f"Broadcast complete! Message sent to **{sent_count}** servers.")
    try:
        await message.add_reaction("✅")
    except:
        pass


async def handle_close_lists_command(m: discord.Message):
    try:
        await m.add_reaction("🗑️")
        await clear_all_persistent_list_prompts()
    except:
        pass
    try:
        await m.channel.send("All list displays cleared.")
        await m.add_reaction("✅")
    except:
        pass


async def handle_raw_list_command(m: discord.Message):
    """Handles the 'list.bot raw' command by dumping data_list as JSON and splitting into multiple messages if needed."""
    global data_list

    if not data_list:
        await m.channel.send("`data_list` is empty.")
        return

    # Dump the data_list to a JSON string with an indent for readability
    raw_json = json.dumps(data_list, indent=2)

    # Target maximum content size inside the code block is 1850 (1900 - 50 for overhead).
    MAX_CONTENT_CHUNK_SIZE = MAX_MESSAGE_LENGTH - 150

    # Split the raw_json string into chunks
    chunks = []
    i = 0
    while i < len(raw_json):
        chunk = raw_json[i:i + MAX_CONTENT_CHUNK_SIZE]
        chunks.append(chunk)
        i += MAX_CONTENT_CHUNK_SIZE

    total_parts = len(chunks)

    for i, chunk in enumerate(chunks):
        part_number = i + 1

        # Add a descriptive header for multi-part messages
        if total_parts > 1:
            header_text = f"Raw List Data (Part {part_number}/{total_parts})"
        else:
            header_text = "Raw List Data"

        # Construct the final message with code block.
        msg_content = f"{header_text}\n```json\n{chunk}\n```"

        await m.channel.send(msg_content)
        # Add a slight delay between messages to avoid rate-limiting
        await asyncio.sleep(0.5)

    try:
        await m.add_reaction("✅")
    except:
        pass


async def send_custom_update_notifications(item_val, name_val, cost_val):
    print(f"Notifications for: Item '{item_val}', Name '{name_val}', Cost '{cost_val}'")
    for cfg in UPDATE_NOTIFICATION_CONFIG:
        cid, fmt, rid = cfg.get("channel_id"), cfg.get("message_format"), cfg.get("role_id_to_ping")
        if not cid or not fmt or cid == 0:
            continue
        chan = client.get_channel(cid)
        if not chan:
            print(f"Notify Err: Chan {cid} not found.")
            continue
        p_str = ""
        if rid and rid != 0 and chan.guild:
            role = chan.guild.get_role(rid)
            p_str = role.mention if role else ""
        try:
            content = fmt.format(item_val=item_val, name_val=name_val, cost_val=cost_val, role_ping=p_str)
        except KeyError as e:
            print(f"Notify Err: Placeholder {e} invalid. Use {{item_val}}, {{name_val}}, {{cost_val}}.")
            continue
        try:
            mentions = discord.AllowedMentions.none()
            if rid and rid != 0 and p_str:
                mentions.roles = [discord.Object(id=rid)]
            await chan.send(content, allowed_mentions=mentions)
        except Exception as e:
            print(f"Notify Err: Failed to send to {cid}: {e}")
        await asyncio.sleep(0.5)


last_on_ready_timestamp = 0


@client.event
async def on_ready():
    global last_on_ready_timestamp

    current_time = time.time()
    if current_time - last_on_ready_timestamp < 60:
        print("Bot restarted too quickly. Skipping a full list update to prevent API rate limit issues.")
        return

    last_on_ready_timestamp = current_time

    print(f'{client.user.name} ({client.user.id}) connected!')
    
    # The order is crucial:
    print("Loading data and persistent message IDs from file...")
    load_data_list() # <--- Loads both item data and channel state IDs.

    print(f'Auto-updates from: {TARGET_BOT_ID_FOR_AUTO_UPDATES}')
    print(f'Admins: {ADMIN_USER_IDS}')
    print(f'Cmds: Announce:"{ANNOUNCE_COMMAND}", Delete:"{DELETE_COMMAND_PREFIX}", Restart:"{RESTART_COMMAND}", Add:"{MANUAL_ADD_COMMAND_PREFIX}", Say:"{SAY_COMMAND_PREFIX}", Message:"{MESSAGE_COMMAND_PREFIX}", Raw:"{RAW_LIST_COMMAND}", Close:"{CLOSE_LISTS_COMMAND}"')

    print("Initializing channel states...")
    for cid in INTERACTIVE_LIST_TARGET_CHANNEL_IDS:
        if cid == 0 or not isinstance(cid, int):
            continue
        # Ensure new channels get a state if not loaded
        if cid not in channel_list_states:
            channel_list_states[cid] = {"message_ids": [], "default_sort_key_for_display": DEFAULT_PERSISTENT_SORT_KEY}

    print("Ensuring persistent list prompts are up-to-date (editing existing messages).")
    # force_new=False uses the loaded message IDs to EDIT the existing messages.
    await update_all_persistent_list_prompts(force_new=False)


@client.event
async def on_message(m: discord.Message):
    if m.author == client.user or (m.author.bot and m.author.id != TARGET_BOT_ID_FOR_AUTO_UPDATES):
        return

    is_admin = m.author.id in ADMIN_USER_IDS
    content_lower_stripped = m.content.strip().lower()

    if is_admin:
        if content_lower_stripped == RESTART_COMMAND.lower():
            await handle_restart_command(m)
            return
        if content_lower_stripped == CLOSE_LISTS_COMMAND.lower():
            await handle_close_lists_command(m)
            return
        if content_lower_stripped == ANNOUNCE_COMMAND.lower():
            await handle_announce_command(m)
            return
        if content_lower_stripped.startswith(MANUAL_ADD_COMMAND_PREFIX.lower()):
            await handle_manual_add_command(m)
            return
        if content_lower_stripped.startswith(DELETE_COMMAND_PREFIX.lower()):
            await handle_delete_command(m)
            return
        if content_lower_stripped.startswith(SAY_COMMAND_PREFIX.lower()):
            await handle_say_command(m)
            return
        if content_lower_stripped == RAW_LIST_COMMAND.lower():
            await handle_raw_list_command(m)
            return
        # NEW COMMAND HANDLER: list.bot message "Message" (Global Broadcast)
        if content_lower_stripped.startswith(MESSAGE_COMMAND_PREFIX.lower()):
            await handle_message_command(m)
            return
        # END NEW COMMAND HANDLER

    if m.author.id == TARGET_BOT_ID_FOR_AUTO_UPDATES:
        match = AUTO_UPDATE_MESSAGE_REGEX.search(m.content)
        if match:
            item_val, name_val = match.group(1).strip(), match.group(2).strip()
            print(f"AutoUpd from {m.author.id}: Item='{item_val}',Name='{name_val}'")
            updated_cost = update_data_for_auto(item_val, name_val)
            await update_all_persistent_list_prompts(force_new=False)
            await send_custom_update_notifications(item_val, name_val, updated_cost)
            return


async def web_server():
    app = aiohttp.web.Application()
    app.router.add_get("/", lambda r: aiohttp.web.Response(text="Bot is running!"))
    runner = aiohttp.web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080))
    site = aiohttp.web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    print(f"Web server started on port {port}")


async def main():
    if not BOT_TOKEN:
        print("ERROR: BOT_TOKEN environment variable not set. Please configure it on Render.")
        return

    web_task = asyncio.create_task(web_server())
    bot_task = asyncio.create_task(client.start(BOT_TOKEN))

    await asyncio.gather(web_task, bot_task)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Bot and web server stopped.")
    except Exception as e:
        print(f"An error occurred during startup: {e}")