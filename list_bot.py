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
RAW_DATA_COMMAND = "list.bot raw" # NEW: Command to output raw data

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

INITIAL_DATA_LIST = []

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
                    print(f"Log Error: No permission to send messages in log channel {EPHEMERAL_REQUEST_LOG_CHANNEL_ID}.")
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
    final_cost = "6"
    for i, row in enumerate(data_list):
        if row[0].lower() == item_val.lower():
            found_idx = i
            break
    if found_idx != -1:
        existing_row = data_list.pop(found_idx)
        existing_row[1] = name_val
        try:
            final_cost = str(int(existing_row[2]) + 1)
        except ValueError:
            final_cost = "1"
        existing_row[2] = final_cost
        data_list.append(existing_row)
    else:
        new_row = [item_val, name_val, final_cost]
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
        disp_row = [str(r[i]) for i in col_indices]
        for i, val in enumerate(disp_row):
            widths[i] = max(widths[i], len(val))

    # Calculate padding for each column.
    # We use a small amount of padding to save space.
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
        # We also need to account for the code block, timestamp, etc.
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

    if sort_key == "sort_config_recent":
        processed_data = list_data_source[-MAX_RECENT_ITEMS_TO_SHOW:]
        if not processed_data and list_data_source:
            processed_data = list_data_source
        elif not processed_data and not list_data_source:
            empty_msg = "No recent changes, and the list is empty." if is_ephemeral else "The list is currently empty."
            timestamp_line = f"<t:{int(time.time())}:R> (Sorted {sort_details['label']})"
            return [f"{empty_msg}\n{timestamp_line}"]
        if not processed_data:
            empty_msg = "No recent changes to display." if is_ephemeral else "The list is currently empty, so no recent changes."
            timestamp_line = f"<t:{int(time.time())}:R> (Sorted {sort_details['label']})"
            return [f"{empty_msg}\n{timestamp_line}"]

        formatted_text_parts = format_list_for_display(processed_data,
                                                       sort_details["column_order_indices"],
                                                       sort_details["headers"])
    else:
        if not list_data_source:
            timestamp_line = f"<t:{int(time.time())}:R> (List is Empty)"
            return [f"The list is currently empty.\n{timestamp_line}"]
        processed_data = sort_details["sort_lambda"](list_data_source)
        if not processed_data:
            timestamp_line = f"<t:{int(time.time())}:R> (List is Empty or Filter Produced No Results)"
            return [f"The list is currently empty (or filter produced no results).\n{timestamp_line}"]

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

        timestamp_line = f"<t:{int(time.time())}:R> {part_header}{ts_msg_base}"
        # Recalculate content length, leaving space for the timestamp and code block
        content_length_with_meta = len(timestamp_line) + len(part) + code_block_overhead + 1 # +1 for newline
        if content_length_with_meta > MAX_MESSAGE_LENGTH:
            # If a part is still too big, we need to be more aggressive
            # This is a fallback that should ideally never be hit
            final_message_parts.append(f"List is too large to display. Please contact an admin.")
            print(f"Warning: A list part exceeded max length. Length: {content_length_with_meta}")
            break

        final_message_parts.append(f"{timestamp_line}\n```\n{part}\n```")

    return final_message_parts if final_message_parts else ["The list is currently empty."]


async def send_or_edit_persistent_list_prompt(target_channel_id: int, force_new: bool = False):
    global channel_list_states
    if target_channel_id not in channel_list_states:
        channel_list_states[target_channel_id] = {"message_ids": [], "default_sort_key_for_display": DEFAULT_PERSISTENT_SORT_KEY}

    state = channel_list_states[target_channel_id]
    msg_ids = state.get("message_ids", [])
    default_sort = state.get("default_sort_key_for_display", DEFAULT_PERSISTENT_SORT_KEY)

    channel = client.get_channel(target_channel_id)
    if not channel:
        state["message_ids"] = []
        return

    content_parts = format_sorted_list_content(default_sort, is_ephemeral=False)
    view = PersistentListPromptView(target_channel_id=target_channel_id)

    if force_new or len(msg_ids) != len(content_parts):
        for msg_id in msg_ids:
            try:
                old_msg = await channel.fetch_message(msg_id)
                await old_msg.delete()
            except:
                pass
            if msg_id in view_message_tracker:
                del view_message_tracker[msg_id]
        state["message_ids"] = []
        msg_ids = []

    sent_messages = []
    for i, content in enumerate(content_parts):
        if i < len(msg_ids):
            try:
                m = await channel.fetch_message(msg_ids[i])
                if i == 0:
                    await m.edit(content=content, view=view)
                    view_message_tracker[m.id] = ("PersistentListPromptView", target_channel_id)
                else:
                    await m.edit(content=content, view=None)
                sent_messages.append(m.id)
            except discord.NotFound:
                new_m = None
                if i == 0:
                    new_m = await channel.send(content=content, view=view)
                    view_message_tracker[new_m.id] = ("PersistentListPromptView", target_channel_id)
                else:
                    new_m = await channel.send(content=content, view=None)
                sent_messages.append(new_m.id)
            except Exception as e:
                print(f"Error editing/sending part {i} of persistent prompt in {target_channel_id}: {e}")
                try:
                    new_m = None
                    if i == 0:
                        new_m = await channel.send(content=content, view=view)
                        view_message_tracker[new_m.id] = ("PersistentListPromptView", target_channel_id)
                    else:
                        new_m = await channel.send(content=content, view=None)
                    sent_messages.append(new_m.id)
                except Exception as e2:
                    print(f"Critical: Failed to send new message for part {i} in {target_channel_id}: {e2}")
        else:
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

    for old_msg_id in msg_ids[len(content_parts):]:
        try:
            old_msg = await channel.fetch_message(old_msg_id)
            await old_msg.delete()
        except:
            pass
        if old_msg_id in view_message_tracker:
            del view_message_tracker[old_msg_id]

    state["message_ids"] = sent_messages


async def update_all_persistent_list_prompts(force_new: bool = False):
    for cid in INTERACTIVE_LIST_TARGET_CHANNEL_IDS:
        if cid and isinstance(cid, int):
            await send_or_edit_persistent_list_prompt(cid, force_new)
        await asyncio.sleep(1)


async def clear_all_persistent_list_prompts():
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


async def handle_restart_command(m: discord.Message):
    try:
        await m.add_reaction("🔄")
        await clear_all_persistent_list_prompts()
    except:
        pass
    await update_all_persistent_list_prompts(force_new=True)
    try:
        await m.add_reaction("✅")
    except:
        pass


async def handle_raw_data_command(m: discord.Message):
    global data_list
    
    # 1. Format the data_list (which is already in historical order) as a JSON string
    try:
        raw_data_json = json.dumps(data_list, indent=4)
    except Exception as e:
        await m.channel.send(f"Error converting list to JSON: {e}")
        return

    # 2. Split the JSON string into chunks that fit Discord's message limits
    #    MAX_MESSAGE_LENGTH is 1900, leaving buffer for code block and header
    max_chunk_size = 1900 - 100 
    chunks = [raw_data_json[i:i + max_chunk_size] for i in range(0, len(raw_data_json), max_chunk_size)]

    if not chunks:
        await m.channel.send("The list is currently empty.")
        return

    # 3. Send the chunks, wrapped in a JSON code block
    try:
        await m.add_reaction("📄")
    except:
        pass

    for i, chunk in enumerate(chunks):
        header = f"Raw List Data (Part {i+1}/{len(chunks)})" if len(chunks) > 1 else "Raw List Data"
        content = f"**{header}**\n```json\n{chunk}\n```"
        try:
            await m.channel.send(content)
        except Exception as e:
            print(f"Failed to send raw data part {i+1}: {e}")
            await m.channel.send(f"Error sending part {i+1} of raw data.")
        await asyncio.sleep(0.5) # Prevent rate-limiting
    
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
    final_cost = "6"

    for i, r in enumerate(data_list):
        if r[0].lower() == item_in.lower():
            found_idx = i
            break

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
        data_list.append(row_to_update)
        resp = f"Updated Item '{item_in}'. Name:'{name_in}',Cost:{final_cost}."
    else:
        final_cost = cost_s if cost_s else "6"
        new_row = [item_in, name_in, final_cost]
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
            print(f"Say Command Err: Channel {cid} not found for saying message.")
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
        await message.channel.send("Could not send your message to any configured channels.")
        try:
            await message.add_reaction("❌")
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
    print("Loading data from file...")
    load_data_list()
    print(f'Auto-updates from: {TARGET_BOT_ID_FOR_AUTO_UPDATES}')
    print(f'Admins: {ADMIN_USER_IDS}')
    print(f'Cmds: Announce:"{ANNOUNCE_COMMAND}", Delete:"{DELETE_COMMAND_PREFIX}", Restart:"{RESTART_COMMAND}", Add:"{MANUAL_ADD_COMMAND_PREFIX}", Say:"{SAY_COMMAND_PREFIX}", Close:"{CLOSE_LISTS_COMMAND}", Raw:"{RAW_DATA_COMMAND}"')

    print("Initializing channel states for persistent views...")
    for cid in INTERACTIVE_LIST_TARGET_CHANNEL_IDS:
        if cid == 0 or not isinstance(cid, int):
            continue
        if cid not in channel_list_states:
            channel_list_states[cid] = {"message_ids": [], "default_sort_key_for_display": DEFAULT_PERSISTENT_SORT_KEY}
        state = channel_list_states[cid]
        msg_ids = state.get("message_ids", [])
        if msg_ids:
            print(f"INFO: Channel {cid} has stored message IDs {msg_ids}. It will be handled by update_all_persistent_list_prompts.")

    print("Ensuring persistent list prompts are up-to-date or posted.")
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
        if content_lower_stripped == RAW_DATA_COMMAND.lower(): # NEW: Raw data command
            await handle_raw_data_command(m)
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