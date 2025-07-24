import os
import discord
import re
import time
import asyncio
from discord.ui import View, Button, button
from discord.enums import ButtonStyle
from collections import Counter # Import Counter for the new sort

# --- CONFIGURATION ---
BOT_TOKEN = os.environ.get("BOT_TOKEN")

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

# --- NEW: Channel ID for logging ephemeral list requests ---
EPHEMERAL_REQUEST_LOG_CHANNEL_ID = 1385094756912205984 # Replace with your actual Log Channel ID, or 0/None to disable

channel_list_states = {} 
DEFAULT_PERSISTENT_SORT_KEY = "sort_config_item"
MAX_RECENT_ITEMS_TO_SHOW = 30
MAX_MESSAGE_LENGTH = 1900 # Keep it under 2000 for safety, leaving room for headers/footers

# Helper function for the new 'Owner' sort
def sort_by_owner_tally(data):
    if not data:
        return []
    # Count occurrences of each name (case-insensitive)
    name_counts = Counter(row[1].lower() for row in data)

    # Sort by:
    # 1. Negative of name count (descending count)
    # 2. Name itself (alphabetical, for consistent ordering of names with same count)
    # 3. Negative of cost (descending cost)
    def custom_sort_key(row):
        name = row[1].lower()
        cost = int(row[2]) 
        return (-name_counts[name], name, -cost) # Negative for descending sort

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
    # NEW SORT CONFIG
    "sort_config_owner": {
        "label": "by Owner Count", "button_label": "Sort: Owner",
        "sort_lambda": sort_by_owner_tally, # Use the new helper function
        "column_order_indices": [1, 0, 2], "headers": ["Name", "Item", "Cost"] # Display Name first
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
intents.messages = True; intents.message_content = True; intents.guilds = True
client = discord.Client(intents=intents)

data_list = [
["Antennae", "Manfred", 7],
  ["Battery", "Umit", 6],
  ["Card", "hqyx", 6],
  ["Claw", "hqyx", 6],
  ["Corruption", "Pehiley", 6],
  ["Dandelion", "FREEDOM08", 6],
  ["Dice", "Pehiley", 6],
  ["Fang", "hqyx", 8],
  ["Light Bulb", "BaiLin2", 6],
  ["Magic Stick", "Pehiley", 6],
  ["Orange", "Solar", 9],
  ["Pincer", "Luai2", 7],
  ["Relic", "gujiga", 6],
  ["Salt", "tarou9n", 6],
  ["Stick", "BaiLin2", 6],
  ["Talisman", "Manfred", 7],
  ["Web", "Manfred", 6],
  ["Wing", "Gainer", 13],
  ["Iris", "Craft_Super", 9],
  ["Beetle Egg", "hqyx", 11],
  ["Heavy", "asds", 6],
  ["Faster", "Manfred", 8],
  ["Mana Orb", "BONER_ALERT", 6],
  ["Bone", "HUDUMOC", 10],
  ["Poker Chip", "PlayFlorrio", 12],
  ["Pearl", "gachanchall", 6],
  ["Missile", "Missile", 8],
  ["Dark Mark", "Craft_Super", 6],
  ["Magic Leaf", "Manfred", 6],
  ["Bubble", "Recon", 6],
  ["Poo", "Gainer", 8],
  ["Rice", "Manfred", 6],
  ["Peas", "WTJ", 6],
  ["Light", "-Sam8375", 6],
  ["Privet Berry", "Abstract", 6],
  ["Yucca", "Pehiley", 12],
  ["Clover", "-Sam8375", 23],
  ["Leaf", "Etin", 7],
  ["Mecha Missile", "Mario", 6],
  ["Magic Missile", "Pehiley", 6],
  ["Sand", "Zorat", 13],
  ["Triangle", "Zeal", 7],
  ["Magic Cactus","Pehiley",6],
  ["Starfish", "CarrotJuice", 7],
  ["Air", "gachanchall", 11],
  ["Cactus", "tianleshan", 7],
  ["Corn","-Sam8375",6],
  ["Glass", "-Sam8375", 16],
  ["Ant Egg", "tianleshan", 11],
  ["Mjölnir", "Manfred", 10],
  ["Wax", "ProH", 6],
  ["Coin", "givemeygg", 26],
  ["Totem", "BONER_ALERT", 6],
  ["Jelly", "tarou9n", 7],
  ["Crown", "Fiona_UwU", 31],
  ["Pincer", "Avril", 8],
]

last_updated_item_details = {"item_val": None, "name_val": None, "cost_val": None}
view_message_tracker = {} 

class EphemeralListView(View):
    def __init__(self, initial_sort_key: str, timeout=300): # timeout in seconds
        super().__init__(timeout=timeout)
        self.current_sort_key = initial_sort_key
        self._update_button_states()

    def _update_button_states(self):
        for child in self.children:
            if isinstance(child, Button):
                if child.custom_id == f"ephem_btn_{self.current_sort_key}":
                    child.disabled = True; child.style = ButtonStyle.success
                else:
                    child.disabled = False; child.style = ButtonStyle.secondary

    async def _update_ephemeral_message(self, interaction: discord.Interaction, new_sort_key: str):
        self.current_sort_key = new_sort_key
        self._update_button_states()
        # Ephemeral messages will only send one part, so we pass is_ephemeral=True
        full_content_parts = format_sorted_list_content(new_sort_key, is_ephemeral=True)
        try: 
            # If full_content_parts is a list, only send the first part for ephemeral.
            # Ephemeral messages cannot be edited to add more parts easily.
            content_to_send = full_content_parts[0] if isinstance(full_content_parts, list) else full_content_parts
            await interaction.response.edit_message(content=content_to_send, view=self)
        except discord.HTTPException as e: print(f"Failed to edit ephemeral message for {interaction.user.name}: {e}")

    @button(label=SORT_CONFIGS["sort_config_item"]["button_label"], style=ButtonStyle.secondary, custom_id="ephem_btn_sort_config_item")
    async def sort_item_btn_e(self, i: discord.Interaction, b: Button): await self._update_ephemeral_message(i, "sort_config_item")
    @button(label=SORT_CONFIGS["sort_config_name"]["button_label"], style=ButtonStyle.secondary, custom_id="ephem_btn_sort_config_name")
    async def sort_name_btn_e(self, i: discord.Interaction, b: Button): await self._update_ephemeral_message(i, "sort_config_name")
    @button(label=SORT_CONFIGS["sort_config_cost"]["button_label"], style=ButtonStyle.secondary, custom_id="ephem_btn_sort_config_cost")
    async def sort_cost_btn_e(self, i: discord.Interaction, b: Button): await self._update_ephemeral_message(i, "sort_config_cost")
    @button(label=SORT_CONFIGS["sort_config_recent"]["button_label"], style=ButtonStyle.secondary, custom_id="ephem_btn_sort_config_recent")
    async def sort_recent_btn_e(self, i: discord.Interaction, b: Button): await self._update_ephemeral_message(i, "sort_config_recent")
    # NEW EPHEMERAL BUTTON
    @button(label=SORT_CONFIGS["sort_config_owner"]["button_label"], style=ButtonStyle.secondary, custom_id="ephem_btn_sort_config_owner")
    async def sort_owner_btn_e(self, i: discord.Interaction, b: Button): await self._update_ephemeral_message(i, "sort_config_owner")

    async def on_timeout(self): print(f"EphemeralListView for sort {self.current_sort_key} timed out.")

class PersistentListPromptView(View):
    def __init__(self, target_channel_id: int, timeout=None):
        super().__init__(timeout=timeout)
        self.target_channel_id = target_channel_id

    async def _send_ephemeral_sorted_list(self, interaction: discord.Interaction, sort_key: str):
        # Console log
        print(f"User {interaction.user.name} (ID: {interaction.user.id}) requested sorted list ('{sort_key}') ephemerally in channel <#{interaction.channel_id}>.")
        
        # Log to Discord Channel
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

        # Ephemeral messages will only send one part, so we pass is_ephemeral=True
        full_content_parts = format_sorted_list_content(sort_key, is_ephemeral=True)
        ephemeral_view = EphemeralListView(initial_sort_key=sort_key)
        try:
            # If full_content_parts is a list, only send the first part for ephemeral.
            content_to_send = full_content_parts[0] if isinstance(full_content_parts, list) else full_content_parts
            await interaction.response.send_message(content=content_to_send, view=ephemeral_view, ephemeral=True)
        except Exception as e:
            print(f"Failed to send ephemeral sorted list to {interaction.user.name}: {e}")
            try: await interaction.followup.send("Sorry, I couldn't generate your list view at this time.", ephemeral=True)
            except: pass

    @button(label=SORT_CONFIGS["sort_config_item"]["button_label"], style=ButtonStyle.primary, custom_id="persist_btn_sort_item")
    async def sort_item_btn_p(self, i: discord.Interaction, b: Button): await self._send_ephemeral_sorted_list(i, "sort_config_item")
    @button(label=SORT_CONFIGS["sort_config_name"]["button_label"], style=ButtonStyle.primary, custom_id="persist_btn_sort_name")
    async def sort_name_btn_p(self, i: discord.Interaction, b: Button): await self._send_ephemeral_sorted_list(i, "sort_config_name")
    @button(label=SORT_CONFIGS["sort_config_cost"]["button_label"], style=ButtonStyle.primary, custom_id="persist_btn_sort_cost")
    async def sort_cost_btn_p(self, i: discord.Interaction, b: Button): await self._send_ephemeral_sorted_list(i, "sort_config_cost")
    @button(label=SORT_CONFIGS["sort_config_recent"]["button_label"], style=ButtonStyle.primary, custom_id="persist_btn_sort_recent")
    async def sort_recent_btn_p(self, i: discord.Interaction, b: Button): await self._send_ephemeral_sorted_list(i, "sort_config_recent")
    # NEW PERSISTENT BUTTON
    @button(label=SORT_CONFIGS["sort_config_owner"]["button_label"], style=ButtonStyle.primary, custom_id="persist_btn_sort_owner")
    async def sort_owner_btn_p(self, i: discord.Interaction, b: Button): await self._send_ephemeral_sorted_list(i, "sort_config_owner")

    async def on_timeout(self): print(f"PersistentListPromptView for channel {self.target_channel_id} supposedly timed out.")


def _update_last_changed_details(item_val, name_val, cost_val):
    global last_updated_item_details
    last_updated_item_details = {"item_val": item_val, "name_val": name_val, "cost_val": cost_val}

def update_data_for_auto(item_val, name_val): 
    global data_list; found_idx = -1; final_cost = "6"
    for i, row in enumerate(data_list):
        if row[0].lower() == item_val.lower(): found_idx = i; break
    if found_idx != -1:
        existing_row = data_list.pop(found_idx)
        existing_row[1] = name_val
        try: final_cost = str(int(existing_row[2]) + 1)
        except ValueError: final_cost = "1"
        existing_row[2] = final_cost
        data_list.append(existing_row)
    else:
        new_row = [item_val, name_val, final_cost]
        data_list.append(new_row)
    _update_last_changed_details(item_val, name_val, final_cost)
    print(f"Data update: Item='{item_val}',Name='{name_val}',NewCost='{final_cost}' (Auto)")
    return final_cost

def format_list_for_display(data, col_indices, headers):
    if not data: return [] # Return an empty list for no data
    disp_data = [[str(r[i]) for i in col_indices] for r in data]
    widths = [len(h) for h in headers]
    for r_disp in disp_data:
        for i,v_disp in enumerate(r_disp): widths[i]=max(widths[i],len(v_disp))
    
    header_line = "  ".join(f"{headers[i]:<{widths[i]}}" for i in range(len(headers)))
    
    message_parts = []
    current_part_lines = [header_line]
    
    for r_disp in disp_data:
        line = "  ".join(f"{r_disp[i]:<{widths[i]}}" for i in range(len(r_disp)))
        # Check if adding the next line would exceed the MAX_MESSAGE_LENGTH
        # We also need to account for the ```\n and \n``` characters around the code block
        if sum(len(l) + 1 for l in current_part_lines) + len(line) + 1 + 6 > MAX_MESSAGE_LENGTH:
            message_parts.append("\n".join(current_part_lines))
            current_part_lines = [header_line, line] # Start new part with header
        else:
            current_part_lines.append(line)
            
    if current_part_lines:
        message_parts.append("\n".join(current_part_lines))

    return message_parts

def format_sorted_list_content(sort_key: str, is_ephemeral: bool = False):
    sort_details = SORT_CONFIGS[sort_key]
    list_data_source = data_list 
    processed_data = [] 

    if sort_key == "sort_config_recent":
        processed_data = list_data_source[-MAX_RECENT_ITEMS_TO_SHOW:]
        if not processed_data and list_data_source: processed_data = list_data_source 
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
    
    for i, part in enumerate(formatted_text_parts):
        part_header = ""
        if len(formatted_text_parts) > 1:
            part_header = f"Part {i+1}/{len(formatted_text_parts)} - "

        timestamp_line = f"<t:{int(time.time())}:R> {part_header}{ts_msg_base}"
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
    if not channel: state["message_ids"] = []; return

    content_parts = format_sorted_list_content(default_sort, is_ephemeral=False)
    view = PersistentListPromptView(target_channel_id=target_channel_id)

    # Clean up old messages if force_new or number of parts changed
    if force_new or len(msg_ids) != len(content_parts):
        for msg_id in msg_ids:
            try: old_msg = await channel.fetch_message(msg_id); await old_msg.delete()
            except: pass 
            if msg_id in view_message_tracker: del view_message_tracker[msg_id]
        state["message_ids"] = []
        msg_ids = []

    sent_messages = []
    for i, content in enumerate(content_parts):
        if i < len(msg_ids): # Try to edit existing messages
            try:
                m = await channel.fetch_message(msg_ids[i])
                if i == 0: # Only the first message gets the interactive view
                    await m.edit(content=content, view=view)
                    view_message_tracker[m.id] = ("PersistentListPromptView", target_channel_id) 
                else:
                    await m.edit(content=content, view=None) # Subsequent messages don't need buttons
                sent_messages.append(m.id)
            except discord.NotFound: # Message deleted, send new
                new_m = None
                if i == 0:
                    new_m = await channel.send(content=content, view=view)
                    view_message_tracker[new_m.id] = ("PersistentListPromptView", target_channel_id)
                else:
                    new_m = await channel.send(content=content, view=None)
                sent_messages.append(new_m.id)
            except Exception as e: 
                print(f"Error editing/sending part {i} of persistent prompt in {target_channel_id}: {e}")
                # If an error occurs, try to send a new message for this part
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

        else: # Send new messages for new parts
            try:
                new_m = None
                if i == 0 and not msg_ids: # Only attach view if it's the very first part and no existing messages
                    new_m = await channel.send(content=content, view=view)
                    view_message_tracker[new_m.id] = ("PersistentListPromptView", target_channel_id)
                else:
                    new_m = await channel.send(content=content, view=None)
                sent_messages.append(new_m.id)
            except Exception as e: 
                print(f"Error sending new part {i} of persistent prompt to {target_channel_id}: {e}")

    # Delete any remaining old messages if the new list is shorter
    for old_msg_id in msg_ids[len(content_parts):]:
        try:
            old_msg = await channel.fetch_message(old_msg_id)
            await old_msg.delete()
        except: pass
        if old_msg_id in view_message_tracker: del view_message_tracker[old_msg_id]

    state["message_ids"] = sent_messages


async def update_all_persistent_list_prompts(force_new: bool = False):
    for cid in INTERACTIVE_LIST_TARGET_CHANNEL_IDS:
        if cid and isinstance(cid, int): await send_or_edit_persistent_list_prompt(cid, force_new)
        await asyncio.sleep(1)

async def clear_all_persistent_list_prompts():
    for cid in list(channel_list_states.keys()): 
        state = channel_list_states[cid]
        msg_ids = state.get("message_ids", [])
        for msg_id in msg_ids:
            if not msg_id: continue
            channel = client.get_channel(cid)
            if not channel: state["message_ids"] = []; continue
            try: m = await channel.fetch_message(msg_id); await m.delete()
            except: pass 
            if msg_id in view_message_tracker: del view_message_tracker[msg_id]
            await asyncio.sleep(0.5)
        state["message_ids"] = [] # Clear all message IDs for the channel


async def handle_restart_command(m: discord.Message):
    try: await m.add_reaction("🔄"); await clear_all_persistent_list_prompts()
    except: pass
    await update_all_persistent_list_prompts(force_new=True)
    try: await m.add_reaction("✅")
    except: pass

async def handle_manual_add_command(m: discord.Message):
    parts = m.content[len(MANUAL_ADD_COMMAND_PREFIX):].strip()
    match = re.fullmatch(r"\"([^\"]+)\"\s+\"([^\"]+)\"(?:\s+(\d+))?", parts)
    if not match: await m.channel.send(f"Format: `{MANUAL_ADD_COMMAND_PREFIX} \"Item\" \"Name\" [Cost]`"); return
    
    item_in, name_in, cost_s = match.group(1), match.group(2), match.group(3)
    global data_list; found_idx = -1; resp=""; final_cost = "6"

    for i, r in enumerate(data_list):
        if r[0].lower()==item_in.lower(): found_idx = i; break
    
    if found_idx != -1:
        row_to_update = data_list.pop(found_idx)
        row_to_update[1] = name_in
        if cost_s: final_cost = cost_s
        else:
            try: final_cost = str(int(row_to_update[2])+1)
            except: final_cost = "1"
        row_to_update[2] = final_cost
        data_list.append(row_to_update)
        resp=f"Updated Item '{item_in}'. Name:'{name_in}',Cost:{final_cost}."
    else:
        final_cost=cost_s if cost_s else "6"
        new_row = [item_in,name_in,final_cost]
        data_list.append(new_row)
        resp=f"Added Item '{item_in}'. Name:'{name_in}',Cost:{final_cost}."
    _update_last_changed_details(item_in, name_in, final_cost)
        
    await m.channel.send(resp)
    await update_all_persistent_list_prompts()

async def handle_delete_command(message: discord.Message):
    parts_str = message.content[len(DELETE_COMMAND_PREFIX):].strip()
    if not (parts_str.startswith('"') and parts_str.endswith('"')):
        await message.channel.send(f"Format: `{DELETE_COMMAND_PREFIX} \"Item Name\"`"); return
    item_to_delete = parts_str[1:-1]
    global data_list; original_len = len(data_list)
    data_list = [r for r in data_list if r[0].lower() != item_to_delete.lower()]
    if len(data_list) < original_len:
        await message.channel.send(f"Item '{item_to_delete}' deleted.")
        if last_updated_item_details.get("item_val") and \
           last_updated_item_details["item_val"].lower() == item_to_delete.lower():
            _update_last_changed_details(None, None, None)
        await update_all_persistent_list_prompts()
    else: await message.channel.send(f"Item '{item_to_delete}' not found.")

async def handle_announce_command(message: discord.Message):
    item, name, cost = last_updated_item_details.get("item_val"), last_updated_item_details.get("name_val"), last_updated_item_details.get("cost_val")
    if item and name and cost is not None:
        await send_custom_update_notifications(item, name, cost)
        await message.channel.send(f"Announcement: Item: {item}, Name: {name}, Cost: {cost}.")
        try: await message.add_reaction("📢")
        except: pass
    else: await message.channel.send("No recent update (with cost) to announce.")

# NEW FUNCTION: handle_say_command
async def handle_say_command(message: discord.Message):
    # Extract the message content within quotes
    match = re.match(rf"{re.escape(SAY_COMMAND_PREFIX)}\s*\"([^\"]*)\"$", message.content.strip(), re.IGNORECASE)
    if not match:
        await message.channel.send(f"Format: `{SAY_COMMAND_PREFIX} \"Your message here\"`"); return
    
    message_to_say = match.group(1).strip()
    
    if not message_to_say:
        await message.channel.send("Please provide a message to say."); return
    
    print(f"Admin {message.author.name} (ID: {message.author.id}) requested to say: '{message_to_say}'")
    
    sent_to_channels = []
    for cfg in UPDATE_NOTIFICATION_CONFIG:
        cid = cfg.get("channel_id")
        if not cid or cid == 0:
            continue
        chan = client.get_channel(cid)
        if not chan:
            print(f"Say Command Err: Channel {cid} not found for saying message."); continue
        
        try:
            # We don't need role pings or special formatting for a direct 'say' command,
            # unless the user explicitly included them in their message_to_say.
            await chan.send(message_to_say)
            sent_to_channels.append(chan.name if hasattr(chan, 'name') else str(cid))
        except Exception as e:
            print(f"Say Command Err: Failed to send message to channel {cid}: {e}")
        await asyncio.sleep(0.5) # Prevent rate limiting

    if sent_to_channels:
        await message.channel.send(f"Your message was sent to: {', '.join(sent_to_channels)}.")
        try: await message.add_reaction("✅")
        except: pass
    else:
        await message.channel.send("Could not send your message to any configured channels.")
        try: await message.add_reaction("❌")
        except: pass


async def handle_close_lists_command(m: discord.Message):
    try: await m.add_reaction("🗑️"); await clear_all_persistent_list_prompts()
    except: pass
    try: await m.channel.send("All list displays cleared."); await m.add_reaction("✅")
    except: pass

async def send_custom_update_notifications(item_val, name_val, cost_val):
    print(f"Notifications for: Item '{item_val}', Name '{name_val}', Cost '{cost_val}'")
    for cfg in UPDATE_NOTIFICATION_CONFIG:
        cid,fmt,rid=cfg.get("channel_id"),cfg.get("message_format"),cfg.get("role_id_to_ping")
        if not cid or not fmt or cid==0: continue
        chan=client.get_channel(cid)
        if not chan: print(f"Notify Err: Chan {cid} not found."); continue
        p_str=""
        if rid and rid!=0 and chan.guild: role=chan.guild.get_role(rid); p_str=role.mention if role else ""
        try: content=fmt.format(item_val=item_val,name_val=name_val,cost_val=cost_val,role_ping=p_str)
        except KeyError as e: print(f"Notify Err: Placeholder {e} invalid. Use {{item_val}}, {{name_val}}, {{cost_val}}."); continue
        try:
            mentions=discord.AllowedMentions.none()
            if rid and rid!=0 and p_str: mentions.roles=[discord.Object(id=rid)]
            await chan.send(content,allowed_mentions=mentions)
        except Exception as e: print(f"Notify Err: Failed to send to {cid}: {e}")
        await asyncio.sleep(0.5)

@client.event
async def on_ready():
    print(f'{client.user.name} ({client.user.id}) connected!')
    print(f'Auto-updates from: {TARGET_BOT_ID_FOR_AUTO_UPDATES}')
    print(f'Admins: {ADMIN_USER_IDS}')
    print(f'Cmds: Announce:"{ANNOUNCE_COMMAND}", Delete:"{DELETE_COMMAND_PREFIX}", Restart:"{RESTART_COMMAND}", Add:"{MANUAL_ADD_COMMAND_PREFIX}", Say:"{SAY_COMMAND_PREFIX}", Close:"{CLOSE_LISTS_COMMAND}"') # Updated print statement


    print("Initializing channel states for persistent views...")
    for cid in INTERACTIVE_LIST_TARGET_CHANNEL_IDS:
        if cid == 0 or not isinstance(cid, int): continue
        if cid not in channel_list_states: 
             channel_list_states[cid] = {"message_ids": [], "default_sort_key_for_display": DEFAULT_PERSISTENT_SORT_KEY}
        
        state = channel_list_states[cid]
        msg_ids = state.get("message_ids", []) # Now a list of message IDs
        
        # Simplified on_ready: Does not attempt to re-attach views from previous session by default.
        # If you had persistent storage for message_ids, you would load them here and then attempt to re-attach.
        if msg_ids:
            # This code path is for if you *did* have a way to load old message_ids
            # For example, if you saved channel_list_states to a file and loaded it.
            # Since we don't do that here, msg_ids will usually be an empty list for a fresh bot start.
            print(f"INFO: Channel {cid} has stored message IDs {msg_ids}. It will be handled by update_all_persistent_list_prompts.")
            # For robust view persistence:
            # try:
            #     chan = await client.fetch_channel(cid)
            #     for msg_id in msg_ids:
            #         if msg_id: # Check if message_id is not None
            #             try:
            #                 await chan.fetch_message(msg_id) 
            #                 view = PersistentListPromptView(target_channel_id=cid)
            #                 client.add_view(view, message_id=msg_id) # Re-attach
            #                 print(f"Re-added PersistentListPromptView to msg {msg_id} in chan {cid}")
            #             except (discord.NotFound, discord.Forbidden, AttributeError):
            #                 # If a message is not found or forbidden, remove it from the list
            #                 print(f"Message {msg_id} not found or forbidden in channel {cid}. Removing it from state.")
            #                 state["message_ids"].remove(msg_id)
            # except Exception as e:
            #     print(f"Err re-adding views for channel {cid}: {e}")
            #     state["message_ids"] = [] # Clear if a broader error occurs
    
    print("Ensuring persistent list prompts are up-to-date or posted.")
    await update_all_persistent_list_prompts(force_new=True) 

@client.event
async def on_message(m: discord.Message):
    if m.author==client.user or (m.author.bot and m.author.id!=TARGET_BOT_ID_FOR_AUTO_UPDATES): return
    
    is_admin = m.author.id in ADMIN_USER_IDS
    content_lower_stripped = m.content.strip().lower()

    if is_admin:
        if content_lower_stripped == RESTART_COMMAND.lower(): await handle_restart_command(m); return
        if content_lower_stripped == CLOSE_LISTS_COMMAND.lower(): await handle_close_lists_command(m); return
        if content_lower_stripped == ANNOUNCE_COMMAND.lower(): await handle_announce_command(m); return
        if content_lower_stripped.startswith(MANUAL_ADD_COMMAND_PREFIX.lower()): await handle_manual_add_command(m); return
        if content_lower_stripped.startswith(DELETE_COMMAND_PREFIX.lower()): await handle_delete_command(m); return
        # NEW COMMAND CHECK
        if content_lower_stripped.startswith(SAY_COMMAND_PREFIX.lower()): await handle_say_command(m); return
    
    if m.author.id==TARGET_BOT_ID_FOR_AUTO_UPDATES:
        match=AUTO_UPDATE_MESSAGE_REGEX.search(m.content) 
        if match:
            item_val, name_val =match.group(1).strip(),match.group(2).strip()
            print(f"AutoUpd from {m.author.id}: Item='{item_val}',Name='{name_val}'")
            updated_cost = update_data_for_auto(item_val,name_val)
            await update_all_persistent_list_prompts(force_new=False) 
            await send_custom_update_notifications(item_val,name_val, updated_cost)
            return
        # else:
            # print(f"DEBUG: Message from target bot {m.author.id} did not match regex: '{m.content[:100]}'")

if __name__=="__main__":
    if BOT_TOKEN=="YOUR_BOT_TOKEN_HERE": print("ERROR: Set VALID BOT_TOKEN.")
    else:
        try: client.run(BOT_TOKEN)
        except discord.LoginFailure: print("LOGIN FAILED: BOT_TOKEN invalid.")
        except discord.PrivilegedIntentsRequired as e: print(f"ERROR: PrivilegedIntentsRequired: {e}.")
        except Exception as e: print(f"Unexpected error running bot: {e}")