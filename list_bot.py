import os
import discord
import re
import time
import asyncio
import json
import aiohttp.web
from aiohttp import web
import aiohttp_cors
from discord.ui import View, Button, button
from discord.enums import ButtonStyle
from collections import Counter
from discord.ext import tasks

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

AUTO_UPDATE_MESSAGE_REGEX = re.compile(r"(.+)\s+is now owned by\s+(.+)")

# --- BOT SETUP ---
intents = discord.Intents.default()
# We only need the message and message content intents, as our bot's functionality
# does not rely on accessing member information.
intents.messages = True
intents.message_content = True

client = discord.Client(intents=intents)

# --- DATA MANAGEMENT ---
def load_data():
    if not os.path.exists(DATA_FILE):
        return {"list": [], "persistent_messages": [], "last_auto_update_time": 0}
    with open(DATA_FILE, "r") as f:
        return json.load(f)

def save_data(data):
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=4)

def get_list_string(items):
    return "\n".join(f"{i+1}. {item}" for i, item in enumerate(items))

# --- DISCORD LISTENERS & COMMANDS ---

@client.event
async def on_ready():
    print(f'Logged in as {client.user} ({client.user.id})')
    print('------')
    print("Starting web server...")
    web_server_task.start()
    
    # Check for any missing persistent messages and resend them
    data = load_data()
    for channel_id, message_id in data.get("persistent_messages", []):
        try:
            channel = client.get_channel(int(channel_id))
            if channel:
                # Attempt to get the message to check if it exists
                await channel.fetch_message(int(message_id))
        except discord.NotFound:
            print(f"Persistent message {message_id} in channel {channel_id} not found. Re-announcing.")
            await announce_list(channel)
        except Exception as e:
            print(f"Error checking persistent message: {e}")


@client.event
async def on_message(message):
    if message.author == client.user:
        return

    m = message
    content = m.content.lower().strip()

    # Admin commands (from Discord)
    if m.author.id in ADMIN_USER_IDS:
        if content == RESTART_COMMAND:
            await m.channel.send("Restarting bot...")
            await client.close()
            return
        elif content == CLOSE_LISTS_COMMAND:
            await close_all_lists(m.channel)
            return
        elif content.startswith(MANUAL_ADD_COMMAND_PREFIX):
            item_val = m.content[len(MANUAL_ADD_COMMAND_PREFIX):].strip()
            if item_val:
                data = load_data()
                data["list"].append(item_val)
                save_data(data)
                await update_all_persistent_list_prompts(force_new=False)
                await m.channel.send(f"Added item to the list.")
            return
        elif content == ANNOUNCE_COMMAND:
            await announce_list(m.channel)
            return
        elif content.startswith(DELETE_COMMAND_PREFIX):
            try:
                index_to_delete = int(m.content[len(DELETE_COMMAND_PREFIX):].strip()) - 1
                data = load_data()
                if 0 <= index_to_delete < len(data["list"]):
                    deleted_item = data["list"].pop(index_to_delete)
                    save_data(data)
                    await update_all_persistent_list_prompts(force_new=False)
                    await m.channel.send(f"Deleted item '{deleted_item}' from the list.")
                else:
                    await m.channel.send("Invalid index.")
            except (ValueError, IndexError):
                await m.channel.send("Invalid command format. Use `list.bot delete <number>`.")
            return
        elif m.content.startswith(SAY_COMMAND_PREFIX):
            say_content = m.content[len(SAY_COMMAND_PREFIX):].strip()
            if say_content:
                await m.channel.send(say_content)
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

# --- WEB SERVER & API ---

async def handle_command(request):
    """Handles POST requests from the web page to execute bot commands."""
    try:
        data = await request.json()
        command = data.get('command')
        if not command:
            return web.json_response({'error': 'No command provided'}, status=400)

        print(f"Received command from web panel: {command}")

        # Execute the command as if it came from a privileged user in a specific channel.
        # This is a critical security consideration. Only trusted sources should call this API.
        # We can use a mock message object to simulate a Discord message.
        class MockMessage:
            def __init__(self, content, author, channel):
                self.content = content
                self.author = author
                self.channel = channel
            async def send(self, message):
                print(f"Bot response to web command: {message}")
                # For now, just return a success message.
                # A more complex system would queue messages to the web UI.
        
        # We'll use the user's ID to authorize the command
        mock_user = discord.Object(id=YOUR_USER_ID)
        mock_user.id = YOUR_USER_ID
        
        # A mock channel is needed to simulate where the command originated.
        # We'll just use a placeholder.
        mock_channel = discord.Object(id=None)
        
        mock_message = MockMessage(command, mock_user, mock_channel)

        await on_message(mock_message)

        # The bot's logic in on_message doesn't return a value, so we'll just send a general success message
        # If you need specific feedback, you would modify the on_message logic to send a message back to this handler.
        return web.json_response({'message': 'Command sent successfully. Check your Discord channel for bot response.'})

    except Exception as e:
        print(f"Error handling web command: {e}")
        return web.json_response({'error': str(e)}, status=500)


@tasks.loop(count=1)
async def web_server_task():
    """Starts the web server."""
    app = aiohttp.web.Application()
    
    # Configure CORS for all routes.
    cors = aiohttp_cors.setup(app, defaults={
        "*": aiohttp_cors.ResourceOptions(
            allow_credentials=True,
            expose_headers="*",
            allow_headers="*",
            allow_methods="*",
        )
    })
    
    # Add a route for the command API and apply the CORS configuration.
    command_route = app.router.add_post("/command", handle_command)
    cors.add(command_route)
    
    # Add a simple route for the root to make sure the server is responsive
    app.router.add_get("/", lambda r: aiohttp.web.Response(text="Bot web server is running!"))

    runner = aiohttp.web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080))
    site = aiohttp.web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    print(f"Web server started on port {port}")

# --- HELPER FUNCTIONS ---

async def announce_list(channel):
    data = load_data()
    if data["list"]:
        list_str = get_list_string(data["list"])
        await channel.send("Current list:\n" + "```\n" + list_str + "```")
    else:
        await channel.send("The list is currently empty.")

async def update_all_persistent_list_prompts(force_new=False):
    data = load_data()
    new_persistent_messages = []
    
    if force_new:
        data["persistent_messages"] = []

    for channel_id, message_id in data["persistent_messages"]:
        try:
            channel = client.get_channel(int(channel_id))
            message = await channel.fetch_message(int(message_id))
            new_content = "Current list:\n" + "```\n" + get_list_string(data["list"]) + "```"
            await message.edit(content=new_content)
            new_persistent_messages.append([channel_id, message_id])
        except (discord.NotFound, discord.Forbidden):
            print(f"Message {message_id} in channel {channel_id} not found or inaccessible, will resend.")
    
    if len(data["list"]) > 0 and (force_new or not new_persistent_messages):
        # We need to create a new message if none exist or if forced
        await announce_list(channel)

    data["persistent_messages"] = new_persistent_messages
    save_data(data)

async def close_all_lists(channel):
    data = load_data()
    
    for _, message_id in data["persistent_messages"]:
        try:
            message = await channel.fetch_message(int(message_id))
            await message.delete()
        except (discord.NotFound, discord.Forbidden):
            continue
    
    data["persistent_messages"] = []
    save_data(data)
    
    await channel.send("All persistent list messages have been removed.")


def update_data_for_auto(item_val, name_val):
    data = load_data()
    
    # Find item and update it
    for i in range(len(data['list'])):
        item = data['list'][i]
        if item_val in item:
            data['list'][i] = f"{item_val} owned by {name_val}"
            save_data(data)
            return data['list'][i] # Return the updated item for notifications
            
    # If not found, add a new one
    new_item = f"{item_val} owned by {name_val}"
    data['list'].append(new_item)
    save_data(data)
    return new_item

async def send_custom_update_notifications(item_val, name_val, updated_item):
    # This function is not implemented in the provided code
    # Placeholder for future functionality
    pass

# --- MAIN ENTRY POINT ---

if __name__ == "__main__":
    if not BOT_TOKEN:
        print("ERROR: BOT_TOKEN environment variable not set. Please configure it.")
    else:
        client.run(BOT_TOKEN)
