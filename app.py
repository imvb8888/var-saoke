import pandas as pd
import os
import logging
import requests
from flask import Flask, request
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters

# Set up logging
logging.basicConfig(level=logging.WARNING)

# Hardcoded CSV file path and bot token
csv_file = "saoke_vietinbank.csv"  # Make sure to upload this to your deployment

bot_token = os.getenv('BOT_TOKEN')

if not bot_token:
    raise ValueError("Bot token not set. Please set the 'BOT_TOKEN' environment variable.")

# webhook_url = "https://var-saoke.onrender.com"  # Update with your actual URL

# Load the CSV file into a pandas DataFrame
df = pd.read_csv(csv_file)

# Create Flask app
app = Flask(__name__)


# Function to split large text into chunks while keeping rows intact
def split_text_by_row(rows, max_size=4000):
    chunks = []
    current_chunk = ""

    for row in rows:
        formatted_row = row + "\n\n"  # Add a line break between rows
        if len(current_chunk) + len(formatted_row) <= max_size:
            current_chunk += formatted_row
        else:
            # When the current chunk reaches the limit, start a new chunk
            chunks.append(current_chunk.strip())  # Append current chunk
            current_chunk = formatted_row  # Start new chunk with the current row

    # Add the last chunk if it's not empty
    if current_chunk:
        chunks.append(current_chunk.strip())

    return chunks


# Function to search the CSV for a keyword
def search_keyword_in_csv(df, keyword):
    # Perform a case-insensitive search across all columns
    mask = df.apply(lambda row: row.astype(str).str.contains(keyword, case=False).any(), axis=1)
    result_df = df[mask]

    # If no results are found, return a "not found" message
    if result_df.empty:
        return f"Khong tim thay '{keyword}'"

    # Format the result: no headers, no row numbers, and space between rows
    result_text = result_df.apply(lambda row: " | ".join(row.astype(str)), axis=1).tolist()
    return result_text


# Async handler for the /start command
async def start(update: Update, context):
    await update.message.reply_text('Xin chao to VAR! Nhap tu khoa de tim kiem sao ke')


# Async handler for messages (keyword search)
async def handle_message(update: Update, context):
    keyword = update.message.text  # The keyword user sends

    result = search_keyword_in_csv(df, keyword)  # Search the CSV

    if isinstance(result, str):  # If result is a "not found" message
        await update.message.reply_text(result)
    else:
        # Split the result into chunks without splitting rows across messages
        result_chunks = split_text_by_row(result)

        # Send each chunk as a separate message
        for chunk in result_chunks:
            await update.message.reply_text(chunk)


# Create and configure Application
application = Application.builder().token(bot_token).build()
application.add_handler(CommandHandler("start", start))
application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))


# Flask route for handling Telegram webhooks
@app.route(f"/{bot_token}", methods=['POST'])
async def telegram_webhook():
    json_update = request.get_json()
    print(f"Received update: {json_update}")
    update = Update.de_json(json_update, application.bot)

    # Process the update in the background asynchronously
    await application.process_update(update)

    return 'OK', 200


# Function to set up webhook
def set_webhook():
    webhook_url = os.getenv('WEBHOOK_URL')
    if not webhook_url:
        raise ValueError("WEBHOOK_URL environment variable not set")

    url = f"https://api.telegram.org/bot{bot_token}/setWebhook"
    response = requests.post(url, json={"url": f"{webhook_url}/{bot_token}"})
    if response.status_code == 200:
        print("Webhook set successfully!")
    else:
        print(f"Failed to set webhook: {response.text}")


if __name__ == "__main__":
    set_webhook()  # Set the webhook when starting the server
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
