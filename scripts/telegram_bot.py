import os
import requests
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

# --- CONFIGURATION & LOGGING ---
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- ENVIRONMENT VARIABLES ---
TOKEN = os.environ.get("AIRFLOW_VAR_TELEGRAM_BOT_TOKEN")
ALLOWED_USERS = os.environ.get("AIRFLOW_VAR_TELEGRAM_ALLOWED_USERS", "").split(",")

AIRFLOW_API_BASE = os.environ.get("AIRFLOW_VAR_API_BASE_URL", "http://airflow-webserver:8080/api/v1")
AIRFLOW_USER = os.environ.get("AIRFLOW_VAR_API_USER", "airflow")
AIRFLOW_PASS = os.environ.get("AIRFLOW_VAR_API_PASS", "airflow")
AIRFLOW_AUTH = (AIRFLOW_USER, AIRFLOW_PASS)

# --- DAG MAPPING ---
DAG_MAP = {
    "01": "01_openaq_ingestion",
    "02": "02_openaq_transformation", 
    "99": "99_recover_datalake_to_bq"
}

# --- SECURITY DECORATOR ---
def is_authorized(user_id: str) -> bool:
    """Validates if the user is in the allowed VIP list."""
    return user_id in ALLOWED_USERS

# --- AIRFLOW API HELPERS ---
def get_dag_state(dag_id: str) -> dict:
    """Fetches the current configuration state of a DAG."""
    url = f"{AIRFLOW_API_BASE}/dags/{dag_id}"
    response = requests.get(url, auth=AIRFLOW_AUTH)
    response.raise_for_status()
    return response.json()

def patch_dag_state(dag_id: str, is_paused: bool) -> bool:
    """Pauses or unpauses a DAG."""
    url = f"{AIRFLOW_API_BASE}/dags/{dag_id}"
    payload = {"is_paused": is_paused}
    response = requests.patch(url, json=payload, auth=AIRFLOW_AUTH)
    return response.status_code == 200

def trigger_dag(dag_id: str) -> bool:
    """Triggers a new DAG run."""
    url = f"{AIRFLOW_API_BASE}/dags/{dag_id}/dagRuns"
    response = requests.post(url, json={}, auth=AIRFLOW_AUTH)
    return response.status_code == 200

def get_latest_run_status(dag_id: str) -> str:
    """Fetches the PAUSE state and the most recent run details."""
    # 1. Check if DAG is paused
    try:
        dag_info = get_dag_state(dag_id)
        is_paused = dag_info.get("is_paused", True)
    except Exception as e:
        logger.error(f"Error fetching DAG state for {dag_id}: {e}")
        return f"⚠️ *{dag_id}*\n└ ❌ Error connecting to API"

    pause_icon = "⏸️ PAUSED" if is_paused else "▶️ ACTIVE"

    # 2. Check the latest run details
    url = f"{AIRFLOW_API_BASE}/dags/{dag_id}/dagRuns?order_by=-execution_date&limit=1"
    try:
        response = requests.get(url, auth=AIRFLOW_AUTH)
        if response.status_code == 200:
            data = response.json()
            if data.get("total_entries", 0) > 0:
                run = data["dag_runs"][0]
                state = run.get("state") 
                
                # Extract and format the end_date (Airflow returns UTC, e.g., 2026-03-04T20:55:34.112Z)
                end_date_str = run.get("end_date")
                if end_date_str:
                    # Clean up the string to show just YYYY-MM-DD HH:MM
                    clean_time = end_date_str.split(".")[0].replace("T", " ")
                    time_display = f"{clean_time} UTC"
                else:
                    time_display = "⏳ Running now..."
                
                # Visual state indicators
                icon = "✅" if state == "success" else "❌" if state == "failed" else "🔄"
                
                # Format the output as a clean tree
                return (
                    f"{icon} *{dag_id}*\n"
                    f"├ Config: `{pause_icon}`\n"
                    f"├ Status: `{state.upper()}`\n"
                    f"└ Ended:  `{time_display}`\n"
                )
                
            return (
                f"⚪ *{dag_id}*\n"
                f"├ Config: `{pause_icon}`\n"
                f"└ Status: `No runs yet`\n"
            )
    except Exception as e:
        logger.error(f"Error fetching run status for {dag_id}: {e}")
        
    return f"⚠️ *{dag_id}*\n└ ❌ Error fetching runs"

# --- BOT HANDLERS ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Sends the main interactive menu if the user is authorized."""
    user_id = str(update.effective_user.id)
    
    if not is_authorized(user_id):
        logger.warning(f"Unauthorized access attempt from User ID: {user_id}")
        # Mensaje explícito de rechazo
        await update.message.reply_text(
            "⛔ *Access Denied*\nYou do not have the necessary permissions to operate this infrastructure.", 
            parse_mode="Markdown"
        )
        return

    # Structured Keyboard Layout
    keyboard = [
        [InlineKeyboardButton("🔓 Unpause 01", callback_data="unpause_01"), InlineKeyboardButton("⏸️ Pause 01", callback_data="pause_01")],
        [InlineKeyboardButton("▶️ Run 01: Ingestion", callback_data="run_01")],
        
        [InlineKeyboardButton("🔓 Unpause 02", callback_data="unpause_02"), InlineKeyboardButton("⏸️ Pause 02", callback_data="pause_02")],
        [InlineKeyboardButton("▶️ Run 02: Transform", callback_data="run_02")],
        
        [InlineKeyboardButton("🔓 Unpause 99", callback_data="unpause_99"), InlineKeyboardButton("⏸️ Pause 99", callback_data="pause_99")],
        [InlineKeyboardButton("🚨 Run 99: DISASTER RECOVERY", callback_data="run_99")],
        
        [InlineKeyboardButton("📋 Check Pipelines Status", callback_data="check_status")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    menu_text = "🎛️ *Airflow Command Center*\nSelect an operation:"

    # If it's a button click (callback) edit the message, if it's the text command /start send a new one
    if hasattr(update, 'callback_query') and update.callback_query:
        await update.callback_query.edit_message_text(menu_text, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        await update.message.reply_text(menu_text, reply_markup=reply_markup, parse_mode="Markdown")

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles all button clicks and routes them dynamically."""
    query = update.callback_query
    user_id = str(query.from_user.id)

    if not is_authorized(user_id):
        await query.answer("Unauthorized.", show_alert=True)
        return

    await query.answer()
    action = query.data

    try:
        if action == "check_status":
            await query.edit_message_text("🔍 *Fetching infrastructure status...*", parse_mode="Markdown")
            
            s1 = get_latest_run_status(DAG_MAP["01"])
            s2 = get_latest_run_status(DAG_MAP["02"])
            s99 = get_latest_run_status(DAG_MAP["99"])
            
            report = (
                "📊 *Pipelines Status Report*\n\n"
                f"{s1}\n"
                f"{s2}\n"
                f"{s99}\n"
                "Type /start to return to the menu."
            )
            await query.edit_message_text(report, parse_mode="Markdown")
            return

        # Dynamic routing for Unpause / Pause / Run
        action_type, dag_key = action.split("_")  
        dag_id = DAG_MAP.get(dag_key)

        if not dag_id:
            await query.edit_message_text("❌ *Error:* Unknown DAG configuration.\nType /start for menu.", parse_mode="Markdown")
            return

        if action_type == "unpause":
            patch_dag_state(dag_id, is_paused=False)
            await query.edit_message_text(f"🔓 *{dag_id}* is now UNPAUSED.\nType /start for menu.", parse_mode="Markdown")

        elif action_type == "pause":
            patch_dag_state(dag_id, is_paused=True)
            await query.edit_message_text(f"⏸️ *{dag_id}* is now PAUSED.\nType /start for menu.", parse_mode="Markdown")

        elif action_type == "run":
            dag_state = get_dag_state(dag_id)
            if dag_state.get("is_paused"):
                await query.edit_message_text(
                    f"❌ *Operation Aborted:*\n`{dag_id}` is currently PAUSED.\nYou must unpause it first.\n\nType /start for menu.", 
                    parse_mode="Markdown"
                )
            else:
                trigger_dag(dag_id)
                await query.edit_message_text(f"🚀 *{dag_id}* triggered successfully.\nType /start for menu.", parse_mode="Markdown")

    except requests.exceptions.RequestException as e:
        logger.error(f"Airflow API Error: {e}")
        await query.edit_message_text(f"❌ *API Error:*\n`{e}`\nType /start for menu.", parse_mode="Markdown")

def main() -> None:
    if not TOKEN or not ALLOWED_USERS:
        logger.error("Environment variables are missing. Exiting.")
        return

    application = Application.builder().token(TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button_handler))

    logger.info("ChatOps Bot v3.1 is online...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()