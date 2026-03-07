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

# --- FORCE NO-CACHE HEADERS ---
API_HEADERS = {"Cache-Control": "no-cache"}

# --- DAG MAPPING ---
DAG_MAP = {
    "01": "01_openaq_ingestion",
    "02": "02_openaq_transformation", 
    "99": "99_recover_datalake_to_bq"
}

# --- SECURITY DECORATOR ---
def is_authorized(user_id: str) -> bool:
    return user_id in ALLOWED_USERS

# --- UI HELPERS (SINGLE SOURCE OF TRUTH) ---
def get_dynamic_menu_keyboard() -> InlineKeyboardMarkup:
    """
    Generates a dynamic menu by fetching BOTH pause states and execution 
    states from Airflow, enforcing a Global UI Lock if any pipeline is running.
    """
    keyboard = []
    dag_states = {}
    any_running = False
    
    # 1. Fetch live truth from Airflow for all DAGs
    for dag_key in ["01", "02", "99"]:
        dag_id = DAG_MAP[dag_key]
        try:
            # Check Config State
            dag_info = get_dag_state(dag_id)
            is_paused = dag_info.get("is_paused", True)
            
            # Check Execution State
            url_runs = f"{AIRFLOW_API_BASE}/dags/{dag_id}/dagRuns?order_by=-start_date&limit=1"
            res_runs = requests.get(url_runs, auth=AIRFLOW_AUTH, headers=API_HEADERS)
            res_runs.raise_for_status()
            data_runs = res_runs.json()
            
            is_running = False
            if data_runs.get("total_entries", 0) > 0:
                latest_state = data_runs["dag_runs"][0].get("state")
                if latest_state in ["running", "queued"]:
                    is_running = True
                    any_running = True
                    
            dag_states[dag_key] = {"paused": is_paused, "running": is_running, "unreachable": False}
        except Exception as e:
            logger.error(f"Error fetching true state for {dag_id}: {e}")
            dag_states[dag_key] = {"paused": True, "running": False, "unreachable": True}
            
    # 2. Build UI Contextually
    for dag_key in ["01", "02", "99"]:
        state = dag_states[dag_key]
        
        # Fallback for API errors
        if state["unreachable"]:
            keyboard.append([
                InlineKeyboardButton(f"⚠️ {dag_key} Unreachable", callback_data="ignore"),
                InlineKeyboardButton("⚠️ Error", callback_data="ignore")
            ])
            continue
            
        # Context-aware Config Toggle
        toggle_btn = InlineKeyboardButton(
            f"▶️ Resume {dag_key}" if state["paused"] else f"⏸️ Pause {dag_key}", 
            callback_data=f"unpause_{dag_key}" if state["paused"] else f"pause_{dag_key}"
        )
            
        # Context-aware Execution Button (Global Lock Logic)
        if any_running:
            if state["running"]:
                exec_btn = InlineKeyboardButton("⏳ Executing...", callback_data="ignore")
            else:
                exec_btn = InlineKeyboardButton("🔒 Locked", callback_data="ignore")
        else:
            if dag_key == "01":
                exec_btn = InlineKeyboardButton("🔄 Retry 01", callback_data="retry_01")
            elif dag_key == "02":
                exec_btn = InlineKeyboardButton("▶️ Run 02", callback_data="run_02")
            elif dag_key == "99":
                exec_btn = InlineKeyboardButton("🚨 Run 99", callback_data="run_99")
                
        keyboard.append([toggle_btn, exec_btn])
        
    keyboard.append([InlineKeyboardButton("🚦 Pipeline Overview", callback_data="check_status")])
    return InlineKeyboardMarkup(keyboard)

# --- AIRFLOW API HELPERS ---
def get_dag_state(dag_id: str) -> dict:
    url = f"{AIRFLOW_API_BASE}/dags/{dag_id}"
    response = requests.get(url, auth=AIRFLOW_AUTH, headers=API_HEADERS)
    response.raise_for_status()
    return response.json()

def patch_dag_state(dag_id: str, is_paused: bool) -> bool:
    url = f"{AIRFLOW_API_BASE}/dags/{dag_id}"
    payload = {"is_paused": is_paused}
    response = requests.patch(url, json=payload, auth=AIRFLOW_AUTH)
    return response.status_code == 200

def trigger_dag(dag_id: str) -> str:
    url = f"{AIRFLOW_API_BASE}/dags/{dag_id}/dagRuns"
    response = requests.post(url, json={}, auth=AIRFLOW_AUTH)
    if response.status_code == 200:
        return response.json().get("dag_run_id")
    return None

def retry_latest_run(dag_id: str) -> tuple[str, str]:
    url_get = f"{AIRFLOW_API_BASE}/dags/{dag_id}/dagRuns?order_by=-start_date&limit=1"
    try:
        response = requests.get(url_get, auth=AIRFLOW_AUTH, headers=API_HEADERS)
        response.raise_for_status()
        data = response.json()
        
        if data.get("total_entries", 0) == 0:
            return f"❌ No runs found for `{dag_id}` to retry.", None
            
        latest_run = data["dag_runs"][0]
        run_id = latest_run["dag_run_id"]
        
        url_clear = f"{AIRFLOW_API_BASE}/dags/{dag_id}/dagRuns/{run_id}/clear"
        payload = {"dry_run": False}
        clear_response = requests.post(url_clear, json=payload, auth=AIRFLOW_AUTH)
        clear_response.raise_for_status()
        
        return f"🔄 *{dag_id}* cleared.\nRun `{run_id}` is queued for retry.", run_id
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error retrying latest run for {dag_id}: {e}")
        return f"❌ *API Error during retry:*\n`{e}`", None

def get_latest_run_status(dag_id: str) -> str:
    try:
        dag_info = get_dag_state(dag_id)
        is_paused = dag_info.get("is_paused", True)
    except Exception as e:
        logger.error(f"Error fetching DAG state for {dag_id}: {e}")
        return f"⚠️ *{dag_id}*\n└ ❌ Error connecting to API"

    pause_icon = "⏸️ PAUSED" if is_paused else "▶️ ACTIVE"
    url = f"{AIRFLOW_API_BASE}/dags/{dag_id}/dagRuns?order_by=-start_date&limit=1"
    
    try:
        response = requests.get(url, auth=AIRFLOW_AUTH, headers=API_HEADERS)
        if response.status_code == 200:
            data = response.json()
            if data.get("total_entries", 0) > 0:
                run = data["dag_runs"][0]
                state = run.get("state") 
                
                end_date_str = run.get("end_date")
                if end_date_str:
                    clean_time = end_date_str.split(".")[0].replace("T", " ")
                    time_display = f"{clean_time} UTC"
                else:
                    time_display = "⏳ Running now..."
                
                icon = "✅" if state == "success" else "❌" if state == "failed" else "🔄"
                
                return (
                    f"{icon} *{dag_id}*\n"
                    f"├ Config: `{pause_icon}`\n"
                    f"├ Status: `{state.upper()}`\n"
                    f"└ Ended:  `{time_display}`\n"
                )
                
            return f"⚪ *{dag_id}*\n├ Config: `{pause_icon}`\n└ Status: `No runs yet`\n"
    except Exception as e:
        logger.error(f"Error fetching run status for {dag_id}: {e}")
        
    return f"⚠️ *{dag_id}*\n└ ❌ Error fetching runs"

# --- ASYNC BACKGROUND JOBS ---
async def auto_refresh_menu(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Background job that pulls the true state from Airflow every 10s and updates the UI silently."""
    job = context.job
    chat_id = job.data.get("chat_id")
    message_id = job.data.get("msg_id")
    
    try:
        # Fetch absolute truth from API
        new_markup = get_dynamic_menu_keyboard()
        await context.bot.edit_message_reply_markup(
            chat_id=chat_id,
            message_id=message_id,
            reply_markup=new_markup
        )
    except Exception as e:
        # Ignore telegram.error.BadRequest when the menu hasn't visually changed
        if "not modified" not in str(e).lower():
            logger.error(f"UI Auto-refresh error: {e}")

def schedule_menu_refresh(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int):
    """Kills any old refresh loops for this chat and starts a new one pinned to the current menu."""
    job_name = f"menu_updater_{chat_id}"
    current_jobs = context.job_queue.get_jobs_by_name(job_name)
    for job in current_jobs:
        job.schedule_removal()
        
    # Schedule UI redraw every 10 seconds
    context.job_queue.run_repeating(
        auto_refresh_menu, 
        interval=10, 
        first=5, 
        data={"chat_id": chat_id, "msg_id": message_id},
        name=job_name
    )

async def monitor_dag_run(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Asynchronous background job that alerts upon execution completion."""
    job = context.job
    chat_id = job.data.get("chat_id")
    dag_id = job.data.get("dag_id")
    run_id = job.data.get("run_id")

    url = f"{AIRFLOW_API_BASE}/dags/{dag_id}/dagRuns/{run_id}"
    try:
        response = requests.get(url, auth=AIRFLOW_AUTH, headers=API_HEADERS)
        if response.status_code == 200:
            state = response.json().get("state")
            
            if state in ["success", "failed"]:
                job.schedule_removal()
                icon = "✅" if state == "success" else "❌"
                alert_msg = (
                    f"{icon} *Execution Completed*\n\n"
                    f"📦 *DAG:* `{dag_id}`\n"
                    f"🔖 *Run ID:* `{run_id}`\n"
                    f"📊 *Final Status:* `{state.upper()}`"
                )
                
                rescue_keyboard = InlineKeyboardMarkup([[
                    InlineKeyboardButton("🎛️ Open Command Center", callback_data="spawn_menu")
                ]])
                
                await context.bot.send_message(
                    chat_id=chat_id, 
                    text=alert_msg, 
                    reply_markup=rescue_keyboard,
                    parse_mode="Markdown"
                )
    except Exception as e:
        logger.error(f"Error during async polling for {dag_id}: {e}")
        job.schedule_removal()

# --- BOT HANDLERS ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = str(update.effective_user.id)
    chat_id = update.effective_chat.id
    
    if not is_authorized(user_id):
        await update.message.reply_text("⛔ *Access Denied*", parse_mode="Markdown")
        return

    menu_text = "🎛️ *Airflow Command Center*\nSelect an operational task:"
    msg = await update.message.reply_text(menu_text, reply_markup=get_dynamic_menu_keyboard(), parse_mode="Markdown")
    
    # Anchor the auto-refresh loop to this new menu
    schedule_menu_refresh(context, chat_id, msg.message_id)

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user_id = str(query.from_user.id)
    chat_id = update.effective_chat.id

    if not is_authorized(user_id):
        await query.answer("Unauthorized.", show_alert=True)
        return

    await query.answer()
    action = query.data

    if action == "ignore":
        return

    try:
        if action == "spawn_menu":
            await query.edit_message_reply_markup(reply_markup=None)
            msg = await context.bot.send_message(
                chat_id=chat_id, 
                text="🎛️ *Airflow Command Center*\nSelect an operational task:", 
                reply_markup=get_dynamic_menu_keyboard(), 
                parse_mode="Markdown"
            )
            schedule_menu_refresh(context, chat_id, msg.message_id)
            return

        if action == "check_status":
            job_name = f"menu_updater_{chat_id}"
            current_jobs = context.job_queue.get_jobs_by_name(job_name)
            for job in current_jobs:
                job.schedule_removal()

            await query.edit_message_text("🔍 *Fetching infrastructure status...*", parse_mode="Markdown")
            s1 = get_latest_run_status(DAG_MAP["01"])
            s2 = get_latest_run_status(DAG_MAP["02"])
            s99 = get_latest_run_status(DAG_MAP["99"])
            report = f"🚦 *Pipeline Overview*\n\n{s1}\n{s2}\n{s99}\n\n_Use the menu below to navigate._"
            
            keyboard = [[InlineKeyboardButton("🔙 Return to Menu", callback_data="spawn_menu")]]
            await query.edit_message_text(report, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
            return

        action_type, dag_key = action.split("_")  
        dag_id = DAG_MAP.get(dag_key)

        if not dag_id:
            await query.edit_message_text("❌ *Error:* Unknown DAG mapping.\nType /start for menu.", parse_mode="Markdown")
            return

        # State Operations
        if action_type in ["unpause", "pause"]:
            patch_dag_state(dag_id, is_paused=(action_type == "pause"))
            await query.answer(f"{dag_id} {'paused' if action_type == 'pause' else 'unpaused'}.", show_alert=False)
            # Force immediate redraw instead of waiting 10s for the background loop
            await query.edit_message_reply_markup(reply_markup=get_dynamic_menu_keyboard())

        # Execution Operations
        elif action_type == "run":
            run_id = trigger_dag(dag_id)
            if run_id:
                # Force immediate redraw (will lock all other buttons)
                await query.edit_message_reply_markup(reply_markup=get_dynamic_menu_keyboard())
                context.job_queue.run_repeating(
                    monitor_dag_run, interval=15, first=5, 
                    data={"chat_id": chat_id, "dag_id": dag_id, "run_id": run_id}
                )
                await query.answer(f"Triggered! Monitoring run: {run_id}", show_alert=False)
            else:
                await query.answer("Failed to trigger DAG.", show_alert=True)

        elif action_type == "retry":
            result_msg, run_id = retry_latest_run(dag_id)
            if run_id:
                await query.edit_message_reply_markup(reply_markup=get_dynamic_menu_keyboard())
                context.job_queue.run_repeating(
                    monitor_dag_run, interval=15, first=5, 
                    data={"chat_id": chat_id, "dag_id": dag_id, "run_id": run_id}
                )
                await query.answer(f"Retry queued! Monitoring run: {run_id}", show_alert=False)
            else:
                await query.answer("Failed to queue retry.", show_alert=True)
                
    except requests.exceptions.RequestException as e:
        logger.error(f"Airflow API Error: {e}")
        await query.answer("API Error.", show_alert=True)

def main() -> None:
    if not TOKEN or not ALLOWED_USERS:
        logger.error("Environment variables are missing. Exiting.")
        return

    application = Application.builder().token(TOKEN).build()
    
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button_handler))

    logger.info("ChatOps SRE Bot is online and ready...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()