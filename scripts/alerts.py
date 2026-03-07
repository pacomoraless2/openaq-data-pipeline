import os
import requests
import logging

logger = logging.getLogger(__name__)

def telegram_failure_callback(context):
    """
    Callback function executed automatically upon task failure in Airflow.
    Extracts the error context and sends a formatted alert via Telegram to all authorized SREs.
    """
    bot_token = os.environ.get("AIRFLOW_VAR_TELEGRAM_BOT_TOKEN")
    # Use the access control list (whitelist)
    allowed_users_str = os.environ.get("AIRFLOW_VAR_TELEGRAM_ALLOWED_USERS", "")

    if not bot_token or not allowed_users_str:
        logger.warning("Telegram credentials or allowed users not found. Skipping alert.")
        return

    allowed_users = allowed_users_str.split(",")

    # Extract execution details from the Airflow context
    task_instance = context.get('task_instance')
    task_id = task_instance.task_id
    dag_id = task_instance.dag_id
    logical_date = context.get('logical_date')

    message = (
        f"🚨 *Airflow Task Failure* 🚨\n\n"
        f"📊 *DAG:* `{dag_id}`\n"
        f"❌ *Task:* `{task_id}`\n"
        f"📅 *Logical Date:* `{logical_date}`\n\n"
    )

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    
    # Notify each user in the whitelist
    for chat_id in allowed_users:
        payload = {
            "chat_id": chat_id.strip(),
            "text": message,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True
        }
        try:
            response = requests.post(url, json=payload)
            response.raise_for_status()
            logger.info(f"Telegram alert successfully sent to user {chat_id}.")
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to send Telegram alert to user {chat_id}: {e}")