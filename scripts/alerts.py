import os
import requests
import logging

logger = logging.getLogger(__name__)

def telegram_failure_callback(context):
    """
    Callback function executed automatically upon task failure in Airflow.
    Extracts the error context and sends a formatted alert via Telegram.
    """
    bot_token = os.environ.get("AIRFLOW_VAR_TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("AIRFLOW_VAR_TELEGRAM_CHAT_ID")

    if not bot_token or not chat_id:
        logger.warning("Telegram credentials not found. Skipping alert.")
        return

    # Extract execution details from the Airflow context
    task_instance = context.get('task_instance')
    task_id = task_instance.task_id
    dag_id = task_instance.dag_id
    logical_date = context.get('logical_date')
    log_url = task_instance.log_url

    # Construct the Markdown formatted message
    message = (
        f"🚨 *Airflow Task Failure* 🚨\n\n"
        f"📊 *DAG:* `{dag_id}`\n"
        f"❌ *Task:* `{task_id}`\n"
        f"📅 *Logical Date:* `{logical_date}`\n\n"
        f"🔗 *Log URL:*\n{log_url}"
    )

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True
    }

    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        logger.info("Telegram alert sent successfully.")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to send Telegram alert: {e}")