"""
Email notifications for alerts, sent over SMTP.

All settings come from .env (via src.config):
  - Mailpit (default, local test inbox): SMTP_HOST=localhost, SMTP_PORT=1025
  - Real mail, e.g. Gmail:  SMTP_HOST=smtp.gmail.com, SMTP_PORT=587,
                            SMTP_USE_TLS=true, SMTP_USERNAME / SMTP_PASSWORD
  - Disabled:               SMTP_HOST= (empty)

Run from the project root to send a test email:  python -m src.notifier
"""
import logging
import smtplib
import ssl
from email.message import EmailMessage

from src.config import (
    ALERT_EMAIL_FROM,
    ALERT_EMAIL_TO,
    SMTP_HOST,
    SMTP_PASSWORD,
    SMTP_PORT,
    SMTP_USE_TLS,
    SMTP_USERNAME,
)

logger = logging.getLogger("notifier")

SMTP_TIMEOUT_SECONDS = 10


def email_enabled() -> bool:
    """True if there is a mail server and a recipient configured."""
    return bool(SMTP_HOST and ALERT_EMAIL_TO)


def send_email(subject: str, body: str) -> bool:
    """Send one plain-text email. Returns True if the server accepted it."""
    if not email_enabled():
        return False

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = ALERT_EMAIL_FROM
    message["To"] = ALERT_EMAIL_TO
    message.set_content(body)

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=SMTP_TIMEOUT_SECONDS) as smtp:
            if SMTP_USE_TLS:
                smtp.starttls(context=ssl.create_default_context())  # encrypt + verify certificate
            if SMTP_USERNAME:
                smtp.login(SMTP_USERNAME, SMTP_PASSWORD)
            smtp.send_message(message)
        return True
    except (smtplib.SMTPException, OSError) as error:  # bad login, server down, timeout ...
        logger.error("Email not sent (%s:%s): %s", SMTP_HOST, SMTP_PORT, error)
        return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    if not email_enabled():
        print("Email is disabled: set SMTP_HOST and ALERT_EMAIL_TO in .env")
    else:
        sent = send_email(
            "Heartbeat monitor: test email",
            "If you can read this, email alerts are working.",
        )
        print(f"Test email to {ALERT_EMAIL_TO} via {SMTP_HOST}:{SMTP_PORT} -> sent = {sent}")