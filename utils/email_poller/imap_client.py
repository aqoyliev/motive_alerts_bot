import imaplib
import email
import logging

logger = logging.getLogger(__name__)

IMAP_SERVER = "imap.gmail.com"
IMAP_PORT = 993
MOTIVE_SENDER = "notifications@gomotive.com"


def fetch_unread_motive_emails(gmail_user: str, gmail_password: str) -> list[str]:
    """
    Connect to Gmail via IMAP, fetch unread emails from Motive, mark them read.
    Returns a list of plain-text email bodies.
    """
    bodies = []

    try:
        mail = imaplib.IMAP4_SSL(IMAP_SERVER, IMAP_PORT)
        mail.login(gmail_user, gmail_password)
        mail.select("INBOX")

        status, data = mail.search(None, f'(UNSEEN FROM "{MOTIVE_SENDER}")')
        if status != "OK" or not data[0]:
            mail.logout()
            return bodies

        for email_id in data[0].split():
            status, msg_data = mail.fetch(email_id, "(RFC822)")
            if status != "OK":
                continue

            msg = email.message_from_bytes(msg_data[0][1])
            body = _extract_text(msg)
            if body:
                bodies.append(body)

            mail.store(email_id, "+FLAGS", "\\Seen")

        mail.logout()

    except Exception as e:
        logger.error(f"IMAP error: {e}")

    return bodies


def _extract_text(msg) -> str:
    """Extract plain text from email, falling back to HTML→text."""
    plain = ""
    html = ""

    if msg.is_multipart():
        for part in msg.walk():
            ct = part.get_content_type()
            payload = part.get_payload(decode=True)
            if not payload:
                continue
            decoded = payload.decode("utf-8", errors="replace")
            if ct == "text/plain" and not plain:
                plain = decoded
            elif ct == "text/html" and not html:
                html = decoded
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            decoded = payload.decode("utf-8", errors="replace")
            if msg.get_content_type() == "text/html":
                html = decoded
            else:
                plain = decoded

    if plain:
        return plain
    if html:
        return _html_to_text(html)
    return ""


def _html_to_text(html: str) -> str:
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")
        lines = [line.strip() for line in soup.get_text(separator="\n").splitlines()]
        return "\n".join(line for line in lines if line)
    except Exception:
        return html
