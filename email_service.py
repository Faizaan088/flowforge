"""Email delivery service for FlowForge.

Supports real SMTP with TLS/SSL when configured via environment variables,
with clearly identified development fallback logging for testing environments.
"""

from __future__ import annotations

import logging
import os
import smtplib
from email.message import EmailMessage
from typing import List, Optional

logger = logging.getLogger("flowforge.email")

# In-memory history for local development/testing inspection only
_RECENT_DISPATCHED_EMAILS: List[dict] = []


def is_smtp_configured() -> bool:
    """Return True if required SMTP settings are present and non-empty."""
    host = os.getenv("SMTP_HOST")
    from_addr = os.getenv("SMTP_FROM")
    return bool(host and host.strip() and from_addr and from_addr.strip())


def get_recent_emails() -> List[dict]:
    """Return recent dispatched emails (for test verification)."""
    return list(_RECENT_DISPATCHED_EMAILS)


get_sent_emails = get_recent_emails


def clear_recent_emails() -> None:
    """Clear test email buffer."""
    _RECENT_DISPATCHED_EMAILS.clear()


clear_sent_emails = clear_recent_emails


def send_email(
    to_email: str,
    subject: str,
    text_content: str,
    html_content: Optional[str] = None,
    metadata: Optional[dict] = None,
) -> bool:
    """Send an email using real SMTP if configured, or record it in development buffer.

    Raises RuntimeError if SMTP is explicitly configured but delivery fails.
    """
    to_email = to_email.strip()
    if not to_email:
        raise ValueError("Recipient email cannot be empty")

    smtp_host = (os.getenv("SMTP_HOST") or "").strip()
    smtp_from = (os.getenv("SMTP_FROM") or "no-reply@flowforge.dev").strip()
    smtp_port_raw = os.getenv("SMTP_PORT", "587")
    try:
        smtp_port = int(float(str(smtp_port_raw).strip()))
    except (ValueError, TypeError):
        smtp_port = 587
    smtp_user = os.getenv("SMTP_USER")
    smtp_pass = os.getenv("SMTP_PASSWORD")
    smtp_tls = os.getenv("SMTP_TLS", "true").lower() in ("true", "1", "yes")

    is_configured = bool(smtp_host and smtp_from)

    record = {
        "to": to_email,
        "subject": subject,
        "text": text_content,
        "html": html_content,
        "metadata": metadata or {},
        "simulated": not is_configured,
        "delivery_mode": "smtp" if is_configured else "dev_fallback",
    }
    _RECENT_DISPATCHED_EMAILS.append(record)

    if not is_configured:
        # Development fallback: never claim real delivery occurred
        logger.info(
            "[DEV-ONLY FALLBACK] Real SMTP not configured; recorded simulated email to '%s' with subject '%s'",
            to_email,
            subject,
        )
        return True

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = smtp_from
    msg["To"] = to_email
    msg.set_content(text_content)
    if html_content:
        msg.add_alternative(html_content, subtype="html")

    try:
        if smtp_tls and smtp_port == 465:
            server = smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=10)
        else:
            server = smtplib.SMTP(smtp_host, smtp_port, timeout=10)
            if smtp_tls:
                server.starttls()

        if smtp_user and smtp_pass:
            server.login(smtp_user, smtp_pass)

        server.send_message(msg)
        server.quit()
        logger.info("Successfully dispatched email via SMTP to '%s'", to_email)
        return True
    except Exception as exc:
        logger.error("Failed to deliver email via SMTP to '%s': %s", to_email, exc)
        raise RuntimeError(f"Email delivery service failure: {exc}") from exc


def send_password_reset_email(
    to_email: str,
    token: str,
    frontend_url: Optional[str] = None,
    username: Optional[str] = None,
    reset_url: Optional[str] = None,
    **kwargs,
) -> bool:
    """Send a secure password reset link to user."""
    base_url = frontend_url or os.getenv("FRONTEND_BASE_URL", "http://localhost:3000")
    link = reset_url or f"{base_url.rstrip('/')}/reset-password?token={token}"
    user_display = username or "User"
    subject = "Reset your FlowForge Password"
    text = (
        f"Hello {user_display},\n\n"
        f"You requested a password reset for your FlowForge account.\n\n"
        f"Click the link below to reset your password (valid for 1 hour):\n"
        f"{link}\n\n"
        f"If you did not request this reset, you can safely ignore this email.\n"
    )
    html = f"""
    <div style="font-family: sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
      <h2 style="color: #06b6d4;">FlowForge Security</h2>
      <p>Hello {user_display},</p>
      <p>You requested a password reset for your FlowForge account.</p>
      <p>
        <a href="{link}" style="display: inline-block; background-color: #06b6d4; color: #020617; padding: 10px 20px; text-decoration: none; border-radius: 8px; font-weight: bold;">
          Reset Password
        </a>
      </p>
      <p style="font-size: 12px; color: #64748b;">This link expires in 1 hour. If you did not request this, please ignore this email.</p>
    </div>
    """
    return send_email(to_email, subject, text, html, metadata={"token": token, "username": username, "url": link})


def send_verification_email(
    to_email: str,
    token: str,
    frontend_url: Optional[str] = None,
    username: Optional[str] = None,
    verification_url: Optional[str] = None,
    **kwargs,
) -> bool:
    """Send an email verification link to user."""
    base_url = frontend_url or os.getenv("FRONTEND_BASE_URL", "http://localhost:3000")
    link = verification_url or f"{base_url.rstrip('/')}/verify-email?token={token}"
    user_display = username or "User"
    subject = "Verify your FlowForge Email Address"
    text = (
        f"Welcome to FlowForge, {user_display}!\n\n"
        f"Please verify your email address by clicking the link below:\n"
        f"{link}\n\n"
        f"This verification link is valid for 24 hours.\n"
    )
    html = f"""
    <div style="font-family: sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
      <h2 style="color: #06b6d4;">FlowForge Welcome</h2>
      <p>Thank you for joining FlowForge, {user_display}. Please verify your email address to activate your account features.</p>
      <p>
        <a href="{link}" style="display: inline-block; background-color: #06b6d4; color: #020617; padding: 10px 20px; text-decoration: none; border-radius: 8px; font-weight: bold;">
          Verify Email Address
        </a>
      </p>
      <p style="font-size: 12px; color: #64748b;">This link expires in 24 hours.</p>
    </div>
    """
    return send_email(to_email, subject, text, html, metadata={"token": token, "username": username, "url": link})


def send_invitation_email(
    to_email: str,
    token: str,
    frontend_url: Optional[str] = None,
    username: Optional[str] = None,
    invitation_url: Optional[str] = None,
    **kwargs,
) -> bool:
    """Send an onboarding invitation link for a new user to set their password."""
    base_url = frontend_url or os.getenv("FRONTEND_BASE_URL", "http://localhost:3000")
    link = invitation_url or f"{base_url.rstrip('/')}/reset-password?token={token}"
    user_display = username or "User"
    subject = "Welcome to FlowForge — Complete Your Account Setup"
    text = (
        f"Hello {user_display},\n\n"
        f"An administrator has created an account for you on FlowForge.\n\n"
        f"Click the link below to set your password and complete onboarding (valid for 24 hours):\n"
        f"{link}\n\n"
        f"If you were not expecting this invitation, you can safely ignore this email.\n"
    )
    html = f"""
    <div style="font-family: sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
      <h2 style="color: #06b6d4;">Welcome to FlowForge</h2>
      <p>Hello {user_display},</p>
      <p>An administrator has created an account for you on FlowForge. Please set your password to activate your access:</p>
      <p>
        <a href="{link}" style="display: inline-block; background-color: #06b6d4; color: #020617; padding: 10px 20px; text-decoration: none; border-radius: 8px; font-weight: bold;">
          Set Account Password
        </a>
      </p>
      <p style="font-size: 12px; color: #64748b;">This invitation link is single-use and expires in 24 hours.</p>
    </div>
    """
    return send_email(to_email, subject, text, html, metadata={"token": token, "username": username, "url": link})
