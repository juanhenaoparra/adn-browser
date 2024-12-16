from .consumer import Consumer, ConsumerType
import json
import os
from typing import Dict
from pydantic import Field
from dotenv import load_dotenv
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

load_dotenv()

default_sender_email = os.getenv("SENDER_EMAIL", "")
gmail_password = os.getenv("GMAIL_APP_PASSWORD", "")


class EmailConsumer(Consumer):
    type: ConsumerType = ConsumerType.EMAIL
    sender_email: str = Field(default=default_sender_email)

    def send_via_smtp(
        self, recipient_email: str, subject: str, html_content: str
    ) -> None:
        try:
            message = MIMEMultipart("alternative")
            message["Subject"] = subject
            message["From"] = self.sender_email
            message["To"] = recipient_email

            html_part = MIMEText(html_content, "html")
            message.attach(html_part)

            with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
                server.login(self.sender_email, gmail_password)
                server.send_message(message)

        except Exception as e:
            print(f"Error sending email via SMTP: {str(e)}")

    def send(self, properties: Dict, body: bytes) -> None:
        try:
            data = json.loads(body.decode("utf-8"))
            recipient_email = data.get("email")
            otp = data.get("otp")

            if not recipient_email or not otp:
                raise ValueError("Missing email or OTP in message body")

            template_path = os.path.join(
                os.path.dirname(__file__), "../templates/otp_template.html"
            )
            with open(template_path, "r") as file:
                html_template = file.read()

            html = html_template.replace("{{ otp }}", otp)

            print(
                f"Sending email from {self.sender_email} to {recipient_email} with OTP {otp}"
            )

            self.send_via_smtp(recipient_email, "Your OTP Code", html)

            print(f"Successfully sent OTP email to {recipient_email}")

        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {str(e)}")
        except Exception as e:
            print(f"Error sending email: {str(e)}")
