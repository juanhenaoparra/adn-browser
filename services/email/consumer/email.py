from .consumer import Consumer, ConsumerType
import json
import os
from typing import Dict
import resend
from pydantic import Field
from dotenv import load_dotenv

load_dotenv()

resend.api_key = os.getenv("RESEND_API_KEY")
default_sender_email = os.getenv("SENDER_EMAIL", "")


class EmailConsumer(Consumer):
    type: ConsumerType = ConsumerType.EMAIL
    sender_email: str = Field(default=default_sender_email)

    def send(self, properties: Dict, body: bytes) -> None:
        try:
            data = json.loads(body.decode("utf-8"))
            recipient_email = data.get("email")
            otp = data.get("otp")

            if not recipient_email or not otp:
                raise ValueError("Missing email or OTP in message body")
            
            template_path = os.path.join(os.path.dirname(__file__), '../templates/otp_template.html')
            with open(template_path, 'r') as file:
                html_template = file.read()

            html = html_template.replace("{{ otp }}", otp)

            

            print(
                f"Sending email from {self.sender_email} to {recipient_email} with OTP {otp}"
            )

            resend.Emails.send(
                {
                    "from": self.sender_email,
                    "to": recipient_email,
                    "subject": "Your OTP Code",
                    "html": html,
                }
            )

            print(f"Successfully sent OTP email to {recipient_email}")

        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {str(e)}")
        except Exception as e:
            print(f"Error sending email: {str(e)}")
