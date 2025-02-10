from dotenv import load_dotenv
import os
from email.mime.text import MIMEText
from smtplib import SMTP

load_dotenv()

SENDER_EMAIL_ADDRESS = os.getenv("SENDER_EMAIL_ADDRESS")
SENDER_EMAIL_PASSWORD = os.getenv("SENDER_EMAIL_PASSWORD")
RECIPIENT_EMAIL_ADDRESS = os.getenv("RECIPIENT_EMAIL_ADDRESS")

def send_email_error(log_erros):
    try:
        if not log_erros:
            return
        email_body = "<br>".join(f"<p>{erro}</p>" for erro in log_erros)
        msg = MIMEText(email_body, 'html')
        msg['Subject'] = "Erro_Script_Live_Waze_Traffic"
        msg['From'] = SENDER_EMAIL_ADDRESS
        msg['To'] = RECIPIENT_EMAIL_ADDRESS
        
        with SMTP("smtp.gmail.com", 587) as smtp:
            smtp.starttls()
            smtp.login(SENDER_EMAIL_ADDRESS, SENDER_EMAIL_PASSWORD)
            smtp.sendmail(msg['From'], msg['To'], msg.as_string())
    except Exception as e:
        print(f"Erro ao enviar e-mail: {e}")