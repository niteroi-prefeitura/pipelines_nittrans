from email.mime.text import MIMEText
from smtplib import SMTP


def send_email_error(credentials, subject, errors):
    try:
        if not errors:
            return
        email_body = "<br>".join(f"<p>{erro}</p>" for erro in errors)
        msg = MIMEText(email_body, 'html')
        msg['Subject'] = subject
        msg['From'] = credentials["sender_email_address"]
        msg['To'] = credentials["recipient_email_address"]

        with SMTP("smtp.gmail.com", 587) as smtp:
            smtp.starttls()
            smtp.login(credentials["sender_email_address"],
                       credentials["sender_email_password"])
            smtp.sendmail(msg['From'], msg['To'], msg.as_string())
    except Exception as e:
        print(f"Erro ao enviar e-mail: {e}")
