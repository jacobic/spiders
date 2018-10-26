# Shamelessly taken from Stack Overflow:
# https://stackoverflow.com/questions/3362600/how-to-send-email-attachments
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
from email import encoders

def send_mail(send_from, send_to, subject, text, files=[], server="localhost",
              port=587, username='', password='', isTls=True):
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    msg.attach(MIMEText(text))

    for path in files:
        part = MIMEBase('application', "octet-stream")
        with open(path, 'rb') as f:
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition',
                        'attachment; filename="{0}"'.format(path))
        msg.attach(part)

    smtp = smtplib.SMTP(server, port)
    if isTls: smtp.starttls()
    smtp.login(username, password)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.quit()
