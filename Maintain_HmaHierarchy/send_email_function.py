from smtplib import SMTP
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.message import EmailMessage
from email import encoders
from typing import Union, List, Dict, NoReturn, Optional

def write_email(subject: str, content: str, sender: str, receiver_list: List[str], attachment_path: str=None, attachment_name: str=None) -> MIMEMultipart:
    print("Writing Email")

    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ", ".join(receiver_list)

    # Set email message    
    combine_html = """
    <html>
      <head></head>
      <body>
        {html}
        <p>
           Please do not reply this email.<br>
        </p>
      </body>
    </html>
    """.format(html=content)
    html_content = MIMEText(combine_html, 'html')
    msg.attach(html_content)
    print(html_content)

    # Add attachement
    if attachment_path is not None:
        part = MIMEBase('application', "octet-stream")
        part.set_payload(open(attachment_path, "rb").read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="{file}"'.format(file=attachment_name))
        msg.attach(part)
    return msg


def send_email(msg: MIMEMultipart) -> NoReturn:
    with SMTP('mail.testing.com', 25) as s:
        s.send_message(msg)
        print("Email is sent.")
