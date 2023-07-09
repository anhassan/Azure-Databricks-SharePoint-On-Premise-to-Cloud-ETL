# Databricks notebook source
# MAGIC %run /Shared/utils/env_var

# COMMAND ----------

import smtplib
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def send_notification_email(sender_email,recipient_emails,subject,content,
                            smtp_server="smtp.mercy.net",smtp_port=25):
  
  msg = MIMEMultipart()
  
  msg["Subject"] = subject
  msg["From"] = sender_email
  recipients_list = recipient_emails.split(",")
  email_body_content = '''<html>
    <head></head>
    <body>
      <h1>{}</h1>
      <p>{}</p>
    </body>
  </html>'''.format(subject,content)
  
  msg.attach(MIMEText(email_body_content,"html"))
  
  smtp_conn = smtplib.SMTP(smtp_server,smtp_port)
  smtp_conn.sendmail(sender_email,recipient_emails,msg.as_string())
  smtp_conn.quit()

# COMMAND ----------

sender_email = str(dbutils.widgets.get("sender_email"))
recipient_emails = str(dbutils.widgets.get("recipient_emails"))
email_subject = str(dbutils.widgets.get("subject"))
email_content = str(dbutils.widgets.get("content"))

send_notification_email(sender_email,recipient_emails,email_subject,email_content)

# COMMAND ----------

