import json
import os
import argparse
import boto3
import json
from datetime import datetime
from botocore.exceptions import ClientError
from io import StringIO
import pandas as pd 
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
import tempfile

def get_content_json_result_html(bucketname, key,module):
    s3 = boto3.resource("s3")
    get_json_msg = json.loads(s3.Object(bucketname, key).get()['Body'].read())
    d1 = pd.DataFrame(get_json_msg)
    return d1.to_html()

def send_email(email_settings, bucketname, key, module):
    print("info: Sending DQ results mail.....")
    #------------------
    # Create raw email
    #------------------
    msg                     = MIMEMultipart()
    #module                  = key.split('/')[-2]
    msg['Subject']          = email_settings.get("email_subject","Issues for ") + module
    msg['From']             = email_settings.get("email_sender","bi.admin@xyz.com.au")
    to_emails               = email_settings.get("email_recipients",["rajesh.kotian@xyz.com.au"])
    email_region            = email_settings.get("email_region","ap-southeast-2")
    #------------------
    # Create email body
    #------------------
    # Encode the text and HTML content and set the character encoding. This step is
    # necessary if you're sending a message with characters outside the ASCII range.
    msg_body                = MIMEMultipart('alternative')
    body_text               = email_settings.get("email_body","Test DQ Report")
    body_html               = email_settings.get("email_body_html_start",f"<p>Hi,</p><p>Please see the results for {module}.") \
                                                + get_content_json_result_html(bucketname, key,module)  \
                                                + email_settings.get("email_body_html_end",".</p><p>Regards,</p><p>Vicinity RIPL Team.</p>")
    charset                 = email_settings.get("email_charset","UTF-8")
    text_part               = MIMEText(body_text.encode(charset), 'plain', charset)
    html_part               = MIMEText(body_html.encode(charset), 'html', charset)
    # Add the text and HTML parts to the child container.
    msg_body.attach(text_part)
    msg_body.attach(html_part)
    msg.attach(msg_body)
    #----------------------
    # Get file
    #----------------------
    s3_resource             = boto3.resource('s3')
    tmp_dir                 = tempfile.gettempdir()
    download_path           = f'{tmp_dir}/{module}'
    s3_resource.Object(bucketname, key).download_file(download_path)
    #----------------------
    # Attach file to email
    #----------------------
    attachment_filepath     = download_path
    attachment_filename     = module +".json"

    email_attachment_zip    = email_settings.get("email_attachment_zip", True)
    if email_attachment_zip:
         # compress attachment to gz since attachment is more than SES 10MB limit
         with zipfile.ZipFile(attachment_filepath + '.zip', 'w', zipfile.ZIP_DEFLATED) as zip:
             zip.write(filename=attachment_filepath, arcname=module)
         attachment_filepath     = download_path + '.zip'
         attachment_filename     = module + '_file.zip'
    
    part = MIMEApplication(open(attachment_filepath, "rb").read())
    part.add_header('Content-Disposition', 'attachment', filename=attachment_filename)
    msg.attach(part)
    #----------------------
    # Send email
    #----------------------
    ses = boto3.client('ses', region_name=email_region)
    email_status = ses.send_raw_email(RawMessage={
                    'Data': msg.as_string(),
                }, 
                Source=msg['From'], 
                Destinations=to_emails
                )
    print("Sent DQ results mail")
    return {'file_attachment': attachment_filename, 'email_status': email_status}
  
def handler(event, context):
    """
    Callers: 
        invoke 
    Parameters:
        event: Example 
            {
                "bucketname": "example_bucket",
                "filepath": "test/conent.json",
                "Module" : "test_module"
            }
    """
    settings = {} # get the settings from config files or some payload 
    email_settings = settings.get("email_settings",{})
    bucket = event['bucketname']
    filepath = event['filepath']
    module = event['module']
    send_email(email_settings, bucket, filepath,module)
    

if __name__ == "__main__":
    event = {
                "bucketname": "example_bucket",
                "filepath": "test/conent.json",
                "Module" : "test_module"
                }
    handler(event, "")
