#!/usr/bin/python
# -*- coding: utf-8 -*-
from ckanclientmtl import ckanclientmtl
import ConfigParser 
import logging
import smtplib
import os
from email.mime.multipart import MIMEMultipart
from email.MIMEText import MIMEText
import email,email.encoders,email.mime.text,email.mime.base

#Initial configuraiton: logger, config, etc.
#Open config file
config = ConfigParser.ConfigParser()
config.read('config.cfg')


logger = logging.getLogger('ckanclientmtl')
logger.setLevel(logging.INFO)

# add a file handler
logfinename = config.get('General', 'log_file')
logfile = logging.FileHandler(logfinename)
logfile.setLevel(logging.INFO)

# create a formatter and set the formatter for the handler.
frmt = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logfile.setFormatter(frmt)

# add the Handler to the logger
logger.addHandler(logfile)

#Get info for the "target" CKAN instance
ckan_target = config.get('General', 'ckan_target')
ckan_target_key = config.get('General', 'ckan_target_api_key')
ckan_version = config.get('General', 'ckan_version')

ckanclient = ckanclientmtl.CkanClientMtl(ckan_target, ckan_target_key, ckan_version)
ckanclient.logger = logger


ckanclient.logger = logger


#Serious stuff starting here


try:
	result, resources_found, resources_success = ckanclient.push_from_directory(config.get('General', 'path_for_resources'), config.get('General', 'treated_item_path'))
except Exception, e:
    self.logger.error("Error : %s" % (repr(e))) 


if resources_found > 0:
	#Send report mail
	sender = config.get('MailConfig', 'sender')
	destination = config.get('MailConfig', 'destination')

	try:
		msg = MIMEMultipart('alternative')
		server = smtplib.SMTP(config.get('MailConfig', 'smtp_server') + ":" + config.get('MailConfig', 'smtp_port'))

		if (config.get('MailConfig', 'smtp_server') == 'Y'):
			server.ehlo()
			server.starttls()

		if (len(config.get('MailConfig', 'smtp_username')) > 1):
			server.login(config.get('MailConfig', 'smtp_username'),config.get('MailConfig', 'smtp_password'))

		msg['From'] = sender
		msg['To'] = destination	
		msg['Subject'] = 'CKAN Uploader script execution'

		#Attach the log file
		f =  open(logfinename) 
		fileMsg = email.mime.base.MIMEBase('text','csv')
		fileMsg.set_payload(file(logfinename).read())
		email.encoders.encode_base64(fileMsg)
		fileMsg.add_header('Content-Disposition','attachment;filename=' + logfinename)
		msg.attach(fileMsg)

		#Set the content
		if result == True:
			body = "The CKAN uploader script has been executed successfully and has uploaded %s resource(s) " % resources_success
		else:
			body = "The CKAN uploader script has been executed with errors. "
			body += "%s resource(s) have been found, %s has been uploaded successfully. " % (resources_found, resources_success)
			body += "See enclosed logs for more details."
		content = MIMEText(body, 'plain')
		msg.attach(content)

		server.sendmail(sender, destination, msg.as_string())
		server.quit()
		os.remove(logfinename)

	except smtplib.SMTPException:
		#We're screwed here if we can't send a status email...
		print "Error: unable to send email"


