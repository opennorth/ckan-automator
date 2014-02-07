#!/usr/bin/python
# -*- coding: utf-8 -*-
import ckanclientmtl
import ConfigParser 
import logging
import smtplib
import os


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

ckansource = ckanclientmtl.CkanClientMtl('http://another.ckan.net/')


'''Delete all groups of an install'''
group_list = ckansource.get_group_list()
ckansource.delete_groups(group_list)


'''Copy groups from another instance (ckansource) to another one (ckanclient)'''
group_list = ckansource.get_group_list()
ckanclient.push_groups(group_list)


'''Flushing all the packages of an instance'''
package_list = ckanclient.get_package_list()
ckanclient.delete_packages(package_list)

'''Copy selected packages from an instance to another'''
source_packages = ckansource.get_package_list(["2001-census-statistics", "2006-surrey-census"])
ckanclient.push_package_list(package_list)



#source_package = ckansource.get_package_list()
#print source_package




'''Declare remote resource, download it locally, upload it to CKAN and link it to package'''
resource = {
	'url': 'https://ckannet-storage.commondatastorage.googleapis.com/2013-10-27T16:40:24.027Z/open-data-census-database-2013-index-presentation-entries.csv', 
	'description': 'J\'ai encore changé la description', 
	'format': 'CSV', 
	'name': 'Test resource'}

ckanclient.push_resource('injected-dataset', resource)


'''Push resource from a directory'''
ckanclient.push_from_directory(config.get('General', 'path_for_resources'), config.get('General', 'treated_item_path'))



#Send report mail
sender = config.get('MailConfig', 'sender')
destination = config.get('MailConfig', 'destination')

try:
	server = smtplib.SMTP(config.get('MailConfig', 'smtp_server') + ":" + config.get('MailConfig', 'smtp_port'))

	if (config.get('MailConfig', 'smtp_server') == 'Y'):
		server.ehlo()
		server.starttls()

	if (len(config.get('MailConfig', 'smtp_username')) > 1):
		server.login(config.get('MailConfig', 'smtp_username'),config.get('MailConfig', 'smtp_password'))


	f =  open(logfinename) 
		

	msg = "\r\n".join([
	  "From: " + sender,
	  "To: " + destination,
	  "Subject: CKAN Uploader script",
	  "",
	  "The CKAN uploader script has run and returned the following messages",
	  f.read()
	  ])

	server.sendmail(sender, destination, msg)
	server.quit()
	os.remove(logfinename)
except smtplib.SMTPException:
	#We're screwed here if we can't send a status email...
	print "Error: unable to send email"


