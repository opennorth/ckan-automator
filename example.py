#!/usr/bin/python
# -*- coding: utf-8 -*-
from ckanclientmtl import ckanclientmtl
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


#Serious stuff starting here
'''Flushing all the packages of an instance
ckanclient.get_package_list(ckanclient.ckan_target)
ckanclient.delete_all_packages()'''


'''Flushing all the groups of an instance
ckanclient.get_group_list(ckanclient.ckan_target)
ckanclient.delete_all_groups()'''


'''Copying groups from another CKAN instance
ckanclient.set_ckan_source('http://donnees.ville.montreal.qc.ca/api/')
ckanclient.get_group_list(ckanclient.ckan_source)
ckanclient.push_all_groups()'''

'''Injecting ad hoc group
ckanclient.group_list.append({
	u'title': u'My custom group', 
	u'description': u'This is an updated description.', 
	u'state': u'active', 
	u'image_url': u'http://www.meteoweb.eu/wp-content/uploads/2011/08/meteo.png', 
	u'type': u'group',
	u'name': u'custom-group'})
ckanclient.push_all_groups()'''


'''Copying packages from another CKAN instance'''
'''Note: The owner organization will only work on CKAN 2.2+
ckanclient.set_ckan_source('http://donnees.ville.montreal.qc.ca/api/')
ckanclient.get_package_list(ckanclient.ckan_source, ('parcours-riverain', 'resultats-elections-2013'))
ckanclient.push_all_packages(False, 'ville-de-montreal', 'SCOL')'''


'''Creating/updating a package directly
ckanclient.package_list.append({
	u'name': u'injected-dataset',
	u'title': u'A dataset injected via the API',
	u'maintainer': u'The Maintainer', 
	u'maintainer_email': u'opendata@mycity.gov',
	u'notes': u'Some notes about the dataset.',
	u'tags': [u'One tag', u'Another tag'],
	u'groups': [u'custom-group']
	})
ckanclient.push_all_packages(False, 'my-org', 'odc-pddl')'''


'''Declare remote resource, download it locally, upload it to CKAN and link it to package'''
resource = {
	'url': 'https://ckannet-storage.commondatastorage.googleapis.com/2013-10-27T16:40:24.027Z/open-data-census-database-2013-index-presentation-entries.csv', 
	'description': 'J\'ai changé la description', 
	'format': 'CSV', 
	'name': 'Test resource'}

#ckanclient.push_resource('injected-dataset', resource)
ckanclient.update_resource('injected-dataset', resource)

'''Upload resource and link it to dataset'''
'''Read directory containing data + json file containing resource metadata
First argument is the directory where is stored the resource to upload,
the second argument is where the uploaded resource has to be moved
ckanclient.push_from_directory(config.get('General', 'path_for_resources'), config.get('General', 'treated_item_path'))'''


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


