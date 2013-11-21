from ckanclientmtl import ckanclientmtl
import ConfigParser 
import logging


logger = logging.getLogger('ckanclientmtl')
logger.setLevel(logging.DEBUG)

# add a file handler
logfile = logging.FileHandler('ckanclientmtl.log')
logfile.setLevel(logging.WARNING)

# create a formatter and set the formatter for the handler.
frmt = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logfile.setFormatter(frmt)

# add the Handler to the logger
logger.addHandler(logfile)


#Open config file
config = ConfigParser.ConfigParser()
config.read('config.cfg')

#Get info for the "target" CKAN instance
ckan_target = config.get('General', 'ckan_target')
ckan_target_key = config.get('General', 'ckan_target_api_key')
ckanclient = ckanclientmtl.CkanClientMtl(ckan_target, ckan_target_key)
ckanclient.logger = logger


#Launch push process giving the path of the directory to use
ckanclient.push_from_directory(config.get('General', 'path_for_resources'), config.get('General', 'treated_item_path'))



