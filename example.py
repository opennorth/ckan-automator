from ckanclientmtl import ckanclientmtl
import ConfigParser 

#Open config file
config = ConfigParser.ConfigParser()
config.read('config.cfg')

#Get info for the "target" CKAN instance
ckan_target = config.get('General', 'ckan_target')
ckan_target_key = config.get('General', 'ckan_target_api_key')
ckanclient = ckanclientmtl.CkanClientMtl(ckan_target, ckan_target_key)

#Launch push process giving the path of the directory to use
ckanclient.push_from_directory(config.get('General', 'path_for_resources'), config.get('General', 'treated_item_path'))

