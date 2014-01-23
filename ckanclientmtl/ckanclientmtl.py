
import ckanclient
import urllib
import os
import json   
import os.path
import logging
from time import gmtime, strftime
import requests

class CkanClientMtl(ckanclient.CkanClient):

    ckan_source = None

    logger = logging.getLogger('ckanclientmtl')

    group_list = []
    package_list = []

    package_param_list = [u'name', u'title', u'type', u'state', u'author', u'author_email', u'maintainer', u'maintainer_email', u'notes', u'url', u'version', u'tags', u'extras', u'groups'] 
    group_param_list = [u'name', u'id', u'title', u'description', u'image_url', u'type', u'state', u'approval_status'] 

    def __init__(self, base_location, api_key, ckan_version=2.2):
        self.ckan_target = ckanclient.CkanClient(base_location, api_key)

    def set_ckan_source (self, url):
        self.ckan_source = ckanclient.CkanClient(base_location=url)

    def get_group_list (self, ckan_instance):
        '''Retrieve all the groups from an instance and populate 'group_list'
        with the content.

        :param ckan_instance: CKAN client object to use to retrieve the groups.
        '''
        ckan_group_list = ckan_instance.group_register_get()

        for group_item in ckan_group_list:
           group_entity =  ckan_instance.group_entity_get(group_item) 
           #print group_entity

           new_group_entity = {}
           for key,value in group_entity.items():
               if key in self.group_param_list:
                   new_group_entity[key] = value 

           self.group_list.append(new_group_entity)
        self.logger.info("%s groups read" % len(self.group_list))

    def group_exist (self, ckan_instance, group_id):
        '''Check if group 'group_id' exists in the CKAN instance
        pointed by the ckan client object 'ckan_instance'
        '''        
        try:
            ckan_instance.group_entity_get(group_id)
            return True
        except ckanclient.CkanApiNotFoundError:
            return False       

    def push_all_groups (self):
        '''POST the groups contained in the group_list list to 
           the ckan instance pointed by the ckan_target object.
           If group does not exist, create it, else update it'''
        for group in self.group_list:

            if (self.group_exist(self.ckan_target, group["name"])):
                self.logger.info("Group exists - put it - %s" % group["name"])
                self.ckan_target.group_entity_put(group)
            else:
                self.logger.info("Group does not exist - post it - %s" % group["name"])
                self.ckan_target.group_register_post(group)                

        del self.group_list[:]

    def get_package_from_json (self, json_file):
        '''Build a package list from a json file 
        and put the content in package_list '''
        package = json.loads(open(json_file).read())

        self.package_list.append(package)

    def delete_all_groups (self):
        '''Delete all the groups listed in group_list'''
        for group in self.group_list:
            self.logger.info("deleting group %s" % group["name"])
            self.ckan_target.group_entity_delete(group["name"])      

        del self.group_list[:]


    def get_package_list (self, ckan_instance, package_limitation=None):
        '''If 'package_limitation' is provided, the function will only
            process these packages. Else, it will read all the package
            available in ckan_instance. The listed packaged will appended
            in package_list (with all the metadata)

            param ckan_instance: ckan instance where the packages should
            be read.
            param package_limitation: list of packaged id/name that will 
            processed (the other will be ignored) '''


        if (package_limitation == None):
            package_source_list = ckan_instance.package_register_get()

        else:
            package_source_list = package_limitation

        for package_item in package_source_list:

            try:
                package_entity =  ckan_instance.package_entity_get(package_item) 

                self.package_list.append(package_entity)
            except ckanclient.CkanApiNotAuthorizedError:
                self.logger.warning("Access not authorized to group %s" % package_item)

        self.logger.info("Number of packages read : %s" % len(self.package_list))


    def package_exist (self, ckan_instance, package_id):
        try:
            ckan_instance.package_entity_get(package_id)
            return True
        except ckanclient.CkanApiNotFoundError:
            return False


    def push_all_packages (self, push_resources=True, organization=None, licence=None):


        for package in self.package_list:
            self.logger.info("Pushing %s" % package["name"])
            self.push_package(package, push_resources, organization, licence)


        del self.package_list[:]

    def push_package (self, package, push_resources=True, organization=None, licence=None):
        '''POST a package item to the ckan_target instance (including
            attached resources if present)'''

        new_package_entity = {}
        for key,value in package.items():
            if key in self.package_param_list:
                new_package_entity[key] = value 

        if organization != None:
            new_package_entity["owner_org"] = organization

        if licence != None:
            new_package_entity["license_id"] = licence

        if (self.package_exist(self.ckan_target, package["name"])):
            self.logger.info("Package exists - put it - %s" % package["name"])
            self.ckan_target.package_entity_put(new_package_entity)
        else:
            self.logger.info("Package does not exist - post it - %s" % package["name"])
            self.ckan_target.package_register_post(new_package_entity)       
        
        #upload resources if requested
        if push_resources == True:
            for resource in package["resources"]:
                self.push_resource(package["name"], resource)


    def delete_all_packages (self):
        '''Delete all the packages listed in packages_list'''
        for package in self.package_list:
            self.logger.info("deleting package %s" % package["name"])
            self.ckan_target.package_entity_delete(package["name"])        

        del self.package_list[:]


    def update_resource(self, package_name, resource):
        
        # Searching for the id of the resource to be updated
        package = self.ckan_target.package_entity_get(package_name) 
        resource_id = ""

        for r in package["resources"]:
            if (r["name"] == resource["name"]):
                if (resource_id != ""):
                    #we have more than one resource
                    raise ckanclient.CkanApiError('There are two similar resource with the same name. Do know which one to update : %s' % resource["name"] )
                else:
                    resource_id = r["id"]
        if (resource_id == ""):
            #We did not find the resource to be updated
            raise ckanclient.CkanApiError('Could not find the resource to update : %s' % resource["name"] )

        if (resource["url"].startswith('http') ):
            #this a remote file, download it
            local_path = self.download_remote_file(resource)
        else:
            local_path = resource["url"]

        datadict = {"id":resource_id}
        datadict.update(resource)

        try:
            requests.post(self.ckan_target.base_location + '/action/resource_update',
              data=datadict,
              headers={"X-CKAN-API-Key": self.ckan_target.api_key},
              files=[('upload', file(local_path))])
        except Exception, e:
             raise ckanclient.CkanApiError('Erreur upload de fichier: %s' % e )


    def push_resource(self, package_name, resource):
        '''Upload file to CKAN.

        If the resource url starts with http, the script download the resource
        locally, else directly take it where it is.

        :param package_name: name of the package to link with the resource
        :param treated_path: structure like CKAN resource structure containing
        the resource_type, the format, the name and the description 
        '''

        #print "Will push resource %s" % resource["url"]
        if (resource["url"].startswith('http') ):
            #this a remote file, download it
            local_path = self.download_remote_file(resource)
        else:
            local_path = resource["url"]

        if (self.ckan_version >= 2.2):

            datadict = {"package_id":package_name}
            datadict.update(resource)

            try:
                requests.post(self.ckan_target.base_location + '/action/resource_create',
                  data=datadict,
                  headers={"X-CKAN-API-Key": self.ckan_target.api_key},
                  files=[('upload', file(local_path))])

            except Exception, e:
                 raise ckanclient.CkanApiError('Erreur upload de fichier: %s' % e )

        else:
        #print "sur le point d'upload %s " % local_path
            try: 
                remote_path, error = self.ckan_target.upload_file(local_path)
            except Exception, e:
                 raise ckanclient.CkanApiError('Erreur upload de fichier: %s' % e )

            try:
                self.ckan_target.add_package_resource(package_name, remote_path,
                    resource_type='data', 
                    format=resource["format"], 
                    name=resource["name"], 
                    description=resource["description"])

            except Exception, e:
                raise ckanclient.CkanApiError('Erreur en liant ressource %s avec package %s: %s' % (remote_path, package_name, e) )


    def download_remote_file (self, resource):
        '''Download a remote file locally
        and returns the path where it was downloaded'''

        directory = "/tmp/" + resource["name"]
        if not os.path.exists(directory):
            os.makedirs(directory)
        local_path = directory  + "/" + resource["url"].split('/')[-1]
        open(local_path, 'a').close()
    
        self.logger.info("downloading remote file to %s" % local_path)

        urllib.urlretrieve (resource["url"], local_path) 

        return local_path

    def push_from_directory(self, path, treated_path):
        '''Send a local file to the ckan_target instance. 

        The local file must constain a JSON metadata file
        The script search for subdirectory in path
        In each subdirectory there should be one metadata
        file and one file to be uploaded.

        :param path: local path where the file/metadata is located
        :param treated_path: local path where the successfully uploaded
        directories will be copied. The target path should exist. 
        '''
        resources_found, resources_success = 0,0
        transfert_subdir = treated_path + 'ckan_import_' + strftime("%Y%m%d%H%M%S", gmtime()) + '/'
        if not os.path.exists(transfert_subdir):
            os.makedirs(transfert_subdir)


        for dirname, dirnames, filenames in os.walk(path):
            
            metadata_file = '/metadata.json'

            if (dirname != path):
                resources_found += 1

                if (os.path.isfile(dirname + metadata_file)):
                    self.logger.info("Directory containing resources found: %s" % (dirname))

                    #We have a resource to upload!
                    #print "Je vais ouvrir un fichier: %s" % (dirname + '/resource.json')
                    try:
                        json_dump = json.loads(open(dirname + metadata_file).read())

                        package_name = json_dump['name']
                        resource = json_dump['resources']
                        resource['url'] = dirname + '/' + resource['url']

                        self.push_resource(package_name, resource)

                        os.rename(dirname, dirname.replace(path, transfert_subdir))
                        resources_success += 1

                    except ValueError:
                        
                        self.logger.error("Error while reading metadata file: %s" % (dirname + '/resource.json')) # and so will this.
                            
                    except OSError, e:
                        self.logger.error("OS Error %s : %s" % (treated_path, str(e))) 

                    except ckanclient.CkanApiError, e:
                        self.logger.error("CKAN Error : %s" % (str(e))) 

                    except Exception, e:
                        self.logger.error("Error : %s" % (repr(e))) 

                else :
                    self.logger.error("No metadata available for file in directory %s" % (dirname)) 

        if resources_found > 0:
            self.logger.info("%s directories found, %s resources successfully uploaded" % (resources_found, resources_success))

        if (resources_found != resources_success):
            self.logger.info("Directory not treated:")

            for dirname, dirnames, filenames in os.walk(path):
                if (dirname != path):
                    self.logger.info("   " + dirname)

            return False, resources_found, resources_success

        else:
            return True, resources_found, resources_success


