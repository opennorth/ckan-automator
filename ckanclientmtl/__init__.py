
import ckanclient
import ckanapi
import urllib
import os
import json   
import os.path
import logging
from time import gmtime, strftime
import requests

class CkanClientMtl():

    logger = logging.getLogger('ckanclientmtl')

    package_param_list = [u'name', u'title', u'type', u'state', u'author', u'author_email', u'maintainer', u'maintainer_email', u'notes', u'url', u'version', u'tags', u'extras', u'groups'] 
    group_param_list = [u'name', u'id', u'title', u'description', u'image_url', u'type', u'state', u'approval_status'] 
    resource_param_list = [u'name', u'description', u'format', u'url']

    def __init__(self, base_location, api_key=None, is_remote= True , ckan_version=2.2):
        self.ckan_target = ckanclient.CkanClient(base_location, api_key)
        self.ckanapi = ckanapi.RemoteCKAN(base_location,
                        apikey=api_key,
                        user_agent='CkanApiScript (+http://TBD)')
        '''TODO: supporter API LOCAL'''


    def get_group_list (self):
        '''Retrieve all the groups from an instance and populate 'group_list'
        with the content.

        '''
        ckan_group_list = self.ckanapi.action.group_list()
        group_list = []

        for group_item in ckan_group_list:  
           group_entity =  self.ckanapi.action.group_show(id=group_item) 

           new_group_entity = {}
           for key,value in group_entity.items():
               if key in self.group_param_list:
                   new_group_entity[key] = value 

           group_list.append(new_group_entity)
        self.logger.info("%s groups read" % len(group_list)) 

        return group_list     

    def group_exist (self, group_id):
        '''Check if group 'group_id' exists in the CKAN instance
        pointed by the ckan client object 'ckan_instance'
        '''        
        try:
            current_group = self.ckanapi.action.group_show(id=group_id)
            return True, current_group
        except ckanapi.NotFound:
            return False, None

    def push_groups (self, group_list):
        '''POST the groups contained in the group_list list to 
           the ckan instance pointed by the ckan_target object.
           If group does not exist, create it, else update it'''
        for group in group_list:

            group_exists, current_group = self.group_exist(group["name"])

            if group_exists :
                self.logger.info("Group exists - put it - %s" % group["name"])

                #We want to only update the new fields and keep the old ones
                current_group.update(group)

                if 'name' in group.keys():
                    #CKAN API does not like having name and id provided. Name can be used as id...
                    group["id"] = group["name"]
                    del group["name"]

                self.ckanapi.action.group_update(**current_group)
            else:
                self.logger.info("Group does not exist - post it - %s" % group["name"])
                self.ckanapi.action.group_create(**group)                



    def delete_groups (self, group_list):
        '''Delete the groups listed in group_list'''
        for group in group_list:
            self.logger.info("deleting group %s" % group["name"])
            self.ckanapi.action.group_delete(id=group["name"])      


    def get_package_from_json (self, json_file):
        '''Build a package list from a json file 
        and put the content in package_list '''
        package = json.loads(open(json_file).read())

        self.package_list.append(package)



    def get_package_list (self, package_limitation=None):
        '''If 'package_limitation' is provided, the function will only
            process these packages. Else, it will read all the package
            available in ckan_instance. The listed packaged will appended
            in package_list (with all the metadata)

            param ckan_instance: ckan instance where the packages should
            be read.
            param package_limitation: list of packaged id/name that will 
            processed (the other will be ignored) '''

        package_list = []
        if (package_limitation == None):
            package_source_list = self.ckanapi.action.package_list()

        else:
            package_source_list = package_limitation

        for package_item in package_source_list:

            try:
                package_entity = self.ckanapi.action.package_show(id=package_item) 

                package_list.append(package_entity)
            except ckanclient.CkanApiNotAuthorizedError:
                self.logger.warning("Access not authorized to group %s" % package_item)
            except ckanapi.errors.NotFound:
                self.logger.warning("Access not authorized to group %s" % package_item)

        self.logger.info("Number of packages read : %s" % len(package_list))

        return package_list


    def package_exist (self, package_id):
        try:
            package = self.ckanapi.action.package_show(id=package_id)
            return True, package
        except ckanapi.NotFound:
            return False, None


    def push_package_list (self, package_list, push_resources=True, organization=None, licence=None):


        for package in package_list:
            self.logger.info("Pushing %s" % package["name"])
            self.push_package(package, push_resources, organization, licence)


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

        #When getting packagin from another source, we don't control the group "id"
        #while the group name is usually ok.
        if "groups" in package.keys():
            for group in package["groups"]:
                if "id" in group:
                    del group["id"]

        if "tags" in package.keys():

            for tag in package["tags"]:
                if "id" in tag:
                    del tag["id"] 
                if "vocabulary_id" in tag : 
                    del tag["vocabulary_id"]
                if "revision_timestamp" in tag:
                    del tag["revision_timestamp"] 
                if "state" in tag:
                    del tag["state"]

        package_exists, current_package = self.package_exist(package["name"])

        

        if package_exists:
            self.logger.info("Package exists - put it - %s" % package["name"])

            #If a field is not provide in the new package, we keep the old value
            current_package.update(new_package_entity)
            self.ckanapi.action.package_update(**current_package)
        else:
            self.logger.info("Package does not exist - post it - %s" % package["name"])
            self.ckanapi.action.package_create(**new_package_entity)       
        
        #upload resources if requested
        if push_resources == True:
            for resource in package["resources"]:
                self.push_resource(package["name"], resource)


    def delete_packages (self, package_list):
        '''Delete all the packages listed in packages_list'''
        for package in package_list:
            self.logger.info("deleting package %s" % package["name"])
            self.ckanapi.action.package_delete(id=package["name"])        


    def resource_exist(self, resource_name, resource_format, package_name):

        package = self.ckanapi.action.package_show(id=package_name)

        for resource in package["resources"]:
            if resource["name"] == resource_name and resource["format"].lower() == resource_format.lower():
                return True, resource

        return False, None




    def push_resource(self, package_name, resource):
        '''Upload file to CKAN.

        If the resource url starts with http, the script download the resource
        locally, else directly take it where it is.

        :param package_name: name of the package to link with the resource
        :param treated_path: structure like CKAN resource structure containing
        the resource_type, the format, the name and the description 
        '''
        link_files = False

        #print "Will push resource %s" % resource["url"]
        if (resource["url"].startswith('http') and link_files == False) :
            #this a remote file, download it
            local_path = self.download_remote_file(resource)
        else:
            local_path = resource["url"]

        #print local_path


        if local_path == None:
            return False

        new_resource_entity = {}
        for key,value in resource.items():
            if key in self.resource_param_list:
                new_resource_entity[key] = value 

        #datadict = {"package_id":package_name, "upload": open(local_path)}
        datadict = {"package_id":package_name}
        datadict.update(new_resource_entity)

        #Check if the resource already exists
        resource_exist, resource = self.resource_exist(resource["name"], resource["format"], package_name)

        try:
            if resource_exist:
                self.logger.info("Resource exists, trying to overwrite it")
                datadict["id"] = resource["id"]
                if (link_files == False):
                    self.logger.info("Trying to upload file %s" % local_path)
                    datadict["url"] = ""
                    self.ckanapi.call_action('resource_update', datadict, files=[('upload', file(local_path))])
                else:
                    self.logger.info("Link file to remote %s" % datadict["url"])
                    datadict.update({"clear_upload":"true"})
                    self.ckanapi.call_action('resource_update', datadict)
            else:
                self.logger.info("Resource does not exist, create it")
                if (link_files == False):
                    self.logger.info("Trying to upload file %s" % local_path)
                    datadict["url"] = ""
                    self.ckanapi.call_action('resource_create', datadict, files=[('upload', file(local_path))])
                else: 
                    self.logger.info("Link file to remote %s" % datadict["url"])
                    self.ckanapi.call_action('resource_create', datadict)

        except requests.exceptions.ConnectionError, e:
            self.logger.error("Error %s when uploading %s" % (e, local_path))
            return False

    def download_remote_file (self, resource):
        '''Download a remote file locally
        and returns the path where it was downloaded'''

        directory = "/tmp/" + resource["name"]
        if not os.path.exists(directory):
            os.makedirs(directory)
        local_path = directory  + "/" + resource["url"].split('/')[-1]
        
        try:
            open(local_path, 'a').close()
        
            self.logger.info("downloading remote file to %s" % local_path)
            resource["url"] = resource["url"].replace("ckanprod","donnees.ville.montreal.qc.ca")
            
            urllib.urlretrieve (resource["url"], local_path) 

            return local_path

        except IOError, e:
            self.logger.error("Error %s when downloading %s" % (e, resource["url"]))
            return None



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


