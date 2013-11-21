
import ckanclient
import urllib
import os
import json   
import os.path
import logging
from time import gmtime, strftime

class CkanClientMtl(ckanclient.CkanClient):

    ckan_source = None

    logger = logging.getLogger('ckanclientmtl')

    group_list = []
    package_list = []
    resource_dict = {}

    #Manque: licence_id, 
    package_param_list = [u'name', u'title', u'type', u'state', u'author', u'author_email', u'maintainer', u'maintainer_email', u'notes', u'url', u'version', u'tags', u'extras', u'groups'] 
    #package_param_list = [u'name', u'title', u'type', u'state', u'author', u'author_email', u'maintainer', u'maintainer_email'] 

    group_param_list = [u'name', u'id', u'title', u'description', u'image_url', u'type', u'state', u'approval_status'] 

    def __init__(self, base_location, api_key):
        self.ckan_target = ckanclient.CkanClient(base_location, api_key)

    def set_ckan_source (self, url):
        self.ckan_source = ckanclient.CkanClient(base_location=url)

    def get_group_list (self, ckan_instance):

        ckan_group_list = ckan_instance.group_register_get()

        for group_item in ckan_group_list:
           group_entity =  ckan_instance.group_entity_get(group_item) 
           #print group_entity

           new_group_entity = {}
           for key,value in group_entity.items():
               if key in self.group_param_list:
                   new_group_entity[key] = value 

           self.group_list.append(new_group_entity)
        print "Nombre de groupes lus : %s" % len(self.group_list)

    def group_exist (self, ckan_instance, group_id):
        try:
            ckan_instance.group_entity_get(group_id)
            return True
        except ckanclient.CkanApiNotFoundError:
            return False       

    def push_group (self):
        '''TODO TESTER SI LE GROUPE EXISTE'''
        for group in self.group_list:
            self.ckan_target.group_register_post(group)

        del self.group_list[:]

    def get_package_from_json (self, json_file):
        package = json.loads(open(json_file).read())

        self.package_list.append(package)


    def get_package_list (self, ckan_instance, package_list=None):



        if (package_list == None):
            package_source_list = ckan_instance.package_register_get()
            #print group_source_list
        else:
            package_source_list = package_list

        for package_item in package_source_list:

            try:
                package_entity =  ckan_instance.package_entity_get(package_item) 
                #print package_entity

                self.package_list.append(package_entity)
            except ckanclient.CkanApiNotAuthorizedError:
                print "Access non autorise pour groupe %s" % package_item

        print "Nombre de packages lus : %s" % len(self.package_list)

    def package_exist (self, ckan_instance, package_id):
        try:
            ckan_instance.package_entity_get(package_id)
            return True
        except ckanclient.CkanApiNotFoundError:
            return False


    def push_all_packages (self):
        '''TODO: VERIFIER SI TOUTES LES DONNEES SONT TRANSFEREE - IL MANQUE AU MOINS LES TAGS D'ARRONDISSEMENT'''
        for package in self.package_list:
            print "Push %s" % package["name"]
            self.push_package(package)


        del self.package_list[:]

    def push_package (self, package):
         #print package

        new_package_entity = {}
        for key,value in package.items():
            if key in self.package_param_list:
                new_package_entity[key] = value 

        if (self.package_exist(self.ckan_target, package["name"])):
            print "Package exists - put it - %s" % package["name"]
            self.ckan_target.package_entity_put(new_package_entity)
        else:
            print "Package does not exist - post it - %s" % package["name"]
            self.ckan_target.package_register_post(new_package_entity)       
        
        #upload resourcs
        print package

        for resource in package["resources"]:
            self.push_resource(package["name"], resource)


    def delete_all_packages (self):
        for package in self.package_list:
            print "delete %s" % package["name"]
            self.ckan_target.package_entity_delete(package["name"])        

        del self.package_list[:]


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
        #print resource
        #print resource["url"]
        directory = "/tmp/" + resource["id"]
        if not os.path.exists(directory):
            os.makedirs(directory)
        local_path = directory  + "/" + resource["url"].split('/')[-1]
        open(local_path, 'a').close()
        print "Telecharge fichier "
        print local_path
        urllib.urlretrieve (resource["url"], local_path) 

        return local_path

    def push_from_directory(self, path, treated_path):
        '''Send a local file to a CKAN instance. 
        TODO : QUELLE INSTANCE

        The local file must constain a JSON metadata file
        The script search for subdirectory in path
        In each subdirectory there should be one metadata
        file and one file to be uploaded.

        :param path: local path where the file/metadata is located
        :param treated_path: local path where the successfully uploaded
        directories will be copied. The target path should exist. 
        '''
        transfert_subdir = treated_path + 'ckan_import_' + strftime("%Y%m%d%H%M%S", gmtime()) + '/'
        if not os.path.exists(transfert_subdir):
            os.makedirs(transfert_subdir)

        for dirname, dirnames, filenames in os.walk(path):
            if (dirname != path):
                if (os.path.isfile(dirname + '/resource.json')):
                    #We have a resource to upload!
                    #print "Je vais ouvrir un fichier: %s" % (dirname + '/resource.json')
                    try:
                        json_dump = json.loads(open(dirname + '/resource.json').read())

                        package_name = json_dump['name'];
                        resource = json_dump['resources'][0]
                        resource['url'] = dirname + '/' + resource['url']


                        self.push_resource(package_name, resource)

                        os.rename(dirname, dirname.replace(path, transfert_subdir))

                    except ValueError:
                        
                        self.logger.error("Erreur de l'ouverture d'un fichier de metadonnees: %s" % (dirname + '/resource.json')) # and so will this.
                            
                    except OSError, e:
                        self.logger.error("Deplacement impossible dans repertoire %s : %s" % (treated_path, str(e))) 

                    except ckanclient.CkanApiError, e:
                        self.logger.error("Erreur CKAN : %s" % (str(e))) 

                    except Exception, e:
                        self.logger.error("Erreur : %s" % (repr(e))) 


                elif (os.path.isfile(dirname + '/package.json')):
                    print "Nous avons un package! -- cette partie n'est pas implementee"
                else :
                    self.logger.error("Repertoire sans fichier de metadonnees %s" % (dirname)) 



