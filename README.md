This package is an encapsulation of ckanclient that simplify management of groups, packages and resources via the API.

The main raison d'être of the package is to upload resources contained in a local directory.

##Important
- In order this package needs a [forked](https://github.com/opennorth/ckanclient) version of the original ckan client, until the proposed changed are released in the official CkanClient.
- In order to upload large file, it might needed to increase the authorized size for upload. Example for nginx:
`client_max_body_size 200M

##Uploading a resource from a local directory.


The package is done to read a directory that must contain one subdirectory for each resource to upload. Each subdirectory must contain

- A metadata file named `metadata.json`
- The file to be uploaded

The metadata file follows the same structure as the CKAN API structure. Below is an example:

```
{
    "name": "modes-transports",
    "resources": [
        {
            "description": "This is the description of the resource",
            "format": "JPG",
            "name": "Resource Name",
            "url": "an-image.jpg"        }
    ]
}
```

Where `name` is the name of dataset and `url` is the filename of the resource to be uploaded (in the same directory). The directory [resource.example](./resource.example) shows an example.

`ckan_uploader` is a complete scripts that uses the package to search for the directories, upload the resources and send a status mail. Most of the configuration is done in the file `config.cfg` (example provided: `config.cfg.example`)

##Other features

The package is also able to help manage groups and packages, for example to manage test/staging environments as well as production environments

Most of the examples below come from the `example.py` script

###Group management

####Delete all groups

```
ckanclient.get_package_list(ckanclient.ckan_target)
ckanclient.delete_all_packages()
```

####Replicate group structure from another CKAN instance

```
ckanclient.set_ckan_source('http://donnees.ville.montreal.qc.ca/api/')
ckanclient.get_group_list(ckanclient.ckan_source)
ckanclient.push_all_groups()
```

####Create ad hoc group

```
ckanclient.group_list.append({
	u'title': u'My custom group', 
	u'description': u'This is an updated description.', 
	u'state': u'active', 
	u'image_url': u'http://www.meteoweb.eu/wp-content/uploads/2011/08/meteo.png', 
	u'type': u'group',
	u'name': u'custom-group'})
ckanclient.push_all_groups()
```

###Package management

*Warning*: package assignment to an organization only works starting CKAN 2.2. Before that, the API does not support the `owner_org` attribute.

####Delete all packages from an instance

```
ckanclient.get_package_list(ckanclient.ckan_target)
ckanclient.delete_all_packages()
```

####Replicate selected packages from another instance 

(could be useful to set up a test/staging environment). If some resources are assigned to the package, they will be downloaded locally and pushed to the target CKAN instance.

```
ckanclient.set_ckan_source('http://donnees.ville.montreal.qc.ca/api/')
ckanclient.get_package_list(ckanclient.ckan_source, ('parcours-riverain', 'resultats-elections-2013'))
ckanclient.push_all_packages(False, 'ville-de-montreal', 'SCOL')
```

####Create an ad hoc package

```
ckanclient.package_list.append({
	u'name': u'injected-dataset',
	u'title': u'A dataset injected via the API',
	u'maintainer': u'The Maintainer', 
	u'maintainer_email': u'opendata@mycity.gov',
	u'notes': u'Some notes about the dataset.',
	u'tags': [u'One tag', u'Another tag'],
	u'groups': [u'custom-group']
	})
ckanclient.push_all_packages(False, 'ville-de-montreal', 'SCOL')
```

###Resource management

On top of scanning a directory, the script is also able to upload an ad hoc resource (the url can be either a local file or a remote file starting with http://) and assign it to a package.

```
resource = {
	'url': 'https://ckannet-storage.commondatastorage.googleapis.com/2013-10-27T16:40:24.027Z/open-data-census-database-2013-index-presentation-entries.csv', 
	'description': 'Test resource', 
	'format': 'CSV', 
	'name': 'Test resource'}

ckanclient.push_resource('injected-dataset', resource)
```