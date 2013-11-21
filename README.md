This package is an encapsulation of ckanclient to do the following:
- Push package or resource via the API using an ad hoc JSON structure
- Replicate the content of a CKAN to another one (even partially)

##Important
- In order to work with non-ASCII file upload, this script needs our own version of the ckan client that supports non-ASCII
- In order to upload large file, it might needed to increase the authorized size for upload. Example for nginx:
`client_max_body_size 200M

##How it work

the package ckanclientmtl contains some functions to easily push packages or resource to a ckan instance either from another CKAN instance of from a directory containing a JSON metadata + file.

example.py shows an example of pushing a resource from a directory.

The metadata file follows the same structure as the CKAN API structure. So to upload a resource, the metadata should be in file named `resource.json` with the following content:

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

Where `name` is the name of dataset and `url` is the filename of the resource to be uploaded (in the same directory).