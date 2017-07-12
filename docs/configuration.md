# Configuration

The configuration for this module has to be specified as described in mapping-configuration.md in the mapping-core module.

| Key        | Values           | Default | Description  |
| ------------- |-------------| -----| ----- |
| operators.gfbiosource.dbcredentials | \<string\> | | The SQL connection string the database containing the GBIF/IUCN/GFBio data e.g. `user = 'user' host = 'localhost' password = 'pass' dbname = 'gfbio'`. |
| gfbio.abcd.datapath | \<string\> | | The path to the directory where the ABCD archives are stored. Note that this directory also has to contain the schema definition file. |
| gfbio.portal.user | \<string\> || The username of the GFBio portal user account for the VAT system to communicate with the portal. This account needs to have admin permissions on the portal |
| gfbio.portal.password| \<string\> || The password of the GFBio portal user account |
| gfbio.portal.authenticateurl | \<string\> || The url of the authenticate webservice of the GFBio portal, e.g https://gfbio-pub1.inf-bb.uni-jena.de/api/jsonws/GFBioProject-portlet.basket/authenticate |
| gfbio.portal.basketwebserviceurl | \<string\> || The url of the basket webservice of the GFBio portal, e.g. https://gfbio-pub1.inf-bb.uni-jena.de/api/jsonws/GFBioProject-portlet.basket/get-baskets-by-user-id |
|gfbio.portal.userdetailswebserviceurl | \<string\> || The url of the userdetails webservice of the GFBio portal, e.g. https://gfbio-pub1.inf-bb.uni-jena.de/api/jsonws/GFBioProject-portlet.basket/get-user-detail |


