#############
Authorization
#############


This document provides an overview of the `Authorization API` endpoints used for managing `CWMS Data API` authentication keys.

Endpoints
---------

*Authentication is required to access these endpoints*


* GET `/cwms-data/auth/keys/{key-name}` : Retrieve `CDA` authentication keys by a specific `name`.



**Request**
- 
**Response**
- **Status**: 200 OK
- **Body**: Returns the details of the specified authentication key.

#### Retrieve a Key


The following table provides an overview of the available endpoints in the Authorization API:

+--------------------------------------------+---------------------------------------------+-----------------------------------+
| **Endpoint**                               | **Description**                             | **Details**                      |
+============================================+=============================================+===================================+
| `GET /cwms-data/auth/keys/{key-name}`      | Retrieve a specific auth key by `keyName`.  | Returns key details in JSON.     |
+--------------------------------------------+---------------------------------------------+-----------------------------------+
| `DELETE /cwms-data/auth/keys/{key-name}`   | Delete a specific auth key by `keyName`.    | Returns `204 No Content` status. |
+--------------------------------------------+---------------------------------------------+-----------------------------------+
| `GET /cwms-data/auth/keys`                 | Retrieve all available auth keys.           | Returns list of keys in JSON.    |
+--------------------------------------------+---------------------------------------------+-----------------------------------+
| `POST /cwms-data/auth/keys`                | Create or update an auth key.               | Returns created/updated key.     |
+--------------------------------------------+---------------------------------------------+-----------------------------------+

========== ================================= ========================================== ===========
**Method** **Endpoint**                      **Description**                            **Details**
========== ================================= ========================================== ===========
`GET`      `/cwms-data/auth/keys/{key-name}` Retrieve a specific auth key by `keyName`. 
`DELETE`   `/cwms-data/auth/keys/{key-name}` Delete a specific auth key by `keyName`.
`GET`      `/cwms-data/auth/keys`            Retrieve all available auth keys.
`POST`     `/cwms-data/auth/keys`            Create or update an auth key.    
========== ================================= ========================================== ===========

Notes
-----
- Replace `{key-name}` with the name of the key you are targeting.
- Ensure authentication headers are included if required.