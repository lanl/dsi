Permissions
===================
DSI is capable of consuming information from files, environments, and in-situ processes which may or may not have the same permissions authority. To track this information for the purposes of returning user queries into DSI storage, we utilize a permissions handler. The permissions handler bundles the authority by which information is read and adds this to each column data structure. Most relational database systems require that types are encforced by column, and DSI extends this idea to require that permissions are enforced by column. By tracking the permissions associated with each column, DSI can save files using the same POSIX permissions authority that initially granted access to the information, therefore preserving POSIX permssions as files are saved.

By default, DSI will stop users from saving any data if the length of the union of the set of column permissions is greater than one. This prevents users from saving files that might have complex security implications. If a user enables the `allow_multiple_permissions` parameter of the `PermissionsManager`, then the number of files that will be saved is equal to the length of the union of the set of column permissions in the middelware data structures being written. There will be one file for each set of columns read by the same permissions authority.

By default, DSI will always respect the POSIX security information by which information was read. If the usr wishes to override this behavior and write all of their metadata to the same file with a unified UID and GID, they can enable the `squash_permissions` perameter of the `PermissionsManager`. The user should be very certain that the information they are writing is protected appropriately in this case.

An example helps illustrate these scenarios:

| Col A | Col B | Col C |
=========================
|Perm D |Perm D | Perm F|
|Row A1 |Row B1 | Row C1|
|Row A2 |Row B2 | Row C2|

By default, DSI will refuse to write this data structure to disk because `len(union({D,D,F})) > 1`

If a user enables the `allow_multiple_permissions` parameter, two files will be saved:

>>> $ cat file1
>>> | Col A  | Col B  |
>>> ===================
>>> | Perm D | Perm D |
>>> | Row A1 | Row B1 |
>>> | Row A2 | Row B2 |
>>> $ get_perms(file1)
>>> Perm D
>>> $ cat file2
>>> | Col C  |
>>> ==========
>>> | Perm F |
>>> | Row C1 |
>>> | Row C2 |
>>> $ get_perms(file2)
>>> Perm F

If a user enables `allow_multiple_permissions` and `squash_permissions`, then a single file will be written with the users UID and effective GID and 660 access:

>>> $ cat file
>>> | Col A  | Col B  | Col C  |
>>> ============================
>>> | Perm D | Perm D | Perm F |
>>> | Row A1 | Row B1 | Row C1 |
>>> | Row A2 | Row B2 | Row C2 |
>>> $ get_perms(file)
>>> My UID and Effective GID, with 660 access controls.


.. automodule:: dsi.permissions.permissions
   :members:
