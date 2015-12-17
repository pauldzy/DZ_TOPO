# DZ_TOPO
Utilities and helper objects for the manipulation and maintenance of Oracle Spatial topologies.
For the most up-to-date documentation see the auto-build  [dz_topo_deploy.pdf](https://github.com/pauldzy/DZ_TOPO/blob/master/dz_topo_deploy.pdf).

## Installation
Simply execute the deployment script into the schema of your choice.  Then execute the code using either the same or a different schema.  All procedures and functions are publically executable and utilize AUTHID CURRENT_USER for permissions handling.

Please note that the [Oracle Spatial Topology Data Model](https://docs.oracle.com/database/121/TOPOL/sdo_topo_concepts.htm#TOPOL100) requires the full Oracle Spatial and Graph license.  Please verify your rights to utilize topologies before any use in production.

## Running the scratch tests
Generic automated testing of complex data structures such a topologies is fairly daunting task and my code packages deliver very little on the matter.  I divide tests into "inmemory" and "scratch" functions.  Checks that can be performed in memory (e.g. such as converting SDO to GML) are fairly easy to provide (see the DZ_WKT packages for a good example).  However, topologies are complex data structures on disk and walking through any testing requires rights and resources you may not have.  Additionally generating sample test topology data from code would probably require a package of its own.  Thus the scratch tests in the DZ_TOPO package need quite a bit more infrastructure than most.  In the constants in the DZ_TOPO_TEST package, C_TEST_SCHEMA should point to a schema where you can create, drop and modify topologies.  C_DZ_TESTDATA needs to point to a schema containing my DZ_TESTDATA resources.  If you don't have those items handy, the scratch tests will just not run.  I am interested in these topics and if you have any feedback or suggestions please send them along.
