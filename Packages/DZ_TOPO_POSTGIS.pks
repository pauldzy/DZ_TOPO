CREATE OR REPLACE PACKAGE dz_topo_postgis
AUTHID CURRENT_USER
AS

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   /*
   Procedure: dz_topo_postgis.export_topology

   Placeholder for a abandoned project to convert an Oracle Spatial topology into 
   a PostGIS topology.   

   Parameters:

      p_topology_owner - optional owner of the topology
      p_topology_name - optional name of the topology
      p_topology_obj - optional object dz_topology
      p_directory - Oracle directory in which to place resulting PostGIS topology 
      creation files
      p_filename_suffix - optional suffix to add to topology creation files
      p_postgis_topology - optional name for PostGIS topology schema if different
      from Oracle Spatial topology name.
      
   Returns:

      p_return_code - zero for success or numeric error code
      p_status_message - message describing any resulting error codes
      
   Notes:
   
   - Dz_topo functions and procedures either utilize an existing dz_topology 
     object or require the name and owner of the topology to internally initialize
     one.  Thus you either must provide name and owner or an initialized 
     dz_topology object to the function.  The latter format is most useful when
     calling several topology procedures in sequence.
   
   - As noted this is project never implemented.  If you have code yourself to
     accomplish this task, please drop me a line.

   */
   PROCEDURE export_topology(
       p_topology_owner    IN  VARCHAR2 DEFAULT NULL  
      ,p_topology_name     IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj      IN  dz_topology DEFAULT NULL
      ,p_directory         IN  VARCHAR2 DEFAULT 'LOADING_DOCK'
      ,p_filename_suffix   IN  VARCHAR2 DEFAULT NULL
      ,p_postgis_topology  IN  VARCHAR2 DEFAULT NULL
      ,p_return_code       OUT NUMBER
      ,p_status_message    OUT VARCHAR2
   );
   
END dz_topo_postgis;
/

GRANT EXECUTE ON dz_topo_postgis TO public;

