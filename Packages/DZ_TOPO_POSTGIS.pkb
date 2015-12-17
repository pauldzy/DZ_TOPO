CREATE OR REPLACE PACKAGE BODY dz_topo_postgis
AS

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE export_topology(
       p_topology_owner    IN  VARCHAR2 DEFAULT NULL  
      ,p_topology_name     IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj      IN  dz_topology DEFAULT NULL
      ,p_directory         IN  VARCHAR2 DEFAULT 'LOADING_DOCK'
      ,p_filename_suffix   IN  VARCHAR2 DEFAULT NULL
      ,p_postgis_topology  IN  VARCHAR2 DEFAULT NULL
      ,p_return_code       OUT NUMBER
      ,p_status_message    OUT VARCHAR2
   )
   AS
      clb_output   CLOB;
      str_filename VARCHAR2(4000 Char);
   
   BEGIN
   
      NULL;
      
   END export_topology;

END dz_topo_postgis;
/

