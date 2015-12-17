CREATE OR REPLACE PACKAGE BODY dz_topo_processing
AS

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION dz_validate(
       p_input    IN  MDSYS.SDO_TOPO_GEOMETRY
      ,p_filter   IN  VARCHAR2 DEFAULT 'ALL'
   ) RETURN VARCHAR2
   AS
   
   BEGIN
      
      NULL;
      
   END dz_validate;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE clean_overlaps_by_allocation(
       p_topology_name   IN  VARCHAR2 DEFAULT NULL
      ,p_topo_layer_name IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj    IN  dz_topology DEFAULT NULL
   )
   AS
      obj_topology       dz_topology := p_topology_obj;
      
   BEGIN
   
      NULL;
      
   END clean_overlaps_by_allocation;

END dz_topo_processing;
/

