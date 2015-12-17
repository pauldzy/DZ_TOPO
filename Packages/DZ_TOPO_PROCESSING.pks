CREATE OR REPLACE PACKAGE dz_topo_processing
AUTHID CURRENT_USER
AS

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION dz_validate(
       p_input    IN  MDSYS.SDO_TOPO_GEOMETRY
      ,p_filter   IN  VARCHAR2 DEFAULT 'ALL'
   ) RETURN VARCHAR2;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE clean_overlaps_by_allocation(
       p_topology_name   IN  VARCHAR2 DEFAULT NULL
      ,p_topo_layer_name IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj    IN  dz_topology DEFAULT NULL
   );

END dz_topo_processing;
/

GRANT EXECUTE ON dz_topo_processing TO public;

