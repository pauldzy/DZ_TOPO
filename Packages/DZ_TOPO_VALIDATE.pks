CREATE OR REPLACE PACKAGE dz_topo_validate
AUTHID CURRENT_USER
AS

   TYPE validation_results IS TABLE OF VARCHAR2(4000 Char);
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   /*
   Function: dz_topo_validate.check_all

   Utility function to validate components of a given topology. 

   Parameters:

      p_topology_owner - owner of the topology
      p_topology_name - name of the topology
      
   Returns:

      Pipelined table of VARCHAR2(4000) informational messages
      
   Notes:
   
   - Note for a large topology this function may require a long time to complete.
   
   */
   FUNCTION check_all(
       p_topology_owner IN  VARCHAR2
      ,p_topology_name  IN  VARCHAR2
   ) RETURN validation_results PIPELINED;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   /*
   Function: dz_topo_validate.check_basic

   Utility function to validate a subset of a given topology. At this time the
   function is the same as check_all.

   Parameters:

      p_topology_owner - owner of the topology
      p_topology_name - name of the topology
      
   Returns:

      Pipelined table of VARCHAR2(4000) informational messages
      
   Notes:
   
   - Note for a large topology this function may require a long time to complete.
   
   */
   FUNCTION check_basic(
       p_topology_owner IN  VARCHAR2
      ,p_topology_name  IN  VARCHAR2
   ) RETURN validation_results PIPELINED;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   /*
   Function: dz_topo_validate.check_universal_face

   Utility function to inspect and report on topology elements which interact
   with the universal face.

   Parameters:

      p_topology_owner - owner of the topology
      p_topology_name - name of the topology
      
   Returns:

      Pipelined table of VARCHAR2(4000) informational messages
      
   Notes:
   
   - This utility is primarily concerned with reporting on island edges which
     are not contained within the universal face.
   
   */
   FUNCTION check_universal_face(
       p_topology_owner IN  VARCHAR2
      ,p_topology_name  IN  VARCHAR2
   ) RETURN validation_results PIPELINED;
   
END dz_topo_validate;
/

GRANT EXECUTE ON dz_topo_validate TO public;

