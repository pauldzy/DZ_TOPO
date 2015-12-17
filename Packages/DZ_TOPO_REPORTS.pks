CREATE OR REPLACE PACKAGE dz_topo_reports
AUTHID CURRENT_USER
AS

   TYPE report_results IS TABLE OF VARCHAR2(4000 Char);
   
   TYPE edge_rec IS RECORD(
       edge_id             NUMBER
      ,start_node_id       NUMBER
      ,end_node_id         NUMBER
      ,next_left_edge_id   NUMBER
      ,prev_left_edge_id   NUMBER
      ,next_right_edge_id  NUMBER
      ,prev_right_edge_id  NUMBER
      ,left_face_id        NUMBER
      ,right_face_id       NUMBER
      ,geometry            MDSYS.SDO_GEOMETRY
      ,left_face_is_outer  VARCHAR2(5 Char)
      ,right_face_is_outer VARCHAR2(5 Char)
   );
   
   TYPE edge_tbl IS TABLE OF edge_rec;
   
   TYPE node_rec IS RECORD(
       node_id            NUMBER
      ,edge_id            NUMBER
      ,face_id            NUMBER
      ,geometry           MDSYS.SDO_GEOMETRY
   );
   
   TYPE node_tbl IS TABLE OF node_rec;
   
   TYPE face_rec IS RECORD(
       face_id             NUMBER
      ,boundary_edge_id    NUMBER
      ,island_edge_id_list MDSYS.SDO_LIST_TYPE
      ,island_node_id_list MDSYS.SDO_LIST_TYPE
      ,mbr_geometry        MDSYS.SDO_GEOMETRY
   );
   
   TYPE face_tbl IS TABLE OF face_rec;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   /*
   Function: dz_topo_reports.edge_report

   Reporting function to dump pertinent information about a given edge and how
   the edge relates to all its neighbors including notes about any corruption
   found amongst the edge neighbors.

   Parameters:

      p_topology_owner - optional owner of the topology
      p_topology_name - optional name of the topology
      p_topology_obj - optional object dz_topology
      p_edge_id - the id of the edge to inspect
      
   Returns:

      Pipelined table of VARCHAR2(4000 Char)
      
   Notes:
   
   - Dz_topo functions and procedures either utilize an existing dz_topology 
     object or require the name and owner of the topology to internally initialize
     one.  Thus you either must provide name and owner or an initialized 
     dz_topology object to the function.  The latter format is most useful when
     calling several topology procedures in sequence.
   
   - The edge report was created for debugging purposes.

   */
   FUNCTION edge_report(
       p_topology_owner IN  VARCHAR2 DEFAULT NULL
      ,p_topology_name  IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
      ,p_edge_id        IN  NUMBER
   ) RETURN report_results PIPELINED;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   /*
   Function: dz_topo_reports.face_report

   Reporting function to dump pertinent information about a given face and how
   the face relates to all its neighbors.   

   Parameters:

      p_topology_owner - optional owner of the topology
      p_topology_name - optional name of the topology
      p_topology_obj - optional object dz_topology
      p_face_id - the id of the face to inspect
      
   Returns:

      Pipelined table of VARCHAR2(4000 Char)
      
   Notes:
   
   - Dz_topo functions and procedures either utilize an existing dz_topology 
     object or require the name and owner of the topology to internally initialize
     one.  Thus you either must provide name and owner or an initialized 
     dz_topology object to the function.  The latter format is most useful when
     calling several topology procedures in sequence.
   
   - The face report was created for debugging purposes.

   */
   FUNCTION face_report(
       p_topology_owner IN  VARCHAR2 DEFAULT NULL
      ,p_topology_name  IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
      ,p_face_id        IN  NUMBER
   ) RETURN report_results PIPELINED;
   
END dz_topo_reports;
/

GRANT EXECUTE ON dz_topo_reports TO public;

