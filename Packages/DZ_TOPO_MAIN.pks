CREATE OR REPLACE PACKAGE dz_topo_main
AUTHID CURRENT_USER
AS
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   /*
   header: DZ_TOPO
     
   - Build ID: DZBUILDIDDZ
   - TFS Change Set: DZCHANGESETDZ
   
   Utilities and helper objects for the manipulation and maintenance of Oracle 
   Spatial topologies.
   
   In order to avoid passing about all the gritty details of a topology
   workflow, the dz_topology object encapsulates the detail of the topology
   and topo map session for easy reuse by topology operators.
   
   Example:
   (start code)
   obj_topo := dz_topology(
       p_topology_name    => 'CATROUGH_NP21'
      ,p_active_table     => 'CATROUGH_NP21_TOPO'
      ,p_active_column    => 'TOPO_GEOM'
   );
         
   obj_topo.set_topo_map_mgr(
      dz_topo_map_mgr(
          p_window_sdo      => ary_shape
         ,p_window_padding  => 0.000001
         ,p_topology_name   => 'CATROUGH_NP21'
      )
   );
      
   obj_topo.java_memory(p_gigs => 13);
      
   FOR i IN 1 .. ary_featureids.COUNT
   LOOP
      topo_shape := obj_topo.create_feature(
         ary_shape(i)
      );
            
      INSERT INTO catrough_np21_topo(
          featureid
         ,topo_geom
      ) VALUES (
          ary_featureids(i)
         ,topo_shape
      );
            
   END LOOP;
      
   obj_topo.commit_topo_map();
   COMMIT;
   (end)
   */
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   
   TYPE topo_edge_hash IS TABLE OF dz_topo_edge
   INDEX BY PLS_INTEGER;

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION sdo_list_count(
      p_input          IN  MDSYS.SDO_LIST_TYPE
   ) RETURN NUMBER;

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   /*
   Function: dz_topo_main.aggregate_topology

   Function which takes an arbitrary number of topo geometries and returns an
   aggregated polygon representing the external boundary of all faces comprising
   the topo geometries. 

   Parameters:

      p_topology_owner - optional owner of the topology
      p_topology_name - optional name of the topology
      p_topology_obj - optional object dz_topology
      p_input_topo - object dz_topo_vry, a varray of topo geometries
      p_remove_holes - optional TRUE/FALSE flag to remove resulting interior rings
      
   Returns:

      MDSYS.SDO_GEOMETRY spatial type
      
   Notes:
   
   - Dz_topo functions and procedures either utilize an existing dz_topology 
     object or require the name and owner of the topology to internally initialize
     one.  Thus you either must provide name and owner or an initialized 
     dz_topology object to the function.  The latter format is most useful when
     calling several topology procedures in sequence.
   
   - By design this function does not invoke the .get_geometry() subroutines of
     Oracle Spatial topologies or utilize any vector spatial aggregation 
     techniques.  Rather the resulting polygon is built directly from chained 
     together topology edges.  The value of this approach is that it
     is not necessary to precompute relationships among existing topo geometries
     to obtain aggregated results.  For example with a topology of watershed
     polygons, provide a drainage area based upon initial location and
     arbitrary upstream distance.  Its unwieldy to impossible to prebuild topology 
     layers that could represent every possibility of polygon combinations.  This
     function provides the ability to aggregate together an arbitrary input of 
     topo geometries.

   */
   FUNCTION aggregate_topology(
       p_topology_owner IN  VARCHAR2 DEFAULT NULL
      ,p_topology_name  IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
      ,p_input_topo     IN  dz_topo_vry
      ,p_remove_holes   IN  VARCHAR2 DEFAULT 'FALSE'
   ) RETURN MDSYS.SDO_GEOMETRY;

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION topo2faces(
       p_topology_owner IN  VARCHAR2 DEFAULT NULL
      ,p_topology_name  IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
      ,p_input_topo     IN  dz_topo_vry
   ) RETURN MDSYS.SDO_NUMBER_ARRAY;

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION faces2exterior_edges(
       p_topology_owner IN  VARCHAR2 DEFAULT NULL
      ,p_topology_name  IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
      ,p_input_faces    IN  MDSYS.SDO_NUMBER_ARRAY
   ) RETURN dz_topo_edge_list;

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION edges2rings(
      p_input_edges     IN  dz_topo_edge_list
   ) RETURN dz_topo_ring_list;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION edges2strings(
      p_input_edges   IN  dz_topo_edge_list
   ) RETURN dz_topo_ring_list;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION strings2rings(
      p_input_strings   IN  dz_topo_ring_list
   ) RETURN dz_topo_ring_list;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION finalize_face_rings(
       p_input_rings      IN  dz_topo_ring_list
      ,p_island_edge_list IN  MDSYS.SDO_LIST_TYPE DEFAULT NULL
      ,p_tolerance        IN  NUMBER DEFAULT 0.05
   ) RETURN dz_topo_ring_list;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION finalize_generic_rings(
       p_input_rings    IN  dz_topo_ring_list
      ,p_tolerance      IN  NUMBER DEFAULT 0.05
   ) RETURN dz_topo_ring_list;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION rings2sdo(
       p_input_rings    IN  dz_topo_ring_list
      ,p_remove_holes   IN  VARCHAR2 DEFAULT 'FALSE'
   ) RETURN MDSYS.SDO_GEOMETRY;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION get_faces(
       p_input          IN  MDSYS.SDO_TOPO_GEOMETRY
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
   ) RETURN MDSYS.SDO_NUMBER_ARRAY;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   /*
   Function: dz_topo_main.face2sdo

   Utility function which provides the sdo geometry polygon representing a
   single face of a topology. 

   Parameters:

      p_topology_owner - optional owner of the topology
      p_topology_name - optional name of the topology
      p_topology_obj - optional object dz_topology
      p_face_id - single numeric face id
      p_remove_holes - optional TRUE/FALSE flag to remove resulting interior rings
      
   Returns:

      MDSYS.SDO_GEOMETRY spatial type
      
   Notes:
   
   - Dz_topo functions and procedures either utilize an existing dz_topology 
     object or require the name and owner of the topology to internally initialize
     one.  Thus you either must provide name and owner or an initialized 
     dz_topology object to the function.  The latter format is most useful when
     calling several topology procedures in sequence.
   
   */
   FUNCTION face2sdo(
       p_topology_owner IN  VARCHAR2 DEFAULT NULL
      ,p_topology_name  IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
      ,p_face_id        IN  NUMBER
      ,p_remove_holes   IN  VARCHAR2 DEFAULT 'FALSE'
   ) RETURN MDSYS.SDO_GEOMETRY;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION get_faces_as_sdo(
       p_input          IN  MDSYS.SDO_TOPO_GEOMETRY
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
   ) RETURN MDSYS.SDO_GEOMETRY;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE remove_faces_from_topo_raw(
       p_input          IN  MDSYS.SDO_TOPO_GEOMETRY
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
      ,p_face_ids       IN  MDSYS.SDO_NUMBER_ARRAY
      ,p_returning      OUT MDSYS.SDO_NUMBER_ARRAY
   );
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE add_faces_to_topo_raw(
       p_input          IN  MDSYS.SDO_TOPO_GEOMETRY
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
      ,p_face_ids       IN  MDSYS.SDO_NUMBER_ARRAY
      ,p_returning      OUT MDSYS.SDO_NUMBER_ARRAY
   );
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION get_interior_edges(
       p_input          IN  MDSYS.SDO_TOPO_GEOMETRY
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
   ) RETURN MDSYS.SDO_NUMBER_ARRAY;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION get_interior_edges_as_sdo(
       p_input          IN  MDSYS.SDO_TOPO_GEOMETRY
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
   ) RETURN MDSYS.SDO_GEOMETRY;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION get_shared_faces(
       p_input          IN  MDSYS.SDO_TOPO_GEOMETRY
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
   ) RETURN MDSYS.SDO_NUMBER_ARRAY;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION get_face_topo_neighbors(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL  
      ,p_topology_name   IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj    IN  dz_topology DEFAULT NULL
      ,p_tg_layer_id     IN  NUMBER 
      ,p_face_id         IN  NUMBER
   ) RETURN dz_topo_vry;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION face_at_point(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL  
      ,p_topology_name   IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj    IN  dz_topology DEFAULT NULL
      ,p_input           IN  MDSYS.SDO_GEOMETRY
   ) RETURN NUMBER;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   /*
   Function: dz_topo_main.face_at_point_sdo

   Utility function which provides the sdo geometry polygon representing a
   single face of a topology located using an input point geometry. 

   Parameters:

      p_topology_owner - optional owner of the topology
      p_topology_name - optional name of the topology
      p_topology_obj - optional object dz_topology
      p_input - sdo geometry input point
      
   Returns:

      MDSYS.SDO_GEOMETRY spatial type
      
   Notes:
   
   - Dz_topo functions and procedures either utilize an existing dz_topology 
     object or require the name and owner of the topology to internally initialize
     one.  Thus you either must provide name and owner or an initialized 
     dz_topology object to the function.  The latter format is most useful when
     calling several topology procedures in sequence.
   
   */
   FUNCTION face_at_point_sdo(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL  
      ,p_topology_name   IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj    IN  dz_topology DEFAULT NULL
      ,p_input           IN  MDSYS.SDO_GEOMETRY
   ) RETURN MDSYS.SDO_GEOMETRY;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE drop_face_clean(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL  
      ,p_topology_name   IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj    IN  dz_topology DEFAULT NULL
      ,p_face_id         IN  NUMBER
   );
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   /*
   Procedure: dz_topo_main.nuke_topology

   Fairly dangerous procedure to remove all traces of given topology including
   all topo tables registered to the topology. Do not use unless you are sure
   you do not need any of the data participating in your topology.

   Parameters:

      p_topology_owner - Optional owner of the topology to remove.
      p_topology_name - Name of the topology to utterly remove.

   Notes:
   
   - Please be careful with this.  I am not responsible for you destroying 
     your topology.

   */
   PROCEDURE nuke_topology(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL 
      ,p_topology_name   IN  VARCHAR2
   );
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE remove_face_island_edge_id(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL  
      ,p_topology_name   IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj    IN  dz_topology DEFAULT NULL
      ,p_face_id         IN  NUMBER
      ,p_edge_id         IN  NUMBER
   );
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE remove_face_island_edge_id(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL  
      ,p_topology_name   IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj    IN  dz_topology DEFAULT NULL
      ,p_face_id         IN  NUMBER
      ,p_edge_id         IN  MDSYS.SDO_NUMBER_ARRAY
   );
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE append_face_island_edge_id(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL  
      ,p_topology_name   IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj    IN  dz_topology DEFAULT NULL
      ,p_face_id         IN  NUMBER
      ,p_edge_id         IN  NUMBER
   );
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE remove_face_island_node_id(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL  
      ,p_topology_name   IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj    IN  dz_topology DEFAULT NULL
      ,p_face_id         IN  NUMBER
      ,p_node_id         IN  NUMBER
   );
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE append_face_island_node_id(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL  
      ,p_topology_name   IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj    IN  dz_topology DEFAULT NULL
      ,p_face_id         IN  NUMBER
      ,p_node_id         IN  NUMBER
   );
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   /*
   Procedure: dz_topo_main.unravel_face

   Utility procedure intended to gather information for the possible manual 
   removal of a topology face by identifying and first removing it's surrounding 
   referenced components.  This is usually done in cases of corruption in the 
   topology.  So upon providing a tg layer and face id, the procedure returns 
   the list of tg ids, faces, edges and nodes that must be removed in order to 
   delete the input face.  A sdo window is also provided for use in 
   constraining a topo map to the work area in question.

   Parameters:

      p_topology_owner - Optional owner of the topology
      p_topology_name - Optional name of the topology
      p_topology_obj - Optional object dz_topology
      p_tg_layer_id - the tg_layer to execute the unravel against
      p_face_id - the face to execute the unravel against
      p_tg_ids - output tg ids 
      p_face_ids - output face ids
      p_edge_ids - output edge ids
      p_node_ids - output node ids
      p_window_sdo - output geometry window surrounding the action
   
   Notes:
   
   - Note this procedure only returns information about a topology and does 
     not make any changes to the input topology.
   
   - Taking the results of this procedures and manually deleting resources
     in your topology is highly dangerous and should not be done unless all 
     other options have been explored first.
   
   - On the other hand this procedure could also function as the core of a 
     nice little topology viewer showing you how your face in question ties
     together with other components.  

   */
   PROCEDURE unravel_face(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL  
      ,p_topology_name   IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj    IN  dz_topology DEFAULT NULL
      ,p_tg_layer_id     IN  NUMBER DEFAULT 1
      ,p_face_id         IN  NUMBER
      ,p_tg_ids          OUT MDSYS.SDO_NUMBER_ARRAY
      ,p_face_ids        OUT MDSYS.SDO_NUMBER_ARRAY
      ,p_edge_ids        OUT MDSYS.SDO_NUMBER_ARRAY
      ,p_node_ids        OUT MDSYS.SDO_NUMBER_ARRAY
      ,p_window_sdo      OUT MDSYS.SDO_GEOMETRY
   );
   
END dz_topo_main;
/

GRANT EXECUTE ON dz_topo_main TO public;

