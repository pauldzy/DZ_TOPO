CREATE OR REPLACE PACKAGE BODY dz_topo_main
AS

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION sdo_list_count(
      p_input      IN  MDSYS.SDO_LIST_TYPE
   ) RETURN NUMBER
   AS
   BEGIN
      RETURN p_input.COUNT;
      
   END sdo_list_count;

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION aggregate_topology(
       p_topology_owner IN  VARCHAR2 DEFAULT NULL
      ,p_topology_name  IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
      ,p_input_topo     IN  dz_topo_vry
      ,p_remove_holes   IN  VARCHAR2 DEFAULT 'FALSE'
   ) RETURN MDSYS.SDO_GEOMETRY
   AS
      obj_topology     dz_topology := p_topology_obj;
      ary_faces        MDSYS.SDO_NUMBER_ARRAY;
      ary_edges        dz_topo_edge_list;
      ary_rings        dz_topo_ring_list;
      sdo_output       MDSYS.SDO_GEOMETRY;
      str_remove_holes VARCHAR2(5 Char) := UPPER(p_remove_holes);

   BEGIN

      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topology_owner => p_topology_owner
            ,p_topology_name  => p_topology_name
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;

      IF str_remove_holes IS NULL
      THEN
         str_remove_holes := 'FALSE';
         
      ELSIF str_remove_holes NOT IN ('TRUE','FALSE')
      THEN
         RAISE_APPLICATION_ERROR(-20001,'boolean error');
         
      END IF;

      --------------------------------------------------------------------------
      -- Step 20
      -- Generate the array of faces
      --------------------------------------------------------------------------
      ary_faces := topo2faces(
          p_topology_obj => obj_topology
         ,p_input_topo   => p_input_topo
      );

      --------------------------------------------------------------------------
      -- Step 20
      -- Generate the array of edges
      --------------------------------------------------------------------------
      ary_edges := faces2exterior_edges(
          p_topology_obj => obj_topology
         ,p_input_faces  => ary_faces
      );

      --------------------------------------------------------------------------
      -- Step 30
      -- Generate the array of rings
      --------------------------------------------------------------------------
      ary_rings := edges2rings(ary_edges);
      
      ary_rings := finalize_generic_rings(ary_rings);

      --------------------------------------------------------------------------
      -- Step 40
      -- Generate the output sdo_geometry
      --------------------------------------------------------------------------
      sdo_output := rings2sdo(ary_rings,str_remove_holes);

      --------------------------------------------------------------------------
      -- Step 50
      -- Return the results
      --------------------------------------------------------------------------
      RETURN sdo_output;

   END aggregate_topology;

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION topo2faces(
       p_topology_owner IN  VARCHAR2 DEFAULT NULL
      ,p_topology_name  IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
      ,p_input_topo     IN  dz_topo_vry
   ) RETURN MDSYS.SDO_NUMBER_ARRAY
   AS
      obj_topology     dz_topology := p_topology_obj;
      str_sql          VARCHAR2(4000 Char);
      ary_output       MDSYS.SDO_NUMBER_ARRAY;

   BEGIN

      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topology_owner => p_topology_owner
            ,p_topology_name  => p_topology_name
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;

      --------------------------------------------------------------------------
      -- Step 20
      -- Fetch the faces from the RELATION$ table
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || 'DISTINCT '
              || 'topo_id '
              || 'FROM ('
              || '   SELECT '
              || '   a.topo_id '
              || '   FROM '
              || '   ' || obj_topology.relation_table() || ' a '
              || '   JOIN '
              || '   TABLE(:p01) b '
              || '   ON '
              || '   a.tg_layer_id = b.tg_layer_id AND '
              || '   a.tg_id = b.tg_id '
              || '   WHERE '
              || '   a.topo_type = 3 '
              || ') ';

      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO ary_output
      USING p_input_topo;

      --------------------------------------------------------------------------
      -- Step 30
      -- Return the array of unique faces
      --------------------------------------------------------------------------
      RETURN ary_output;

   END topo2faces;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE faces2edges(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL
      ,p_topology_name   IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj    IN  dz_topology DEFAULT NULL
      ,p_faces           IN  MDSYS.SDO_NUMBER_ARRAY
      ,p_subset          IN  VARCHAR2 DEFAULT NULL
      ,p_output          OUT dz_topo_edge_list
   )
   AS
      obj_topology     dz_topology := p_topology_obj;
      str_sql          VARCHAR2(4000 Char);
      str_subset       VARCHAR2(4000 Char) := UPPER(p_subset);
      
   BEGIN

      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_faces IS NULL
      OR p_faces.COUNT = 0
      THEN
         RETURN;
         
      END IF;
      
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topology_owner => p_topology_owner
            ,p_topology_name  => p_topology_name
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;

      --------------------------------------------------------------------------
      -- Step 20
      -- Check possible subset values
      --------------------------------------------------------------------------
      IF str_subset IS NULL
      THEN
         str_subset := 'ALL';
         
      ELSIF str_subset NOT IN ('ALL','INTERIOR','EXTERIOR')
      THEN
          RAISE_APPLICATION_ERROR(-20001,'subset values are ALL, INTERIOR or EXTERIOR');
          
      END IF;

      --------------------------------------------------------------------------
      -- Step 30
      -- Build SQL to fetch edges from topology
      --------------------------------------------------------------------------
      IF str_subset = 'INTERIOR'
      THEN
         str_sql := 'SELECT '
                 || dz_topo_util.c_schema() || '.dz_topo_edge( '
                 || '   a.edge_id, '
                 || '   ''L'', '
                 || '   a.start_node_id, '
                 || '   a.end_node_id, '
                 || '   a.geometry, '
                 || '   0 '
                 || ') '
                 || 'FROM '
                 || obj_topology.edge_table() || ' a '
                 || 'WHERE '
                 || 'a.left_face_id IN (SELECT * FROM TABLE(:p01)) AND '
                 || 'a.right_face_id IN (SELECT * FROM TABLE(:p02)) ';
         
         EXECUTE IMMEDIATE str_sql
         BULK COLLECT INTO p_output
         USING p_faces,p_faces;
         
      ELSIF str_subset = 'EXTERIOR'
      THEN
         str_sql := 'SELECT '
                 || dz_topo_util.c_schema() || '.dz_topo_edge( '
                 || '   a.edge_id, '
                 || '   ''L'', '
                 || '   a.start_node_id, '
                 || '   a.end_node_id, '
                 || '   a.geometry, '
                 || '   0 '
                 || ') '
                 || 'FROM '
                 || obj_topology.edge_table() || ' a '
                 || 'WHERE '
                 || 'a.left_face_id IN (SELECT * FROM TABLE(:p01)) AND '
                 || 'a.right_face_id NOT IN (SELECT * FROM TABLE(:p02)) '
                 || 'UNION ALL SELECT '
                 || dz_topo_util.c_schema() || '.dz_topo_edge( '
                 || '   a.edge_id, '
                 || '   ''R'', '
                 || '   a.start_node_id, '
                 || '   a.end_node_id, '
                 || '   a.geometry, '
                 || '   0 '
                 || ') '
                 || 'FROM '
                 || obj_topology.edge_table() || ' a '
                 || 'WHERE '
                 || 'a.left_face_id NOT IN (SELECT * FROM TABLE(:p01)) AND '
                 || 'a.right_face_id IN (SELECT * FROM TABLE(:p02)) '; 
              
         EXECUTE IMMEDIATE str_sql
         BULK COLLECT INTO p_output
         USING p_faces,p_faces,p_faces,p_faces;
         
      ELSIF str_subset = 'ALL'
      THEN
         str_sql := 'SELECT '
                 || dz_topo_util.c_schema() || '.dz_topo_edge( '
                 || '   a.edge_id, '
                 || '   ''L'', '
                 || '   a.start_node_id, '
                 || '   a.end_node_id, '
                 || '   a.geometry, '
                 || '   0 '
                 || ') '
                 || 'FROM '
                 || obj_topology.edge_table() || ' a '
                 || 'WHERE '
                 || 'a.left_face_id IN (SELECT * FROM TABLE(:p01)) AND '
                 || 'a.right_face_id NOT IN (SELECT * FROM TABLE(:p02)) '
                 || 'UNION ALL SELECT '
                 || dz_topo_util.c_schema() || '.dz_topo_edge( '
                 || '   a.edge_id, '
                 || '   ''R'', '
                 || '   a.start_node_id, '
                 || '   a.end_node_id, '
                 || '   a.geometry, '
                 || '   0 '
                 || ') '
                 || 'FROM '
                 || obj_topology.edge_table() || ' a '
                 || 'WHERE '
                 || 'a.left_face_id NOT IN (SELECT * FROM TABLE(:p01)) AND '
                 || 'a.right_face_id IN (SELECT * FROM TABLE(:p02)) '
                 || 'UNION ALL SELECT '
                 || dz_topo_util.c_schema() || '.dz_topo_edge( '
                 || '   a.edge_id, '
                 || '   ''L'', '
                 || '   a.start_node_id, '
                 || '   a.end_node_id, '
                 || '   a.geometry, '
                 || '   0 '
                 || ') '
                 || 'FROM '
                 || obj_topology.edge_table() || ' a '
                 || 'WHERE '
                 || 'a.left_face_id IN (SELECT * FROM TABLE(:p01)) AND '
                 || 'a.right_face_id IN (SELECT * FROM TABLE(:p02)) ';
              
         EXECUTE IMMEDIATE str_sql
         BULK COLLECT INTO p_output
         USING p_faces,p_faces,p_faces,p_faces,p_faces,p_faces;
         
      END IF;
   
   END faces2edges;

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION faces2exterior_edges(
       p_topology_owner IN  VARCHAR2 DEFAULT NULL
      ,p_topology_name  IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
      ,p_input_faces    IN  MDSYS.SDO_NUMBER_ARRAY
   ) RETURN dz_topo_edge_list
   AS
      obj_topology     dz_topology := p_topology_obj;
      ary_output       dz_topo_edge_list;

   BEGIN

      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_input_faces IS NULL
      OR p_input_faces.COUNT = 0
      THEN
         RETURN NULL;
         
      END IF;

      --------------------------------------------------------------------------
      -- Step 20
      -- Determine topology location
      --------------------------------------------------------------------------
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topology_owner => p_topology_owner
            ,p_topology_name  => p_topology_name
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;

      --------------------------------------------------------------------------
      -- Step 30
      -- Fetch edges having our faces on the LEFT side
      --------------------------------------------------------------------------
      faces2edges(
          p_topology_obj    => obj_topology
         ,p_faces           => p_input_faces
         ,p_subset          => 'EXTERIOR'
         ,p_output          => ary_output
      );

      --------------------------------------------------------------------------
      -- Step 50
      -- Return the results
      --------------------------------------------------------------------------
      RETURN ary_output;

   END faces2exterior_edges;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION faces2interior_edges(
       p_topology_owner IN  VARCHAR2 DEFAULT NULL
      ,p_topology_name  IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
      ,p_input_faces    IN  MDSYS.SDO_NUMBER_ARRAY
   ) RETURN dz_topo_edge_list
   AS
      obj_topology     dz_topology := p_topology_obj;
      ary_output       dz_topo_edge_list;

   BEGIN

      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_input_faces IS NULL
      OR p_input_faces.COUNT = 0
      THEN
         RETURN NULL;
         
      END IF;

      --------------------------------------------------------------------------
      -- Step 20
      -- Determine topology location
      --------------------------------------------------------------------------
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topology_owner => p_topology_owner
            ,p_topology_name  => p_topology_name
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;

      --------------------------------------------------------------------------
      -- Step 30
      -- Fetch edges having our faces on the LEFT side
      --------------------------------------------------------------------------
      faces2edges(
          p_topology_obj    => obj_topology
         ,p_faces           => p_input_faces
         ,p_subset          => 'INTERIOR'
         ,p_output          => ary_output
      );

      --------------------------------------------------------------------------
      -- Step 50
      -- Return the results
      --------------------------------------------------------------------------
      RETURN ary_output;

   END faces2interior_edges;

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION edges2rings(
      p_input_edges     IN  dz_topo_edge_list
   ) RETURN dz_topo_ring_list
   AS
      ary_output       dz_topo_ring_list;
      hash_edges       topo_edge_hash;
      obj_current_edge dz_topo_edge;
      int_ring_index   PLS_INTEGER;
      int_deadman      PLS_INTEGER;
      int_hash_index   PLS_INTEGER;
      ary_candidates   dz_topo_edge_list;
      int_candidates   PLS_INTEGER;
      boo_check        BOOLEAN := TRUE;

   BEGIN
      
      --------------------------------------------------------------------------
      -- Step 10
      -- Initialize ring creation
      --------------------------------------------------------------------------
      int_ring_index := 1;
      ary_output := dz_topo_ring_list();
      
      --------------------------------------------------------------------------
      -- Step 10
      -- Convert the incoming object array into a hash with edge id as key
      -- Store a second copy for retrieving edges when we unravel a ring
      --------------------------------------------------------------------------
      FOR i IN 1 .. p_input_edges.COUNT
      LOOP
         -- Pull out any single loops
         IF p_input_edges(i).start_node_id = p_input_edges(i).end_node_id
         THEN
            ary_output.EXTEND();
            ary_output(int_ring_index) := dz_topo_ring(
               p_input_edges(i),
               int_ring_index
            );
            
            IF ary_output(int_ring_index).shape.get_gtype() = 2
            THEN
               ary_output(int_ring_index).shape := MDSYS.SDO_GEOMETRY(
                   TO_NUMBER(ary_output(int_ring_index).shape.get_dims() || '003')
                  ,ary_output(int_ring_index).shape.SDO_SRID
                  ,NULL
                  ,MDSYS.SDO_ELEM_INFO_ARRAY(1,1003,1)
                  ,ary_output(int_ring_index).shape.SDO_ORDINATES
               );
               
            END IF;
            
            int_ring_index := int_ring_index + 1;
            
         ELSE
            hash_edges(p_input_edges(i).edge_id) := p_input_edges(i);
            
         END IF;
         
      END LOOP;
    
      --------------------------------------------------------------------------
      -- Step 30
      -- Exit if hash is empty of edges
      --------------------------------------------------------------------------
      int_deadman := 0;
      <<new_ring>>
      WHILE boo_check
      LOOP
         IF hash_edges.COUNT = 0
         THEN
            RETURN ary_output;
            
         END IF;

      --------------------------------------------------------------------------
      -- Step 30
      -- Grab an edge from the bucket
      --------------------------------------------------------------------------
         int_hash_index := hash_edges.FIRST;
         obj_current_edge := hash_edges(int_hash_index);
         hash_edges.DELETE(int_hash_index);

      --------------------------------------------------------------------------
      -- Step 40
      -- Create a new ring object
      --------------------------------------------------------------------------
         ary_output.EXTEND();
         ary_output(int_ring_index) := dz_topo_ring(
             obj_current_edge
            ,int_ring_index
         );

      --------------------------------------------------------------------------
      -- Step 50
      -- Search for a connecting node
      --------------------------------------------------------------------------
         <<new_edge>>
         WHILE boo_check
         LOOP
            ary_candidates := dz_topo_edge_list();
            int_candidates := 1;

      --------------------------------------------------------------------------
      -- Step 60
      -- Loop through the hash looking for node matches
      --------------------------------------------------------------------------
            int_hash_index := hash_edges.FIRST;
            LOOP
               EXIT WHEN NOT hash_edges.EXISTS(int_hash_index);

         ----------------------------------------------------------------------
         -- Step 60.10
         -- Search for a node matches in the bucket
         ----------------------------------------------------------------------
               IF  ary_output(int_ring_index).head_node_id = hash_edges(int_hash_index).start_node_id
               AND ary_output(int_ring_index).ring_interior = hash_edges(int_hash_index).interior_side
               THEN
                  ary_candidates.EXTEND();
                  ary_candidates(int_candidates) := hash_edges(int_hash_index);
                  int_candidates := int_candidates + 1;
                  
               ELSIF ary_output(int_ring_index).head_node_id = hash_edges(int_hash_index).end_node_id
               AND ary_output(int_ring_index).ring_interior = hash_edges(int_hash_index).exterior_side()
               THEN
                  ary_candidates.EXTEND();
                  ary_candidates(int_candidates) := hash_edges(int_hash_index);
                  ary_candidates(int_candidates).flip();
                  int_candidates := int_candidates + 1;
                  
               END IF;

         ----------------------------------------------------------------------
         -- Step 60.20
         -- Increment to the next edge in the hash bucket
         ----------------------------------------------------------------------
               int_hash_index  := hash_edges.NEXT(int_hash_index);

            END LOOP;

      --------------------------------------------------------------------------
      -- Step 70
      -- Decide what to do based on the number of matches, one is most easy
      --------------------------------------------------------------------------
            IF ary_candidates.COUNT = 1
            THEN
               ary_output(int_ring_index).append_edge(ary_candidates(1));
               hash_edges.DELETE(ary_candidates(1).edge_id);
               
            ELSE
               -- Reversing the direction seems to be the most efficient way to proceed
               IF ary_output(int_ring_index).try_reversal = 'TRUE'
               THEN
                  ary_output(int_ring_index).flip();
                  ary_output(int_ring_index).try_reversal := 'FALSE';
                  CONTINUE new_edge;
                  
               END IF;

               -- Do not go through the intersection
               -- rather move on to a new ring
               int_ring_index := int_ring_index + 1;
               CONTINUE new_ring;
            
            END IF;
     
      --------------------------------------------------------------------------
      -- Step 80
      -- Check that process is not stuck
      --------------------------------------------------------------------------
            IF int_deadman > p_input_edges.COUNT * 2
            THEN
               RAISE_APPLICATION_ERROR(-20001,'deadman switch!');
               
            ELSE
               int_deadman := int_deadman + 1;
               
            END IF;

      --------------------------------------------------------------------------
      -- Step 90
      -- Check if ring is complete
      --------------------------------------------------------------------------
            IF ary_output(int_ring_index).head_node_id = ary_output(int_ring_index).tail_node_id
            THEN
               IF ary_output(int_ring_index).shape.get_gtype() = 2
               THEN
                  ary_output(int_ring_index).shape := MDSYS.SDO_GEOMETRY(
                     TO_NUMBER(ary_output(int_ring_index).shape.get_dims() || '003'),
                     ary_output(int_ring_index).shape.SDO_SRID,
                     NULL,
                     MDSYS.SDO_ELEM_INFO_ARRAY(1,1003,1),
                     ary_output(int_ring_index).shape.SDO_ORDINATES
                  );
                     
               END IF;
                  
               int_ring_index := int_ring_index + 1;
               
               CONTINUE new_ring;

            END IF;
 
      --------------------------------------------------------------------------
      -- Step 100
      -- Go back and try again
      --------------------------------------------------------------------------
         END LOOP; --new edge--
         
      END LOOP; --new ring--
   
   END edges2rings;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION edges2strings(
      p_input_edges   IN  dz_topo_edge_list
   ) RETURN dz_topo_ring_list
   AS
      ary_output       dz_topo_ring_list;
      hash_edges       topo_edge_hash;
      obj_current_edge dz_topo_edge;
      int_ring_index   PLS_INTEGER;
      int_deadman      PLS_INTEGER;
      int_hash_index   PLS_INTEGER;
      ary_candidates   dz_topo_edge_list;
      int_candidates   PLS_INTEGER;
      boo_check        BOOLEAN := TRUE;

   BEGIN
      
      --------------------------------------------------------------------------
      -- Step 10
      -- Initialize ring creation
      --------------------------------------------------------------------------
      int_ring_index := 1;
      ary_output := dz_topo_ring_list();
      
      --------------------------------------------------------------------------
      -- Step 10
      -- Convert the incoming object array into a hash with edge id as key
      -- Store a second copy for retrieving edges when we unravel a ring
      --------------------------------------------------------------------------
      FOR i IN 1 .. p_input_edges.COUNT
      LOOP
         -- Pull out any single loops
         IF p_input_edges(i).start_node_id = p_input_edges(i).end_node_id
         THEN
            ary_output.EXTEND();
            ary_output(int_ring_index) := dz_topo_ring(
               p_input_edges(i),
               int_ring_index
            );
            ary_output(int_ring_index).finalize();
            int_ring_index := int_ring_index + 1;
         ELSE
            hash_edges(p_input_edges(i).edge_id) := p_input_edges(i);
         END IF;
         
      END LOOP;

      --------------------------------------------------------------------------
      -- Step 30
      -- Exit if hash is empty of edges
      --------------------------------------------------------------------------
      int_deadman := 0;
      <<new_ring>>
      WHILE boo_check
      LOOP
         IF hash_edges.COUNT = 0
         THEN
            RETURN ary_output;
         END IF;

      --------------------------------------------------------------------------
      -- Step 30
      -- Grab an edge from the bucket
      --------------------------------------------------------------------------
         int_hash_index := hash_edges.FIRST;
         obj_current_edge := hash_edges(int_hash_index);
         hash_edges.DELETE(int_hash_index);

      --------------------------------------------------------------------------
      -- Step 40
      -- Create a new ring object
      --------------------------------------------------------------------------
         ary_output.EXTEND();
         ary_output(int_ring_index) := dz_topo_ring(
             obj_current_edge
            ,int_ring_index
         );

      --------------------------------------------------------------------------
      -- Step 50
      -- Search for a connecting node
      --------------------------------------------------------------------------
         <<new_edge>>
         WHILE boo_check
         LOOP
            ary_candidates := dz_topo_edge_list();
            int_candidates := 1;

      --------------------------------------------------------------------------
      -- Step 60
      -- Loop through the hash looking for node matches
      --------------------------------------------------------------------------
            int_hash_index := hash_edges.FIRST;
            LOOP
               EXIT WHEN NOT hash_edges.EXISTS(int_hash_index);

               ----------------------------------------------------------------------
               -- Step 60.10
               -- Search for a node matches in the bucket
               ----------------------------------------------------------------------
               IF  ary_output(int_ring_index).head_node_id = hash_edges(int_hash_index).start_node_id
               AND ary_output(int_ring_index).ring_interior = hash_edges(int_hash_index).interior_side
               THEN
                  ary_candidates.EXTEND();
                  ary_candidates(int_candidates) := hash_edges(int_hash_index);
                  int_candidates := int_candidates + 1;
                  
               ELSIF ary_output(int_ring_index).head_node_id = hash_edges(int_hash_index).end_node_id
               AND ary_output(int_ring_index).ring_interior = hash_edges(int_hash_index).exterior_side()
               THEN
                  ary_candidates.EXTEND();
                  ary_candidates(int_candidates) := hash_edges(int_hash_index);
                  ary_candidates(int_candidates).flip();
                  int_candidates := int_candidates + 1;
                  
               END IF;

               ----------------------------------------------------------------------
               -- Step 60.20
               -- Increment to the next edge in the hash bucket
               ----------------------------------------------------------------------
               int_hash_index  := hash_edges.NEXT(int_hash_index);

            END LOOP;

      --------------------------------------------------------------------------
      -- Step 70
      -- Decide what to do based on the number of matches, one is most easy
      --------------------------------------------------------------------------
            IF ary_candidates.COUNT = 1
            THEN
               ary_output(int_ring_index).append_edge(ary_candidates(1));
               hash_edges.DELETE(ary_candidates(1).edge_id);
               
            ELSE
               
               -- Reversing the direction seems to be the most efficient way to proceed
               IF ary_output(int_ring_index).try_reversal = 'TRUE'
               THEN
                  ary_output(int_ring_index).flip();
                  ary_output(int_ring_index).try_reversal := 'FALSE';
                  CONTINUE new_edge;
                  
               END IF;

               -- Do not go through the intersection
               -- rather move on to a new ring
               int_ring_index := int_ring_index + 1;
               CONTINUE new_ring;
            
            END IF;
      
      --------------------------------------------------------------------------
      -- Step 80
      -- Check that process is not stuck
      --------------------------------------------------------------------------
            IF int_deadman > p_input_edges.COUNT * 2
            THEN
               RAISE_APPLICATION_ERROR(-20001,'deadman switch');
               
            ELSE
               int_deadman := int_deadman + 1;
               
            END IF;

      --------------------------------------------------------------------------
      -- Step 90
      -- Check if ring is complete
      --------------------------------------------------------------------------
            IF ary_output(int_ring_index).head_node_id = ary_output(int_ring_index).tail_node_id
            THEN
               ary_output(int_ring_index).finalize();
               int_ring_index := int_ring_index + 1;
               CONTINUE new_ring;

            END IF;

      --------------------------------------------------------------------------
      -- Step 100
      -- Go back and try again
      --------------------------------------------------------------------------
         END LOOP;
      
      END LOOP;

   END edges2strings;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION strings2rings(
      p_input_strings   IN  dz_topo_ring_list
   ) RETURN dz_topo_ring_list
   AS
      ary_rings        dz_topo_ring_list;
      TYPE dz_topo_ring_hash IS TABLE OF dz_topo_ring
      INDEX BY PLS_INTEGER;
      hash_strings     dz_topo_ring_hash;
      ary_candidates   dz_topo_ring_list;
      int_candidates   PLS_INTEGER;
      obj_string       dz_topo_ring;
      int_ring_index   PLS_INTEGER;
      int_hash_index   PLS_INTEGER;
      int_guess        PLS_INTEGER;
      boo_check        BOOLEAN := TRUE;
      int_deadman      PLS_INTEGER := 1;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Create a new array of rings that are complete and hash of incompletes
      --------------------------------------------------------------------------
      int_ring_index := 1;
      ary_rings := dz_topo_ring_list();
      FOR i IN 1 .. p_input_strings.COUNT
      LOOP
         IF p_input_strings(i).ring_status = 'C'
         THEN
            ary_rings.EXTEND(1);
            ary_rings(int_ring_index) := p_input_strings(i);
            int_ring_index := int_ring_index + 1;
            
         ELSIF p_input_strings(i).ring_status = 'I'
         THEN
            hash_strings(p_input_strings(i).ring_id) := p_input_strings(i);
            hash_strings(p_input_strings(i).ring_id).try_reversal := 'FALSE';
            hash_strings(p_input_strings(i).ring_id).node_list := MDSYS.SDO_NUMBER_ARRAY();
            hash_strings(p_input_strings(i).ring_id).node_list.EXTEND(2);
            hash_strings(p_input_strings(i).ring_id).node_list(1) := hash_strings(p_input_strings(i).ring_id).head_node_id;
            hash_strings(p_input_strings(i).ring_id).node_list(2) := hash_strings(p_input_strings(i).ring_id).tail_node_id;
            hash_strings(p_input_strings(i).ring_id).edge_list := MDSYS.SDO_NUMBER_ARRAY();
            hash_strings(p_input_strings(i).ring_id).edge_list.EXTEND(1);
            hash_strings(p_input_strings(i).ring_id).edge_list(1) := p_input_strings(i).ring_id;
            
         END IF;
         
      END LOOP;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Exit early if everything is complete
      --------------------------------------------------------------------------
      <<new_string>>
      WHILE boo_check
      LOOP
         IF hash_strings.COUNT = 0
         THEN
            RETURN ary_rings;
            
         END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Grab one string out of the bucket
      --------------------------------------------------------------------------
         int_hash_index := hash_strings.FIRST;
         obj_string := hash_strings(int_hash_index);
         hash_strings.DELETE(int_hash_index);
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Init the candidates bucket
      --------------------------------------------------------------------------
         <<new_try>>
         WHILE boo_check
         LOOP
            ary_candidates := dz_topo_ring_list();
            int_candidates := 1;
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Loop through the hash of strings trying to make connections
      --------------------------------------------------------------------------
            int_hash_index := hash_strings.FIRST;
            LOOP
               EXIT WHEN NOT hash_strings.EXISTS(int_hash_index);
               
               IF obj_string.head_node_id = hash_strings(int_hash_index).tail_node_id
               AND obj_string.ring_interior = hash_strings(int_hash_index).ring_interior
               THEN
                  ary_candidates.EXTEND();
                  ary_candidates(int_candidates) := hash_strings(int_hash_index);
                  int_candidates := int_candidates + 1;
                  
               ELSIF obj_string.head_node_id = hash_strings(int_hash_index).head_node_id
               AND obj_string.ring_interior != hash_strings(int_hash_index).ring_interior
               THEN
                  ary_candidates.EXTEND();
                  hash_strings(int_hash_index).flip();
                  ary_candidates(int_candidates) := hash_strings(int_hash_index);
                  int_candidates := int_candidates + 1;
                  
               END IF;
               
               int_hash_index  := hash_strings.NEXT(int_hash_index);
               
            END LOOP; 
      
      --------------------------------------------------------------------------
      -- Step 50
      -- Decide what to do based on the number of matches, one is most easy
      --------------------------------------------------------------------------
            IF ary_candidates.COUNT = 1
            THEN
               obj_string.append_edge(ary_candidates(1));
               hash_strings.DELETE(ary_candidates(1).ring_id);
               
            ELSE
               
               -- Reversing the direction seems to be the most efficient way to proceed
               IF obj_string.try_reversal = 'TRUE'
               THEN
                  obj_string.flip();
                  obj_string.try_reversal := 'FALSE';
                  CONTINUE new_try;
                  
               END IF;

               int_guess := ROUND(dbms_random.value(1,ary_candidates.COUNT));
               obj_string.append_edge(ary_candidates(int_guess));
               hash_strings.DELETE(ary_candidates(int_guess).ring_id);
            
            END IF;
      
      --------------------------------------------------------------------------
      -- Step 60
      -- Check if ring is complete
      --------------------------------------------------------------------------
            IF obj_string.tail_node_id = obj_string.head_node_id
            THEN
               obj_string.finalize();
               ary_rings.EXTEND(1);
               ary_rings(int_ring_index) := obj_string;
               int_ring_index := int_ring_index + 1;
               CONTINUE new_string;

            END IF;

      --------------------------------------------------------------------------
      -- Step 70
      -- Go back and try again
      --------------------------------------------------------------------------
         END LOOP; --new_try--
         
         int_deadman := int_deadman + 1;
         
         IF int_deadman > 2000000
         THEN
            RAISE_APPLICATION_ERROR(-20001,'deadman');
            
         END IF;
         
      END LOOP; --new string--
   
   END strings2rings;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION finalize_face_rings(
       p_input_rings      IN  dz_topo_ring_list
      ,p_island_edge_list IN  MDSYS.SDO_LIST_TYPE DEFAULT NULL
      ,p_tolerance        IN  NUMBER DEFAULT 0.05
   ) RETURN dz_topo_ring_list
   AS
      ary_output    dz_topo_ring_list := p_input_rings;
      num_tolerance NUMBER := p_tolerance;
      
   BEGIN
      
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_input_rings IS NULL
      OR p_input_rings.COUNT = 0
      THEN
         RETURN NULL;
         
      END IF;
      
      IF num_tolerance IS NULL
      THEN
         num_tolerance := 0.05;
      
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Assign rings using the island_edge_id_list
      --------------------------------------------------------------------------
      FOR i IN 1 .. p_input_rings.COUNT
      LOOP
         IF p_island_edge_list IS NULL
         THEN
            ary_output(i).ring_type := 'E';
            dz_topo_util.verify_ordinate_rotation(
                p_rotation => 'CCW'
               ,p_input    => ary_output(i).shape
               ,p_area     => ary_output(i).ring_size
            );
         
         ELSE
            <<outer>>
            
            FOR j IN 1 .. p_island_edge_list.COUNT
            LOOP
               <<inner>>
               
               FOR k IN 1 .. ary_output(i).edge_list.COUNT
               LOOP
                  IF ABS(p_island_edge_list(j)) = ary_output(i).edge_list(k)
                  THEN
                     ary_output(i).ring_type := 'I';
                     
                     dz_topo_util.verify_ordinate_rotation(
                         p_rotation => 'CW'
                        ,p_input    => ary_output(i).shape
                        ,p_area     => ary_output(i).ring_size
                     );
                     
                     ary_output(i).ring_status := 'C';
                     
                     EXIT outer;
                     
                  END IF; 
               
               END LOOP;
               
            END LOOP;
            
         END IF;
         
      END LOOP;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Return what we got
      --------------------------------------------------------------------------
      RETURN ary_output;
   
   END finalize_face_rings;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION finalize_generic_rings(
       p_input_rings    IN  dz_topo_ring_list
      ,p_tolerance      IN  NUMBER DEFAULT 0.05
   ) RETURN dz_topo_ring_list
   AS
      ary_output    dz_topo_ring_list := p_input_rings;
      num_tolerance NUMBER := p_tolerance;
      str_relate    VARCHAR2(4000 Char);
      
   BEGIN
      
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF num_tolerance IS NULL
      THEN
         num_tolerance := 0.05;
      
      END IF;
      
      IF p_input_rings IS NULL
      OR p_input_rings.COUNT = 0
      THEN
         RETURN NULL;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Make sure the rings have an id and zero the insider count
      --------------------------------------------------------------------------
      FOR i IN 1 .. ary_output.COUNT
      LOOP
         IF ary_output(i).ring_id IS NULL
         THEN
            ary_output(i).ring_id := i;
            
         END IF;
         
         ary_output(i).insider_count := 0;
         
      END LOOP;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Generate insider count
      --------------------------------------------------------------------------
      FOR i IN 1 .. ary_output.COUNT
      LOOP
         FOR j IN 1 .. ary_output.COUNT
         LOOP
            IF ary_output(i).ring_id <> ary_output(j).ring_id
            THEN
               str_relate := MDSYS.SDO_GEOM.RELATE(
                   ary_output(i).shape
                  ,'DETERMINE'
                  ,ary_output(j).shape
                  ,num_tolerance
               );
               
               IF str_relate IN ('INSIDE','COVEREDBY')
               THEN
                  ary_output(i).insider_count := ary_output(i).insider_count + 1;
                  
               END IF;
            
            END IF;
            
         END LOOP;
         
      END LOOP;
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Assign the proper type code
      --------------------------------------------------------------------------
      FOR i IN 1 .. ary_output.COUNT
      LOOP
         IF ary_output(i).insider_count = 0
         OR MOD(ary_output(i).insider_count,2) = 0
         THEN
            ary_output(i).ring_type := 'E';
            dz_topo_util.verify_ordinate_rotation(
                p_rotation => 'CCW'
               ,p_input    => ary_output(i).shape
               ,p_area     => ary_output(i).ring_size
            );
            
         ELSE
            ary_output(i).ring_type := 'I';
            dz_topo_util.verify_ordinate_rotation(
                p_rotation => 'CW'
               ,p_input    => ary_output(i).shape
               ,p_area     => ary_output(i).ring_size
            );
            
         END IF;
         
      END LOOP;
      
      --------------------------------------------------------------------------
      -- Step 50
      -- Return what we got
      --------------------------------------------------------------------------
      RETURN ary_output;
      
   END finalize_generic_rings;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION sort_rings_by_size(
       p_input_rings   IN  dz_topo_ring_list
      ,p_direction     IN VARCHAR2 DEFAULT 'ASC'
   ) RETURN dz_topo_ring_list
   AS
      str_direction VARCHAR2(4000 Char) := UPPER(p_direction);
      obj_temp      dz_topo_ring;
      ary_output    dz_topo_ring_list;
      i             PLS_INTEGER;

   BEGIN

      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF str_direction IS NULL
      THEN
         str_direction := 'ASC';
         
      ELSIF str_direction NOT IN ('ASC','DESC')
      THEN
         RAISE_APPLICATION_ERROR(-20001,'direction parameter may only be ASC or DESC');
         
      END IF;

      --------------------------------------------------------------------------
      -- Step 20
      -- Exit early if input is empty or one item
      --------------------------------------------------------------------------
      ary_output := dz_topo_ring_list();
      IF p_input_rings IS NULL
      OR p_input_rings.COUNT = 0
      THEN
         RETURN ary_output;
         
      ELSIF p_input_rings.COUNT = 1
      THEN
         RETURN p_input_rings;
         
      END IF;

      --------------------------------------------------------------------------
      -- Step 30
      -- Bubble sort the objects on ring_size attribute
      --------------------------------------------------------------------------
      ary_output := p_input_rings;
      i := p_input_rings.COUNT-1;
      
      WHILE ( i > 0 )
      LOOP
         FOR j IN 1 .. i
         LOOP
            IF str_direction = 'DESC'
            THEN
               IF ary_output(j).ring_size < ary_output(j+1).ring_size
               THEN
                  obj_temp        := ary_output(j);
                  ary_output(j)   := ary_output(j+1);
                  ary_output(j+1) := obj_temp;
                  
               END IF;

            ELSE
               IF ary_output(j).ring_size > ary_output(j+1).ring_size
               THEN
                  obj_temp        := ary_output(j);
                  ary_output(j)   := ary_output(j+1);
                  ary_output(j+1) := obj_temp;
                  
               END IF;

            END IF;

         END LOOP;

         i := i - 1;

      END LOOP;

      --------------------------------------------------------------------------
      -- Step 40
      -- Cough back results
      --------------------------------------------------------------------------
      RETURN ary_output;

   END sort_rings_by_size;

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION rings2sdo(
       p_input_rings    IN  dz_topo_ring_list
      ,p_remove_holes   IN  VARCHAR2 DEFAULT 'FALSE'
   ) RETURN MDSYS.SDO_GEOMETRY
   AS
      str_remove_holes VARCHAR2(5 Char) := UPPER(p_remove_holes);
      sdo_output       MDSYS.SDO_GEOMETRY;
      ary_exteriors    dz_topo_ring_list;
      ary_interiors    dz_topo_ring_list;
      int_exter        PLS_INTEGER;
      int_inter        PLS_INTEGER;

   BEGIN

      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF str_remove_holes IS NULL
      THEN
         str_remove_holes := 'FALSE';
         
      ELSIF str_remove_holes NOT IN ('TRUE','FALSE')
      THEN
         RAISE_APPLICATION_ERROR(-20001,'boolean error');
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Exit early if possible
      --------------------------------------------------------------------------
      IF p_input_rings IS NULL
      OR p_input_rings.COUNT = 0
      THEN
         RETURN NULL;
         
      ELSIF p_input_rings.COUNT = 1
      THEN
         RETURN p_input_rings(1).shape;
         
      END IF;

      --------------------------------------------------------------------------
      -- Step 30
      -- Segregate rings into holes and exteriors
      --------------------------------------------------------------------------
      int_exter := 1;
      int_inter := 1;
      ary_exteriors := dz_topo_ring_list();
      ary_interiors := dz_topo_ring_list();
      FOR i IN 1 .. p_input_rings.COUNT
      LOOP
         IF p_input_rings(i).ring_type IS NULL
         OR p_input_rings(i).ring_type = 'E'
         THEN
            ary_exteriors.EXTEND();
            ary_exteriors(int_exter) := p_input_rings(i);
            int_exter := int_exter + 1;
            
         ELSIF p_input_rings(i).ring_type = 'I'
         AND str_remove_holes = 'FALSE'
         THEN
            ary_interiors.EXTEND();
            ary_interiors(int_inter) := p_input_rings(i);
            int_inter := int_inter + 1;
            
         END IF;

      END LOOP;

      --------------------------------------------------------------------------
      -- Step 40
      -- Sort exterior rings by size sets
      --------------------------------------------------------------------------
      ary_exteriors := sort_rings_by_size(
          p_input_rings => ary_exteriors
         ,p_direction   => 'DESC'
      );

      --------------------------------------------------------------------------
      -- Step 50
      -- If exterior rings only, then append together and exit
      -- Note the condition of multi-polygon for a single face is an error
      --------------------------------------------------------------------------
      IF ary_interiors.COUNT = 0
      OR str_remove_holes = 'TRUE'
      THEN
         FOR i IN 1 .. ary_exteriors.COUNT
         LOOP
            IF sdo_output IS NULL
            THEN
               sdo_output := ary_exteriors(i).shape;
               
            ELSE
               sdo_output := MDSYS.SDO_UTIL.APPEND(
                   sdo_output
                  ,ary_exteriors(i).shape
               );
               
            END IF;
            
         END LOOP;

         RETURN sdo_output;

      END IF;
      
      --------------------------------------------------------------------------
      -- Step 60
      -- If only one exterior and some amount of interiors
      --------------------------------------------------------------------------
      IF ary_exteriors.COUNT = 1
      THEN
         sdo_output := ary_exteriors(1).shape;
         
         FOR i IN 1 .. ary_interiors.COUNT
         LOOP
            sdo_output := dz_topo_util.append_hole_to_polygon(
                p_input => sdo_output
               ,p_hole  => ary_interiors(i).shape
            );
            
         END LOOP;
      
         RETURN sdo_output;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 70
      -- More complicated system, 
      --------------------------------------------------------------------------
      FOR i IN 1 .. ary_exteriors.COUNT
      LOOP
         IF ary_exteriors(i).ring_group IS NULL
         THEN
            ary_exteriors(i).ring_group := i;
         
         END IF;
         
      END LOOP;
      
      <<outer>>
      FOR i IN 1 .. ary_interiors.COUNT
      LOOP
         IF ary_interiors(i).ring_group IS NULL
         THEN
            ary_interiors(i).ring_group := 1;
            
         END IF;
         
         <<inner>>
         FOR j IN 1 .. ary_exteriors.COUNT
         LOOP
            IF ary_interiors(i).ring_group = ary_exteriors(j).ring_group
            THEN
               ary_exteriors(j).shape := dz_topo_util.append_hole_to_polygon(
                   p_input => ary_exteriors(j).shape
                  ,p_hole  => ary_interiors(i).shape
               );
               EXIT inner;
               
            END IF;
            
         END LOOP;
         
      END LOOP;
      
      FOR i IN 1 .. ary_exteriors.COUNT
      LOOP
         IF sdo_output IS NULL
         THEN
            sdo_output := ary_exteriors(i).shape;
               
         ELSE
            sdo_output := MDSYS.SDO_UTIL.APPEND(sdo_output,ary_exteriors(i).shape);
               
         END IF;
            
      END LOOP;
      
      RETURN sdo_output;

   END rings2sdo;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE get_faces(
       p_input          IN  MDSYS.SDO_TOPO_GEOMETRY
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
      ,p_output         OUT MDSYS.SDO_NUMBER_ARRAY
   ) 
   AS
      obj_topology       dz_topology := p_topology_obj;
      str_sql            VARCHAR2(4000 Char);
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_input IS NULL
      THEN
         RETURN;
         
      END IF;
      
      IF p_input.tg_type NOT IN (3,4)
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topo geometry must be polygon');
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Get topology name
      --------------------------------------------------------------------------
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topo_geom => p_input
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;

      IF obj_topology.topo_layer().tg_layer_level <> 0
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topo geom must be layer zero');
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Pull the list of faces from the relation table
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || 'a.topo_id '
              || 'FROM '
              || obj_topology.relation_table() || ' a '
              || 'WHERE '
              || '    a.tg_layer_id = :p01 '
              || 'AND a.tg_id = :p02 '
              || 'AND a.topo_type = 3 ';
   
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO p_output
      USING p_input.tg_layer_id,p_input.tg_id;
         
   END get_faces;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION get_faces(
       p_input          IN  MDSYS.SDO_TOPO_GEOMETRY
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
   ) RETURN MDSYS.SDO_NUMBER_ARRAY
   AS
      ary_output    MDSYS.SDO_NUMBER_ARRAY;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_input IS NULL
      THEN
         RETURN NULL;
         
      END IF;
      
      IF p_input.tg_type NOT IN (3,4)
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topo geometry must be polygon');
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Return faces
      --------------------------------------------------------------------------
      get_faces(
          p_input         => p_input
         ,p_topology_obj  => p_topology_obj
         ,p_output        => ary_output
      );
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Return results
      --------------------------------------------------------------------------
      RETURN ary_output;
      
   END get_faces;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION face2sdo(
       p_topology_owner IN  VARCHAR2 DEFAULT NULL
      ,p_topology_name  IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
      ,p_face_id        IN  NUMBER
      ,p_remove_holes   IN  VARCHAR2 DEFAULT 'FALSE'
   ) RETURN MDSYS.SDO_GEOMETRY
   AS
      obj_topology     dz_topology := p_topology_obj;
      str_remove_holes VARCHAR2(4000 Char) := UPPER(p_remove_holes);
      str_sql          VARCHAR2(4000 Char);
      ary_edges        dz_topo_edge_list;
      sdo_output       MDSYS.SDO_GEOMETRY;
      ary_rings        dz_topo_ring_list;
      ary_island_edges MDSYS.SDO_LIST_TYPE;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_face_id IS NULL
      THEN
         RETURN NULL;
         
      END IF;
      
      IF str_remove_holes IS NULL
      THEN
         str_remove_holes := 'FALSE';
      
      ELSIF str_remove_holes NOT IN ('TRUE','FALSE')
      THEN
         RAISE_APPLICATION_ERROR(-20001,'boolean error');
      
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Verify topology
      --------------------------------------------------------------------------
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topology_owner => p_topology_owner
            ,p_topology_name  => p_topology_name
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;
      
      -------------------------------------------------------------------------
      -- Step 30
      -- Pull the island edges
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || 'a.island_edge_id_list '
              || 'FROM '
              || obj_topology.face_table() || ' a '
              || 'WHERE '
              || 'a.face_id = :p01 '; 
              
      EXECUTE IMMEDIATE str_sql
      INTO ary_island_edges
      USING p_face_id;
       
      --------------------------------------------------------------------------
      -- Step 40
      -- Pull the required edges
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || dz_topo_util.c_schema() || '.dz_topo_edge( '
              || '    a.edge_id '
              || '   ,''L'' '
              || '   ,a.start_node_id '
              || '   ,a.end_node_id '
              || '   ,a.geometry '
              || '   ,0 '
              || ') '
              || 'FROM '
              || obj_topology.edge_table() || ' a '
              || 'WHERE '
              || 'a.left_face_id = :p01 AND '
              || 'a.right_face_id <> :p02 '
              || 'UNION ALL SELECT '
              || dz_topo_util.c_schema() || '.dz_topo_edge( '
              || '    a.edge_id '
              || '   ,''R'' '
              || '   ,a.start_node_id '
              || '   ,a.end_node_id '
              || '   ,a.geometry '
              || '   ,0 '
              || ') '
              || 'FROM '
              || obj_topology.edge_table() || ' a '
              || 'WHERE '
              || 'a.left_face_id <> :p03 AND '
              || 'a.right_face_id = :p04 ';
      
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO ary_edges
      USING p_face_id,p_face_id,p_face_id,p_face_id;
      
      --------------------------------------------------------------------------
      -- Step 50
      -- Convert to sdo
      --------------------------------------------------------------------------
      ary_rings := edges2rings(
          p_input_edges  => ary_edges
      );
      
      ary_rings := finalize_face_rings(
          p_input_rings      => ary_rings
         ,p_island_edge_list => ary_island_edges
         ,p_tolerance        => obj_topology.tolerance
      );
      
      sdo_output := rings2sdo(
          p_input_rings  => ary_rings
         ,p_remove_holes => str_remove_holes
      );
      
      --------------------------------------------------------------------------
      -- Step 60
      --Return what we got
      --------------------------------------------------------------------------
      RETURN sdo_output;
 
   END face2sdo;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION get_faces_as_sdo(
       p_input          IN  MDSYS.SDO_TOPO_GEOMETRY
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
   ) RETURN MDSYS.SDO_GEOMETRY
   AS
      obj_topology       dz_topology := p_topology_obj;
      ary_faces          MDSYS.SDO_NUMBER_ARRAY;
      sdo_output         MDSYS.SDO_GEOMETRY;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_input IS NULL
      THEN
         RETURN NULL;
         
      END IF;
      
      IF p_input.tg_type NOT IN (3,4)
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topo geometry must be polygon');
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Check topology object
      --------------------------------------------------------------------------
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topo_geom => p_input
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- get faces
      --------------------------------------------------------------------------
      get_faces(
          p_input         => p_input
         ,p_topology_obj  => obj_topology 
         ,p_output        => ary_faces
      );
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Collect each face as a separate sdo geometry
      --------------------------------------------------------------------------
      FOR i IN 1 .. ary_faces.COUNT
      LOOP
         IF sdo_output IS NULL
         THEN
            sdo_output := face2sdo(
                p_topology_obj   => obj_topology
               ,p_face_id        => ary_faces(i)
            );
            
         ELSE
            sdo_output := MDSYS.SDO_UTIL.APPEND(
                sdo_output
               ,face2sdo(
                   p_topology_obj   => obj_topology
                  ,p_face_id        => ary_faces(i)
               )
            );
            
         END IF;
      
      END LOOP;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Return what we got
      --------------------------------------------------------------------------
      RETURN sdo_output;
      
   END get_faces_as_sdo;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE remove_faces_from_topo_raw(
       p_input          IN  MDSYS.SDO_TOPO_GEOMETRY
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
      ,p_face_ids       IN  MDSYS.SDO_NUMBER_ARRAY
      ,p_returning      OUT MDSYS.SDO_NUMBER_ARRAY
   )
   AS
      obj_topology       dz_topology := p_topology_obj;
      str_sql            VARCHAR2(4000 Char);
      
   BEGIN
   
      p_returning := MDSYS.SDO_NUMBER_ARRAY();
      
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_input IS NULL
      OR p_face_ids IS NULL
      OR p_face_ids.COUNT = 0
      THEN
         RETURN;
      END IF;
      
      IF p_input.tg_type NOT IN (3,4)
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topo geometry must be polygon');
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Verify topology object
      --------------------------------------------------------------------------   
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topo_geom => p_input
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;
      
      IF obj_topology.topo_layer().tg_layer_level <> 0
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topo geom must be layer zero to alter faces');
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Remove the faces from the geometry
      --------------------------------------------------------------------------
      str_sql := 'DELETE FROM '
              || obj_topology.relation_table() || ' a '
              || 'WHERE '
              || '    a.tg_layer_id = :p01 '
              || 'AND a.tg_id = :p02 '
              || 'AND a.topo_type = 3 '
              || 'AND a.topo_id IN (SELECT column_value FROM TABLE(:p03)) '
              || 'RETURNING a.topo_id INTO :p04 ';
   
      EXECUTE IMMEDIATE str_sql
      USING p_input.tg_layer_id,p_input.tg_id,p_face_ids
      RETURNING BULK COLLECT INTO p_returning;
      
      COMMIT;
      
   END remove_faces_from_topo_raw;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE add_faces_to_topo_raw(
       p_input          IN  MDSYS.SDO_TOPO_GEOMETRY
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
      ,p_face_ids       IN  MDSYS.SDO_NUMBER_ARRAY
      ,p_returning      OUT MDSYS.SDO_NUMBER_ARRAY
   )
   AS
      obj_topology       dz_topology := p_topology_obj;
      str_sql            VARCHAR2(4000 Char);
      ary_valid_faces    MDSYS.SDO_NUMBER_ARRAY;
      
   BEGIN
   
      p_returning := MDSYS.SDO_NUMBER_ARRAY();
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_input IS NULL
      OR p_face_ids IS NULL
      OR p_face_ids.COUNT = 0
      THEN
         RETURN;
         
      END IF;
      
      IF p_input.tg_type NOT IN (3,4)
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topo geometry must be polygon');
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Verify topology object
      --------------------------------------------------------------------------   
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topo_geom => p_input
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;
      
      IF obj_topology.topo_layer().tg_layer_level <> 0
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topo geom must be layer zero to alter faces');
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Filter face list to existing faces that are not already part of topo
      -------------------------------------------------------------------------- 
      str_sql := 'SELECT '
              || 'a.face_id '
              || 'FROM '
              || obj_topology.face_table() || ' a '
              || 'WHERE '
              || '    a.face_id IN (SELECT column_value FROM TABLE(:p01)) '
              || 'AND a.face_id NOT IN ( '
              || '   SELECT '
              || '   bb.topo_id '
              || '   FROM '
              || '   ' || obj_topology.relation_table() || ' bb '
              || '   WHERE '
              || '       bb.tg_id = :p01 '
              || '   AND bb.tg_layer_id = :p02 '
              || '   AND bb.topo_type = 3 '
              || ') ';
 
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO ary_valid_faces
      USING 
       p_face_ids
      ,p_input.tg_id
      ,p_input.tg_layer_id;
      
      IF ary_valid_faces IS NULL
      OR ary_valid_faces.COUNT = 0
      THEN
         RETURN;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Add faces to relation tables for topo
      --------------------------------------------------------------------------
      str_sql := 'INSERT INTO ' || obj_topology.relation_table() || ' ('
              || '    tg_layer_id '
              || '   ,tg_id '
              || '   ,topo_id '
              || '   ,topo_type '
              || '   ,topo_attribute '
              || ') '
              || 'SELECT '
              || ' :p01 '
              || ',:p02 '
              || ',a.column_value '
              || ',3 '
              || ',NULL '
              || 'FROM '
              || 'TABLE(:p03) a ';
   
      EXECUTE IMMEDIATE str_sql
      USING 
       p_input.tg_layer_id
      ,p_input.tg_id
      ,ary_valid_faces;
      
      COMMIT;
      
      p_returning := ary_valid_faces;
      
   END add_faces_to_topo_raw;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION get_interior_edges(
       p_input          IN  MDSYS.SDO_TOPO_GEOMETRY
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
   ) RETURN MDSYS.SDO_NUMBER_ARRAY
   AS
      obj_topology       dz_topology := p_topology_obj;
      ary_faces          MDSYS.SDO_NUMBER_ARRAY;
      ary_unneeded_edges MDSYS.SDO_NUMBER_ARRAY;
      ary_topo_edges     dz_topo_edge_list;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_input IS NULL
      THEN
         RETURN NULL;
         
      END IF;
      
      IF p_input.tg_type NOT IN (3,4)
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topo geometry must be polygon');
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Verify topology object
      --------------------------------------------------------------------------
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topo_geom => p_input
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Get faces for the topo feature
      --------------------------------------------------------------------------   
      get_faces(
          p_input         => p_input
         ,p_topology_obj  => obj_topology
         ,p_output        => ary_faces
      );
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Get interior edges
      --------------------------------------------------------------------------
      faces2edges(
          p_topology_obj    => obj_topology
         ,p_faces           => ary_faces
         ,p_subset          => 'INTERIOR'
         ,p_output          => ary_topo_edges
      );
      
      --------------------------------------------------------------------------
      -- Step 50
      -- Get edge ids
      --------------------------------------------------------------------------
      ary_unneeded_edges := MDSYS.SDO_NUMBER_ARRAY();
      ary_unneeded_edges.EXTEND(ary_topo_edges.COUNT);
      FOR i IN 1 .. ary_topo_edges.COUNT
      LOOP
         ary_unneeded_edges(i) := ary_topo_edges(i).edge_id;
         
      END LOOP;
      
      RETURN ary_unneeded_edges;
      
   END get_interior_edges;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION get_interior_edges_as_sdo(
       p_input          IN  MDSYS.SDO_TOPO_GEOMETRY
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
   ) RETURN MDSYS.SDO_GEOMETRY
   AS
      obj_topology       dz_topology := p_topology_obj;
      ary_faces          MDSYS.SDO_NUMBER_ARRAY;
      sdo_output         MDSYS.SDO_GEOMETRY;
      ary_topo_edges     dz_topo_edge_list;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_input IS NULL
      THEN
         RETURN NULL;
         
      END IF;
      
      IF p_input.tg_type NOT IN (3,4)
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topo geometry must be polygon');
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Verify topology object
      --------------------------------------------------------------------------
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topo_geom => p_input
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Get faces for the topo feature
      --------------------------------------------------------------------------   
      get_faces(
          p_input         => p_input
         ,p_topology_obj  => obj_topology
         ,p_output        => ary_faces
      );
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Get interior edges
      --------------------------------------------------------------------------
      faces2edges(
          p_topology_obj    => obj_topology
         ,p_faces           => ary_faces
         ,p_subset          => 'INTERIOR'
         ,p_output          => ary_topo_edges
      );
      
      --------------------------------------------------------------------------
      -- Step 50
      -- Get edge ids
      --------------------------------------------------------------------------
      FOR i IN 1 .. ary_topo_edges.COUNT
      LOOP
         IF sdo_output IS NULL
         THEN
            sdo_output := ary_topo_edges(i).shape;
            
         ELSE
            sdo_output := MDSYS.SDO_UTIL.APPEND(
                sdo_output
               ,ary_topo_edges(i).shape
            );
            
         END IF;
         
      END LOOP;
      
      RETURN sdo_output;
      
   END get_interior_edges_as_sdo;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION get_shared_faces(
       p_input          IN  MDSYS.SDO_TOPO_GEOMETRY
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
   ) RETURN MDSYS.SDO_NUMBER_ARRAY
   -- ( tg_layer_id, tg_id, face_id, areasqkm )
   AS
      obj_topology       dz_topology := p_topology_obj;
      str_sql            VARCHAR2(4000 Char);
      ary_faces          MDSYS.SDO_NUMBER_ARRAY;
      ary_shared_tglyrs  MDSYS.SDO_NUMBER_ARRAY;
      ary_shared_tgids   MDSYS.SDO_NUMBER_ARRAY;
      ary_shared_faces   MDSYS.SDO_NUMBER_ARRAY;
      ary_output         MDSYS.SDO_NUMBER_ARRAY;
      int_counter        PLS_INTEGER;
      
   BEGIN
   
      ary_shared_faces := MDSYS.SDO_NUMBER_ARRAY();
      
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_input IS NULL
      THEN
         RETURN NULL;
         
      END IF;
      
      IF p_input.tg_type NOT IN (3,4)
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topo geometry must be polygon');
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Verify topology object
      --------------------------------------------------------------------------
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topo_geom => p_input
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Get faces for the topo feature
      --------------------------------------------------------------------------   
      get_faces(
          p_input         => p_input
         ,p_topology_obj  => obj_topology
         ,p_output        => ary_faces
      );
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Check which faces have more than the parameter feature assigned to them
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || ' a.tg_layer_id '
              || ',a.tg_id '
              || ',a.topo_id '
              || 'FROM '
              || obj_topology.relation_table() || ' a '
              || 'WHERE '
              || '   a.topo_id IN (SELECT column_value FROM TABLE(:p01)) '
              || 'AND a.tg_id <> :p02 '
              || 'AND a.tg_layer_id = :p03 '
              || 'AND a.topo_type = 3 ';
              
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO 
       ary_shared_tglyrs
      ,ary_shared_tgids
      ,ary_shared_faces
      USING 
       ary_faces
      ,p_input.tg_id
      ,p_input.tg_layer_id;
      
      IF ary_shared_tgids IS NULL
      OR ary_shared_tgids.COUNT = 0
      THEN
         RETURN NULL;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Weave into output array
      --------------------------------------------------------------------------
      ary_output := MDSYS.SDO_NUMBER_ARRAY();
      ary_output.EXTEND(ary_shared_tgids.COUNT * 3);
      int_counter := 1;
      FOR i IN 1 .. ary_shared_tgids.COUNT
      LOOP
         ary_output(int_counter) := ary_shared_tglyrs(i);
         int_counter := int_counter + 1;
         
         ary_output(int_counter) := ary_shared_tgids(i);
         int_counter := int_counter + 1;
         
         ary_output(int_counter) := ary_shared_faces(i);
         int_counter := int_counter + 1;
         
         ary_output(int_counter) := MDSYS.SDO_GEOM.SDO_AREA(
             face2sdo(
                 p_topology_obj   => obj_topology
                ,p_face_id        => ary_shared_faces(i)
             )
            ,0.05
            ,'UNIT=SQ_KM'
         );
         int_counter := int_counter + 1;
         
      END LOOP;
      
      RETURN ary_output;
   
   END get_shared_faces;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION get_face_topo_neighbors(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL  
      ,p_topology_name   IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj    IN  dz_topology DEFAULT NULL
      ,p_tg_layer_id     IN  NUMBER 
      ,p_face_id         IN  NUMBER
   ) RETURN dz_topo_vry
   AS
      obj_topology    dz_topology := p_topology_obj;
      str_sql         VARCHAR2(4000 Char);
      ary_output      dz_topo_vry;
      
   BEGIN
      
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_face_id IS NULL
      THEN
         RETURN NULL;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Verify topology object
      --------------------------------------------------------------------------
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topology_owner => p_topology_owner
            ,p_topology_name  => p_topology_name
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;
      
      obj_topology.set_active_topo_layer(
         p_tg_layer_id => p_tg_layer_id
      );
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Get SUM of exterior edges by face id
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || 'a.' || obj_topology.active_topo_column() || ' '
              || 'FROM '
              || obj_topology.active_topo_table() || ' a '
              || 'WHERE '
              || 'a.' || obj_topology.active_topo_column() || '.tg_id IN ('
              || '   SELECT '
              || '    aa.tg_id '
              || '   FROM '
              || '   ' || obj_topology.relation_table() || ' aa '
              || '   WHERE '
              || '   aa.topo_id IN ('
              || '      SELECT '
              || '      aaa.left_face_id AS face_id '
              || '      FROM '
              || '      ' || obj_topology.edge_table() || ' aaa '
              || '      WHERE '
              || '          aaa.right_face_id = :p01 '
              || '      AND aaa.left_face_id <> :p02 '
              || '      UNION ALL '
              || '      SELECT '
              || '      bbb.right_face_id AS face_id '
              || '      FROM '
              || '      ' || obj_topology.edge_table() || ' bbb '
              || '      WHERE '
              || '          bbb.right_face_id <> :p03 '
              || '      AND bbb.left_face_id = :p04 '
              || '   ) '
              || ') ';

      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO ary_output
      USING 
       p_face_id
      ,p_face_id
      ,p_face_id
      ,p_face_id;
      
      RETURN ary_output;
             
   END get_face_topo_neighbors;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION face_at_point(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL  
      ,p_topology_name   IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj    IN  dz_topology DEFAULT NULL
      ,p_input           IN  MDSYS.SDO_GEOMETRY
   ) RETURN NUMBER
   AS
      sdo_input       MDSYS.SDO_GEOMETRY := p_input;
      obj_topology    dz_topology := p_topology_obj;
      ary_results     MDSYS.SDO_TOPO_OBJECT_ARRAY;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF sdo_input IS NULL
      THEN
         RETURN NULL;
         
      END IF;
      
      IF sdo_input.get_gtype() <> 1
      THEN
         IF sdo_input.get_gtype() IN (3,7)
         THEN
            sdo_input := MDSYS.SDO_GEOM.SDO_CENTROID(
                sdo_input
               ,0.05
            );
            
         ELSE
            sdo_input := MDSYS.SDO_GEOM.SDO_CENTROID(
                MDSYS.SDO_GEOM.SDO_MBR(sdo_input)
               ,0.05
            );
         
         END IF;
         
      END IF;

      --------------------------------------------------------------------------
      -- Step 20
      -- Determine topology location
      --------------------------------------------------------------------------
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topology_owner => p_topology_owner
            ,p_topology_name  => p_topology_name
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Query the topology
      --------------------------------------------------------------------------
      ary_results := MDSYS.SDO_TOPO.GET_TOPO_OBJECTS(
          topology => obj_topology.topology_name
         ,geometry => sdo_input
      );
      
      IF ary_results IS NULL
      OR ary_results.COUNT = 0
      THEN
         RETURN NULL;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Extract the face, take the first face in case of border cases
      --------------------------------------------------------------------------
      FOR i IN 1 .. ary_results.COUNT
      LOOP
         IF ary_results(i).topo_type = 3
         THEN
            RETURN ary_results(i).topo_id;
      
         END IF;
      
      END LOOP;
      
      --------------------------------------------------------------------------
      -- Step 50
      -- No faces found so return nothing
      --------------------------------------------------------------------------
      RETURN NULL;
      
   END face_at_point;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION face_at_point_sdo(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL  
      ,p_topology_name   IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj    IN  dz_topology DEFAULT NULL
      ,p_input           IN  MDSYS.SDO_GEOMETRY
   ) RETURN MDSYS.SDO_GEOMETRY
   AS
      num_face_id   NUMBER;
      obj_topology  dz_topology := p_topology_obj;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_input IS NULL
      THEN
         RETURN NULL;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Determine topology location
      --------------------------------------------------------------------------
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topology_owner => p_topology_owner
            ,p_topology_name  => p_topology_name
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Get the face id at point, not sure what to do if we get back the uf
      --------------------------------------------------------------------------
      num_face_id := face_at_point(
          p_topology_obj   => obj_topology
         ,p_input          => p_input
      );
      
      IF num_face_id IS NULL
      OR num_face_id = -1
      THEN
         RETURN NULL;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Convert face into sdo
      --------------------------------------------------------------------------
      RETURN face2sdo(
          p_topology_obj => obj_topology
         ,p_face_id      => num_face_id
      );
      
   END face_at_point_sdo;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE drop_face_clean(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL  
      ,p_topology_name   IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj    IN  dz_topology DEFAULT NULL
      ,p_face_id         IN  NUMBER
   )
   AS
      str_sql      VARCHAR2(4000 Char);
      obj_topology dz_topology := p_topology_obj;
      ary_edges    MDSYS.SDO_NUMBER_ARRAY;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_face_id IS NULL
      THEN
         RETURN;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Determine topology location
      --------------------------------------------------------------------------
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topology_owner => p_topology_owner
            ,p_topology_name  => p_topology_name
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;
   
      str_sql := 'SELECT '
              || 'a.edge_id '
              || 'FROM '
              || obj_topology.edge_table() || ' a '
              || 'WHERE '
              || '   a.right_face_id = :p03 '
              || 'OR a.left_face_id  = :p04 ';
              
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO ary_edges
      USING p_face_id,p_face_id;
      
      FOR i IN 1 .. ary_edges.COUNT
      LOOP
         MDSYS.SDO_TOPO_MAP.REMOVE_EDGE(
            topology => obj_topology.topology_name
           ,edge_id  => ary_edges(i)
         );
      
      END LOOP;
      
      COMMIT;
      
   END drop_face_clean;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE nuke_topology(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL  
      ,p_topology_name   IN  VARCHAR2
   )
   AS
      obj_topology dz_topology;
      str_owner    VARCHAR2(30 Char) := p_topology_owner;
      
   BEGIN
    
      IF str_owner IS NULL
      THEN
         str_owner := USER;
         
      END IF;
   
      obj_topology := dz_topology(
          p_topology_owner => str_owner
         ,p_topology_name  => p_topology_name
      );
      
      IF obj_topology IS NOT NULL
      AND obj_topology.topo_layers IS NOT NULL
      AND obj_topology.topo_layers.COUNT > 0
      THEN
         FOR i IN 1.. obj_topology.topo_layers.COUNT
         LOOP
            MDSYS.SDO_TOPO.DELETE_TOPO_GEOMETRY_LAYER(
                topology    => obj_topology.topology_name
               ,table_name  => obj_topology.topo_layers(i).table_name
               ,column_name => obj_topology.topo_layers(i).column_name
            );
            
            BEGIN
               EXECUTE IMMEDIATE 
               'DROP TABLE ' || obj_topology.topo_layers(i).table_owner || '.' || obj_topology.topo_layers(i).table_name;
            
            EXCEPTION
               WHEN OTHERS THEN NULL;
               
            END;
                                      
         END LOOP;
      
      END IF;
      
      BEGIN
         EXECUTE IMMEDIATE 
         'DROP TABLE ' || obj_topology.topology_owner || '.' || obj_topology.exp_table();
      
      EXCEPTION
         WHEN OTHERS THEN NULL;
         
      END;
   
      MDSYS.SDO_TOPO.DROP_TOPOLOGY(
         topology    => obj_topology.topology_name
      );
   
   END nuke_topology;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE remove_face_island_edge_id(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL  
      ,p_topology_name   IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj    IN  dz_topology DEFAULT NULL
      ,p_face_id         IN  NUMBER
      ,p_edge_id         IN  NUMBER
   )
   AS
      obj_topology  dz_topology := p_topology_obj;
      ary_edge_list MDSYS.SDO_LIST_TYPE;
      str_sql       VARCHAR2(4000 Char);
      num_check     PLS_INTEGER;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_face_id IS NULL
      OR p_edge_id IS NULL
      THEN
         RETURN;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Determine topology location
      --------------------------------------------------------------------------
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topology_owner => p_topology_owner
            ,p_topology_name  => p_topology_name
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Verify that face exists
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || 'COUNT(*) '
              || 'FROM '
              || obj_topology.face_table() || ' a '
              || 'WHERE '
              || 'a.face_id = :p01 ';
              
      EXECUTE IMMEDIATE str_sql
      INTO num_check
      USING p_face_id;
     
      IF num_check <> 1
      THEN
         RAISE_APPLICATION_ERROR(
             -20001
            ,'face ' || p_face_id || ' does not exist in topology.'
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Collect the list of edge ids excluding the indicated one
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || 'b.column_value '
              || 'FROM '
              || obj_topology.face_table() || ' a '
              || 'CROSS JOIN '
              || 'TABLE(a.island_edge_id_list) b '
              || 'WHERE '
              || '    a.face_id = :p01 '
              || 'AND ABS(b.column_value) <> :p02 ';
              
     EXECUTE IMMEDIATE str_sql
     BULK COLLECT INTO ary_edge_list
     USING p_face_id, ABS(p_edge_id);
     
     --------------------------------------------------------------------------
      -- Step 50
      -- Update the face table with the results
      --------------------------------------------------------------------------
      str_sql := 'UPDATE ' || obj_topology.face_table() || ' a '
              || 'SET '
              || 'a.island_edge_id_list = :p01 '
              || 'WHERE '
              || 'a.face_id = :p02 ';
              
      EXECUTE IMMEDIATE str_sql
      USING ary_edge_list,p_face_id;
      
      COMMIT;
         
   END remove_face_island_edge_id;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE remove_face_island_edge_id(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL  
      ,p_topology_name   IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj    IN  dz_topology DEFAULT NULL
      ,p_face_id         IN  NUMBER
      ,p_edge_id         IN  MDSYS.SDO_NUMBER_ARRAY
   )
   AS
      obj_topology  dz_topology := p_topology_obj;
      ary_edge_list MDSYS.SDO_LIST_TYPE;
      str_sql       VARCHAR2(4000 Char);
      num_check     PLS_INTEGER;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_face_id IS NULL
      OR p_edge_id IS NULL
      THEN
         RETURN;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Determine topology location
      --------------------------------------------------------------------------
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topology_owner => p_topology_owner
            ,p_topology_name  => p_topology_name
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Verify that face exists
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || 'COUNT(*) '
              || 'FROM '
              || obj_topology.face_table() || ' a '
              || 'WHERE '
              || 'a.face_id = :p01 ';
              
      EXECUTE IMMEDIATE str_sql
      INTO num_check
      USING p_face_id;
     
      IF num_check <> 1
      THEN
         RAISE_APPLICATION_ERROR(
             -20001
            ,'face ' || p_face_id || ' does not exist in topology.'
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Collect the list of edge ids excluding the indicated one
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || 'b.column_value '
              || 'FROM '
              || obj_topology.face_table() || ' a '
              || 'CROSS JOIN '
              || 'TABLE(a.island_edge_id_list) b '
              || 'WHERE '
              || '    a.face_id = :p01 '
              || 'AND ABS(b.column_value) NOT IN (SELECT ABS(column_value) FROM TABLE(:p02)) ';
              
     EXECUTE IMMEDIATE str_sql
     BULK COLLECT INTO ary_edge_list
     USING p_face_id, p_edge_id;
     
     --------------------------------------------------------------------------
      -- Step 50
      -- Update the face table with the results
      --------------------------------------------------------------------------
      str_sql := 'UPDATE ' || obj_topology.face_table() || ' a '
              || 'SET '
              || 'a.island_edge_id_list = :p01 '
              || 'WHERE '
              || 'a.face_id = :p02 ';
              
      EXECUTE IMMEDIATE str_sql
      USING ary_edge_list,p_face_id;
      
      COMMIT;
         
   END remove_face_island_edge_id;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE append_face_island_edge_id(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL  
      ,p_topology_name   IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj    IN  dz_topology DEFAULT NULL
      ,p_face_id         IN  NUMBER
      ,p_edge_id         IN  NUMBER
   )
   AS
      obj_topology  dz_topology := p_topology_obj;
      ary_edge_list MDSYS.SDO_LIST_TYPE;
      str_sql       VARCHAR2(4000 Char);
      num_check     PLS_INTEGER;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_face_id IS NULL
      OR p_edge_id IS NULL
      THEN
         RETURN;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Determine topology location
      --------------------------------------------------------------------------
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topology_owner => p_topology_owner
            ,p_topology_name  => p_topology_name
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Verify that face exists
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || 'COUNT(*) '
              || 'FROM '
              || obj_topology.face_table() || ' a '
              || 'WHERE '
              || 'a.face_id = :p01 ';
              
      EXECUTE IMMEDIATE str_sql
      INTO num_check
      USING p_face_id;
     
      IF num_check <> 1
      THEN
         RAISE_APPLICATION_ERROR(
             -20001
            ,'face ' || p_face_id || ' does not exist in topology.'
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Collect the list of edge ids excluding the indicated one if it already
      -- exists
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || 'b.column_value '
              || 'FROM '
              || obj_topology.face_table() || ' a '
              || 'CROSS JOIN '
              || 'TABLE(a.island_edge_id_list) b '
              || 'WHERE '
              || '    a.face_id = :p01 '
              || 'AND ABS(b.column_value) <> :p02 ';
              
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO ary_edge_list
      USING p_face_id, ABS(p_edge_id);
      
      --------------------------------------------------------------------------
      -- Step 50
      -- Append the edge id
      --------------------------------------------------------------------------
      IF ary_edge_list IS NULL
      THEN
         ary_edge_list := MDSYS.SDO_LIST_TYPE();
         ary_edge_list.EXTEND();
         ary_edge_list(1) := p_edge_id;
         
      ELSE
         ary_edge_list.EXTEND();
         ary_edge_list(ary_edge_list.COUNT) := p_edge_id;
      
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 50
      -- Update the face table with the results
      --------------------------------------------------------------------------
      str_sql := 'UPDATE ' || obj_topology.face_table() || ' a '
              || 'SET '
              || 'a.island_edge_id_list = :p01 '
              || 'WHERE '
              || 'a.face_id = :p02 ';
              
      EXECUTE IMMEDIATE str_sql
      USING ary_edge_list,p_face_id;
      
      COMMIT;
         
   END append_face_island_edge_id;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE remove_face_island_node_id(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL  
      ,p_topology_name   IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj    IN  dz_topology DEFAULT NULL
      ,p_face_id         IN  NUMBER
      ,p_node_id         IN  NUMBER
   )
   AS
      obj_topology  dz_topology := p_topology_obj;
      ary_node_list MDSYS.SDO_LIST_TYPE;
      str_sql       VARCHAR2(4000 Char);
      num_check     PLS_INTEGER;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_face_id IS NULL
      OR p_node_id IS NULL
      THEN
         RETURN;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Determine topology location
      --------------------------------------------------------------------------
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topology_owner => p_topology_owner
            ,p_topology_name  => p_topology_name
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Verify that face exists
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || 'COUNT(*) '
              || 'FROM '
              || obj_topology.face_table() || ' a '
              || 'WHERE '
              || 'a.face_id = :p01 ';
              
      EXECUTE IMMEDIATE str_sql
      INTO num_check
      USING p_face_id;
     
      IF num_check <> 1
      THEN
         RAISE_APPLICATION_ERROR(
             -20001
            ,'face ' || p_face_id || ' does not exist in topology.'
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Collect the list of edge ids excluding the indicated one
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || 'b.column_value '
              || 'FROM '
              || obj_topology.face_table() || ' a '
              || 'CROSS JOIN '
              || 'TABLE(a.island_node_id_list) b '
              || 'WHERE '
              || '    a.face_id = :p01 '
              || 'AND b.column_value <> :p02 ';
              
     EXECUTE IMMEDIATE str_sql
     BULK COLLECT INTO ary_node_list
     USING p_face_id, p_node_id;
     
     --------------------------------------------------------------------------
      -- Step 50
      -- Update the face table with the results
      --------------------------------------------------------------------------
      str_sql := 'UPDATE ' || obj_topology.face_table() || ' a '
              || 'SET '
              || 'a.island_node_id_list = :p01 '
              || 'WHERE '
              || 'a.face_id = :p02 ';
              
      EXECUTE IMMEDIATE str_sql
      USING ary_node_list,p_face_id;
      
      COMMIT;
         
   END remove_face_island_node_id;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE append_face_island_node_id(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL  
      ,p_topology_name   IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj    IN  dz_topology DEFAULT NULL
      ,p_face_id         IN  NUMBER
      ,p_node_id         IN  NUMBER
   )
   AS
      obj_topology  dz_topology := p_topology_obj;
      ary_node_list MDSYS.SDO_LIST_TYPE;
      str_sql       VARCHAR2(4000 Char);
      num_check     PLS_INTEGER;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_face_id IS NULL
      OR p_node_id IS NULL
      THEN
         RETURN;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Determine topology location
      --------------------------------------------------------------------------
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topology_owner => p_topology_owner
            ,p_topology_name  => p_topology_name
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Verify that face exists
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || 'COUNT(*) '
              || 'FROM '
              || obj_topology.face_table() || ' a '
              || 'WHERE '
              || 'a.face_id = :p01 ';
              
      EXECUTE IMMEDIATE str_sql
      INTO num_check
      USING p_face_id;
     
      IF num_check <> 1
      THEN
         RAISE_APPLICATION_ERROR(
             -20001
            ,'face ' || p_face_id || ' does not exist in topology.'
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Collect the list of edge ids excluding the indicated one
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || 'b.column_value '
              || 'FROM '
              || obj_topology.face_table() || ' a '
              || 'CROSS JOIN '
              || 'TABLE(a.island_node_id_list) b '
              || 'WHERE '
              || '    a.face_id = :p01 '
              || 'AND b.column_value <> :p02 ';
              
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO ary_node_list
      USING p_face_id, p_node_id;
      
      --------------------------------------------------------------------------
      -- Step 50
      -- Append the new id
      --------------------------------------------------------------------------
      IF ary_node_list IS NULL
      THEN
         ary_node_list := MDSYS.SDO_LIST_TYPE();
         ary_node_list.EXTEND();
         ary_node_list(1) := p_node_id;
      
      ELSE
         ary_node_list.EXTEND();
         ary_node_list(ary_node_list.COUNT) := p_node_id;
      
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 50
      -- Update the face table with the results
      --------------------------------------------------------------------------
      str_sql := 'UPDATE ' || obj_topology.face_table() || ' a '
              || 'SET '
              || 'a.island_node_id_list = :p01 '
              || 'WHERE '
              || 'a.face_id = :p02 ';
              
      EXECUTE IMMEDIATE str_sql
      USING ary_node_list,p_face_id;
      
      COMMIT;
         
   END append_face_island_node_id;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
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
   )
   AS
      obj_topology    dz_topology := p_topology_obj;
      ary_edges_temp  MDSYS.SDO_NUMBER_ARRAY;
      ary_faces_temp  MDSYS.SDO_NUMBER_ARRAY;
      str_sql         VARCHAR2(4000 Char);
      num_check       PLS_INTEGER;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      p_tg_ids   := MDSYS.SDO_NUMBER_ARRAY();
      p_face_ids := MDSYS.SDO_NUMBER_ARRAY();
      p_edge_ids := MDSYS.SDO_NUMBER_ARRAY();
      p_node_ids := MDSYS.SDO_NUMBER_ARRAY();
      
      IF p_face_id IS NULL
      THEN
         RETURN;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Determine topology location
      --------------------------------------------------------------------------
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topology_owner => p_topology_owner
            ,p_topology_name  => p_topology_name
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;
      
      IF p_tg_layer_id IS NOT NULL
      THEN
         obj_topology.set_active_topo_layer(
            p_tg_layer_id => p_tg_layer_id
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Verify that face exists
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || 'COUNT(*) '
              || 'FROM '
              || obj_topology.face_table() || ' a '
              || 'WHERE '
              || 'a.face_id = :p01 ';
              
      EXECUTE IMMEDIATE str_sql
      INTO num_check
      USING p_face_id;
     
      IF num_check <> 1
      THEN
         RAISE_APPLICATION_ERROR(
             -20001
            ,'face ' || p_face_id || ' does not exist in topology.'
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Collect the list of edge ids and face_ids bordering face on the right
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || ' a.edge_id '
              || ',a.right_face_id '
              || 'FROM '
              || obj_topology.edge_table() || ' a '
              || 'WHERE '
              || '    a.left_face_id  =  :p01 '
              || 'AND a.right_face_id <> :p02 ';
              
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO 
       ary_edges_temp
      ,ary_faces_temp
      USING p_face_id, p_face_id;
     
      IF ary_edges_temp IS NOT NULL
      OR ary_edges_temp.COUNT > 0
      THEN
         dz_topo_util.append2(
             p_input_array => p_edge_ids
            ,p_input_value => ary_edges_temp
            ,p_unique      => 'TRUE'
         );
         
         dz_topo_util.append2(
             p_input_array => p_face_ids
            ,p_input_value => ary_faces_temp
            ,p_unique      => 'TRUE'
         );
         
      END IF;
     
      --------------------------------------------------------------------------
      -- Step 40
      -- Collect the list of edge ids and face_ids bordering face on the left
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || ' a.edge_id '
              || ',a.left_face_id '
              || 'FROM '
              || obj_topology.edge_table() || ' a '
              || 'WHERE '
              || '    a.left_face_id  <> :p01 '
              || 'AND a.right_face_id =  :p02 ';
              
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO 
       ary_edges_temp
      ,ary_faces_temp
      USING p_face_id, p_face_id;
     
      IF ary_edges_temp IS NOT NULL
      OR ary_edges_temp.COUNT > 0
      THEN
         dz_topo_util.append2(
             p_input_array => p_edge_ids
            ,p_input_value => ary_edges_temp
            ,p_unique      => 'TRUE'
         );
         
         dz_topo_util.append2(
             p_input_array => p_face_ids
            ,p_input_value => ary_faces_temp
            ,p_unique      => 'TRUE'
         );
         
      END IF;
     
      --------------------------------------------------------------------------
      -- Step 50
      -- Collect the list of edge ids and face_ids bordering face on the left
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || 'a.edge_id '
              || 'FROM '
              || obj_topology.edge_table() || ' a '
              || 'WHERE '
              || '    a.left_face_id  IN (SELECT * FROM TABLE(:p01)) '
              || 'AND a.right_face_id IN (SELECT * FROM TABLE(:p02)) ';
      
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO 
      ary_edges_temp
      USING p_face_ids, p_face_ids;
      
      IF ary_edges_temp IS NOT NULL
      OR ary_edges_temp.COUNT > 0
      THEN
         dz_topo_util.append2(
             p_input_array => p_edge_ids
            ,p_input_value => ary_edges_temp
            ,p_unique      => 'TRUE'
         );
         
      END IF;
       
      --------------------------------------------------------------------------
      -- Step 60
      -- Collect the list of tg ids that match any of the faces
      --------------------------------------------------------------------------
      IF p_tg_layer_id IS NOT NULL 
      THEN
         str_sql := 'SELECT '
                 || 'DISTINCT a.tg_id '
                 || 'FROM '
                 || obj_topology.relation_table() || ' a '
                 || 'WHERE '
                 || '    a.topo_type = 3 '
                 || 'AND a.tg_layer_id = :p01 '
                 || 'AND ( '
                 || '   a.topo_id IN (SELECT * FROM TABLE(:p02)) OR a.topo_id = :p03 '
                 || ') ';
         
         EXECUTE IMMEDIATE str_sql
         BULK COLLECT INTO 
         p_tg_ids
         USING 
          p_tg_layer_id
         ,p_face_ids
         ,p_face_id;
      
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 70
      -- Create the 
      --------------------------------------------------------------------------
      ary_faces_temp := p_face_ids;
      
      dz_topo_util.append2(
          p_input_array => ary_faces_temp
         ,p_input_value => p_face_id
         ,p_unique      => 'TRUE'
      );
      str_sql := 'SELECT '
              || 'SDO_AGGR_MBR(a.mbr_geometry) '
              || 'FROM '
              || obj_topology.face_table() || ' a '
              || 'WHERE '
              || 'a.face_id IN (SELECT * FROM TABLE(:p01)) ';
              
      EXECUTE IMMEDIATE str_sql
      INTO p_window_sdo
      USING ary_faces_temp;
      
  END unravel_face;

END dz_topo_main;
/

