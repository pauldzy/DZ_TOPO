CREATE OR REPLACE PACKAGE BODY dz_topo_reports
AS

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION in_list(
       p_list_ary       IN  MDSYS.SDO_LIST_TYPE
      ,p_input          IN  NUMBER
   ) RETURN BOOLEAN
   AS
   BEGIN
   
      IF p_list_ary IS NULL
      OR p_list_ary.COUNT = 0
      THEN
         RETURN FALSE;
         
      END IF;
      
      IF p_input IS NULL
      THEN
         RETURN NULL;
         
      END IF;
      
      FOR i IN 1 .. p_list_ary.COUNT
      LOOP
         IF ABS(p_list_ary(i)) = p_input
         THEN
            RETURN TRUE;
            
         END IF;
         
      END LOOP;
      
      RETURN FALSE;
   
   END in_list;

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION edge_report(
       p_topology_owner IN  VARCHAR2 DEFAULT NULL
      ,p_topology_name  IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
      ,p_edge_id        IN  NUMBER
   ) RETURN report_results PIPELINED
   AS
      obj_topology        dz_topology := p_topology_obj;
      str_sql             VARCHAR2(4000 Char);
      obj_report_edge     edge_rec;
      obj_prev_left_edge  edge_rec;
      obj_prev_right_edge edge_rec;
      obj_next_left_edge  edge_rec;
      obj_next_right_edge edge_rec;
      num_errors          PLS_INTEGER := 0;
      obj_left_face       face_rec;
      obj_right_face      face_rec;
      obj_start_node      node_rec;
      obj_end_node        node_rec;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check that topology exists and reports valid
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
            PIPE ROW('Topology reporting as invalid');
            RETURN;
            
         END IF;
         
      END IF;

      --------------------------------------------------------------------------
      -- Step 20
      -- Grab the report edge
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || ' a.edge_id '
              || ',a.start_node_id '
              || ',a.end_node_id '
              || ',a.next_left_edge_id '
              || ',a.prev_left_edge_id '
              || ',a.next_right_edge_id '
              || ',a.prev_right_edge_id '
              || ',a.left_face_id '
              || ',a.right_face_id '
              || ',a.geometry '
              || ',NULL '
              || ',NULL '
              || 'FROM '
              || obj_topology.edge_table() || ' a '
              || 'WHERE '
              || 'a.edge_id = :p01 ';
              
      EXECUTE IMMEDIATE str_sql
      INTO obj_report_edge
      USING p_edge_id;
      
      IF obj_report_edge.edge_id IS NULL
      THEN
         PIPE ROW('Edge ' || p_edge_id || ' not found in topology.');
         RETURN;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Grab the previous left edge
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || ' a.edge_id '
              || ',a.start_node_id '
              || ',a.end_node_id '
              || ',a.next_left_edge_id '
              || ',a.prev_left_edge_id '
              || ',a.next_right_edge_id '
              || ',a.prev_right_edge_id '
              || ',a.left_face_id '
              || ',a.right_face_id '
              || ',a.geometry '
              || ',NULL '
              || ',NULL '
              || 'FROM '
              || obj_topology.edge_table() || ' a '
              || 'WHERE '
              || 'a.edge_id = :p01 ';
              
      EXECUTE IMMEDIATE str_sql
      INTO obj_prev_left_edge
      USING ABS(obj_report_edge.prev_left_edge_id);
      
      IF obj_prev_left_edge.edge_id IS NULL
      THEN
         PIPE ROW('Edge ' || p_edge_id || ' previous left edge ' || obj_report_edge.prev_left_edge_id || ' not found in topology.');
         num_errors := num_errors + 1;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Grab the previous right edge
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || ' a.edge_id '
              || ',a.start_node_id '
              || ',a.end_node_id '
              || ',a.next_left_edge_id '
              || ',a.prev_left_edge_id '
              || ',a.next_right_edge_id '
              || ',a.prev_right_edge_id '
              || ',a.left_face_id '
              || ',a.right_face_id '
              || ',a.geometry '
              || ',NULL '
              || ',NULL '
              || 'FROM '
              || obj_topology.edge_table() || ' a '
              || 'WHERE '
              || 'a.edge_id = :p01 ';
              
      EXECUTE IMMEDIATE str_sql
      INTO obj_prev_right_edge
      USING ABS(obj_report_edge.prev_right_edge_id);
      
      IF obj_prev_right_edge.edge_id IS NULL
      THEN
         PIPE ROW('Edge ' || p_edge_id || ' previous right edge ' || obj_report_edge.prev_right_edge_id || ' not found in topology.');
         num_errors := num_errors + 1;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 50
      -- Grab the next left edge
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || ' a.edge_id '
              || ',a.start_node_id '
              || ',a.end_node_id '
              || ',a.next_left_edge_id '
              || ',a.prev_left_edge_id '
              || ',a.next_right_edge_id '
              || ',a.prev_right_edge_id '
              || ',a.left_face_id '
              || ',a.right_face_id '
              || ',a.geometry '
              || ',NULL '
              || ',NULL '
              || 'FROM '
              || obj_topology.edge_table() || ' a '
              || 'WHERE '
              || 'a.edge_id = :p01 ';
              
      EXECUTE IMMEDIATE str_sql
      INTO obj_next_left_edge
      USING ABS(obj_report_edge.next_left_edge_id);
      
      IF obj_next_left_edge.edge_id IS NULL
      THEN
         PIPE ROW('Edge ' || p_edge_id || ' next left edge ' || obj_report_edge.next_left_edge_id || ' not found in topology.');
         num_errors := num_errors + 1;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 60
      -- Grab the next right edge
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || ' a.edge_id '
              || ',a.start_node_id '
              || ',a.end_node_id '
              || ',a.next_left_edge_id '
              || ',a.prev_left_edge_id '
              || ',a.next_right_edge_id '
              || ',a.prev_right_edge_id '
              || ',a.left_face_id '
              || ',a.right_face_id '
              || ',a.geometry '
              || ',NULL '
              || ',NULL '
              || 'FROM '
              || obj_topology.edge_table() || ' a '
              || 'WHERE '
              || 'a.edge_id = :p01 ';
              
      EXECUTE IMMEDIATE str_sql
      INTO obj_next_right_edge
      USING ABS(obj_report_edge.next_right_edge_id);
      
      IF obj_next_right_edge.edge_id IS NULL
      THEN
         PIPE ROW('Edge ' || p_edge_id || ' next right edge ' || obj_report_edge.next_right_edge_id || ' not found in topology.');
         num_errors := num_errors + 1;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 70
      -- Grab the left face
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || ' a.face_id '
              || ',a.boundary_edge_id '
              || ',a.island_edge_id_list '
              || ',a.island_node_id_list '
              || ',a.mbr_geometry '
              || 'FROM '
              || obj_topology.face_table() || ' a '
              || 'WHERE '
              || 'a.face_id = :p01 ';
              
      EXECUTE IMMEDIATE str_sql
      INTO obj_left_face
      USING obj_report_edge.left_face_id;
      
      IF obj_left_face.face_id IS NULL
      THEN
         PIPE ROW('Edge ' || p_edge_id || ' left face ' || obj_report_edge.left_face_id || ' not found in topology.');
         num_errors := num_errors + 1;
         
      ELSE
         IF  obj_left_face.island_edge_id_list IS NOT NULL
         AND obj_left_face.island_edge_id_list.COUNT > 0
         THEN
            IF in_list(
                p_list_ary => obj_left_face.island_edge_id_list
               ,p_input    => obj_report_edge.edge_id
            )
            THEN
               obj_report_edge.left_face_is_outer := 'TRUE';
               
            ELSE
               obj_report_edge.left_face_is_outer := 'FALSE';
               
            END IF;
            
         ELSE
            obj_report_edge.left_face_is_outer := 'FALSE';
            
         END IF;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 80
      -- Grab the right face
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || ' a.face_id '
              || ',a.boundary_edge_id '
              || ',a.island_edge_id_list '
              || ',a.island_node_id_list '
              || ',a.mbr_geometry '
              || 'FROM '
              || obj_topology.face_table() || ' a '
              || 'WHERE '
              || 'a.face_id = :p01 ';
              
      EXECUTE IMMEDIATE str_sql
      INTO obj_right_face
      USING obj_report_edge.right_face_id;
      
      IF obj_right_face.face_id IS NULL
      THEN
         PIPE ROW('Edge ' || p_edge_id || ' right face ' || obj_report_edge.right_face_id || ' not found in topology.');
         num_errors := num_errors + 1;
         
      ELSE
         IF  obj_right_face.island_edge_id_list IS NOT NULL
         AND obj_right_face.island_edge_id_list.COUNT > 0
         THEN
            IF in_list(
                p_list_ary => obj_right_face.island_edge_id_list
               ,p_input    => obj_report_edge.edge_id
            )
            THEN
               obj_report_edge.right_face_is_outer := 'TRUE';
               
            ELSE
               obj_report_edge.right_face_is_outer := 'FALSE';
               
            END IF;
            
         ELSE
            obj_report_edge.right_face_is_outer := 'FALSE';
            
         END IF;
         
      END IF; 
      
      --------------------------------------------------------------------------
      -- Step 90
      -- Grab the start node
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || ' a.node_id '
              || ',a.edge_id '
              || ',a.face_id '
              || ',a.geometry '
              || 'FROM '
              || obj_topology.node_table() || ' a '
              || 'WHERE '
              || 'a.node_id = :p01 ';
              
      EXECUTE IMMEDIATE str_sql
      INTO obj_start_node
      USING obj_report_edge.start_node_id;
      
      IF obj_start_node.node_id IS NULL
      THEN
         PIPE ROW('Edge ' || p_edge_id || ' start node ' || obj_report_edge.start_node_id || ' not found in topology.');
         num_errors := num_errors + 1;
         
      END IF; 
      
      --------------------------------------------------------------------------
      -- Step 100
      -- Grab the end node
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || ' a.node_id '
              || ',a.edge_id '
              || ',a.face_id '
              || ',a.geometry '
              || 'FROM '
              || obj_topology.node_table() || ' a '
              || 'WHERE '
              || 'a.node_id = :p01 ';
              
      EXECUTE IMMEDIATE str_sql
      INTO obj_end_node
      USING obj_report_edge.end_node_id;
      
      IF obj_end_node.node_id IS NULL
      THEN
         PIPE ROW('Edge ' || p_edge_id || ' end node ' || obj_report_edge.end_node_id || ' not found in topology.');
         num_errors := num_errors + 1;
         
      END IF; 
      
      --------------------------------------------------------------------------
      -- Step 110
      -- Exit if any errors so far
      --------------------------------------------------------------------------
      IF num_errors > 0
      THEN
         RETURN;
         
      END IF;
      
      num_errors := 0;
      
      PIPE ROW('Edge: ' || obj_report_edge.edge_id);
      PIPE ROW('----------');
      PIPE ROW('   Previous Left Edge: ' || obj_report_edge.prev_left_edge_id);
      PIPE ROW('   Next Left Edge: ' || obj_report_edge.next_left_edge_id);
      PIPE ROW('   Previous Right Edge: ' || obj_report_edge.prev_right_edge_id);
      PIPE ROW('   Next Right Edge: ' || obj_report_edge.next_right_edge_id);
      PIPE ROW('----------');
      PIPE ROW('   Start Node: ' || obj_report_edge.start_node_id);
      PIPE ROW('   End Node: ' || obj_report_edge.end_node_id);
      PIPE ROW('----------');
      IF obj_report_edge.left_face_is_outer = 'TRUE'
      THEN
         PIPE ROW('   Left Face: ' || obj_report_edge.left_face_id || ' OUTER');
      ELSE
         PIPE ROW('   Left Face: ' || obj_report_edge.left_face_id);
      
      END IF;
      IF obj_report_edge.right_face_is_outer = 'TRUE'
      THEN
         PIPE ROW('   Right Face: ' || obj_report_edge.right_face_id || ' OUTER');
         
      ELSE
         PIPE ROW('   Right Face: ' || obj_report_edge.right_face_id);
         
      END IF;
      PIPE ROW('----------');
      
      --------------------------------------------------------------------------
      -- Step 120
      -- Check whether prev left nodes are correct
      --------------------------------------------------------------------------
      IF obj_report_edge.prev_left_edge_id > 0
      THEN
         IF obj_report_edge.start_node_id <> obj_prev_left_edge.end_node_id
         THEN
            PIPE ROW('** Previous left edge end node does not match start node.');
            num_errors := num_errors + 1;
            
         END IF;
         
      ELSE
         IF obj_report_edge.start_node_id <> obj_prev_left_edge.start_node_id
         THEN
            PIPE ROW('** Previous left edge start node does not match start node.');
            num_errors := num_errors + 1;
            
         END IF;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 130
      -- Check whether next left nodes are correct
      --------------------------------------------------------------------------
      IF obj_report_edge.next_left_edge_id > 0
      THEN
         IF obj_report_edge.end_node_id <> obj_next_left_edge.start_node_id
         THEN
            PIPE ROW('** Next left edge start node does not match end node.');
            num_errors := num_errors + 1;
            
         END IF;
         
      ELSE
         IF obj_report_edge.end_node_id <> obj_next_left_edge.end_node_id
         THEN
            PIPE ROW('** Next left edge end node does not match end node.');
            num_errors := num_errors + 1;
            
         END IF;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 140
      -- Check whether prev right nodes are correct
      --------------------------------------------------------------------------
      IF obj_report_edge.prev_right_edge_id > 0
      THEN
         IF obj_report_edge.end_node_id <> obj_prev_right_edge.end_node_id
         THEN
            PIPE ROW('** Prev right edge end node does not match end node.');
            num_errors := num_errors + 1;
            
         END IF;
         
      ELSE
         IF obj_report_edge.end_node_id <> obj_prev_right_edge.start_node_id
         THEN
            PIPE ROW('** Prev right edge start node does not match end node.');
            num_errors := num_errors + 1;
            
         END IF;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 150
      -- Check whether next right nodes are correct
      --------------------------------------------------------------------------
      IF obj_report_edge.next_right_edge_id > 0
      THEN
         IF obj_report_edge.start_node_id <> obj_next_right_edge.start_node_id
         THEN
            PIPE ROW('** Next right edge start node does not match start node.');
            num_errors := num_errors + 1;
            
         END IF;
         
      ELSE
         IF obj_report_edge.start_node_id <> obj_next_right_edge.end_node_id
         THEN
            PIPE ROW('** Next right edge end node does not match start node.');
            num_errors := num_errors + 1;
            
         END IF;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 160
      -- Exit if any errors so far
      --------------------------------------------------------------------------
      IF num_errors > 0
      THEN
         RETURN;
         
      END IF;
      
      num_errors := 0;
      PIPE ROW('** All nodes connections seems correct.');
      
   END edge_report;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION face_report(
       p_topology_owner IN  VARCHAR2 DEFAULT NULL
      ,p_topology_name  IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
      ,p_face_id        IN  NUMBER
   ) RETURN report_results PIPELINED
   AS
      obj_topology        dz_topology := p_topology_obj;
      str_sql             VARCHAR2(4000 Char);
      obj_report_face     face_rec;
      num_errors          PLS_INTEGER := 0;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check that topology exists and reports valid
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
            PIPE ROW('Topology reporting as invalid');
            RETURN;
            
         END IF;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Grab the report face
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || ' a.face_id '
              || ',a.boundary_edge_id '
              || ',a.island_edge_id_list '
              || ',a.island_node_id_list '
              || ',a.mbr_geometry '
              || 'FROM '
              || obj_topology.face_table() || ' a '
              || 'WHERE '
              || 'a.face_id = :p01 ';
              
      EXECUTE IMMEDIATE str_sql
      INTO obj_report_face
      USING p_face_id;
      
      IF obj_report_face.face_id IS NULL
      THEN
         PIPE ROW('Face ' || p_face_id || ' not found in topology.');
         RETURN;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 100
      -- Exit if any errors so far
      --------------------------------------------------------------------------
      IF num_errors > 0
      THEN
         RETURN;
         
      END IF;
      
      num_errors := 0;
      
      PIPE ROW('Face: ' || obj_report_face.face_id);
      PIPE ROW('----------');
      PIPE ROW('   Boundary Edge: ' || obj_report_face.boundary_edge_id);
      PIPE ROW('   Island Edge Count: ' || obj_report_face.island_edge_id_list.COUNT);
      PIPE ROW('   Island Node Count: ' || obj_report_face.island_node_id_list.COUNT);
      PIPE ROW('----------');
      
   
   END face_report;
   
END dz_topo_reports;
/

