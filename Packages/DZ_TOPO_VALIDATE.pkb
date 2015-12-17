CREATE OR REPLACE PACKAGE BODY dz_topo_validate
AS

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION concatenate_results(
       p_input_a         IN validation_results
      ,p_input_b         IN validation_results
   ) RETURN validation_results
   AS
      ary_output  validation_results := validation_results();
      int_counter PLS_INTEGER;
      
   BEGIN
   
      IF p_input_a IS NULL
      OR p_input_a.COUNT = 0
      THEN
         RETURN p_input_b;
         
      ELSIF p_input_b IS NULL
      OR p_input_b.COUNT = 0
      THEN
         RETURN p_input_a;
         
      END IF;
      
      ary_output := p_input_a;
      int_counter := p_input_a.COUNT + 1;
      ary_output.EXTEND(p_input_b.COUNT);
      
      FOR i IN 1 .. p_input_b.COUNT
      LOOP
         ary_output(int_counter) := p_input_b(i);
         int_counter := int_counter + 1;
         
      END LOOP;
      
      RETURN ary_output;
   
   END concatenate_results;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION check_indexes(
      p_topology_obj   IN  dz_topology
   ) RETURN validation_results
   AS
      ary_output  validation_results := validation_results();
      int_counter PLS_INTEGER := 1;
      
   BEGIN
      
      IF dz_topo_util.index_exists(
          p_owner       => p_topology_obj.topology_owner
         ,p_table_name  => p_topology_obj.edge_table_name()
         ,p_column_name => 'NEXT_LEFT_EDGE_ID'
      ) <> 'TRUE'
      THEN
         ary_output.EXTEND();
         ary_output(int_counter) := 'Edge table needs index on next_left_edge_id to validate';
         int_counter := int_counter + 1;
            
      END IF;
         
      IF dz_topo_util.index_exists(
          p_owner       => p_topology_obj.topology_owner
         ,p_table_name  => p_topology_obj.edge_table_name()
         ,p_column_name => 'NEXT_RIGHT_EDGE_ID'
      ) <> 'TRUE'
      THEN
         ary_output.EXTEND();
         ary_output(int_counter) := 'Edge table needs index on next_right_edge_id to validate';
         int_counter := int_counter + 1;
            
      END IF;
         
      IF dz_topo_util.index_exists(
          p_owner       => p_topology_obj.topology_owner
         ,p_table_name  => p_topology_obj.edge_table_name()
         ,p_column_name => 'PREV_LEFT_EDGE_ID'
      ) <> 'TRUE'
      THEN
         ary_output.EXTEND();
         ary_output(int_counter) := 'Edge table needs index on prev_left_edge_id to validate';
         int_counter := int_counter + 1;
            
      END IF;
         
      IF dz_topo_util.index_exists(
          p_owner       => p_topology_obj.topology_owner
         ,p_table_name  => p_topology_obj.edge_table_name()
         ,p_column_name => 'PREV_RIGHT_EDGE_ID'
      ) <> 'TRUE'
      THEN
         ary_output.EXTEND();
         ary_output(int_counter) := 'Edge table needs index on prev_right_edge_id to validate';
         int_counter := int_counter + 1;
            
      END IF;
      
      RETURN ary_output;
         
   END check_indexes;

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION z_check_basic(
       p_topology_obj   IN  dz_topology DEFAULT NULL
   ) RETURN validation_results
   AS
      ary_temp     validation_results;
      ary_output   validation_results;
      obj_topology dz_topology := p_topology_obj;
      str_sql      VARCHAR2(4000 Char);
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check for edges with bad left_face_ids
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || '''edge '' || a.edge_id || '' has bad left_face_id ('' || a.left_face_id || '')'''
              || 'FROM '
              ||  obj_topology.edge_table() || ' a '
              || 'WHERE '
              || 'a.left_face_id NOT IN ( '
              || '   SELECT '
              || '   b.face_id '
              || '   FROM '
              || '   ' || obj_topology.face_table() || ' b '
              || ')';
              
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO ary_temp;
      
      IF ary_temp IS NOT NULL
      AND ary_temp.COUNT > 0
      THEN
         ary_output := concatenate_results(
             p_input_a => ary_output
            ,p_input_b => ary_temp
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Check for edges with bad right_face_ids
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || '''edge '' || a.edge_id || '' has bad right_face_id ('' || a.right_face_id || '')'''
              || 'FROM '
              ||  obj_topology.edge_table() || ' a '
              || 'WHERE '
              || 'a.right_face_id NOT IN ( '
              || '   SELECT '
              || '   b.face_id '
              || '   FROM '
              || '   ' || obj_topology.face_table() || ' b '
              || ')';
              
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO ary_temp;
      
      IF ary_temp IS NOT NULL
      AND ary_temp.COUNT > 0
      THEN
         ary_output := concatenate_results(
             p_input_a => ary_output
            ,p_input_b => ary_temp
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Check for edges with bad next_left_edge_id
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || '''edge '' || a.edge_id || '' has bad next_left_edge_id ('' || a.next_left_edge_id || '')'''
              || 'FROM '
              ||  obj_topology.edge_table() || ' a '
              || 'WHERE '
              || 'ABS(a.next_left_edge_id) NOT IN ( '
              || '   SELECT '
              || '   b.edge_id '
              || '   FROM '
              || '   ' || obj_topology.edge_table() || ' b '
              || ')';
              
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO ary_temp;
      
      IF ary_temp IS NOT NULL
      AND ary_temp.COUNT > 0
      THEN
         ary_output := concatenate_results(
             p_input_a => ary_output
            ,p_input_b => ary_temp
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Check for edges with bad next_right_edge_id
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || '''edge '' || a.edge_id || '' has bad next_right_edge_id ('' || a.next_right_edge_id || '')'''
              || 'FROM '
              ||  obj_topology.edge_table() || ' a '
              || 'WHERE '
              || 'ABS(a.next_right_edge_id) NOT IN ( '
              || '   SELECT '
              || '   b.edge_id '
              || '   FROM '
              || '   ' || obj_topology.edge_table() || ' b '
              || ')';
              
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO ary_temp;
      
      IF ary_temp IS NOT NULL
      AND ary_temp.COUNT > 0
      THEN
         ary_output := concatenate_results(
             p_input_a => ary_output
            ,p_input_b => ary_temp
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 50
      -- Check for edges with bad prev_left_edge_id
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || '''edge '' || a.edge_id || '' has bad prev_left_edge_id ('' || a.prev_left_edge_id || '')'''
              || 'FROM '
              ||  obj_topology.edge_table() || ' a '
              || 'WHERE '
              || 'ABS(a.prev_left_edge_id) NOT IN ( '
              || '   SELECT '
              || '   b.edge_id '
              || '   FROM '
              || '   ' || obj_topology.edge_table() || ' b '
              || ')';
              
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO ary_temp;
      
      IF ary_temp IS NOT NULL
      AND ary_temp.COUNT > 0
      THEN
         ary_output := concatenate_results(
             p_input_a => ary_output
            ,p_input_b => ary_temp
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 60
      -- Check for edges with bad prev_right_edge_id
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || '''edge '' || a.edge_id || '' has bad prev_right_edge_id ('' || a.prev_right_edge_id || '')'''
              || 'FROM '
              ||  obj_topology.edge_table() || ' a '
              || 'WHERE '
              || 'ABS(a.prev_right_edge_id) NOT IN ( '
              || '   SELECT '
              || '   b.edge_id '
              || '   FROM '
              || '   ' || obj_topology.edge_table() || ' b '
              || ')';
              
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO ary_temp;
      
      IF ary_temp IS NOT NULL
      AND ary_temp.COUNT > 0
      THEN
         ary_output := concatenate_results(
             p_input_a => ary_output
            ,p_input_b => ary_temp
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 70
      -- Check for edges with bad start_node_id
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || '''edge '' || a.edge_id || '' has bad start_node_id ('' || a.start_node_id || '')'''
              || 'FROM '
              ||  obj_topology.edge_table() || ' a '
              || 'WHERE '
              || 'a.start_node_id NOT IN ( '
              || '   SELECT '
              || '   b.node_id '
              || '   FROM '
              || '   ' || obj_topology.node_table() || ' b '
              || ')';
              
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO ary_temp;
      
      IF ary_temp IS NOT NULL
      AND ary_temp.COUNT > 0
      THEN
         ary_output := concatenate_results(
             p_input_a => ary_output
            ,p_input_b => ary_temp
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 80
      -- Check for edges with bad end_node_id
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || '''edge '' || a.edge_id || '' has bad end_node_id ('' || a.end_node_id || '')'''
              || 'FROM '
              ||  obj_topology.edge_table() || ' a '
              || 'WHERE '
              || 'a.end_node_id NOT IN ( '
              || '   SELECT '
              || '   b.node_id '
              || '   FROM '
              || '   ' || obj_topology.node_table() || ' b '
              || ')';
              
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO ary_temp;
      
      IF ary_temp IS NOT NULL
      AND ary_temp.COUNT > 0
      THEN
         ary_output := concatenate_results(
             p_input_a => ary_output
            ,p_input_b => ary_temp
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 90
      -- Check for nodes with bad edge_id
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || '''node '' || a.node_id || '' has bad edge_id ('' || a.edge_id || '')'''
              || 'FROM '
              ||  obj_topology.node_table() || ' a '
              || 'WHERE '
              || 'ABS(a.edge_id) NOT IN ( '
              || '   SELECT '
              || '   b.edge_id '
              || '   FROM '
              || '   ' || obj_topology.edge_table() || ' b '
              || ')';
              
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO ary_temp;
      
      IF ary_temp IS NOT NULL
      AND ary_temp.COUNT > 0
      THEN
         ary_output := concatenate_results(
             p_input_a => ary_output
            ,p_input_b => ary_temp
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 100
      -- Check for nodes with bad face_id
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || '''node '' || a.node_id || '' has bad face_id ('' || a.face_id || '')'''
              || 'FROM '
              ||  obj_topology.node_table() || ' a '
              || 'WHERE '
              || 'a.face_id <> 0 '
              || 'AND a.face_id NOT IN ( '
              || '   SELECT '
              || '   b.face_id '
              || '   FROM '
              || '   ' || obj_topology.face_table() || ' b '
              || ')';
             
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO ary_temp;
      
      IF ary_temp IS NOT NULL
      AND ary_temp.COUNT > 0
      THEN
         ary_output := concatenate_results(
             p_input_a => ary_output
            ,p_input_b => ary_temp
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 110
      -- Check for faces with bad boundary_edge_id
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || '''face '' || a.face_id || '' has bad boundary_edge_id ('' || a.boundary_edge_id || '')'''
              || 'FROM '
              ||  obj_topology.face_table() || ' a '
              || 'WHERE '
              || 'ABS(a.boundary_edge_id) NOT IN ( '
              || '   SELECT '
              || '   b.edge_id '
              || '   FROM '
              || '   ' || obj_topology.edge_table() || ' b '
              || ')';
              
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO ary_temp;
      
      IF ary_temp IS NOT NULL
      AND ary_temp.COUNT > 0
      THEN
         ary_output := concatenate_results(
             p_input_a => ary_output
            ,p_input_b => ary_temp
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 120
      -- Check for faces with bad island_edge_id
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || '''face '' || a.face_id || '' has bad island_edge_id ('' || a.island_edge_id || '')'''
              || 'FROM ( '
              || '   SELECT '
              || '    aa.face_id '
              || '   ,bb.column_value as island_edge_id '
              || '   FROM '
              || '   ' || obj_topology.face_table() || ' aa, '
              || '   TABLE(aa.island_edge_id_list) bb '
              || ') a '
              || 'WHERE ' 
              || 'ABS(a.island_edge_id) NOT IN ( '
              || '   SELECT '
              || '   b.edge_id '
              || '   FROM '
              || '   ' || obj_topology.edge_table() || ' b '
              || ')';
              
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO ary_temp;
      
      IF ary_temp IS NOT NULL
      AND ary_temp.COUNT > 0
      THEN
         ary_output := concatenate_results(
             p_input_a => ary_output
            ,p_input_b => ary_temp
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 130
      -- Check for faces with bad island_edge_id
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || '''face '' || a.face_id || '' has bad island_node_id ('' || a.island_node_id || '')''' 
              || 'FROM ( '
              || '   SELECT '
              || '    aa.face_id '
              || '   ,bb.column_value as island_node_id '
              || '   FROM '
              || '   ' || obj_topology.face_table() || ' aa, '
              || '   TABLE(aa.island_node_id_list) bb '
              || ') a '
              || 'WHERE ' 
              || 'ABS(a.island_node_id) NOT IN ( '
              || '   SELECT '
              || '   b.node_id '
              || '   FROM '
              || '   ' || obj_topology.node_table() || ' b '
              || ')';
              
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO ary_temp;
      
      IF ary_temp IS NOT NULL
      AND ary_temp.COUNT > 0
      THEN
         ary_output := concatenate_results(
             p_input_a => ary_output
            ,p_input_b => ary_temp
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 140
      -- Check for relations with bad polygon faces
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || '''relation tg_id '' || a.tg_id || '' has bad topo face_id ('' || a.topo_id || '')'''
              || 'FROM '
              ||  obj_topology.relation_table() || ' a '
              || 'WHERE '
              || '    a.topo_type = 3 '
              || 'AND a.topo_id NOT IN ( '
              || '   SELECT '
              || '   b.face_id '
              || '   FROM '
              || '   ' || obj_topology.face_table() || ' b '
              || ')';
              
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO ary_temp;
      
      IF ary_temp IS NOT NULL
      AND ary_temp.COUNT > 0
      THEN
         ary_output := concatenate_results(
             p_input_a => ary_output
            ,p_input_b => ary_temp
         );
         
      END IF;
      
      IF ary_output IS NULL
      OR ary_output.COUNT = 0
      THEN
         ary_output := validation_results();
         ary_output.EXTEND();
         ary_output(1) := 'Basic test set all okay.';
         
      END IF;
      
      RETURN ary_output;
   
   END z_check_basic;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION check_all(
       p_topology_owner IN  VARCHAR2
      ,p_topology_name  IN  VARCHAR2
   ) RETURN validation_results PIPELINED
   AS
      ary_output   validation_results;
      obj_topology dz_topology;
      
   BEGIN
      
      --------------------------------------------------------------------------
      -- Step 10
      -- Check that topology exists and reports valid
      --------------------------------------------------------------------------
      obj_topology := dz_topology(
          p_topology_owner => p_topology_owner
         ,p_topology_name  => p_topology_name
      );
         
      IF obj_topology.valid() <> 'TRUE'
      THEN
         PIPE ROW('Topology reporting as invalid');
         RETURN;
            
      END IF;

      --------------------------------------------------------------------------
      -- Step 20
      -- Check that edge table has supplemental indexes on edge links
      --------------------------------------------------------------------------
      ary_output := check_indexes(
         p_topology_obj => obj_topology
      );

      IF ary_output.COUNT > 0
      THEN
         FOR i IN 1 .. ary_output.COUNT
         LOOP
            PIPE ROW(ary_output(i));
               
         END LOOP;
            
         RETURN;
            
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Do basic tests
      --------------------------------------------------------------------------
      ary_output := z_check_basic(
          p_topology_obj => obj_topology
      );
      
      IF ary_output IS NOT NULL
      AND ary_output.COUNT > 0
      THEN
         FOR i IN 1 .. ary_output.COUNT
         LOOP
            PIPE ROW(ary_output(i));
            
         END LOOP;
         
      END IF;
   
   END check_all;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION check_basic(
       p_topology_owner IN  VARCHAR2
      ,p_topology_name  IN  VARCHAR2
   ) RETURN validation_results PIPELINED
   AS
      obj_topology dz_topology;
      ary_output   validation_results;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check that topology exists and reports valid
      --------------------------------------------------------------------------
      obj_topology := dz_topology(
          p_topology_owner => p_topology_owner
         ,p_topology_name  => p_topology_name
      );
         
      IF obj_topology.valid() <> 'TRUE'
      THEN
         PIPE ROW('Topology reporting as invalid');
         RETURN;
            
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Check that edge table has supplemental indexes on edge links
      --------------------------------------------------------------------------
      ary_output := check_indexes(
         p_topology_obj => obj_topology
      );

      IF ary_output.COUNT > 0
      THEN
         FOR i IN 1 .. ary_output.COUNT
         LOOP
            PIPE ROW(ary_output(i));
               
         END LOOP;
            
         RETURN;
            
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Execute the basic checks
      --------------------------------------------------------------------------
      ary_output := z_check_basic(
          p_topology_obj => obj_topology
      );
      
      IF ary_output IS NOT NULL
      AND ary_output.COUNT > 0
      THEN
         FOR i IN 1 .. ary_output.COUNT
         LOOP
            PIPE ROW(ary_output(i));
            
         END LOOP;
         
      END IF;
      
   END check_basic;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION z_check_universal_face(
       p_topology_obj   IN  dz_topology
   ) RETURN validation_results
   AS
      ary_temp         validation_results;
      ary_output       validation_results;
      int_counter      PLS_INTEGER := 1;
      obj_topology     dz_topology := p_topology_obj;
      str_sql          VARCHAR2(4000 Char);
      ary_island_edges MDSYS.SDO_LIST_TYPE;
      
   BEGIN
   
      ary_output := validation_results();
       
      --------------------------------------------------------------------------
      -- Step 10
      -- Slurp up the universal face island edges
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || 'a.island_edge_id_list '
              || 'FROM '
              ||  obj_topology.face_table() || ' a '
              || 'WHERE '
              || 'a.face_id = -1 ';
              
      EXECUTE IMMEDIATE str_sql
      INTO ary_island_edges;
      
      IF ary_island_edges IS NULL
      OR ary_island_edges.COUNT = 0
      THEN
         ary_output.EXTEND();
         ary_output(int_counter) := 'Univeral face has no island edges.';
         int_counter := int_counter + 1;
         
      ELSE
         ary_output.EXTEND();
         ary_output(int_counter) := 'Univeral face has ' || ary_island_edges.COUNT || ' island edges.';
         int_counter := int_counter + 1;
        
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Look for any island edges that are not on the universal face
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || '''Universal island edge '' || a.edge_id || '' is not on the universal face '''
              || 'FROM '
              ||  obj_topology.edge_table() || ' a '
              || 'WHERE '
              ||     'a.edge_id IN (SELECT * FROM TABLE(:p01)) '
              || 'AND ( '
              || '   a.left_face_id = -1 OR a.right_face_id = -1 '
              || ') ';
      
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO ary_temp
      USING ary_island_edges;
      
      IF ary_temp IS NOT NULL
      AND ary_temp.COUNT > 0
      THEN
         ary_output := concatenate_results(
             p_input_a => ary_output
            ,p_input_b => ary_temp
         );
         
      END IF; 
      
      
      RETURN ary_output;
      
   END z_check_universal_face;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION check_universal_face(
       p_topology_owner IN  VARCHAR2
      ,p_topology_name  IN  VARCHAR2
   ) RETURN validation_results PIPELINED
   AS
      obj_topology dz_topology;
      ary_output   validation_results;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check that topology exists and reports valid
      --------------------------------------------------------------------------
      obj_topology := dz_topology(
          p_topology_owner => p_topology_owner
         ,p_topology_name  => p_topology_name
      );
         
      IF obj_topology.valid() <> 'TRUE'
      THEN
         PIPE ROW('Topology reporting as invalid');
         RETURN;
            
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Check that edge table has supplemental indexes on edge links
      --------------------------------------------------------------------------
      ary_output := check_indexes(
         p_topology_obj => obj_topology
      );

      IF ary_output.COUNT > 0
      THEN
         FOR i IN 1 .. ary_output.COUNT
         LOOP
            PIPE ROW(ary_output(i));
               
         END LOOP;
            
         RETURN;
            
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Execute the basic checks
      --------------------------------------------------------------------------
      ary_output := z_check_universal_face(
          p_topology_obj => obj_topology
      );
      
      IF ary_output IS NOT NULL
      AND ary_output.COUNT > 0
      THEN
         FOR i IN 1 .. ary_output.COUNT
         LOOP
            PIPE ROW(ary_output(i));
            
         END LOOP;
         
      END IF;
      
   END check_universal_face;
   
END dz_topo_validate;
/

