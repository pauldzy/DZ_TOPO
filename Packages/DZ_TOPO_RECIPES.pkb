CREATE OR REPLACE PACKAGE BODY dz_topo_recipes
AS

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE align_edges(
       p_table_name            IN  VARCHAR2
      ,p_column_name           IN  VARCHAR2
      ,p_work_table_name       IN  VARCHAR2 DEFAULT 'GERINGER_TMP'
   )
   AS
      /*
      obj_spdx waters_code.ow_spatial_index;
      str_sql  VARCHAR2(4000);
      num_tolerance NUMBER := 0.0000005;
         
      TYPE cursor_type is REF CURSOR;
      query_crs cursor_type;
         
      a_rowid        ROWID;
      b_rowid        ROWID;
      update_window  BOOLEAN;
      update_next    BOOLEAN;
      window_geom    SDO_GEOMETRY;
      next_geom      SDO_GEOMETRY;
      */
      
   BEGIN
      
      /*
      --------------------------------------------------------------------------
      -- Step 10
      -- Determine the SRID from the table and check things are kosher
      --------------------------------------------------------------------------
      obj_spdx := waters_code.ow_spatial_index(
          p_table_name => p_table_name
         ,p_column_name => p_column_name
      );
         
      --------------------------------------------------------------------------
      -- Step 20
      -- Update SRID to NULL
      --------------------------------------------------------------------------
      IF obj_spdx.get_srid() IS NOT NULL
      THEN
         obj_spdx.change_table_srid(NULL);
      
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Rebuild the index regardless
      --------------------------------------------------------------------------
      obj_spdx.redefine_index(NULL,'GEODETIC');
         
      --------------------------------------------------------------------------
      -- Step 30
      -- Create Geringer work table
      --------------------------------------------------------------------------
      str_sql := 'CREATE TABLE ' || p_work_table_name || '( '
              || '    sdo_rowid ROWID '
              || '    ,' || p_column_name || ' SDO_GEOMETRY '
              || ')';
                 
      BEGIN
         EXECUTE IMMEDIATE str_sql;
            
      EXCEPTION
         WHEN OTHERS 
         THEN
            IF SQLCODE = -00955
            THEN
               EXECUTE IMMEDIATE 'DROP TABLE ' || p_work_table_name;
               EXECUTE IMMEDIATE str_sql;
                  
            ELSE
               RAISE;
            END IF;
               
      END;
         
      --------------------------------------------------------------------------
      -- Step 40
      -- Define the cursor string
      --------------------------------------------------------------------------
      str_sql := 'SELECT /' || '*+ ordered use_nl (a b) use_nl (a c) *' || '/ '
              || ' a.rowid1 AS a_rowid '
              || ',a.rowid2 AS b_rowid ' 
              || 'FROM '
              || 'TABLE( '
              || '   SDO_JOIN( '
              || '       ''' || p_table_name || ''',''' || p_column_name || ''' '
              || '      ,''' || p_table_name || ''',''' || p_column_name || ''' '
              || '   ) '
              || ') a, ' 
              || p_table_name || ' b,' 
              || p_table_name || ' c ' 
              || 'WHERE '
              || '    a.rowid1 = b.rowid '
              || 'AND a.rowid2 = c.rowid '
              || 'AND a.rowid1 < a.rowid2 '
              || 'AND SDO_GEOM.RELATE( '
              || '    b.' || p_column_name 
              || '   ,''ANYINTERACT'' '
              || '   ,c.' || p_column_name 
              || '   ,' || num_tolerance
              || ') = ''TRUE''';
              
      --------------------------------------------------------------------------
      -- Step 40
      -- Fetch the results
      --------------------------------------------------------------------------
      OPEN query_crs FOR str_sql;

      LOOP
         FETCH query_crs INTO a_rowid, b_rowid;
         EXIT WHEN query_crs%NOTFOUND;
            
         BEGIN
            EXECUTE IMMEDIATE 
               'SELECT '
            || 'a.' || p_column_name || ' ' 
            || 'FROM ' 
            || p_work_table_name || ' a '
            || 'WHERE '
            || 'a.sdo_rowid = ''' || a_rowid || ''' '
            INTO window_geom;

            update_window := TRUE;
               
         EXCEPTION
            WHEN NO_DATA_FOUND 
            THEN
               EXECUTE IMMEDIATE 
                  'SELECT ' 
               || 'a.' || p_column_name || ' ' 
               || 'FROM '
               || p_table_name  || ' a '
               || 'WHERE '
               || 'a.rowid = ''' || a_rowid || ''' '
               INTO window_geom;
               
            update_window := FALSE;
            
         END;

         BEGIN
            EXECUTE IMMEDIATE 
               'SELECT ' 
            || 'a.' || p_column_name || ' '
            || 'FROM ' 
            || p_work_table_name || ' a '
            || 'WHERE '
            || 'a.sdo_rowid = ''' || b_rowid || ''' '
            INTO next_geom;
               
            update_next := TRUE;
            
         EXCEPTION
            WHEN NO_DATA_FOUND 
            THEN
               
               EXECUTE IMMEDIATE 
                  'SELECT ' 
               || 'a.' || p_column_name || ' '
               || 'FROM '
               || p_table_name  || ' a ' 
               || 'WHERE '
               || 'a.rowid = ''' || b_rowid || ''' '
               INTO next_geom;
                  
               update_next := FALSE;
                  
         END;

         window_geom := SDO_GEOM.SDO_DIFFERENCE(
             window_geom
            ,next_geom
            ,num_tolerance
         );
            
         next_geom   := SDO_GEOM.SDO_DIFFERENCE(
             next_geom
            ,window_geom
            ,num_tolerance
         );
            
         IF update_next = TRUE
         THEN
            EXECUTE IMMEDIATE 
               'UPDATE ' || p_work_table_name || ' a '
            || 'SET a.' || p_column_name || ' = :p01 '
            || 'WHERE '
            || 'a.sdo_rowid = :p02'
            USING next_geom,b_rowid;
               
         ELSE
            EXECUTE IMMEDIATE 
               'INSERT INTO ' || p_work_table_name || '( '
            || '    sdo_rowid '
            || '   ,' || p_column_name || ' '
            || ') VALUES ( '
            || '     :p01 '
            || '    ,:p02 '
            || ') '
            USING b_rowid,next_geom;
               
         END IF;

         IF update_window = TRUE
         THEN
            EXECUTE IMMEDIATE 
               'UPDATE ' || p_work_table_name || ' a '
            || 'SET a.' || p_column_name || ' = :p01 '
            || 'WHERE '
            || 'a.sdo_rowid = :p02 '
            USING window_geom,a_rowid;

         ELSE
            EXECUTE IMMEDIATE 
               'INSERT INTO ' || p_work_table_name || '( '
            || '    sdo_rowid '
            || '   ,' || p_column_name || ' '
            || ') VALUES ( '
            || '    :p01 '
            || '   ,:p02 '
            || ') '
            USING a_rowid,window_geom;
               
         END IF;

         COMMIT;
            
      END LOOP;

      CLOSE query_crs;
      
      */
      
      NULL;
         
   END align_edges;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE generic_loader
   AS
      /*
      obj_topo           waters_code.dz_topology;
      ary_featureids     MDSYS.SDO_NUMBER_ARRAY;
      ary_sourcefc       MDSYS.SDO_STRING2_ARRAY;
      ary_nhdplus_region MDSYS.SDO_STRING2_ARRAY;
      ary_areasqkm       MDSYS.SDO_NUMBER_ARRAY;
      ary_shape          MDSYS.SDO_GEOMETRY_ARRAY;
      topo_shape         MDSYS.SDO_TOPO_GEOMETRY;
      num_gulp           NUMBER := 900;
      */
   BEGIN
      
      /*
      SELECT 
       a.featureid
      ,a.sourcefc
      ,a.nhdplus_region
      ,a.areasqkm
      ,a.shape
      BULK COLLECT INTO 
       ary_featureids
      ,ary_sourcefc
      ,ary_nhdplus_region
      ,ary_areasqkm
      ,ary_shape
      FROM 
      dziemiela.catrough_np21_sdo a 
      WHERE 
          a.nhdplus_region = '&&1'
      AND substr(reachcode,1,8) = '&&2'
      AND a.featureid NOT IN (
         SELECT
         b.featureid
         FROM
         dziemiela.catrough_np21_topo b
      ) and rownum <= num_gulp
      ORDER BY reachcode DESC;
      
      IF ary_featureids.COUNT = 0
      THEN
         raise_application_error(-20001,'query found no records (' || ary_featureids.COUNT || ')');
      END IF;
      
      obj_topo := waters_code.dz_topology(
          p_topology_name    => 'CATROUGH_NP21'
         ,p_active_table     => 'CATROUGH_NP21_TOPO'
         ,p_active_column    => 'TOPO_GEOM'
      );
         
      obj_topo.set_topo_map_mgr(
         waters_code.dz_topo_map_mgr(
             p_window_sdo      => ary_shape
            ,p_window_padding  => 0.000001
            ,p_topology_name   => 'CATROUGH_NP21'
            ,p_number_of_edges => num_gulp * 8
            ,p_number_of_nodes => num_gulp * 10
            ,p_number_of_faces => num_gulp * 3
         )
      );
      
      obj_topo.java_memory(p_gigs => 13);
      
      FOR i IN 1 .. ary_featureids.COUNT
      LOOP
         topo_shape := obj_topo.create_feature(
            ary_shape(i)
         );
            
         INSERT INTO dziemiela.catrough_np21_topo(
             featureid
            ,sourcefc
            ,nhdplus_region
            ,areasqkm
            ,topo_geom
         ) VALUES (
             ary_featureids(i)
            ,ary_sourcefc(i)
            ,ary_nhdplus_region(i)
            ,ary_areasqkm(i)
            ,topo_shape
         );
            
      END LOOP;
      
      obj_topo.commit_topo_map();
      COMMIT;
      
      */
      
      NULL;
      
   END generic_loader;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE scorched_earth_around_face(
       p_face_id         IN  NUMBER
   )
   AS
      str_sql            VARCHAR2(4000 Char);
      str_topology_owner VARCHAR2(30 Char) := 'NHDPLUS';
      str_topology_name  VARCHAR2(30 Char) := 'CATCHMENT_SM_NP21';
      num_tg_layer_id    NUMBER := 1;
      obj_topology       dz_topology;
      ary_topos          dz_topo_vry;
      
   BEGIN
   
      obj_topology := dz_topology(
          p_topology_owner => str_topology_owner
         ,p_topology_name  => str_topology_name
      );
         
      IF obj_topology.valid() <> 'TRUE'
      THEN
         RAISE_APPLICATION_ERROR(-20001,'invalid topology');
         
      END IF;
      
      obj_topology.set_active_topo_layer(
         p_tg_layer_id => num_tg_layer_id
      );
      
      ary_topos := dz_topo_main.get_face_topo_neighbors(
          p_topology_obj    => obj_topology
         ,p_tg_layer_id     => num_tg_layer_id
         ,p_face_id         => p_face_id
      );
      
      str_sql  := 'DELETE FROM '
               || obj_topology.active_topo_table() || ' a '
               || 'WHERE '
               || 'a.' || obj_topology.active_topo_column() || '.tg_id IN ( '
               || '  SELECT tg_id FROM TABLE(:p01) '
               || ') ';
      
      EXECUTE IMMEDIATE str_sql
      USING ary_topos;
             
      COMMIT;
      
      dz_topo_main.drop_face_clean(
          p_topology_obj    => obj_topology
         ,p_face_id         => p_face_id
      );
   
   END scorched_earth_around_face;

END dz_topo_recipes;
/

