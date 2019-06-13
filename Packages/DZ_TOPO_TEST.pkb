CREATE OR REPLACE PACKAGE BODY dz_topo_test
AS

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION random_name(
       p_length NUMBER DEFAULT 30
   ) RETURN VARCHAR2
   AS
      str_seq    VARCHAR2(4000 Char);

   BEGIN

      str_seq := TO_CHAR(
         SYS_CONTEXT('USERENV', 'SESSIONID')
      ) || TO_CHAR(
         REPLACE(
             DBMS_RANDOM.RANDOM
            ,'-'
            ,''
         )
      );
      
      IF LENGTH(str_seq) > p_length - 3
      THEN
        str_seq := SUBSTR(str_seq,1,p_length - 3);
        
      END IF;

      RETURN 'DZ' || str_seq || 'X';
   
   END random_name;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION prerequisites
   RETURN NUMBER
   AS
      num_check NUMBER;
      
   BEGIN
      
      FOR i IN 1 .. C_PREREQUISITES.COUNT
      LOOP
         SELECT 
         COUNT(*)
         INTO num_check
         FROM 
         user_objects a
         WHERE 
             a.object_name = C_PREREQUISITES(i) || '_TEST'
         AND a.object_type = 'PACKAGE';
         
         IF num_check <> 1
         THEN
            RETURN 1;
         
         END IF;
      
      END LOOP;
      
      RETURN 0;
   
   END prerequisites;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION version
   RETURN VARCHAR2
   AS
   BEGIN
      RETURN '{'
      || ' "GITRELEASE":"'    || C_GITRELEASE    || '"'
      || ',"GITCOMMIT":"'     || C_GITCOMMIT     || '"'
      || ',"GITCOMMITDATE":"' || C_GITCOMMITDATE || '"'
      || ',"GITCOMMITAUTH":"' || C_GITCOMMITAUTH || '"'
      || '}';
      
   END version;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION inmemory_test
   RETURN NUMBER
   AS
   BEGIN
      RETURN 0;
      
   END inmemory_test;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION scratch_test
   RETURN NUMBER
   AS
      num_counter  NUMBER;
      num_results  NUMBER := 0;
      num_area     NUMBER;
      str_results  VARCHAR2(255 Char);
      str_topology VARCHAR2(20 Char);
      obj_topo     dz_topology;
      ary_ids      MDSYS.SDO_NUMBER_ARRAY;
      ary_shape    MDSYS.SDO_GEOMETRY_ARRAY;
      topo_shape   MDSYS.SDO_TOPO_GEOMETRY;
      ary_topos    dz_topo_vry;
      sdo_temp     MDSYS.SDO_GEOMETRY;
      
   BEGIN
      
      --------------------------------------------------------------------------
      -- Step 10
      -- Bail if there is no scratch schema or test data
      --------------------------------------------------------------------------
      SELECT
      COUNT(*)
      INTO num_counter
      FROM
      all_users a
      WHERE
      a.username IN (C_TEST_SCHEMA);
      
      IF num_counter <> 1
      THEN
         RETURN 0;
         
      END IF;
      
      SELECT
      COUNT(*)
      INTO num_counter
      FROM
      all_users a
      WHERE
      a.username IN (C_DZ_TESTDATA);
      
      IF num_counter <> 1
      THEN
         RETURN 0;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Topology test #1
      -- Create and populate topology
      --------------------------------------------------------------------------
      str_topology := random_name(15);
      
      MDSYS.SDO_TOPO.CREATE_TOPOLOGY(
          topology  => str_topology
         ,tolerance => 0.05
         ,srid      => 8265
      );
 
      EXECUTE IMMEDIATE q'[     
         INSERT INTO ]' || str_topology || q'[_face$(
             face_id 
            ,boundary_edge_id 
            ,island_edge_id_list 
            ,island_node_id_list 
            ,mbr_geometry 
         ) VALUES ( 
             -1 
            ,NULL 
            ,MDSYS.SDO_LIST_TYPE() 
            ,MDSYS.SDO_LIST_TYPE() 
            ,NULL 
         )
      ]';
      COMMIT;
      
      EXECUTE IMMEDIATE q'[
         CREATE TABLE ]' || str_topology || q'[_topo(
             featureid INTEGER
            ,topo_geom MDSYS.SDO_TOPO_GEOMETRY
         )
      ]';
      
      MDSYS.SDO_TOPO.ADD_TOPO_GEOMETRY_LAYER(
          topology      => str_topology
         ,table_name    => str_topology || '_TOPO'
         ,column_name   => 'TOPO_GEOM'
         ,topo_geometry_layer_type => 'POLYGON'
      );
      
      MDSYS.SDO_TOPO.INITIALIZE_METADATA(
         topology => str_topology
      );

      obj_topo := dz_topology(
          p_topology_name    => str_topology 
         ,p_active_table     => str_topology || '_TOPO'
         ,p_active_column    => 'TOPO_GEOM'
      );    
                   
      obj_topo.set_topo_map_mgr(
         dz_topo_map_mgr(
             p_topology_name => str_topology
         )
      );     
      
      obj_topo.java_memory(p_gigs => 2);
         
      EXECUTE IMMEDIATE q'[
         SELECT 
          a.featureid
         ,a.shape
         FROM
         ]' || C_TEST_SCHEMA || q'[.nhdplus21_catchment_55059 a
         WHERE 
         a.featureid IN (
             14784009
            ,14784007
            ,14784003
            ,14783997
            ,14783991
            ,14783993
            ,14783977
            ,14783975
            ,14784191
            ,14783971
            ,14783965
            ,14784193
            ,14783981
            ,14783973
            ,14784001
            ,14783995
            ,14783979
            ,14783967
            ,14783999
            ,14783985
         )
         ORDER BY
         a.featureid
      ]'
      BULK COLLECT INTO ary_ids,ary_shape;
                
      FOR i IN 1 .. ary_ids.COUNT
      LOOP
         topo_shape := obj_topo.create_feature(
            ary_shape(i)
         );
           
         EXECUTE IMMEDIATE q'[             
            INSERT INTO ]' || str_topology || q'[_topo(
                featureid
               ,topo_geom
            ) VALUES (
                :p01
               ,:p02
            )
         ]' 
         USING
          ary_ids(i)
         ,topo_shape;
                        
      END LOOP;
                  
      obj_topo.commit_topo_map();
      COMMIT;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Topology test #2
      -- Aggregate all topo geoms into one polygon output
      --------------------------------------------------------------------------
      EXECUTE IMMEDIATE q'[
         SELECT 
          a.topo_geom
         FROM
         ]' || str_topology || q'[_topo a
      ]'
      BULK COLLECT INTO ary_topos;
      
      sdo_temp := dz_topo_main.aggregate_topology(
          p_topology_obj   => obj_topo
         ,p_input_topo     => ary_topos
      );
      
      num_area := MDSYS.SDO_GEOM.SDO_AREA(
          sdo_temp
         ,0.05
        ,'UNIT=SQ_KM'
      );
      
      IF ROUND(num_area,2) <>  45.08
      THEN
         dbms_output.put_line('Bad area results ' || num_area);
         num_results := num_results - 1;
         
      END IF;
      
      str_results := MDSYS.SDO_GEOM.VALIDATE_GEOMETRY_WITH_CONTEXT(
          sdo_temp
         ,0.05
      );
      
      IF str_results <> 'TRUE'
      THEN
         dbms_output.put_line('Bad geom validity ' || str_results);
         num_results := num_results - 1;
      
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Drop the test topology
      --------------------------------------------------------------------------
      dz_topo_main.nuke_topology(
          p_topology_name => str_topology 
      );
      
      RETURN num_results;
      
   END scratch_test;

END dz_topo_test;
/

