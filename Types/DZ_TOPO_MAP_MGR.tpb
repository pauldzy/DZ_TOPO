CREATE OR REPLACE TYPE BODY dz_topo_map_mgr
AS

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   CONSTRUCTOR FUNCTION dz_topo_map_mgr
   RETURN SELF AS RESULT
   AS
   BEGIN
      RETURN;
      
   END dz_topo_map_mgr;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   CONSTRUCTOR FUNCTION dz_topo_map_mgr(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL
      ,p_topology_name   IN  VARCHAR2
      ,p_topo_map_name   IN  VARCHAR2 DEFAULT NULL
      ,p_allow_updates   IN  VARCHAR2 DEFAULT 'TRUE'
      ,p_number_of_edges IN  NUMBER   DEFAULT 100
      ,p_number_of_nodes IN  NUMBER   DEFAULT 80
      ,p_number_of_faces IN  NUMBER   DEFAULT 30
   ) RETURN SELF AS RESULT
   AS
   BEGIN
      
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_topology_name IS NULL
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topology name required');
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Set up the object
      --------------------------------------------------------------------------
      initialize_topo_basics(
          p_topology_owner  => p_topology_owner
         ,p_topology_name   => p_topology_name
         ,p_topo_map_name   => p_topo_map_name
         ,p_allow_updates   => p_allow_updates
         ,p_number_of_edges => p_number_of_edges
         ,p_number_of_nodes => p_number_of_nodes
         ,p_number_of_faces => p_number_of_faces
      );
      
      IF self.verify_topology() = 'FALSE'
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topology not found');
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Create new topo map 
      --------------------------------------------------------------------------
      self.create_topo_map();
      self.load_topo_map();
      
      RETURN;
      
   END dz_topo_map_mgr;
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   CONSTRUCTOR FUNCTION dz_topo_map_mgr(
       p_window_sdo      IN  MDSYS.SDO_GEOMETRY
      ,p_window_padding  IN  NUMBER   DEFAULT 0
      ,p_topology_owner  IN  VARCHAR2 DEFAULT NULL
      ,p_topology_name   IN  VARCHAR2
      ,p_topo_map_name   IN  VARCHAR2 DEFAULT NULL
      ,p_allow_updates   IN  VARCHAR2 DEFAULT 'TRUE'
      ,p_number_of_edges IN  NUMBER   DEFAULT 100
      ,p_number_of_nodes IN  NUMBER   DEFAULT 80
      ,p_number_of_faces IN  NUMBER   DEFAULT 30
   ) RETURN SELF AS RESULT
   AS
      num_window_padding NUMBER := p_window_padding;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_window_sdo IS NULL
      THEN
         RAISE_APPLICATION_ERROR(-20001,'sdo window required');
         
      END IF;
      
      IF p_topology_name IS NULL
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topology name required');
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Verify topology exists and is writeable
      --------------------------------------------------------------------------
      initialize_topo_basics(
          p_topology_owner  => p_topology_owner
         ,p_topology_name   => p_topology_name
         ,p_topo_map_name   => p_topo_map_name
         ,p_allow_updates   => p_allow_updates
         ,p_number_of_edges => p_number_of_edges
         ,p_number_of_nodes => p_number_of_nodes
         ,p_number_of_faces => p_number_of_faces
      );
      
      IF self.verify_topology() = 'FALSE'
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topology not found');
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Get MBR of input sdo and add padding.
      --------------------------------------------------------------------------
      IF num_window_padding IS NULL
      THEN
         num_window_padding := 0;
         
      END IF;
      
      self.topo_map_window := MDSYS.SDO_GEOM.SDO_MBR(
         geom => p_window_sdo
      );
      
      self.topo_map_window.SDO_ORDINATES(1) := 
         self.topo_map_window.SDO_ORDINATES(1) - num_window_padding;
         
      self.topo_map_window.SDO_ORDINATES(2) := 
         self.topo_map_window.SDO_ORDINATES(2) - num_window_padding;
         
      self.topo_map_window.SDO_ORDINATES(3) := 
         self.topo_map_window.SDO_ORDINATES(3) + num_window_padding;
         
      self.topo_map_window.SDO_ORDINATES(4) := 
         self.topo_map_window.SDO_ORDINATES(4) + num_window_padding;
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Create new topo map 
      --------------------------------------------------------------------------
      self.create_topo_map();
      self.load_topo_map();
      
      RETURN;
      
   END dz_topo_map_mgr;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   CONSTRUCTOR FUNCTION dz_topo_map_mgr(
       p_window_sdo      IN  MDSYS.SDO_GEOMETRY_ARRAY
      ,p_window_padding  IN  NUMBER   DEFAULT 0
      ,p_topology_owner  IN  VARCHAR2 DEFAULT NULL
      ,p_topology_name   IN  VARCHAR2
      ,p_topo_map_name   IN  VARCHAR2 DEFAULT NULL
      ,p_allow_updates   IN  VARCHAR2 DEFAULT 'TRUE'
      ,p_number_of_edges IN  NUMBER   DEFAULT 100
      ,p_number_of_nodes IN  NUMBER   DEFAULT 80
      ,p_number_of_faces IN  NUMBER   DEFAULT 30
   ) RETURN SELF AS RESULT
   AS
      num_window_padding NUMBER := p_window_padding;
      
   BEGIN
      
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_window_sdo IS NULL
      OR p_window_sdo.COUNT = 0
      THEN
         RAISE_APPLICATION_ERROR(-20001,'sdo window required');
         
      END IF;
      
      IF p_topology_name IS NULL
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topology name required');
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Verify topology exists and is writeable
      --------------------------------------------------------------------------
      initialize_topo_basics(
          p_topology_owner  => p_topology_owner
         ,p_topology_name   => p_topology_name
         ,p_topo_map_name   => p_topo_map_name
         ,p_allow_updates   => p_allow_updates
         ,p_number_of_edges => p_number_of_edges
         ,p_number_of_nodes => p_number_of_nodes
         ,p_number_of_faces => p_number_of_faces
      );
      
      IF self.verify_topology() = 'FALSE'
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topology not found');
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Get MBR of input sdo and add padding.
      --------------------------------------------------------------------------
      IF num_window_padding IS NULL
      THEN
         num_window_padding := 0;
         
      END IF;
      
      SELECT
      MDSYS.SDO_AGGR_MBR(
         MDSYS.SDO_GEOMETRY(
             a.sdo_gtype
            ,a.sdo_srid
            ,a.sdo_point
            ,a.sdo_elem_info
            ,a.sdo_ordinates
         )
      )
      INTO self.topo_map_window
      FROM
      TABLE(p_window_sdo) a;
      
      self.topo_map_window.SDO_ORDINATES(1) := 
         self.topo_map_window.SDO_ORDINATES(1) - num_window_padding;
         
      self.topo_map_window.SDO_ORDINATES(2) := 
         self.topo_map_window.SDO_ORDINATES(2) - num_window_padding;
         
      self.topo_map_window.SDO_ORDINATES(3) := 
         self.topo_map_window.SDO_ORDINATES(3) + num_window_padding;
         
      self.topo_map_window.SDO_ORDINATES(4) := 
         self.topo_map_window.SDO_ORDINATES(4) + num_window_padding;
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Create new topo map 
      --------------------------------------------------------------------------
      self.create_topo_map();
      self.load_topo_map();
      
      RETURN;
      
   END dz_topo_map_mgr;
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   CONSTRUCTOR FUNCTION dz_topo_map_mgr(
       p_xmin            IN  NUMBER
      ,p_ymin            IN  NUMBER
      ,p_xmax            IN  NUMBER
      ,p_ymax            IN  NUMBER
      ,p_topology_owner  IN  VARCHAR2 DEFAULT NULL
      ,p_topology_name   IN  VARCHAR2
      ,p_topo_map_name   IN  VARCHAR2 DEFAULT NULL
      ,p_allow_updates   IN  VARCHAR2 DEFAULT 'TRUE'
      ,p_number_of_edges IN  NUMBER   DEFAULT 100
      ,p_number_of_nodes IN  NUMBER   DEFAULT 80
      ,p_number_of_faces IN  NUMBER   DEFAULT 30
   ) RETURN SELF AS RESULT
   AS
   BEGIN
      
       --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_xmin IS NULL
      OR p_ymin IS NULL
      OR p_xmax IS NULL
      OR p_ymax IS NULL
      THEN
         RAISE_APPLICATION_ERROR(-20001,'sdo window coordiantes required');
         
      END IF;
      
      IF p_topology_name IS NULL
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topology name required');
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Verify topology exists and is writeable
      --------------------------------------------------------------------------
      initialize_topo_basics(
          p_topology_owner  => p_topology_owner
         ,p_topology_name   => p_topology_name
         ,p_topo_map_name   => p_topo_map_name
         ,p_allow_updates   => p_allow_updates
         ,p_number_of_edges => p_number_of_edges
         ,p_number_of_nodes => p_number_of_nodes
         ,p_number_of_faces => p_number_of_faces
      );
      
      IF self.verify_topology() = 'FALSE'
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topology not found');
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Get MBR of input sdo and add padding.
      --------------------------------------------------------------------------
      self.topo_map_window := MDSYS.SDO_GEOMETRY(
          2003
         ,NULL
         ,NULL
         ,MDSYS.SDO_ELEM_INFO_ARRAY(1,1003,3)
         ,MDSYS.SDO_ORDINATE_ARRAY(p_xmin,p_ymin,p_xmax,p_ymax)
      );
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Create new topo map 
      --------------------------------------------------------------------------
      self.create_topo_map();
      self.load_topo_map();
      
      RETURN;
      
   END dz_topo_map_mgr;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER PROCEDURE create_topo_map
   AS
      str_updateable_topo_map VARCHAR2(4000 Char);
      
   BEGIN
      
      /* This logic does not work due to some kind of bug  
      
      str_updateable_topo_map := self.current_updateable_topo_map();
   
      IF str_updateable_topo_map IS NOT NULL
      THEN
         MDSYS.SDO_TOPO_MAP.DROP_TOPO_MAP(
            topo_map => str_updateable_topo_map
         );
         
      END IF;
      
      As a result just drop all topo maps in the sesssion */
      
      self.purge_all_topo_maps();
      
      MDSYS.SDO_TOPO_MAP.CREATE_TOPO_MAP(
           topology        => self.topology_name
          ,topo_map        => self.topo_map_name
          ,number_of_edges => self.number_of_edges
          ,number_of_nodes => self.number_of_nodes
          ,number_of_faces => self.number_of_faces
      );
      
   END create_topo_map;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER PROCEDURE load_topo_map
   AS
   BEGIN
   
      IF self.verify_topo_map() <> 'VALID'
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topo map has not been created');
         
      END IF;
      
      IF self.topo_map_window IS NULL
      THEN
         MDSYS.SDO_TOPO_MAP.LOAD_TOPO_MAP(
             topo_map      => self.topo_map_name
            ,allow_updates => self.allow_updates
            ,build_indexes => 'TRUE'
         );
        
      ELSE
         MDSYS.SDO_TOPO_MAP.LOAD_TOPO_MAP(
              topo_map      => self.topo_map_name
             ,xmin          => self.topo_map_window.SDO_ORDINATES(1)
             ,ymin          => self.topo_map_window.SDO_ORDINATES(2)
             ,xmax          => self.topo_map_window.SDO_ORDINATES(3)
             ,ymax          => self.topo_map_window.SDO_ORDINATES(4)
             ,allow_updates => self.allow_updates
             ,build_indexes => 'TRUE'
          );
      
      END IF;
       
   END load_topo_map;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION create_feature(
       p_table_name       IN  VARCHAR2
      ,p_column_name      IN  VARCHAR2
      ,p_geometry         IN  MDSYS.SDO_GEOMETRY
   ) RETURN MDSYS.SDO_TOPO_GEOMETRY
   AS
      topo_output MDSYS.SDO_TOPO_GEOMETRY;
      
   BEGIN
   
      topo_output := MDSYS.SDO_TOPO_MAP.CREATE_FEATURE(
          self.topology_name 
         ,p_table_name
         ,p_column_name
         ,p_geometry
      );
      
      RETURN topo_output;
   
   END create_feature;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER PROCEDURE commit_topo_map(
       p_validate        IN  VARCHAR2 DEFAULT 'TRUE'
      ,p_validate_level  IN  NUMBER DEFAULT 1
   )
   AS
      str_validate VARCHAR2(4000 Char);
      
   BEGIN
      
      IF self.verify_topo_map() <> 'VALID'
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topo map has not been created');
         
      END IF;
      
      IF p_validate = 'TRUE'
      THEN
         str_validate := self.validate_topo_map(
            p_validate_level => p_validate_level
         );
         
         IF str_validate <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'topo map does not validate');
            
         END IF;
         
      END IF; 
      
      MDSYS.SDO_TOPO_MAP.COMMIT_TOPO_MAP;
      
   END commit_topo_map;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION validate_topo_map(
      p_validate_level  IN  NUMBER DEFAULT 1
   ) RETURN VARCHAR2
   AS
   BEGIN
   
      RETURN MDSYS.SDO_TOPO_MAP.VALIDATE_TOPO_MAP(
          self.topo_map_name
         ,p_validate_level
      );
      
   END validate_topo_map;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER PROCEDURE drop_topo_map
   AS
   BEGIN
   
      IF self.verify_topo_map() = 'VALID'
      THEN
         MDSYS.SDO_TOPO_MAP.DROP_TOPO_MAP(
            topo_map => self.topology_name
         );
         
      END IF;
         
   END drop_topo_map;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER PROCEDURE clear_topo_map
   AS
   BEGIN
   
      MDSYS.SDO_TOPO_MAP.CLEAR_TOPO_MAP(
         topo_map => self.topo_map_name
      );
      
   END clear_topo_map;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER PROCEDURE java_memory(
       p_bytes     IN  NUMBER DEFAULT NULL
      ,p_gigs      IN  NUMBER DEFAULT NULL
   )
   AS
      num_bytes NUMBER := p_bytes;
      
   BEGIN
   
      IF num_bytes IS NULL
      THEN
         num_bytes := p_gigs * 1073741824;
         
      END IF;   
   
      MDSYS.SDO_TOPO_MAP.SET_MAX_MEMORY_SIZE(
         num_bytes
      );
   
   END java_memory;
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER PROCEDURE create_indexes
   AS
   BEGIN
    
      MDSYS.SDO_TOPO_MAP.CREATE_EDGE_INDEX(
         self.topo_map_name
      );
      MDSYS.SDO_TOPO_MAP.CREATE_FACE_INDEX(
         self.topo_map_name
      );
      
   END create_indexes;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION get_topology_owner
   RETURN VARCHAR2
   AS
   BEGIN
      RETURN self.topology_owner;
      
   END get_topology_owner;
     
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION get_topology_name
   RETURN VARCHAR2
   AS
   BEGIN
      RETURN self.topology_name;
      
   END get_topology_name;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION verify_topology
   RETURN VARCHAR
   AS
      int_counter PLS_INTEGER;
      
   BEGIN
      
      IF self.topology_owner = USER
      OR self.allow_updates = 'FALSE'
      THEN
         SELECT
         COUNT(*)
         INTO int_counter
         FROM
         all_tables a
         WHERE
             a.owner = self.topology_owner
         AND a.table_name IN (
             self.topology_name || '_FACE$'
            ,self.topology_name || '_NODE$'
            ,self.topology_name || '_EDGE$'
            ,self.topology_name || '_RELATION$'
         );
      
      ELSE
         RAISE_APPLICATION_ERROR(-20001,'updateable nonowner check not implemented');
         -- Add logic against all_tab_privs to check that nonowner has insert, update, etc
         -- on topology
      
      END IF;
      
      IF int_counter = 4
      THEN
         RETURN 'TRUE';
         
      ELSE
         RETURN 'FALSE';
         
      END IF;
   
   END verify_topology;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION current_updateable_topo_map
   RETURN VARCHAR2
   AS
      ary_topo_maps MDSYS.SDO_STRING2_ARRAY;
      ary_parts     MDSYS.SDO_STRING2_ARRAY;
      
   BEGIN
     
      ary_topo_maps := self.get_topo_maps();
   
      FOR i IN 1 .. ary_topo_maps.COUNT
      LOOP
         ary_parts := dz_topo_util.gz_split(
             p_str   => ary_topo_maps(i)
            ,p_regex => ','
            ,p_trim  => 'TRUE'
         );
         
         IF LOWER(ary_parts(3)) IN ('updatable','updateable')
         THEN
            RETURN ary_parts(1);
         
         END IF;
         
      END LOOP;
   
      RETURN NULL;
   
   END current_updateable_topo_map;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION get_topo_maps
   RETURN MDSYS.SDO_STRING2_ARRAY
   AS
      str_topo_maps VARCHAR2(4000 Char);
      ary_topo_maps MDSYS.SDO_STRING2_ARRAY;
      
   BEGIN
   
      str_topo_maps := MDSYS.SDO_TOPO_MAP.LIST_TOPO_MAPS();
      --dbms_output.put_line(MDSYS.SDO_TOPO_MAP.LIST_TOPO_MAPS());
      
      IF str_topo_maps IS NULL
      THEN
         RETURN NULL;
         
      END IF;
      
      ary_topo_maps := dz_topo_util.gz_split(
          p_str   => str_topo_maps
         ,p_regex => '\)\, \('
         ,p_trim  => 'TRUE'
      );
      
      FOR i IN 1 .. ary_topo_maps.COUNT
      LOOP
         ary_topo_maps(i) := REPLACE(ary_topo_maps(i),'(','');
         ary_topo_maps(i) := REPLACE(ary_topo_maps(i),')','');
         --dbms_output.put_line(ary_topo_maps(i));
      
      END LOOP;
      
      RETURN ary_topo_maps;
   
   END get_topo_maps;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER PROCEDURE purge_all_topo_maps
   AS
      ary_topo_maps MDSYS.SDO_STRING2_ARRAY;
      ary_parts     MDSYS.SDO_STRING2_ARRAY;
      
   BEGIN
   
      ary_topo_maps := self.get_topo_maps();
      
      IF ary_topo_maps IS NULL
      OR ary_topo_maps.COUNT = 0
      THEN
         RETURN;
         
      END IF;
      
      FOR i IN 1 .. ary_topo_maps.COUNT
      LOOP
         ary_parts := dz_topo_util.gz_split(
             p_str   => ary_topo_maps(i)
            ,p_regex => ','
            ,p_trim  => 'TRUE'
         );
         
         MDSYS.SDO_TOPO_MAP.DROP_TOPO_MAP(
            topo_map => ary_parts(1)
         );
         
      END LOOP;
      
   END purge_all_topo_maps;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION verify_topo_map
   RETURN VARCHAR2
   AS
      str_topomaps   VARCHAR2(4000 Char);

   BEGIN
   
      str_topomaps := MDSYS.SDO_TOPO_MAP.LIST_TOPO_MAPS();
      
      IF str_topomaps IS NULL
      OR INSTR(str_topomaps,self.topo_map_name) = 0
      THEN
         RETURN 'INVALID';
         
      ELSE
         RETURN 'VALID';
         
      END IF;
      
   END verify_topo_map;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER PROCEDURE initialize_topo_basics(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL
      ,p_topology_name   IN  VARCHAR2
      ,p_topo_map_name   IN  VARCHAR2 DEFAULT NULL
      ,p_allow_updates   IN  VARCHAR2 DEFAULT 'TRUE'
      ,p_number_of_edges IN  NUMBER   DEFAULT 100
      ,p_number_of_nodes IN  NUMBER   DEFAULT 80
      ,p_number_of_faces IN  NUMBER   DEFAULT 30
   )
   AS
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Verify topology exists and is writeable
      --------------------------------------------------------------------------
      IF p_topology_owner IS NULL
      THEN
         self.topology_owner := USER;
         
      ELSE
         self.topology_owner := p_topology_owner;
         
      END IF;
      
      self.topology_name := p_topology_name;
      
      IF p_allow_updates IS NULL
      THEN
         self.allow_updates := 'TRUE';
         
      ELSE
         self.allow_updates := p_allow_updates;
         
      END IF;
      
      IF p_topo_map_name IS NULL
      THEN
         self.topo_map_name := 'DZ' || upper(RAWTOHEX(SYS_GUID()));
         
      ELSE
         self.topo_map_name := p_topo_map_name;
         
      END IF;
      
      IF p_number_of_edges IS NULL
      THEN
         self.number_of_edges := 100;
         
      ELSE
         self.number_of_edges := p_number_of_edges;
         
      END IF;
      
      IF p_number_of_nodes IS NULL
      THEN
         self.number_of_nodes := 80;
         
      ELSE
         self.number_of_nodes := p_number_of_nodes;
         
      END IF;
      
      IF p_number_of_faces IS NULL
      THEN
         self.number_of_faces := 30;
         
      ELSE
         self.number_of_faces := p_number_of_faces;
         
      END IF;
      
   END initialize_topo_basics;
   
END;
/

