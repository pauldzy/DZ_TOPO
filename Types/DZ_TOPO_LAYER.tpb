CREATE OR REPLACE TYPE BODY dz_topo_layer
AS

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   CONSTRUCTOR FUNCTION dz_topo_layer
   RETURN SELF AS RESULT
   AS
   BEGIN
      RETURN;
   
   END dz_topo_layer;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   CONSTRUCTOR FUNCTION dz_topo_layer(
       p_table_name   IN  VARCHAR2
      ,p_column_name  IN  VARCHAR2
   ) RETURN SELF AS RESULT
   AS
   BEGIN
      
      self.table_name := p_table_name;
      self.column_name := p_column_name;
      
      self.harvest_topo_metadata();
      
      RETURN;
      
   END dz_topo_layer;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   CONSTRUCTOR FUNCTION dz_topo_layer(
       p_table_name   IN  VARCHAR2
      ,p_column_name  IN  VARCHAR2
      ,p_topo_map_mgr IN  dz_topo_map_mgr
   ) RETURN SELF AS RESULT
   AS
   BEGIN
      self.table_name := p_table_name;
      self.column_name := p_column_name;
      
      self.harvest_topo_metadata();
      
      self.topo_map_mgr := p_topo_map_mgr;
      
      RETURN;
      
   END dz_topo_layer;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   CONSTRUCTOR FUNCTION dz_topo_layer(
      p_topo_geom    IN  MDSYS.SDO_TOPO_GEOMETRY
   ) RETURN SELF AS RESULT
   AS
   BEGIN
      self.tg_layer_id  := p_topo_geom.tg_id;
      self.topology_id  := p_topo_geom.topology_id;
      
      self.harvest_topo_metadata();
      
      RETURN;
      
   END dz_topo_layer;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   CONSTRUCTOR FUNCTION dz_topo_layer(
       p_topology_owner    IN  VARCHAR2
      ,p_topology_name     IN  VARCHAR2
      ,p_topology_id       IN  NUMBER
      ,p_topology_srid     IN  NUMBER
      ,p_tolerance         IN  NUMBER
      ,p_table_owner       IN  VARCHAR2
      ,p_table_name        IN  VARCHAR2
      ,p_column_name       IN  VARCHAR2
      ,p_tg_layer_id       IN  NUMBER
      ,p_tg_layer_type     IN  VARCHAR2
      ,p_tg_layer_level    IN  NUMBER
   ) RETURN SELF AS RESULT
   AS
   BEGIN
      self.topology_owner    := p_topology_owner;
      self.topology_name     := p_topology_name;
      self.topology_id       := p_topology_id;
      self.topology_srid     := p_topology_srid;
      self.tolerance         := p_tolerance;
      self.table_owner       := p_table_owner;
      self.table_name        := p_table_name;
      self.column_name       := p_column_name;
      self.tg_layer_id       := p_tg_layer_id;
      self.tg_layer_type     := p_tg_layer_type;
      self.tg_layer_level    := p_tg_layer_level;
      
      RETURN;
   
   END dz_topo_layer;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION valid
   RETURN VARCHAR2
   AS
   BEGIN
      IF self.table_name IS NULL
      OR self.tg_layer_id IS NULL
      THEN
         RETURN 'FALSE';
         
      ELSE
         RETURN 'TRUE';
         
      END IF;
      
   END valid;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION get_topo_map_mgr
   RETURN dz_topo_map_mgr
   AS
   BEGIN
   
      IF self.topo_map_mgr IS NOT NULL
      THEN
         RETURN self.topo_map_mgr;
         
      ELSE
         RAISE_APPLICATION_ERROR(-20001,'topo map mgr not active');
         
      END IF;
   
   END get_topo_map_mgr;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER PROCEDURE set_topo_map_mgr(
     p_topo_map_mgr  IN  dz_topo_map_mgr
   )
   AS
   BEGIN
   
      self.topo_map_mgr := p_topo_map_mgr;
   
   END set_topo_map_mgr;
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER PROCEDURE harvest_topo_metadata
   AS
   BEGIN
      
      IF self.table_name IS NOT NULL
      AND self.column_name IS NOT NULL
      THEN
         SELECT
          a.owner
         ,a.topology
         ,a.topology_id
         ,a.srid
         ,a.tolerance
         ,a.table_owner
         ,a.tg_layer_id
         ,a.tg_layer_type
         ,a.tg_layer_level
         INTO
          self.topology_owner
         ,self.topology_name
         ,self.topology_id
         ,self.topology_srid
         ,self.tolerance
         ,self.table_owner
         ,self.tg_layer_id
         ,self.tg_layer_type
         ,self.tg_layer_level
         FROM
         all_sdo_topo_metadata a
         WHERE
             a.table_name = self.table_name
         AND a.column_name = self.column_name;
         
      ELSIF self.tg_layer_id IS NOT NULL
      AND self.topology_id IS NOT NULL
      THEN
      
         SELECT
          a.owner
         ,a.topology
         ,a.topology_id
         ,a.srid
         ,a.tolerance
         ,a.table_owner
         ,a.tg_layer_id
         ,a.tg_layer_type
         ,a.tg_layer_level
         INTO
          self.topology_owner
         ,self.topology_name
         ,self.topology_id
         ,self.topology_srid
         ,self.tolerance
         ,self.table_owner
         ,self.tg_layer_id
         ,self.tg_layer_type
         ,self.tg_layer_level
         FROM
         all_sdo_topo_metadata a
         WHERE
             a.topology_id = self.topology_id
         AND a.tg_layer_id = self.tg_layer_id;
      
      END IF;
   
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         NULL;
         
      WHEN OTHERS
      THEN
         RAISE;
      
   END harvest_topo_metadata;
   
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
   
      MDSYS.SDO_TOPO_MAP.SET_MAX_MEMORY_SIZE(num_bytes);
   
   END java_memory;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION create_feature(
       self           IN OUT dz_topo_layer
      ,p_input        IN     MDSYS.SDO_GEOMETRY
   ) RETURN MDSYS.SDO_TOPO_GEOMETRY
   AS
      topo_output MDSYS.SDO_TOPO_GEOMETRY;
      
   BEGIN
   
      IF self.topo_map_mgr IS NULL
      OR self.topo_map_mgr.allow_updates <> 'TRUE'
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topo map mgr not active');
         
      END IF;
      
      topo_output := self.topo_map_mgr.create_feature(
          p_table_name    => self.table_name
         ,p_column_name   => self.column_name
         ,p_geometry      => p_input
      );
      
      RETURN topo_output;
   
   END create_feature;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION add_polygon_to_layer(
       self           IN OUT dz_topo_layer
      ,p_input        IN     MDSYS.SDO_GEOMETRY
   ) RETURN MDSYS.SDO_TOPO_GEOMETRY
   AS
       obj_output   MDSYS.SDO_TOPO_GEOMETRY;
       ary_number   MDSYS.SDO_NUMBER_ARRAY;
       ary_topo_obj MDSYS.SDO_TOPO_OBJECT_ARRAY;
       
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check that topology layer is layer 0
      --------------------------------------------------------------------------
      IF self.tg_layer_id IS NULL
      THEN
         self.harvest_topo_metadata();
         IF self.tg_layer_id IS NULL
         THEN
            RAISE_APPLICATION_ERROR(-20001,'topology layer invalid');
            
         END IF;
         
      END IF;
      
      IF self.tg_layer_level <> 0
      THEN
         RAISE_APPLICATION_ERROR(-20001,'error topo hierarchy support not implemented');
      
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Check that topology layer is polygon
      --------------------------------------------------------------------------
      IF self.tg_layer_type <> 'POLYGON'
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topology layer is not polygon layer');
      
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Check that topo map object is ready
      --------------------------------------------------------------------------
      IF self.topo_map_mgr IS NULL
      OR self.topo_map_mgr.topo_map_name IS NULL
      OR self.topo_map_mgr.allow_updates <> 'TRUE'
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topo map not initialized');
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Check incoming srid 
      --------------------------------------------------------------------------
      IF p_input.SDO_SRID <> self.topology_srid
      THEN
         RAISE_APPLICATION_ERROR(-20001,'error input polygon does not match topo srid');
      
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 50
      -- Validate incoming polygon 
      --------------------------------------------------------------------------

      
      --------------------------------------------------------------------------
      -- Step 60
      -- Add polygon to topology
      --------------------------------------------------------------------------
      ary_number := MDSYS.SDO_TOPO_MAP.ADD_POLYGON_GEOMETRY(
          topology => NULL
         ,polygon  => p_input
      );
      
      --------------------------------------------------------------------------
      -- Step 70
      -- Just update the topo map 
      --------------------------------------------------------------------------
      MDSYS.SDO_TOPO_MAP.UPDATE_TOPO_MAP();
      
      --------------------------------------------------------------------------
      -- Step 80
      -- Convert face ids to topo objects
      --------------------------------------------------------------------------
      ary_topo_obj := MDSYS.SDO_TOPO_OBJECT_ARRAY();
      ary_topo_obj.EXTEND(ary_number.COUNT);
      FOR i IN 1 .. ary_number.COUNT
      LOOP
         ary_topo_obj(i) := MDSYS.SDO_TOPO_OBJECT(
             topo_id   => ary_number(i)
            ,topo_type => 3
         );
      
      END LOOP;
      
      --------------------------------------------------------------------------
      -- Step 90
      -- Build the output
      --------------------------------------------------------------------------
      obj_output := MDSYS.SDO_TOPO_GEOMETRY(
          topology    => self.topology_name
         ,tg_type     => 3
         ,tg_layer_id => self.tg_layer_id
         ,topo_ids    => ary_topo_obj
         
      );
      
      --------------------------------------------------------------------------
      -- Step 100
      -- Return what we got
      --------------------------------------------------------------------------  
      RETURN obj_output;
      
   END add_polygon_to_layer; 
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER PROCEDURE commit_topo_map(
       p_validate        IN  VARCHAR2 DEFAULT 'TRUE'
      ,p_validate_level  IN  NUMBER   DEFAULT 1
   )
   AS
   BEGIN
   
      IF self.topo_map_mgr IS NULL
      OR self.topo_map_mgr.allow_updates <> 'TRUE'
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topo map mgr not active');
         
      END IF;
      
      self.topo_map_mgr.commit_topo_map(
          p_validate        => p_validate
         ,p_validate_level  => p_validate_level
      );
   
   END commit_topo_map;

END;
/

