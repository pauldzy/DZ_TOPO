CREATE OR REPLACE TYPE BODY dz_topology
AS

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   CONSTRUCTOR FUNCTION dz_topology
   RETURN SELF AS RESULT
   AS
   BEGIN
      RETURN;
   
   END dz_topology;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   CONSTRUCTOR FUNCTION dz_topology(
       p_topology_owner IN  VARCHAR2
      ,p_topology_name  IN  VARCHAR2
   ) RETURN SELF AS RESULT
   AS
   BEGIN
      
      self.topology_owner := p_topology_owner;
      self.topology_name  := p_topology_name;
      
      self.harvest_topo_metadata();
      
      RETURN;
      
   END dz_topology;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   CONSTRUCTOR FUNCTION dz_topology(
       p_topology_name IN  VARCHAR2
   ) RETURN SELF AS RESULT
   AS
   BEGIN
   
      self.topology_name  := p_topology_name;
      
      self.harvest_topo_metadata();
      
      RETURN;
      
   END dz_topology;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   CONSTRUCTOR FUNCTION dz_topology(
       p_topo_geom     IN  MDSYS.SDO_TOPO_GEOMETRY
   ) RETURN SELF AS RESULT
   AS
   BEGIN
      self.topology_id := p_topo_geom.topology_id;
      
      self.harvest_topo_metadata();
      
      self.set_active_topo_layer(p_topo_geom.tg_id);
      
      RETURN;
   
   END dz_topology;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   CONSTRUCTOR FUNCTION dz_topology(
       p_topology_name   IN  VARCHAR2
      ,p_active_table    IN  VARCHAR2
      ,p_active_column   IN  VARCHAR2
   ) RETURN SELF AS RESULT
   AS
      num_tg_id NUMBER;
      
   BEGIN
   
      self.topology_name  := p_topology_name;
      
      self.harvest_topo_metadata();
      
      FOR i IN 1 .. self.topo_layers.COUNT
      LOOP
         IF  self.topo_layers(i).table_name = p_active_table
         AND self.topo_layers(i).column_name = p_active_column
         THEN
            num_tg_id := i;
         
         END IF;
      
      END LOOP;
      
      IF num_tg_id IS NULL
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topo layer not found');
      
      END IF;
      
      self.set_active_topo_layer(num_tg_id);
      
      RETURN;
      
   END dz_topology;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   CONSTRUCTOR FUNCTION dz_topology(
       p_topology_name   IN  VARCHAR2
      ,p_active_tg_id    IN  NUMBER
   ) RETURN SELF AS RESULT
   AS
   BEGIN
   
      self.topology_name  := p_topology_name;
      
      self.harvest_topo_metadata();
      
      self.set_active_topo_layer(p_active_tg_id);
      
      RETURN;
   
   END dz_topology;
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER PROCEDURE harvest_topo_metadata
   AS
      str_sql    VARCHAR2(4000 Char);
      
   BEGIN
   
      IF self.topology_name IS NOT NULL
      THEN
         IF self.topology_owner IS NULL
         THEN
            self.topology_owner := USER;
      
         END IF;
      
         str_sql := 'SELECT ' 
                 || dz_topo_util.c_schema || '.dz_topo_layer( '
                 || '    p_topology_owner    => a.owner '
                 || '   ,p_topology_name     => a.topology '
                 || '   ,p_topology_id       => a.topology_id '
                 || '   ,p_topology_srid     => a.srid '
                 || '   ,p_tolerance         => a.tolerance '
                 || '   ,p_table_owner       => a.table_owner '
                 || '   ,p_table_name        => a.table_name '
                 || '   ,p_column_name       => a.column_name '
                 || '   ,p_tg_layer_id       => a.tg_layer_id '
                 || '   ,p_tg_layer_type     => a.tg_layer_type '
                 || '   ,p_tg_layer_level    => a.tg_layer_level '
                 || ') '
                 || 'FROM '
                 || 'all_sdo_topo_metadata a '
                 || 'WHERE '
                 || '    a.owner = :p01 '
                 || 'AND a.topology  = :p02 ';
         
         EXECUTE IMMEDIATE str_sql
         BULK COLLECT INTO self.topo_layers
         USING self.topology_owner,self.topology_name;
         
      ELSIF self.topology_id IS NOT NULL
      THEN
         str_sql := 'SELECT ' 
                 || dz_topo_util.c_schema || '.dz_topo_layer( '
                 || '    p_topology_owner    => a.owner '
                 || '   ,p_topology_name     => a.topology '
                 || '   ,p_topology_id       => a.topology_id '
                 || '   ,p_topology_srid     => a.srid '
                 || '   ,p_tolerance         => a.tolerance '
                 || '   ,p_table_owner       => a.table_owner '
                 || '   ,p_table_name        => a.table_name '
                 || '   ,p_column_name       => a.column_name '
                 || '   ,p_tg_layer_id       => a.tg_layer_id '
                 || '   ,p_tg_layer_type     => a.tg_layer_type '
                 || '   ,p_tg_layer_level    => a.tg_layer_level '
                 || ') '
                 || 'FROM '
                 || 'all_sdo_topo_metadata a '
                 || 'WHERE '
                 || 'a.topology_id = :p01 ';
                 
         EXECUTE IMMEDIATE str_sql
         BULK COLLECT INTO self.topo_layers
         USING self.topology_id;
      
      END IF;
      
      IF self.topo_layers IS NOT NULL
      AND self.topo_layers.COUNT > 0
      THEN
         self.topology_owner    := self.topo_layers(1).topology_owner;
         self.topology_name     := self.topo_layers(1).topology_name;
         self.topology_id       := self.topo_layers(1).topology_id;
         self.topology_srid     := self.topo_layers(1).topology_srid;
         self.tolerance         := self.topo_layers(1).tolerance;
         self.layer_count       := self.topo_layers.COUNT;
      
      ELSE
         self.topo_layers := NULL;
         
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
   MEMBER PROCEDURE set_active_topo_layer(
     p_tg_layer_id  IN  NUMBER
   )
   AS
   BEGIN
   
      IF self.topo_layers IS NULL
      OR self.topo_layers.COUNT = 0
      THEN
         RETURN;
      END IF;
      
      FOR i IN 1 .. self.topo_layers.COUNT
      LOOP
         IF self.topo_layers(i).tg_layer_id = p_tg_layer_id
         THEN
            self.active_layer_index := i;
         
         END IF;
      
      END LOOP;
   
   END set_active_topo_layer;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION valid
   RETURN VARCHAR2
   AS
   BEGIN
      IF self.topology_name IS NOT NULL
      AND self.topology_id IS NOT NULL
      THEN
         RETURN 'TRUE';
      ELSE
         RETURN 'FALSE';
      END IF;   
   
   END valid;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION topo_layer(
      p_tg_layer_id  IN  NUMBER DEFAULT NULL
   ) RETURN dz_topo_layer
   AS
      num_layer_index  NUMBER;
      
   BEGIN
   
      IF  self.topo_layers IS NULL
      AND self.topo_layers.COUNT = 0
      THEN
         RETURN NULL;
      
      END IF;
      
      IF  p_tg_layer_id IS NULL
      AND self.active_layer_index IS NULL
      THEN
         RAISE_APPLICATION_ERROR(-20001,'no active layer set');
      
      END IF;
      
      IF p_tg_layer_id IS NULL
      THEN
         num_layer_index := self.active_layer_index;
      
      ELSE
         FOR i IN 1 .. self.topo_layers.COUNT
         LOOP
            IF self.topo_layers(i).tg_layer_id = p_tg_layer_id
            THEN
               num_layer_index := i;
            
            END IF;
         
         END LOOP;
         
         IF  num_layer_index IS NULL
         THEN
            RAISE_APPLICATION_ERROR(-20001,'tg layer id not found');
         
         END IF;
      
      END IF;
      
      RETURN self.topo_layers(num_layer_index);
         
   END topo_layer;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION get_topo_map_mgr
   RETURN dz_topo_map_mgr
   AS
   BEGIN
   
      IF self.topo_layer() IS NOT NULL
      THEN
         RETURN self.topo_layer().get_topo_map_mgr();
         
      ELSE
         RAISE_APPLICATION_ERROR(-20001,'topo layer not active');
         
      END IF;
   
   END get_topo_map_mgr;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER PROCEDURE set_topo_map_mgr(
     p_topo_map_mgr IN  dz_topo_map_mgr
   )
   AS
   BEGIN
   
      IF self.topo_layer() IS NOT NULL
      THEN
         self.topo_layers(self.active_layer_index).set_topo_map_mgr(p_topo_map_mgr);
         
      ELSE
         RAISE_APPLICATION_ERROR(-20001,'topo layer not active');
         
      END IF;
   
   END set_topo_map_mgr;
   
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
   MEMBER FUNCTION node_table
   RETURN VARCHAR2
   AS
   BEGIN
      RETURN self.topology_owner || '.'
      || self.topology_name
      || '_NODE$';
      
   END node_table;
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION edge_table
   RETURN VARCHAR2
   AS
   BEGIN
      RETURN self.topology_owner || '.'
      || self.topology_name
      || '_EDGE$';
      
   END edge_table;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION edge_table_name
   RETURN VARCHAR2
   AS
   BEGIN
      RETURN self.topology_name
      || '_EDGE$';
      
   END edge_table_name;
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION face_table
   RETURN VARCHAR2
   AS
   BEGIN
      RETURN self.topology_owner || '.'
      || self.topology_name
      || '_FACE$';
      
   END face_table;
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION relation_table
   RETURN VARCHAR2
   AS
   BEGIN
      RETURN self.topology_owner || '.'
      || self.topology_name
      || '_RELATION$';
      
   END relation_table;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION exp_table
   RETURN VARCHAR2
   AS
   BEGIN
      RETURN self.topology_owner || '.'
      || self.topology_name
      || '_EXP$';
      
   END exp_table;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION active_topo_table
   RETURN VARCHAR2
   AS
   
   BEGIN
   
      IF self.active_layer_index IS NULL
      OR self.topo_layers IS NULL
      OR self.topo_layers.COUNT = 0
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topo layer not ready');
      
      END IF;
      
      RETURN self.topo_layers(self.active_layer_index).table_name;
   
   END active_topo_table;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION active_topo_column
   RETURN VARCHAR2
   AS
   
   BEGIN
   
      IF self.active_layer_index IS NULL
      OR self.topo_layers IS NULL
      OR self.topo_layers.COUNT = 0
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topo layer not ready');
      
      END IF;
      
      RETURN self.topo_layers(self.active_layer_index).column_name;
   
   END active_topo_column;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION create_feature(
       self           IN OUT dz_topology
      ,p_input        IN     MDSYS.SDO_GEOMETRY
   ) RETURN MDSYS.SDO_TOPO_GEOMETRY
   AS
   BEGIN
   
      IF self.active_layer_index IS NULL
      OR self.topo_layers IS NULL
      OR self.topo_layers.COUNT = 0
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topo layer not ready');
      
      END IF;
      
      RETURN self.topo_layers(
         self.active_layer_index
      ).create_feature(
         p_input => p_input
      );
   
   END create_feature;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION add_polygon_to_layer(
       self           IN OUT dz_topology
      ,p_input        IN     MDSYS.SDO_GEOMETRY
   ) RETURN MDSYS.SDO_TOPO_GEOMETRY
   AS
   BEGIN
   
      IF self.active_layer_index IS NULL
      OR self.topo_layers IS NULL
      OR self.topo_layers.COUNT = 0
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topo layer not ready');
      
      END IF;
      
      RETURN self.topo_layers(self.active_layer_index).add_polygon_to_layer(
         p_input => p_input
      );
   
   END add_polygon_to_layer;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER PROCEDURE commit_topo_map(
       p_validate        IN  VARCHAR2 DEFAULT 'TRUE'
      ,p_validate_level  IN  NUMBER   DEFAULT 1
   )
   AS
   BEGIN
   
      IF self.topo_layer() IS NULL
      OR self.topo_layers(self.active_layer_index).topo_map_mgr IS NULL
      OR self.topo_layers(self.active_layer_index).topo_map_mgr.allow_updates <> 'TRUE'
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topo map mgr invalid');
         
      END IF;
      
      self.topo_layers(self.active_layer_index).topo_map_mgr.commit_topo_map(
          p_validate        => p_validate
         ,p_validate_level  => p_validate_level
      );
   
   END commit_topo_map;
   
END;
/

