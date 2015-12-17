CREATE OR REPLACE TYPE dz_topo_layer FORCE
AUTHID CURRENT_USER
AS OBJECT (
    topology_owner    VARCHAR2(30 Char)
   ,topology_name     VARCHAR2(20 Char)
   ,topology_id       NUMBER
   ,topology_srid     NUMBER
   ,tolerance         NUMBER
   ,table_owner       VARCHAR2(30 Char)
   ,table_name        VARCHAR2(30 Char)
   ,column_name       VARCHAR2(30 Char)
   ,tg_layer_id       NUMBER
   ,tg_layer_type     VARCHAR2(255 Char)
   ,tg_layer_level    NUMBER
   ,topo_map_mgr      dz_topo_map_mgr
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,CONSTRUCTOR FUNCTION dz_topo_layer
    RETURN SELF AS RESULT
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,CONSTRUCTOR FUNCTION dz_topo_layer(
        p_table_name   IN  VARCHAR2
       ,p_column_name  IN  VARCHAR2
    ) RETURN SELF AS RESULT
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,CONSTRUCTOR FUNCTION dz_topo_layer(
        p_table_name   IN  VARCHAR2
       ,p_column_name  IN  VARCHAR2
       ,p_topo_map_mgr IN  dz_topo_map_mgr
    ) RETURN SELF AS RESULT
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,CONSTRUCTOR FUNCTION dz_topo_layer(
        p_topo_geom    IN  MDSYS.SDO_TOPO_GEOMETRY
    ) RETURN SELF AS RESULT
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,CONSTRUCTOR FUNCTION dz_topo_layer(
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
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION valid
    RETURN VARCHAR2
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER PROCEDURE harvest_topo_metadata
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION get_topo_map_mgr
    RETURN dz_topo_map_mgr
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER PROCEDURE set_topo_map_mgr(
      p_topo_map_mgr IN  dz_topo_map_mgr
    )
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER PROCEDURE java_memory(
        p_bytes     IN  NUMBER DEFAULT NULL
       ,p_gigs      IN  NUMBER DEFAULT NULL
    )
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION create_feature(
        self           IN OUT dz_topo_layer
       ,p_input        IN     MDSYS.SDO_GEOMETRY
    ) RETURN MDSYS.SDO_TOPO_GEOMETRY
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION add_polygon_to_layer(
        self           IN OUT dz_topo_layer
       ,p_input        IN     MDSYS.SDO_GEOMETRY
    ) RETURN MDSYS.SDO_TOPO_GEOMETRY
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER PROCEDURE commit_topo_map(
        p_validate        IN  VARCHAR2 DEFAULT 'TRUE'
       ,p_validate_level  IN  NUMBER   DEFAULT 1
    )
   
);
/

GRANT EXECUTE ON dz_topo_layer TO PUBLIC;

