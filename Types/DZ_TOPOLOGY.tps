CREATE OR REPLACE TYPE dz_topology FORCE
AUTHID CURRENT_USER
AS OBJECT (
    topology_owner     VARCHAR2(30 Char)
   ,topology_name      VARCHAR2(20 Char)
   ,topology_id        NUMBER
   ,topology_srid      NUMBER
   ,tolerance          NUMBER
   ,layer_count        NUMBER
   ,active_layer_index NUMBER
   ,topo_layers        dz_topo_layer_list
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,CONSTRUCTOR FUNCTION dz_topology
    RETURN SELF AS RESULT
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,CONSTRUCTOR FUNCTION dz_topology(
        p_topology_owner  IN  VARCHAR2
       ,p_topology_name   IN  VARCHAR2
    ) RETURN SELF AS RESULT
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,CONSTRUCTOR FUNCTION dz_topology(
        p_topology_name   IN  VARCHAR2
    ) RETURN SELF AS RESULT
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,CONSTRUCTOR FUNCTION dz_topology(
        p_topology_name   IN  VARCHAR2
       ,p_active_table    IN  VARCHAR2
       ,p_active_column   IN  VARCHAR2
    ) RETURN SELF AS RESULT
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,CONSTRUCTOR FUNCTION dz_topology(
        p_topology_name   IN  VARCHAR2
       ,p_active_tg_id    IN  NUMBER
    ) RETURN SELF AS RESULT
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,CONSTRUCTOR FUNCTION dz_topology(
        p_topo_geom     IN  MDSYS.SDO_TOPO_GEOMETRY
    ) RETURN SELF AS RESULT
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER PROCEDURE harvest_topo_metadata
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER PROCEDURE set_active_topo_layer(
      p_tg_layer_id  IN  NUMBER
    )
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION valid
    RETURN VARCHAR2
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION topo_layer(
      p_tg_layer_id  IN  NUMBER DEFAULT NULL
    ) RETURN dz_topo_layer
    
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
   ,MEMBER FUNCTION node_table
    RETURN VARCHAR2
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION edge_table
    RETURN VARCHAR2
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION edge_table_name
    RETURN VARCHAR2
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION face_table
    RETURN VARCHAR2
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION relation_table
    RETURN VARCHAR2
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION exp_table
    RETURN VARCHAR2
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION active_topo_table
    RETURN VARCHAR2
    
    -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION active_topo_column
    RETURN VARCHAR2
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION create_feature(
        self           IN OUT dz_topology
       ,p_input        IN     MDSYS.SDO_GEOMETRY
    ) RETURN MDSYS.SDO_TOPO_GEOMETRY
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION add_polygon_to_layer(
        self           IN OUT dz_topology
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

GRANT EXECUTE ON dz_topology TO PUBLIC;

