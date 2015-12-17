CREATE OR REPLACE TYPE dz_topo_map_mgr FORCE
AUTHID CURRENT_USER
AS OBJECT (
    topo_map_name     VARCHAR2(4000 Char)
   ,allow_updates     VARCHAR2(4000 Char)
   ,topology_owner    VARCHAR2(30 Char)
   ,topology_name     VARCHAR2(20 Char)
   ,topo_map_window   MDSYS.SDO_GEOMETRY
   ,number_of_edges   NUMBER
   ,number_of_nodes   NUMBER
   ,number_of_faces   NUMBER
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,CONSTRUCTOR FUNCTION dz_topo_map_mgr
    RETURN SELF AS RESULT
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,CONSTRUCTOR FUNCTION dz_topo_map_mgr(
        p_topology_owner  IN  VARCHAR2 DEFAULT NULL
       ,p_topology_name   IN  VARCHAR2
       ,p_topo_map_name   IN  VARCHAR2 DEFAULT NULL
       ,p_allow_updates   IN  VARCHAR2 DEFAULT 'TRUE'
       ,p_number_of_edges IN  NUMBER   DEFAULT 100
       ,p_number_of_nodes IN  NUMBER   DEFAULT 80
       ,p_number_of_faces IN  NUMBER   DEFAULT 30
    ) RETURN SELF AS RESULT
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,CONSTRUCTOR FUNCTION dz_topo_map_mgr(
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
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,CONSTRUCTOR FUNCTION dz_topo_map_mgr(
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
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,CONSTRUCTOR FUNCTION dz_topo_map_mgr(
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
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER PROCEDURE create_topo_map
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER PROCEDURE load_topo_map
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION create_feature(
       p_table_name       IN  VARCHAR2
      ,p_column_name      IN  VARCHAR2
      ,p_geometry         IN  MDSYS.SDO_GEOMETRY
    ) RETURN MDSYS.SDO_TOPO_GEOMETRY
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER PROCEDURE commit_topo_map(
        p_validate        IN  VARCHAR2 DEFAULT 'TRUE'
       ,p_validate_level  IN  NUMBER   DEFAULT 1
    )
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION validate_topo_map(
      p_validate_level    IN  NUMBER DEFAULT 1
    ) RETURN VARCHAR2
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER PROCEDURE drop_topo_map
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER PROCEDURE clear_topo_map
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER PROCEDURE java_memory(
        p_bytes     IN  NUMBER DEFAULT NULL
       ,p_gigs      IN  NUMBER DEFAULT NULL
    )
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER PROCEDURE create_indexes
     
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION get_topology_owner
    RETURN VARCHAR2
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION get_topology_name
    RETURN VARCHAR2
     
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION verify_topology
    RETURN VARCHAR
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION verify_topo_map
    RETURN VARCHAR2
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION current_updateable_topo_map
    RETURN VARCHAR2
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION get_topo_maps
    RETURN MDSYS.SDO_STRING2_ARRAY
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER PROCEDURE purge_all_topo_maps
        
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER PROCEDURE initialize_topo_basics(
        p_topology_owner  IN  VARCHAR2 DEFAULT NULL
       ,p_topology_name   IN  VARCHAR2
       ,p_topo_map_name   IN  VARCHAR2 DEFAULT NULL
       ,p_allow_updates   IN  VARCHAR2 DEFAULT 'TRUE'
       ,p_number_of_edges IN  NUMBER   DEFAULT 100
       ,p_number_of_nodes IN  NUMBER   DEFAULT 80
       ,p_number_of_faces IN  NUMBER   DEFAULT 30
    )
    
);
/

GRANT EXECUTE ON dz_topo_map_mgr TO PUBLIC;

