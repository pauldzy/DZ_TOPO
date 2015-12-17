CREATE OR REPLACE TYPE dz_topo_ring FORCE
AUTHID CURRENT_USER
AS OBJECT (
    ring_id       NUMBER
   ,ring_type     VARCHAR2(1 Char)  -- I for interior ring, E for exterior ring
   ,ring_interior VARCHAR2(1 Char)  -- L for left, R for right
   ,ring_rotation VARCHAR2(3 Char)  -- CW for clockwise, CCW for counter-clockwise
   ,ring_group    NUMBER     
   ,ring_size     NUMBER
   ,tail_node_id  NUMBER
   ,head_node_id  NUMBER
   ,node_list     MDSYS.SDO_NUMBER_ARRAY
   ,edge_list     MDSYS.SDO_NUMBER_ARRAY
   ,shape         MDSYS.SDO_GEOMETRY
   ,insider_count NUMBER
   ,ring_status   VARCHAR2(1 Char)  -- I for incomplete, C for complete, U for unravel
   ,try_reversal  VARCHAR2(5 Char)  -- TRUE/FALSE
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,CONSTRUCTOR FUNCTION dz_topo_ring
    RETURN SELF AS RESULT
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,CONSTRUCTOR FUNCTION dz_topo_ring(
       p_input   IN  dz_topo_edge,
       p_ring_id IN  NUMBER DEFAULT NULL
    ) RETURN SELF AS RESULT
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER PROCEDURE append_edge(
       p_input   IN  dz_topo_edge
    )
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER PROCEDURE append_edge(
       p_input   IN  dz_topo_ring
    )
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER PROCEDURE flip
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER PROCEDURE finalize
   
);
/

GRANT EXECUTE ON dz_topo_ring TO PUBLIC;

