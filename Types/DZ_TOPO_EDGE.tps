CREATE OR REPLACE TYPE dz_topo_edge FORCE
AUTHID CURRENT_USER
AS OBJECT (
    edge_id       NUMBER 
   ,interior_side VARCHAR2(1 Char)
   ,start_node_id NUMBER
   ,end_node_id   NUMBER
   ,shape         MDSYS.SDO_GEOMETRY 
   ,touch_count   NUMBER
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,CONSTRUCTOR FUNCTION dz_topo_edge
    RETURN SELF AS RESULT
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION exterior_side
    RETURN VARCHAR2
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER PROCEDURE flip
   
);
/

GRANT EXECUTE ON dz_topo_edge TO PUBLIC;

