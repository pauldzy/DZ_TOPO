CREATE OR REPLACE TYPE dz_topo_edge_list FORCE                                
AS 
TABLE OF dz_topo_edge;
/

GRANT EXECUTE ON dz_topo_edge_list TO PUBLIC;

