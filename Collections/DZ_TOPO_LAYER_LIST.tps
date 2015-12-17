CREATE OR REPLACE TYPE dz_topo_layer_list FORCE                                      
AS 
TABLE OF dz_topo_layer;
/

GRANT EXECUTE ON dz_topo_layer_list TO PUBLIC;

