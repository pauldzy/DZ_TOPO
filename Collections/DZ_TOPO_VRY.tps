CREATE OR REPLACE TYPE dz_topo_vry FORCE                                    
AS 
VARRAY(1048576) OF MDSYS.SDO_TOPO_GEOMETRY;
/

GRANT EXECUTE ON dz_topo_vry TO PUBLIC;

