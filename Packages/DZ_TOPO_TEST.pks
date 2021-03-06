CREATE OR REPLACE PACKAGE dz_topo_test
AUTHID CURRENT_USER
AS

   C_GITRELEASE    CONSTANT VARCHAR2(255 Char) := 'NULL';
   C_GITCOMMIT     CONSTANT VARCHAR2(255 Char) := 'NULL';
   C_GITCOMMITDATE CONSTANT VARCHAR2(255 Char) := 'NULL';
   C_GITCOMMITAUTH CONSTANT VARCHAR2(255 Char) := 'NULL';

   C_TEST_SCHEMA   CONSTANT VARCHAR2(30 Char)  := 'DZ_SCRATCH';
   C_DZ_TESTDATA   CONSTANT VARCHAR2(30 Char)  := 'DZ_TESTDATA';
   C_PREREQUISITES CONSTANT MDSYS.SDO_STRING2_ARRAY := MDSYS.SDO_STRING2_ARRAY(
   );
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION prerequisites
   RETURN NUMBER;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION version
   RETURN VARCHAR2;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION inmemory_test
   RETURN NUMBER;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION scratch_test
   RETURN NUMBER;
   
END dz_topo_test;
/

GRANT EXECUTE ON dz_topo_test TO public;

