CREATE OR REPLACE PACKAGE dz_topo_recipes
AUTHID CURRENT_USER
AS

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE align_edges(
       p_table_name            IN  VARCHAR2
      ,p_column_name           IN  VARCHAR2
      ,p_work_table_name       IN  VARCHAR2 DEFAULT 'GERINGER_TMP'
   );
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE generic_loader;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE scorched_earth_around_face(
       p_face_id     IN  NUMBER
   );
   
END dz_topo_recipes;
/

GRANT EXECUTE ON dz_topo_recipes TO public;

