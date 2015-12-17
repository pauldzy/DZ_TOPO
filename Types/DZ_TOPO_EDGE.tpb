CREATE OR REPLACE TYPE BODY dz_topo_edge
AS

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   CONSTRUCTOR FUNCTION dz_topo_edge
   RETURN SELF AS RESULT
   AS
   BEGIN
      RETURN;
      
   END dz_topo_edge;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION exterior_side
   RETURN VARCHAR2
   AS
   BEGIN
      IF self.interior_side = 'R'
      THEN
         RETURN 'L';
         
      ELSIF self.interior_side = 'L'
      THEN
         RETURN 'R';
         
      ELSE
         RAISE_APPLICATION_ERROR(-20001,'object not initialized');
         
      END IF;     
          
   END exterior_side;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER PROCEDURE flip
   AS
      num_tmp NUMBER;
      
   BEGIN
   
      IF self.interior_side = 'L'
      THEN
         self.interior_side := 'R';
         
      ELSIF self.interior_side = 'R'
      THEN
         self.interior_side := 'L';
         
      ELSE
         RAISE_APPLICATION_ERROR(-20001,'edge object is not initialized');
         
      END IF;
      
      num_tmp := self.start_node_id;
      self.start_node_id := self.end_node_id;
      self.end_node_id := num_tmp;
      
      self.shape := sdo_util.reverse_linestring(self.shape);
   
   END flip;
   
END;
/

