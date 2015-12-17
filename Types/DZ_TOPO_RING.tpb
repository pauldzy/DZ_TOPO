CREATE OR REPLACE TYPE BODY dz_topo_ring
AS

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   CONSTRUCTOR FUNCTION dz_topo_ring
   RETURN SELF AS RESULT
   AS
   BEGIN
      self.try_reversal := 'TRUE';
      self.ring_status  := 'I';
      
      RETURN;
      
   END dz_topo_ring;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   CONSTRUCTOR FUNCTION dz_topo_ring(
       p_input   IN dz_topo_edge
      ,p_ring_id IN NUMBER DEFAULT NULL
   ) RETURN SELF AS RESULT
   AS
   BEGIN
   
      IF p_ring_id IS NULL
      THEN
         self.ring_id := 1;
         
      ELSE
         self.ring_id := p_ring_id;
         
      END IF;
      
      self.ring_interior := p_input.interior_side;
      self.head_node_id  := p_input.end_node_id;
      self.tail_node_id  := p_input.start_node_id;
      
      dz_topo_util.append2(self.node_list,p_input.end_node_id);
      dz_topo_util.append2(self.node_list,p_input.start_node_id);
      dz_topo_util.append2(self.edge_list,p_input.edge_id);
      
      self.shape         := p_input.shape;
      self.try_reversal := 'TRUE';
      self.ring_status  := 'I';
      
      RETURN;
      
   END dz_topo_ring;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER PROCEDURE append_edge(
      p_input   IN  dz_topo_edge
   )
   AS
      obj_edge  dz_topo_edge := p_input;

   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Reverse edge if needed
      --------------------------------------------------------------------------
      IF self.ring_interior != obj_edge.interior_side
      THEN
         obj_edge.flip();
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Verify that node information matches
      --------------------------------------------------------------------------
      IF self.head_node_id != obj_edge.start_node_id
      THEN
         RAISE_APPLICATION_ERROR(
             -20001
            ,'append node start does not match ring head id'
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Append the data
      --------------------------------------------------------------------------
      self.head_node_id := obj_edge.end_node_id;
      
      dz_topo_util.append2(self.node_list,obj_edge.end_node_id);
      dz_topo_util.append2(self.edge_list,obj_edge.edge_id);
      
      self.shape := sdo_util.concat_lines(self.shape,obj_edge.shape);
      
      RETURN;
   
   END append_edge;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER PROCEDURE append_edge(
      p_input   IN  dz_topo_ring
   )
   AS
      obj_ring  dz_topo_ring := p_input;

   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Reverse edge if needed
      --------------------------------------------------------------------------
      IF self.ring_interior != obj_ring.ring_interior
      THEN
         obj_ring.flip();
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Verify that node information matches
      --------------------------------------------------------------------------
      IF self.head_node_id != obj_ring.tail_node_id
      THEN
         RAISE_APPLICATION_ERROR(
             -20001
            ,'append node start does not match ring head id'
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Append the data
      --------------------------------------------------------------------------
      self.head_node_id := obj_ring.head_node_id;
      
      dz_topo_util.append2(self.node_list,obj_ring.node_list);
      dz_topo_util.append2(self.edge_list,obj_ring.edge_list);
      
      self.shape := sdo_util.concat_lines(self.shape,obj_ring.shape);
      
      RETURN;
   
   END append_edge;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER PROCEDURE flip
   AS
      num_temp NUMBER;
   
   BEGIN
   
      IF self.ring_type = 'I'
      THEN
         self.ring_type := 'E';
         
      ELSIF self.ring_type = 'E'
      THEN
         self.ring_type := 'I';
         
      END IF;
      
      IF self.ring_interior = 'L'
      THEN
         self.ring_interior := 'R';
         
      ELSIF self.ring_interior = 'R'
      THEN
         self.ring_interior := 'L';
         
      END IF;
      
      IF self.ring_rotation = 'CW'
      THEN
         self.ring_rotation := 'CCW';
         
      ELSIF self.ring_rotation = 'CCW'
      THEN
         self.ring_rotation := 'CW';
         
      END IF;
      
      num_temp := self.tail_node_id;
      self.tail_node_id := self.head_node_id;
      self.head_node_id := num_temp;
      
      IF self.shape IS NOT NULL
      THEN
         self.shape := MDSYS.SDO_UTIL.REVERSE_LINESTRING(
            self.shape
         );
         
      END IF;

   END flip;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER PROCEDURE finalize
   AS
   BEGIN
      
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over object attributes
      --------------------------------------------------------------------------
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Convert linestring into polygon ring
      --------------------------------------------------------------------------
      IF dz_topo_util.is_ring(self.shape) = 'TRUE'
      THEN
         IF self.shape.get_gtype() = 2
         THEN
            self.shape := MDSYS.SDO_GEOMETRY(
               TO_NUMBER(self.shape.get_dims() || '003'),
               self.shape.SDO_SRID,
               NULL,
               MDSYS.SDO_ELEM_INFO_ARRAY(1,1003,1),
               self.shape.SDO_ORDINATES
            );
         END IF;
         
      ELSE
         RAISE_APPLICATION_ERROR(-20001,'ERROR, ring is not complete!');
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Calculate the rotation and size, note size is in square decimal degrees
      -- and is used only for ordering the rings by size
      --------------------------------------------------------------------------
      dz_topo_util.test_ordinate_rotation(
          p_input   => self.shape
         ,p_results => self.ring_rotation
         ,p_area    => self.ring_size
      );
   
      --------------------------------------------------------------------------
      -- Step 50
      -- Set the ring type
      --------------------------------------------------------------------------
      IF self.ring_rotation = 'CW' AND self.ring_interior = 'R'
      THEN
         self.ring_type := 'E';
      ELSIF self.ring_rotation = 'CW' AND self.ring_interior = 'L'
      THEN
         self.ring_type := 'I';
      ELSIF self.ring_rotation = 'CCW' AND self.ring_interior = 'R'
      THEN
         self.ring_type := 'I';
      ELSIF self.ring_rotation = 'CCW' AND self.ring_interior = 'L'
      THEN
         self.ring_type := 'E';
      ELSE
         RAISE_APPLICATION_ERROR(-20001,'ERROR');
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 60
      -- Should be okay
      --------------------------------------------------------------------------
      self.ring_status := 'C';
      RETURN;
      
   END finalize;
   
END;
/

