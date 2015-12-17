CREATE OR REPLACE PACKAGE BODY dz_topo_util
AS

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION c_schema(
       p_call_stack IN VARCHAR2 DEFAULT NULL
      ,p_type       IN VARCHAR2 DEFAULT 'SCHEMA'
      ,p_depth      IN NUMBER   DEFAULT 1
   ) RETURN VARCHAR2 DETERMINISTIC
   AS
      str_call_stack VARCHAR2(4000 Char) := p_call_stack;
      ary_lines      MDSYS.SDO_STRING2_ARRAY;
      ary_words      MDSYS.SDO_STRING2_ARRAY;
      int_handle     PLS_INTEGER := 0;
      int_n          PLS_INTEGER;
      str_name       VARCHAR2(30 Char);
      str_owner      VARCHAR2(30 Char);
      str_type       VARCHAR2(30 Char);
      
   BEGIN
   
      IF str_call_stack IS NULL
      THEN
         str_call_stack := DBMS_UTILITY.FORMAT_CALL_STACK;
         
      END IF;

      ary_lines := gz_split(
          p_str   => str_call_stack
         ,p_regex => CHR(10)
      );

      FOR i IN 1 .. ary_lines.COUNT
      LOOP
         IF ary_lines(i) LIKE '%handle%number%name%'
         THEN
            int_handle := i + p_depth;
            
         END IF;

      END LOOP;

      IF int_handle = 0
      OR NOT ary_lines.EXISTS(int_handle)
      THEN
         RAISE_APPLICATION_ERROR(
             -20001
            ,'error in parsing stack, no stack call found at ' || p_depth || ' depth'
         );
         
      END IF;

      ary_words := gz_split(
          p_str   => ary_lines(int_handle)
         ,p_regex =>  '\s+'
         ,p_end   => 3
      );

      IF NOT ary_words.EXISTS(3)
      THEN
         RAISE_APPLICATION_ERROR(
             -20001
            ,'error in parsing call stack line for info' || chr(10) || ary_lines(int_handle)
         );
         
      END IF;

      IF ary_words(3) LIKE 'pr%'
      THEN
        int_n := LENGTH('procedure ');
      
      ELSIF ary_words(3) LIKE 'fun%'
      THEN
         int_n := LENGTH('function ');
         
      ELSIF ary_words(3) LIKE 'package body%'
      THEN
         int_n := LENGTH('package body ');
      
      ELSIF ary_words(3) LIKE 'pack%'
      THEN
         int_n := LENGTH('package ');
      
      ELSIF ary_words(3) LIKE 'anonymous%'
      THEN
         int_n := LENGTH('anonymous block ');
      
      ELSE
         int_n := null;
      
      END IF;

      IF int_n IS NOT NULL
      THEN
         str_type := TRIM(UPPER(SUBSTR( ary_words(3), 1, int_n - 1 )));
         
      ELSE
         str_type := 'TRIGGER';
      
      END IF;

      str_owner := TRIM(
         SUBSTR(ary_words(3),int_n + 1,INSTR(ary_words(3),'.') - (int_n + 1))
      );
      str_name := TRIM(
         SUBSTR(ary_words(3),INSTR(ary_words(3),'.') + 1)
      );

      IF UPPER(p_type) = 'NAME'
      THEN
         RETURN str_name;
         
      ELSIF UPPER(p_type) = 'SCHEMA.NAME'
      OR    UPPER(p_type) = 'OWNER.NAME'
      THEN
         RETURN str_owner || '.' || str_name;
         
      ELSIF UPPER(p_type) = 'TYPE'
      THEN
         RETURN str_type;
         
      ELSE
         RETURN str_owner;
         
      END IF;

   END c_schema;

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE append2(
       p_input_array      IN OUT MDSYS.SDO_NUMBER_ARRAY
      ,p_input_value      IN     NUMBER
      ,p_unique           IN     VARCHAR2 DEFAULT 'FALSE'
   ) 
   AS
      boo_check   BOOLEAN;
      num_index   PLS_INTEGER;
      str_unique  VARCHAR2(5 Char);
      
   BEGIN
   
      IF p_unique IS NULL
      THEN
         str_unique := 'FALSE';
         
      ELSIF UPPER(p_unique) IN ('FALSE','TRUE')
      THEN
         str_unique := UPPER(p_unique);
         
      ELSE
         RAISE_APPLICATION_ERROR(-20001,'p_unique flag must be TRUE or FALSE');
         
      END IF;

      IF p_input_array IS NULL
      THEN
         p_input_array := MDSYS.SDO_NUMBER_ARRAY();
         
      END IF;

      IF p_input_array.COUNT > 0
      THEN
         IF str_unique = 'TRUE'
         THEN
            boo_check := FALSE;
            
            FOR i IN 1 .. p_input_array.COUNT
            LOOP
               IF p_input_value = p_input_array(i)
               THEN
                  boo_check := TRUE;
                  
               END IF;
               
            END LOOP;

            IF boo_check = TRUE
            THEN
               -- Do Nothing
               RETURN;
               
            END IF;

         END IF;

      END IF;

      num_index := p_input_array.COUNT + 1;
      p_input_array.EXTEND(1);
      p_input_array(num_index) := p_input_value;

   END append2;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE append2(
       p_input_array      IN OUT MDSYS.SDO_NUMBER_ARRAY
      ,p_input_value      IN     MDSYS.SDO_NUMBER_ARRAY
      ,p_unique           IN     VARCHAR2 DEFAULT 'FALSE'
   ) 
   AS
   BEGIN
      FOR i IN 1 .. p_input_value.COUNT
      LOOP
         append2(
            p_input_array => p_input_array,
            p_input_value => p_input_value(i),
            p_unique      => p_unique
         );
         
      END LOOP;

   END append2;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION has_holes(
      p_input             IN MDSYS.SDO_GEOMETRY
   ) RETURN VARCHAR2
   AS
      i PLS_INTEGER := 1;
      
   BEGIN

      IF p_input IS NULL
      THEN
         RETURN 'FALSE';
         
      END IF;

      WHILE i <= p_input.SDO_ELEM_INFO.COUNT
      LOOP
         i := i + 1;
         
         IF p_input.SDO_ELEM_INFO(i) = 2003
         OR p_input.SDO_ELEM_INFO(i) = 2005
         THEN
            RETURN 'TRUE';
            
         END IF;
         
         i := i + 2;
         
      END LOOP;

      RETURN 'FALSE';
      
   END has_holes;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION is_ring(
      p_input             IN MDSYS.SDO_GEOMETRY
   ) RETURN VARCHAR2
   AS
   BEGIN

      --------------------------------------------------------------------------
      -- Step 10
      -- Input geometry must be a 2 or 3
      --------------------------------------------------------------------------
      IF p_input.get_gtype() NOT IN (2,3)
      THEN
         RETURN 'FALSE';
         
      END IF;

      --------------------------------------------------------------------------
      -- Step 20
      -- Polygons cannot have holes
      --------------------------------------------------------------------------
      IF  p_input.get_gtype() = 3
      AND has_holes(p_input) = 'TRUE'
      THEN
         RETURN 'FALSE';
         
      END IF;

      --------------------------------------------------------------------------
      -- Step 30
      -- Input geometry must have the same start and end points
      -- Only checking X and Y
      --------------------------------------------------------------------------
      IF p_input.sdo_ordinates(1) = p_input.sdo_ordinates(p_input.sdo_ordinates.COUNT - (p_input.get_dims() - 2))
      AND p_input.sdo_ordinates(2) = p_input.sdo_ordinates(p_input.sdo_ordinates.COUNT - (p_input.get_dims() - 1))
      THEN
         RETURN 'FALSE';
         
      END IF;
         
      --------------------------------------------------------------------------
      -- Step 40
      -- Assume its okay
      --------------------------------------------------------------------------
      RETURN 'TRUE';
   
   END is_ring;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE test_ordinate_rotation(
      p_input       IN  MDSYS.SDO_GEOMETRY,
      p_lower_bound IN  NUMBER DEFAULT 1,
      p_upper_bound IN  NUMBER DEFAULT NULL,
      p_results     OUT VARCHAR2,
      p_area        OUT NUMBER
   )
   AS
      int_dims      PLS_INTEGER;
      int_lb        PLS_INTEGER := p_lower_bound;
      int_ub        PLS_INTEGER := p_upper_bound;
      num_x         NUMBER;
      num_y         NUMBER;
      num_lastx     NUMBER;
      num_lasty     NUMBER;

   BEGIN

      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF int_ub IS NULL
      THEN
         int_ub  := p_input.sdo_ordinates.COUNT;
         
      END IF;

      IF int_lb IS NULL
      THEN
         int_lb  := 1;
         
      END IF;

      --------------------------------------------------------------------------
      -- Step 20
      -- Get the number of dimensions in the geometry
      --------------------------------------------------------------------------
      int_dims := p_input.get_dims();

      --------------------------------------------------------------------------
      -- Step 30
      -- Loop through the ordinates create the area value
      --------------------------------------------------------------------------
      p_area  := 0;
      num_lastx := 0;
      num_lasty := 0;
      
      WHILE int_lb <= int_ub
      LOOP
         num_x := p_input.SDO_ORDINATES(int_lb);
         num_y := p_input.SDO_ORDINATES(int_lb + 1);
         p_area := p_area + ( (num_lasty * num_x ) - ( num_lastx * num_y) );
         num_lastx := num_x;
         num_lasty := num_y;
         int_lb := int_lb + int_dims;
         
      END LOOP;

      --------------------------------------------------------------------------
      -- Step 40
      -- If area is positive, then its clockwise
      --------------------------------------------------------------------------
      IF p_area > 0
      THEN
         p_results := 'CW';
         
      ELSE
         p_results := 'CCW';
         
      END IF;

      --------------------------------------------------------------------------
      -- Step 50
      -- Preserve the area value if required by the caller
      --------------------------------------------------------------------------
      p_area := ABS(p_area);

   END test_ordinate_rotation;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE verify_ordinate_rotation(
       p_rotation    IN            VARCHAR2
      ,p_input       IN OUT NOCOPY MDSYS.SDO_GEOMETRY
      ,p_lower_bound IN            PLS_INTEGER DEFAULT 1
      ,p_upper_bound IN            PLS_INTEGER DEFAULT NULL
   )
   AS
      num_area      NUMBER;
      
   BEGIN
      verify_ordinate_rotation(
          p_rotation    => p_rotation
         ,p_input       => p_input
         ,p_area        => num_area
         ,p_lower_bound => p_lower_bound 
         ,p_upper_bound => p_upper_bound
      );

   END verify_ordinate_rotation;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE verify_ordinate_rotation(
       p_rotation    IN            VARCHAR2
      ,p_input       IN OUT NOCOPY MDSYS.SDO_GEOMETRY
      ,p_area        IN OUT NOCOPY NUMBER
      ,p_lower_bound IN            PLS_INTEGER DEFAULT 1
      ,p_upper_bound IN            PLS_INTEGER DEFAULT NULL
   )
   AS
      str_rotation  VARCHAR2(3 Char);
      int_lb        PLS_INTEGER := p_lower_bound;
      int_ub        PLS_INTEGER := p_upper_bound;
      
   BEGIN

      IF p_rotation NOT IN ('CW','CCW')
      THEN
         RAISE_APPLICATION_ERROR(
             -20001
            ,'rotation values are CW or CCW'
         );
         
      END IF;

      IF p_upper_bound IS NULL
      THEN
         int_ub  := p_input.SDO_ORDINATES.COUNT;
         
      END IF;

      test_ordinate_rotation(
          p_input       => p_input
         ,p_lower_bound => int_lb
         ,p_upper_bound => int_ub
         ,p_results     => str_rotation
         ,p_area        => p_area
      );
 
      IF p_rotation = str_rotation
      THEN
         RETURN;
         
      ELSE
         reverse_ordinate_rotation(
             p_input       => p_input
            ,p_lower_bound => p_lower_bound
            ,p_upper_bound => p_upper_bound
         );
         
         RETURN;
         
      END IF;

   END verify_ordinate_rotation;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE reverse_ordinate_rotation(
       p_input       IN OUT NOCOPY MDSYS.SDO_GEOMETRY
      ,p_lower_bound IN            PLS_INTEGER DEFAULT 1
      ,p_upper_bound IN            PLS_INTEGER DEFAULT NULL
   ) 
   AS
      int_n         PLS_INTEGER;
      int_m         PLS_INTEGER;
      int_li        PLS_INTEGER;
      int_ui        PLS_INTEGER;
      num_tempx     NUMBER;
      num_tempy     NUMBER;
      num_tempz     NUMBER;
      num_tempm     NUMBER;
      int_lb        PLS_INTEGER := p_lower_bound;
      int_ub        PLS_INTEGER := p_upper_bound;
      int_dims      PLS_INTEGER;
      
   BEGIN
      
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF int_lb IS NULL
      THEN
         int_lb := 1;
         
      END IF;
      
      IF int_ub IS NULL
      THEN
         int_ub  := p_input.SDO_ORDINATES.COUNT;
         
      END IF;
      
      int_dims := p_input.get_dims();

      int_n := int_ub - int_lb + 1;

      -- Exit if only a single ordinate
      IF int_n <= int_dims
      THEN
         RETURN;
         
      END IF;

      -- Calculate the start n1, the end n2, and the middle m
      int_m  := int_lb + (int_n / 2);
      int_li := int_lb;
      int_ui := int_ub;
      WHILE int_li < int_m
      LOOP
         IF int_dims = 2
         THEN
            num_tempx := p_input.SDO_ORDINATES(int_li);
            num_tempy := p_input.SDO_ORDINATES(int_li + 1);

            p_input.SDO_ORDINATES(int_li)     := p_input.SDO_ORDINATES(int_ui - 1);
            p_input.SDO_ORDINATES(int_li + 1) := p_input.SDO_ORDINATES(int_ui);

            p_input.SDO_ORDINATES(int_ui - 1) := num_tempx;
            p_input.SDO_ORDINATES(int_ui)     := num_tempy;

         ELSIF int_dims = 3
         THEN
            num_tempx := p_input.SDO_ORDINATES(int_li);
            num_tempy := p_input.SDO_ORDINATES(int_li + 1);
            num_tempz := p_input.SDO_ORDINATES(int_li + 2);

            p_input.SDO_ORDINATES(int_li)     := p_input.SDO_ORDINATES(int_ui - 2);
            p_input.SDO_ORDINATES(int_li + 1) := p_input.SDO_ORDINATES(int_ui - 1);
            p_input.SDO_ORDINATES(int_li + 2) := p_input.SDO_ORDINATES(int_ui);

            p_input.SDO_ORDINATES(int_ui - 2) := num_tempx;
            p_input.SDO_ORDINATES(int_ui - 1) := num_tempy;
            p_input.SDO_ORDINATES(int_ui)     := num_tempz;
            
         ELSIF int_dims = 4
         THEN
            num_tempx := p_input.SDO_ORDINATES(int_li);
            num_tempy := p_input.SDO_ORDINATES(int_li + 1);
            num_tempz := p_input.SDO_ORDINATES(int_li + 2);
            num_tempm := p_input.SDO_ORDINATES(int_li + 3);

            p_input.SDO_ORDINATES(int_li)     := p_input.SDO_ORDINATES(int_ui - 3);
            p_input.SDO_ORDINATES(int_li + 1) := p_input.SDO_ORDINATES(int_ui - 2);
            p_input.SDO_ORDINATES(int_li + 2) := p_input.SDO_ORDINATES(int_ui - 1);
            p_input.SDO_ORDINATES(int_li + 3) := p_input.SDO_ORDINATES(int_ui);

            p_input.SDO_ORDINATES(int_ui - 3) := num_tempx;
            p_input.SDO_ORDINATES(int_ui - 2) := num_tempy;
            p_input.SDO_ORDINATES(int_ui - 1) := num_tempz;
            p_input.SDO_ORDINATES(int_ui)     := num_tempm;
            
         END IF;

         int_li := int_li + int_dims;
         int_ui := int_ui - int_dims;

      END LOOP;

   END reverse_ordinate_rotation;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE reverse_ordinate_rotation(
       p_input       IN OUT NOCOPY MDSYS.SDO_ORDINATE_ARRAY
      ,p_lower_bound IN            PLS_INTEGER DEFAULT 1
      ,p_upper_bound IN            PLS_INTEGER DEFAULT NULL
      ,p_num_dims    IN            PLS_INTEGER DEFAULT 2
   ) 
   AS
      int_n         PLS_INTEGER;
      num_m         NUMBER;
      int_li        PLS_INTEGER;
      int_ui        PLS_INTEGER;
      num_tempx     NUMBER;
      num_tempy     NUMBER;
      num_tempz     NUMBER;
      num_tempm     NUMBER;
      int_lb        PLS_INTEGER := p_lower_bound;
      int_ub        PLS_INTEGER := p_upper_bound;
      int_dims      PLS_INTEGER := p_num_dims;
      
   BEGIN

      IF int_lb IS NULL
      THEN
         int_lb := 1;
         
      END IF;
      
      IF int_ub IS NULL
      THEN
         int_ub  := p_input.COUNT;
         
      END IF;
      
      IF int_dims IS NULL
      THEN
         int_dims := 2;
         
      END IF;

      int_n := int_ub - int_lb + 1;

      -- Exit if only a single ordinate
      IF int_n <= int_dims
      THEN
         RETURN;
         
      END IF;

      -- Calculate the start n1, the end n2, and the middle m
      num_m  := int_lb + (int_n / 2); 
      int_li := int_lb;
      int_ui := int_ub;

      WHILE int_li < num_m
      LOOP
         IF int_dims = 2
         THEN
            num_tempx := p_input(int_li);
            num_tempy := p_input(int_li + 1);

            p_input(int_li)     := p_input(int_ui - 1);
            p_input(int_li + 1) := p_input(int_ui);

            p_input(int_ui - 1) := num_tempx;
            p_input(int_ui)     := num_tempy;

         ELSIF int_dims = 3
         THEN
            num_tempx := p_input(int_li);
            num_tempy := p_input(int_li + 1);
            num_tempz := p_input(int_li + 2);

            p_input(int_li)     := p_input(int_ui - 2);
            p_input(int_li + 1) := p_input(int_ui - 1);
            p_input(int_li + 2) := p_input(int_ui);

            p_input(int_ui - 2) := num_tempx;
            p_input(int_ui - 1) := num_tempy;
            p_input(int_ui)     := num_tempz;
            
         ELSIF int_dims = 4
         THEN
            num_tempx := p_input(int_li);
            num_tempy := p_input(int_li + 1);
            num_tempz := p_input(int_li + 2);
            num_tempm := p_input(int_li + 3);

            p_input(int_li)     := p_input(int_ui - 3);
            p_input(int_li + 1) := p_input(int_ui - 2);
            p_input(int_li + 2) := p_input(int_ui - 1);
            p_input(int_li + 3) := p_input(int_ui);

            p_input(int_ui - 3) := num_tempx;
            p_input(int_ui - 2) := num_tempy;
            p_input(int_ui - 1) := num_tempz;
            p_input(int_ui)     := num_tempm;
            
         END IF;

         int_li := int_li + int_dims;
         int_ui := int_ui - int_dims;

      END LOOP;

   END reverse_ordinate_rotation;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION gz_split(
       p_str              IN VARCHAR2
      ,p_regex            IN VARCHAR2
      ,p_match            IN VARCHAR2 DEFAULT NULL
      ,p_end              IN NUMBER   DEFAULT 0
      ,p_trim             IN VARCHAR2 DEFAULT 'FALSE'
   ) RETURN MDSYS.SDO_STRING2_ARRAY DETERMINISTIC 
   AS
      int_delim      PLS_INTEGER;
      int_position   PLS_INTEGER := 1;
      int_counter    PLS_INTEGER := 1;
      ary_output     MDSYS.SDO_STRING2_ARRAY;
      num_end        NUMBER      := p_end;
      str_trim       VARCHAR2(5 Char) := UPPER(p_trim);
      
      FUNCTION trim_varray(
         p_input            IN MDSYS.SDO_STRING2_ARRAY
      ) RETURN MDSYS.SDO_STRING2_ARRAY
      AS
         ary_output MDSYS.SDO_STRING2_ARRAY := MDSYS.SDO_STRING2_ARRAY();
         int_index  PLS_INTEGER := 1;
         str_check  VARCHAR2(4000 Char);
         
      BEGIN

         --------------------------------------------------------------------------
         -- Step 10
         -- Exit if input is empty
         --------------------------------------------------------------------------
         IF p_input IS NULL
         OR p_input.COUNT = 0
         THEN
            RETURN ary_output;
            
         END IF;

         --------------------------------------------------------------------------
         -- Step 20
         -- Trim the strings removing anything utterly trimmed away
         --------------------------------------------------------------------------
         FOR i IN 1 .. p_input.COUNT
         LOOP
            str_check := TRIM(p_input(i));
            
            IF str_check IS NULL
            OR str_check = ''
            THEN
               NULL;
               
            ELSE
               ary_output.EXTEND(1);
               ary_output(int_index) := str_check;
               int_index := int_index + 1;
               
            END IF;

         END LOOP;

         --------------------------------------------------------------------------
         -- Step 10
         -- Return the results
         --------------------------------------------------------------------------
         RETURN ary_output;

      END trim_varray;

   BEGIN

      --------------------------------------------------------------------------
      -- Step 10
      -- Create the output array and check parameters
      --------------------------------------------------------------------------
      ary_output := MDSYS.SDO_STRING2_ARRAY();

      IF str_trim IS NULL
      THEN
         str_trim := 'FALSE';
         
      ELSIF str_trim NOT IN ('TRUE','FALSE')
      THEN
         RAISE_APPLICATION_ERROR(-20001,'boolean error');
         
      END IF;

      IF num_end IS NULL
      THEN
         num_end := 0;
         
      END IF;

      --------------------------------------------------------------------------
      -- Step 20
      -- Exit early if input is empty
      --------------------------------------------------------------------------
      IF p_str IS NULL
      OR p_str = ''
      THEN
         RETURN ary_output;
         
      END IF;

      --------------------------------------------------------------------------
      -- Step 30
      -- Account for weird instance of pure character breaking
      --------------------------------------------------------------------------
      IF p_regex IS NULL
      OR p_regex = ''
      THEN
         FOR i IN 1 .. LENGTH(p_str)
         LOOP
            ary_output.EXTEND(1);
            ary_output(i) := SUBSTR(p_str,i,1);
            
         END LOOP;
         
         RETURN ary_output;
         
      END IF;

      --------------------------------------------------------------------------
      -- Step 40
      -- Break string using the usual REGEXP functions
      --------------------------------------------------------------------------
      LOOP
         EXIT WHEN int_position = 0;
         int_delim  := REGEXP_INSTR(p_str,p_regex,int_position,1,0,p_match);
         
         IF  int_delim = 0
         THEN
            -- no more matches found
            ary_output.EXTEND(1);
            ary_output(int_counter) := SUBSTR(p_str,int_position);
            int_position  := 0;
            
         ELSE
            IF int_counter = num_end
            THEN
               -- take the rest as is
               ary_output.EXTEND(1);
               ary_output(int_counter) := SUBSTR(p_str,int_position);
               int_position  := 0;
               
            ELSE
               --dbms_output.put_line(ary_output.COUNT);
               ary_output.EXTEND(1);
               ary_output(int_counter) := SUBSTR(p_str,int_position,int_delim-int_position);
               int_counter := int_counter + 1;
               int_position := REGEXP_INSTR(p_str,p_regex,int_position,1,1,p_match);
               
            END IF;
            
         END IF;
         
      END LOOP;

      --------------------------------------------------------------------------
      -- Step 50
      -- Trim results if so desired
      --------------------------------------------------------------------------
      IF str_trim = 'TRUE'
      THEN
         RETURN trim_varray(
            p_input => ary_output
         );
         
      END IF;

      --------------------------------------------------------------------------
      -- Step 60
      -- Cough out the results
      --------------------------------------------------------------------------
      RETURN ary_output;
      
   END gz_split;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION index_exists(
       p_owner            IN  VARCHAR2 DEFAULT NULL
      ,p_table_name       IN  VARCHAR2
      ,p_column_name      IN  VARCHAR2
   ) RETURN VARCHAR2
   AS
      str_owner      VARCHAR2(30 Char) := UPPER(p_owner);
      num_counter    NUMBER;

   BEGIN

      IF str_owner IS NULL
      THEN
         str_owner := USER;
      
      END IF;

      SELECT 
      COUNT(*) 
      INTO num_counter
      FROM 
      all_indexes a 
      JOIN
      all_ind_columns b
      ON
          a.table_owner = b.table_owner
      AND a.table_name  = b.table_name
      AND a.index_name  = b.index_name
      WHERE 
          a.table_owner = str_owner 
      AND a.table_name  = p_table_name
      AND b.column_name = p_column_name;

      IF num_counter = 0
      THEN
         RETURN 'FALSE';
         
      ELSIF num_counter = 1
      THEN
         RETURN 'TRUE';
         
      ELSE
         RAISE_APPLICATION_ERROR(-20001,'error');
         
      END IF;

   END index_exists;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION append_hole_to_polygon(
       p_input           IN MDSYS.SDO_GEOMETRY   
      ,p_hole            IN MDSYS.SDO_GEOMETRY
   ) RETURN MDSYS.SDO_GEOMETRY
   AS
      sdo_hole       MDSYS.SDO_GEOMETRY := p_hole;
      ary_ordinates  MDSYS.SDO_ORDINATE_ARRAY;
      ary_elem_infos MDSYS.SDO_ELEM_INFO_ARRAY;
      int_info_indx  PLS_INTEGER;
      int_ords_indx  PLS_INTEGER;
      
   BEGIN
     
      ---------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      ---------------------------------------------------------------------------
      IF p_input IS NULL
      THEN
         RETURN NULL;
         
      END IF;
     
      IF p_input.get_gtype() <> 3
      THEN
         RAISE_APPLICATION_ERROR(-20001,'input must be single polygon');
     
      END IF;
      
      IF sdo_hole IS NULL
      THEN
         RETURN p_input;
       
      END IF;
     
      IF sdo_hole.get_gtype() <> 3
      THEN
         RAISE_APPLICATION_ERROR(-20001,'hole must be single polygon');
     
      END IF;
     
      IF p_input.get_dims() <> sdo_hole.get_dims()
      THEN
         RAISE_APPLICATION_ERROR(-20001,'input and hole must be same number of dims');
     
      END IF;
     
      IF p_input.SDO_SRID <> sdo_hole.SDO_SRID
      THEN
         RAISE_APPLICATION_ERROR(-20001,'input and hole have different srids');
     
      END IF;
     
      ---------------------------------------------------------------------------
      -- Step 20
      -- Make sure hole is CW
      ---------------------------------------------------------------------------
      verify_ordinate_rotation(
          p_rotation => 'CW'
         ,p_input    => sdo_hole
      );
      
     ---------------------------------------------------------------------------
     -- Step 30
     -- Set up arrays
     ---------------------------------------------------------------------------
     ary_elem_infos := p_input.SDO_ELEM_INFO;
     ary_ordinates  := p_input.SDO_ORDINATES;
     int_info_indx  := ary_elem_infos.COUNT + 1;
     int_ords_indx  := ary_ordinates.COUNT + 1;
     
     ary_elem_infos.EXTEND(3);
     ary_ordinates.EXTEND(sdo_hole.SDO_ORDINATES.COUNT);
     
     ---------------------------------------------------------------------------
     -- Step 40
     -- Adjust the elem info array
     ---------------------------------------------------------------------------
     ary_elem_infos(int_info_indx) := int_ords_indx;
     int_info_indx := int_info_indx + 1;
     
     ary_elem_infos(int_info_indx) := 2003;
     int_info_indx := int_info_indx + 1;
     
     ary_elem_infos(int_info_indx) := 1;
     int_info_indx := int_info_indx + 1;
     
     ---------------------------------------------------------------------------
     -- Step 50
     -- Append the data to the ordinates
     ---------------------------------------------------------------------------
     FOR i IN 1 .. sdo_hole.SDO_ORDINATES.COUNT
     LOOP
        ary_ordinates(int_ords_indx) := sdo_hole.SDO_ORDINATES(i);
        int_ords_indx := int_ords_indx + 1;
        
     END LOOP;
     
     ---------------------------------------------------------------------------
     -- Step 60
     -- Return the output geometry
     ---------------------------------------------------------------------------
     RETURN MDSYS.SDO_GEOMETRY(
         p_input.SDO_GTYPE
        ,p_input.SDO_SRID
        ,NULL
        ,ary_elem_infos
        ,ary_ordinates
     );
     
   END append_hole_to_polygon;

END dz_topo_util;
/

