
--*************************--
PROMPT sqlplus_header.sql;

WHENEVER SQLERROR EXIT -99;
WHENEVER OSERROR  EXIT -98;
SET DEFINE OFF;



--*************************--
PROMPT DZ_TOPO_VRY.tps;

CREATE OR REPLACE TYPE dz_topo_vry FORCE                                    
AS 
VARRAY(1048576) OF MDSYS.SDO_TOPO_GEOMETRY;
/

GRANT EXECUTE ON dz_topo_vry TO PUBLIC;


--*************************--
PROMPT DZ_TOPO_UTIL.pks;

CREATE OR REPLACE PACKAGE dz_topo_util
AUTHID CURRENT_USER
AS

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION c_schema(
       p_call_stack IN VARCHAR2 DEFAULT NULL
      ,p_type       IN VARCHAR2 DEFAULT 'SCHEMA'
      ,p_depth      IN NUMBER   DEFAULT 1
   ) RETURN VARCHAR2 DETERMINISTIC;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE append2(
       p_input_array      IN OUT MDSYS.SDO_NUMBER_ARRAY
      ,p_input_value      IN     NUMBER
      ,p_unique           IN     VARCHAR2 DEFAULT 'FALSE'
   );
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE append2(
       p_input_array      IN OUT MDSYS.SDO_NUMBER_ARRAY
      ,p_input_value      IN     MDSYS.SDO_NUMBER_ARRAY
      ,p_unique           IN     VARCHAR2 DEFAULT 'FALSE'
   );
     
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION has_holes(
      p_input             IN MDSYS.SDO_GEOMETRY
   ) RETURN VARCHAR2;
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION is_ring(
      p_input             IN MDSYS.SDO_GEOMETRY
   ) RETURN VARCHAR2;
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE test_ordinate_rotation(
       p_input       IN  MDSYS.SDO_GEOMETRY
      ,p_lower_bound IN  NUMBER DEFAULT 1
      ,p_upper_bound IN  NUMBER DEFAULT NULL
      ,p_results     OUT VARCHAR2
      ,p_area        OUT NUMBER
   );
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE verify_ordinate_rotation(
       p_rotation    IN            VARCHAR2
      ,p_input       IN OUT NOCOPY MDSYS.SDO_GEOMETRY
      ,p_lower_bound IN            PLS_INTEGER DEFAULT 1
      ,p_upper_bound IN            PLS_INTEGER DEFAULT NULL
   );
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE verify_ordinate_rotation(
       p_rotation    IN            VARCHAR2
      ,p_input       IN OUT NOCOPY MDSYS.SDO_GEOMETRY
      ,p_area        IN OUT NOCOPY NUMBER
      ,p_lower_bound IN            PLS_INTEGER DEFAULT 1
      ,p_upper_bound IN            PLS_INTEGER DEFAULT NULL
   );
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE reverse_ordinate_rotation(
       p_input       IN OUT NOCOPY MDSYS.SDO_GEOMETRY
      ,p_lower_bound IN            PLS_INTEGER DEFAULT 1
      ,p_upper_bound IN            PLS_INTEGER DEFAULT NULL
   );
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE reverse_ordinate_rotation(
       p_input       IN OUT NOCOPY MDSYS.SDO_ORDINATE_ARRAY
      ,p_lower_bound IN            PLS_INTEGER DEFAULT 1
      ,p_upper_bound IN            PLS_INTEGER DEFAULT NULL
      ,p_num_dims    IN            PLS_INTEGER DEFAULT 2
   );
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION gz_split(
      p_str              IN VARCHAR2
     ,p_regex            IN VARCHAR2
     ,p_match            IN VARCHAR2 DEFAULT NULL
     ,p_end              IN NUMBER   DEFAULT 0
     ,p_trim             IN VARCHAR2 DEFAULT 'FALSE'
  ) RETURN MDSYS.SDO_STRING2_ARRAY DETERMINISTIC;
  
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION index_exists(
       p_owner            IN  VARCHAR2 DEFAULT NULL
      ,p_table_name       IN  VARCHAR2
      ,p_column_name      IN  VARCHAR2
   ) RETURN VARCHAR2;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION append_hole_to_polygon(
       p_input           IN MDSYS.SDO_GEOMETRY   
      ,p_hole            IN MDSYS.SDO_GEOMETRY
   ) RETURN MDSYS.SDO_GEOMETRY;

END dz_topo_util;
/

GRANT EXECUTE ON dz_topo_util TO public;


--*************************--
PROMPT DZ_TOPO_UTIL.pkb;

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


--*************************--
PROMPT DZ_TOPO_EDGE.tps;

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


--*************************--
PROMPT DZ_TOPO_EDGE.tpb;

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


--*************************--
PROMPT DZ_TOPO_EDGE_LIST.tps;

CREATE OR REPLACE TYPE dz_topo_edge_list FORCE                                
AS 
TABLE OF dz_topo_edge;
/

GRANT EXECUTE ON dz_topo_edge_list TO PUBLIC;


--*************************--
PROMPT DZ_TOPO_RING.tps;

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


--*************************--
PROMPT DZ_TOPO_RING.tpb;

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


--*************************--
PROMPT DZ_TOPO_RING_LIST.tps;

CREATE OR REPLACE TYPE dz_topo_ring_list FORCE                                       
AS 
TABLE OF dz_topo_ring;
/

GRANT EXECUTE ON dz_topo_ring_list TO PUBLIC;


--*************************--
PROMPT DZ_TOPO_MAP_MGR.tps;

CREATE OR REPLACE TYPE dz_topo_map_mgr FORCE
AUTHID CURRENT_USER
AS OBJECT (
    topo_map_name     VARCHAR2(4000 Char)
   ,allow_updates     VARCHAR2(4000 Char)
   ,topology_owner    VARCHAR2(30 Char)
   ,topology_name     VARCHAR2(20 Char)
   ,topo_map_window   MDSYS.SDO_GEOMETRY
   ,number_of_edges   NUMBER
   ,number_of_nodes   NUMBER
   ,number_of_faces   NUMBER
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,CONSTRUCTOR FUNCTION dz_topo_map_mgr
    RETURN SELF AS RESULT
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,CONSTRUCTOR FUNCTION dz_topo_map_mgr(
        p_topology_owner  IN  VARCHAR2 DEFAULT NULL
       ,p_topology_name   IN  VARCHAR2
       ,p_topo_map_name   IN  VARCHAR2 DEFAULT NULL
       ,p_allow_updates   IN  VARCHAR2 DEFAULT 'TRUE'
       ,p_number_of_edges IN  NUMBER   DEFAULT 100
       ,p_number_of_nodes IN  NUMBER   DEFAULT 80
       ,p_number_of_faces IN  NUMBER   DEFAULT 30
    ) RETURN SELF AS RESULT
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,CONSTRUCTOR FUNCTION dz_topo_map_mgr(
        p_window_sdo      IN  MDSYS.SDO_GEOMETRY
       ,p_window_padding  IN  NUMBER   DEFAULT 0
       ,p_topology_owner  IN  VARCHAR2 DEFAULT NULL
       ,p_topology_name   IN  VARCHAR2
       ,p_topo_map_name   IN  VARCHAR2 DEFAULT NULL
       ,p_allow_updates   IN  VARCHAR2 DEFAULT 'TRUE'
       ,p_number_of_edges IN  NUMBER   DEFAULT 100
       ,p_number_of_nodes IN  NUMBER   DEFAULT 80
       ,p_number_of_faces IN  NUMBER   DEFAULT 30
    ) RETURN SELF AS RESULT
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,CONSTRUCTOR FUNCTION dz_topo_map_mgr(
        p_window_sdo      IN  MDSYS.SDO_GEOMETRY_ARRAY
       ,p_window_padding  IN  NUMBER   DEFAULT 0
       ,p_topology_owner  IN  VARCHAR2 DEFAULT NULL
       ,p_topology_name   IN  VARCHAR2
       ,p_topo_map_name   IN  VARCHAR2 DEFAULT NULL
       ,p_allow_updates   IN  VARCHAR2 DEFAULT 'TRUE'
       ,p_number_of_edges IN  NUMBER   DEFAULT 100
       ,p_number_of_nodes IN  NUMBER   DEFAULT 80
       ,p_number_of_faces IN  NUMBER   DEFAULT 30
    ) RETURN SELF AS RESULT
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,CONSTRUCTOR FUNCTION dz_topo_map_mgr(
        p_xmin            IN  NUMBER
       ,p_ymin            IN  NUMBER
       ,p_xmax            IN  NUMBER
       ,p_ymax            IN  NUMBER
       ,p_topology_owner  IN  VARCHAR2 DEFAULT NULL
       ,p_topology_name   IN  VARCHAR2
       ,p_topo_map_name   IN  VARCHAR2 DEFAULT NULL
       ,p_allow_updates   IN  VARCHAR2 DEFAULT 'TRUE'
       ,p_number_of_edges IN  NUMBER   DEFAULT 100
       ,p_number_of_nodes IN  NUMBER   DEFAULT 80
       ,p_number_of_faces IN  NUMBER   DEFAULT 30
    ) RETURN SELF AS RESULT
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER PROCEDURE create_topo_map
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER PROCEDURE load_topo_map
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION create_feature(
       p_table_name       IN  VARCHAR2
      ,p_column_name      IN  VARCHAR2
      ,p_geometry         IN  MDSYS.SDO_GEOMETRY
    ) RETURN MDSYS.SDO_TOPO_GEOMETRY
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER PROCEDURE commit_topo_map(
        p_validate        IN  VARCHAR2 DEFAULT 'TRUE'
       ,p_validate_level  IN  NUMBER   DEFAULT 1
    )
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION validate_topo_map(
      p_validate_level    IN  NUMBER DEFAULT 1
    ) RETURN VARCHAR2
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER PROCEDURE drop_topo_map
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER PROCEDURE clear_topo_map
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER PROCEDURE java_memory(
        p_bytes     IN  NUMBER DEFAULT NULL
       ,p_gigs      IN  NUMBER DEFAULT NULL
    )
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER PROCEDURE create_indexes
     
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION get_topology_owner
    RETURN VARCHAR2
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION get_topology_name
    RETURN VARCHAR2
     
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION verify_topology
    RETURN VARCHAR
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION verify_topo_map
    RETURN VARCHAR2
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION current_updateable_topo_map
    RETURN VARCHAR2
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION get_topo_maps
    RETURN MDSYS.SDO_STRING2_ARRAY
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER PROCEDURE purge_all_topo_maps
        
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER PROCEDURE initialize_topo_basics(
        p_topology_owner  IN  VARCHAR2 DEFAULT NULL
       ,p_topology_name   IN  VARCHAR2
       ,p_topo_map_name   IN  VARCHAR2 DEFAULT NULL
       ,p_allow_updates   IN  VARCHAR2 DEFAULT 'TRUE'
       ,p_number_of_edges IN  NUMBER   DEFAULT 100
       ,p_number_of_nodes IN  NUMBER   DEFAULT 80
       ,p_number_of_faces IN  NUMBER   DEFAULT 30
    )
    
);
/

GRANT EXECUTE ON dz_topo_map_mgr TO PUBLIC;


--*************************--
PROMPT DZ_TOPO_MAP_MGR.tpb;

CREATE OR REPLACE TYPE BODY dz_topo_map_mgr
AS

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   CONSTRUCTOR FUNCTION dz_topo_map_mgr
   RETURN SELF AS RESULT
   AS
   BEGIN
      RETURN;
      
   END dz_topo_map_mgr;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   CONSTRUCTOR FUNCTION dz_topo_map_mgr(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL
      ,p_topology_name   IN  VARCHAR2
      ,p_topo_map_name   IN  VARCHAR2 DEFAULT NULL
      ,p_allow_updates   IN  VARCHAR2 DEFAULT 'TRUE'
      ,p_number_of_edges IN  NUMBER   DEFAULT 100
      ,p_number_of_nodes IN  NUMBER   DEFAULT 80
      ,p_number_of_faces IN  NUMBER   DEFAULT 30
   ) RETURN SELF AS RESULT
   AS
   BEGIN
      
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_topology_name IS NULL
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topology name required');
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Set up the object
      --------------------------------------------------------------------------
      initialize_topo_basics(
          p_topology_owner  => p_topology_owner
         ,p_topology_name   => p_topology_name
         ,p_topo_map_name   => p_topo_map_name
         ,p_allow_updates   => p_allow_updates
         ,p_number_of_edges => p_number_of_edges
         ,p_number_of_nodes => p_number_of_nodes
         ,p_number_of_faces => p_number_of_faces
      );
      
      IF self.verify_topology() = 'FALSE'
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topology not found');
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Create new topo map 
      --------------------------------------------------------------------------
      self.create_topo_map();
      self.load_topo_map();
      
      RETURN;
      
   END dz_topo_map_mgr;
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   CONSTRUCTOR FUNCTION dz_topo_map_mgr(
       p_window_sdo      IN  MDSYS.SDO_GEOMETRY
      ,p_window_padding  IN  NUMBER   DEFAULT 0
      ,p_topology_owner  IN  VARCHAR2 DEFAULT NULL
      ,p_topology_name   IN  VARCHAR2
      ,p_topo_map_name   IN  VARCHAR2 DEFAULT NULL
      ,p_allow_updates   IN  VARCHAR2 DEFAULT 'TRUE'
      ,p_number_of_edges IN  NUMBER   DEFAULT 100
      ,p_number_of_nodes IN  NUMBER   DEFAULT 80
      ,p_number_of_faces IN  NUMBER   DEFAULT 30
   ) RETURN SELF AS RESULT
   AS
      num_window_padding NUMBER := p_window_padding;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_window_sdo IS NULL
      THEN
         RAISE_APPLICATION_ERROR(-20001,'sdo window required');
         
      END IF;
      
      IF p_topology_name IS NULL
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topology name required');
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Verify topology exists and is writeable
      --------------------------------------------------------------------------
      initialize_topo_basics(
          p_topology_owner  => p_topology_owner
         ,p_topology_name   => p_topology_name
         ,p_topo_map_name   => p_topo_map_name
         ,p_allow_updates   => p_allow_updates
         ,p_number_of_edges => p_number_of_edges
         ,p_number_of_nodes => p_number_of_nodes
         ,p_number_of_faces => p_number_of_faces
      );
      
      IF self.verify_topology() = 'FALSE'
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topology not found');
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Get MBR of input sdo and add padding.
      --------------------------------------------------------------------------
      IF num_window_padding IS NULL
      THEN
         num_window_padding := 0;
         
      END IF;
      
      self.topo_map_window := MDSYS.SDO_GEOM.SDO_MBR(
         geom => p_window_sdo
      );
      
      self.topo_map_window.SDO_ORDINATES(1) := 
         self.topo_map_window.SDO_ORDINATES(1) - num_window_padding;
         
      self.topo_map_window.SDO_ORDINATES(2) := 
         self.topo_map_window.SDO_ORDINATES(2) - num_window_padding;
         
      self.topo_map_window.SDO_ORDINATES(3) := 
         self.topo_map_window.SDO_ORDINATES(3) + num_window_padding;
         
      self.topo_map_window.SDO_ORDINATES(4) := 
         self.topo_map_window.SDO_ORDINATES(4) + num_window_padding;
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Create new topo map 
      --------------------------------------------------------------------------
      self.create_topo_map();
      self.load_topo_map();
      
      RETURN;
      
   END dz_topo_map_mgr;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   CONSTRUCTOR FUNCTION dz_topo_map_mgr(
       p_window_sdo      IN  MDSYS.SDO_GEOMETRY_ARRAY
      ,p_window_padding  IN  NUMBER   DEFAULT 0
      ,p_topology_owner  IN  VARCHAR2 DEFAULT NULL
      ,p_topology_name   IN  VARCHAR2
      ,p_topo_map_name   IN  VARCHAR2 DEFAULT NULL
      ,p_allow_updates   IN  VARCHAR2 DEFAULT 'TRUE'
      ,p_number_of_edges IN  NUMBER   DEFAULT 100
      ,p_number_of_nodes IN  NUMBER   DEFAULT 80
      ,p_number_of_faces IN  NUMBER   DEFAULT 30
   ) RETURN SELF AS RESULT
   AS
      num_window_padding NUMBER := p_window_padding;
      
   BEGIN
      
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_window_sdo IS NULL
      OR p_window_sdo.COUNT = 0
      THEN
         RAISE_APPLICATION_ERROR(-20001,'sdo window required');
         
      END IF;
      
      IF p_topology_name IS NULL
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topology name required');
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Verify topology exists and is writeable
      --------------------------------------------------------------------------
      initialize_topo_basics(
          p_topology_owner  => p_topology_owner
         ,p_topology_name   => p_topology_name
         ,p_topo_map_name   => p_topo_map_name
         ,p_allow_updates   => p_allow_updates
         ,p_number_of_edges => p_number_of_edges
         ,p_number_of_nodes => p_number_of_nodes
         ,p_number_of_faces => p_number_of_faces
      );
      
      IF self.verify_topology() = 'FALSE'
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topology not found');
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Get MBR of input sdo and add padding.
      --------------------------------------------------------------------------
      IF num_window_padding IS NULL
      THEN
         num_window_padding := 0;
         
      END IF;
      
      SELECT
      MDSYS.SDO_AGGR_MBR(
         MDSYS.SDO_GEOMETRY(
             a.sdo_gtype
            ,a.sdo_srid
            ,a.sdo_point
            ,a.sdo_elem_info
            ,a.sdo_ordinates
         )
      )
      INTO self.topo_map_window
      FROM
      TABLE(p_window_sdo) a;
      
      self.topo_map_window.SDO_ORDINATES(1) := 
         self.topo_map_window.SDO_ORDINATES(1) - num_window_padding;
         
      self.topo_map_window.SDO_ORDINATES(2) := 
         self.topo_map_window.SDO_ORDINATES(2) - num_window_padding;
         
      self.topo_map_window.SDO_ORDINATES(3) := 
         self.topo_map_window.SDO_ORDINATES(3) + num_window_padding;
         
      self.topo_map_window.SDO_ORDINATES(4) := 
         self.topo_map_window.SDO_ORDINATES(4) + num_window_padding;
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Create new topo map 
      --------------------------------------------------------------------------
      self.create_topo_map();
      self.load_topo_map();
      
      RETURN;
      
   END dz_topo_map_mgr;
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   CONSTRUCTOR FUNCTION dz_topo_map_mgr(
       p_xmin            IN  NUMBER
      ,p_ymin            IN  NUMBER
      ,p_xmax            IN  NUMBER
      ,p_ymax            IN  NUMBER
      ,p_topology_owner  IN  VARCHAR2 DEFAULT NULL
      ,p_topology_name   IN  VARCHAR2
      ,p_topo_map_name   IN  VARCHAR2 DEFAULT NULL
      ,p_allow_updates   IN  VARCHAR2 DEFAULT 'TRUE'
      ,p_number_of_edges IN  NUMBER   DEFAULT 100
      ,p_number_of_nodes IN  NUMBER   DEFAULT 80
      ,p_number_of_faces IN  NUMBER   DEFAULT 30
   ) RETURN SELF AS RESULT
   AS
   BEGIN
      
       --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_xmin IS NULL
      OR p_ymin IS NULL
      OR p_xmax IS NULL
      OR p_ymax IS NULL
      THEN
         RAISE_APPLICATION_ERROR(-20001,'sdo window coordiantes required');
         
      END IF;
      
      IF p_topology_name IS NULL
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topology name required');
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Verify topology exists and is writeable
      --------------------------------------------------------------------------
      initialize_topo_basics(
          p_topology_owner  => p_topology_owner
         ,p_topology_name   => p_topology_name
         ,p_topo_map_name   => p_topo_map_name
         ,p_allow_updates   => p_allow_updates
         ,p_number_of_edges => p_number_of_edges
         ,p_number_of_nodes => p_number_of_nodes
         ,p_number_of_faces => p_number_of_faces
      );
      
      IF self.verify_topology() = 'FALSE'
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topology not found');
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Get MBR of input sdo and add padding.
      --------------------------------------------------------------------------
      self.topo_map_window := MDSYS.SDO_GEOMETRY(
          2003
         ,NULL
         ,NULL
         ,MDSYS.SDO_ELEM_INFO_ARRAY(1,1003,3)
         ,MDSYS.SDO_ORDINATE_ARRAY(p_xmin,p_ymin,p_xmax,p_ymax)
      );
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Create new topo map 
      --------------------------------------------------------------------------
      self.create_topo_map();
      self.load_topo_map();
      
      RETURN;
      
   END dz_topo_map_mgr;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER PROCEDURE create_topo_map
   AS
      str_updateable_topo_map VARCHAR2(4000 Char);
      
   BEGIN
      
      /* This logic does not work due to some kind of bug  
      
      str_updateable_topo_map := self.current_updateable_topo_map();
   
      IF str_updateable_topo_map IS NOT NULL
      THEN
         MDSYS.SDO_TOPO_MAP.DROP_TOPO_MAP(
            topo_map => str_updateable_topo_map
         );
         
      END IF;
      
      As a result just drop all topo maps in the sesssion */
      
      self.purge_all_topo_maps();
      
      MDSYS.SDO_TOPO_MAP.CREATE_TOPO_MAP(
           topology        => self.topology_name
          ,topo_map        => self.topo_map_name
          ,number_of_edges => self.number_of_edges
          ,number_of_nodes => self.number_of_nodes
          ,number_of_faces => self.number_of_faces
      );
      
   END create_topo_map;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER PROCEDURE load_topo_map
   AS
   BEGIN
   
      IF self.verify_topo_map() <> 'VALID'
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topo map has not been created');
         
      END IF;
      
      IF self.topo_map_window IS NULL
      THEN
         MDSYS.SDO_TOPO_MAP.LOAD_TOPO_MAP(
             topo_map      => self.topo_map_name
            ,allow_updates => self.allow_updates
            ,build_indexes => 'TRUE'
         );
        
      ELSE
         MDSYS.SDO_TOPO_MAP.LOAD_TOPO_MAP(
              topo_map      => self.topo_map_name
             ,xmin          => self.topo_map_window.SDO_ORDINATES(1)
             ,ymin          => self.topo_map_window.SDO_ORDINATES(2)
             ,xmax          => self.topo_map_window.SDO_ORDINATES(3)
             ,ymax          => self.topo_map_window.SDO_ORDINATES(4)
             ,allow_updates => self.allow_updates
             ,build_indexes => 'TRUE'
          );
      
      END IF;
       
   END load_topo_map;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION create_feature(
       p_table_name       IN  VARCHAR2
      ,p_column_name      IN  VARCHAR2
      ,p_geometry         IN  MDSYS.SDO_GEOMETRY
   ) RETURN MDSYS.SDO_TOPO_GEOMETRY
   AS
      topo_output MDSYS.SDO_TOPO_GEOMETRY;
      
   BEGIN
   
      topo_output := MDSYS.SDO_TOPO_MAP.CREATE_FEATURE(
          self.topology_name 
         ,p_table_name
         ,p_column_name
         ,p_geometry
      );
      
      RETURN topo_output;
   
   END create_feature;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER PROCEDURE commit_topo_map(
       p_validate        IN  VARCHAR2 DEFAULT 'TRUE'
      ,p_validate_level  IN  NUMBER DEFAULT 1
   )
   AS
      str_validate VARCHAR2(4000 Char);
      
   BEGIN
      
      IF self.verify_topo_map() <> 'VALID'
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topo map has not been created');
         
      END IF;
      
      IF p_validate = 'TRUE'
      THEN
         str_validate := self.validate_topo_map(
            p_validate_level => p_validate_level
         );
         
         IF str_validate <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'topo map does not validate');
            
         END IF;
         
      END IF; 
      
      MDSYS.SDO_TOPO_MAP.COMMIT_TOPO_MAP;
      
   END commit_topo_map;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION validate_topo_map(
      p_validate_level  IN  NUMBER DEFAULT 1
   ) RETURN VARCHAR2
   AS
   BEGIN
   
      RETURN MDSYS.SDO_TOPO_MAP.VALIDATE_TOPO_MAP(
          self.topo_map_name
         ,p_validate_level
      );
      
   END validate_topo_map;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER PROCEDURE drop_topo_map
   AS
   BEGIN
   
      IF self.verify_topo_map() = 'VALID'
      THEN
         MDSYS.SDO_TOPO_MAP.DROP_TOPO_MAP(
            topo_map => self.topology_name
         );
         
      END IF;
         
   END drop_topo_map;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER PROCEDURE clear_topo_map
   AS
   BEGIN
   
      MDSYS.SDO_TOPO_MAP.CLEAR_TOPO_MAP(
         topo_map => self.topo_map_name
      );
      
   END clear_topo_map;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER PROCEDURE java_memory(
       p_bytes     IN  NUMBER DEFAULT NULL
      ,p_gigs      IN  NUMBER DEFAULT NULL
   )
   AS
      num_bytes NUMBER := p_bytes;
      
   BEGIN
   
      IF num_bytes IS NULL
      THEN
         num_bytes := p_gigs * 1073741824;
         
      END IF;   
   
      MDSYS.SDO_TOPO_MAP.SET_MAX_MEMORY_SIZE(
         num_bytes
      );
   
   END java_memory;
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER PROCEDURE create_indexes
   AS
   BEGIN
    
      MDSYS.SDO_TOPO_MAP.CREATE_EDGE_INDEX(
         self.topo_map_name
      );
      MDSYS.SDO_TOPO_MAP.CREATE_FACE_INDEX(
         self.topo_map_name
      );
      
   END create_indexes;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION get_topology_owner
   RETURN VARCHAR2
   AS
   BEGIN
      RETURN self.topology_owner;
      
   END get_topology_owner;
     
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION get_topology_name
   RETURN VARCHAR2
   AS
   BEGIN
      RETURN self.topology_name;
      
   END get_topology_name;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION verify_topology
   RETURN VARCHAR
   AS
      int_counter PLS_INTEGER;
      
   BEGIN
      
      IF self.topology_owner = USER
      OR self.allow_updates = 'FALSE'
      THEN
         SELECT
         COUNT(*)
         INTO int_counter
         FROM
         all_tables a
         WHERE
             a.owner = self.topology_owner
         AND a.table_name IN (
             self.topology_name || '_FACE$'
            ,self.topology_name || '_NODE$'
            ,self.topology_name || '_EDGE$'
            ,self.topology_name || '_RELATION$'
         );
      
      ELSE
         RAISE_APPLICATION_ERROR(-20001,'updateable nonowner check not implemented');
         -- Add logic against all_tab_privs to check that nonowner has insert, update, etc
         -- on topology
      
      END IF;
      
      IF int_counter = 4
      THEN
         RETURN 'TRUE';
         
      ELSE
         RETURN 'FALSE';
         
      END IF;
   
   END verify_topology;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION current_updateable_topo_map
   RETURN VARCHAR2
   AS
      ary_topo_maps MDSYS.SDO_STRING2_ARRAY;
      ary_parts     MDSYS.SDO_STRING2_ARRAY;
      
   BEGIN
     
      ary_topo_maps := self.get_topo_maps();
   
      FOR i IN 1 .. ary_topo_maps.COUNT
      LOOP
         ary_parts := dz_topo_util.gz_split(
             p_str   => ary_topo_maps(i)
            ,p_regex => ','
            ,p_trim  => 'TRUE'
         );
         
         IF LOWER(ary_parts(3)) IN ('updatable','updateable')
         THEN
            RETURN ary_parts(1);
         
         END IF;
         
      END LOOP;
   
      RETURN NULL;
   
   END current_updateable_topo_map;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION get_topo_maps
   RETURN MDSYS.SDO_STRING2_ARRAY
   AS
      str_topo_maps VARCHAR2(4000 Char);
      ary_topo_maps MDSYS.SDO_STRING2_ARRAY;
      
   BEGIN
   
      str_topo_maps := MDSYS.SDO_TOPO_MAP.LIST_TOPO_MAPS();
      --dbms_output.put_line(MDSYS.SDO_TOPO_MAP.LIST_TOPO_MAPS());
      
      IF str_topo_maps IS NULL
      THEN
         RETURN NULL;
         
      END IF;
      
      ary_topo_maps := dz_topo_util.gz_split(
          p_str   => str_topo_maps
         ,p_regex => '\)\, \('
         ,p_trim  => 'TRUE'
      );
      
      FOR i IN 1 .. ary_topo_maps.COUNT
      LOOP
         ary_topo_maps(i) := REPLACE(ary_topo_maps(i),'(','');
         ary_topo_maps(i) := REPLACE(ary_topo_maps(i),')','');
         --dbms_output.put_line(ary_topo_maps(i));
      
      END LOOP;
      
      RETURN ary_topo_maps;
   
   END get_topo_maps;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER PROCEDURE purge_all_topo_maps
   AS
      ary_topo_maps MDSYS.SDO_STRING2_ARRAY;
      ary_parts     MDSYS.SDO_STRING2_ARRAY;
      
   BEGIN
   
      ary_topo_maps := self.get_topo_maps();
      
      IF ary_topo_maps IS NULL
      OR ary_topo_maps.COUNT = 0
      THEN
         RETURN;
         
      END IF;
      
      FOR i IN 1 .. ary_topo_maps.COUNT
      LOOP
         ary_parts := dz_topo_util.gz_split(
             p_str   => ary_topo_maps(i)
            ,p_regex => ','
            ,p_trim  => 'TRUE'
         );
         
         MDSYS.SDO_TOPO_MAP.DROP_TOPO_MAP(
            topo_map => ary_parts(1)
         );
         
      END LOOP;
      
   END purge_all_topo_maps;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION verify_topo_map
   RETURN VARCHAR2
   AS
      str_topomaps   VARCHAR2(4000 Char);

   BEGIN
   
      str_topomaps := MDSYS.SDO_TOPO_MAP.LIST_TOPO_MAPS();
      
      IF str_topomaps IS NULL
      OR INSTR(str_topomaps,self.topo_map_name) = 0
      THEN
         RETURN 'INVALID';
         
      ELSE
         RETURN 'VALID';
         
      END IF;
      
   END verify_topo_map;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER PROCEDURE initialize_topo_basics(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL
      ,p_topology_name   IN  VARCHAR2
      ,p_topo_map_name   IN  VARCHAR2 DEFAULT NULL
      ,p_allow_updates   IN  VARCHAR2 DEFAULT 'TRUE'
      ,p_number_of_edges IN  NUMBER   DEFAULT 100
      ,p_number_of_nodes IN  NUMBER   DEFAULT 80
      ,p_number_of_faces IN  NUMBER   DEFAULT 30
   )
   AS
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Verify topology exists and is writeable
      --------------------------------------------------------------------------
      IF p_topology_owner IS NULL
      THEN
         self.topology_owner := USER;
         
      ELSE
         self.topology_owner := p_topology_owner;
         
      END IF;
      
      self.topology_name := p_topology_name;
      
      IF p_allow_updates IS NULL
      THEN
         self.allow_updates := 'TRUE';
         
      ELSE
         self.allow_updates := p_allow_updates;
         
      END IF;
      
      IF p_topo_map_name IS NULL
      THEN
         self.topo_map_name := 'DZ' || upper(RAWTOHEX(SYS_GUID()));
         
      ELSE
         self.topo_map_name := p_topo_map_name;
         
      END IF;
      
      IF p_number_of_edges IS NULL
      THEN
         self.number_of_edges := 100;
         
      ELSE
         self.number_of_edges := p_number_of_edges;
         
      END IF;
      
      IF p_number_of_nodes IS NULL
      THEN
         self.number_of_nodes := 80;
         
      ELSE
         self.number_of_nodes := p_number_of_nodes;
         
      END IF;
      
      IF p_number_of_faces IS NULL
      THEN
         self.number_of_faces := 30;
         
      ELSE
         self.number_of_faces := p_number_of_faces;
         
      END IF;
      
   END initialize_topo_basics;
   
END;
/


--*************************--
PROMPT DZ_TOPO_LAYER.tps;

CREATE OR REPLACE TYPE dz_topo_layer FORCE
AUTHID CURRENT_USER
AS OBJECT (
    topology_owner    VARCHAR2(30 Char)
   ,topology_name     VARCHAR2(20 Char)
   ,topology_id       NUMBER
   ,topology_srid     NUMBER
   ,tolerance         NUMBER
   ,table_owner       VARCHAR2(30 Char)
   ,table_name        VARCHAR2(30 Char)
   ,column_name       VARCHAR2(30 Char)
   ,tg_layer_id       NUMBER
   ,tg_layer_type     VARCHAR2(255 Char)
   ,tg_layer_level    NUMBER
   ,topo_map_mgr      dz_topo_map_mgr
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,CONSTRUCTOR FUNCTION dz_topo_layer
    RETURN SELF AS RESULT
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,CONSTRUCTOR FUNCTION dz_topo_layer(
        p_table_name   IN  VARCHAR2
       ,p_column_name  IN  VARCHAR2
    ) RETURN SELF AS RESULT
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,CONSTRUCTOR FUNCTION dz_topo_layer(
        p_table_name   IN  VARCHAR2
       ,p_column_name  IN  VARCHAR2
       ,p_topo_map_mgr IN  dz_topo_map_mgr
    ) RETURN SELF AS RESULT
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,CONSTRUCTOR FUNCTION dz_topo_layer(
        p_topo_geom    IN  MDSYS.SDO_TOPO_GEOMETRY
    ) RETURN SELF AS RESULT
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,CONSTRUCTOR FUNCTION dz_topo_layer(
        p_topology_owner    IN  VARCHAR2
       ,p_topology_name     IN  VARCHAR2
       ,p_topology_id       IN  NUMBER
       ,p_topology_srid     IN  NUMBER
       ,p_tolerance         IN  NUMBER
       ,p_table_owner       IN  VARCHAR2
       ,p_table_name        IN  VARCHAR2
       ,p_column_name       IN  VARCHAR2
       ,p_tg_layer_id       IN  NUMBER
       ,p_tg_layer_type     IN  VARCHAR2
       ,p_tg_layer_level    IN  NUMBER
    ) RETURN SELF AS RESULT
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION valid
    RETURN VARCHAR2
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER PROCEDURE harvest_topo_metadata
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION get_topo_map_mgr
    RETURN dz_topo_map_mgr
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER PROCEDURE set_topo_map_mgr(
      p_topo_map_mgr IN  dz_topo_map_mgr
    )
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER PROCEDURE java_memory(
        p_bytes     IN  NUMBER DEFAULT NULL
       ,p_gigs      IN  NUMBER DEFAULT NULL
    )
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION create_feature(
        self           IN OUT dz_topo_layer
       ,p_input        IN     MDSYS.SDO_GEOMETRY
    ) RETURN MDSYS.SDO_TOPO_GEOMETRY
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION add_polygon_to_layer(
        self           IN OUT dz_topo_layer
       ,p_input        IN     MDSYS.SDO_GEOMETRY
    ) RETURN MDSYS.SDO_TOPO_GEOMETRY
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER PROCEDURE commit_topo_map(
        p_validate        IN  VARCHAR2 DEFAULT 'TRUE'
       ,p_validate_level  IN  NUMBER   DEFAULT 1
    )
   
);
/

GRANT EXECUTE ON dz_topo_layer TO PUBLIC;


--*************************--
PROMPT DZ_TOPO_LAYER.tpb;

CREATE OR REPLACE TYPE BODY dz_topo_layer
AS

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   CONSTRUCTOR FUNCTION dz_topo_layer
   RETURN SELF AS RESULT
   AS
   BEGIN
      RETURN;
   
   END dz_topo_layer;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   CONSTRUCTOR FUNCTION dz_topo_layer(
       p_table_name   IN  VARCHAR2
      ,p_column_name  IN  VARCHAR2
   ) RETURN SELF AS RESULT
   AS
   BEGIN
      
      self.table_name := p_table_name;
      self.column_name := p_column_name;
      
      self.harvest_topo_metadata();
      
      RETURN;
      
   END dz_topo_layer;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   CONSTRUCTOR FUNCTION dz_topo_layer(
       p_table_name   IN  VARCHAR2
      ,p_column_name  IN  VARCHAR2
      ,p_topo_map_mgr IN  dz_topo_map_mgr
   ) RETURN SELF AS RESULT
   AS
   BEGIN
      self.table_name := p_table_name;
      self.column_name := p_column_name;
      
      self.harvest_topo_metadata();
      
      self.topo_map_mgr := p_topo_map_mgr;
      
      RETURN;
      
   END dz_topo_layer;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   CONSTRUCTOR FUNCTION dz_topo_layer(
      p_topo_geom    IN  MDSYS.SDO_TOPO_GEOMETRY
   ) RETURN SELF AS RESULT
   AS
   BEGIN
      self.tg_layer_id  := p_topo_geom.tg_id;
      self.topology_id  := p_topo_geom.topology_id;
      
      self.harvest_topo_metadata();
      
      RETURN;
      
   END dz_topo_layer;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   CONSTRUCTOR FUNCTION dz_topo_layer(
       p_topology_owner    IN  VARCHAR2
      ,p_topology_name     IN  VARCHAR2
      ,p_topology_id       IN  NUMBER
      ,p_topology_srid     IN  NUMBER
      ,p_tolerance         IN  NUMBER
      ,p_table_owner       IN  VARCHAR2
      ,p_table_name        IN  VARCHAR2
      ,p_column_name       IN  VARCHAR2
      ,p_tg_layer_id       IN  NUMBER
      ,p_tg_layer_type     IN  VARCHAR2
      ,p_tg_layer_level    IN  NUMBER
   ) RETURN SELF AS RESULT
   AS
   BEGIN
      self.topology_owner    := p_topology_owner;
      self.topology_name     := p_topology_name;
      self.topology_id       := p_topology_id;
      self.topology_srid     := p_topology_srid;
      self.tolerance         := p_tolerance;
      self.table_owner       := p_table_owner;
      self.table_name        := p_table_name;
      self.column_name       := p_column_name;
      self.tg_layer_id       := p_tg_layer_id;
      self.tg_layer_type     := p_tg_layer_type;
      self.tg_layer_level    := p_tg_layer_level;
      
      RETURN;
   
   END dz_topo_layer;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION valid
   RETURN VARCHAR2
   AS
   BEGIN
      IF self.table_name IS NULL
      OR self.tg_layer_id IS NULL
      THEN
         RETURN 'FALSE';
         
      ELSE
         RETURN 'TRUE';
         
      END IF;
      
   END valid;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION get_topo_map_mgr
   RETURN dz_topo_map_mgr
   AS
   BEGIN
   
      IF self.topo_map_mgr IS NOT NULL
      THEN
         RETURN self.topo_map_mgr;
         
      ELSE
         RAISE_APPLICATION_ERROR(-20001,'topo map mgr not active');
         
      END IF;
   
   END get_topo_map_mgr;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER PROCEDURE set_topo_map_mgr(
     p_topo_map_mgr  IN  dz_topo_map_mgr
   )
   AS
   BEGIN
   
      self.topo_map_mgr := p_topo_map_mgr;
   
   END set_topo_map_mgr;
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER PROCEDURE harvest_topo_metadata
   AS
   BEGIN
      
      IF self.table_name IS NOT NULL
      AND self.column_name IS NOT NULL
      THEN
         SELECT
          a.owner
         ,a.topology
         ,a.topology_id
         ,a.srid
         ,a.tolerance
         ,a.table_owner
         ,a.tg_layer_id
         ,a.tg_layer_type
         ,a.tg_layer_level
         INTO
          self.topology_owner
         ,self.topology_name
         ,self.topology_id
         ,self.topology_srid
         ,self.tolerance
         ,self.table_owner
         ,self.tg_layer_id
         ,self.tg_layer_type
         ,self.tg_layer_level
         FROM
         all_sdo_topo_metadata a
         WHERE
             a.table_name = self.table_name
         AND a.column_name = self.column_name;
         
      ELSIF self.tg_layer_id IS NOT NULL
      AND self.topology_id IS NOT NULL
      THEN
      
         SELECT
          a.owner
         ,a.topology
         ,a.topology_id
         ,a.srid
         ,a.tolerance
         ,a.table_owner
         ,a.tg_layer_id
         ,a.tg_layer_type
         ,a.tg_layer_level
         INTO
          self.topology_owner
         ,self.topology_name
         ,self.topology_id
         ,self.topology_srid
         ,self.tolerance
         ,self.table_owner
         ,self.tg_layer_id
         ,self.tg_layer_type
         ,self.tg_layer_level
         FROM
         all_sdo_topo_metadata a
         WHERE
             a.topology_id = self.topology_id
         AND a.tg_layer_id = self.tg_layer_id;
      
      END IF;
   
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         NULL;
         
      WHEN OTHERS
      THEN
         RAISE;
      
   END harvest_topo_metadata;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER PROCEDURE java_memory(
       p_bytes     IN  NUMBER DEFAULT NULL
      ,p_gigs      IN  NUMBER DEFAULT NULL
   )
   AS
      num_bytes NUMBER := p_bytes;
      
   BEGIN
   
      IF num_bytes IS NULL
      THEN
         num_bytes := p_gigs * 1073741824;
         
      END IF;   
   
      MDSYS.SDO_TOPO_MAP.SET_MAX_MEMORY_SIZE(num_bytes);
   
   END java_memory;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION create_feature(
       self           IN OUT dz_topo_layer
      ,p_input        IN     MDSYS.SDO_GEOMETRY
   ) RETURN MDSYS.SDO_TOPO_GEOMETRY
   AS
      topo_output MDSYS.SDO_TOPO_GEOMETRY;
      
   BEGIN
   
      IF self.topo_map_mgr IS NULL
      OR self.topo_map_mgr.allow_updates <> 'TRUE'
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topo map mgr not active');
         
      END IF;
      
      topo_output := self.topo_map_mgr.create_feature(
          p_table_name    => self.table_name
         ,p_column_name   => self.column_name
         ,p_geometry      => p_input
      );
      
      RETURN topo_output;
   
   END create_feature;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION add_polygon_to_layer(
       self           IN OUT dz_topo_layer
      ,p_input        IN     MDSYS.SDO_GEOMETRY
   ) RETURN MDSYS.SDO_TOPO_GEOMETRY
   AS
       obj_output   MDSYS.SDO_TOPO_GEOMETRY;
       ary_number   MDSYS.SDO_NUMBER_ARRAY;
       ary_topo_obj MDSYS.SDO_TOPO_OBJECT_ARRAY;
       
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check that topology layer is layer 0
      --------------------------------------------------------------------------
      IF self.tg_layer_id IS NULL
      THEN
         self.harvest_topo_metadata();
         IF self.tg_layer_id IS NULL
         THEN
            RAISE_APPLICATION_ERROR(-20001,'topology layer invalid');
            
         END IF;
         
      END IF;
      
      IF self.tg_layer_level <> 0
      THEN
         RAISE_APPLICATION_ERROR(-20001,'error topo hierarchy support not implemented');
      
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Check that topology layer is polygon
      --------------------------------------------------------------------------
      IF self.tg_layer_type <> 'POLYGON'
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topology layer is not polygon layer');
      
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Check that topo map object is ready
      --------------------------------------------------------------------------
      IF self.topo_map_mgr IS NULL
      OR self.topo_map_mgr.topo_map_name IS NULL
      OR self.topo_map_mgr.allow_updates <> 'TRUE'
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topo map not initialized');
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Check incoming srid 
      --------------------------------------------------------------------------
      IF p_input.SDO_SRID <> self.topology_srid
      THEN
         RAISE_APPLICATION_ERROR(-20001,'error input polygon does not match topo srid');
      
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 50
      -- Validate incoming polygon 
      --------------------------------------------------------------------------

      
      --------------------------------------------------------------------------
      -- Step 60
      -- Add polygon to topology
      --------------------------------------------------------------------------
      ary_number := MDSYS.SDO_TOPO_MAP.ADD_POLYGON_GEOMETRY(
          topology => NULL
         ,polygon  => p_input
      );
      
      --------------------------------------------------------------------------
      -- Step 70
      -- Just update the topo map 
      --------------------------------------------------------------------------
      MDSYS.SDO_TOPO_MAP.UPDATE_TOPO_MAP();
      
      --------------------------------------------------------------------------
      -- Step 80
      -- Convert face ids to topo objects
      --------------------------------------------------------------------------
      ary_topo_obj := MDSYS.SDO_TOPO_OBJECT_ARRAY();
      ary_topo_obj.EXTEND(ary_number.COUNT);
      FOR i IN 1 .. ary_number.COUNT
      LOOP
         ary_topo_obj(i) := MDSYS.SDO_TOPO_OBJECT(
             topo_id   => ary_number(i)
            ,topo_type => 3
         );
      
      END LOOP;
      
      --------------------------------------------------------------------------
      -- Step 90
      -- Build the output
      --------------------------------------------------------------------------
      obj_output := MDSYS.SDO_TOPO_GEOMETRY(
          topology    => self.topology_name
         ,tg_type     => 3
         ,tg_layer_id => self.tg_layer_id
         ,topo_ids    => ary_topo_obj
         
      );
      
      --------------------------------------------------------------------------
      -- Step 100
      -- Return what we got
      --------------------------------------------------------------------------  
      RETURN obj_output;
      
   END add_polygon_to_layer; 
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER PROCEDURE commit_topo_map(
       p_validate        IN  VARCHAR2 DEFAULT 'TRUE'
      ,p_validate_level  IN  NUMBER   DEFAULT 1
   )
   AS
   BEGIN
   
      IF self.topo_map_mgr IS NULL
      OR self.topo_map_mgr.allow_updates <> 'TRUE'
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topo map mgr not active');
         
      END IF;
      
      self.topo_map_mgr.commit_topo_map(
          p_validate        => p_validate
         ,p_validate_level  => p_validate_level
      );
   
   END commit_topo_map;

END;
/


--*************************--
PROMPT DZ_TOPO_LAYER_LIST.tps;

CREATE OR REPLACE TYPE dz_topo_layer_list FORCE                                      
AS 
TABLE OF dz_topo_layer;
/

GRANT EXECUTE ON dz_topo_layer_list TO PUBLIC;


--*************************--
PROMPT DZ_TOPOLOGY.tps;

CREATE OR REPLACE TYPE dz_topology FORCE
AUTHID CURRENT_USER
AS OBJECT (
    topology_owner     VARCHAR2(30 Char)
   ,topology_name      VARCHAR2(20 Char)
   ,topology_id        NUMBER
   ,topology_srid      NUMBER
   ,tolerance          NUMBER
   ,layer_count        NUMBER
   ,active_layer_index NUMBER
   ,topo_layers        dz_topo_layer_list
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,CONSTRUCTOR FUNCTION dz_topology
    RETURN SELF AS RESULT
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,CONSTRUCTOR FUNCTION dz_topology(
        p_topology_owner  IN  VARCHAR2
       ,p_topology_name   IN  VARCHAR2
    ) RETURN SELF AS RESULT
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,CONSTRUCTOR FUNCTION dz_topology(
        p_topology_name   IN  VARCHAR2
    ) RETURN SELF AS RESULT
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,CONSTRUCTOR FUNCTION dz_topology(
        p_topology_name   IN  VARCHAR2
       ,p_active_table    IN  VARCHAR2
       ,p_active_column   IN  VARCHAR2
    ) RETURN SELF AS RESULT
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,CONSTRUCTOR FUNCTION dz_topology(
        p_topology_name   IN  VARCHAR2
       ,p_active_tg_id    IN  NUMBER
    ) RETURN SELF AS RESULT
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,CONSTRUCTOR FUNCTION dz_topology(
        p_topo_geom     IN  MDSYS.SDO_TOPO_GEOMETRY
    ) RETURN SELF AS RESULT
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER PROCEDURE harvest_topo_metadata
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER PROCEDURE set_active_topo_layer(
      p_tg_layer_id  IN  NUMBER
    )
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION valid
    RETURN VARCHAR2
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION topo_layer(
      p_tg_layer_id  IN  NUMBER DEFAULT NULL
    ) RETURN dz_topo_layer
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION get_topo_map_mgr
    RETURN dz_topo_map_mgr
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER PROCEDURE set_topo_map_mgr(
      p_topo_map_mgr IN  dz_topo_map_mgr
    )
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER PROCEDURE java_memory(
        p_bytes     IN  NUMBER DEFAULT NULL
       ,p_gigs      IN  NUMBER DEFAULT NULL
    )
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION node_table
    RETURN VARCHAR2
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION edge_table
    RETURN VARCHAR2
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION edge_table_name
    RETURN VARCHAR2
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION face_table
    RETURN VARCHAR2
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION relation_table
    RETURN VARCHAR2
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION exp_table
    RETURN VARCHAR2
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION active_topo_table
    RETURN VARCHAR2
    
    -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION active_topo_column
    RETURN VARCHAR2
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION create_feature(
        self           IN OUT dz_topology
       ,p_input        IN     MDSYS.SDO_GEOMETRY
    ) RETURN MDSYS.SDO_TOPO_GEOMETRY
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER FUNCTION add_polygon_to_layer(
        self           IN OUT dz_topology
       ,p_input        IN     MDSYS.SDO_GEOMETRY
    ) RETURN MDSYS.SDO_TOPO_GEOMETRY
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   ,MEMBER PROCEDURE commit_topo_map(
        p_validate        IN  VARCHAR2 DEFAULT 'TRUE'
       ,p_validate_level  IN  NUMBER   DEFAULT 1
    )
   
);
/

GRANT EXECUTE ON dz_topology TO PUBLIC;


--*************************--
PROMPT DZ_TOPOLOGY.tpb;

CREATE OR REPLACE TYPE BODY dz_topology
AS

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   CONSTRUCTOR FUNCTION dz_topology
   RETURN SELF AS RESULT
   AS
   BEGIN
      RETURN;
   
   END dz_topology;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   CONSTRUCTOR FUNCTION dz_topology(
       p_topology_owner IN  VARCHAR2
      ,p_topology_name  IN  VARCHAR2
   ) RETURN SELF AS RESULT
   AS
   BEGIN
      
      self.topology_owner := p_topology_owner;
      self.topology_name  := p_topology_name;
      
      self.harvest_topo_metadata();
      
      RETURN;
      
   END dz_topology;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   CONSTRUCTOR FUNCTION dz_topology(
       p_topology_name IN  VARCHAR2
   ) RETURN SELF AS RESULT
   AS
   BEGIN
   
      self.topology_name  := p_topology_name;
      
      self.harvest_topo_metadata();
      
      RETURN;
      
   END dz_topology;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   CONSTRUCTOR FUNCTION dz_topology(
       p_topo_geom     IN  MDSYS.SDO_TOPO_GEOMETRY
   ) RETURN SELF AS RESULT
   AS
   BEGIN
      self.topology_id := p_topo_geom.topology_id;
      
      self.harvest_topo_metadata();
      
      self.set_active_topo_layer(p_topo_geom.tg_id);
      
      RETURN;
   
   END dz_topology;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   CONSTRUCTOR FUNCTION dz_topology(
       p_topology_name   IN  VARCHAR2
      ,p_active_table    IN  VARCHAR2
      ,p_active_column   IN  VARCHAR2
   ) RETURN SELF AS RESULT
   AS
      num_tg_id NUMBER;
      
   BEGIN
   
      self.topology_name  := p_topology_name;
      
      self.harvest_topo_metadata();
      
      FOR i IN 1 .. self.topo_layers.COUNT
      LOOP
         IF  self.topo_layers(i).table_name = p_active_table
         AND self.topo_layers(i).column_name = p_active_column
         THEN
            num_tg_id := i;
         
         END IF;
      
      END LOOP;
      
      IF num_tg_id IS NULL
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topo layer not found');
      
      END IF;
      
      self.set_active_topo_layer(num_tg_id);
      
      RETURN;
      
   END dz_topology;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   CONSTRUCTOR FUNCTION dz_topology(
       p_topology_name   IN  VARCHAR2
      ,p_active_tg_id    IN  NUMBER
   ) RETURN SELF AS RESULT
   AS
   BEGIN
   
      self.topology_name  := p_topology_name;
      
      self.harvest_topo_metadata();
      
      self.set_active_topo_layer(p_active_tg_id);
      
      RETURN;
   
   END dz_topology;
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER PROCEDURE harvest_topo_metadata
   AS
      str_sql    VARCHAR2(4000 Char);
      
   BEGIN
   
      IF self.topology_name IS NOT NULL
      THEN
         IF self.topology_owner IS NULL
         THEN
            self.topology_owner := USER;
      
         END IF;
      
         str_sql := 'SELECT ' 
                 || dz_topo_util.c_schema || '.dz_topo_layer( '
                 || '    p_topology_owner    => a.owner '
                 || '   ,p_topology_name     => a.topology '
                 || '   ,p_topology_id       => a.topology_id '
                 || '   ,p_topology_srid     => a.srid '
                 || '   ,p_tolerance         => a.tolerance '
                 || '   ,p_table_owner       => a.table_owner '
                 || '   ,p_table_name        => a.table_name '
                 || '   ,p_column_name       => a.column_name '
                 || '   ,p_tg_layer_id       => a.tg_layer_id '
                 || '   ,p_tg_layer_type     => a.tg_layer_type '
                 || '   ,p_tg_layer_level    => a.tg_layer_level '
                 || ') '
                 || 'FROM '
                 || 'all_sdo_topo_metadata a '
                 || 'WHERE '
                 || '    a.owner = :p01 '
                 || 'AND a.topology  = :p02 ';
         
         EXECUTE IMMEDIATE str_sql
         BULK COLLECT INTO self.topo_layers
         USING self.topology_owner,self.topology_name;
         
      ELSIF self.topology_id IS NOT NULL
      THEN
         str_sql := 'SELECT ' 
                 || dz_topo_util.c_schema || '.dz_topo_layer( '
                 || '    p_topology_owner    => a.owner '
                 || '   ,p_topology_name     => a.topology '
                 || '   ,p_topology_id       => a.topology_id '
                 || '   ,p_topology_srid     => a.srid '
                 || '   ,p_tolerance         => a.tolerance '
                 || '   ,p_table_owner       => a.table_owner '
                 || '   ,p_table_name        => a.table_name '
                 || '   ,p_column_name       => a.column_name '
                 || '   ,p_tg_layer_id       => a.tg_layer_id '
                 || '   ,p_tg_layer_type     => a.tg_layer_type '
                 || '   ,p_tg_layer_level    => a.tg_layer_level '
                 || ') '
                 || 'FROM '
                 || 'all_sdo_topo_metadata a '
                 || 'WHERE '
                 || 'a.topology_id = :p01 ';
                 
         EXECUTE IMMEDIATE str_sql
         BULK COLLECT INTO self.topo_layers
         USING self.topology_id;
      
      END IF;
      
      IF self.topo_layers IS NOT NULL
      AND self.topo_layers.COUNT > 0
      THEN
         self.topology_owner    := self.topo_layers(1).topology_owner;
         self.topology_name     := self.topo_layers(1).topology_name;
         self.topology_id       := self.topo_layers(1).topology_id;
         self.topology_srid     := self.topo_layers(1).topology_srid;
         self.tolerance         := self.topo_layers(1).tolerance;
         self.layer_count       := self.topo_layers.COUNT;
      
      ELSE
         self.topo_layers := NULL;
         
      END IF;
   
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         NULL;
         
      WHEN OTHERS
      THEN
         RAISE;
      
   END harvest_topo_metadata;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER PROCEDURE set_active_topo_layer(
     p_tg_layer_id  IN  NUMBER
   )
   AS
   BEGIN
   
      IF self.topo_layers IS NULL
      OR self.topo_layers.COUNT = 0
      THEN
         RETURN;
      END IF;
      
      FOR i IN 1 .. self.topo_layers.COUNT
      LOOP
         IF self.topo_layers(i).tg_layer_id = p_tg_layer_id
         THEN
            self.active_layer_index := i;
         
         END IF;
      
      END LOOP;
   
   END set_active_topo_layer;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION valid
   RETURN VARCHAR2
   AS
   BEGIN
      IF self.topology_name IS NOT NULL
      AND self.topology_id IS NOT NULL
      THEN
         RETURN 'TRUE';
      ELSE
         RETURN 'FALSE';
      END IF;   
   
   END valid;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION topo_layer(
      p_tg_layer_id  IN  NUMBER DEFAULT NULL
   ) RETURN dz_topo_layer
   AS
      num_layer_index  NUMBER;
      
   BEGIN
   
      IF  self.topo_layers IS NULL
      AND self.topo_layers.COUNT = 0
      THEN
         RETURN NULL;
      
      END IF;
      
      IF  p_tg_layer_id IS NULL
      AND self.active_layer_index IS NULL
      THEN
         RAISE_APPLICATION_ERROR(-20001,'no active layer set');
      
      END IF;
      
      IF p_tg_layer_id IS NULL
      THEN
         num_layer_index := self.active_layer_index;
      
      ELSE
         FOR i IN 1 .. self.topo_layers.COUNT
         LOOP
            IF self.topo_layers(i).tg_layer_id = p_tg_layer_id
            THEN
               num_layer_index := i;
            
            END IF;
         
         END LOOP;
         
         IF  num_layer_index IS NULL
         THEN
            RAISE_APPLICATION_ERROR(-20001,'tg layer id not found');
         
         END IF;
      
      END IF;
      
      RETURN self.topo_layers(num_layer_index);
         
   END topo_layer;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION get_topo_map_mgr
   RETURN dz_topo_map_mgr
   AS
   BEGIN
   
      IF self.topo_layer() IS NOT NULL
      THEN
         RETURN self.topo_layer().get_topo_map_mgr();
         
      ELSE
         RAISE_APPLICATION_ERROR(-20001,'topo layer not active');
         
      END IF;
   
   END get_topo_map_mgr;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER PROCEDURE set_topo_map_mgr(
     p_topo_map_mgr IN  dz_topo_map_mgr
   )
   AS
   BEGIN
   
      IF self.topo_layer() IS NOT NULL
      THEN
         self.topo_layers(self.active_layer_index).set_topo_map_mgr(p_topo_map_mgr);
         
      ELSE
         RAISE_APPLICATION_ERROR(-20001,'topo layer not active');
         
      END IF;
   
   END set_topo_map_mgr;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER PROCEDURE java_memory(
       p_bytes     IN  NUMBER DEFAULT NULL
      ,p_gigs      IN  NUMBER DEFAULT NULL
   )
   AS
      num_bytes NUMBER := p_bytes;
      
   BEGIN
   
      IF num_bytes IS NULL
      THEN
         num_bytes := p_gigs * 1073741824;
      END IF;   
   
      MDSYS.SDO_TOPO_MAP.SET_MAX_MEMORY_SIZE(num_bytes);
   
   END java_memory;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION node_table
   RETURN VARCHAR2
   AS
   BEGIN
      RETURN self.topology_owner || '.'
      || self.topology_name
      || '_NODE$';
      
   END node_table;
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION edge_table
   RETURN VARCHAR2
   AS
   BEGIN
      RETURN self.topology_owner || '.'
      || self.topology_name
      || '_EDGE$';
      
   END edge_table;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION edge_table_name
   RETURN VARCHAR2
   AS
   BEGIN
      RETURN self.topology_name
      || '_EDGE$';
      
   END edge_table_name;
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION face_table
   RETURN VARCHAR2
   AS
   BEGIN
      RETURN self.topology_owner || '.'
      || self.topology_name
      || '_FACE$';
      
   END face_table;
    
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION relation_table
   RETURN VARCHAR2
   AS
   BEGIN
      RETURN self.topology_owner || '.'
      || self.topology_name
      || '_RELATION$';
      
   END relation_table;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION exp_table
   RETURN VARCHAR2
   AS
   BEGIN
      RETURN self.topology_owner || '.'
      || self.topology_name
      || '_EXP$';
      
   END exp_table;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION active_topo_table
   RETURN VARCHAR2
   AS
   
   BEGIN
   
      IF self.active_layer_index IS NULL
      OR self.topo_layers IS NULL
      OR self.topo_layers.COUNT = 0
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topo layer not ready');
      
      END IF;
      
      RETURN self.topo_layers(self.active_layer_index).table_name;
   
   END active_topo_table;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION active_topo_column
   RETURN VARCHAR2
   AS
   
   BEGIN
   
      IF self.active_layer_index IS NULL
      OR self.topo_layers IS NULL
      OR self.topo_layers.COUNT = 0
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topo layer not ready');
      
      END IF;
      
      RETURN self.topo_layers(self.active_layer_index).column_name;
   
   END active_topo_column;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION create_feature(
       self           IN OUT dz_topology
      ,p_input        IN     MDSYS.SDO_GEOMETRY
   ) RETURN MDSYS.SDO_TOPO_GEOMETRY
   AS
   BEGIN
   
      IF self.active_layer_index IS NULL
      OR self.topo_layers IS NULL
      OR self.topo_layers.COUNT = 0
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topo layer not ready');
      
      END IF;
      
      RETURN self.topo_layers(
         self.active_layer_index
      ).create_feature(
         p_input => p_input
      );
   
   END create_feature;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER FUNCTION add_polygon_to_layer(
       self           IN OUT dz_topology
      ,p_input        IN     MDSYS.SDO_GEOMETRY
   ) RETURN MDSYS.SDO_TOPO_GEOMETRY
   AS
   BEGIN
   
      IF self.active_layer_index IS NULL
      OR self.topo_layers IS NULL
      OR self.topo_layers.COUNT = 0
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topo layer not ready');
      
      END IF;
      
      RETURN self.topo_layers(self.active_layer_index).add_polygon_to_layer(
         p_input => p_input
      );
   
   END add_polygon_to_layer;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   MEMBER PROCEDURE commit_topo_map(
       p_validate        IN  VARCHAR2 DEFAULT 'TRUE'
      ,p_validate_level  IN  NUMBER   DEFAULT 1
   )
   AS
   BEGIN
   
      IF self.topo_layer() IS NULL
      OR self.topo_layers(self.active_layer_index).topo_map_mgr IS NULL
      OR self.topo_layers(self.active_layer_index).topo_map_mgr.allow_updates <> 'TRUE'
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topo map mgr invalid');
         
      END IF;
      
      self.topo_layers(self.active_layer_index).topo_map_mgr.commit_topo_map(
          p_validate        => p_validate
         ,p_validate_level  => p_validate_level
      );
   
   END commit_topo_map;
   
END;
/


--*************************--
PROMPT DZ_TOPO_MAIN.pks;

CREATE OR REPLACE PACKAGE dz_topo_main
AUTHID CURRENT_USER
AS
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   /*
   header: DZ_TOPO
     
   - Build ID: 5
   - TFS Change Set: 8290
   
   Utilities and helper objects for the manipulation and maintenance of Oracle 
   Spatial topologies.
   
   In order to avoid passing about all the gritty details of a topology
   workflow, the dz_topology object encapsulates the detail of the topology
   and topo map session for easy reuse by topology operators.
   
   Example:
   (start code)
   obj_topo := dz_topology(
       p_topology_name    => 'CATROUGH_NP21'
      ,p_active_table     => 'CATROUGH_NP21_TOPO'
      ,p_active_column    => 'TOPO_GEOM'
   );
         
   obj_topo.set_topo_map_mgr(
      dz_topo_map_mgr(
          p_window_sdo      => ary_shape
         ,p_window_padding  => 0.000001
         ,p_topology_name   => 'CATROUGH_NP21'
      )
   );
      
   obj_topo.java_memory(p_gigs => 13);
      
   FOR i IN 1 .. ary_featureids.COUNT
   LOOP
      topo_shape := obj_topo.create_feature(
         ary_shape(i)
      );
            
      INSERT INTO catrough_np21_topo(
          featureid
         ,topo_geom
      ) VALUES (
          ary_featureids(i)
         ,topo_shape
      );
            
   END LOOP;
      
   obj_topo.commit_topo_map();
   COMMIT;
   (end)
   */
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   
   TYPE topo_edge_hash IS TABLE OF dz_topo_edge
   INDEX BY PLS_INTEGER;

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION sdo_list_count(
      p_input          IN  MDSYS.SDO_LIST_TYPE
   ) RETURN NUMBER;

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   /*
   Function: dz_topo_main.aggregate_topology

   Function which takes an arbitrary number of topo geometries and returns an
   aggregated polygon representing the external boundary of all faces comprising
   the topo geometries. 

   Parameters:

      p_topology_owner - optional owner of the topology
      p_topology_name - optional name of the topology
      p_topology_obj - optional object dz_topology
      p_input_topo - object dz_topo_vry, a varray of topo geometries
      p_remove_holes - optional TRUE/FALSE flag to remove resulting interior rings
      
   Returns:

      MDSYS.SDO_GEOMETRY spatial type
      
   Notes:
   
   - Dz_topo functions and procedures either utilize an existing dz_topology 
     object or require the name and owner of the topology to internally initialize
     one.  Thus you either must provide name and owner or an initialized 
     dz_topology object to the function.  The latter format is most useful when
     calling several topology procedures in sequence.
   
   - By design this function does not invoke the .get_geometry() subroutines of
     Oracle Spatial topologies or utilize any vector spatial aggregation 
     techniques.  Rather the resulting polygon is built directly from chained 
     together topology edges.  The value of this approach is that it
     is not necessary to precompute relationships among existing topo geometries
     to obtain aggregated results.  For example with a topology of watershed
     polygons, provide a drainage area based upon initial location and
     arbitrary upstream distance.  Its unwieldy to impossible to prebuild topology 
     layers that could represent every possibility of polygon combinations.  This
     function provides the ability to aggregate together an arbitrary input of 
     topo geometries.

   */
   FUNCTION aggregate_topology(
       p_topology_owner IN  VARCHAR2 DEFAULT NULL
      ,p_topology_name  IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
      ,p_input_topo     IN  dz_topo_vry
      ,p_remove_holes   IN  VARCHAR2 DEFAULT 'FALSE'
   ) RETURN MDSYS.SDO_GEOMETRY;

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION topo2faces(
       p_topology_owner IN  VARCHAR2 DEFAULT NULL
      ,p_topology_name  IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
      ,p_input_topo     IN  dz_topo_vry
   ) RETURN MDSYS.SDO_NUMBER_ARRAY;

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION faces2exterior_edges(
       p_topology_owner IN  VARCHAR2 DEFAULT NULL
      ,p_topology_name  IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
      ,p_input_faces    IN  MDSYS.SDO_NUMBER_ARRAY
   ) RETURN dz_topo_edge_list;

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION edges2rings(
      p_input_edges     IN  dz_topo_edge_list
   ) RETURN dz_topo_ring_list;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION edges2strings(
      p_input_edges   IN  dz_topo_edge_list
   ) RETURN dz_topo_ring_list;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION strings2rings(
      p_input_strings   IN  dz_topo_ring_list
   ) RETURN dz_topo_ring_list;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION finalize_face_rings(
       p_input_rings      IN  dz_topo_ring_list
      ,p_island_edge_list IN  MDSYS.SDO_LIST_TYPE DEFAULT NULL
      ,p_tolerance        IN  NUMBER DEFAULT 0.05
   ) RETURN dz_topo_ring_list;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION finalize_generic_rings(
       p_input_rings    IN  dz_topo_ring_list
      ,p_tolerance      IN  NUMBER DEFAULT 0.05
   ) RETURN dz_topo_ring_list;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION rings2sdo(
       p_input_rings    IN  dz_topo_ring_list
      ,p_remove_holes   IN  VARCHAR2 DEFAULT 'FALSE'
   ) RETURN MDSYS.SDO_GEOMETRY;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION get_faces(
       p_input          IN  MDSYS.SDO_TOPO_GEOMETRY
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
   ) RETURN MDSYS.SDO_NUMBER_ARRAY;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   /*
   Function: dz_topo_main.face2sdo

   Utility function which provides the sdo geometry polygon representing a
   single face of a topology. 

   Parameters:

      p_topology_owner - optional owner of the topology
      p_topology_name - optional name of the topology
      p_topology_obj - optional object dz_topology
      p_face_id - single numeric face id
      p_remove_holes - optional TRUE/FALSE flag to remove resulting interior rings
      
   Returns:

      MDSYS.SDO_GEOMETRY spatial type
      
   Notes:
   
   - Dz_topo functions and procedures either utilize an existing dz_topology 
     object or require the name and owner of the topology to internally initialize
     one.  Thus you either must provide name and owner or an initialized 
     dz_topology object to the function.  The latter format is most useful when
     calling several topology procedures in sequence.
   
   */
   FUNCTION face2sdo(
       p_topology_owner IN  VARCHAR2 DEFAULT NULL
      ,p_topology_name  IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
      ,p_face_id        IN  NUMBER
      ,p_remove_holes   IN  VARCHAR2 DEFAULT 'FALSE'
   ) RETURN MDSYS.SDO_GEOMETRY;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION get_faces_as_sdo(
       p_input          IN  MDSYS.SDO_TOPO_GEOMETRY
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
   ) RETURN MDSYS.SDO_GEOMETRY;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE remove_faces_from_topo_raw(
       p_input          IN  MDSYS.SDO_TOPO_GEOMETRY
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
      ,p_face_ids       IN  MDSYS.SDO_NUMBER_ARRAY
      ,p_returning      OUT MDSYS.SDO_NUMBER_ARRAY
   );
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE add_faces_to_topo_raw(
       p_input          IN  MDSYS.SDO_TOPO_GEOMETRY
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
      ,p_face_ids       IN  MDSYS.SDO_NUMBER_ARRAY
      ,p_returning      OUT MDSYS.SDO_NUMBER_ARRAY
   );
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION get_interior_edges(
       p_input          IN  MDSYS.SDO_TOPO_GEOMETRY
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
   ) RETURN MDSYS.SDO_NUMBER_ARRAY;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION get_interior_edges_as_sdo(
       p_input          IN  MDSYS.SDO_TOPO_GEOMETRY
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
   ) RETURN MDSYS.SDO_GEOMETRY;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION get_shared_faces(
       p_input          IN  MDSYS.SDO_TOPO_GEOMETRY
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
   ) RETURN MDSYS.SDO_NUMBER_ARRAY;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION get_face_topo_neighbors(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL  
      ,p_topology_name   IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj    IN  dz_topology DEFAULT NULL
      ,p_tg_layer_id     IN  NUMBER 
      ,p_face_id         IN  NUMBER
   ) RETURN dz_topo_vry;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION face_at_point(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL  
      ,p_topology_name   IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj    IN  dz_topology DEFAULT NULL
      ,p_input           IN  MDSYS.SDO_GEOMETRY
   ) RETURN NUMBER;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   /*
   Function: dz_topo_main.face_at_point_sdo

   Utility function which provides the sdo geometry polygon representing a
   single face of a topology located using an input point geometry. 

   Parameters:

      p_topology_owner - optional owner of the topology
      p_topology_name - optional name of the topology
      p_topology_obj - optional object dz_topology
      p_input - sdo geometry input point
      
   Returns:

      MDSYS.SDO_GEOMETRY spatial type
      
   Notes:
   
   - Dz_topo functions and procedures either utilize an existing dz_topology 
     object or require the name and owner of the topology to internally initialize
     one.  Thus you either must provide name and owner or an initialized 
     dz_topology object to the function.  The latter format is most useful when
     calling several topology procedures in sequence.
   
   */
   FUNCTION face_at_point_sdo(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL  
      ,p_topology_name   IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj    IN  dz_topology DEFAULT NULL
      ,p_input           IN  MDSYS.SDO_GEOMETRY
   ) RETURN MDSYS.SDO_GEOMETRY;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE drop_face_clean(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL  
      ,p_topology_name   IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj    IN  dz_topology DEFAULT NULL
      ,p_face_id         IN  NUMBER
   );
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   /*
   Procedure: dz_topo_main.nuke_topology

   Fairly dangerous procedure to remove all traces of given topology including
   all topo tables registered to the topology. Do not use unless you are sure
   you do not need any of the data participating in your topology.

   Parameters:

      p_topology_owner - Optional owner of the topology to remove.
      p_topology_name - Name of the topology to utterly remove.

   Notes:
   
   - Please be careful with this.  I am not responsible for you destroying 
     your topology.

   */
   PROCEDURE nuke_topology(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL 
      ,p_topology_name   IN  VARCHAR2
   );
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE remove_face_island_edge_id(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL  
      ,p_topology_name   IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj    IN  dz_topology DEFAULT NULL
      ,p_face_id         IN  NUMBER
      ,p_edge_id         IN  NUMBER
   );
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE remove_face_island_edge_id(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL  
      ,p_topology_name   IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj    IN  dz_topology DEFAULT NULL
      ,p_face_id         IN  NUMBER
      ,p_edge_id         IN  MDSYS.SDO_NUMBER_ARRAY
   );
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE append_face_island_edge_id(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL  
      ,p_topology_name   IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj    IN  dz_topology DEFAULT NULL
      ,p_face_id         IN  NUMBER
      ,p_edge_id         IN  NUMBER
   );
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE remove_face_island_node_id(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL  
      ,p_topology_name   IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj    IN  dz_topology DEFAULT NULL
      ,p_face_id         IN  NUMBER
      ,p_node_id         IN  NUMBER
   );
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE append_face_island_node_id(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL  
      ,p_topology_name   IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj    IN  dz_topology DEFAULT NULL
      ,p_face_id         IN  NUMBER
      ,p_node_id         IN  NUMBER
   );
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   /*
   Procedure: dz_topo_main.unravel_face

   Utility procedure intended to gather information for the possible manual 
   removal of a topology face by identifying and first removing it's surrounding 
   referenced components.  This is usually done in cases of corruption in the 
   topology.  So upon providing a tg layer and face id, the procedure returns 
   the list of tg ids, faces, edges and nodes that must be removed in order to 
   delete the input face.  A sdo window is also provided for use in 
   constraining a topo map to the work area in question.

   Parameters:

      p_topology_owner - Optional owner of the topology
      p_topology_name - Optional name of the topology
      p_topology_obj - Optional object dz_topology
      p_tg_layer_id - the tg_layer to execute the unravel against
      p_face_id - the face to execute the unravel against
      p_tg_ids - output tg ids 
      p_face_ids - output face ids
      p_edge_ids - output edge ids
      p_node_ids - output node ids
      p_window_sdo - output geometry window surrounding the action
   
   Notes:
   
   - Note this procedure only returns information about a topology and does 
     not make any changes to the input topology.
   
   - Taking the results of this procedures and manually deleting resources
     in your topology is highly dangerous and should not be done unless all 
     other options have been explored first.
   
   - On the other hand this procedure could also function as the core of a 
     nice little topology viewer showing you how your face in question ties
     together with other components.  

   */
   PROCEDURE unravel_face(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL  
      ,p_topology_name   IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj    IN  dz_topology DEFAULT NULL
      ,p_tg_layer_id     IN  NUMBER DEFAULT 1
      ,p_face_id         IN  NUMBER
      ,p_tg_ids          OUT MDSYS.SDO_NUMBER_ARRAY
      ,p_face_ids        OUT MDSYS.SDO_NUMBER_ARRAY
      ,p_edge_ids        OUT MDSYS.SDO_NUMBER_ARRAY
      ,p_node_ids        OUT MDSYS.SDO_NUMBER_ARRAY
      ,p_window_sdo      OUT MDSYS.SDO_GEOMETRY
   );
   
END dz_topo_main;
/

GRANT EXECUTE ON dz_topo_main TO public;


--*************************--
PROMPT DZ_TOPO_MAIN.pkb;

CREATE OR REPLACE PACKAGE BODY dz_topo_main
AS

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION sdo_list_count(
      p_input      IN  MDSYS.SDO_LIST_TYPE
   ) RETURN NUMBER
   AS
   BEGIN
      RETURN p_input.COUNT;
      
   END sdo_list_count;

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION aggregate_topology(
       p_topology_owner IN  VARCHAR2 DEFAULT NULL
      ,p_topology_name  IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
      ,p_input_topo     IN  dz_topo_vry
      ,p_remove_holes   IN  VARCHAR2 DEFAULT 'FALSE'
   ) RETURN MDSYS.SDO_GEOMETRY
   AS
      obj_topology     dz_topology := p_topology_obj;
      ary_faces        MDSYS.SDO_NUMBER_ARRAY;
      ary_edges        dz_topo_edge_list;
      ary_rings        dz_topo_ring_list;
      sdo_output       MDSYS.SDO_GEOMETRY;
      str_remove_holes VARCHAR2(5 Char) := UPPER(p_remove_holes);

   BEGIN

      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topology_owner => p_topology_owner
            ,p_topology_name  => p_topology_name
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;

      IF str_remove_holes IS NULL
      THEN
         str_remove_holes := 'FALSE';
         
      ELSIF str_remove_holes NOT IN ('TRUE','FALSE')
      THEN
         RAISE_APPLICATION_ERROR(-20001,'boolean error');
         
      END IF;

      --------------------------------------------------------------------------
      -- Step 20
      -- Generate the array of faces
      --------------------------------------------------------------------------
      ary_faces := topo2faces(
          p_topology_obj => obj_topology
         ,p_input_topo   => p_input_topo
      );

      --------------------------------------------------------------------------
      -- Step 20
      -- Generate the array of edges
      --------------------------------------------------------------------------
      ary_edges := faces2exterior_edges(
          p_topology_obj => obj_topology
         ,p_input_faces  => ary_faces
      );

      --------------------------------------------------------------------------
      -- Step 30
      -- Generate the array of rings
      --------------------------------------------------------------------------
      ary_rings := edges2rings(ary_edges);
      
      ary_rings := finalize_generic_rings(ary_rings);

      --------------------------------------------------------------------------
      -- Step 40
      -- Generate the output sdo_geometry
      --------------------------------------------------------------------------
      sdo_output := rings2sdo(ary_rings,str_remove_holes);

      --------------------------------------------------------------------------
      -- Step 50
      -- Return the results
      --------------------------------------------------------------------------
      RETURN sdo_output;

   END aggregate_topology;

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION topo2faces(
       p_topology_owner IN  VARCHAR2 DEFAULT NULL
      ,p_topology_name  IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
      ,p_input_topo     IN  dz_topo_vry
   ) RETURN MDSYS.SDO_NUMBER_ARRAY
   AS
      obj_topology     dz_topology := p_topology_obj;
      str_sql          VARCHAR2(4000 Char);
      ary_output       MDSYS.SDO_NUMBER_ARRAY;

   BEGIN

      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topology_owner => p_topology_owner
            ,p_topology_name  => p_topology_name
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;

      --------------------------------------------------------------------------
      -- Step 20
      -- Fetch the faces from the RELATION$ table
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || 'DISTINCT '
              || 'topo_id '
              || 'FROM ('
              || '   SELECT '
              || '   a.topo_id '
              || '   FROM '
              || '   ' || obj_topology.relation_table() || ' a '
              || '   JOIN '
              || '   TABLE(:p01) b '
              || '   ON '
              || '   a.tg_layer_id = b.tg_layer_id AND '
              || '   a.tg_id = b.tg_id '
              || '   WHERE '
              || '   a.topo_type = 3 '
              || ') ';

      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO ary_output
      USING p_input_topo;

      --------------------------------------------------------------------------
      -- Step 30
      -- Return the array of unique faces
      --------------------------------------------------------------------------
      RETURN ary_output;

   END topo2faces;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE faces2edges(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL
      ,p_topology_name   IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj    IN  dz_topology DEFAULT NULL
      ,p_faces           IN  MDSYS.SDO_NUMBER_ARRAY
      ,p_subset          IN  VARCHAR2 DEFAULT NULL
      ,p_output          OUT dz_topo_edge_list
   )
   AS
      obj_topology     dz_topology := p_topology_obj;
      str_sql          VARCHAR2(4000 Char);
      str_subset       VARCHAR2(4000 Char) := UPPER(p_subset);
      
   BEGIN

      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_faces IS NULL
      OR p_faces.COUNT = 0
      THEN
         RETURN;
         
      END IF;
      
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topology_owner => p_topology_owner
            ,p_topology_name  => p_topology_name
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;

      --------------------------------------------------------------------------
      -- Step 20
      -- Check possible subset values
      --------------------------------------------------------------------------
      IF str_subset IS NULL
      THEN
         str_subset := 'ALL';
         
      ELSIF str_subset NOT IN ('ALL','INTERIOR','EXTERIOR')
      THEN
          RAISE_APPLICATION_ERROR(-20001,'subset values are ALL, INTERIOR or EXTERIOR');
          
      END IF;

      --------------------------------------------------------------------------
      -- Step 30
      -- Build SQL to fetch edges from topology
      --------------------------------------------------------------------------
      IF str_subset = 'INTERIOR'
      THEN
         str_sql := 'SELECT '
                 || dz_topo_util.c_schema() || '.dz_topo_edge( '
                 || '   a.edge_id, '
                 || '   ''L'', '
                 || '   a.start_node_id, '
                 || '   a.end_node_id, '
                 || '   a.geometry, '
                 || '   0 '
                 || ') '
                 || 'FROM '
                 || obj_topology.edge_table() || ' a '
                 || 'WHERE '
                 || 'a.left_face_id IN (SELECT * FROM TABLE(:p01)) AND '
                 || 'a.right_face_id IN (SELECT * FROM TABLE(:p02)) ';
         
         EXECUTE IMMEDIATE str_sql
         BULK COLLECT INTO p_output
         USING p_faces,p_faces;
         
      ELSIF str_subset = 'EXTERIOR'
      THEN
         str_sql := 'SELECT '
                 || dz_topo_util.c_schema() || '.dz_topo_edge( '
                 || '   a.edge_id, '
                 || '   ''L'', '
                 || '   a.start_node_id, '
                 || '   a.end_node_id, '
                 || '   a.geometry, '
                 || '   0 '
                 || ') '
                 || 'FROM '
                 || obj_topology.edge_table() || ' a '
                 || 'WHERE '
                 || 'a.left_face_id IN (SELECT * FROM TABLE(:p01)) AND '
                 || 'a.right_face_id NOT IN (SELECT * FROM TABLE(:p02)) '
                 || 'UNION ALL SELECT '
                 || dz_topo_util.c_schema() || '.dz_topo_edge( '
                 || '   a.edge_id, '
                 || '   ''R'', '
                 || '   a.start_node_id, '
                 || '   a.end_node_id, '
                 || '   a.geometry, '
                 || '   0 '
                 || ') '
                 || 'FROM '
                 || obj_topology.edge_table() || ' a '
                 || 'WHERE '
                 || 'a.left_face_id NOT IN (SELECT * FROM TABLE(:p01)) AND '
                 || 'a.right_face_id IN (SELECT * FROM TABLE(:p02)) '; 
              
         EXECUTE IMMEDIATE str_sql
         BULK COLLECT INTO p_output
         USING p_faces,p_faces,p_faces,p_faces;
         
      ELSIF str_subset = 'ALL'
      THEN
         str_sql := 'SELECT '
                 || dz_topo_util.c_schema() || '.dz_topo_edge( '
                 || '   a.edge_id, '
                 || '   ''L'', '
                 || '   a.start_node_id, '
                 || '   a.end_node_id, '
                 || '   a.geometry, '
                 || '   0 '
                 || ') '
                 || 'FROM '
                 || obj_topology.edge_table() || ' a '
                 || 'WHERE '
                 || 'a.left_face_id IN (SELECT * FROM TABLE(:p01)) AND '
                 || 'a.right_face_id NOT IN (SELECT * FROM TABLE(:p02)) '
                 || 'UNION ALL SELECT '
                 || dz_topo_util.c_schema() || '.dz_topo_edge( '
                 || '   a.edge_id, '
                 || '   ''R'', '
                 || '   a.start_node_id, '
                 || '   a.end_node_id, '
                 || '   a.geometry, '
                 || '   0 '
                 || ') '
                 || 'FROM '
                 || obj_topology.edge_table() || ' a '
                 || 'WHERE '
                 || 'a.left_face_id NOT IN (SELECT * FROM TABLE(:p01)) AND '
                 || 'a.right_face_id IN (SELECT * FROM TABLE(:p02)) '
                 || 'UNION ALL SELECT '
                 || dz_topo_util.c_schema() || '.dz_topo_edge( '
                 || '   a.edge_id, '
                 || '   ''L'', '
                 || '   a.start_node_id, '
                 || '   a.end_node_id, '
                 || '   a.geometry, '
                 || '   0 '
                 || ') '
                 || 'FROM '
                 || obj_topology.edge_table() || ' a '
                 || 'WHERE '
                 || 'a.left_face_id IN (SELECT * FROM TABLE(:p01)) AND '
                 || 'a.right_face_id IN (SELECT * FROM TABLE(:p02)) ';
              
         EXECUTE IMMEDIATE str_sql
         BULK COLLECT INTO p_output
         USING p_faces,p_faces,p_faces,p_faces,p_faces,p_faces;
         
      END IF;
   
   END faces2edges;

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION faces2exterior_edges(
       p_topology_owner IN  VARCHAR2 DEFAULT NULL
      ,p_topology_name  IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
      ,p_input_faces    IN  MDSYS.SDO_NUMBER_ARRAY
   ) RETURN dz_topo_edge_list
   AS
      obj_topology     dz_topology := p_topology_obj;
      ary_output       dz_topo_edge_list;

   BEGIN

      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_input_faces IS NULL
      OR p_input_faces.COUNT = 0
      THEN
         RETURN NULL;
         
      END IF;

      --------------------------------------------------------------------------
      -- Step 20
      -- Determine topology location
      --------------------------------------------------------------------------
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topology_owner => p_topology_owner
            ,p_topology_name  => p_topology_name
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;

      --------------------------------------------------------------------------
      -- Step 30
      -- Fetch edges having our faces on the LEFT side
      --------------------------------------------------------------------------
      faces2edges(
          p_topology_obj    => obj_topology
         ,p_faces           => p_input_faces
         ,p_subset          => 'EXTERIOR'
         ,p_output          => ary_output
      );

      --------------------------------------------------------------------------
      -- Step 50
      -- Return the results
      --------------------------------------------------------------------------
      RETURN ary_output;

   END faces2exterior_edges;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION faces2interior_edges(
       p_topology_owner IN  VARCHAR2 DEFAULT NULL
      ,p_topology_name  IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
      ,p_input_faces    IN  MDSYS.SDO_NUMBER_ARRAY
   ) RETURN dz_topo_edge_list
   AS
      obj_topology     dz_topology := p_topology_obj;
      ary_output       dz_topo_edge_list;

   BEGIN

      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_input_faces IS NULL
      OR p_input_faces.COUNT = 0
      THEN
         RETURN NULL;
         
      END IF;

      --------------------------------------------------------------------------
      -- Step 20
      -- Determine topology location
      --------------------------------------------------------------------------
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topology_owner => p_topology_owner
            ,p_topology_name  => p_topology_name
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;

      --------------------------------------------------------------------------
      -- Step 30
      -- Fetch edges having our faces on the LEFT side
      --------------------------------------------------------------------------
      faces2edges(
          p_topology_obj    => obj_topology
         ,p_faces           => p_input_faces
         ,p_subset          => 'INTERIOR'
         ,p_output          => ary_output
      );

      --------------------------------------------------------------------------
      -- Step 50
      -- Return the results
      --------------------------------------------------------------------------
      RETURN ary_output;

   END faces2interior_edges;

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION edges2rings(
      p_input_edges     IN  dz_topo_edge_list
   ) RETURN dz_topo_ring_list
   AS
      ary_output       dz_topo_ring_list;
      hash_edges       topo_edge_hash;
      obj_current_edge dz_topo_edge;
      int_ring_index   PLS_INTEGER;
      int_deadman      PLS_INTEGER;
      int_hash_index   PLS_INTEGER;
      ary_candidates   dz_topo_edge_list;
      int_candidates   PLS_INTEGER;
      boo_check        BOOLEAN := TRUE;

   BEGIN
      
      --------------------------------------------------------------------------
      -- Step 10
      -- Initialize ring creation
      --------------------------------------------------------------------------
      int_ring_index := 1;
      ary_output := dz_topo_ring_list();
      
      --------------------------------------------------------------------------
      -- Step 10
      -- Convert the incoming object array into a hash with edge id as key
      -- Store a second copy for retrieving edges when we unravel a ring
      --------------------------------------------------------------------------
      FOR i IN 1 .. p_input_edges.COUNT
      LOOP
         -- Pull out any single loops
         IF p_input_edges(i).start_node_id = p_input_edges(i).end_node_id
         THEN
            ary_output.EXTEND();
            ary_output(int_ring_index) := dz_topo_ring(
               p_input_edges(i),
               int_ring_index
            );
            
            IF ary_output(int_ring_index).shape.get_gtype() = 2
            THEN
               ary_output(int_ring_index).shape := MDSYS.SDO_GEOMETRY(
                   TO_NUMBER(ary_output(int_ring_index).shape.get_dims() || '003')
                  ,ary_output(int_ring_index).shape.SDO_SRID
                  ,NULL
                  ,MDSYS.SDO_ELEM_INFO_ARRAY(1,1003,1)
                  ,ary_output(int_ring_index).shape.SDO_ORDINATES
               );
               
            END IF;
            
            int_ring_index := int_ring_index + 1;
            
         ELSE
            hash_edges(p_input_edges(i).edge_id) := p_input_edges(i);
            
         END IF;
         
      END LOOP;
    
      --------------------------------------------------------------------------
      -- Step 30
      -- Exit if hash is empty of edges
      --------------------------------------------------------------------------
      int_deadman := 0;
      <<new_ring>>
      WHILE boo_check
      LOOP
         IF hash_edges.COUNT = 0
         THEN
            RETURN ary_output;
            
         END IF;

      --------------------------------------------------------------------------
      -- Step 30
      -- Grab an edge from the bucket
      --------------------------------------------------------------------------
         int_hash_index := hash_edges.FIRST;
         obj_current_edge := hash_edges(int_hash_index);
         hash_edges.DELETE(int_hash_index);

      --------------------------------------------------------------------------
      -- Step 40
      -- Create a new ring object
      --------------------------------------------------------------------------
         ary_output.EXTEND();
         ary_output(int_ring_index) := dz_topo_ring(
             obj_current_edge
            ,int_ring_index
         );

      --------------------------------------------------------------------------
      -- Step 50
      -- Search for a connecting node
      --------------------------------------------------------------------------
         <<new_edge>>
         WHILE boo_check
         LOOP
            ary_candidates := dz_topo_edge_list();
            int_candidates := 1;

      --------------------------------------------------------------------------
      -- Step 60
      -- Loop through the hash looking for node matches
      --------------------------------------------------------------------------
            int_hash_index := hash_edges.FIRST;
            LOOP
               EXIT WHEN NOT hash_edges.EXISTS(int_hash_index);

         ----------------------------------------------------------------------
         -- Step 60.10
         -- Search for a node matches in the bucket
         ----------------------------------------------------------------------
               IF  ary_output(int_ring_index).head_node_id = hash_edges(int_hash_index).start_node_id
               AND ary_output(int_ring_index).ring_interior = hash_edges(int_hash_index).interior_side
               THEN
                  ary_candidates.EXTEND();
                  ary_candidates(int_candidates) := hash_edges(int_hash_index);
                  int_candidates := int_candidates + 1;
                  
               ELSIF ary_output(int_ring_index).head_node_id = hash_edges(int_hash_index).end_node_id
               AND ary_output(int_ring_index).ring_interior = hash_edges(int_hash_index).exterior_side()
               THEN
                  ary_candidates.EXTEND();
                  ary_candidates(int_candidates) := hash_edges(int_hash_index);
                  ary_candidates(int_candidates).flip();
                  int_candidates := int_candidates + 1;
                  
               END IF;

         ----------------------------------------------------------------------
         -- Step 60.20
         -- Increment to the next edge in the hash bucket
         ----------------------------------------------------------------------
               int_hash_index  := hash_edges.NEXT(int_hash_index);

            END LOOP;

      --------------------------------------------------------------------------
      -- Step 70
      -- Decide what to do based on the number of matches, one is most easy
      --------------------------------------------------------------------------
            IF ary_candidates.COUNT = 1
            THEN
               ary_output(int_ring_index).append_edge(ary_candidates(1));
               hash_edges.DELETE(ary_candidates(1).edge_id);
               
            ELSE
               -- Reversing the direction seems to be the most efficient way to proceed
               IF ary_output(int_ring_index).try_reversal = 'TRUE'
               THEN
                  ary_output(int_ring_index).flip();
                  ary_output(int_ring_index).try_reversal := 'FALSE';
                  CONTINUE new_edge;
                  
               END IF;

               -- Do not go through the intersection
               -- rather move on to a new ring
               int_ring_index := int_ring_index + 1;
               CONTINUE new_ring;
            
            END IF;
     
      --------------------------------------------------------------------------
      -- Step 80
      -- Check that process is not stuck
      --------------------------------------------------------------------------
            IF int_deadman > p_input_edges.COUNT * 2
            THEN
               RAISE_APPLICATION_ERROR(-20001,'deadman switch!');
               
            ELSE
               int_deadman := int_deadman + 1;
               
            END IF;

      --------------------------------------------------------------------------
      -- Step 90
      -- Check if ring is complete
      --------------------------------------------------------------------------
            IF ary_output(int_ring_index).head_node_id = ary_output(int_ring_index).tail_node_id
            THEN
               IF ary_output(int_ring_index).shape.get_gtype() = 2
               THEN
                  ary_output(int_ring_index).shape := MDSYS.SDO_GEOMETRY(
                     TO_NUMBER(ary_output(int_ring_index).shape.get_dims() || '003'),
                     ary_output(int_ring_index).shape.SDO_SRID,
                     NULL,
                     MDSYS.SDO_ELEM_INFO_ARRAY(1,1003,1),
                     ary_output(int_ring_index).shape.SDO_ORDINATES
                  );
                     
               END IF;
                  
               int_ring_index := int_ring_index + 1;
               
               CONTINUE new_ring;

            END IF;
 
      --------------------------------------------------------------------------
      -- Step 100
      -- Go back and try again
      --------------------------------------------------------------------------
         END LOOP; --new edge--
         
      END LOOP; --new ring--
   
   END edges2rings;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION edges2strings(
      p_input_edges   IN  dz_topo_edge_list
   ) RETURN dz_topo_ring_list
   AS
      ary_output       dz_topo_ring_list;
      hash_edges       topo_edge_hash;
      obj_current_edge dz_topo_edge;
      int_ring_index   PLS_INTEGER;
      int_deadman      PLS_INTEGER;
      int_hash_index   PLS_INTEGER;
      ary_candidates   dz_topo_edge_list;
      int_candidates   PLS_INTEGER;
      boo_check        BOOLEAN := TRUE;

   BEGIN
      
      --------------------------------------------------------------------------
      -- Step 10
      -- Initialize ring creation
      --------------------------------------------------------------------------
      int_ring_index := 1;
      ary_output := dz_topo_ring_list();
      
      --------------------------------------------------------------------------
      -- Step 10
      -- Convert the incoming object array into a hash with edge id as key
      -- Store a second copy for retrieving edges when we unravel a ring
      --------------------------------------------------------------------------
      FOR i IN 1 .. p_input_edges.COUNT
      LOOP
         -- Pull out any single loops
         IF p_input_edges(i).start_node_id = p_input_edges(i).end_node_id
         THEN
            ary_output.EXTEND();
            ary_output(int_ring_index) := dz_topo_ring(
               p_input_edges(i),
               int_ring_index
            );
            ary_output(int_ring_index).finalize();
            int_ring_index := int_ring_index + 1;
         ELSE
            hash_edges(p_input_edges(i).edge_id) := p_input_edges(i);
         END IF;
         
      END LOOP;

      --------------------------------------------------------------------------
      -- Step 30
      -- Exit if hash is empty of edges
      --------------------------------------------------------------------------
      int_deadman := 0;
      <<new_ring>>
      WHILE boo_check
      LOOP
         IF hash_edges.COUNT = 0
         THEN
            RETURN ary_output;
         END IF;

      --------------------------------------------------------------------------
      -- Step 30
      -- Grab an edge from the bucket
      --------------------------------------------------------------------------
         int_hash_index := hash_edges.FIRST;
         obj_current_edge := hash_edges(int_hash_index);
         hash_edges.DELETE(int_hash_index);

      --------------------------------------------------------------------------
      -- Step 40
      -- Create a new ring object
      --------------------------------------------------------------------------
         ary_output.EXTEND();
         ary_output(int_ring_index) := dz_topo_ring(
             obj_current_edge
            ,int_ring_index
         );

      --------------------------------------------------------------------------
      -- Step 50
      -- Search for a connecting node
      --------------------------------------------------------------------------
         <<new_edge>>
         WHILE boo_check
         LOOP
            ary_candidates := dz_topo_edge_list();
            int_candidates := 1;

      --------------------------------------------------------------------------
      -- Step 60
      -- Loop through the hash looking for node matches
      --------------------------------------------------------------------------
            int_hash_index := hash_edges.FIRST;
            LOOP
               EXIT WHEN NOT hash_edges.EXISTS(int_hash_index);

               ----------------------------------------------------------------------
               -- Step 60.10
               -- Search for a node matches in the bucket
               ----------------------------------------------------------------------
               IF  ary_output(int_ring_index).head_node_id = hash_edges(int_hash_index).start_node_id
               AND ary_output(int_ring_index).ring_interior = hash_edges(int_hash_index).interior_side
               THEN
                  ary_candidates.EXTEND();
                  ary_candidates(int_candidates) := hash_edges(int_hash_index);
                  int_candidates := int_candidates + 1;
                  
               ELSIF ary_output(int_ring_index).head_node_id = hash_edges(int_hash_index).end_node_id
               AND ary_output(int_ring_index).ring_interior = hash_edges(int_hash_index).exterior_side()
               THEN
                  ary_candidates.EXTEND();
                  ary_candidates(int_candidates) := hash_edges(int_hash_index);
                  ary_candidates(int_candidates).flip();
                  int_candidates := int_candidates + 1;
                  
               END IF;

               ----------------------------------------------------------------------
               -- Step 60.20
               -- Increment to the next edge in the hash bucket
               ----------------------------------------------------------------------
               int_hash_index  := hash_edges.NEXT(int_hash_index);

            END LOOP;

      --------------------------------------------------------------------------
      -- Step 70
      -- Decide what to do based on the number of matches, one is most easy
      --------------------------------------------------------------------------
            IF ary_candidates.COUNT = 1
            THEN
               ary_output(int_ring_index).append_edge(ary_candidates(1));
               hash_edges.DELETE(ary_candidates(1).edge_id);
               
            ELSE
               
               -- Reversing the direction seems to be the most efficient way to proceed
               IF ary_output(int_ring_index).try_reversal = 'TRUE'
               THEN
                  ary_output(int_ring_index).flip();
                  ary_output(int_ring_index).try_reversal := 'FALSE';
                  CONTINUE new_edge;
                  
               END IF;

               -- Do not go through the intersection
               -- rather move on to a new ring
               int_ring_index := int_ring_index + 1;
               CONTINUE new_ring;
            
            END IF;
      
      --------------------------------------------------------------------------
      -- Step 80
      -- Check that process is not stuck
      --------------------------------------------------------------------------
            IF int_deadman > p_input_edges.COUNT * 2
            THEN
               RAISE_APPLICATION_ERROR(-20001,'deadman switch');
               
            ELSE
               int_deadman := int_deadman + 1;
               
            END IF;

      --------------------------------------------------------------------------
      -- Step 90
      -- Check if ring is complete
      --------------------------------------------------------------------------
            IF ary_output(int_ring_index).head_node_id = ary_output(int_ring_index).tail_node_id
            THEN
               ary_output(int_ring_index).finalize();
               int_ring_index := int_ring_index + 1;
               CONTINUE new_ring;

            END IF;

      --------------------------------------------------------------------------
      -- Step 100
      -- Go back and try again
      --------------------------------------------------------------------------
         END LOOP;
      
      END LOOP;

   END edges2strings;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION strings2rings(
      p_input_strings   IN  dz_topo_ring_list
   ) RETURN dz_topo_ring_list
   AS
      ary_rings        dz_topo_ring_list;
      TYPE dz_topo_ring_hash IS TABLE OF dz_topo_ring
      INDEX BY PLS_INTEGER;
      hash_strings     dz_topo_ring_hash;
      ary_candidates   dz_topo_ring_list;
      int_candidates   PLS_INTEGER;
      obj_string       dz_topo_ring;
      int_ring_index   PLS_INTEGER;
      int_hash_index   PLS_INTEGER;
      int_guess        PLS_INTEGER;
      boo_check        BOOLEAN := TRUE;
      int_deadman      PLS_INTEGER := 1;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Create a new array of rings that are complete and hash of incompletes
      --------------------------------------------------------------------------
      int_ring_index := 1;
      ary_rings := dz_topo_ring_list();
      FOR i IN 1 .. p_input_strings.COUNT
      LOOP
         IF p_input_strings(i).ring_status = 'C'
         THEN
            ary_rings.EXTEND(1);
            ary_rings(int_ring_index) := p_input_strings(i);
            int_ring_index := int_ring_index + 1;
            
         ELSIF p_input_strings(i).ring_status = 'I'
         THEN
            hash_strings(p_input_strings(i).ring_id) := p_input_strings(i);
            hash_strings(p_input_strings(i).ring_id).try_reversal := 'FALSE';
            hash_strings(p_input_strings(i).ring_id).node_list := MDSYS.SDO_NUMBER_ARRAY();
            hash_strings(p_input_strings(i).ring_id).node_list.EXTEND(2);
            hash_strings(p_input_strings(i).ring_id).node_list(1) := hash_strings(p_input_strings(i).ring_id).head_node_id;
            hash_strings(p_input_strings(i).ring_id).node_list(2) := hash_strings(p_input_strings(i).ring_id).tail_node_id;
            hash_strings(p_input_strings(i).ring_id).edge_list := MDSYS.SDO_NUMBER_ARRAY();
            hash_strings(p_input_strings(i).ring_id).edge_list.EXTEND(1);
            hash_strings(p_input_strings(i).ring_id).edge_list(1) := p_input_strings(i).ring_id;
            
         END IF;
         
      END LOOP;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Exit early if everything is complete
      --------------------------------------------------------------------------
      <<new_string>>
      WHILE boo_check
      LOOP
         IF hash_strings.COUNT = 0
         THEN
            RETURN ary_rings;
            
         END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Grab one string out of the bucket
      --------------------------------------------------------------------------
         int_hash_index := hash_strings.FIRST;
         obj_string := hash_strings(int_hash_index);
         hash_strings.DELETE(int_hash_index);
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Init the candidates bucket
      --------------------------------------------------------------------------
         <<new_try>>
         WHILE boo_check
         LOOP
            ary_candidates := dz_topo_ring_list();
            int_candidates := 1;
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Loop through the hash of strings trying to make connections
      --------------------------------------------------------------------------
            int_hash_index := hash_strings.FIRST;
            LOOP
               EXIT WHEN NOT hash_strings.EXISTS(int_hash_index);
               
               IF obj_string.head_node_id = hash_strings(int_hash_index).tail_node_id
               AND obj_string.ring_interior = hash_strings(int_hash_index).ring_interior
               THEN
                  ary_candidates.EXTEND();
                  ary_candidates(int_candidates) := hash_strings(int_hash_index);
                  int_candidates := int_candidates + 1;
                  
               ELSIF obj_string.head_node_id = hash_strings(int_hash_index).head_node_id
               AND obj_string.ring_interior != hash_strings(int_hash_index).ring_interior
               THEN
                  ary_candidates.EXTEND();
                  hash_strings(int_hash_index).flip();
                  ary_candidates(int_candidates) := hash_strings(int_hash_index);
                  int_candidates := int_candidates + 1;
                  
               END IF;
               
               int_hash_index  := hash_strings.NEXT(int_hash_index);
               
            END LOOP; 
      
      --------------------------------------------------------------------------
      -- Step 50
      -- Decide what to do based on the number of matches, one is most easy
      --------------------------------------------------------------------------
            IF ary_candidates.COUNT = 1
            THEN
               obj_string.append_edge(ary_candidates(1));
               hash_strings.DELETE(ary_candidates(1).ring_id);
               
            ELSE
               
               -- Reversing the direction seems to be the most efficient way to proceed
               IF obj_string.try_reversal = 'TRUE'
               THEN
                  obj_string.flip();
                  obj_string.try_reversal := 'FALSE';
                  CONTINUE new_try;
                  
               END IF;

               int_guess := ROUND(dbms_random.value(1,ary_candidates.COUNT));
               obj_string.append_edge(ary_candidates(int_guess));
               hash_strings.DELETE(ary_candidates(int_guess).ring_id);
            
            END IF;
      
      --------------------------------------------------------------------------
      -- Step 60
      -- Check if ring is complete
      --------------------------------------------------------------------------
            IF obj_string.tail_node_id = obj_string.head_node_id
            THEN
               obj_string.finalize();
               ary_rings.EXTEND(1);
               ary_rings(int_ring_index) := obj_string;
               int_ring_index := int_ring_index + 1;
               CONTINUE new_string;

            END IF;

      --------------------------------------------------------------------------
      -- Step 70
      -- Go back and try again
      --------------------------------------------------------------------------
         END LOOP; --new_try--
         
         int_deadman := int_deadman + 1;
         
         IF int_deadman > 2000000
         THEN
            RAISE_APPLICATION_ERROR(-20001,'deadman');
            
         END IF;
         
      END LOOP; --new string--
   
   END strings2rings;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION finalize_face_rings(
       p_input_rings      IN  dz_topo_ring_list
      ,p_island_edge_list IN  MDSYS.SDO_LIST_TYPE DEFAULT NULL
      ,p_tolerance        IN  NUMBER DEFAULT 0.05
   ) RETURN dz_topo_ring_list
   AS
      ary_output    dz_topo_ring_list := p_input_rings;
      num_tolerance NUMBER := p_tolerance;
      
   BEGIN
      
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_input_rings IS NULL
      OR p_input_rings.COUNT = 0
      THEN
         RETURN NULL;
         
      END IF;
      
      IF num_tolerance IS NULL
      THEN
         num_tolerance := 0.05;
      
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Assign rings using the island_edge_id_list
      --------------------------------------------------------------------------
      FOR i IN 1 .. p_input_rings.COUNT
      LOOP
         IF p_island_edge_list IS NULL
         THEN
            ary_output(i).ring_type := 'E';
            dz_topo_util.verify_ordinate_rotation(
                p_rotation => 'CCW'
               ,p_input    => ary_output(i).shape
               ,p_area     => ary_output(i).ring_size
            );
         
         ELSE
            <<outer>>
            
            FOR j IN 1 .. p_island_edge_list.COUNT
            LOOP
               <<inner>>
               
               FOR k IN 1 .. ary_output(i).edge_list.COUNT
               LOOP
                  IF ABS(p_island_edge_list(j)) = ary_output(i).edge_list(k)
                  THEN
                     ary_output(i).ring_type := 'I';
                     
                     dz_topo_util.verify_ordinate_rotation(
                         p_rotation => 'CW'
                        ,p_input    => ary_output(i).shape
                        ,p_area     => ary_output(i).ring_size
                     );
                     
                     ary_output(i).ring_status := 'C';
                     
                     EXIT outer;
                     
                  END IF; 
               
               END LOOP;
               
            END LOOP;
            
         END IF;
         
      END LOOP;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Return what we got
      --------------------------------------------------------------------------
      RETURN ary_output;
   
   END finalize_face_rings;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION finalize_generic_rings(
       p_input_rings    IN  dz_topo_ring_list
      ,p_tolerance      IN  NUMBER DEFAULT 0.05
   ) RETURN dz_topo_ring_list
   AS
      ary_output    dz_topo_ring_list := p_input_rings;
      num_tolerance NUMBER := p_tolerance;
      str_relate    VARCHAR2(4000 Char);
      
   BEGIN
      
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF num_tolerance IS NULL
      THEN
         num_tolerance := 0.05;
      
      END IF;
      
      IF p_input_rings IS NULL
      OR p_input_rings.COUNT = 0
      THEN
         RETURN NULL;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Make sure the rings have an id and zero the insider count
      --------------------------------------------------------------------------
      FOR i IN 1 .. ary_output.COUNT
      LOOP
         IF ary_output(i).ring_id IS NULL
         THEN
            ary_output(i).ring_id := i;
            
         END IF;
         
         ary_output(i).insider_count := 0;
         
      END LOOP;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Generate insider count
      --------------------------------------------------------------------------
      FOR i IN 1 .. ary_output.COUNT
      LOOP
         FOR j IN 1 .. ary_output.COUNT
         LOOP
            IF ary_output(i).ring_id <> ary_output(j).ring_id
            THEN
               str_relate := MDSYS.SDO_GEOM.RELATE(
                   ary_output(i).shape
                  ,'DETERMINE'
                  ,ary_output(j).shape
                  ,num_tolerance
               );
               
               IF str_relate IN ('INSIDE','COVEREDBY')
               THEN
                  ary_output(i).insider_count := ary_output(i).insider_count + 1;
                  
               END IF;
            
            END IF;
            
         END LOOP;
         
      END LOOP;
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Assign the proper type code
      --------------------------------------------------------------------------
      FOR i IN 1 .. ary_output.COUNT
      LOOP
         IF ary_output(i).insider_count = 0
         OR MOD(ary_output(i).insider_count,2) = 0
         THEN
            ary_output(i).ring_type := 'E';
            dz_topo_util.verify_ordinate_rotation(
                p_rotation => 'CCW'
               ,p_input    => ary_output(i).shape
               ,p_area     => ary_output(i).ring_size
            );
            
         ELSE
            ary_output(i).ring_type := 'I';
            dz_topo_util.verify_ordinate_rotation(
                p_rotation => 'CW'
               ,p_input    => ary_output(i).shape
               ,p_area     => ary_output(i).ring_size
            );
            
         END IF;
         
      END LOOP;
      
      --------------------------------------------------------------------------
      -- Step 50
      -- Return what we got
      --------------------------------------------------------------------------
      RETURN ary_output;
      
   END finalize_generic_rings;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION sort_rings_by_size(
       p_input_rings   IN  dz_topo_ring_list
      ,p_direction     IN VARCHAR2 DEFAULT 'ASC'
   ) RETURN dz_topo_ring_list
   AS
      str_direction VARCHAR2(4000 Char) := UPPER(p_direction);
      obj_temp      dz_topo_ring;
      ary_output    dz_topo_ring_list;
      i             PLS_INTEGER;

   BEGIN

      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF str_direction IS NULL
      THEN
         str_direction := 'ASC';
         
      ELSIF str_direction NOT IN ('ASC','DESC')
      THEN
         RAISE_APPLICATION_ERROR(-20001,'direction parameter may only be ASC or DESC');
         
      END IF;

      --------------------------------------------------------------------------
      -- Step 20
      -- Exit early if input is empty or one item
      --------------------------------------------------------------------------
      ary_output := dz_topo_ring_list();
      IF p_input_rings IS NULL
      OR p_input_rings.COUNT = 0
      THEN
         RETURN ary_output;
         
      ELSIF p_input_rings.COUNT = 1
      THEN
         RETURN p_input_rings;
         
      END IF;

      --------------------------------------------------------------------------
      -- Step 30
      -- Bubble sort the objects on ring_size attribute
      --------------------------------------------------------------------------
      ary_output := p_input_rings;
      i := p_input_rings.COUNT-1;
      
      WHILE ( i > 0 )
      LOOP
         FOR j IN 1 .. i
         LOOP
            IF str_direction = 'DESC'
            THEN
               IF ary_output(j).ring_size < ary_output(j+1).ring_size
               THEN
                  obj_temp        := ary_output(j);
                  ary_output(j)   := ary_output(j+1);
                  ary_output(j+1) := obj_temp;
                  
               END IF;

            ELSE
               IF ary_output(j).ring_size > ary_output(j+1).ring_size
               THEN
                  obj_temp        := ary_output(j);
                  ary_output(j)   := ary_output(j+1);
                  ary_output(j+1) := obj_temp;
                  
               END IF;

            END IF;

         END LOOP;

         i := i - 1;

      END LOOP;

      --------------------------------------------------------------------------
      -- Step 40
      -- Cough back results
      --------------------------------------------------------------------------
      RETURN ary_output;

   END sort_rings_by_size;

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION rings2sdo(
       p_input_rings    IN  dz_topo_ring_list
      ,p_remove_holes   IN  VARCHAR2 DEFAULT 'FALSE'
   ) RETURN MDSYS.SDO_GEOMETRY
   AS
      str_remove_holes VARCHAR2(5 Char) := UPPER(p_remove_holes);
      sdo_output       MDSYS.SDO_GEOMETRY;
      ary_exteriors    dz_topo_ring_list;
      ary_interiors    dz_topo_ring_list;
      int_exter        PLS_INTEGER;
      int_inter        PLS_INTEGER;

   BEGIN

      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF str_remove_holes IS NULL
      THEN
         str_remove_holes := 'FALSE';
         
      ELSIF str_remove_holes NOT IN ('TRUE','FALSE')
      THEN
         RAISE_APPLICATION_ERROR(-20001,'boolean error');
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Exit early if possible
      --------------------------------------------------------------------------
      IF p_input_rings IS NULL
      OR p_input_rings.COUNT = 0
      THEN
         RETURN NULL;
         
      ELSIF p_input_rings.COUNT = 1
      THEN
         RETURN p_input_rings(1).shape;
         
      END IF;

      --------------------------------------------------------------------------
      -- Step 30
      -- Segregate rings into holes and exteriors
      --------------------------------------------------------------------------
      int_exter := 1;
      int_inter := 1;
      ary_exteriors := dz_topo_ring_list();
      ary_interiors := dz_topo_ring_list();
      FOR i IN 1 .. p_input_rings.COUNT
      LOOP
         IF p_input_rings(i).ring_type IS NULL
         OR p_input_rings(i).ring_type = 'E'
         THEN
            ary_exteriors.EXTEND();
            ary_exteriors(int_exter) := p_input_rings(i);
            int_exter := int_exter + 1;
            
         ELSIF p_input_rings(i).ring_type = 'I'
         AND str_remove_holes = 'FALSE'
         THEN
            ary_interiors.EXTEND();
            ary_interiors(int_inter) := p_input_rings(i);
            int_inter := int_inter + 1;
            
         END IF;

      END LOOP;

      --------------------------------------------------------------------------
      -- Step 40
      -- Sort exterior rings by size sets
      --------------------------------------------------------------------------
      ary_exteriors := sort_rings_by_size(
          p_input_rings => ary_exteriors
         ,p_direction   => 'DESC'
      );

      --------------------------------------------------------------------------
      -- Step 50
      -- If exterior rings only, then append together and exit
      -- Note the condition of multi-polygon for a single face is an error
      --------------------------------------------------------------------------
      IF ary_interiors.COUNT = 0
      OR str_remove_holes = 'TRUE'
      THEN
         FOR i IN 1 .. ary_exteriors.COUNT
         LOOP
            IF sdo_output IS NULL
            THEN
               sdo_output := ary_exteriors(i).shape;
               
            ELSE
               sdo_output := MDSYS.SDO_UTIL.APPEND(
                   sdo_output
                  ,ary_exteriors(i).shape
               );
               
            END IF;
            
         END LOOP;

         RETURN sdo_output;

      END IF;
      
      --------------------------------------------------------------------------
      -- Step 60
      -- If only one exterior and some amount of interiors
      --------------------------------------------------------------------------
      IF ary_exteriors.COUNT = 1
      THEN
         sdo_output := ary_exteriors(1).shape;
         
         FOR i IN 1 .. ary_interiors.COUNT
         LOOP
            sdo_output := dz_topo_util.append_hole_to_polygon(
                p_input => sdo_output
               ,p_hole  => ary_interiors(i).shape
            );
            
         END LOOP;
      
         RETURN sdo_output;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 70
      -- More complicated system, 
      --------------------------------------------------------------------------
      FOR i IN 1 .. ary_exteriors.COUNT
      LOOP
         IF ary_exteriors(i).ring_group IS NULL
         THEN
            ary_exteriors(i).ring_group := i;
         
         END IF;
         
      END LOOP;
      
      <<outer>>
      FOR i IN 1 .. ary_interiors.COUNT
      LOOP
         IF ary_interiors(i).ring_group IS NULL
         THEN
            ary_interiors(i).ring_group := 1;
            
         END IF;
         
         <<inner>>
         FOR j IN 1 .. ary_exteriors.COUNT
         LOOP
            IF ary_interiors(i).ring_group = ary_exteriors(j).ring_group
            THEN
               ary_exteriors(j).shape := dz_topo_util.append_hole_to_polygon(
                   p_input => ary_exteriors(j).shape
                  ,p_hole  => ary_interiors(i).shape
               );
               EXIT inner;
               
            END IF;
            
         END LOOP;
         
      END LOOP;
      
      FOR i IN 1 .. ary_exteriors.COUNT
      LOOP
         IF sdo_output IS NULL
         THEN
            sdo_output := ary_exteriors(i).shape;
               
         ELSE
            sdo_output := MDSYS.SDO_UTIL.APPEND(sdo_output,ary_exteriors(i).shape);
               
         END IF;
            
      END LOOP;
      
      RETURN sdo_output;

   END rings2sdo;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE get_faces(
       p_input          IN  MDSYS.SDO_TOPO_GEOMETRY
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
      ,p_output         OUT MDSYS.SDO_NUMBER_ARRAY
   ) 
   AS
      obj_topology       dz_topology := p_topology_obj;
      str_sql            VARCHAR2(4000 Char);
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_input IS NULL
      THEN
         RETURN;
         
      END IF;
      
      IF p_input.tg_type NOT IN (3,4)
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topo geometry must be polygon');
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Get topology name
      --------------------------------------------------------------------------
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topo_geom => p_input
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;

      IF obj_topology.topo_layer().tg_layer_level <> 0
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topo geom must be layer zero');
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Pull the list of faces from the relation table
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || 'a.topo_id '
              || 'FROM '
              || obj_topology.relation_table() || ' a '
              || 'WHERE '
              || '    a.tg_layer_id = :p01 '
              || 'AND a.tg_id = :p02 '
              || 'AND a.topo_type = 3 ';
   
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO p_output
      USING p_input.tg_layer_id,p_input.tg_id;
         
   END get_faces;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION get_faces(
       p_input          IN  MDSYS.SDO_TOPO_GEOMETRY
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
   ) RETURN MDSYS.SDO_NUMBER_ARRAY
   AS
      ary_output    MDSYS.SDO_NUMBER_ARRAY;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_input IS NULL
      THEN
         RETURN NULL;
         
      END IF;
      
      IF p_input.tg_type NOT IN (3,4)
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topo geometry must be polygon');
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Return faces
      --------------------------------------------------------------------------
      get_faces(
          p_input         => p_input
         ,p_topology_obj  => p_topology_obj
         ,p_output        => ary_output
      );
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Return results
      --------------------------------------------------------------------------
      RETURN ary_output;
      
   END get_faces;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION face2sdo(
       p_topology_owner IN  VARCHAR2 DEFAULT NULL
      ,p_topology_name  IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
      ,p_face_id        IN  NUMBER
      ,p_remove_holes   IN  VARCHAR2 DEFAULT 'FALSE'
   ) RETURN MDSYS.SDO_GEOMETRY
   AS
      obj_topology     dz_topology := p_topology_obj;
      str_remove_holes VARCHAR2(4000 Char) := UPPER(p_remove_holes);
      str_sql          VARCHAR2(4000 Char);
      ary_edges        dz_topo_edge_list;
      sdo_output       MDSYS.SDO_GEOMETRY;
      ary_rings        dz_topo_ring_list;
      ary_island_edges MDSYS.SDO_LIST_TYPE;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_face_id IS NULL
      THEN
         RETURN NULL;
         
      END IF;
      
      IF str_remove_holes IS NULL
      THEN
         str_remove_holes := 'FALSE';
      
      ELSIF str_remove_holes NOT IN ('TRUE','FALSE')
      THEN
         RAISE_APPLICATION_ERROR(-20001,'boolean error');
      
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Verify topology
      --------------------------------------------------------------------------
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topology_owner => p_topology_owner
            ,p_topology_name  => p_topology_name
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;
      
      -------------------------------------------------------------------------
      -- Step 30
      -- Pull the island edges
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || 'a.island_edge_id_list '
              || 'FROM '
              || obj_topology.face_table() || ' a '
              || 'WHERE '
              || 'a.face_id = :p01 '; 
              
      EXECUTE IMMEDIATE str_sql
      INTO ary_island_edges
      USING p_face_id;
       
      --------------------------------------------------------------------------
      -- Step 40
      -- Pull the required edges
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || dz_topo_util.c_schema() || '.dz_topo_edge( '
              || '    a.edge_id '
              || '   ,''L'' '
              || '   ,a.start_node_id '
              || '   ,a.end_node_id '
              || '   ,a.geometry '
              || '   ,0 '
              || ') '
              || 'FROM '
              || obj_topology.edge_table() || ' a '
              || 'WHERE '
              || 'a.left_face_id = :p01 AND '
              || 'a.right_face_id <> :p02 '
              || 'UNION ALL SELECT '
              || dz_topo_util.c_schema() || '.dz_topo_edge( '
              || '    a.edge_id '
              || '   ,''R'' '
              || '   ,a.start_node_id '
              || '   ,a.end_node_id '
              || '   ,a.geometry '
              || '   ,0 '
              || ') '
              || 'FROM '
              || obj_topology.edge_table() || ' a '
              || 'WHERE '
              || 'a.left_face_id <> :p03 AND '
              || 'a.right_face_id = :p04 ';
      
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO ary_edges
      USING p_face_id,p_face_id,p_face_id,p_face_id;
      
      --------------------------------------------------------------------------
      -- Step 50
      -- Convert to sdo
      --------------------------------------------------------------------------
      ary_rings := edges2rings(
          p_input_edges  => ary_edges
      );
      
      ary_rings := finalize_face_rings(
          p_input_rings      => ary_rings
         ,p_island_edge_list => ary_island_edges
         ,p_tolerance        => obj_topology.tolerance
      );
      
      sdo_output := rings2sdo(
          p_input_rings  => ary_rings
         ,p_remove_holes => str_remove_holes
      );
      
      --------------------------------------------------------------------------
      -- Step 60
      --Return what we got
      --------------------------------------------------------------------------
      RETURN sdo_output;
 
   END face2sdo;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION get_faces_as_sdo(
       p_input          IN  MDSYS.SDO_TOPO_GEOMETRY
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
   ) RETURN MDSYS.SDO_GEOMETRY
   AS
      obj_topology       dz_topology := p_topology_obj;
      ary_faces          MDSYS.SDO_NUMBER_ARRAY;
      sdo_output         MDSYS.SDO_GEOMETRY;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_input IS NULL
      THEN
         RETURN NULL;
         
      END IF;
      
      IF p_input.tg_type NOT IN (3,4)
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topo geometry must be polygon');
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Check topology object
      --------------------------------------------------------------------------
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topo_geom => p_input
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- get faces
      --------------------------------------------------------------------------
      get_faces(
          p_input         => p_input
         ,p_topology_obj  => obj_topology 
         ,p_output        => ary_faces
      );
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Collect each face as a separate sdo geometry
      --------------------------------------------------------------------------
      FOR i IN 1 .. ary_faces.COUNT
      LOOP
         IF sdo_output IS NULL
         THEN
            sdo_output := face2sdo(
                p_topology_obj   => obj_topology
               ,p_face_id        => ary_faces(i)
            );
            
         ELSE
            sdo_output := MDSYS.SDO_UTIL.APPEND(
                sdo_output
               ,face2sdo(
                   p_topology_obj   => obj_topology
                  ,p_face_id        => ary_faces(i)
               )
            );
            
         END IF;
      
      END LOOP;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Return what we got
      --------------------------------------------------------------------------
      RETURN sdo_output;
      
   END get_faces_as_sdo;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE remove_faces_from_topo_raw(
       p_input          IN  MDSYS.SDO_TOPO_GEOMETRY
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
      ,p_face_ids       IN  MDSYS.SDO_NUMBER_ARRAY
      ,p_returning      OUT MDSYS.SDO_NUMBER_ARRAY
   )
   AS
      obj_topology       dz_topology := p_topology_obj;
      str_sql            VARCHAR2(4000 Char);
      
   BEGIN
   
      p_returning := MDSYS.SDO_NUMBER_ARRAY();
      
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_input IS NULL
      OR p_face_ids IS NULL
      OR p_face_ids.COUNT = 0
      THEN
         RETURN;
      END IF;
      
      IF p_input.tg_type NOT IN (3,4)
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topo geometry must be polygon');
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Verify topology object
      --------------------------------------------------------------------------   
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topo_geom => p_input
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;
      
      IF obj_topology.topo_layer().tg_layer_level <> 0
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topo geom must be layer zero to alter faces');
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Remove the faces from the geometry
      --------------------------------------------------------------------------
      str_sql := 'DELETE FROM '
              || obj_topology.relation_table() || ' a '
              || 'WHERE '
              || '    a.tg_layer_id = :p01 '
              || 'AND a.tg_id = :p02 '
              || 'AND a.topo_type = 3 '
              || 'AND a.topo_id IN (SELECT column_value FROM TABLE(:p03)) '
              || 'RETURNING a.topo_id INTO :p04 ';
   
      EXECUTE IMMEDIATE str_sql
      USING p_input.tg_layer_id,p_input.tg_id,p_face_ids
      RETURNING BULK COLLECT INTO p_returning;
      
      COMMIT;
      
   END remove_faces_from_topo_raw;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE add_faces_to_topo_raw(
       p_input          IN  MDSYS.SDO_TOPO_GEOMETRY
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
      ,p_face_ids       IN  MDSYS.SDO_NUMBER_ARRAY
      ,p_returning      OUT MDSYS.SDO_NUMBER_ARRAY
   )
   AS
      obj_topology       dz_topology := p_topology_obj;
      str_sql            VARCHAR2(4000 Char);
      ary_valid_faces    MDSYS.SDO_NUMBER_ARRAY;
      
   BEGIN
   
      p_returning := MDSYS.SDO_NUMBER_ARRAY();
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_input IS NULL
      OR p_face_ids IS NULL
      OR p_face_ids.COUNT = 0
      THEN
         RETURN;
         
      END IF;
      
      IF p_input.tg_type NOT IN (3,4)
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topo geometry must be polygon');
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Verify topology object
      --------------------------------------------------------------------------   
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topo_geom => p_input
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;
      
      IF obj_topology.topo_layer().tg_layer_level <> 0
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topo geom must be layer zero to alter faces');
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Filter face list to existing faces that are not already part of topo
      -------------------------------------------------------------------------- 
      str_sql := 'SELECT '
              || 'a.face_id '
              || 'FROM '
              || obj_topology.face_table() || ' a '
              || 'WHERE '
              || '    a.face_id IN (SELECT column_value FROM TABLE(:p01)) '
              || 'AND a.face_id NOT IN ( '
              || '   SELECT '
              || '   bb.topo_id '
              || '   FROM '
              || '   ' || obj_topology.relation_table() || ' bb '
              || '   WHERE '
              || '       bb.tg_id = :p01 '
              || '   AND bb.tg_layer_id = :p02 '
              || '   AND bb.topo_type = 3 '
              || ') ';
 
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO ary_valid_faces
      USING 
       p_face_ids
      ,p_input.tg_id
      ,p_input.tg_layer_id;
      
      IF ary_valid_faces IS NULL
      OR ary_valid_faces.COUNT = 0
      THEN
         RETURN;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Add faces to relation tables for topo
      --------------------------------------------------------------------------
      str_sql := 'INSERT INTO ' || obj_topology.relation_table() || ' ('
              || '    tg_layer_id '
              || '   ,tg_id '
              || '   ,topo_id '
              || '   ,topo_type '
              || '   ,topo_attribute '
              || ') '
              || 'SELECT '
              || ' :p01 '
              || ',:p02 '
              || ',a.column_value '
              || ',3 '
              || ',NULL '
              || 'FROM '
              || 'TABLE(:p03) a ';
   
      EXECUTE IMMEDIATE str_sql
      USING 
       p_input.tg_layer_id
      ,p_input.tg_id
      ,ary_valid_faces;
      
      COMMIT;
      
      p_returning := ary_valid_faces;
      
   END add_faces_to_topo_raw;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION get_interior_edges(
       p_input          IN  MDSYS.SDO_TOPO_GEOMETRY
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
   ) RETURN MDSYS.SDO_NUMBER_ARRAY
   AS
      obj_topology       dz_topology := p_topology_obj;
      ary_faces          MDSYS.SDO_NUMBER_ARRAY;
      ary_unneeded_edges MDSYS.SDO_NUMBER_ARRAY;
      ary_topo_edges     dz_topo_edge_list;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_input IS NULL
      THEN
         RETURN NULL;
         
      END IF;
      
      IF p_input.tg_type NOT IN (3,4)
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topo geometry must be polygon');
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Verify topology object
      --------------------------------------------------------------------------
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topo_geom => p_input
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Get faces for the topo feature
      --------------------------------------------------------------------------   
      get_faces(
          p_input         => p_input
         ,p_topology_obj  => obj_topology
         ,p_output        => ary_faces
      );
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Get interior edges
      --------------------------------------------------------------------------
      faces2edges(
          p_topology_obj    => obj_topology
         ,p_faces           => ary_faces
         ,p_subset          => 'INTERIOR'
         ,p_output          => ary_topo_edges
      );
      
      --------------------------------------------------------------------------
      -- Step 50
      -- Get edge ids
      --------------------------------------------------------------------------
      ary_unneeded_edges := MDSYS.SDO_NUMBER_ARRAY();
      ary_unneeded_edges.EXTEND(ary_topo_edges.COUNT);
      FOR i IN 1 .. ary_topo_edges.COUNT
      LOOP
         ary_unneeded_edges(i) := ary_topo_edges(i).edge_id;
         
      END LOOP;
      
      RETURN ary_unneeded_edges;
      
   END get_interior_edges;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION get_interior_edges_as_sdo(
       p_input          IN  MDSYS.SDO_TOPO_GEOMETRY
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
   ) RETURN MDSYS.SDO_GEOMETRY
   AS
      obj_topology       dz_topology := p_topology_obj;
      ary_faces          MDSYS.SDO_NUMBER_ARRAY;
      sdo_output         MDSYS.SDO_GEOMETRY;
      ary_topo_edges     dz_topo_edge_list;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_input IS NULL
      THEN
         RETURN NULL;
         
      END IF;
      
      IF p_input.tg_type NOT IN (3,4)
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topo geometry must be polygon');
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Verify topology object
      --------------------------------------------------------------------------
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topo_geom => p_input
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Get faces for the topo feature
      --------------------------------------------------------------------------   
      get_faces(
          p_input         => p_input
         ,p_topology_obj  => obj_topology
         ,p_output        => ary_faces
      );
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Get interior edges
      --------------------------------------------------------------------------
      faces2edges(
          p_topology_obj    => obj_topology
         ,p_faces           => ary_faces
         ,p_subset          => 'INTERIOR'
         ,p_output          => ary_topo_edges
      );
      
      --------------------------------------------------------------------------
      -- Step 50
      -- Get edge ids
      --------------------------------------------------------------------------
      FOR i IN 1 .. ary_topo_edges.COUNT
      LOOP
         IF sdo_output IS NULL
         THEN
            sdo_output := ary_topo_edges(i).shape;
            
         ELSE
            sdo_output := MDSYS.SDO_UTIL.APPEND(
                sdo_output
               ,ary_topo_edges(i).shape
            );
            
         END IF;
         
      END LOOP;
      
      RETURN sdo_output;
      
   END get_interior_edges_as_sdo;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION get_shared_faces(
       p_input          IN  MDSYS.SDO_TOPO_GEOMETRY
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
   ) RETURN MDSYS.SDO_NUMBER_ARRAY
   -- ( tg_layer_id, tg_id, face_id, areasqkm )
   AS
      obj_topology       dz_topology := p_topology_obj;
      str_sql            VARCHAR2(4000 Char);
      ary_faces          MDSYS.SDO_NUMBER_ARRAY;
      ary_shared_tglyrs  MDSYS.SDO_NUMBER_ARRAY;
      ary_shared_tgids   MDSYS.SDO_NUMBER_ARRAY;
      ary_shared_faces   MDSYS.SDO_NUMBER_ARRAY;
      ary_output         MDSYS.SDO_NUMBER_ARRAY;
      int_counter        PLS_INTEGER;
      
   BEGIN
   
      ary_shared_faces := MDSYS.SDO_NUMBER_ARRAY();
      
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_input IS NULL
      THEN
         RETURN NULL;
         
      END IF;
      
      IF p_input.tg_type NOT IN (3,4)
      THEN
         RAISE_APPLICATION_ERROR(-20001,'topo geometry must be polygon');
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Verify topology object
      --------------------------------------------------------------------------
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topo_geom => p_input
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Get faces for the topo feature
      --------------------------------------------------------------------------   
      get_faces(
          p_input         => p_input
         ,p_topology_obj  => obj_topology
         ,p_output        => ary_faces
      );
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Check which faces have more than the parameter feature assigned to them
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || ' a.tg_layer_id '
              || ',a.tg_id '
              || ',a.topo_id '
              || 'FROM '
              || obj_topology.relation_table() || ' a '
              || 'WHERE '
              || '   a.topo_id IN (SELECT column_value FROM TABLE(:p01)) '
              || 'AND a.tg_id <> :p02 '
              || 'AND a.tg_layer_id = :p03 '
              || 'AND a.topo_type = 3 ';
              
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO 
       ary_shared_tglyrs
      ,ary_shared_tgids
      ,ary_shared_faces
      USING 
       ary_faces
      ,p_input.tg_id
      ,p_input.tg_layer_id;
      
      IF ary_shared_tgids IS NULL
      OR ary_shared_tgids.COUNT = 0
      THEN
         RETURN NULL;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Weave into output array
      --------------------------------------------------------------------------
      ary_output := MDSYS.SDO_NUMBER_ARRAY();
      ary_output.EXTEND(ary_shared_tgids.COUNT * 3);
      int_counter := 1;
      FOR i IN 1 .. ary_shared_tgids.COUNT
      LOOP
         ary_output(int_counter) := ary_shared_tglyrs(i);
         int_counter := int_counter + 1;
         
         ary_output(int_counter) := ary_shared_tgids(i);
         int_counter := int_counter + 1;
         
         ary_output(int_counter) := ary_shared_faces(i);
         int_counter := int_counter + 1;
         
         ary_output(int_counter) := MDSYS.SDO_GEOM.SDO_AREA(
             face2sdo(
                 p_topology_obj   => obj_topology
                ,p_face_id        => ary_shared_faces(i)
             )
            ,0.05
            ,'UNIT=SQ_KM'
         );
         int_counter := int_counter + 1;
         
      END LOOP;
      
      RETURN ary_output;
   
   END get_shared_faces;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION get_face_topo_neighbors(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL  
      ,p_topology_name   IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj    IN  dz_topology DEFAULT NULL
      ,p_tg_layer_id     IN  NUMBER 
      ,p_face_id         IN  NUMBER
   ) RETURN dz_topo_vry
   AS
      obj_topology    dz_topology := p_topology_obj;
      str_sql         VARCHAR2(4000 Char);
      ary_output      dz_topo_vry;
      
   BEGIN
      
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_face_id IS NULL
      THEN
         RETURN NULL;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Verify topology object
      --------------------------------------------------------------------------
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topology_owner => p_topology_owner
            ,p_topology_name  => p_topology_name
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;
      
      obj_topology.set_active_topo_layer(
         p_tg_layer_id => p_tg_layer_id
      );
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Get SUM of exterior edges by face id
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || 'a.' || obj_topology.active_topo_column() || ' '
              || 'FROM '
              || obj_topology.active_topo_table() || ' a '
              || 'WHERE '
              || 'a.' || obj_topology.active_topo_column() || '.tg_id IN ('
              || '   SELECT '
              || '    aa.tg_id '
              || '   FROM '
              || '   ' || obj_topology.relation_table() || ' aa '
              || '   WHERE '
              || '   aa.topo_id IN ('
              || '      SELECT '
              || '      aaa.left_face_id AS face_id '
              || '      FROM '
              || '      ' || obj_topology.edge_table() || ' aaa '
              || '      WHERE '
              || '          aaa.right_face_id = :p01 '
              || '      AND aaa.left_face_id <> :p02 '
              || '      UNION ALL '
              || '      SELECT '
              || '      bbb.right_face_id AS face_id '
              || '      FROM '
              || '      ' || obj_topology.edge_table() || ' bbb '
              || '      WHERE '
              || '          bbb.right_face_id <> :p03 '
              || '      AND bbb.left_face_id = :p04 '
              || '   ) '
              || ') ';

      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO ary_output
      USING 
       p_face_id
      ,p_face_id
      ,p_face_id
      ,p_face_id;
      
      RETURN ary_output;
             
   END get_face_topo_neighbors;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION face_at_point(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL  
      ,p_topology_name   IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj    IN  dz_topology DEFAULT NULL
      ,p_input           IN  MDSYS.SDO_GEOMETRY
   ) RETURN NUMBER
   AS
      sdo_input       MDSYS.SDO_GEOMETRY := p_input;
      obj_topology    dz_topology := p_topology_obj;
      ary_results     MDSYS.SDO_TOPO_OBJECT_ARRAY;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF sdo_input IS NULL
      THEN
         RETURN NULL;
         
      END IF;
      
      IF sdo_input.get_gtype() <> 1
      THEN
         IF sdo_input.get_gtype() IN (3,7)
         THEN
            sdo_input := MDSYS.SDO_GEOM.SDO_CENTROID(
                sdo_input
               ,0.05
            );
            
         ELSE
            sdo_input := MDSYS.SDO_GEOM.SDO_CENTROID(
                MDSYS.SDO_GEOM.SDO_MBR(sdo_input)
               ,0.05
            );
         
         END IF;
         
      END IF;

      --------------------------------------------------------------------------
      -- Step 20
      -- Determine topology location
      --------------------------------------------------------------------------
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topology_owner => p_topology_owner
            ,p_topology_name  => p_topology_name
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Query the topology
      --------------------------------------------------------------------------
      ary_results := MDSYS.SDO_TOPO.GET_TOPO_OBJECTS(
          topology => obj_topology.topology_name
         ,geometry => sdo_input
      );
      
      IF ary_results IS NULL
      OR ary_results.COUNT = 0
      THEN
         RETURN NULL;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Extract the face, take the first face in case of border cases
      --------------------------------------------------------------------------
      FOR i IN 1 .. ary_results.COUNT
      LOOP
         IF ary_results(i).topo_type = 3
         THEN
            RETURN ary_results(i).topo_id;
      
         END IF;
      
      END LOOP;
      
      --------------------------------------------------------------------------
      -- Step 50
      -- No faces found so return nothing
      --------------------------------------------------------------------------
      RETURN NULL;
      
   END face_at_point;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION face_at_point_sdo(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL  
      ,p_topology_name   IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj    IN  dz_topology DEFAULT NULL
      ,p_input           IN  MDSYS.SDO_GEOMETRY
   ) RETURN MDSYS.SDO_GEOMETRY
   AS
      num_face_id   NUMBER;
      obj_topology  dz_topology := p_topology_obj;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_input IS NULL
      THEN
         RETURN NULL;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Determine topology location
      --------------------------------------------------------------------------
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topology_owner => p_topology_owner
            ,p_topology_name  => p_topology_name
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Get the face id at point, not sure what to do if we get back the uf
      --------------------------------------------------------------------------
      num_face_id := face_at_point(
          p_topology_obj   => obj_topology
         ,p_input          => p_input
      );
      
      IF num_face_id IS NULL
      OR num_face_id = -1
      THEN
         RETURN NULL;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Convert face into sdo
      --------------------------------------------------------------------------
      RETURN face2sdo(
          p_topology_obj => obj_topology
         ,p_face_id      => num_face_id
      );
      
   END face_at_point_sdo;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE drop_face_clean(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL  
      ,p_topology_name   IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj    IN  dz_topology DEFAULT NULL
      ,p_face_id         IN  NUMBER
   )
   AS
      str_sql      VARCHAR2(4000 Char);
      obj_topology dz_topology := p_topology_obj;
      ary_edges    MDSYS.SDO_NUMBER_ARRAY;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_face_id IS NULL
      THEN
         RETURN;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Determine topology location
      --------------------------------------------------------------------------
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topology_owner => p_topology_owner
            ,p_topology_name  => p_topology_name
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;
   
      str_sql := 'SELECT '
              || 'a.edge_id '
              || 'FROM '
              || obj_topology.edge_table() || ' a '
              || 'WHERE '
              || '   a.right_face_id = :p03 '
              || 'OR a.left_face_id  = :p04 ';
              
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO ary_edges
      USING p_face_id,p_face_id;
      
      FOR i IN 1 .. ary_edges.COUNT
      LOOP
         MDSYS.SDO_TOPO_MAP.REMOVE_EDGE(
            topology => obj_topology.topology_name
           ,edge_id  => ary_edges(i)
         );
      
      END LOOP;
      
      COMMIT;
      
   END drop_face_clean;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE nuke_topology(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL  
      ,p_topology_name   IN  VARCHAR2
   )
   AS
      obj_topology dz_topology;
      str_owner    VARCHAR2(30 Char) := p_topology_owner;
      
   BEGIN
    
      IF str_owner IS NULL
      THEN
         str_owner := USER;
         
      END IF;
   
      obj_topology := dz_topology(
          p_topology_owner => str_owner
         ,p_topology_name  => p_topology_name
      );
      
      IF obj_topology IS NOT NULL
      AND obj_topology.topo_layers IS NOT NULL
      AND obj_topology.topo_layers.COUNT > 0
      THEN
         FOR i IN 1.. obj_topology.topo_layers.COUNT
         LOOP
            MDSYS.SDO_TOPO.DELETE_TOPO_GEOMETRY_LAYER(
                topology    => obj_topology.topology_name
               ,table_name  => obj_topology.topo_layers(i).table_name
               ,column_name => obj_topology.topo_layers(i).column_name
            );
            
            BEGIN
               EXECUTE IMMEDIATE 
               'DROP TABLE ' || obj_topology.topo_layers(i).table_owner || '.' || obj_topology.topo_layers(i).table_name;
            
            EXCEPTION
               WHEN OTHERS THEN NULL;
               
            END;
                                      
         END LOOP;
      
      END IF;
      
      BEGIN
         EXECUTE IMMEDIATE 
         'DROP TABLE ' || obj_topology.topology_owner || '.' || obj_topology.exp_table();
      
      EXCEPTION
         WHEN OTHERS THEN NULL;
         
      END;
   
      MDSYS.SDO_TOPO.DROP_TOPOLOGY(
         topology    => obj_topology.topology_name
      );
   
   END nuke_topology;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE remove_face_island_edge_id(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL  
      ,p_topology_name   IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj    IN  dz_topology DEFAULT NULL
      ,p_face_id         IN  NUMBER
      ,p_edge_id         IN  NUMBER
   )
   AS
      obj_topology  dz_topology := p_topology_obj;
      ary_edge_list MDSYS.SDO_LIST_TYPE;
      str_sql       VARCHAR2(4000 Char);
      num_check     PLS_INTEGER;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_face_id IS NULL
      OR p_edge_id IS NULL
      THEN
         RETURN;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Determine topology location
      --------------------------------------------------------------------------
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topology_owner => p_topology_owner
            ,p_topology_name  => p_topology_name
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Verify that face exists
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || 'COUNT(*) '
              || 'FROM '
              || obj_topology.face_table() || ' a '
              || 'WHERE '
              || 'a.face_id = :p01 ';
              
      EXECUTE IMMEDIATE str_sql
      INTO num_check
      USING p_face_id;
     
      IF num_check <> 1
      THEN
         RAISE_APPLICATION_ERROR(
             -20001
            ,'face ' || p_face_id || ' does not exist in topology.'
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Collect the list of edge ids excluding the indicated one
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || 'b.column_value '
              || 'FROM '
              || obj_topology.face_table() || ' a '
              || 'CROSS JOIN '
              || 'TABLE(a.island_edge_id_list) b '
              || 'WHERE '
              || '    a.face_id = :p01 '
              || 'AND ABS(b.column_value) <> :p02 ';
              
     EXECUTE IMMEDIATE str_sql
     BULK COLLECT INTO ary_edge_list
     USING p_face_id, ABS(p_edge_id);
     
     --------------------------------------------------------------------------
      -- Step 50
      -- Update the face table with the results
      --------------------------------------------------------------------------
      str_sql := 'UPDATE ' || obj_topology.face_table() || ' a '
              || 'SET '
              || 'a.island_edge_id_list = :p01 '
              || 'WHERE '
              || 'a.face_id = :p02 ';
              
      EXECUTE IMMEDIATE str_sql
      USING ary_edge_list,p_face_id;
      
      COMMIT;
         
   END remove_face_island_edge_id;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE remove_face_island_edge_id(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL  
      ,p_topology_name   IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj    IN  dz_topology DEFAULT NULL
      ,p_face_id         IN  NUMBER
      ,p_edge_id         IN  MDSYS.SDO_NUMBER_ARRAY
   )
   AS
      obj_topology  dz_topology := p_topology_obj;
      ary_edge_list MDSYS.SDO_LIST_TYPE;
      str_sql       VARCHAR2(4000 Char);
      num_check     PLS_INTEGER;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_face_id IS NULL
      OR p_edge_id IS NULL
      THEN
         RETURN;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Determine topology location
      --------------------------------------------------------------------------
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topology_owner => p_topology_owner
            ,p_topology_name  => p_topology_name
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Verify that face exists
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || 'COUNT(*) '
              || 'FROM '
              || obj_topology.face_table() || ' a '
              || 'WHERE '
              || 'a.face_id = :p01 ';
              
      EXECUTE IMMEDIATE str_sql
      INTO num_check
      USING p_face_id;
     
      IF num_check <> 1
      THEN
         RAISE_APPLICATION_ERROR(
             -20001
            ,'face ' || p_face_id || ' does not exist in topology.'
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Collect the list of edge ids excluding the indicated one
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || 'b.column_value '
              || 'FROM '
              || obj_topology.face_table() || ' a '
              || 'CROSS JOIN '
              || 'TABLE(a.island_edge_id_list) b '
              || 'WHERE '
              || '    a.face_id = :p01 '
              || 'AND ABS(b.column_value) NOT IN (SELECT ABS(column_value) FROM TABLE(:p02)) ';
              
     EXECUTE IMMEDIATE str_sql
     BULK COLLECT INTO ary_edge_list
     USING p_face_id, p_edge_id;
     
     --------------------------------------------------------------------------
      -- Step 50
      -- Update the face table with the results
      --------------------------------------------------------------------------
      str_sql := 'UPDATE ' || obj_topology.face_table() || ' a '
              || 'SET '
              || 'a.island_edge_id_list = :p01 '
              || 'WHERE '
              || 'a.face_id = :p02 ';
              
      EXECUTE IMMEDIATE str_sql
      USING ary_edge_list,p_face_id;
      
      COMMIT;
         
   END remove_face_island_edge_id;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE append_face_island_edge_id(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL  
      ,p_topology_name   IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj    IN  dz_topology DEFAULT NULL
      ,p_face_id         IN  NUMBER
      ,p_edge_id         IN  NUMBER
   )
   AS
      obj_topology  dz_topology := p_topology_obj;
      ary_edge_list MDSYS.SDO_LIST_TYPE;
      str_sql       VARCHAR2(4000 Char);
      num_check     PLS_INTEGER;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_face_id IS NULL
      OR p_edge_id IS NULL
      THEN
         RETURN;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Determine topology location
      --------------------------------------------------------------------------
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topology_owner => p_topology_owner
            ,p_topology_name  => p_topology_name
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Verify that face exists
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || 'COUNT(*) '
              || 'FROM '
              || obj_topology.face_table() || ' a '
              || 'WHERE '
              || 'a.face_id = :p01 ';
              
      EXECUTE IMMEDIATE str_sql
      INTO num_check
      USING p_face_id;
     
      IF num_check <> 1
      THEN
         RAISE_APPLICATION_ERROR(
             -20001
            ,'face ' || p_face_id || ' does not exist in topology.'
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Collect the list of edge ids excluding the indicated one if it already
      -- exists
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || 'b.column_value '
              || 'FROM '
              || obj_topology.face_table() || ' a '
              || 'CROSS JOIN '
              || 'TABLE(a.island_edge_id_list) b '
              || 'WHERE '
              || '    a.face_id = :p01 '
              || 'AND ABS(b.column_value) <> :p02 ';
              
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO ary_edge_list
      USING p_face_id, ABS(p_edge_id);
      
      --------------------------------------------------------------------------
      -- Step 50
      -- Append the edge id
      --------------------------------------------------------------------------
      IF ary_edge_list IS NULL
      THEN
         ary_edge_list := MDSYS.SDO_LIST_TYPE();
         ary_edge_list.EXTEND();
         ary_edge_list(1) := p_edge_id;
         
      ELSE
         ary_edge_list.EXTEND();
         ary_edge_list(ary_edge_list.COUNT) := p_edge_id;
      
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 50
      -- Update the face table with the results
      --------------------------------------------------------------------------
      str_sql := 'UPDATE ' || obj_topology.face_table() || ' a '
              || 'SET '
              || 'a.island_edge_id_list = :p01 '
              || 'WHERE '
              || 'a.face_id = :p02 ';
              
      EXECUTE IMMEDIATE str_sql
      USING ary_edge_list,p_face_id;
      
      COMMIT;
         
   END append_face_island_edge_id;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE remove_face_island_node_id(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL  
      ,p_topology_name   IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj    IN  dz_topology DEFAULT NULL
      ,p_face_id         IN  NUMBER
      ,p_node_id         IN  NUMBER
   )
   AS
      obj_topology  dz_topology := p_topology_obj;
      ary_node_list MDSYS.SDO_LIST_TYPE;
      str_sql       VARCHAR2(4000 Char);
      num_check     PLS_INTEGER;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_face_id IS NULL
      OR p_node_id IS NULL
      THEN
         RETURN;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Determine topology location
      --------------------------------------------------------------------------
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topology_owner => p_topology_owner
            ,p_topology_name  => p_topology_name
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Verify that face exists
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || 'COUNT(*) '
              || 'FROM '
              || obj_topology.face_table() || ' a '
              || 'WHERE '
              || 'a.face_id = :p01 ';
              
      EXECUTE IMMEDIATE str_sql
      INTO num_check
      USING p_face_id;
     
      IF num_check <> 1
      THEN
         RAISE_APPLICATION_ERROR(
             -20001
            ,'face ' || p_face_id || ' does not exist in topology.'
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Collect the list of edge ids excluding the indicated one
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || 'b.column_value '
              || 'FROM '
              || obj_topology.face_table() || ' a '
              || 'CROSS JOIN '
              || 'TABLE(a.island_node_id_list) b '
              || 'WHERE '
              || '    a.face_id = :p01 '
              || 'AND b.column_value <> :p02 ';
              
     EXECUTE IMMEDIATE str_sql
     BULK COLLECT INTO ary_node_list
     USING p_face_id, p_node_id;
     
     --------------------------------------------------------------------------
      -- Step 50
      -- Update the face table with the results
      --------------------------------------------------------------------------
      str_sql := 'UPDATE ' || obj_topology.face_table() || ' a '
              || 'SET '
              || 'a.island_node_id_list = :p01 '
              || 'WHERE '
              || 'a.face_id = :p02 ';
              
      EXECUTE IMMEDIATE str_sql
      USING ary_node_list,p_face_id;
      
      COMMIT;
         
   END remove_face_island_node_id;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE append_face_island_node_id(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL  
      ,p_topology_name   IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj    IN  dz_topology DEFAULT NULL
      ,p_face_id         IN  NUMBER
      ,p_node_id         IN  NUMBER
   )
   AS
      obj_topology  dz_topology := p_topology_obj;
      ary_node_list MDSYS.SDO_LIST_TYPE;
      str_sql       VARCHAR2(4000 Char);
      num_check     PLS_INTEGER;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF p_face_id IS NULL
      OR p_node_id IS NULL
      THEN
         RETURN;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Determine topology location
      --------------------------------------------------------------------------
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topology_owner => p_topology_owner
            ,p_topology_name  => p_topology_name
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Verify that face exists
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || 'COUNT(*) '
              || 'FROM '
              || obj_topology.face_table() || ' a '
              || 'WHERE '
              || 'a.face_id = :p01 ';
              
      EXECUTE IMMEDIATE str_sql
      INTO num_check
      USING p_face_id;
     
      IF num_check <> 1
      THEN
         RAISE_APPLICATION_ERROR(
             -20001
            ,'face ' || p_face_id || ' does not exist in topology.'
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Collect the list of edge ids excluding the indicated one
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || 'b.column_value '
              || 'FROM '
              || obj_topology.face_table() || ' a '
              || 'CROSS JOIN '
              || 'TABLE(a.island_node_id_list) b '
              || 'WHERE '
              || '    a.face_id = :p01 '
              || 'AND b.column_value <> :p02 ';
              
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO ary_node_list
      USING p_face_id, p_node_id;
      
      --------------------------------------------------------------------------
      -- Step 50
      -- Append the new id
      --------------------------------------------------------------------------
      IF ary_node_list IS NULL
      THEN
         ary_node_list := MDSYS.SDO_LIST_TYPE();
         ary_node_list.EXTEND();
         ary_node_list(1) := p_node_id;
      
      ELSE
         ary_node_list.EXTEND();
         ary_node_list(ary_node_list.COUNT) := p_node_id;
      
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 50
      -- Update the face table with the results
      --------------------------------------------------------------------------
      str_sql := 'UPDATE ' || obj_topology.face_table() || ' a '
              || 'SET '
              || 'a.island_node_id_list = :p01 '
              || 'WHERE '
              || 'a.face_id = :p02 ';
              
      EXECUTE IMMEDIATE str_sql
      USING ary_node_list,p_face_id;
      
      COMMIT;
         
   END append_face_island_node_id;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE unravel_face(
       p_topology_owner  IN  VARCHAR2 DEFAULT NULL  
      ,p_topology_name   IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj    IN  dz_topology DEFAULT NULL
      ,p_tg_layer_id     IN  NUMBER DEFAULT 1
      ,p_face_id         IN  NUMBER
      ,p_tg_ids          OUT MDSYS.SDO_NUMBER_ARRAY
      ,p_face_ids        OUT MDSYS.SDO_NUMBER_ARRAY
      ,p_edge_ids        OUT MDSYS.SDO_NUMBER_ARRAY
      ,p_node_ids        OUT MDSYS.SDO_NUMBER_ARRAY
      ,p_window_sdo      OUT MDSYS.SDO_GEOMETRY
   )
   AS
      obj_topology    dz_topology := p_topology_obj;
      ary_edges_temp  MDSYS.SDO_NUMBER_ARRAY;
      ary_faces_temp  MDSYS.SDO_NUMBER_ARRAY;
      str_sql         VARCHAR2(4000 Char);
      num_check       PLS_INTEGER;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      p_tg_ids   := MDSYS.SDO_NUMBER_ARRAY();
      p_face_ids := MDSYS.SDO_NUMBER_ARRAY();
      p_edge_ids := MDSYS.SDO_NUMBER_ARRAY();
      p_node_ids := MDSYS.SDO_NUMBER_ARRAY();
      
      IF p_face_id IS NULL
      THEN
         RETURN;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Determine topology location
      --------------------------------------------------------------------------
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topology_owner => p_topology_owner
            ,p_topology_name  => p_topology_name
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            RAISE_APPLICATION_ERROR(-20001,'invalid topology');
            
         END IF;
         
      END IF;
      
      IF p_tg_layer_id IS NOT NULL
      THEN
         obj_topology.set_active_topo_layer(
            p_tg_layer_id => p_tg_layer_id
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Verify that face exists
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || 'COUNT(*) '
              || 'FROM '
              || obj_topology.face_table() || ' a '
              || 'WHERE '
              || 'a.face_id = :p01 ';
              
      EXECUTE IMMEDIATE str_sql
      INTO num_check
      USING p_face_id;
     
      IF num_check <> 1
      THEN
         RAISE_APPLICATION_ERROR(
             -20001
            ,'face ' || p_face_id || ' does not exist in topology.'
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Collect the list of edge ids and face_ids bordering face on the right
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || ' a.edge_id '
              || ',a.right_face_id '
              || 'FROM '
              || obj_topology.edge_table() || ' a '
              || 'WHERE '
              || '    a.left_face_id  =  :p01 '
              || 'AND a.right_face_id <> :p02 ';
              
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO 
       ary_edges_temp
      ,ary_faces_temp
      USING p_face_id, p_face_id;
     
      IF ary_edges_temp IS NOT NULL
      OR ary_edges_temp.COUNT > 0
      THEN
         dz_topo_util.append2(
             p_input_array => p_edge_ids
            ,p_input_value => ary_edges_temp
            ,p_unique      => 'TRUE'
         );
         
         dz_topo_util.append2(
             p_input_array => p_face_ids
            ,p_input_value => ary_faces_temp
            ,p_unique      => 'TRUE'
         );
         
      END IF;
     
      --------------------------------------------------------------------------
      -- Step 40
      -- Collect the list of edge ids and face_ids bordering face on the left
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || ' a.edge_id '
              || ',a.left_face_id '
              || 'FROM '
              || obj_topology.edge_table() || ' a '
              || 'WHERE '
              || '    a.left_face_id  <> :p01 '
              || 'AND a.right_face_id =  :p02 ';
              
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO 
       ary_edges_temp
      ,ary_faces_temp
      USING p_face_id, p_face_id;
     
      IF ary_edges_temp IS NOT NULL
      OR ary_edges_temp.COUNT > 0
      THEN
         dz_topo_util.append2(
             p_input_array => p_edge_ids
            ,p_input_value => ary_edges_temp
            ,p_unique      => 'TRUE'
         );
         
         dz_topo_util.append2(
             p_input_array => p_face_ids
            ,p_input_value => ary_faces_temp
            ,p_unique      => 'TRUE'
         );
         
      END IF;
     
      --------------------------------------------------------------------------
      -- Step 50
      -- Collect the list of edge ids and face_ids bordering face on the left
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || 'a.edge_id '
              || 'FROM '
              || obj_topology.edge_table() || ' a '
              || 'WHERE '
              || '    a.left_face_id  IN (SELECT * FROM TABLE(:p01)) '
              || 'AND a.right_face_id IN (SELECT * FROM TABLE(:p02)) ';
      
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO 
      ary_edges_temp
      USING p_face_ids, p_face_ids;
      
      IF ary_edges_temp IS NOT NULL
      OR ary_edges_temp.COUNT > 0
      THEN
         dz_topo_util.append2(
             p_input_array => p_edge_ids
            ,p_input_value => ary_edges_temp
            ,p_unique      => 'TRUE'
         );
         
      END IF;
       
      --------------------------------------------------------------------------
      -- Step 60
      -- Collect the list of tg ids that match any of the faces
      --------------------------------------------------------------------------
      IF p_tg_layer_id IS NOT NULL 
      THEN
         str_sql := 'SELECT '
                 || 'DISTINCT a.tg_id '
                 || 'FROM '
                 || obj_topology.relation_table() || ' a '
                 || 'WHERE '
                 || '    a.topo_type = 3 '
                 || 'AND a.tg_layer_id = :p01 '
                 || 'AND ( '
                 || '   a.topo_id IN (SELECT * FROM TABLE(:p02)) OR a.topo_id = :p03 '
                 || ') ';
         
         EXECUTE IMMEDIATE str_sql
         BULK COLLECT INTO 
         p_tg_ids
         USING 
          p_tg_layer_id
         ,p_face_ids
         ,p_face_id;
      
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 70
      -- Create the 
      --------------------------------------------------------------------------
      ary_faces_temp := p_face_ids;
      
      dz_topo_util.append2(
          p_input_array => ary_faces_temp
         ,p_input_value => p_face_id
         ,p_unique      => 'TRUE'
      );
      str_sql := 'SELECT '
              || 'SDO_AGGR_MBR(a.mbr_geometry) '
              || 'FROM '
              || obj_topology.face_table() || ' a '
              || 'WHERE '
              || 'a.face_id IN (SELECT * FROM TABLE(:p01)) ';
              
      EXECUTE IMMEDIATE str_sql
      INTO p_window_sdo
      USING ary_faces_temp;
      
  END unravel_face;

END dz_topo_main;
/


--*************************--
PROMPT DZ_TOPO_PROCESSING.pks;

CREATE OR REPLACE PACKAGE dz_topo_processing
AUTHID CURRENT_USER
AS

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION dz_validate(
       p_input    IN  MDSYS.SDO_TOPO_GEOMETRY
      ,p_filter   IN  VARCHAR2 DEFAULT 'ALL'
   ) RETURN VARCHAR2;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE clean_overlaps_by_allocation(
       p_topology_name   IN  VARCHAR2 DEFAULT NULL
      ,p_topo_layer_name IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj    IN  dz_topology DEFAULT NULL
   );

END dz_topo_processing;
/

GRANT EXECUTE ON dz_topo_processing TO public;


--*************************--
PROMPT DZ_TOPO_PROCESSING.pkb;

CREATE OR REPLACE PACKAGE BODY dz_topo_processing
AS

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION dz_validate(
       p_input    IN  MDSYS.SDO_TOPO_GEOMETRY
      ,p_filter   IN  VARCHAR2 DEFAULT 'ALL'
   ) RETURN VARCHAR2
   AS
   
   BEGIN
      
      NULL;
      
   END dz_validate;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE clean_overlaps_by_allocation(
       p_topology_name   IN  VARCHAR2 DEFAULT NULL
      ,p_topo_layer_name IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj    IN  dz_topology DEFAULT NULL
   )
   AS
      obj_topology       dz_topology := p_topology_obj;
      
   BEGIN
   
      NULL;
      
   END clean_overlaps_by_allocation;

END dz_topo_processing;
/


--*************************--
PROMPT DZ_TOPO_TEST.pks;

CREATE OR REPLACE PACKAGE dz_topo_test
AUTHID CURRENT_USER
AS

   C_TFS_CHANGESET CONSTANT NUMBER := 8290;
   C_JENKINS_JOBNM CONSTANT VARCHAR2(255 Char) := 'NULL';
   C_JENKINS_BUILD CONSTANT NUMBER := 5;
   C_JENKINS_BLDID CONSTANT VARCHAR2(255 Char) := 'NULL';

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


--*************************--
PROMPT DZ_TOPO_TEST.pkb;

CREATE OR REPLACE PACKAGE BODY dz_topo_test
AS

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION random_name(
       p_length NUMBER DEFAULT 30
   ) RETURN VARCHAR2
   AS
      str_seq    VARCHAR2(4000 Char);

   BEGIN

      str_seq := TO_CHAR(
         SYS_CONTEXT('USERENV', 'SESSIONID')
      ) || TO_CHAR(
         REPLACE(
             DBMS_RANDOM.RANDOM
            ,'-'
            ,''
         )
      );
      
      IF LENGTH(str_seq) > p_length - 3
      THEN
        str_seq := SUBSTR(str_seq,1,p_length - 3);
        
      END IF;

      RETURN 'DZ' || str_seq || 'X';
   
   END random_name;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION prerequisites
   RETURN NUMBER
   AS
      num_check NUMBER;
      
   BEGIN
      
      FOR i IN 1 .. C_PREREQUISITES.COUNT
      LOOP
         SELECT 
         COUNT(*)
         INTO num_check
         FROM 
         user_objects a
         WHERE 
             a.object_name = C_PREREQUISITES(i) || '_TEST'
         AND a.object_type = 'PACKAGE';
         
         IF num_check <> 1
         THEN
            RETURN 1;
         
         END IF;
      
      END LOOP;
      
      RETURN 0;
   
   END prerequisites;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION version
   RETURN VARCHAR2
   AS
   BEGIN
      RETURN '{"TFS":' || C_TFS_CHANGESET || ','
      || '"JOBN":"' || C_JENKINS_JOBNM || '",'   
      || '"BUILD":' || C_JENKINS_BUILD || ','
      || '"BUILDID":"' || C_JENKINS_BLDID || '"}';
      
   END version;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION inmemory_test
   RETURN NUMBER
   AS
   BEGIN
      RETURN 0;
      
   END inmemory_test;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION scratch_test
   RETURN NUMBER
   AS
      num_counter  NUMBER;
      num_results  NUMBER := 0;
      num_area     NUMBER;
      str_results  VARCHAR2(255 Char);
      str_topology VARCHAR2(20 Char);
      obj_topo     dz_topology;
      ary_ids      MDSYS.SDO_NUMBER_ARRAY;
      ary_shape    MDSYS.SDO_GEOMETRY_ARRAY;
      topo_shape   MDSYS.SDO_TOPO_GEOMETRY;
      ary_topos    dz_topo_vry;
      sdo_temp     MDSYS.SDO_GEOMETRY;
      
   BEGIN
      
      --------------------------------------------------------------------------
      -- Step 10
      -- Bail if there is no scratch schema or test data
      --------------------------------------------------------------------------
      SELECT
      COUNT(*)
      INTO num_counter
      FROM
      all_users a
      WHERE
      a.username IN (C_TEST_SCHEMA);
      
      IF num_counter <> 1
      THEN
         RETURN 0;
         
      END IF;
      
      SELECT
      COUNT(*)
      INTO num_counter
      FROM
      all_users a
      WHERE
      a.username IN (C_DZ_TESTDATA);
      
      IF num_counter <> 1
      THEN
         RETURN 0;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Topology test #1
      -- Create and populate topology
      --------------------------------------------------------------------------
      str_topology := random_name(15);
      
      MDSYS.SDO_TOPO.CREATE_TOPOLOGY(
          topology  => str_topology
         ,tolerance => 0.05
         ,srid      => 8265
      );
 
      EXECUTE IMMEDIATE q'[     
         INSERT INTO ]' || str_topology || q'[_face$(
             face_id 
            ,boundary_edge_id 
            ,island_edge_id_list 
            ,island_node_id_list 
            ,mbr_geometry 
         ) VALUES ( 
             -1 
            ,NULL 
            ,MDSYS.SDO_LIST_TYPE() 
            ,MDSYS.SDO_LIST_TYPE() 
            ,NULL 
         )
      ]';
      COMMIT;
      
      EXECUTE IMMEDIATE q'[
         CREATE TABLE ]' || str_topology || q'[_topo(
             featureid INTEGER
            ,topo_geom MDSYS.SDO_TOPO_GEOMETRY
         )
      ]';
      
      MDSYS.SDO_TOPO.ADD_TOPO_GEOMETRY_LAYER(
          topology      => str_topology
         ,table_name    => str_topology || '_TOPO'
         ,column_name   => 'TOPO_GEOM'
         ,topo_geometry_layer_type => 'POLYGON'
      );
      
      MDSYS.SDO_TOPO.INITIALIZE_METADATA(
         topology => str_topology
      );

      obj_topo := dz_topology(
          p_topology_name    => str_topology 
         ,p_active_table     => str_topology || '_TOPO'
         ,p_active_column    => 'TOPO_GEOM'
      );    
                   
      obj_topo.set_topo_map_mgr(
         dz_topo_map_mgr(
             p_topology_name => str_topology
         )
      );     
      
      obj_topo.java_memory(p_gigs => 2);
         
      EXECUTE IMMEDIATE q'[
         SELECT 
          a.featureid
         ,a.shape
         FROM
         ]' || C_TEST_SCHEMA || q'[.nhdplus21_catchment_55059 a
         WHERE 
         a.featureid IN (
             14784009
            ,14784007
            ,14784003
            ,14783997
            ,14783991
            ,14783993
            ,14783977
            ,14783975
            ,14784191
            ,14783971
            ,14783965
            ,14784193
            ,14783981
            ,14783973
            ,14784001
            ,14783995
            ,14783979
            ,14783967
            ,14783999
            ,14783985
         )
         ORDER BY
         a.featureid
      ]'
      BULK COLLECT INTO ary_ids,ary_shape;
                
      FOR i IN 1 .. ary_ids.COUNT
      LOOP
         topo_shape := obj_topo.create_feature(
            ary_shape(i)
         );
           
         EXECUTE IMMEDIATE q'[             
            INSERT INTO ]' || str_topology || q'[_topo(
                featureid
               ,topo_geom
            ) VALUES (
                :p01
               ,:p02
            )
         ]' 
         USING
          ary_ids(i)
         ,topo_shape;
                        
      END LOOP;
                  
      obj_topo.commit_topo_map();
      COMMIT;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Topology test #2
      -- Aggregate all topo geoms into one polygon output
      --------------------------------------------------------------------------
      EXECUTE IMMEDIATE q'[
         SELECT 
          a.topo_geom
         FROM
         ]' || str_topology || q'[_topo a
      ]'
      BULK COLLECT INTO ary_topos;
      
      sdo_temp := dz_topo_main.aggregate_topology(
          p_topology_obj   => obj_topo
         ,p_input_topo     => ary_topos
      );
      
      num_area := MDSYS.SDO_GEOM.SDO_AREA(
          sdo_temp
         ,0.05
        ,'UNIT=SQ_KM'
      );
      
      IF ROUND(num_area,2) <>  45.08
      THEN
         dbms_output.put_line('Bad area results ' || num_area);
         num_results := num_results - 1;
         
      END IF;
      
      str_results := MDSYS.SDO_GEOM.VALIDATE_GEOMETRY_WITH_CONTEXT(
          sdo_temp
         ,0.05
      );
      
      IF str_results <> 'TRUE'
      THEN
         dbms_output.put_line('Bad geom validity ' || str_results);
         num_results := num_results - 1;
      
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Drop the test topology
      --------------------------------------------------------------------------
      dz_topo_main.nuke_topology(
          p_topology_name => str_topology 
      );
      
      RETURN num_results;
      
   END scratch_test;

END dz_topo_test;
/


--*************************--
PROMPT DZ_TOPO_RECIPES.pks;

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


--*************************--
PROMPT DZ_TOPO_RECIPES.pkb;

CREATE OR REPLACE PACKAGE BODY dz_topo_recipes
AS

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE align_edges(
       p_table_name            IN  VARCHAR2
      ,p_column_name           IN  VARCHAR2
      ,p_work_table_name       IN  VARCHAR2 DEFAULT 'GERINGER_TMP'
   )
   AS
      /*
      obj_spdx waters_code.ow_spatial_index;
      str_sql  VARCHAR2(4000);
      num_tolerance NUMBER := 0.0000005;
         
      TYPE cursor_type is REF CURSOR;
      query_crs cursor_type;
         
      a_rowid        ROWID;
      b_rowid        ROWID;
      update_window  BOOLEAN;
      update_next    BOOLEAN;
      window_geom    SDO_GEOMETRY;
      next_geom      SDO_GEOMETRY;
      */
      
   BEGIN
      
      /*
      --------------------------------------------------------------------------
      -- Step 10
      -- Determine the SRID from the table and check things are kosher
      --------------------------------------------------------------------------
      obj_spdx := waters_code.ow_spatial_index(
          p_table_name => p_table_name
         ,p_column_name => p_column_name
      );
         
      --------------------------------------------------------------------------
      -- Step 20
      -- Update SRID to NULL
      --------------------------------------------------------------------------
      IF obj_spdx.get_srid() IS NOT NULL
      THEN
         obj_spdx.change_table_srid(NULL);
      
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Rebuild the index regardless
      --------------------------------------------------------------------------
      obj_spdx.redefine_index(NULL,'GEODETIC');
         
      --------------------------------------------------------------------------
      -- Step 30
      -- Create Geringer work table
      --------------------------------------------------------------------------
      str_sql := 'CREATE TABLE ' || p_work_table_name || '( '
              || '    sdo_rowid ROWID '
              || '    ,' || p_column_name || ' SDO_GEOMETRY '
              || ')';
                 
      BEGIN
         EXECUTE IMMEDIATE str_sql;
            
      EXCEPTION
         WHEN OTHERS 
         THEN
            IF SQLCODE = -00955
            THEN
               EXECUTE IMMEDIATE 'DROP TABLE ' || p_work_table_name;
               EXECUTE IMMEDIATE str_sql;
                  
            ELSE
               RAISE;
            END IF;
               
      END;
         
      --------------------------------------------------------------------------
      -- Step 40
      -- Define the cursor string
      --------------------------------------------------------------------------
      str_sql := 'SELECT /' || '*+ ordered use_nl (a b) use_nl (a c) *' || '/ '
              || ' a.rowid1 AS a_rowid '
              || ',a.rowid2 AS b_rowid ' 
              || 'FROM '
              || 'TABLE( '
              || '   SDO_JOIN( '
              || '       ''' || p_table_name || ''',''' || p_column_name || ''' '
              || '      ,''' || p_table_name || ''',''' || p_column_name || ''' '
              || '   ) '
              || ') a, ' 
              || p_table_name || ' b,' 
              || p_table_name || ' c ' 
              || 'WHERE '
              || '    a.rowid1 = b.rowid '
              || 'AND a.rowid2 = c.rowid '
              || 'AND a.rowid1 < a.rowid2 '
              || 'AND SDO_GEOM.RELATE( '
              || '    b.' || p_column_name 
              || '   ,''ANYINTERACT'' '
              || '   ,c.' || p_column_name 
              || '   ,' || num_tolerance
              || ') = ''TRUE''';
              
      --------------------------------------------------------------------------
      -- Step 40
      -- Fetch the results
      --------------------------------------------------------------------------
      OPEN query_crs FOR str_sql;

      LOOP
         FETCH query_crs INTO a_rowid, b_rowid;
         EXIT WHEN query_crs%NOTFOUND;
            
         BEGIN
            EXECUTE IMMEDIATE 
               'SELECT '
            || 'a.' || p_column_name || ' ' 
            || 'FROM ' 
            || p_work_table_name || ' a '
            || 'WHERE '
            || 'a.sdo_rowid = ''' || a_rowid || ''' '
            INTO window_geom;

            update_window := TRUE;
               
         EXCEPTION
            WHEN NO_DATA_FOUND 
            THEN
               EXECUTE IMMEDIATE 
                  'SELECT ' 
               || 'a.' || p_column_name || ' ' 
               || 'FROM '
               || p_table_name  || ' a '
               || 'WHERE '
               || 'a.rowid = ''' || a_rowid || ''' '
               INTO window_geom;
               
            update_window := FALSE;
            
         END;

         BEGIN
            EXECUTE IMMEDIATE 
               'SELECT ' 
            || 'a.' || p_column_name || ' '
            || 'FROM ' 
            || p_work_table_name || ' a '
            || 'WHERE '
            || 'a.sdo_rowid = ''' || b_rowid || ''' '
            INTO next_geom;
               
            update_next := TRUE;
            
         EXCEPTION
            WHEN NO_DATA_FOUND 
            THEN
               
               EXECUTE IMMEDIATE 
                  'SELECT ' 
               || 'a.' || p_column_name || ' '
               || 'FROM '
               || p_table_name  || ' a ' 
               || 'WHERE '
               || 'a.rowid = ''' || b_rowid || ''' '
               INTO next_geom;
                  
               update_next := FALSE;
                  
         END;

         window_geom := SDO_GEOM.SDO_DIFFERENCE(
             window_geom
            ,next_geom
            ,num_tolerance
         );
            
         next_geom   := SDO_GEOM.SDO_DIFFERENCE(
             next_geom
            ,window_geom
            ,num_tolerance
         );
            
         IF update_next = TRUE
         THEN
            EXECUTE IMMEDIATE 
               'UPDATE ' || p_work_table_name || ' a '
            || 'SET a.' || p_column_name || ' = :p01 '
            || 'WHERE '
            || 'a.sdo_rowid = :p02'
            USING next_geom,b_rowid;
               
         ELSE
            EXECUTE IMMEDIATE 
               'INSERT INTO ' || p_work_table_name || '( '
            || '    sdo_rowid '
            || '   ,' || p_column_name || ' '
            || ') VALUES ( '
            || '     :p01 '
            || '    ,:p02 '
            || ') '
            USING b_rowid,next_geom;
               
         END IF;

         IF update_window = TRUE
         THEN
            EXECUTE IMMEDIATE 
               'UPDATE ' || p_work_table_name || ' a '
            || 'SET a.' || p_column_name || ' = :p01 '
            || 'WHERE '
            || 'a.sdo_rowid = :p02 '
            USING window_geom,a_rowid;

         ELSE
            EXECUTE IMMEDIATE 
               'INSERT INTO ' || p_work_table_name || '( '
            || '    sdo_rowid '
            || '   ,' || p_column_name || ' '
            || ') VALUES ( '
            || '    :p01 '
            || '   ,:p02 '
            || ') '
            USING a_rowid,window_geom;
               
         END IF;

         COMMIT;
            
      END LOOP;

      CLOSE query_crs;
      
      */
      
      NULL;
         
   END align_edges;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE generic_loader
   AS
      /*
      obj_topo           waters_code.dz_topology;
      ary_featureids     MDSYS.SDO_NUMBER_ARRAY;
      ary_sourcefc       MDSYS.SDO_STRING2_ARRAY;
      ary_nhdplus_region MDSYS.SDO_STRING2_ARRAY;
      ary_areasqkm       MDSYS.SDO_NUMBER_ARRAY;
      ary_shape          MDSYS.SDO_GEOMETRY_ARRAY;
      topo_shape         MDSYS.SDO_TOPO_GEOMETRY;
      num_gulp           NUMBER := 900;
      */
   BEGIN
      
      /*
      SELECT 
       a.featureid
      ,a.sourcefc
      ,a.nhdplus_region
      ,a.areasqkm
      ,a.shape
      BULK COLLECT INTO 
       ary_featureids
      ,ary_sourcefc
      ,ary_nhdplus_region
      ,ary_areasqkm
      ,ary_shape
      FROM 
      dziemiela.catrough_np21_sdo a 
      WHERE 
          a.nhdplus_region = '&&1'
      AND substr(reachcode,1,8) = '&&2'
      AND a.featureid NOT IN (
         SELECT
         b.featureid
         FROM
         dziemiela.catrough_np21_topo b
      ) and rownum <= num_gulp
      ORDER BY reachcode DESC;
      
      IF ary_featureids.COUNT = 0
      THEN
         raise_application_error(-20001,'query found no records (' || ary_featureids.COUNT || ')');
      END IF;
      
      obj_topo := waters_code.dz_topology(
          p_topology_name    => 'CATROUGH_NP21'
         ,p_active_table     => 'CATROUGH_NP21_TOPO'
         ,p_active_column    => 'TOPO_GEOM'
      );
         
      obj_topo.set_topo_map_mgr(
         waters_code.dz_topo_map_mgr(
             p_window_sdo      => ary_shape
            ,p_window_padding  => 0.000001
            ,p_topology_name   => 'CATROUGH_NP21'
            ,p_number_of_edges => num_gulp * 8
            ,p_number_of_nodes => num_gulp * 10
            ,p_number_of_faces => num_gulp * 3
         )
      );
      
      obj_topo.java_memory(p_gigs => 13);
      
      FOR i IN 1 .. ary_featureids.COUNT
      LOOP
         topo_shape := obj_topo.create_feature(
            ary_shape(i)
         );
            
         INSERT INTO dziemiela.catrough_np21_topo(
             featureid
            ,sourcefc
            ,nhdplus_region
            ,areasqkm
            ,topo_geom
         ) VALUES (
             ary_featureids(i)
            ,ary_sourcefc(i)
            ,ary_nhdplus_region(i)
            ,ary_areasqkm(i)
            ,topo_shape
         );
            
      END LOOP;
      
      obj_topo.commit_topo_map();
      COMMIT;
      
      */
      
      NULL;
      
   END generic_loader;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE scorched_earth_around_face(
       p_face_id         IN  NUMBER
   )
   AS
      str_sql            VARCHAR2(4000 Char);
      str_topology_owner VARCHAR2(30 Char) := 'NHDPLUS';
      str_topology_name  VARCHAR2(30 Char) := 'CATCHMENT_SM_NP21';
      num_tg_layer_id    NUMBER := 1;
      obj_topology       dz_topology;
      ary_topos          dz_topo_vry;
      
   BEGIN
   
      obj_topology := dz_topology(
          p_topology_owner => str_topology_owner
         ,p_topology_name  => str_topology_name
      );
         
      IF obj_topology.valid() <> 'TRUE'
      THEN
         RAISE_APPLICATION_ERROR(-20001,'invalid topology');
         
      END IF;
      
      obj_topology.set_active_topo_layer(
         p_tg_layer_id => num_tg_layer_id
      );
      
      ary_topos := dz_topo_main.get_face_topo_neighbors(
          p_topology_obj    => obj_topology
         ,p_tg_layer_id     => num_tg_layer_id
         ,p_face_id         => p_face_id
      );
      
      str_sql  := 'DELETE FROM '
               || obj_topology.active_topo_table() || ' a '
               || 'WHERE '
               || 'a.' || obj_topology.active_topo_column() || '.tg_id IN ( '
               || '  SELECT tg_id FROM TABLE(:p01) '
               || ') ';
      
      EXECUTE IMMEDIATE str_sql
      USING ary_topos;
             
      COMMIT;
      
      dz_topo_main.drop_face_clean(
          p_topology_obj    => obj_topology
         ,p_face_id         => p_face_id
      );
   
   END scorched_earth_around_face;

END dz_topo_recipes;
/


--*************************--
PROMPT DZ_TOPO_VALIDATE.pks;

CREATE OR REPLACE PACKAGE dz_topo_validate
AUTHID CURRENT_USER
AS

   TYPE validation_results IS TABLE OF VARCHAR2(4000 Char);
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   /*
   Function: dz_topo_validate.check_all

   Utility function to validate components of a given topology. 

   Parameters:

      p_topology_owner - owner of the topology
      p_topology_name - name of the topology
      
   Returns:

      Pipelined table of VARCHAR2(4000) informational messages
      
   Notes:
   
   - Note for a large topology this function may require a long time to complete.
   
   */
   FUNCTION check_all(
       p_topology_owner IN  VARCHAR2
      ,p_topology_name  IN  VARCHAR2
   ) RETURN validation_results PIPELINED;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   /*
   Function: dz_topo_validate.check_basic

   Utility function to validate a subset of a given topology. At this time the
   function is the same as check_all.

   Parameters:

      p_topology_owner - owner of the topology
      p_topology_name - name of the topology
      
   Returns:

      Pipelined table of VARCHAR2(4000) informational messages
      
   Notes:
   
   - Note for a large topology this function may require a long time to complete.
   
   */
   FUNCTION check_basic(
       p_topology_owner IN  VARCHAR2
      ,p_topology_name  IN  VARCHAR2
   ) RETURN validation_results PIPELINED;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   /*
   Function: dz_topo_validate.check_universal_face

   Utility function to inspect and report on topology elements which interact
   with the universal face.

   Parameters:

      p_topology_owner - owner of the topology
      p_topology_name - name of the topology
      
   Returns:

      Pipelined table of VARCHAR2(4000) informational messages
      
   Notes:
   
   - This utility is primarily concerned with reporting on island edges which
     are not contained within the universal face.
   
   */
   FUNCTION check_universal_face(
       p_topology_owner IN  VARCHAR2
      ,p_topology_name  IN  VARCHAR2
   ) RETURN validation_results PIPELINED;
   
END dz_topo_validate;
/

GRANT EXECUTE ON dz_topo_validate TO public;


--*************************--
PROMPT DZ_TOPO_VALIDATE.pkb;

CREATE OR REPLACE PACKAGE BODY dz_topo_validate
AS

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION concatenate_results(
       p_input_a         IN validation_results
      ,p_input_b         IN validation_results
   ) RETURN validation_results
   AS
      ary_output  validation_results := validation_results();
      int_counter PLS_INTEGER;
      
   BEGIN
   
      IF p_input_a IS NULL
      OR p_input_a.COUNT = 0
      THEN
         RETURN p_input_b;
         
      ELSIF p_input_b IS NULL
      OR p_input_b.COUNT = 0
      THEN
         RETURN p_input_a;
         
      END IF;
      
      ary_output := p_input_a;
      int_counter := p_input_a.COUNT + 1;
      ary_output.EXTEND(p_input_b.COUNT);
      
      FOR i IN 1 .. p_input_b.COUNT
      LOOP
         ary_output(int_counter) := p_input_b(i);
         int_counter := int_counter + 1;
         
      END LOOP;
      
      RETURN ary_output;
   
   END concatenate_results;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION check_indexes(
      p_topology_obj   IN  dz_topology
   ) RETURN validation_results
   AS
      ary_output  validation_results := validation_results();
      int_counter PLS_INTEGER := 1;
      
   BEGIN
      
      IF dz_topo_util.index_exists(
          p_owner       => p_topology_obj.topology_owner
         ,p_table_name  => p_topology_obj.edge_table_name()
         ,p_column_name => 'NEXT_LEFT_EDGE_ID'
      ) <> 'TRUE'
      THEN
         ary_output.EXTEND();
         ary_output(int_counter) := 'Edge table needs index on next_left_edge_id to validate';
         int_counter := int_counter + 1;
            
      END IF;
         
      IF dz_topo_util.index_exists(
          p_owner       => p_topology_obj.topology_owner
         ,p_table_name  => p_topology_obj.edge_table_name()
         ,p_column_name => 'NEXT_RIGHT_EDGE_ID'
      ) <> 'TRUE'
      THEN
         ary_output.EXTEND();
         ary_output(int_counter) := 'Edge table needs index on next_right_edge_id to validate';
         int_counter := int_counter + 1;
            
      END IF;
         
      IF dz_topo_util.index_exists(
          p_owner       => p_topology_obj.topology_owner
         ,p_table_name  => p_topology_obj.edge_table_name()
         ,p_column_name => 'PREV_LEFT_EDGE_ID'
      ) <> 'TRUE'
      THEN
         ary_output.EXTEND();
         ary_output(int_counter) := 'Edge table needs index on prev_left_edge_id to validate';
         int_counter := int_counter + 1;
            
      END IF;
         
      IF dz_topo_util.index_exists(
          p_owner       => p_topology_obj.topology_owner
         ,p_table_name  => p_topology_obj.edge_table_name()
         ,p_column_name => 'PREV_RIGHT_EDGE_ID'
      ) <> 'TRUE'
      THEN
         ary_output.EXTEND();
         ary_output(int_counter) := 'Edge table needs index on prev_right_edge_id to validate';
         int_counter := int_counter + 1;
            
      END IF;
      
      RETURN ary_output;
         
   END check_indexes;

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION z_check_basic(
       p_topology_obj   IN  dz_topology DEFAULT NULL
   ) RETURN validation_results
   AS
      ary_temp     validation_results;
      ary_output   validation_results;
      obj_topology dz_topology := p_topology_obj;
      str_sql      VARCHAR2(4000 Char);
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check for edges with bad left_face_ids
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || '''edge '' || a.edge_id || '' has bad left_face_id ('' || a.left_face_id || '')'''
              || 'FROM '
              ||  obj_topology.edge_table() || ' a '
              || 'WHERE '
              || 'a.left_face_id NOT IN ( '
              || '   SELECT '
              || '   b.face_id '
              || '   FROM '
              || '   ' || obj_topology.face_table() || ' b '
              || ')';
              
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO ary_temp;
      
      IF ary_temp IS NOT NULL
      AND ary_temp.COUNT > 0
      THEN
         ary_output := concatenate_results(
             p_input_a => ary_output
            ,p_input_b => ary_temp
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Check for edges with bad right_face_ids
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || '''edge '' || a.edge_id || '' has bad right_face_id ('' || a.right_face_id || '')'''
              || 'FROM '
              ||  obj_topology.edge_table() || ' a '
              || 'WHERE '
              || 'a.right_face_id NOT IN ( '
              || '   SELECT '
              || '   b.face_id '
              || '   FROM '
              || '   ' || obj_topology.face_table() || ' b '
              || ')';
              
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO ary_temp;
      
      IF ary_temp IS NOT NULL
      AND ary_temp.COUNT > 0
      THEN
         ary_output := concatenate_results(
             p_input_a => ary_output
            ,p_input_b => ary_temp
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Check for edges with bad next_left_edge_id
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || '''edge '' || a.edge_id || '' has bad next_left_edge_id ('' || a.next_left_edge_id || '')'''
              || 'FROM '
              ||  obj_topology.edge_table() || ' a '
              || 'WHERE '
              || 'ABS(a.next_left_edge_id) NOT IN ( '
              || '   SELECT '
              || '   b.edge_id '
              || '   FROM '
              || '   ' || obj_topology.edge_table() || ' b '
              || ')';
              
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO ary_temp;
      
      IF ary_temp IS NOT NULL
      AND ary_temp.COUNT > 0
      THEN
         ary_output := concatenate_results(
             p_input_a => ary_output
            ,p_input_b => ary_temp
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Check for edges with bad next_right_edge_id
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || '''edge '' || a.edge_id || '' has bad next_right_edge_id ('' || a.next_right_edge_id || '')'''
              || 'FROM '
              ||  obj_topology.edge_table() || ' a '
              || 'WHERE '
              || 'ABS(a.next_right_edge_id) NOT IN ( '
              || '   SELECT '
              || '   b.edge_id '
              || '   FROM '
              || '   ' || obj_topology.edge_table() || ' b '
              || ')';
              
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO ary_temp;
      
      IF ary_temp IS NOT NULL
      AND ary_temp.COUNT > 0
      THEN
         ary_output := concatenate_results(
             p_input_a => ary_output
            ,p_input_b => ary_temp
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 50
      -- Check for edges with bad prev_left_edge_id
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || '''edge '' || a.edge_id || '' has bad prev_left_edge_id ('' || a.prev_left_edge_id || '')'''
              || 'FROM '
              ||  obj_topology.edge_table() || ' a '
              || 'WHERE '
              || 'ABS(a.prev_left_edge_id) NOT IN ( '
              || '   SELECT '
              || '   b.edge_id '
              || '   FROM '
              || '   ' || obj_topology.edge_table() || ' b '
              || ')';
              
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO ary_temp;
      
      IF ary_temp IS NOT NULL
      AND ary_temp.COUNT > 0
      THEN
         ary_output := concatenate_results(
             p_input_a => ary_output
            ,p_input_b => ary_temp
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 60
      -- Check for edges with bad prev_right_edge_id
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || '''edge '' || a.edge_id || '' has bad prev_right_edge_id ('' || a.prev_right_edge_id || '')'''
              || 'FROM '
              ||  obj_topology.edge_table() || ' a '
              || 'WHERE '
              || 'ABS(a.prev_right_edge_id) NOT IN ( '
              || '   SELECT '
              || '   b.edge_id '
              || '   FROM '
              || '   ' || obj_topology.edge_table() || ' b '
              || ')';
              
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO ary_temp;
      
      IF ary_temp IS NOT NULL
      AND ary_temp.COUNT > 0
      THEN
         ary_output := concatenate_results(
             p_input_a => ary_output
            ,p_input_b => ary_temp
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 70
      -- Check for edges with bad start_node_id
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || '''edge '' || a.edge_id || '' has bad start_node_id ('' || a.start_node_id || '')'''
              || 'FROM '
              ||  obj_topology.edge_table() || ' a '
              || 'WHERE '
              || 'a.start_node_id NOT IN ( '
              || '   SELECT '
              || '   b.node_id '
              || '   FROM '
              || '   ' || obj_topology.node_table() || ' b '
              || ')';
              
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO ary_temp;
      
      IF ary_temp IS NOT NULL
      AND ary_temp.COUNT > 0
      THEN
         ary_output := concatenate_results(
             p_input_a => ary_output
            ,p_input_b => ary_temp
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 80
      -- Check for edges with bad end_node_id
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || '''edge '' || a.edge_id || '' has bad end_node_id ('' || a.end_node_id || '')'''
              || 'FROM '
              ||  obj_topology.edge_table() || ' a '
              || 'WHERE '
              || 'a.end_node_id NOT IN ( '
              || '   SELECT '
              || '   b.node_id '
              || '   FROM '
              || '   ' || obj_topology.node_table() || ' b '
              || ')';
              
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO ary_temp;
      
      IF ary_temp IS NOT NULL
      AND ary_temp.COUNT > 0
      THEN
         ary_output := concatenate_results(
             p_input_a => ary_output
            ,p_input_b => ary_temp
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 90
      -- Check for nodes with bad edge_id
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || '''node '' || a.node_id || '' has bad edge_id ('' || a.edge_id || '')'''
              || 'FROM '
              ||  obj_topology.node_table() || ' a '
              || 'WHERE '
              || 'ABS(a.edge_id) NOT IN ( '
              || '   SELECT '
              || '   b.edge_id '
              || '   FROM '
              || '   ' || obj_topology.edge_table() || ' b '
              || ')';
              
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO ary_temp;
      
      IF ary_temp IS NOT NULL
      AND ary_temp.COUNT > 0
      THEN
         ary_output := concatenate_results(
             p_input_a => ary_output
            ,p_input_b => ary_temp
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 100
      -- Check for nodes with bad face_id
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || '''node '' || a.node_id || '' has bad face_id ('' || a.face_id || '')'''
              || 'FROM '
              ||  obj_topology.node_table() || ' a '
              || 'WHERE '
              || 'a.face_id <> 0 '
              || 'AND a.face_id NOT IN ( '
              || '   SELECT '
              || '   b.face_id '
              || '   FROM '
              || '   ' || obj_topology.face_table() || ' b '
              || ')';
             
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO ary_temp;
      
      IF ary_temp IS NOT NULL
      AND ary_temp.COUNT > 0
      THEN
         ary_output := concatenate_results(
             p_input_a => ary_output
            ,p_input_b => ary_temp
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 110
      -- Check for faces with bad boundary_edge_id
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || '''face '' || a.face_id || '' has bad boundary_edge_id ('' || a.boundary_edge_id || '')'''
              || 'FROM '
              ||  obj_topology.face_table() || ' a '
              || 'WHERE '
              || 'ABS(a.boundary_edge_id) NOT IN ( '
              || '   SELECT '
              || '   b.edge_id '
              || '   FROM '
              || '   ' || obj_topology.edge_table() || ' b '
              || ')';
              
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO ary_temp;
      
      IF ary_temp IS NOT NULL
      AND ary_temp.COUNT > 0
      THEN
         ary_output := concatenate_results(
             p_input_a => ary_output
            ,p_input_b => ary_temp
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 120
      -- Check for faces with bad island_edge_id
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || '''face '' || a.face_id || '' has bad island_edge_id ('' || a.island_edge_id || '')'''
              || 'FROM ( '
              || '   SELECT '
              || '    aa.face_id '
              || '   ,bb.column_value as island_edge_id '
              || '   FROM '
              || '   ' || obj_topology.face_table() || ' aa, '
              || '   TABLE(aa.island_edge_id_list) bb '
              || ') a '
              || 'WHERE ' 
              || 'ABS(a.island_edge_id) NOT IN ( '
              || '   SELECT '
              || '   b.edge_id '
              || '   FROM '
              || '   ' || obj_topology.edge_table() || ' b '
              || ')';
              
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO ary_temp;
      
      IF ary_temp IS NOT NULL
      AND ary_temp.COUNT > 0
      THEN
         ary_output := concatenate_results(
             p_input_a => ary_output
            ,p_input_b => ary_temp
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 130
      -- Check for faces with bad island_edge_id
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || '''face '' || a.face_id || '' has bad island_node_id ('' || a.island_node_id || '')''' 
              || 'FROM ( '
              || '   SELECT '
              || '    aa.face_id '
              || '   ,bb.column_value as island_node_id '
              || '   FROM '
              || '   ' || obj_topology.face_table() || ' aa, '
              || '   TABLE(aa.island_node_id_list) bb '
              || ') a '
              || 'WHERE ' 
              || 'ABS(a.island_node_id) NOT IN ( '
              || '   SELECT '
              || '   b.node_id '
              || '   FROM '
              || '   ' || obj_topology.node_table() || ' b '
              || ')';
              
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO ary_temp;
      
      IF ary_temp IS NOT NULL
      AND ary_temp.COUNT > 0
      THEN
         ary_output := concatenate_results(
             p_input_a => ary_output
            ,p_input_b => ary_temp
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 140
      -- Check for relations with bad polygon faces
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || '''relation tg_id '' || a.tg_id || '' has bad topo face_id ('' || a.topo_id || '')'''
              || 'FROM '
              ||  obj_topology.relation_table() || ' a '
              || 'WHERE '
              || '    a.topo_type = 3 '
              || 'AND a.topo_id NOT IN ( '
              || '   SELECT '
              || '   b.face_id '
              || '   FROM '
              || '   ' || obj_topology.face_table() || ' b '
              || ')';
              
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO ary_temp;
      
      IF ary_temp IS NOT NULL
      AND ary_temp.COUNT > 0
      THEN
         ary_output := concatenate_results(
             p_input_a => ary_output
            ,p_input_b => ary_temp
         );
         
      END IF;
      
      IF ary_output IS NULL
      OR ary_output.COUNT = 0
      THEN
         ary_output := validation_results();
         ary_output.EXTEND();
         ary_output(1) := 'Basic test set all okay.';
         
      END IF;
      
      RETURN ary_output;
   
   END z_check_basic;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION check_all(
       p_topology_owner IN  VARCHAR2
      ,p_topology_name  IN  VARCHAR2
   ) RETURN validation_results PIPELINED
   AS
      ary_output   validation_results;
      obj_topology dz_topology;
      
   BEGIN
      
      --------------------------------------------------------------------------
      -- Step 10
      -- Check that topology exists and reports valid
      --------------------------------------------------------------------------
      obj_topology := dz_topology(
          p_topology_owner => p_topology_owner
         ,p_topology_name  => p_topology_name
      );
         
      IF obj_topology.valid() <> 'TRUE'
      THEN
         PIPE ROW('Topology reporting as invalid');
         RETURN;
            
      END IF;

      --------------------------------------------------------------------------
      -- Step 20
      -- Check that edge table has supplemental indexes on edge links
      --------------------------------------------------------------------------
      ary_output := check_indexes(
         p_topology_obj => obj_topology
      );

      IF ary_output.COUNT > 0
      THEN
         FOR i IN 1 .. ary_output.COUNT
         LOOP
            PIPE ROW(ary_output(i));
               
         END LOOP;
            
         RETURN;
            
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Do basic tests
      --------------------------------------------------------------------------
      ary_output := z_check_basic(
          p_topology_obj => obj_topology
      );
      
      IF ary_output IS NOT NULL
      AND ary_output.COUNT > 0
      THEN
         FOR i IN 1 .. ary_output.COUNT
         LOOP
            PIPE ROW(ary_output(i));
            
         END LOOP;
         
      END IF;
   
   END check_all;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION check_basic(
       p_topology_owner IN  VARCHAR2
      ,p_topology_name  IN  VARCHAR2
   ) RETURN validation_results PIPELINED
   AS
      obj_topology dz_topology;
      ary_output   validation_results;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check that topology exists and reports valid
      --------------------------------------------------------------------------
      obj_topology := dz_topology(
          p_topology_owner => p_topology_owner
         ,p_topology_name  => p_topology_name
      );
         
      IF obj_topology.valid() <> 'TRUE'
      THEN
         PIPE ROW('Topology reporting as invalid');
         RETURN;
            
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Check that edge table has supplemental indexes on edge links
      --------------------------------------------------------------------------
      ary_output := check_indexes(
         p_topology_obj => obj_topology
      );

      IF ary_output.COUNT > 0
      THEN
         FOR i IN 1 .. ary_output.COUNT
         LOOP
            PIPE ROW(ary_output(i));
               
         END LOOP;
            
         RETURN;
            
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Execute the basic checks
      --------------------------------------------------------------------------
      ary_output := z_check_basic(
          p_topology_obj => obj_topology
      );
      
      IF ary_output IS NOT NULL
      AND ary_output.COUNT > 0
      THEN
         FOR i IN 1 .. ary_output.COUNT
         LOOP
            PIPE ROW(ary_output(i));
            
         END LOOP;
         
      END IF;
      
   END check_basic;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION z_check_universal_face(
       p_topology_obj   IN  dz_topology
   ) RETURN validation_results
   AS
      ary_temp         validation_results;
      ary_output       validation_results;
      int_counter      PLS_INTEGER := 1;
      obj_topology     dz_topology := p_topology_obj;
      str_sql          VARCHAR2(4000 Char);
      ary_island_edges MDSYS.SDO_LIST_TYPE;
      
   BEGIN
   
      ary_output := validation_results();
       
      --------------------------------------------------------------------------
      -- Step 10
      -- Slurp up the universal face island edges
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || 'a.island_edge_id_list '
              || 'FROM '
              ||  obj_topology.face_table() || ' a '
              || 'WHERE '
              || 'a.face_id = -1 ';
              
      EXECUTE IMMEDIATE str_sql
      INTO ary_island_edges;
      
      IF ary_island_edges IS NULL
      OR ary_island_edges.COUNT = 0
      THEN
         ary_output.EXTEND();
         ary_output(int_counter) := 'Univeral face has no island edges.';
         int_counter := int_counter + 1;
         
      ELSE
         ary_output.EXTEND();
         ary_output(int_counter) := 'Univeral face has ' || ary_island_edges.COUNT || ' island edges.';
         int_counter := int_counter + 1;
        
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Look for any island edges that are not on the universal face
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || '''Universal island edge '' || a.edge_id || '' is not on the universal face '''
              || 'FROM '
              ||  obj_topology.edge_table() || ' a '
              || 'WHERE '
              ||     'a.edge_id IN (SELECT * FROM TABLE(:p01)) '
              || 'AND ( '
              || '   a.left_face_id = -1 OR a.right_face_id = -1 '
              || ') ';
      
      EXECUTE IMMEDIATE str_sql
      BULK COLLECT INTO ary_temp
      USING ary_island_edges;
      
      IF ary_temp IS NOT NULL
      AND ary_temp.COUNT > 0
      THEN
         ary_output := concatenate_results(
             p_input_a => ary_output
            ,p_input_b => ary_temp
         );
         
      END IF; 
      
      
      RETURN ary_output;
      
   END z_check_universal_face;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION check_universal_face(
       p_topology_owner IN  VARCHAR2
      ,p_topology_name  IN  VARCHAR2
   ) RETURN validation_results PIPELINED
   AS
      obj_topology dz_topology;
      ary_output   validation_results;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check that topology exists and reports valid
      --------------------------------------------------------------------------
      obj_topology := dz_topology(
          p_topology_owner => p_topology_owner
         ,p_topology_name  => p_topology_name
      );
         
      IF obj_topology.valid() <> 'TRUE'
      THEN
         PIPE ROW('Topology reporting as invalid');
         RETURN;
            
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Check that edge table has supplemental indexes on edge links
      --------------------------------------------------------------------------
      ary_output := check_indexes(
         p_topology_obj => obj_topology
      );

      IF ary_output.COUNT > 0
      THEN
         FOR i IN 1 .. ary_output.COUNT
         LOOP
            PIPE ROW(ary_output(i));
               
         END LOOP;
            
         RETURN;
            
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Execute the basic checks
      --------------------------------------------------------------------------
      ary_output := z_check_universal_face(
          p_topology_obj => obj_topology
      );
      
      IF ary_output IS NOT NULL
      AND ary_output.COUNT > 0
      THEN
         FOR i IN 1 .. ary_output.COUNT
         LOOP
            PIPE ROW(ary_output(i));
            
         END LOOP;
         
      END IF;
      
   END check_universal_face;
   
END dz_topo_validate;
/


--*************************--
PROMPT DZ_TOPO_REPORTS.pks;

CREATE OR REPLACE PACKAGE dz_topo_reports
AUTHID CURRENT_USER
AS

   TYPE report_results IS TABLE OF VARCHAR2(4000 Char);
   
   TYPE edge_rec IS RECORD(
       edge_id             NUMBER
      ,start_node_id       NUMBER
      ,end_node_id         NUMBER
      ,next_left_edge_id   NUMBER
      ,prev_left_edge_id   NUMBER
      ,next_right_edge_id  NUMBER
      ,prev_right_edge_id  NUMBER
      ,left_face_id        NUMBER
      ,right_face_id       NUMBER
      ,geometry            MDSYS.SDO_GEOMETRY
      ,left_face_is_outer  VARCHAR2(5 Char)
      ,right_face_is_outer VARCHAR2(5 Char)
   );
   
   TYPE edge_tbl IS TABLE OF edge_rec;
   
   TYPE node_rec IS RECORD(
       node_id            NUMBER
      ,edge_id            NUMBER
      ,face_id            NUMBER
      ,geometry           MDSYS.SDO_GEOMETRY
   );
   
   TYPE node_tbl IS TABLE OF node_rec;
   
   TYPE face_rec IS RECORD(
       face_id             NUMBER
      ,boundary_edge_id    NUMBER
      ,island_edge_id_list MDSYS.SDO_LIST_TYPE
      ,island_node_id_list MDSYS.SDO_LIST_TYPE
      ,mbr_geometry        MDSYS.SDO_GEOMETRY
   );
   
   TYPE face_tbl IS TABLE OF face_rec;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   /*
   Function: dz_topo_reports.edge_report

   Reporting function to dump pertinent information about a given edge and how
   the edge relates to all its neighbors including notes about any corruption
   found amongst the edge neighbors.

   Parameters:

      p_topology_owner - optional owner of the topology
      p_topology_name - optional name of the topology
      p_topology_obj - optional object dz_topology
      p_edge_id - the id of the edge to inspect
      
   Returns:

      Pipelined table of VARCHAR2(4000 Char)
      
   Notes:
   
   - Dz_topo functions and procedures either utilize an existing dz_topology 
     object or require the name and owner of the topology to internally initialize
     one.  Thus you either must provide name and owner or an initialized 
     dz_topology object to the function.  The latter format is most useful when
     calling several topology procedures in sequence.
   
   - The edge report was created for debugging purposes.

   */
   FUNCTION edge_report(
       p_topology_owner IN  VARCHAR2 DEFAULT NULL
      ,p_topology_name  IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
      ,p_edge_id        IN  NUMBER
   ) RETURN report_results PIPELINED;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   /*
   Function: dz_topo_reports.face_report

   Reporting function to dump pertinent information about a given face and how
   the face relates to all its neighbors.   

   Parameters:

      p_topology_owner - optional owner of the topology
      p_topology_name - optional name of the topology
      p_topology_obj - optional object dz_topology
      p_face_id - the id of the face to inspect
      
   Returns:

      Pipelined table of VARCHAR2(4000 Char)
      
   Notes:
   
   - Dz_topo functions and procedures either utilize an existing dz_topology 
     object or require the name and owner of the topology to internally initialize
     one.  Thus you either must provide name and owner or an initialized 
     dz_topology object to the function.  The latter format is most useful when
     calling several topology procedures in sequence.
   
   - The face report was created for debugging purposes.

   */
   FUNCTION face_report(
       p_topology_owner IN  VARCHAR2 DEFAULT NULL
      ,p_topology_name  IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
      ,p_face_id        IN  NUMBER
   ) RETURN report_results PIPELINED;
   
END dz_topo_reports;
/

GRANT EXECUTE ON dz_topo_reports TO public;


--*************************--
PROMPT DZ_TOPO_REPORTS.pkb;

CREATE OR REPLACE PACKAGE BODY dz_topo_reports
AS

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION in_list(
       p_list_ary       IN  MDSYS.SDO_LIST_TYPE
      ,p_input          IN  NUMBER
   ) RETURN BOOLEAN
   AS
   BEGIN
   
      IF p_list_ary IS NULL
      OR p_list_ary.COUNT = 0
      THEN
         RETURN FALSE;
         
      END IF;
      
      IF p_input IS NULL
      THEN
         RETURN NULL;
         
      END IF;
      
      FOR i IN 1 .. p_list_ary.COUNT
      LOOP
         IF ABS(p_list_ary(i)) = p_input
         THEN
            RETURN TRUE;
            
         END IF;
         
      END LOOP;
      
      RETURN FALSE;
   
   END in_list;

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION edge_report(
       p_topology_owner IN  VARCHAR2 DEFAULT NULL
      ,p_topology_name  IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
      ,p_edge_id        IN  NUMBER
   ) RETURN report_results PIPELINED
   AS
      obj_topology        dz_topology := p_topology_obj;
      str_sql             VARCHAR2(4000 Char);
      obj_report_edge     edge_rec;
      obj_prev_left_edge  edge_rec;
      obj_prev_right_edge edge_rec;
      obj_next_left_edge  edge_rec;
      obj_next_right_edge edge_rec;
      num_errors          PLS_INTEGER := 0;
      obj_left_face       face_rec;
      obj_right_face      face_rec;
      obj_start_node      node_rec;
      obj_end_node        node_rec;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check that topology exists and reports valid
      --------------------------------------------------------------------------
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topology_owner => p_topology_owner
            ,p_topology_name  => p_topology_name
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            PIPE ROW('Topology reporting as invalid');
            RETURN;
            
         END IF;
         
      END IF;

      --------------------------------------------------------------------------
      -- Step 20
      -- Grab the report edge
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || ' a.edge_id '
              || ',a.start_node_id '
              || ',a.end_node_id '
              || ',a.next_left_edge_id '
              || ',a.prev_left_edge_id '
              || ',a.next_right_edge_id '
              || ',a.prev_right_edge_id '
              || ',a.left_face_id '
              || ',a.right_face_id '
              || ',a.geometry '
              || ',NULL '
              || ',NULL '
              || 'FROM '
              || obj_topology.edge_table() || ' a '
              || 'WHERE '
              || 'a.edge_id = :p01 ';
              
      EXECUTE IMMEDIATE str_sql
      INTO obj_report_edge
      USING p_edge_id;
      
      IF obj_report_edge.edge_id IS NULL
      THEN
         PIPE ROW('Edge ' || p_edge_id || ' not found in topology.');
         RETURN;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Grab the previous left edge
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || ' a.edge_id '
              || ',a.start_node_id '
              || ',a.end_node_id '
              || ',a.next_left_edge_id '
              || ',a.prev_left_edge_id '
              || ',a.next_right_edge_id '
              || ',a.prev_right_edge_id '
              || ',a.left_face_id '
              || ',a.right_face_id '
              || ',a.geometry '
              || ',NULL '
              || ',NULL '
              || 'FROM '
              || obj_topology.edge_table() || ' a '
              || 'WHERE '
              || 'a.edge_id = :p01 ';
              
      EXECUTE IMMEDIATE str_sql
      INTO obj_prev_left_edge
      USING ABS(obj_report_edge.prev_left_edge_id);
      
      IF obj_prev_left_edge.edge_id IS NULL
      THEN
         PIPE ROW('Edge ' || p_edge_id || ' previous left edge ' || obj_report_edge.prev_left_edge_id || ' not found in topology.');
         num_errors := num_errors + 1;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Grab the previous right edge
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || ' a.edge_id '
              || ',a.start_node_id '
              || ',a.end_node_id '
              || ',a.next_left_edge_id '
              || ',a.prev_left_edge_id '
              || ',a.next_right_edge_id '
              || ',a.prev_right_edge_id '
              || ',a.left_face_id '
              || ',a.right_face_id '
              || ',a.geometry '
              || ',NULL '
              || ',NULL '
              || 'FROM '
              || obj_topology.edge_table() || ' a '
              || 'WHERE '
              || 'a.edge_id = :p01 ';
              
      EXECUTE IMMEDIATE str_sql
      INTO obj_prev_right_edge
      USING ABS(obj_report_edge.prev_right_edge_id);
      
      IF obj_prev_right_edge.edge_id IS NULL
      THEN
         PIPE ROW('Edge ' || p_edge_id || ' previous right edge ' || obj_report_edge.prev_right_edge_id || ' not found in topology.');
         num_errors := num_errors + 1;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 50
      -- Grab the next left edge
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || ' a.edge_id '
              || ',a.start_node_id '
              || ',a.end_node_id '
              || ',a.next_left_edge_id '
              || ',a.prev_left_edge_id '
              || ',a.next_right_edge_id '
              || ',a.prev_right_edge_id '
              || ',a.left_face_id '
              || ',a.right_face_id '
              || ',a.geometry '
              || ',NULL '
              || ',NULL '
              || 'FROM '
              || obj_topology.edge_table() || ' a '
              || 'WHERE '
              || 'a.edge_id = :p01 ';
              
      EXECUTE IMMEDIATE str_sql
      INTO obj_next_left_edge
      USING ABS(obj_report_edge.next_left_edge_id);
      
      IF obj_next_left_edge.edge_id IS NULL
      THEN
         PIPE ROW('Edge ' || p_edge_id || ' next left edge ' || obj_report_edge.next_left_edge_id || ' not found in topology.');
         num_errors := num_errors + 1;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 60
      -- Grab the next right edge
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || ' a.edge_id '
              || ',a.start_node_id '
              || ',a.end_node_id '
              || ',a.next_left_edge_id '
              || ',a.prev_left_edge_id '
              || ',a.next_right_edge_id '
              || ',a.prev_right_edge_id '
              || ',a.left_face_id '
              || ',a.right_face_id '
              || ',a.geometry '
              || ',NULL '
              || ',NULL '
              || 'FROM '
              || obj_topology.edge_table() || ' a '
              || 'WHERE '
              || 'a.edge_id = :p01 ';
              
      EXECUTE IMMEDIATE str_sql
      INTO obj_next_right_edge
      USING ABS(obj_report_edge.next_right_edge_id);
      
      IF obj_next_right_edge.edge_id IS NULL
      THEN
         PIPE ROW('Edge ' || p_edge_id || ' next right edge ' || obj_report_edge.next_right_edge_id || ' not found in topology.');
         num_errors := num_errors + 1;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 70
      -- Grab the left face
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || ' a.face_id '
              || ',a.boundary_edge_id '
              || ',a.island_edge_id_list '
              || ',a.island_node_id_list '
              || ',a.mbr_geometry '
              || 'FROM '
              || obj_topology.face_table() || ' a '
              || 'WHERE '
              || 'a.face_id = :p01 ';
              
      EXECUTE IMMEDIATE str_sql
      INTO obj_left_face
      USING obj_report_edge.left_face_id;
      
      IF obj_left_face.face_id IS NULL
      THEN
         PIPE ROW('Edge ' || p_edge_id || ' left face ' || obj_report_edge.left_face_id || ' not found in topology.');
         num_errors := num_errors + 1;
         
      ELSE
         IF  obj_left_face.island_edge_id_list IS NOT NULL
         AND obj_left_face.island_edge_id_list.COUNT > 0
         THEN
            IF in_list(
                p_list_ary => obj_left_face.island_edge_id_list
               ,p_input    => obj_report_edge.edge_id
            )
            THEN
               obj_report_edge.left_face_is_outer := 'TRUE';
               
            ELSE
               obj_report_edge.left_face_is_outer := 'FALSE';
               
            END IF;
            
         ELSE
            obj_report_edge.left_face_is_outer := 'FALSE';
            
         END IF;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 80
      -- Grab the right face
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || ' a.face_id '
              || ',a.boundary_edge_id '
              || ',a.island_edge_id_list '
              || ',a.island_node_id_list '
              || ',a.mbr_geometry '
              || 'FROM '
              || obj_topology.face_table() || ' a '
              || 'WHERE '
              || 'a.face_id = :p01 ';
              
      EXECUTE IMMEDIATE str_sql
      INTO obj_right_face
      USING obj_report_edge.right_face_id;
      
      IF obj_right_face.face_id IS NULL
      THEN
         PIPE ROW('Edge ' || p_edge_id || ' right face ' || obj_report_edge.right_face_id || ' not found in topology.');
         num_errors := num_errors + 1;
         
      ELSE
         IF  obj_right_face.island_edge_id_list IS NOT NULL
         AND obj_right_face.island_edge_id_list.COUNT > 0
         THEN
            IF in_list(
                p_list_ary => obj_right_face.island_edge_id_list
               ,p_input    => obj_report_edge.edge_id
            )
            THEN
               obj_report_edge.right_face_is_outer := 'TRUE';
               
            ELSE
               obj_report_edge.right_face_is_outer := 'FALSE';
               
            END IF;
            
         ELSE
            obj_report_edge.right_face_is_outer := 'FALSE';
            
         END IF;
         
      END IF; 
      
      --------------------------------------------------------------------------
      -- Step 90
      -- Grab the start node
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || ' a.node_id '
              || ',a.edge_id '
              || ',a.face_id '
              || ',a.geometry '
              || 'FROM '
              || obj_topology.node_table() || ' a '
              || 'WHERE '
              || 'a.node_id = :p01 ';
              
      EXECUTE IMMEDIATE str_sql
      INTO obj_start_node
      USING obj_report_edge.start_node_id;
      
      IF obj_start_node.node_id IS NULL
      THEN
         PIPE ROW('Edge ' || p_edge_id || ' start node ' || obj_report_edge.start_node_id || ' not found in topology.');
         num_errors := num_errors + 1;
         
      END IF; 
      
      --------------------------------------------------------------------------
      -- Step 100
      -- Grab the end node
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || ' a.node_id '
              || ',a.edge_id '
              || ',a.face_id '
              || ',a.geometry '
              || 'FROM '
              || obj_topology.node_table() || ' a '
              || 'WHERE '
              || 'a.node_id = :p01 ';
              
      EXECUTE IMMEDIATE str_sql
      INTO obj_end_node
      USING obj_report_edge.end_node_id;
      
      IF obj_end_node.node_id IS NULL
      THEN
         PIPE ROW('Edge ' || p_edge_id || ' end node ' || obj_report_edge.end_node_id || ' not found in topology.');
         num_errors := num_errors + 1;
         
      END IF; 
      
      --------------------------------------------------------------------------
      -- Step 110
      -- Exit if any errors so far
      --------------------------------------------------------------------------
      IF num_errors > 0
      THEN
         RETURN;
         
      END IF;
      
      num_errors := 0;
      
      PIPE ROW('Edge: ' || obj_report_edge.edge_id);
      PIPE ROW('----------');
      PIPE ROW('   Previous Left Edge: ' || obj_report_edge.prev_left_edge_id);
      PIPE ROW('   Next Left Edge: ' || obj_report_edge.next_left_edge_id);
      PIPE ROW('   Previous Right Edge: ' || obj_report_edge.prev_right_edge_id);
      PIPE ROW('   Next Right Edge: ' || obj_report_edge.next_right_edge_id);
      PIPE ROW('----------');
      PIPE ROW('   Start Node: ' || obj_report_edge.start_node_id);
      PIPE ROW('   End Node: ' || obj_report_edge.end_node_id);
      PIPE ROW('----------');
      IF obj_report_edge.left_face_is_outer = 'TRUE'
      THEN
         PIPE ROW('   Left Face: ' || obj_report_edge.left_face_id || ' OUTER');
      ELSE
         PIPE ROW('   Left Face: ' || obj_report_edge.left_face_id);
      
      END IF;
      IF obj_report_edge.right_face_is_outer = 'TRUE'
      THEN
         PIPE ROW('   Right Face: ' || obj_report_edge.right_face_id || ' OUTER');
         
      ELSE
         PIPE ROW('   Right Face: ' || obj_report_edge.right_face_id);
         
      END IF;
      PIPE ROW('----------');
      
      --------------------------------------------------------------------------
      -- Step 120
      -- Check whether prev left nodes are correct
      --------------------------------------------------------------------------
      IF obj_report_edge.prev_left_edge_id > 0
      THEN
         IF obj_report_edge.start_node_id <> obj_prev_left_edge.end_node_id
         THEN
            PIPE ROW('** Previous left edge end node does not match start node.');
            num_errors := num_errors + 1;
            
         END IF;
         
      ELSE
         IF obj_report_edge.start_node_id <> obj_prev_left_edge.start_node_id
         THEN
            PIPE ROW('** Previous left edge start node does not match start node.');
            num_errors := num_errors + 1;
            
         END IF;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 130
      -- Check whether next left nodes are correct
      --------------------------------------------------------------------------
      IF obj_report_edge.next_left_edge_id > 0
      THEN
         IF obj_report_edge.end_node_id <> obj_next_left_edge.start_node_id
         THEN
            PIPE ROW('** Next left edge start node does not match end node.');
            num_errors := num_errors + 1;
            
         END IF;
         
      ELSE
         IF obj_report_edge.end_node_id <> obj_next_left_edge.end_node_id
         THEN
            PIPE ROW('** Next left edge end node does not match end node.');
            num_errors := num_errors + 1;
            
         END IF;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 140
      -- Check whether prev right nodes are correct
      --------------------------------------------------------------------------
      IF obj_report_edge.prev_right_edge_id > 0
      THEN
         IF obj_report_edge.end_node_id <> obj_prev_right_edge.end_node_id
         THEN
            PIPE ROW('** Prev right edge end node does not match end node.');
            num_errors := num_errors + 1;
            
         END IF;
         
      ELSE
         IF obj_report_edge.end_node_id <> obj_prev_right_edge.start_node_id
         THEN
            PIPE ROW('** Prev right edge start node does not match end node.');
            num_errors := num_errors + 1;
            
         END IF;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 150
      -- Check whether next right nodes are correct
      --------------------------------------------------------------------------
      IF obj_report_edge.next_right_edge_id > 0
      THEN
         IF obj_report_edge.start_node_id <> obj_next_right_edge.start_node_id
         THEN
            PIPE ROW('** Next right edge start node does not match start node.');
            num_errors := num_errors + 1;
            
         END IF;
         
      ELSE
         IF obj_report_edge.start_node_id <> obj_next_right_edge.end_node_id
         THEN
            PIPE ROW('** Next right edge end node does not match start node.');
            num_errors := num_errors + 1;
            
         END IF;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 160
      -- Exit if any errors so far
      --------------------------------------------------------------------------
      IF num_errors > 0
      THEN
         RETURN;
         
      END IF;
      
      num_errors := 0;
      PIPE ROW('** All nodes connections seems correct.');
      
   END edge_report;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION face_report(
       p_topology_owner IN  VARCHAR2 DEFAULT NULL
      ,p_topology_name  IN  VARCHAR2 DEFAULT NULL
      ,p_topology_obj   IN  dz_topology DEFAULT NULL
      ,p_face_id        IN  NUMBER
   ) RETURN report_results PIPELINED
   AS
      obj_topology        dz_topology := p_topology_obj;
      str_sql             VARCHAR2(4000 Char);
      obj_report_face     face_rec;
      num_errors          PLS_INTEGER := 0;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check that topology exists and reports valid
      --------------------------------------------------------------------------
      IF obj_topology IS NULL
      OR obj_topology.valid() <> 'TRUE'
      THEN
         obj_topology := dz_topology(
             p_topology_owner => p_topology_owner
            ,p_topology_name  => p_topology_name
         );
         
         IF obj_topology.valid() <> 'TRUE'
         THEN
            PIPE ROW('Topology reporting as invalid');
            RETURN;
            
         END IF;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Grab the report face
      --------------------------------------------------------------------------
      str_sql := 'SELECT '
              || ' a.face_id '
              || ',a.boundary_edge_id '
              || ',a.island_edge_id_list '
              || ',a.island_node_id_list '
              || ',a.mbr_geometry '
              || 'FROM '
              || obj_topology.face_table() || ' a '
              || 'WHERE '
              || 'a.face_id = :p01 ';
              
      EXECUTE IMMEDIATE str_sql
      INTO obj_report_face
      USING p_face_id;
      
      IF obj_report_face.face_id IS NULL
      THEN
         PIPE ROW('Face ' || p_face_id || ' not found in topology.');
         RETURN;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 100
      -- Exit if any errors so far
      --------------------------------------------------------------------------
      IF num_errors > 0
      THEN
         RETURN;
         
      END IF;
      
      num_errors := 0;
      
      PIPE ROW('Face: ' || obj_report_face.face_id);
      PIPE ROW('----------');
      PIPE ROW('   Boundary Edge: ' || obj_report_face.boundary_edge_id);
      PIPE ROW('   Island Edge Count: ' || obj_report_face.island_edge_id_list.COUNT);
      PIPE ROW('   Island Node Count: ' || obj_report_face.island_node_id_list.COUNT);
      PIPE ROW('----------');
      
   
   END face_report;
   
END dz_topo_reports;
/


--*************************--
PROMPT sqlplus_footer.sql;


SHOW ERROR;

DECLARE
   l_num_errors PLS_INTEGER;

BEGIN

   SELECT
   COUNT(*)
   INTO l_num_errors
   FROM
   user_errors a
   WHERE
   a.name LIKE 'DZ_TOPO%';

   IF l_num_errors <> 0
   THEN
      RAISE_APPLICATION_ERROR(-20001,'COMPILE ERROR');

   END IF;

   l_num_errors := DZ_TOPO_TEST.inmemory_test();

   IF l_num_errors <> 0
   THEN
      RAISE_APPLICATION_ERROR(-20001,'INMEMORY TEST ERROR');

   END IF;

END;
/

EXIT;

