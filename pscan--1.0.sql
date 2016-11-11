/* pscan/pscan--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pscan" to load this file. \quit

-- Tuple Scan method with 'int' workers
CREATE FUNCTION p_tuplescan(regclass, int)
RETURNS text
AS 'MODULE_PATHNAME', 'p_tuplescan'
LANGUAGE C STRICT;

-- Tuple Scan method with 'pscan.nworkers' workers
CREATE FUNCTION p_tuplescan(rel regclass)
RETURNS text
AS $$
   SELECT p_tuplescan(rel, -1);
$$ LANGUAGE SQL;

-- BRange Scan method with 'int' workers
CREATE FUNCTION p_brangescan(regclass, int)
RETURNS text
AS 'MODULE_PATHNAME', 'p_brangescan'
LANGUAGE C STRICT;

-- Brange Scan method with 'pscan.nworkers' workers
CREATE FUNCTION p_brangescan(rel regclass)
RETURNS text
AS $$
   SELECT p_brangescan(rel, -1);
$$ LANGUAGE SQL;
