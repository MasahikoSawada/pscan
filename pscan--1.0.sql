/* pscan/pscan--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pscan" to load this file. \quit

-- Show visibility map information.
CREATE FUNCTION p_tuplescan(regclass)
RETURNS text
AS 'MODULE_PATHNAME', 'p_tuplescan'
LANGUAGE C STRICT;

-- Show visibility map and page-level visibility information.
CREATE FUNCTION p_brangescan(regclass)
RETURNS text
AS 'MODULE_PATHNAME', 'p_brangescan'
LANGUAGE C STRICT;

