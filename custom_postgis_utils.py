# -*- coding: utf-8 -*-

"""
***************************************************************************
    postgis_utils.py
    ---------------------
    Date                 : November 2012
    Copyright            : (C) 2012 by Martin Dobias
    Email                : volayaf at gmail dot com
***************************************************************************
*                                                                         *
*   This program is free software; you can redistribute it and/or modify  *
*   it under the terms of the GNU General Public License as published by  *
*   the Free Software Foundation; either version 2 of the License, or     *
*   (at your option) any later version.                                   *
*                                                                         *
***************************************************************************
"""

__author__ = 'Martin Dobias'
__date__ = 'November 2012'
__copyright__ = '(C) 2012, Martin Dobias'

# This will get replaced with a git SHA1 when you do a git archive

__revision__ = '$Format:%H$'

import psycopg2
import psycopg2.extensions  # For isolation levels
import re

# Use unicode!
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)


class TableAttribute:

    def __init__(self, row):
        (self.num,
         self.name,
         self.data_type,
         self.char_max_len,
         self.modifier,
         self.notnull,
         self.hasdefault,
         self.default,
         ) = row


class TableConstraint:
    """Class that represents a constraint of a table (relation).
    """

    (TypeCheck, TypeForeignKey, TypePrimaryKey, TypeUnique) = range(4)
    types = {
        'c': TypeCheck,
        'f': TypeForeignKey,
        'p': TypePrimaryKey,
        'u': TypeUnique,
    }

    on_action = {
        'a': 'NO ACTION',
        'r': 'RESTRICT',
        'c': 'CASCADE',
        'n': 'SET NULL',
        'd': 'SET DEFAULT',
    }
    match_types = {'u': 'UNSPECIFIED', 'f': 'FULL', 'p': 'PARTIAL'}

    def __init__(self, row):
        (self.name, con_type, self.is_defferable, self.is_deffered, keys) = row[:5]
        self.keys = map(int, keys.split(' '))
        self.con_type = TableConstraint.types[con_type]  # Convert to enum
        if self.con_type == TableConstraint.TypeCheck:
            self.check_src = row[5]
        elif self.con_type == TableConstraint.TypeForeignKey:
            self.foreign_table = row[6]
            self.foreign_on_update = TableConstraint.on_action[row[7]]
            self.foreign_on_delete = TableConstraint.on_action[row[8]]
            self.foreign_match_type = TableConstraint.match_types[row[9]]
            self.foreign_keys = row[10]


class TableIndex:

    def __init__(self, row):
        (self.name, columns) = row
        self.columns = map(int, columns.split(' '))


class DbError(Exception):

    def __init__(self, message, query=None):
        # Save error. funny that the variables are in utf-8
        self.message = unicode(message, 'utf-8')
        self.query = (unicode(query, 'utf-8') if query is not None else None)

    def __str__(self):
        return 'MESSAGE: %s\nQUERY: %s' % (self.message, self.query)


class TableField:

    def __init__(self, name, data_type, is_null=None, default=None,
                modifier=None):
        (self.name, self.data_type, self.is_null, self.default,
         self.modifier) = (name, data_type, is_null, default, modifier)

    def is_null_txt(self):
        if self.is_null:
            return 'NULL'
        else:
            return 'NOT NULL'

    def field_def(self):
        """Return field definition as used for CREATE TABLE or
        ALTER TABLE command.
        """

        data_type = (self.data_type if not self.modifier or self.modifier
                     < 0 else '%s(%d)' % (self.data_type, self.modifier))
        txt = '%s %s %s' % (self._quote(self.name), data_type,
                            self.is_null_txt())
        if self.default and len(self.default) > 0:
            txt += ' DEFAULT %s' % self.default
        return txt

    def _quote(self, ident):
        if re.match(r"^\w+$", ident) is not None:
            return ident
        else:
            return '"%s"' % ident.replace('"', '""')


class GeoDB:

    def __init__(self, host=None, port=None, dbname=None, user=None,
                 passwd=None):
        # Regular expression for identifiers without need to quote them
        self.re_ident_ok = re.compile(r"^\w+$")

        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.passwd = passwd

        if self.dbname == '' or self.dbname is None:
            self.dbname = self.user

        try:
            self.con = psycopg2.connect(self.con_info())
        except psycopg2.OperationalError, e:
            raise DbError(e.message)

        self.has_postgis = self.check_postgis()

    def con_info(self):
        con_str = ''
        if self.host:
            con_str += "host='%s' " % self.host
        if self.port:
            con_str += 'port=%d ' % self.port
        if self.dbname:
            con_str += "dbname='%s' " % self.dbname
        if self.user:
            con_str += "user='%s' " % self.user
        if self.passwd:
            con_str += "password='%s' " % self.passwd
        return con_str

    def get_info(self):
        c = self.con.cursor()
        self._exec_sql(c, 'SELECT version()')
        return c.fetchone()[0]

    def check_postgis(self):
        """Check whether postgis_version is present in catalog.
        """

        c = self.con.cursor()
        self._exec_sql(c,
            "SELECT COUNT(*) FROM pg_proc WHERE proname = 'postgis_version'")
        return c.fetchone()[0] > 0

    def get_postgis_info(self):
        """Returns tuple about postgis support:
              - lib version
              - installed scripts version
              - released scripts version
              - geos version
              - proj version
              - whether uses stats
        """

        c = self.con.cursor()
        self._exec_sql(c,
            'SELECT postgis_lib_version(), postgis_scripts_installed(), \
            postgis_scripts_released(), postgis_geos_version(), \
            postgis_proj_version(), postgis_uses_stats()')
        return c.fetchone()

    def list_schemas(self):
        """Get list of schemas in tuples: (oid, name, owner, perms).
        """

        c = self.con.cursor()
        sql = "SELECT oid, nspname, pg_get_userbyid(nspowner), nspacl \
               FROM pg_namespace \
               WHERE nspname !~ '^pg_' AND nspname != 'information_schema'"
        self._exec_sql(c, sql)
        return c.fetchall()

    def list_geotables(self, schema=None):
        """Get list of tables with schemas, whether user has privileges,
        whether table has geometry column(s) etc.

        Geometry_columns:
          - f_table_schema
          - f_table_name
          - f_geometry_column
          - coord_dimension
          - srid
          - type
        """

        c = self.con.cursor()

        if schema:
            schema_where = " AND nspname = '%s' " % self._quote_str(schema)
        else:
            schema_where = \
                " AND (nspname != 'information_schema' AND nspname !~ 'pg_') "

        # LEFT OUTER JOIN: like LEFT JOIN but if there are more matches,
        # for join, all are used (not only one)

        # First find out whether postgis is enabled
        if not self.has_postgis:
            # Get all tables and views
            sql = """SELECT pg_class.relname, pg_namespace.nspname,
                            pg_class.relkind, pg_get_userbyid(relowner),
                            reltuples, relpages, NULL, NULL, NULL, NULL
                  FROM pg_class
                  JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
                  WHERE pg_class.relkind IN ('v', 'r')""" \
                  + schema_where + 'ORDER BY nspname, relname'
        else:
            # Discovery of all tables and whether they contain a
            # geometry column
            sql = """SELECT pg_class.relname, pg_namespace.nspname,
                            pg_class.relkind, pg_get_userbyid(relowner),
                            reltuples, relpages, pg_attribute.attname,
                            pg_attribute.atttypid::regtype, NULL, NULL
                  FROM pg_class
                  JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
                  LEFT OUTER JOIN pg_attribute ON
                      pg_attribute.attrelid = pg_class.oid AND
                      (pg_attribute.atttypid = 'geometry'::regtype
                      OR pg_attribute.atttypid IN
                          (SELECT oid FROM pg_type
                           WHERE typbasetype='geometry'::regtype))
                  WHERE pg_class.relkind IN ('v', 'r') """ \
                  + schema_where + 'ORDER BY nspname, relname, attname'

        self._exec_sql(c, sql)
        items = c.fetchall()

        # Get geometry info from geometry_columns if exists
        if self.has_postgis:
            sql = """SELECT relname, nspname, relkind,
                            pg_get_userbyid(relowner), reltuples, relpages,
                            geometry_columns.f_geometry_column,
                            geometry_columns.type,
                            geometry_columns.coord_dimension,
                            geometry_columns.srid
                  FROM pg_class
                  JOIN pg_namespace ON relnamespace=pg_namespace.oid
                  LEFT OUTER JOIN geometry_columns ON
                      relname=f_table_name AND nspname=f_table_schema
                  WHERE (relkind = 'r' or relkind='v') """ \
                  + schema_where + 'ORDER BY nspname, relname, \
                  f_geometry_column'
            self._exec_sql(c, sql)

            # Merge geometry info to "items"
            for (i, geo_item) in enumerate(c.fetchall()):
                if geo_item[7]:
                    items[i] = geo_item

        return items

    def get_table_rows(self, table, schema=None):
        c = self.con.cursor()
        self._exec_sql(c, 'SELECT COUNT(*) FROM %s' % self._table_name(schema,
                       table))
        return c.fetchone()[0]

    def get_table_fields(self, table, schema=None):
        """Return list of columns in table"""

        c = self.con.cursor()
        schema_where = (" AND nspname='%s' "
                        % self._quote_str(schema) if schema is not None else ''
                        )
        sql = """SELECT a.attnum AS ordinal_position,
                        a.attname AS column_name,
                        t.typname AS data_type,
                        a.attlen AS char_max_len,
                        a.atttypmod AS modifier,
                        a.attnotnull AS notnull,
                        a.atthasdef AS hasdefault,
                        adef.adsrc AS default_value
              FROM pg_class c
              JOIN pg_attribute a ON a.attrelid = c.oid
              JOIN pg_type t ON a.atttypid = t.oid
              JOIN pg_namespace nsp ON c.relnamespace = nsp.oid
              LEFT JOIN pg_attrdef adef ON adef.adrelid = a.attrelid
                  AND adef.adnum = a.attnum
              WHERE
                  c.relname = '%s' %s AND
                  a.attnum > 0
              ORDER BY a.attnum""" \
              % (self._quote_str(table), schema_where)

        self._exec_sql(c, sql)
        attrs = []
        for row in c.fetchall():
            attrs.append(TableAttribute(row))
        return attrs

    def get_table_indexes(self, table, schema=None):
        """Get info about table's indexes. ignore primary key and unique
        index, they get listed in constaints.
        """

        c = self.con.cursor()

        schema_where = (" AND nspname='%s' "
                        % self._quote_str(schema) if schema is not None else ''
                        )
        sql = """SELECT relname, indkey
              FROM pg_class, pg_index
              WHERE pg_class.oid = pg_index.indexrelid AND pg_class.oid IN (
                     SELECT indexrelid
                     FROM pg_index, pg_class
                     JOIN pg_namespace nsp ON pg_class.relnamespace = nsp.oid
                     WHERE pg_class.relname='%s' %s AND
                         pg_class.oid=pg_index.indrelid
                         AND indisunique != 't' AND indisprimary != 't' )""" \
              % (self._quote_str(table), schema_where)
        self._exec_sql(c, sql)
        indexes = []
        for row in c.fetchall():
            indexes.append(TableIndex(row))
        return indexes

    def get_table_constraints(self, table, schema=None):
        c = self.con.cursor()

        schema_where = (" AND nspname='%s' "
                        % self._quote_str(schema) if schema is not None else ''
                        )
        sql = """SELECT c.conname, c.contype, c.condeferrable, c.condeferred,
                        array_to_string(c.conkey, ' '), c.consrc, t2.relname,
                        c.confupdtype, c.confdeltype, c.confmatchtype,
                        array_to_string(c.confkey, ' ')
              FROM pg_constraint c
              LEFT JOIN pg_class t ON c.conrelid = t.oid
              LEFT JOIN pg_class t2 ON c.confrelid = t2.oid
              JOIN pg_namespace nsp ON t.relnamespace = nsp.oid
              WHERE t.relname = '%s' %s """ \
              % (self._quote_str(table), schema_where)

        self._exec_sql(c, sql)

        constrs = []
        for row in c.fetchall():
            constrs.append(TableConstraint(row))
        return constrs

    def get_view_definition(self, view, schema=None):
        """Returns definition of the view."""

        schema_where = (" AND nspname='%s' "
                        % self._quote_str(schema) if schema is not None else ''
                        )
        sql = """SELECT pg_get_viewdef(c.oid)
              FROM pg_class c
              JOIN pg_namespace nsp ON c.relnamespace = nsp.oid
              WHERE relname='%s' %s AND relkind='v'""" \
              % (self._quote_str(view), schema_where)
        c = self.con.cursor()
        self._exec_sql(c, sql)
        return c.fetchone()[0]

    def add_geometry_column(self, table, geom_type, schema=None,
                            geom_column='the_geom', srid=-1, dim=2):
        # Use schema if explicitly specified
        if schema:
            schema_part = "'%s', " % self._quote_str(schema)
        else:
            schema_part = ''
        sql = "SELECT AddGeometryColumn(%s'%s', '%s', %d, '%s', %d)" % (
            schema_part,
            self._quote_str(table),
            self._quote_str(geom_column),
            srid,
            self._quote_str(geom_type),
            dim,
        )
        self._exec_sql_and_commit(sql)

    def delete_geometry_column(self, table, geom_column, schema=None):
        """Use PostGIS function to delete geometry column correctly."""

        if schema:
            schema_part = "'%s', " % self._quote_str(schema)
        else:
            schema_part = ''
        sql = "SELECT DropGeometryColumn(%s'%s', '%s')" % (schema_part,
                self._quote_str(table), self._quote_str(geom_column))
        self._exec_sql_and_commit(sql)

    def delete_geometry_table(self, table, schema=None):
        """Delete table with one or more geometries using postgis function."""

        if schema:
            schema_part = "'%s', " % self._quote_str(schema)
        else:
            schema_part = ''
        sql = "SELECT DropGeometryTable(%s'%s')" % (schema_part,
                self._quote_str(table))
        self._exec_sql_and_commit(sql)

    def create_table(self, table, fields, pkey=None, schema=None):
        """Create ordinary table.

        'fields' is array containing instances of TableField
        'pkey' contains name of column to be used as primary key
        """

        if len(fields) == 0:
            return False

        table_name = self._table_name(schema, table)

        sql = 'CREATE TABLE %s (%s' % (table_name, fields[0].field_def())
        for field in fields[1:]:
            sql += ', %s' % field.field_def()
        if pkey:
            sql += ', PRIMARY KEY (%s)' % self._quote(pkey)
        sql += ')'
        self._exec_sql_and_commit(sql)
        return True

    def delete_table(self, table, schema=None):
        """Delete table from the database."""

        table_name = self._table_name(schema, table)
        sql = 'DROP TABLE %s' % table_name
        self._exec_sql_and_commit(sql)

    def empty_table(self, table, schema=None):
        """Delete all rows from table."""

        table_name = self._table_name(schema, table)
        sql = 'DELETE FROM %s' % table_name
        self._exec_sql_and_commit(sql)

    def rename_table(self, table, new_table, schema=None):
        """Rename a table in database."""

        table_name = self._table_name(schema, table)
        sql = 'ALTER TABLE %s RENAME TO %s' % (table_name,
                self._quote(new_table))
        self._exec_sql_and_commit(sql)

        # Update geometry_columns if postgis is enabled
        if self.has_postgis:
            sql = "UPDATE geometry_columns SET f_table_name='%s' \
                   WHERE f_table_name='%s'" \
                   % (self._quote_str(new_table), self._quote_str(table))
            if schema is not None:
                sql += " AND f_table_schema='%s'" % self._quote_str(schema)
            self._exec_sql_and_commit(sql)

    def create_view(self, name, query, schema=None):
        view_name = self._table_name(schema, name)
        sql = 'CREATE VIEW %s AS %s' % (view_name, query)
        self._exec_sql_and_commit(sql)

    def delete_view(self, name, schema=None):
        view_name = self._table_name(schema, name)
        sql = 'DROP VIEW %s' % view_name
        self._exec_sql_and_commit(sql)

    def rename_view(self, name, new_name, schema=None):
        """Rename view in database."""

        self.rename_table(name, new_name, schema)

    def create_schema(self, schema):
        """Create a new empty schema in database."""

        sql = 'CREATE SCHEMA %s' % self._quote(schema)
        self._exec_sql_and_commit(sql)

    def delete_schema(self, schema):
        """Drop (empty) schema from database."""

        sql = 'DROP SCHEMA %s' % self._quote(schema)
        self._exec_sql_and_commit(sql)

    def rename_schema(self, schema, new_schema):
        """Rename a schema in database."""

        sql = 'ALTER SCHEMA %s RENAME TO %s' % (self._quote(schema),
                self._quote(new_schema))
        self._exec_sql_and_commit(sql)

        # Update geometry_columns if postgis is enabled
        if self.has_postgis:
            sql = \
                "UPDATE geometry_columns SET f_table_schema='%s' \
                 WHERE f_table_schema='%s'" \
                 % (self._quote_str(new_schema), self._quote_str(schema))
            self._exec_sql_and_commit(sql)

    def table_add_column(self, table, field, schema=None):
        """Add a column to table (passed as TableField instance)."""

        table_name = self._table_name(schema, table)
        sql = 'ALTER TABLE %s ADD %s' % (table_name, field.field_def())
        self._exec_sql_and_commit(sql)

    def table_delete_column(self, table, field, schema=None):
        """Delete column from a table."""

        table_name = self._table_name(schema, table)
        sql = 'ALTER TABLE %s DROP %s' % (table_name, self._quote(field))
        self._exec_sql_and_commit(sql)

    def table_column_rename(self, table, name, new_name, schema=None):
        """Rename column in a table."""

        table_name = self._table_name(schema, table)
        sql = 'ALTER TABLE %s RENAME %s TO %s' % (table_name,
                self._quote(name), self._quote(new_name))
        self._exec_sql_and_commit(sql)

        # Update geometry_columns if postgis is enabled
        if self.has_postgis:
            sql = "UPDATE geometry_columns SET f_geometry_column='%s' \
                   WHERE f_geometry_column='%s' AND f_table_name='%s'" \
                   % (self._quote_str(new_name), self._quote_str(name),
                      self._quote_str(table))
            if schema is not None:
                sql += " AND f_table_schema='%s'" % self._quote(schema)
            self._exec_sql_and_commit(sql)

    def table_column_set_type(self, table, column, data_type, schema=None):
        """Change column type."""

        table_name = self._table_name(schema, table)
        sql = 'ALTER TABLE %s ALTER %s TYPE %s' % (table_name,
                self._quote(column), data_type)
        self._exec_sql_and_commit(sql)

    def table_column_set_default(self, table, column, default, schema=None):
        """Change column's default value.

        If default=None drop default value.
        """

        table_name = self._table_name(schema, table)
        if default:
            sql = 'ALTER TABLE %s ALTER %s SET DEFAULT %s' % (table_name,
                    self._quote(column), default)
        else:
            sql = 'ALTER TABLE %s ALTER %s DROP DEFAULT' % (table_name,
                    self._quote(column))
        self._exec_sql_and_commit(sql)

    def table_column_set_null(self, table, column, is_null, schema=None):
        """Change whether column can contain null values."""

        table_name = self._table_name(schema, table)
        sql = 'ALTER TABLE %s ALTER %s ' % (table_name, self._quote(column))
        if is_null:
            sql += 'DROP NOT NULL'
        else:
            sql += 'SET NOT NULL'
        self._exec_sql_and_commit(sql)

    def table_add_primary_key(self, table, column, schema=None):
        """Add a primery key (with one column) to a table."""

        table_name = self._table_name(schema, table)
        sql = 'ALTER TABLE %s ADD PRIMARY KEY (%s)' % (table_name,
                self._quote(column))
        self._exec_sql_and_commit(sql)

    def table_add_unique_constraint(self, table, column, schema=None):
        """Add a unique constraint to a table."""

        table_name = self._table_name(schema, table)
        sql = 'ALTER TABLE %s ADD UNIQUE (%s)' % (table_name,
                self._quote(column))
        self._exec_sql_and_commit(sql)

    def table_delete_constraint(self, table, constraint, schema=None):
        """Delete constraint in a table."""

        table_name = self._table_name(schema, table)
        sql = 'ALTER TABLE %s DROP CONSTRAINT %s' % (table_name,
                self._quote(constraint))
        self._exec_sql_and_commit(sql)

    def table_move_to_schema(self, table, new_schema, schema=None):
        if new_schema == schema:
            return
        table_name = self._table_name(schema, table)
        sql = 'ALTER TABLE %s SET SCHEMA %s' % (table_name,
                self._quote(new_schema))
        self._exec_sql_and_commit(sql)

        # Update geometry_columns if postgis is enabled
        if self.has_postgis:
            sql = "UPDATE geometry_columns SET f_table_schema='%s' \
                   WHERE f_table_name='%s'" \
                   % (self._quote_str(new_schema), self._quote_str(table))
            if schema is not None:
                sql += " AND f_table_schema='%s'" % self._quote_str(schema)
            self._exec_sql_and_commit(sql)

    def create_index(self, table, name, column, schema=None):
        """Create index on one column using default options."""

        table_name = self._table_name(schema, table)
        idx_name = self._quote(name)
        sql = 'CREATE INDEX %s ON %s (%s)' % (idx_name, table_name,
                self._quote(column))
        self._exec_sql_and_commit(sql)

    def create_spatial_index(self, table, schema=None, geom_column='the_geom'):
        table_name = self._table_name(schema, table)
        idx_name = self._quote('sidx_' + table)
        sql = 'CREATE INDEX %s ON %s USING GIST(%s)' % (idx_name, table_name,
                self._quote(geom_column))
        self._exec_sql_and_commit(sql)

    def delete_index(self, name, schema=None):
        index_name = self._table_name(schema, name)
        sql = 'DROP INDEX %s' % index_name
        self._exec_sql_and_commit(sql)

    def get_database_privileges(self):
        """DB privileges: (can create schemas, can create temp. tables).
        """

        sql = "SELECT has_database_privilege('%(d)s', 'CREATE'), \
                      has_database_privilege('%(d)s', 'TEMP')" \
              % {'d': self._quote_str(self.dbname)}
        c = self.con.cursor()
        self._exec_sql(c, sql)
        return c.fetchone()

    def get_schema_privileges(self, schema):
        """Schema privileges: (can create new objects, can access objects
        in schema)."""

        sql = "SELECT has_schema_privilege('%(s)s', 'CREATE'), \
                      has_schema_privilege('%(s)s', 'USAGE')" \
              % {'s': self._quote_str(schema)}
        c = self.con.cursor()
        self._exec_sql(c, sql)
        return c.fetchone()

    def get_table_privileges(self, table, schema=None):
        """Table privileges: (select, insert, update, delete).
        """

        t = self._table_name(schema, table)
        sql = """SELECT has_table_privilege('%(t)s', 'SELECT'),
                        has_table_privilege('%(t)s', 'INSERT'),
                        has_table_privilege('%(t)s', 'UPDATE'),
                        has_table_privilege('%(t)s', 'DELETE')""" \
              % {'t': self._quote_str(t)}
        c = self.con.cursor()
        self._exec_sql(c, sql)
        return c.fetchone()

    def vacuum_analyze(self, table, schema=None):
        """Run VACUUM ANALYZE on a table."""

        t = self._table_name(schema, table)

        # VACUUM ANALYZE must be run outside transaction block - we
        # have to change isolation level
        self.con.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        c = self.con.cursor()
        self._exec_sql(c, 'VACUUM ANALYZE %s' % t)
        self.con.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)

    def sr_info_for_srid(self, srid):
        if not self.has_postgis:
            return 'Unknown'

        try:
            c = self.con.cursor()
            self._exec_sql(c,
                    "SELECT srtext FROM spatial_ref_sys WHERE srid = '%d'"
                    % srid)
            srtext = c.fetchone()[0]

            # Try to extract just SR name (should be qouted in double
            # quotes)
            x = re.search('"([^"]+)"', srtext)
            if x is not None:
                srtext = x.group()
            return srtext
        except DbError, e:
            return 'Unknown'

    def insert_table_row(self, table, values, schema=None, cursor=None):
        """Insert a row with specified values to a table.

        If a cursor is specified, it doesn't commit (expecting that
        there will be more inserts) otherwise it commits immediately.
        """

        t = self._table_name(schema, table)
        sql = ''
        for value in values:
            # TODO: quote values?
            if sql:
                sql += ', '
            sql += value
        sql = 'INSERT INTO %s VALUES (%s)' % (t, sql)
        if cursor:
            self._exec_sql(cursor, sql)
        else:
            self._exec_sql_and_commit(sql)

    def _exec_sql(self, cursor, sql):
        try:
            cursor.execute(sql)
        except psycopg2.Error, e:
            raise DbError(e.message, e.cursor.query)

    def _exec_sql_and_commit(self, sql):
        """Tries to execute and commit some action, on error it rolls
        back the change.
        """

        try:
            c = self.con.cursor()
            self._exec_sql(c, sql)
            self.con.commit()
        except DbError, e:
            self.con.rollback()
            raise

    def _quote(self, identifier):
        """Quote identifier if needed."""

        # Make sure it's python unicode string
        identifier = unicode(identifier)

        # Is it needed to quote the identifier?
        if self.re_ident_ok.match(identifier) is not None:
            return identifier

        # It's needed - let's quote it (and double the double-quotes)
        return u'"%s"' % identifier.replace('"', '""')

    def _quote_str(self, txt):
        """Make the string safe - replace ' with ''.
        """

        # make sure it's python unicode string
        txt = unicode(txt)
        return txt.replace("'", "''")

    def _table_name(self, schema, table):
        if not schema:
            return self._quote(table)
        else:
            return u'%s.%s' % (self._quote(schema), self._quote(table))

#################################################################################################

    # def list_schemas_privilege(self, user='current_user', privilege='USAGE'):
    #     """Get list of schema where the user has the privilege gived
    #     Privilege on schema can be : 'USAGE' or 'CREATE'
    #     """
    #     if privilege not in ('USAGE', 'CREATE'):
    #       return []
    #     else:
    #       all_schemas_list = [s[1] for s in self.list_schemas()]
    #       schema_list_with_privilege = [s for s in all_schemas_list if self.has_schema_privilege(user,s,privilege)]
    #       return sorted(schema_list_with_privilege)

    def list_schemas_privilege(self, user='current_user', privilege = ['USAGE']):
        """Get list of where the user has the privilege given
        Privilege on schema : 'USAGE', 'CREATE', ['CREATE', 'USAGE']
        """
        if user == 'current_user':
          sql="""\
            WITH privilege_schema as (
              SELECT {user} as user, c.nspname as schema, array(select privs from unnest(ARRAY[
            ( CASE WHEN has_schema_privilege({user},c.oid,'CREATE') THEN 'CREATE' ELSE NULL END),
            (CASE WHEN has_schema_privilege({user},c.oid,'USAGE') THEN 'USAGE' ELSE NULL END)])foo(privs) WHERE privs IS NOT NULL) as privilege
            FROM pg_namespace c 
            where has_schema_privilege({user},c.oid,'CREATE,USAGE') AND (nspname != 'information_schema' AND nspname !~ 'pg_')
            ORDER BY user, nspname)

            SELECT * from privilege_schema
            WHERE ARRAY{privilege} <@ privilege""".format(user=user,privilege=str(privilege))
          #print(sql)
        else:
          sql = """\
          WITH privilege_schema as (
              SELECT '{user}' as user, c.nspname as schema, array(select privs from unnest(ARRAY[
            ( CASE WHEN has_schema_privilege('{user}',c.oid,'CREATE') THEN 'CREATE' ELSE NULL END),
            (CASE WHEN has_schema_privilege('{user}',c.oid,'USAGE') THEN 'USAGE' ELSE NULL END)])foo(privs) WHERE privs IS NOT NULL) as privilege
            FROM pg_namespace c 
            where has_schema_privilege('{user}',c.oid,'CREATE,USAGE') AND (nspname != 'information_schema' AND nspname !~ 'pg_')
            ORDER BY user, nspname)

            SELECT * from privilege_schema
            WHERE ARRAY{privilege} <@ privilege""".format(user=user, privilege=str(privilege))
          #print(sql)
        try:
          c = self.con.cursor()
          self._exec_sql(c, sql)
          return [schema[1] for schema in c.fetchall()]
        except:
          return []

    def list_table_privilege(self, user='current_user', schema = None, privilege = ['SELECT'], type=None ):
        """Get list of object (table or view) where the user has the privilege given
        type : 'r' for table
                'v' for view
        Privilege on table : SELECT, INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER
        """
        if user == 'current_user':
          sql ="""\
          with table_privilege as (SELECT c.oid::regclass, pg_namespace.nspname as schema, c.relname as object,
                            c.relkind as type, pg_get_userbyid(relowner) as owner,
                            pg_attribute.attname as geomColumn,
                            array(select privs from unnest(ARRAY [ 
        ( CASE WHEN has_table_privilege({user},c.oid,'SELECT') THEN 'SELECT' ELSE NULL END),
        (CASE WHEN has_table_privilege({user},c.oid,'INSERT') THEN 'INSERT' ELSE NULL END),
        (CASE WHEN has_table_privilege({user},c.oid,'UPDATE') THEN 'UPDATE' ELSE NULL END),
        (CASE WHEN has_table_privilege({user},c.oid,'DELETE') THEN 'DELETE' ELSE NULL END),
        (CASE WHEN has_table_privilege({user},c.oid,'TRUNCATE') THEN 'TRUNCATE' ELSE NULL END),
        (CASE WHEN has_table_privilege({user},c.oid,'REFERENCES') THEN 'REFERENCES' ELSE NULL END),
        (CASE WHEN has_table_privilege({user},c.oid,'TRIGGER') THEN 'TRIGGER' ELSE NULL END)]) foo(privs) where privs is not null) as privilege
                  FROM pg_class c
                  JOIN pg_namespace ON pg_namespace.oid = c.relnamespace
                  LEFT OUTER JOIN pg_attribute ON
                      pg_attribute.attrelid = c.oid AND
                      (pg_attribute.atttypid = 'geometry'::regtype
                      OR pg_attribute.atttypid IN
                          (SELECT oid FROM pg_type
                           WHERE typbasetype='geometry'::regtype))
                  WHERE pg_namespace.nspname not in ('information_schema','pg_catalog','sys') and c.relkind IN ('v', 'r') and pg_attribute.atttypid = 'geometry'::regtype and
        has_table_privilege({user},c.oid,'SELECT, INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER') AND has_schema_privilege({user},c.relnamespace,'USAGE'))

        select * from table_privilege
        where ARRAY{privilege} <@ privilege""". format(user=user, privilege=str(privilege))
        else:
          sql="""\
          with table_privilege as (SELECT c.oid::regclass, pg_namespace.nspname as schema, c.relname as object,
                              c.relkind as type, pg_get_userbyid(relowner) as owner,
                              pg_attribute.attname as geomColumn,
                              array(select privs from unnest(ARRAY [ 
          ( CASE WHEN has_table_privilege('{user}',c.oid,'SELECT') THEN 'SELECT' ELSE NULL END),
          (CASE WHEN has_table_privilege('{user}',c.oid,'INSERT') THEN 'INSERT' ELSE NULL END),
          (CASE WHEN has_table_privilege('{user}',c.oid,'UPDATE') THEN 'UPDATE' ELSE NULL END),
          (CASE WHEN has_table_privilege('{user}',c.oid,'DELETE') THEN 'DELETE' ELSE NULL END),
          (CASE WHEN has_table_privilege('{user}',c.oid,'TRUNCATE') THEN 'TRUNCATE' ELSE NULL END),
          (CASE WHEN has_table_privilege('{user}',c.oid,'REFERENCES') THEN 'REFERENCES' ELSE NULL END),
          (CASE WHEN has_table_privilege('{user}',c.oid,'TRIGGER') THEN 'TRIGGER' ELSE NULL END)]) foo(privs) where privs is not null) as privilege
                    FROM pg_class c
                    JOIN pg_namespace ON pg_namespace.oid = c.relnamespace
                    LEFT OUTER JOIN pg_attribute ON
                        pg_attribute.attrelid = c.oid AND
                        (pg_attribute.atttypid = 'geometry'::regtype
                        OR pg_attribute.atttypid IN
                            (SELECT oid FROM pg_type
                             WHERE typbasetype='geometry'::regtype))
                    WHERE pg_namespace.nspname not in ('information_schema','pg_catalog','sys') and c.relkind IN ('v', 'r') and pg_attribute.atttypid = 'geometry'::regtype and
        has_table_privilege('{user}',c.oid,'SELECT, INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER') AND has_schema_privilege('{user}',c.relnamespace,'USAGE'))

        select * from table_privilege
        where ARRAY{privilege} <@ privilege
          """. format(user=user, privilege=str(privilege))
        if type:
          sql += """ AND type='{}'""".format(type)
        if schema:
          sql += """ AND schema='{}'""".format(schema)
        sql += """ ORDER BY schema, object"""
        try:
          c = self.con.cursor()
          self._exec_sql(c, sql)
          return [oid[0] for oid in c.fetchall()]
        except:
          return []

    def has_schema_privilege (self, user='current_user', schema='public', privilege = 'USAGE'):
        c = self.con.cursor()
        if user == 'current_user':
          sql = "SELECT has_schema_privilege({},'{}','{}')".format(user, schema, privilege)
        else:
          sql = "SELECT has_schema_privilege('{}','{}','{}')".format(user, schema, privilege)
        self._exec_sql(c, sql)
        return c.fetchone()[0]

    def has_table_privilege(self, user='current_user', schema='public', privilege = 'SELECT'):
        c = self.con.cursor()
        if user == 'current_user':
          sql = "SELECT h"

    def list_unique_value_column(self, column, table, schema = 'public'):
        c = self.con.cursor()
        sql = """SELECT DISTINCT "{}" FROM "{}"."{}"\
        """.format(column, schema, table)
        if column in self.get_table_columnname(table, schema):
          self._exec_sql(c,sql)
          return [row[0] for row in c.fetchall()]
        else:
          raise DbError("Wrong parameter(s)", sql)

    def get_table_columnname(self, table, schema = 'public'):
        c = self.con.cursor()
        fields = self.get_table_fields(table, schema)
        return [f.name for f in fields]

#########################################################################################################
# For debugging / testing
if __name__ == '__main__':

    db = GeoDB(host='10.231.241.7', port= 8765, dbname='bddirno', user='servanne.quiniou', passwd='dirno')
    print db.list_schemas()
    print '=========='

    for row in db.list_geotables():
        print row
    print '=========='

    for row in db.get_table_indexes('trencin'):
        print row
    print '=========='

    for row in db.get_table_constraints('trencin'):
        print row
    print '=========='

    print db.get_table_rows('trencin')

    # for fld in db.get_table_metadata('trencin'):
    # ....print fld
    # try:
    # ....db.create_table('trrrr', [('id','serial'), ('test','text')])
    # except DbError, e:
    # ....print e.message, e.query
