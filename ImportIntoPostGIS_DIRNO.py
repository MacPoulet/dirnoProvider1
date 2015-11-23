# -*- coding: utf-8 -*-

"""
***************************************************************************
    ImportIntoPostGIS.py
    ---------------------
    Date                 : October 2012
    Copyright            : (C) 2012 by Victor Olaya
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

__author__ = 'Victor Olaya'
__date__ = 'October 2012'
__copyright__ = '(C) 2012, Victor Olaya'

# This will get replaced with a git SHA1 when you do a git archive

__revision__ = '$Format:%H$'

from PyQt4.QtCore import QSettings
from qgis.core import QgsDataSourceURI, QgsVectorLayerImport, QgsMapLayerRegistry, QgsMessageLog, QgsCoordinateReferenceSystem

from processing.core.GeoAlgorithm import GeoAlgorithm
from processing.core.GeoAlgorithmExecutionException import GeoAlgorithmExecutionException
from processing.core.parameters import ParameterBoolean
from processing.core.parameters import ParameterVector
from processing.core.parameters import ParameterString
from processing.core.parameters import ParameterSelection
from processing.core.parameters import ParameterTableField
from processing.tools import dataobjects
from processing.algs.qgis import postgis_utils
from  datetime import date


class ImportIntoPostGIS_DIRNO(GeoAlgorithm):

    DATABASE = 'DATABASE'
    MILLESIME = 'MILLESIME'
    SCHEMA = 'SCHEMA'
    INPUT = 'INPUT'
    OVERWRITE = 'OVERWRITE'
    CREATEINDEX = 'CREATEINDEX'
    LOWERCASE_NAMES = 'LOWERCASE_NAMES'
    DROP_STRING_LENGTH = 'DROP_STRING_LENGTH'
    PRIMARY_KEY = 'PRIMARY_KEY'

    def processAlgorithm(self, progress):
        connection = self.DB_CONNECTIONS[self.getParameterValue(self.DATABASE)]
        schema = "0-referentiels-ign"
        millesime = self.getParameterValue(self.MILLESIME)
        overwrite = self.getParameterValue(self.OVERWRITE)
        createIndex = self.getParameterValue(self.CREATEINDEX)
        convertLowerCase = self.getParameterValue(self.LOWERCASE_NAMES)
        dropStringLength = self.getParameterValue(self.DROP_STRING_LENGTH)
        primaryKeyField = self.getParameterValue(self.PRIMARY_KEY)
        settings = QSettings()
        mySettings = '/PostgreSQL/connections/' + connection
        progress.setInfo("Tentative de connection a PostgreSQL:")
        try:
            database = settings.value(mySettings + '/database')
            username = settings.value(mySettings + '/username')
            host = settings.value(mySettings + '/host')
            port = settings.value(mySettings + '/port', type=int)
            password = settings.value(mySettings + '/password')
        except Exception, e:
            raise GeoAlgorithmExecutionException(
                self.tr('Mauvais nom de connection de base de donnee: %s' % connection))
        progress.setInfo(u"Réussi...")
        QgsMessageLog.logMessage(u"Début de l'import", 'processing dirno', QgsMessageLog.INFO)
        
        layerUri = self.getParameterValue(self.INPUT)
        layer = dataobjects.getObjectFromUri(layerUri,forceLoad=True)
        QgsMapLayerRegistry.instance().addMapLayer(layer)
        layer.setSubsetString("gestionnaire='DIRNO'")
        table = "reseau_dirno_"+str(self.getParameterValue(self.MILLESIME))
        progress.setInfo("Initialisation du script, patientez...")
        providerName = 'postgres'

        try:
            db = postgis_utils.GeoDB(host=host, port=port, dbname=database,
                                     user=username, passwd=password)
        except postgis_utils.DbError, e:
            raise GeoAlgorithmExecutionException(
                self.tr("Impossible de ce connecter a la database:\n%s" % e.message))

        geomColumn = 'geom'


        options = {}
        if overwrite:
            options['overwrite'] = True
        if convertLowerCase:
            options['lowercaseFieldNames'] = True
            geomColumn = geomColumn.lower()
        if dropStringLength:
            options['dropStringConstraints'] = True

        #clear geometry column for non-geometry tables
        if not layer.hasGeometryType():
            geomColumn = None

        uri = QgsDataSourceURI()
        uri.setConnection(host, str(port), database, username, password)
        if primaryKeyField:
            uri.setDataSource(schema, table, geomColumn, '', primaryKeyField)
        else:
            uri.setDataSource(schema, table, geomColumn, '')
        
        progress.setInfo(u"Importation du réseau dirno vers PostgreSQL (cela peut prendre un moment)")

        (ret, errMsg) = QgsVectorLayerImport.importLayer(
            layer,
            uri.uri(),
            providerName,
            self.crs,
            False,
            False,
            options,
        )
        
        if ret != 0:
            raise GeoAlgorithmExecutionException(
                self.tr('Erreur importation dans PostGIS\n%s' % errMsg))
        progress.setInfo(u"Réussi...")
        if geomColumn and createIndex:
            db.create_spatial_index(table, schema, geomColumn)

        db.vacuum_analyze(table, schema)
        progress.setInfo(u"Création du buffer...")
        sql = """BEGIN;"""
        sql +="""DROP TABLE IF EXISTS "{schema}"."tampon_dirno";""".format(schema=schema)
        sql +="""CREATE TABLE "{schema}"."tampon_dirno" AS (SELECT lib_rte, ST_buffer(ST_union(geom),'50') as geometry from "{schema}"."{table}" GROUP BY lib_rte);""".format(table=table,schema=schema)
        sql +="""COMMIT;"""
        sql = sql.replace('\n', ' ')
        
        try:
            db._exec_sql_and_commit(str(sql))
        except postgis_utils.DbError, e:
            raise GeoAlgorithmExecutionException(
                self.tr('Erreur execution SQL:\n%s' % e.message))
        db.vacuum_analyze(table, schema)
        
        QgsMessageLog.logMessage(u"Fin de l'import", 'processing dirno', QgsMessageLog.INFO)
        
        progress.setInfo(u"Réussi...")        
    def dbConnectionNames(self):
        settings = QSettings()
        settings.beginGroup('/PostgreSQL/connections/')
        return settings.childGroups()

    def defineCharacteristics(self):
        self.name = 'Import VSMAP_SERVEUR'
        self.group = 'Database DIRNO'
        self.addParameter(ParameterVector(self.INPUT,
            self.tr('VSMAP a importer')))
        self.DB_CONNECTIONS = self.dbConnectionNames()
        self.addParameter(ParameterSelection(self.DATABASE,
            self.tr('Database (nom de la connection)'), self.DB_CONNECTIONS))
        self.addParameter(ParameterString(self.MILLESIME,
            self.tr(u'Entrer le millesime (année en cours par defaut)'), date.today().year))
        self.addParameter(ParameterBoolean(self.OVERWRITE,
            self.tr('Ecraser si deja existant'), True))
        self.addParameter(ParameterBoolean(self.CREATEINDEX,
            self.tr('Creer un index spatial'), True))
        self.addParameter(ParameterBoolean(self.LOWERCASE_NAMES,
            self.tr('Convertir le nom des champs en minuscule'), True))
        self.addParameter(ParameterBoolean(self.DROP_STRING_LENGTH,
            self.tr(u'Enlever la contrainte de taille sur les champs de caractères'), False))
