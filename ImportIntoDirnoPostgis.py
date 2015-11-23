# -*- coding: utf-8 -*-

"""
***************************************************************************
    ImportIntoDirnoPostgis.py
    ---------------------
    Date                 : October 2015
    Copyright            : (C) 2015 by DIRNO/PEGR
    Email                : dict dot pegr dot spt dot dirno at developpement-durable dot gouv dot fr
***************************************************************************
*                                                                         *
*   This program is free software; you can redistribute it and/or modify  *
*   it under the terms of the GNU General Public License as published by  *
*   the Free Software Foundation; either version 2 of the License, or     *
*   (at your option) any later version.                                   *
*                                                                         *
***************************************************************************
"""

__author__ = 'DIRNO/PEGR'
__date__ = 'October 2015'
__copyright__ = '(C) 2015, DIRNO/PEGR'

# This will get replaced with a git SHA1 when you do a git archive

__revision__ = '$Format:%H$'

from PyQt4.QtCore import QSettings
from qgis.core import QgsDataSourceURI, QgsVectorLayerImport

from processing.core.GeoAlgorithm import GeoAlgorithm
from processing.core.GeoAlgorithmExecutionException import GeoAlgorithmExecutionException
from qgis.core import QgsDataSourceURI, QgsVectorLayerImport, QgsMapLayerRegistry, QgsMessageLog, QgsCoordinateReferenceSystem
from processing.core.parameters import ParameterBoolean
from processing.core.parameters import ParameterVector
from processing.core.parameters import ParameterString
from processing.core.parameters import ParameterSelection
from processing.core.parameters import ParameterTableField
from processing.tools import dataobjects
import custom_postgis_utils as postgis_utils


class ImportIntoDirnoPostgis(GeoAlgorithm):

    DATABASE = 'DATABASE'
    TABLENAME = 'TABLENAME'
    SCHEMA = 'SCHEMA'
    INPUT = 'INPUT'
    PRIMARY_KEY = 'PRIMARY_KEY'

    def processAlgorithm(self, progress):
        connection = 'Test_Fin'
        schema = self.dbSchemaNames[self.getParameterValue(self.SCHEMA)]
        primaryKeyField = self.getParameterValue(self.PRIMARY_KEY)
        settings = QSettings()
        mySettings = '/PostgreSQL/connections/' + connection
        try:
            database = settings.value(mySettings + '/database')
            username = settings.value(mySettings + '/username')
            host = settings.value(mySettings + '/host')
            port = settings.value(mySettings + '/port', type=int)
            password = settings.value(mySettings + '/password')
        except Exception, e:
            raise GeoAlgorithmExecutionException(
                self.tr('Mauvais logs de connection: %s' % connection))
        QgsMessageLog.logMessage(u"Debut de l'import", 'processing dirno', QgsMessageLog.INFO)
        layerUri = self.getParameterValue(self.INPUT)
        layer = dataobjects.getObjectFromUri(layerUri)
        QgsMapLayerRegistry.instance().addMapLayer(layer)
        providerName = 'postgres'

        try:
            db = postgis_utils.GeoDB(host=host, port=port, dbname=database,
                                     user=username, passwd=password)
        except postgis_utils.DbError, e:
            raise GeoAlgorithmExecutionException(
                self.tr("Impossible de se connecter a la database:\n%s" % e.message))

        geomColumn = 'geom'
        hasGeometry = True
        #clear geometry column for non-geometry tables
        if not layer.hasGeometryType():
            geomColumn = None
            hasGeometry = False
        
        #Definition du nom de la table
        table = self.getParameterValue(self.TABLENAME).strip()
        if table == '':
            table = layer.name().lower()
        table.replace(' ', '')
            
        #Option du script    
        options = {}
        options['overwrite'] = True
        options['lowercaseFieldNames'] = True
        options['dropStringConstraints'] = True
        createIndex = True

        #clear geometry column for non-geometry tables
        if not layer.hasGeometryType():
            geomColumn = None
        
        # Import de la couche
        #self.importTable(db, table, schema, primaryKeyField, geomColumn, options)
        
        uri = QgsDataSourceURI()
        uri.setConnection(host, str(port), database, username, password)
        newCrs = QgsCoordinateReferenceSystem()
        newCrs.createFromUserInput(u"EPSG:2154")
        
        #Gid = cle primaire par defaut
        uri.setDataSource(schema, table, geomColumn, '', "gid")
        
        progress.setInfo(u"Importation de la table vers PostgreSQL (cela peut prendre un moment)")
        
        #Import
        (ret, errMsg) = QgsVectorLayerImport.importLayer(
            layer,
            uri.uri(),
            providerName,
            newCrs,
            False,
            False,
            options,
        )
        if ret != 0:
            raise GeoAlgorithmExecutionException(
                self.tr('Erreur importation dans PostGIS\n%s' % errMsg))
                
        if primaryKeyField:
            sql2 ="""alter table "{schema}".{table} drop constraint {table}_pkey;""".format(schema=schema,table=table)
            sql2 +="""alter table "{schema}".{table} add primary key ({primaryKeyField});""".format(schema=schema,table=table,primaryKeyField=primaryKeyField)
            sql2 +="""COMMIT;"""
            
            try:
                db._exec_sql_and_commit(str(sql2))
            except:
                msg = u"{} - ATTENTION: Erreur clé primaire (gid recuperé par defaut)".format(self.name)
                iface.messageBar().pushMessage(msg, level=QgsMessageBar.WARNING)
                QgsMessageLog.logMessage(msg, 'processing dirno', QgsMessageLog.INFO)
                pass
        
        QgsMessageLog.logMessage(u"Fin de l'import", 'processing dirno', QgsMessageLog.INFO)
        
        sql3 = """BEGIN;"""      
        sql3 +="""CREATE OR REPLACE VIEW "{schema}".{table}_view AS SELECT * FROM "{schema}".{table};""".format(schema=schema,table=table)
        sql3 +="""COMMIT;"""

        try:
            db._exec_sql_and_commit(str(sql3))
        except postgis_utils.DbError, e:
                raise GeoAlgorithmExecutionException(
                    self.tr('Erreur execution SQL:\n%s' % e.message))    
        
        if geomColumn and createIndex:
            db.create_spatial_index(table, schema, geomColumn)

        db.vacuum_analyze(table, schema)

    def dbConnectionNames(self):
        settings = QSettings()
        settings.beginGroup('/PostgreSQL/connections/')
        return settings.childGroups()

    def dbSchemaNames(self):
        settings = QSettings()
        #connection = self.DB_CONNECTIONS[self.getParameterValue(self.DATABASE)]
        connection = u'Test_Fin'
        mySettings = '/PostgreSQL/connections/' + connection
        try:
            database = settings.value(mySettings + '/database')
            username = settings.value(mySettings + '/username')
            host = settings.value(mySettings + '/host')
            port = settings.value(mySettings + '/port', type=int)
            password = settings.value(mySettings + '/password')
        except Exception, e:
            raise GeoAlgorithmExecutionException(
                self.tr('Mauvais logs de connection: %s' % connection))
        try:
            db = postgis_utils.GeoDB(host=host, port=port, dbname=database,
                                     user=username, passwd=password)
        except postgis_utils.DbError, e:
            raise GeoAlgorithmExecutionException(
                self.tr("Impossible de se connecter a la database:\n%s" % e.message))
        return db.list_schemas_privilege(privilege=['USAGE'])

    def defineCharacteristics(self):
        self.name = u'Importation de toute la donnée'
        self.group = 'Database SERVEUR'
        self.addParameter(ParameterVector(self.INPUT,
            self.tr(u'Couche à importer')))
        self.addParameter(ParameterString(self.TABLENAME,
            self.tr('Renommmer la table en... (laisser blanc pour utiliser le nom du layer)')))
        self.addParameter(ParameterTableField(self.PRIMARY_KEY,
            self.tr('Nom de la colonne cle primaire'), optional=True))    
        self.dbSchemaNames = self.dbSchemaNames()
        self.addParameter(ParameterSelection(self.SCHEMA,
            self.tr(u'Schema (nom du schema)'), self.dbSchemaNames))


