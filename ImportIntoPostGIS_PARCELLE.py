# -*- coding: utf-8 -*-

"""
***************************************************************************
    ImportIntoPostGIS_PARCELLE.py
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
from PyQt4.QtGui import *
from PyQt4.QtCore import QSettings
from qgis.core import QgsDataSourceURI, QgsVectorLayerImport, QgsMapLayerRegistry, QgsMessageLog, QgsCoordinateReferenceSystem
import processing
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



class ImportIntoPostGIS_PARCELLE(GeoAlgorithm):

    DATABASE = 'DATABASE'
    DEP = 'DEP'
    SCHEMA = 'SCHEMA'
    INPUT = 'INPUT'
    OVERWRITE = 'OVERWRITE'
    CREATEINDEX = 'CREATEINDEX'
    LOWERCASE_NAMES = 'LOWERCASE_NAMES'
    DROP_STRING_LENGTH = 'DROP_STRING_LENGTH'

    def processAlgorithm(self, progress):
        connection = self.DB_CONNECTIONS[self.getParameterValue(self.DATABASE)]
        schema = "0-referentiels-ign"
        dep = self.getParameterValue(self.DEP)
        overwrite = self.getParameterValue(self.OVERWRITE)
        createIndex = self.getParameterValue(self.CREATEINDEX)
        convertLowerCase = self.getParameterValue(self.LOWERCASE_NAMES)
        dropStringLength = self.getParameterValue(self.DROP_STRING_LENGTH)
        settings = QSettings()
        mySettings = '/PostgreSQL/connections/' + connection
        progress.setInfo(u"Tentative de connection a PostgreSQL:")
        try:
            database = settings.value(mySettings + '/database')
            username = settings.value(mySettings + '/username')
            host = settings.value(mySettings + '/host')
            port = settings.value(mySettings + '/port', type=int)
            password = settings.value(mySettings + '/password')
        except Exception, e:
            raise GeoAlgorithmExecutionException(
                self.tr(u'Mauvais nom de connection pour la base de donnee: %s' % connection))

        progress.setInfo(u"Réussi...")
        QgsMessageLog.logMessage(u"Début de l'import", 'processing dirno', QgsMessageLog.INFO)
        
        layerUri = self.getParameterValue(self.INPUT)
        layer = dataobjects.getObjectFromUri(layerUri,forceLoad=True)
        QgsMapLayerRegistry.instance().addMapLayer(layer)
        nom = layer.name().split('/')[-1]
        
        if 'parcelle' in nom.lower():
            table = "parcelle_dirno_"+str(self.getParameterValue(self.DEP))
        else:    
            if 'localisant' in nom.lower():
                table = "localisant_dirno_"+str(self.getParameterValue(self.DEP))
            else:
                raise GeoAlgorithmExecutionException(self.tr(u'Mauvais fichier (parcelle ou localisant seulement): %s' % connection))
            
        progress.setText("")
        progress.setInfo(u"Initialisation du script, patientez...")
        providerName = 'postgres'
        
        try:
            db = postgis_utils.GeoDB(host=host, port=port, dbname=database,
                                     user=username, passwd=password)
        except postgis_utils.DbError, e:
            raise GeoAlgorithmExecutionException(
                self.tr(u"Impossible de ce connecter à la database:\n%s" % e.message))

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

        progress.setText("")
        progress.setInfo(u"Importation de la table vers PostgreSQL (cela peut prendre un moment)")
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
        progress.setText("")
        progress.setInfo(u"Création de la table des intersections...")
        
        sql = """BEGIN;"""
        sql +="""CREATE TABLE "{schema}"."intersect_{dep}" AS (select * from "{schema}"."{table}", "0-Stockage"."tampon_dirno" where (ST_Intersects("{schema}"."{table}"."geom","0-Stockage"."tampon_dirno"."geometry")) = true);""".format(dep=dep,table=table,schema=schema)
        sql +="""COMMIT;"""
        sql = sql.replace('\n', ' ')

        try:
            db._exec_sql_and_commit(str(sql))
        except postgis_utils.DbError, e:
            raise GeoAlgorithmExecutionException(
                self.tr('Erreur execution SQL:\n%s' % e.message))
        db.vacuum_analyze(table, schema)
        
        progress.setInfo(u"Réussi...")
        QgsMessageLog.logMessage(u"Fin de l'import", 'processing dirno', QgsMessageLog.INFO)
        
    def dbConnectionNames(self):
        settings = QSettings()
        settings.beginGroup('/PostgreSQL/connections/')
        return settings.childGroups()

    def defineCharacteristics(self):
        self.name = 'Import LOCALISANT ou PARCELLE'
        self.group = 'Database DIRNO'
        self.addParameter(ParameterVector(self.INPUT,
            self.tr(u'VSMAP a importer')))
        self.DB_CONNECTIONS = self.dbConnectionNames()
        self.addParameter(ParameterSelection(self.DATABASE,
            self.tr(u'Database (nom de la connection)'), self.DB_CONNECTIONS))
        self.addParameter(ParameterString(self.DEP,
            self.tr(u'Entrer le département'), ''))
        self.addParameter(ParameterBoolean(self.OVERWRITE,
            self.tr(u'Ecraser si déja existant'), True))
        self.addParameter(ParameterBoolean(self.CREATEINDEX,
            self.tr(u'Creer un index spatial'), True))
        self.addParameter(ParameterBoolean(self.LOWERCASE_NAMES,
            self.tr(u'Convertir le nom des champs en minuscule'), True))
        self.addParameter(ParameterBoolean(self.DROP_STRING_LENGTH,
            self.tr(u'Enleve les contraintes de taille sur les champs de caractères'), False))
