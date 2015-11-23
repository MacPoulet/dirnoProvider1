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
from qgis.core import QgsDataSourceURI, QgsVectorLayerImport
from qgis.gui import QgsMessageBar
from processing.core.GeoAlgorithm import GeoAlgorithm
from processing.core.GeoAlgorithmExecutionException import GeoAlgorithmExecutionException
from qgis.core import QgsDataSourceURI, QgsVectorLayerImport, QgsMapLayerRegistry, QgsMessageLog, QgsCoordinateReferenceSystem
from processing.core.parameters import ParameterBoolean
from processing.core.parameters import ParameterVector
from processing.core.parameters import ParameterString
from processing.core.parameters import ParameterSelection
from processing.core.parameters import ParameterTableField
from processing.tools import dataobjects
from qgis.utils import iface
import custom_postgis_utils as postgis_utils


class Modif_Droit(GeoAlgorithm):
    TABLE = 'TABLE'
    PAR_SCHEMA = 'PAR_SCHEMA'
    TABLE_PUBLIQUE = 'TABLE_PUBLIQUE'


    def processAlgorithm(self, progress):
        connection = 'Test_Fin'
        table = self.dbTableNames[self.getParameterValue(self.TABLE)]
        par_Schema = self.getParameterValue(self.PAR_SCHEMA)
        table_Publique = self.getParameterValue(self.TABLE_PUBLIQUE)
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
                
        QgsMessageLog.logMessage(u"Debut de la suppression", 'processing dirno', QgsMessageLog.INFO)

        providerName = 'postgres'

        try:
            db = postgis_utils.GeoDB(host=host, port=port, dbname=database,
                                     user=username, passwd=password)
        except postgis_utils.DbError, e:
            raise GeoAlgorithmExecutionException(
                self.tr("Impossible de se connecter a la database:\n%s" % e.message))
            
           
        #Changement des privileges sur la table
        if par_Schema: 
            sql ="""GRANT ALL PRIVILEGES ON {} TO public;""".format(table)
            try:
                db._exec_sql_and_commit(str(sql))
            except postgis_utils.DbError, e:
                raise GeoAlgorithmExecutionException(self.tr('Erreur execution SQL:\n%s' % e.message))
        
        if table_Publique:
            sql2 ="""GRANT SELECT ON {} TO public;""".format(table)
            try:
                db._exec_sql_and_commit(str(sql2))
            except postgis_utils.DbError, e:
                raise GeoAlgorithmExecutionException(
                    self.tr('Erreur execution SQL:\n%s' % e.message))
                    
        QgsMessageLog.logMessage(u"Fin des modifications", 'processing dirno', QgsMessageLog.INFO)
        
        
    def dbConnectionNames(self):
        settings = QSettings()
        settings.beginGroup('/PostgreSQL/connections/')
        return settings.childGroups()
        
        
    def dbTableNames(self):
        settings = QSettings()
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
        return db.list_table_privilege(privilege=['UPDATE'], type = 'r')


    def defineCharacteristics(self):
        self.name = u'MODIFICATION droit table serveur'
        self.group = 'Database SERVEUR'
        self.dbTableNames = self.dbTableNames()
        self.addParameter(ParameterSelection(self.TABLE,
            self.tr(u'Selectionner la table à supprimer'), self.dbTableNames))
        self.addParameter(ParameterBoolean(self.PAR_SCHEMA,
            self.tr(u'Donner les droits a ceux dans le schema'), True))
        self.addParameter(ParameterBoolean(self.TABLE_PUBLIQUE,
            self.tr(u'Rendre la table publique'), False))
  
        




