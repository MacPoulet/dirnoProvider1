# -*- coding: utf-8 -*-

"""
***************************************************************************
    deleteDirnoPostgisLayer.py
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
import os
#load the custom postgis_utils
import custom_postgis_utils as postgis_utils
from deleteDirnoPostgisLayerDialog import deleteDirnoPostgisLayerDialog


class deleteDirnoPostgisLayer(GeoAlgorithm):
    TABLE = 'TABLE'
    ESSAI = 'ESSAI'

    def processAlgorithm(self, progress):
        connection = 'Test_Fin'
        table = self.getParameterValue(self.TABLE)
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
            
           
        #Suppression de la table
        sql = """BEGIN;"""
        sql +="""drop table {table} CASCADE;""".format(table=table)
        sql +="""COMMIT;"""
        sql = sql.replace('\n', ' ')
        QgsMessageLog.logMessage(u"Fin de la suppression", 'processing dirno', QgsMessageLog.INFO)
        try:
            db._exec_sql_and_commit(str(sql))
        except postgis_utils.DbError, e:
            raise GeoAlgorithmExecutionException(
                self.tr('Erreur execution SQL:\n%s' % e.message))

    def defineCharacteristics(self):
        self.cmdName = "deleteDirnoPostgisLayer"
        self.name = u'supprimer une couche du serveur'
        self.group = 'Database SERVEUR'
        self.addParameter(ParameterString(self.TABLE,
            self.tr(u'Selectionner la couche à supprimer')))
        self.addParameter(ParameterString(self.ESSAI,
            self.tr(u'Mon essai')))

    def help(self):
        helppath = os.path.join(os.path.dirname(__file__), "help", self.cmdName + ".html")
        if os.path.isfile(helppath):
            return False, helppath
        else:
            return False, None 
  
    def getCustomParametersDialog(self):
        return deleteDirnoPostgisLayerDialog(self)
