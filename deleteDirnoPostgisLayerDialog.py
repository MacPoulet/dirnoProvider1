# -*- coding: utf-8 -*-

"""
***************************************************************************
    deleteDirnoPostgisLayerDialog.py
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

import os

from PyQt4.QtGui import QComboBox, QSpacerItem, QLabel, QComboBox, QMessageBox, QWidget
from PyQt4.QtCore import QSettings
from PyQt4 import uic

from qgis.core import QgsMessageLog, QgsApplication

from processing.gui.AlgorithmDialog import AlgorithmDialog, AlgorithmDialogBase
from processing.gui.ParametersPanel import ParametersPanel

import custom_postgis_utils as postgis_utils

class CustomParam:
    def __init__(self):
        tableWidget = QComboBox()
        tableWidget.addItems(self.dbTableNames())
        self.customParam = {'TABLE': tableWidget}

    def getCustomParam(self):
        return self.customParam

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
        return db.list_table_privilege(privilege=['DELETE'], type = 'r')

class CustomParametersPanel(ParametersPanel):
    def __init__(self, parent, alg):
        self.customParam = CustomParam().getCustomParam()
        ParametersPanel.__init__(self, parent, alg)

    def getWidgetFromParameter(self, param):
        if param.name in self.customParam.keys():
            return self.customParam[param.name]
        return ParametersPanel.getWidgetFromParameter(self, param)


class deleteDirnoPostgisLayerDialog(AlgorithmDialog):
    def __init__(self, alg):
        AlgorithmDialogBase.__init__(self,alg)
        self.customParam = CustomParam().getCustomParam()
        self.mainWidget = CustomParametersPanel(self, alg)
        self.setMainWidget()

    def setParamValue(self, param, widget, alg=None):
        if param.name in self.customParam.keys():
            if isinstance(self.customParam[param.name], QComboBox):
                return param.setValue(widget.currentText())
        return AlgorithmDialog.setParamValue(self, param, widget, alg)


