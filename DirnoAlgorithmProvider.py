# -*- coding: utf-8 -*-

"""
***************************************************************************
    DirnoAlgorithmProvider.py
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
import shutil
import PyQt4.QtGui
from qgis.core import QgsMessageLog
from processing.core.AlgorithmProvider import AlgorithmProvider
from processing.core.ProcessingConfig import Setting, ProcessingConfig
# from ImportIntoPostGIS_COMMUNE import ImportIntoPostGIS_COMMUNE
# from ImportIntoPostGIS_DEPARTEMENT import ImportIntoPostGIS_DEPARTEMENT
# from ImportIntoPostGIS_DIRNO import ImportIntoPostGIS_DIRNO
# from ImportIntoPostGIS_PARCELLE import ImportIntoPostGIS_PARCELLE
# from ImportIntoPostGIS2 import ImportIntoPostGIS2
# from updateModelAction import updateModelAction

# from ImportIntoDirnoPostgis import ImportIntoDirnoPostgis
# from ImportIntoDirnoPostgisCei import ImportIntoDirnoPostgisCei
# from ImportIntoDirnoPostgisDistrict import ImportIntoDirnoPostgisDistrict

from deleteDirnoPostgisLayer import deleteDirnoPostgisLayer

from Modif_Droit import Modif_Droit

class DirnoAlgorithmProvider(AlgorithmProvider):

    MY_DUMMY_SETTING = 'MY_DUMMY_SETTING'

    def __init__(self):
        AlgorithmProvider.__init__(self)

        # Deactivate provider by default
        self.activate = True
        
        #add action to update
        #self.actions = [updateModelAction()]

        # Load algorithms
        self.alglist = [
        deleteDirnoPostgisLayer()]
        for alg in self.alglist:
            alg.provider = self

        #build help files
        self.helpFilesBuilded()

    def initializeSettings(self):
        """In this method we add settings needed to configure our
        provider.

        Do not forget to call the parent method, since it takes care
        or automatically adding a setting for activating or
        deactivating the algorithms in the provider.
        """
        AlgorithmProvider.initializeSettings(self)
        ProcessingConfig.addSetting(Setting('Dirno algorithmes',
            DirnoAlgorithmProvider.MY_DUMMY_SETTING,
            'Example setting', 'Default value'))

    def unload(self):
        """Setting should be removed here, so they do not appear anymore
        when the plugin is unloaded.
        """
        AlgorithmProvider.unload(self)
        ProcessingConfig.removeSetting(
            DirnoAlgorithmProvider.MY_DUMMY_SETTING)

    def getName(self):
        """This is the name that will appear on the toolbox group.

        It is also used to create the command line name of all the
        algorithms from this provider.
        """
        return 'Dirno provider'

    def getDescription(self):
        """This is the provired full name.
        """
        return 'Dirno scripts'

    def getIcon(self):
        """We return the default icon.
        """
        #return AlgorithmProvider.getIcon(self)
        return PyQt4.QtGui.QIcon(os.path.dirname(__file__) + "/icons/iconProvider.png")

    def _loadAlgorithms(self):
        """Here we fill the list of algorithms in self.algs.

        This method is called whenever the list of algorithms should
        be updated. If the list of algorithms can change (for instance,
        if it contains algorithms from user-defined scripts and a new
        script might have been added), you should create the list again
        here.

        In this case, since the list is always the same, we assign from
        the pre-made list. This assignment has to be done in this method
        even if the list does not change, since the self.algs list is
        cleared before calling this method.
        """
        self.algs = self.alglist

    def helpFilesBuilded(self):
        """ Build help files : QWebView does not support relative path in *.html files
        """
        path = os.path.dirname(__file__)
        helpFolder = path + '/help'
        helpSourceFolder = path + '/help_src'
        helpIniFile = helpFolder + '/help.ini'

        if os.path.isdir(helpFolder):   
            if os.path.isfile(helpIniFile):
                f = open(helpIniFile, 'r')
                if path == f.readline():
                    f.close()
                    return True
                else:
                    shutil.copyfile(helpSourceFolder + '/style.css', helpFolder  + '/style.css' )
                    self.buildHelpFiles(helpIniFile, path)
            else:
                shutil.copyfile(helpSourceFolder + '/style.css', helpFolder  + '/style.css' )
                self.buildHelpFiles(helpIniFile, path)
        else:
            os.mkdir(helpFolder)
            shutil.copyfile(helpSourceFolder + '/style.css', helpFolder  + '/style.css' )
            self.buildHelpFiles(helpIniFile, path)

    def buildHelpFiles(self, helpIniFile, path):
        f = open(helpIniFile, 'w')
        f.write(path)
        f.close()
        htmlFiles = [fic for fic in os.listdir(path +'/help_src') if '.html' in fic]
        #QgsMessageLog.logMessage(str(htmlFiles), 'processing dirno', QgsMessageLog.INFO)
        for f in htmlFiles:
            htmlFile = open(path + '/help_src/' + f, 'r')
            htmlFileContent = htmlFile.readlines()
            QgsMessageLog.logMessage(str(htmlFileContent), 'processing dirno', QgsMessageLog.INFO)
            htmlFile.close()
            htmlFile = open(path + '/help/' + f, 'w')
            for line in htmlFileContent:
                htmlFile.write(line.format(path=path))
            htmlFile.close()