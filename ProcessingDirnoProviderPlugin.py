# -*- coding: utf-8 -*-

"""
***************************************************************************
    __init__.py
    ---------------------
    Date                 : July 2013
    Copyright            : (C) 2013 by Victor Olaya
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

__author__ = 'Jean-Daniel Lomenède'
__date__ = 'Ocotber 2015'
__copyright__ = '(C) 2015, Jean-Daniel Lomenède'

# This will get replaced with a git SHA1 when you do a git archive

__revision__ = '$Format:%H$'

import os
import sys
import inspect
from PyQt4.QtGui import QAction, QIcon

from processing.core.Processing import Processing
from DirnoAlgorithmProvider import DirnoAlgorithmProvider
from DirnoAlgorithmProfil import DirnoAlgorithmProfilDialog

cmd_folder = os.path.split(inspect.getfile(inspect.currentframe()))[0]

if cmd_folder not in sys.path:
    sys.path.insert(0, cmd_folder)


class ProcessingDirnoProviderPlugin:

    def __init__(self, iface):
        self.provider = DirnoAlgorithmProvider()
        self.iface = iface

    def initGui(self):
        Processing.addProvider(self.provider)
        self.profilAction = QAction(QIcon(os.path.dirname(__file__) + "/icons/iconProvider.png"), "Ajouter un profil", self.iface.mainWindow())
        self.profilAction.setObjectName("dirnoAction")
        self.profilAction.setWhatsThis("Dirno plugin")
        self.profilAction.setStatusTip("This is status tip")
        #QObject.connect(self.profilAction, SIGNAL("triggered()"), self.run)

        # add toolbar button and menu item
        #self.iface.addToolBarIcon(self.profilAction)
        self.iface.addPluginToMenu("&Dirno scripts", self.profilAction)
        self.profilAction.triggered.connect(self.launchDirnoAlgorithProfilDialog)

    def unload(self):
        Processing.removeProvider(self.provider)
        self.iface.removePluginMenu("&Dirno scripts", self.profilAction)
        #self.iface.removeToolBarIcon(self.profilAction)

    def launchDirnoAlgorithProfilDialog(self):
        return DirnoAlgorithmProfilDialog()
