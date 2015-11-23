# -*- coding: utf-8 -*-

"""
***************************************************************************
    DirnoAlgorithmProfil.py
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

import os, utils
from PyQt4 import uic

pluginPath = os.path.split(os.path.dirname(__file__))[0]
WIDGET, BASE = uic.loadUiType(
    os.path.join(pluginPath, 'dirnoProvider', 'ui', 'DirnoAlgorithmProfilDialog.ui'))

class DirnoAlgorithmProfilDialog(BASE, WIDGET):
    def __init__(self):
        super(DirnoAlgorithmProfilDialog, self).__init__()
        self.setupUi(self)
        self.initGui()
        self.exec_()

    def initGui(self):
    	self.comboBoxConnection.addItems(utils.dbDirnoConnectionNames())

