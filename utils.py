# -*- coding: utf-8 -*-

"""
***************************************************************************
    utils.py
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
__date__ = 'November 2015'
__copyright__ = '(C) 2015, DIRNO/PEGR'

# This will get replaced with a git SHA1 when you do a git archive

__revision__ = '$Format:%H$'

from PyQt4.QtCore import QSettings
from processing.core.GeoAlgorithmExecutionException import GeoAlgorithmExecutionException

_HOST = '10.231.241.7'
_DATABASE = 'Test_final'
_PORT = 8765


def dbConnectionNames():
    settings = QSettings()
    settings.beginGroup('/PostgreSQL/connections/')
    return settings.childGroups()

def dbDirnoConnectionNames():
    """return connection where host = '10.231.241.7'
    """
    settings = QSettings()
    dbConnection = []
    for connection in dbConnectionNames():
        mySettings = '/PostgreSQL/connections/' + connection
        database = settings.value(mySettings + '/database')
        host = settings.value(mySettings + '/host')
        port = settings.value(mySettings + '/port', type=int)
        if host == _HOST and database == _DATABASE and port == _PORT:
            dbConnection.append(connection)
    return dbConnection

def getParametersConnection(connection):
    settings = QSettings()
    mySettings = '/PostgreSQL/connections/' + connection
    try:
        database = settings.value(mySettings + '/database')
        username = settings.value(mySettings + '/username')
        host = settings.value(mySettings + '/host')
        port = settings.value(mySettings + '/port', type=int)
        password = settings.value(mySettings + '/password')
        return (database, username, host, port, password)
    except Exception, e:
        pass
        #raise GeoAlgorithmExecutionException(
         #   self.tr('Mauvais logs de connection: %s' % connection))
