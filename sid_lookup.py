from ldap3 import Server, Connection, ALL, NONE, NTLM, Tls, SIMPLE, ASYNC, SUBTREE, SYNC
from ldap3.utils.conv import escape_bytes, to_raw

from ad_config import *

TEST_SID = "S-1-5-21-4202559609-2341556158-3224923410-1111"


def bind():
    """Use ad_config settings, bind to ldap defined there, return Connection"""
    s = Server(server_name)
    c = Connection(s, user_name, password, auto_bind='NONE')

    c.open()
    c.start_tls()
    c.bind()

    return c


def accountname_from_sid(sid):
    """From text SID get accountname via LDAP query"""
    connection = bind()
    binary_sid = to_raw(sid)  # your sid must be in binary format
    connection.search(baseDN,
                      '(objectsid=' + escape_bytes(binary_sid) + ')',
                      attributes=['samaccountname'])
    return connection.response[0]['attributes'][u'sAMAccountName']


if __name__ == '__main__':
    print accountname_from_sid(TEST_SID)
